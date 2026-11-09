package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/quic-go/quic-go"
	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
)

const streamTypeAncestry byte = 0x05

type ancestryReader interface {
	ReadAncestry(string, effects.Tip, func(*pb.AncestryResponse) error) error
}

// ReadAncestry streams resident ancestry through its missing boundaries. No
// network loads or frontier publication happen on the serving node. QUIC
// backpressure bounds buffered bytes; each frame retains the normal size limit.
func (r *EngineLogReader) ReadAncestry(key string, root effects.Tip, send func(*pb.AncestryResponse) error) error {
	if r.effectCache == nil {
		return fmt.Errorf("effect cache not initialized")
	}

	var sendErr error

	count, bytes := 0, 0

	r.effectCache.VisitAncestry(key, root, func(tip effects.Tip, eff *pb.Effect, isRoot bool) bool {
		resp := &pb.AncestryResponse{Ref: &pb.EffectRef{NodeId: tip[0], Offset: tip[1]}, IsRoot: isRoot}
		if eff != nil {
			resp.EffectData, sendErr = MarshalEffectWire(eff)
			if sendErr != nil {
				return false
			}
		}

		if len(resp.GetEffectData()) > maxFrameSize-128 {
			sendErr = fmt.Errorf("ancestry effect exceeds frame limit")

			return false
		}

		sendErr = send(resp)
		if sendErr == nil && eff != nil {
			RecordFetchServed()
		}

		count++
		bytes += len(resp.GetEffectData())

		return sendErr == nil
	})
	slog.Debug("ancestry: served stream", "key", key, "root", root, "records", count, "bytes", bytes, "error", sendErr)

	return sendErr
}

func handleAncestryStream(stream *quic.Stream, reader LogReader) {
	defer stream.CancelRead(0)
	// Bound a stalled reader without holding any node/publication locks.
	if err := stream.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return
	}

	req := &pb.AncestryRequest{}
	if err := readMessage(stream, req); err != nil {
		return
	}

	var err error
	if source, ok := reader.(ancestryReader); ok && req.GetRoot() != nil && req.GetKey() != "" {
		err = source.ReadAncestry(req.GetKey(), effects.Tip{req.GetRoot().GetNodeId(), req.GetRoot().GetOffset()},
			func(resp *pb.AncestryResponse) error {
				if err := stream.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
					return err
				}

				return writeMessage(stream, resp)
			})
	} else {
		err = fmt.Errorf("resident ancestry reader unavailable or invalid request")
	}

	end := &pb.AncestryResponse{Done: true}
	if err != nil {
		end.Error = err.Error()
	}

	if err := writeMessage(stream, end); err != nil {
		stream.CancelWrite(0)
	}
}

func ancestryOverConn(ctx context.Context, conn *quic.Conn, key string, root effects.Tip,
	have func(effects.Tip) bool, receive func(effects.Tip, []byte, bool) error,
) error {
	if conn == nil {
		return ErrPeerUnavailable
	}

	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	defer stream.CancelRead(0)
	defer stream.CancelWrite(0)

	stop := context.AfterFunc(ctx, func() {
		stream.CancelRead(0)
		stream.CancelWrite(0)
	})
	defer stop()

	if err := stream.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}

	if _, err := stream.Write([]byte{streamTypeAncestry}); err != nil {
		return err
	}

	if err := writeMessage(stream, &pb.AncestryRequest{Key: key, Root: &pb.EffectRef{NodeId: root[0], Offset: root[1]}}); err != nil {
		return err
	}

	if err := stream.Close(); err != nil {
		return err
	}

	found, answered := false, false

	var missing []effects.Tip

	for count := 0; ; count++ {
		if err := stream.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
			return err
		}

		resp := &pb.AncestryResponse{}
		if err := readMessage(stream, resp); err != nil {
			return err
		}

		if resp.GetError() != "" {
			return fmt.Errorf("ancestry source: %s", resp.GetError())
		}

		if resp.GetDone() {
			if !answered {
				return fmt.Errorf("ancestry source omitted root")
			}

			if !found {
				return effects.ErrEffectNotFound
			}

			// Publish boundary evidence only after the stream finishes.
			for _, tip := range missing {
				if err := receive(tip, nil, false); err != nil {
					return err
				}
			}

			return nil
		}

		if resp.GetRef() == nil {
			return fmt.Errorf("ancestry source omitted effect identity")
		}

		tip := effects.Tip{resp.GetRef().GetNodeId(), resp.GetRef().GetOffset()}
		if count == 0 && tip != root {
			return fmt.Errorf("ancestry source returned a different root")
		}

		if tip == root {
			answered, found = true, len(resp.GetEffectData()) != 0
		}

		if len(resp.GetEffectData()) == 0 {
			missing = append(missing, tip)

			continue
		}

		if have(tip) {
			if resp.GetIsRoot() {
				if err := receive(tip, nil, true); err != nil {
					return err
				}
			}
			// Residency does not prove that this vertex or its siblings have closure.
			continue
		}

		if err := receive(tip, resp.GetEffectData(), resp.GetIsRoot()); err != nil {
			return err
		}
	}
}

func (pm *PeerManager) ancestryConn(peer NodeId) *quic.Conn {
	token := pm.mu.RLock()

	var conn *quic.Conn
	if pc := pm.peers[peer]; pc != nil {
		conn = pc.GetQuicConn()
	}

	pm.mu.RUnlock(token)

	if conn == nil || conn.Context().Err() != nil {
		pm.inboundMu.RLock()
		conn = pm.inboundConns[peer]
		pm.inboundMu.RUnlock()
	}

	return conn
}

// FetchAncestry streams one selected holder's available closure, with one
// fallback peer. Subscription replication reconciles further divergence.
func (pm *PeerManager) FetchAncestry(key string, root effects.Tip, peers []pb.NodeID,
	have func(effects.Tip) bool, receive func(effects.Tip, []byte, bool) error,
) error {
	var unavailable error

	for _, peer := range peers[:min(2, len(peers))] {
		err := ancestryOverConn(pm.ctx, pm.ancestryConn(peer), key, root, have, receive)
		if err == nil {
			return nil
		}

		if !errors.Is(err, effects.ErrEffectNotFound) {
			unavailable = errors.Join(unavailable, fmt.Errorf("peer %d: %w", peer, err))
		}
	}

	token := pm.mu.RLock()
	cdn := pm.cdnFetcher
	pm.mu.RUnlock(token)

	if cdn != nil {
		ctx, cancel := context.WithTimeout(pm.ctx, cloudBackstopTimeout)
		defer cancel()

		data, err := cdn.FetchFromCDN(ctx, &pb.EffectRef{NodeId: root[0], Offset: root[1]})
		if err == nil && len(data) > 0 {
			return receive(root, data, false)
		}

		if err != nil && !errors.Is(err, effects.ErrCDNBlobMissing) {
			unavailable = errors.Join(unavailable, err)
		}

		if unavailable != nil {
			return unavailable
		}

		if err != nil {
			return err
		}

		return fmt.Errorf("CDN returned empty ancestry effect without absence evidence")
	}

	if unavailable != nil {
		return unavailable
	}

	if len(peers) == 0 {
		return ErrPeerUnavailable
	}

	return effects.ErrEffectNotFound
}
