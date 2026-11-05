/*
 * Copyright 2026 Swytch Labs BV
 *
 * This file is part of Swytch.
 *
 * Swytch is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Swytch is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Swytch. If not, see <https://www.gnu.org/licenses/>.
 *
 * Additional permissions under GNU AGPL version 3 section 7 apply to
 * this file. See the NOTICE.md file distributed with this source code
 * for the current set of additional permissions.
 */

package cluster

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	pb "github.com/swytchdb/engine/cluster/proto"
	"google.golang.org/protobuf/proto"
)

const (
	backoffBase = 100 * time.Millisecond
	backoffCap  = 10 * time.Second

	// Bidirectional request types; notifications use QUIC uni-streams.
	streamTypeFetch   byte = 0x02
	streamTypeForward byte = 0x04
)

// PeerConn manages a single QUIC connection to a peer node.
// Used for fetch, forward, and notification transport.
type PeerConn struct {
	nodeID      NodeId
	addr        string
	region      string // peer's region
	localRegion string // this node's region
	tlsConfig   *tls.Config

	quicConn atomic.Pointer[quic.Conn]

	// onConnected is invoked every time the PeerConn successfully
	// finishes a dial. QUIC is symmetric: once a connection is
	// established, either side can open streams. Without an accept
	// loop on our outbound conn, the peer's return traffic (ACKs,
	// NACKs, fetch responses opened on OUR dialed connection rather
	// than a separate one from their side) would be silently dropped.
	// PeerManager uses this hook to spawn the same stream acceptors
	// it runs on inbound QUIC connections.
	onConnected func(*quic.Conn)

	cancel       context.CancelFunc
	streamReady  atomic.Bool
	firstAttempt chan struct{}
}

// newPeerConn creates a new PeerConn but does not connect.
// onConnected, when non-nil, is invoked after every successful dial
// with the live *quic.Conn — see the onConnected field for the
// rationale.
func newPeerConn(
	nodeID NodeId,
	addr string,
	peerRegion, localRegion string,
	tlsConfig *tls.Config,
	onConnected func(*quic.Conn),
) *PeerConn {
	return &PeerConn{
		nodeID:       nodeID,
		addr:         addr,
		region:       peerRegion,
		localRegion:  localRegion,
		tlsConfig:    tlsConfig,
		onConnected:  onConnected,
		firstAttempt: make(chan struct{}),
	}
}

// GetQuicConn returns the current QUIC connection, or nil if not connected.
func (pc *PeerConn) GetQuicConn() *quic.Conn {
	return pc.quicConn.Load()
}

// IsSameRegion returns true if the peer is in the same region as the local node.
func (pc *PeerConn) IsSameRegion() bool {
	return pc.region != "" && pc.region == pc.localRegion
}

// Start dials the peer and begins the connection loop in a goroutine.
func (pc *PeerConn) Start(ctx context.Context) {
	ctx, pc.cancel = context.WithCancel(ctx)
	go pc.connectLoop(ctx)
}

// Stop closes the connection.
func (pc *PeerConn) Stop() {
	if pc.cancel != nil {
		pc.cancel()
	}

	if conn := pc.GetQuicConn(); conn != nil {
		if err := conn.CloseWithError(0, "shutdown"); err != nil {
			slog.Warn("failed to close peer connection", "peer", pc.nodeID, "error", err)
		}
	}
}

// Fetch opens a short-lived QUIC stream to request effect data from this peer.
func (pc *PeerConn) Fetch(ctx context.Context, offset *pb.EffectRef) ([]byte, error) {
	return fetchOverConn(ctx, pc.GetQuicConn(), offset)
}

// fetchOverConn runs the fetch request/response protocol on an already-open
// QUIC connection. Shared by PeerConn.Fetch (a connection we dialed) and the
// PeerManager's inbound-connection fallback, so a peer we only have an inbound
// connection to is still a valid fetch source — symmetric with the send path,
// which already falls back to inboundConns in the notify transport.
func fetchOverConn(ctx context.Context, conn *quic.Conn, offset *pb.EffectRef) ([]byte, error) {
	resp := &pb.FetchResponse{}
	if err := requestOverConn(ctx, conn, streamTypeFetch, &pb.FetchRequest{Ref: offset}, resp); err != nil {
		return nil, err
	}

	return resp.GetEffectData(), nil
}

// ForwardTransaction opens a QUIC stream to forward a transaction to this peer
// (the serialization leader). Returns the leader's response.
func (pc *PeerConn) ForwardTransaction(ctx context.Context, tx *pb.ForwardedTransaction) (*pb.ForwardedResponse, error) {
	resp := &pb.ForwardedResponse{}
	if err := requestOverConn(ctx, pc.GetQuicConn(), streamTypeForward, tx, resp); err != nil {
		return nil, err
	}

	return resp, nil
}

// requestOverConn owns the shared fetch/forward stream lifecycle and framing.
func requestOverConn(ctx context.Context, conn *quic.Conn, streamType byte, request, response proto.Message) error {
	if conn == nil {
		return ErrPeerUnavailable
	}

	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return fmt.Errorf("open stream: %w", err)
	}
	defer stream.CancelRead(0)

	// Stream I/O does not observe ctx after opening. Register cancellation
	// without keeping a watcher goroutine and channel alive per request.
	stop := context.AfterFunc(ctx, func() {
		stream.CancelWrite(0)
		stream.CancelRead(0)
	})
	defer stop()

	// Write stream type prefix
	if _, err := stream.Write([]byte{streamType}); err != nil {
		return fmt.Errorf("write stream type: %w", err)
	}

	if err := writeMessage(stream, request); err != nil {
		return fmt.Errorf("write request: %w", err)
	}

	// Close write side to signal we're done sending
	if err := stream.Close(); err != nil {
		return fmt.Errorf("close write side: %w", err)
	}

	if err := readMessage(stream, response); err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	return nil
}

// IsStreamReady returns true if the QUIC connection is established.
func (pc *PeerConn) IsStreamReady() bool {
	return pc.streamReady.Load()
}

// connectLoop handles connection establishment and reconnection with backoff.
func (pc *PeerConn) connectLoop(ctx context.Context) {
	backoff := backoffBase

	// Only this loop reports attempts; no synchronization is needed here.
	firstAttempt := pc.firstAttempt

	attempted := func() {
		if firstAttempt != nil {
			close(firstAttempt)
			firstAttempt = nil
		}
	}
	defer attempted()

	for {
		if ctx.Err() != nil {
			return
		}

		err := pc.dial(ctx)
		if err != nil {
			attempted()
			slog.Warn("failed to dial peer", "peer", pc.nodeID, "address", pc.addr, "error", err)

			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}

			backoff = min(backoff*2, backoffCap)

			continue
		}

		// Reset backoff on successful connection
		backoff = backoffBase

		slog.Info("connected to peer", "peer", pc.nodeID, "address", pc.addr)
		RecordPeerConnected(pc.nodeID)
		RecordPeerReconnect(pc.nodeID)

		pc.streamReady.Store(true)

		conn := pc.GetQuicConn()
		if pc.onConnected != nil {
			pc.onConnected(conn)
		}

		attempted()

		// Wait for connection to close
		<-conn.Context().Done()

		pc.streamReady.Store(false)
		pc.quicConn.Store(nil)

		if ctx.Err() != nil {
			RecordPeerDisconnected(pc.nodeID)

			return
		}

		slog.Warn("peer connection lost", "peer", pc.nodeID)
		RecordPeerDisconnected(pc.nodeID)
	}
}

// dial establishes a QUIC connection to the peer.
func (pc *PeerConn) dial(ctx context.Context) error {
	if pc.tlsConfig == nil {
		return fmt.Errorf("TLS configuration is required for cluster connections")
	}

	tlsCfg := pc.tlsConfig
	// QUIC requires ALPN. Use CA verification when available;
	// fall back to Noise-layer identity verification otherwise.
	tlsCfg = tlsCfg.Clone()

	tlsCfg.NextProtos = []string{"swytch-cluster"}
	if tlsCfg.RootCAs == nil {
		tlsCfg.InsecureSkipVerify = true
	}

	conn, err := quic.DialAddr(ctx, pc.addr, tlsCfg, clusterQUICConfig())
	if err != nil {
		return err
	}

	pc.quicConn.Store(conn)
	// Stop can cancel while DialAddr is completing. Publish first, then
	// check cancellation so either Stop or this path closes the connection.
	if ctx.Err() != nil {
		pc.Stop()
	}

	return nil
}

// writeMessage serializes a protobuf message and writes it as [4-byte LE length][data].
func writeMessage(w io.Writer, msg proto.Message) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(data)))

	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}

	_, err = w.Write(data)

	return err
}

// maxFrameSize is the upper bound on a single framed protobuf message (16 MiB).
// Prevents a malicious/buggy peer from triggering unbounded allocations.
const maxFrameSize = 16 << 20

// readMessage reads a [4-byte LE length][data] framed protobuf message.
func readMessage(r io.Reader, msg proto.Message) error {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return err
	}

	length := binary.LittleEndian.Uint32(lenBuf[:])
	if length > maxFrameSize {
		return fmt.Errorf("frame size %d exceeds maximum %d", length, maxFrameSize)
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return err
	}

	return proto.Unmarshal(data, msg)
}
