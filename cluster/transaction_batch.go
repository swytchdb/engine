package cluster

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/quic-go/quic-go"
	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"google.golang.org/protobuf/proto"
)

// A transaction uses one notification stream and one existing ACK/NACK. Each
// frame keeps the ordinary size limit; no whole-transaction buffer is required.
// Enabled only after the cluster's minimum version supports batch delivery.
const wireTransaction byte = 2

type transactionTransport interface {
	SendTransaction(NodeId, []byte, []*pb.OffsetNotify) error
}

type transactionHandler interface {
	HandleTransaction(*pb.OffsetNotify, effects.TransactionReceiver) ([]*pb.NackNotify, error)
}

type transactionPacketHandler interface {
	HandleTransactionNotify(NodeId, uint64, *pb.OffsetNotify, effects.TransactionReceiver)
}

func (pm *PeerManager) ReplicateTransaction(_ *pb.OffsetNotify, body []byte, records []*pb.OffsetNotify, target pb.NodeID) ([]*pb.NackNotify, error) {
	if pm.replicator == nil {
		return nil, ErrNoPeers
	}

	return pm.replicator.replicateBodyWith(body, target, func(packet []byte) error {
		transport, ok := pm.replicator.transport.(transactionTransport)
		if !ok {
			return fmt.Errorf("transaction transport unavailable")
		}

		return transport.SendTransaction(target, packet, records)
	})
}

func (pm *PeerManager) HandleTransactionNotify(peer NodeId, request uint64, notify *pb.OffsetNotify, receive effects.TransactionReceiver) {
	if pm.replicator == nil {
		return
	}

	handler, ok := pm.replicator.handler.(transactionHandler)
	if !ok {
		return
	}

	nacks, err := handler.HandleTransaction(notify, receive)
	if err != nil {
		slog.Warn("transaction batch ingestion failed", "peer", peer, "request", request, "error", err)

		return
	}

	pm.replicator.respondToNotify(peer, request, nacks)
}

func (h *EngineEffectHandler) HandleTransaction(notify *pb.OffsetNotify, receive effects.TransactionReceiver) ([]*pb.NackNotify, error) {
	if h.engine == nil {
		return nil, fmt.Errorf("transaction receiver unavailable")
	}

	return h.engine.HandleTransaction(notify, receive)
}

func (t *QUICNotifyTransport) SendTransaction(peer NodeId, packet []byte, records []*pb.OffsetNotify) error {
	conn := t.connFunc(peer)
	if conn == nil {
		return ErrPeerUnavailable
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream, err := conn.OpenUniStreamSync(ctx)
	if err != nil {
		return err
	}

	complete := false
	defer func() {
		if !complete {
			stream.CancelWrite(1)
		}
	}()

	if err := stream.SetWriteDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return err
	}

	var header [9]byte
	binary.LittleEndian.PutUint64(header[:8], uint64(t.nodeID))

	header[8] = wireTransaction
	if _, err := stream.Write(header[:]); err != nil {
		return err
	}

	if err := writeTransactionFrame(stream, packet); err != nil {
		return err
	}

	var count [8]byte
	binary.LittleEndian.PutUint64(count[:], uint64(len(records)))

	if _, err := stream.Write(count[:]); err != nil {
		return err
	}

	for _, record := range records {
		data, err := proto.Marshal(&pb.FetchResponse{Ref: record.GetOrigin(), EffectData: record.GetEffectData()})
		if err != nil {
			return err
		}

		if err := writeTransactionFrame(stream, data); err != nil {
			return err
		}
	}

	if err := stream.Close(); err != nil {
		return err
	}

	complete = true

	RecordQUICStreamOpened()

	return nil
}

// Frames are [4:length][1:compression][payload]. Compression and frame buffers
// have a per-record lifetime, rather than one goroutine per record.
func writeTransactionFrame(w io.Writer, data []byte) error {
	if len(data) > maxFrameSize {
		return fmt.Errorf("transaction frame exceeds limit: %d", len(data))
	}

	flag := wireRaw

	if len(data) >= compressThreshold {
		if compressed := effects.Compress(data); len(compressed) < len(data) {
			data, flag = compressed, wireZstd
		}
	}

	var header [5]byte
	binary.LittleEndian.PutUint32(header[:4], uint32(len(data)))

	header[4] = flag
	if _, err := w.Write(header[:]); err != nil {
		return err
	}

	_, err := w.Write(data)

	return err
}

func readTransactionFrame(r io.Reader) ([]byte, error) {
	var header [5]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}

	size := binary.LittleEndian.Uint32(header[:4])
	if size > maxFrameSize {
		return nil, fmt.Errorf("transaction frame exceeds limit: %d", size)
	}

	data := make([]byte, size)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}

	switch header[4] {
	case wireZstd:
		var err error

		data, err = effects.Decompress(data)
		if err != nil {
			return nil, err
		}
	case wireRaw:
	default:
		return nil, fmt.Errorf("invalid transaction frame compression")
	}

	if len(data) > maxFrameSize {
		return nil, fmt.Errorf("decoded transaction frame exceeds limit: %d", len(data))
	}

	return data, nil
}

func receiveTransactionRecords(r io.Reader, accept func(*pb.FetchResponse) error) error {
	var count [8]byte
	if _, err := io.ReadFull(r, count[:]); err != nil {
		return err
	}

	for remaining := binary.LittleEndian.Uint64(count[:]); remaining > 0; remaining-- {
		data, err := readTransactionFrame(r)
		if err != nil {
			return err
		}

		record := &pb.FetchResponse{}
		if err := proto.Unmarshal(data, record); err != nil {
			return err
		}

		if err := accept(record); err != nil {
			return err
		}
	}
	// A clean FIN is part of complete delivery. Reset, truncation, or trailing
	// records must not allow the bind to be processed.
	var extra [1]byte
	if n, err := r.Read(extra[:]); n != 0 || err != io.EOF {
		return fmt.Errorf("transaction stream did not end cleanly: %v", err)
	}

	return nil
}

func (t *QUICNotifyTransport) handleTransactionStream(peer NodeId, stream *quic.ReceiveStream) {
	if err := stream.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return
	}

	packet, err := readTransactionFrame(stream)
	if err != nil || len(packet) == 0 || packet[0] != PacketTypeNotify {
		return
	}

	request, notify, err := parseNotifyPacket(packet)
	if err != nil || request == 0 {
		return
	}

	handler, ok := t.handler.(transactionPacketHandler)
	if !ok {
		return
	}

	key := [2]uint64{uint64(peer), request}
	if _, loaded := t.activeNotifies.LoadOrStore(key, struct{}{}); loaded {
		return
	}
	defer t.activeNotifies.Delete(key)

	handler.HandleTransactionNotify(peer, request, notify, func(accept func(*pb.FetchResponse) error) error {
		return receiveTransactionRecords(stream, accept)
	})
}
