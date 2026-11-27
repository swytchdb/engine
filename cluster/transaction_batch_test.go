package cluster

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
	"google.golang.org/protobuf/proto"
)

type failingNotifyHandler struct{ recordingHandler }

func (*failingNotifyHandler) HandleOffsetNotify(*pb.OffsetNotify) ([]*pb.NackNotify, error) {
	return nil, errors.New("required dependency unavailable")
}

func TestFailedIngestionDoesNotACK(t *testing.T) {
	r := NewReplicator(1, "local", NewPeerHealthTable(ClockConfig{}), notificationTransportFunc(func(NodeId, []byte) (int, error) {
		t.Fatal("failed ingestion emitted an ACK/NACK")

		return 0, nil
	}), &failingNotifyHandler{}, time.Second)
	r.HandleNotify(2, 1, &pb.OffsetNotify{Origin: &pb.EffectRef{NodeId: 2, Offset: 1}})
}

func TestMalformedNACKDoesNotResolve(t *testing.T) {
	r := NewReplicator(1, "local", NewPeerHealthTable(ClockConfig{}), nil, nil, time.Second)

	r.transport = notificationTransportFunc(func(peer NodeId, packet []byte) (int, error) {
		request, _, err := parseNotifyPacket(packet)
		if err != nil {
			return 0, err
		}

		broken := MarshalNotifyACKPacket(peer, request, 1, 0)
		r.HandleNotifyACKWithData(peer, request, 1, 0, broken)

		return len(packet), nil
	})
	if _, err := r.ReplicateTo(&pb.OffsetNotify{}, nil, 2); err == nil {
		t.Fatal("empty NACK completed replication successfully")
	}
}

func TestTransactionBatchFivePeers(t *testing.T) {
	t.Setenv("BPLUS_SCALE_VERSION", "dev")
	bplusBenchFixture(t, 5, 128, "SingleTx", nil)
}

func TestTransactionFramesRequireCompleteTransfer(t *testing.T) {
	record := &pb.FetchResponse{Ref: &pb.EffectRef{NodeId: 1, Offset: 2}, EffectData: bytes.Repeat([]byte("effect"), 500)}

	data, err := proto.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}

	var (
		encoded bytes.Buffer
		count   [8]byte
	)
	binary.LittleEndian.PutUint64(count[:], 2)
	encoded.Write(count[:])

	for range 2 {
		if err := writeTransactionFrame(&encoded, data); err != nil {
			t.Fatal(err)
		}
	}

	for _, tc := range []struct {
		name string
		data []byte
		good bool
	}{
		{"complete", encoded.Bytes(), true},
		{"partial count", encoded.Bytes()[:4], false},
		{"partial record", encoded.Bytes()[:encoded.Len()-1], false},
		{"trailing bytes", append(bytes.Clone(encoded.Bytes()), 1), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seen := 0

			err := receiveTransactionRecords(bytes.NewReader(tc.data), func(got *pb.FetchResponse) error {
				seen++

				if !proto.Equal(got, record) {
					t.Fatal("record changed")
				}

				return nil
			})
			if (err == nil) != tc.good {
				t.Fatalf("error=%v", err)
			}

			if tc.good && seen != 2 {
				t.Fatal("missing record")
			}
		})
	}

	if err := receiveTransactionRecords(bytes.NewReader(encoded.Bytes()), func(*pb.FetchResponse) error { return io.ErrClosedPipe }); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal(err)
	}
}
