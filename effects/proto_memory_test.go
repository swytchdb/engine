package effects

import (
	"bytes"
	"runtime"
	"sync"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"google.golang.org/protobuf/proto"
)

func TestMemoryProtobufSharedSnapshotBuffer(t *testing.T) {
	p := newVertexPool()
	baseline := p.Bytes()
	value := make([]byte, 1<<20)
	data := &pb.DataEffect{Value: &pb.DataEffect_Raw{Raw: value}}
	write := &pb.Effect{Kind: &pb.Effect_Data{Data: data}}
	snapshot := &pb.Effect{Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
		State: &pb.ReducedEffect{Scalar: cloneData(data)},
	}}}

	if err := p.PutSizedCache(Tip{1, 1}, write, 1); err != nil {
		t.Fatal(err)
	}

	first := p.Bytes()
	if first-baseline < int64(cap(value)) {
		t.Fatal("wire length hid resident buffer capacity")
	}

	if err := p.PutSizedCache(Tip{1, 2}, snapshot, 1); err != nil {
		t.Fatal(err)
	}

	shared := p.Bytes()
	if shared-first >= int64(cap(value)) {
		t.Fatal("snapshot charged the shared value twice")
	}

	if !p.Incref(Tip{1, 2}) {
		t.Fatal("missing snapshot")
	}

	p.drainReclaim()

	if p.Bytes()-baseline < int64(cap(value)) {
		t.Fatal("first release credited a still-owned buffer")
	}

	// Equal content with independent allocations must still be charged twice.
	independent := proto.Clone(snapshot).(*pb.Effect)

	beforeCopy := p.Bytes()
	if err := p.PutSizedCache(Tip{1, 3}, independent, 1); err != nil {
		t.Fatal(err)
	}

	if p.Bytes()-beforeCopy < int64(cap(value)) {
		t.Fatal("distinct equal buffers were deduplicated")
	}

	p.Decref(Tip{1, 2})
	p.drainReclaim()

	if p.Bytes() != baseline {
		t.Fatalf("last release left a charge: %d", p.Bytes()-baseline)
	}

	if !bytes.Equal(snapshot.GetSnapshot().GetState().GetScalar().GetRaw(), value) {
		t.Fatal("reclamation invalidated the reader's snapshot")
	}

	runtime.KeepAlive(snapshot)
}

func TestMemoryProtobufCapacityAndUnknownFields(t *testing.T) {
	p := newVertexPool()
	baseline := p.Bytes()
	buffer := make([]byte, 1, 1<<20)
	eff := &pb.Effect{Kind: &pb.Effect_Data{Data: &pb.DataEffect{Value: &pb.DataEffect_Raw{Raw: buffer}}}}
	eff.ProtoReflect().SetUnknown([]byte{0xa0, 6, 1})

	if err := p.PutSizedCache(Tip{1, 1}, eff, 1); err != nil {
		t.Fatal(err)
	}

	if p.Bytes()-baseline < int64(cap(buffer)) {
		t.Fatal("only charged slice length")
	}

	p.drainReclaim()

	if p.Bytes() != baseline {
		t.Fatal("protobuf allocation ledger did not empty")
	}
}

func TestMemoryConcurrentDuplicatePut(t *testing.T) {
	p := newVertexPool()
	baseline := p.Bytes()
	eff := scalarSet("key", "value")

	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for range 100 {
				if err := p.Put(Tip{1, 1}, eff); err != nil {
					t.Error(err)
				}
			}
		})
	}

	wg.Wait()
	p.decrefChainNode("key", Tip{1, 1})
	p.drainReclaim()

	if p.Bytes() != baseline {
		t.Fatalf("duplicate publication leaked accounting: %d", p.Bytes()-baseline)
	}
}
