package effects

import (
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
)

func BenchmarkHorizonLifecycle(b *testing.B) {
	e := newHorizonTestEngine()
	bind := testBind([]*pb.EffectRef{{NodeId: 1, Offset: 10}}, &pb.EffectRef{NodeId: 1, Offset: 20})

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		e.horizon.Add("txn", Tip{1, 30}, bind)
		e.horizon.MakeVisible("txn")
	}
}

func BenchmarkVertexOwnership(b *testing.B) {
	p := newVertexPool()
	eff := &pb.Effect{Key: []byte("key")}

	tip := Tip{1, 1}
	if err := p.Put(tip, eff); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		if err := p.Put(tip, eff); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSubscriberChurn(b *testing.B) {
	e := newHorizonTestEngine()
	e.index.Insert("key", nil, keytrie.NewTipSet())
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		e.addPeerSubscriber("key", 2)
		e.removePeerSubscriber("key", 2)
	}
}
