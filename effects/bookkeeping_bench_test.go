package effects

import (
	"fmt"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
)

func BenchmarkHorizonLookup(b *testing.B) {
	for _, size := range []int{1, 64, 1024} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			e := newHorizonTestEngine()

			ids := make([]string, size)
			for i := range ids {
				ids[i] = fmt.Sprintf("txn-%08d", i)
				bind := testBind([]*pb.EffectRef{{NodeId: 1, Offset: uint64(i + 1)}}, &pb.EffectRef{NodeId: 2, Offset: uint64(i + 1)})
				e.horizon.Add(ids[i], Tip{3, uint64(i + 1)}, bind)
			}

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(p *testing.PB) {
				i := 0
				for p.Next() {
					if !e.horizon.IsInvisible(ids[i%size]) {
						b.Error("pending transaction became visible")
					}

					i++
				}
			})
		})
	}
}

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
