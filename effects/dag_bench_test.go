package effects

import (
	"fmt"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

func BenchmarkDAGPrepare(b *testing.B) {
	for _, width := range []int{1, 4} {
		for _, depth := range []int{16, 256} {
			b.Run(fmt.Sprintf("width=%d/depth=%d", width, depth), func(b *testing.B) {
				e := newEngine(EngineConfig{})

				b.Cleanup(func() {
					if err := e.Close(); err != nil {
						b.Error(err)
					}
				})

				var tips []Tip

				for level := range depth {
					next := make([]Tip, 0, width)

					for branch := range width {
						tip := Tip{uint64(branch + 1), uint64(level + 1)}
						eff := &pb.Effect{Key: []byte("key"), Deps: toPbRefs(tips), ForkChoiceHash: []byte{byte(branch)}}

						if err := e.effectCache.Put(tip, eff); err != nil {
							b.Fatal(err)
						}

						next = append(next, tip)
					}

					tips = next
				}

				b.ReportAllocs()
				b.ResetTimer()

				for b.Loop() {
					d := newDag(e, "key", "")
					if err := d.prepare(tips); err != nil {
						b.Fatal(err)
					}

					if len(d.topoOrder) != width*depth {
						b.Fatal("incomplete traversal")
					}
				}
			})
		}
	}
}
