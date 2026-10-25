package effects

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

// Generate real effects with Context, including a cross-node witness, before
// benchmarking the affected cached-history and interactive snapshot paths.
func BenchmarkTemporalEngine(b *testing.B) {
	for _, workload := range []string{"CachedLocal", "CachedRemote", "ContextSnapshot", "ContextWrite"} {
		b.Run(workload, func(b *testing.B) {
			e := NewEngine(EngineConfig{DefaultMode: UnsafeMode})
			source := NewEngine(EngineConfig{DefaultMode: UnsafeMode})

			b.Cleanup(func() {
				if err := source.Close(); err != nil {
					b.Error(err)
				}

				if err := e.Close(); err != nil {
					b.Error(err)
				}
			})

			if err := e.ensureSubscribed("witness"); err != nil {
				b.Fatal(err)
			}

			var remote Tip

			source.SetOnLocalEffect(func(tip Tip, effect *pb.Effect) {
				wire, err := MarshalEffect(effect)
				if err != nil {
					b.Fatal(err)
				}

				if _, err := e.HandleRemote(BuildOffsetNotify(source.NodeID(), tip, effect, wire, context.Background())); err != nil {
					b.Fatal(err)
				}

				if effect.GetData() != nil {
					remote = tip
				}
			})

			write := func(engine *Engine, key string) (Tip, error) {
				c := engine.NewContext()
				if err := c.Emit(&pb.Effect{Key: []byte(key), Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("value"))}}); err != nil {
					return Tip{}, err
				}

				tip := c.keys[key].lastOffset

				return tip, c.Flush()
			}
			if _, err := write(source, "witness"); err != nil {
				b.Fatal(err)
			}

			local, err := write(e, "witness")
			if err != nil {
				b.Fatal(err)
			}

			localEffect, err := e.getEffect("witness", local)
			if err != nil || len(allDeps(localEffect)) == 0 {
				b.Fatalf("missing real witness dependency: %v", err)
			}

			if _, err := e.getEffect("witness", remote); err != nil {
				b.Fatal(err)
			}

			var workers atomic.Uint64

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(p *testing.PB) {
				key := fmt.Sprintf("worker-%d", workers.Add(1))

				for p.Next() {
					var err error

					switch workload {
					case "CachedLocal":
						_, err = e.getEffect("witness", local)
					case "CachedRemote":
						_, err = e.getEffect("witness", remote)
					case "ContextSnapshot":
						c := e.NewContext()
						c.BeginTx()

						var got *pb.ReducedEffect

						got, _, err = c.GetSnapshot("witness")
						if err == nil && string(got.GetScalar().GetRaw()) != "value" {
							b.Error("incorrect snapshot value")
						}

						c.Abort()
					case "ContextWrite":
						_, err = write(e, key)
					}

					if err != nil {
						b.Error(err)

						return
					}
				}
			})
		})
	}
}
