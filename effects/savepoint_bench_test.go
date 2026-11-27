package effects

import (
	"fmt"
	"runtime"
	"testing"
)

// Separate key-count growth from queued-history growth on one key. Fixture
// emission is outside the timed loop, and every savepoint captures the same
// pending state. These are actual emitted effects, not fabricated slice sizes.
func BenchmarkContextSavepoint(b *testing.B) {
	for _, shape := range []struct{ keys, writes int }{
		{1, 1}, {64, 1}, {1024, 1}, {1, 64}, {1, 4096},
	} {
		b.Run(fmt.Sprintf("Keys%d/WritesPerKey%d", shape.keys, shape.writes), func(b *testing.B) {
			e := NewEngine(EngineConfig{DefaultMode: SafeMode, BuildVersion: "dev"})
			c := e.NewContext()

			b.Cleanup(func() {
				c.Abort()

				if err := e.Close(); err != nil {
					b.Error(err)
				}
			})

			c.BeginTx()

			for i := range shape.keys {
				key := fmt.Sprintf("key-%04d", i)
				for range shape.writes {
					if err := c.Emit(scalarSet(key, "value")); err != nil {
						b.Fatal(err)
					}
				}
			}

			var notices int
			for _, key := range c.keys {
				notices += len(key.notifies)
			}

			b.Run("Take", func(b *testing.B) {
				var sp *ContextSavepoint

				b.ReportAllocs()

				for b.Loop() {
					sp = c.TakeSavepoint()
				}

				runtime.KeepAlive(sp)
				b.ReportMetric(float64(notices), "queued-notices")
			})
			b.Run("Restore", func(b *testing.B) {
				sp := c.TakeSavepoint()

				b.ReportAllocs()

				for b.Loop() {
					c.RestoreSavepoint(sp)
				}

				b.ReportMetric(float64(notices), "queued-notices")
			})
		})
	}
}
