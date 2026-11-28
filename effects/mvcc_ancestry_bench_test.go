package effects

import (
	"fmt"
	"testing"
)

// Hold the requested value and its history depth constant while increasing the
// number of unrelated keys written by the same completed transaction. Modern
// verdicts carry no compatibility evidence and have no outstanding subscribers.
// The decisions metric counts unique effects classified by MVCC, independently
// of allocation costs or the time spent reconstructing the requested value.
func BenchmarkMVCCCompletedBindRead(b *testing.B) {
	for _, width := range []int{1, 16, 64, 256, 1024} {
		b.Run(fmt.Sprintf("Keys%d", width), func(b *testing.B) {
			e := NewEngine(EngineConfig{DefaultMode: SafeMode, BuildVersion: "dev"})

			b.Cleanup(func() {
				if err := e.Close(); err != nil {
					b.Error(err)
				}
			})

			writer := e.NewContext()
			writer.BeginTx()

			for i := range width {
				if err := writer.Emit(scalarSet(fmt.Sprintf("key-%04d", i), "value")); err != nil {
					b.Fatal(err)
				}
			}

			if err := writer.Flush(); err != nil {
				b.Fatal(err)
			}

			frontier := e.index.Contains("key-0000").Tips()
			if len(frontier) != 1 {
				b.Fatalf("expected one completed verdict, got %v", frontier)
			}

			eff, err := e.getEffect(logicalAddress("key-0000"), frontier[0])
			if err != nil {
				b.Fatal(err)
			}

			snap := eff.GetSnapshot()
			if snap == nil || snap.GetState() != nil || snap.GetEvidence() != nil ||
				snap.GetOutstandingSubscribers() == nil || len(snap.GetOutstandingSubscribers().GetNodeIds()) != 0 {
				b.Fatal("fixture must end in a closed, evidence-free verdict-only snapshot")
			}

			for _, mode := range []string{"Live", "MVCC"} {
				b.Run(mode, func(b *testing.B) {
					var decisions, frozenKeys int

					b.ReportAllocs()

					for b.Loop() {
						c := e.NewContext()
						if mode == "MVCC" {
							c.BeginTx()
						}

						got, _, err := c.GetSnapshot("key-0000")
						if c.txSnapshot != nil {
							decisions = len(c.txSnapshot.decided)
							frozenKeys = len(c.txSnapshot.views)
						}

						c.Abort()

						if err != nil || string(got.GetScalar().GetRaw()) != "value" {
							b.Fatalf("read: %v, %v", got, err)
						}
					}

					b.ReportMetric(float64(decisions), "decisions/op")
					b.ReportMetric(float64(frozenKeys), "frozen-keys/op")
				})
			}
		})
	}
}
