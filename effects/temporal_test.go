package effects

import (
	"sync"
	"testing"

	"github.com/swytchdb/engine/stn"
)

// These are publication/concurrency tests of mathematical constraints, not
// fabricated engine histories or tests of transaction arbitration.
func TestTemporalMergeStaleVersion(t *testing.T) {
	e := &Engine{}
	frozen := e.clockEvidence()
	a := stn.Sample{Domain: 1}
	b := stn.Sample{Domain: 2}
	c := stn.Sample{Domain: 3}

	e.temporal.merge(frozen, []temporalObservation{{a, b}})
	first := e.clockEvidence()
	// Deliberately start from the version held before the other publication.
	// A plain Store would erase a -> b here; CAS must replay b -> c.
	e.temporal.merge(frozen, []temporalObservation{{b, c}})

	latest := e.clockEvidence()
	if lo, _, err := latest.Difference(a, c); err != nil || lo != 0 {
		t.Fatalf("lost transitive evidence: lower=%d err=%v", lo, err)
	}

	for _, old := range []*stn.Clocks{frozen, first} {
		if lo, hi, err := old.Difference(a, c); err != nil || lo != -stn.Infinity || hi != stn.Infinity {
			t.Fatalf("mutated frozen evidence: [%d,%d] err=%v", lo, hi, err)
		}
	}
	// Redundant input against an obsolete base must not publish that base.
	e.temporal.merge(first, []temporalObservation{{a, b}})

	if e.clockEvidence() != latest {
		t.Fatal("redundant observation replaced stronger evidence")
	}
}

func TestTemporalMergeConcurrentTightening(t *testing.T) {
	e := &Engine{}
	frozen := e.clockEvidence()

	const writers, updates = 8, 16

	boundary := stn.Sample{Domain: 1}
	start := make(chan struct{})

	var wg sync.WaitGroup
	for writer := range writers {
		wg.Go(func() {
			<-start

			for i := 1; i <= updates; i++ {
				sample := stn.Sample{Domain: uint64(writer + 2), Tick: int64(i) * 1_000_000}
				// Reuse a stale starting version to exercise replay even when
				// the scheduler happens to run this goroutine alone.
				e.temporal.merge(frozen, []temporalObservation{{sample, boundary}})

				if _, _, err := e.clockEvidence().Difference(sample, boundary); err != nil {
					t.Error(err)
				}

				if lo, hi, err := frozen.Difference(sample, boundary); err != nil || lo != -stn.Infinity || hi != stn.Infinity {
					t.Errorf("frozen graph changed: [%d,%d] err=%v", lo, hi, err)
				}
			}
		})
	}

	close(start)
	wg.Wait()

	for writer := range writers {
		sample := stn.Sample{Domain: uint64(writer + 2), Tick: updates * 1_000_000}
		// Drift widening may weaken the direct bound slightly when a domain
		// was first anchored by an earlier update. A 100us margin dwarfs it,
		// but is too small to hide even one lost tightening step.
		sample.Tick -= 100_000
		if lo, _, err := e.clockEvidence().Difference(sample, boundary); err != nil || lo <= 0 {
			t.Errorf("writer %d lost final tightening: lower=%d err=%v", writer, lo, err)
		}
	}
}
