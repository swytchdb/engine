package effects

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
)

// Use real Context-emitted history and the normal fetch/prepare path. Fetching
// its newest effect naturally discovers the child before its dependencies;
// no effects, timestamps, DAG edges, or index frontiers are fabricated.
func TestTemporalFetchedHistoryObservedOnce(t *testing.T) {
	source := NewEngine(EngineConfig{DefaultMode: UnsafeMode})
	reader := NewEngine(EngineConfig{DefaultMode: UnsafeMode})

	t.Cleanup(func() {
		if err := reader.Close(); err != nil {
			t.Error(err)
		}

		if err := source.Close(); err != nil {
			t.Error(err)
		}
	})

	const key = "temporal-fetched-history"

	var newest Tip

	source.SetOnLocalEffect(func(tip Tip, eff *pb.Effect) {
		if eff.GetData() != nil {
			newest = tip
		}
	})
	historicalTestWrite(t, source, map[string]string{key: "old"}, false)
	historicalTestWrite(t, source, map[string]string{key: "new"}, false)

	if err := reader.ensureSubscribed(key); err != nil {
		t.Fatal(err)
	}

	reader.broadcaster = &bootstrapBroadcaster{remoteEngine: source}

	var clockReads atomic.Uint64

	origin := time.Now()

	reader.clock.SetClock(func() time.Time {
		clockReads.Add(1)

		return origin.Add(time.Since(origin))
	})

	eff, err := reader.getEffect(key, newest)
	if err != nil {
		t.Fatal(err)
	}

	if len(allDeps(eff)) == 0 {
		t.Fatal("emitted history has no dependencies")
	}

	v, ok := reader.effectCache.m.Load(newest)
	if !ok {
		t.Fatal("fetched effect missing from cache")
	}

	if got := v.temporal.Load(); got != temporalReceiptObserved {
		t.Fatalf("expected receipt with dependencies still missing, got %b", got)
	}

	if got := clockReads.Load(); got != 1 {
		t.Fatalf("fetch recorded %d receipts, want one", got)
	}

	initialEvidence := reader.clockEvidence()
	initialHits := reader.effectCache.hits.Load()

	const repeats = 100
	for range repeats {
		if got, err := reader.getEffect(key, newest); err != nil || got != eff {
			t.Fatalf("cached read: effect=%p err=%v", got, err)
		}
	}

	if got := reader.effectCache.hits.Load() - initialHits; got != repeats {
		t.Fatalf("cache hits walked dependencies: %d lookups for %d reads", got, repeats)
	}

	if clockReads.Load() != 1 || reader.clockEvidence() != initialEvidence {
		t.Fatal("cached reads sampled receipts or published evidence")
	}

	d := newDag(reader, key, "")
	if err := d.prepare([]Tip{newest}); err != nil {
		t.Fatal(err)
	}
	// Preparation fetches the absent parents. The explicit dependency pass
	// must finish the child's evidence without sampling another receipt.
	beforeCompletion := clockReads.Load()

	reader.observeTemporalDag(d)

	if got := v.temporal.Load(); got != temporalDepsObserved|temporalReceiptObserved {
		t.Fatalf("prepared history did not complete observation: %b", got)
	}

	if clockReads.Load() != beforeCompletion {
		t.Fatal("dependency completion resampled receipts")
	}

	data, _, err := reader.reconstruct(key, []Tip{newest}, "", false)
	if err != nil || string(data.GetScalar().GetRaw()) != "new" {
		t.Fatalf("fetched reconstruction: %v, %v", data, err)
	}

	wire, err := reader.broadcaster.FetchFromAny(toPbRef(newest), reader.fetchHint(key))
	if err != nil {
		t.Fatal(err)
	}

	stableEvidence := reader.clockEvidence()
	stableReceipts := clockReads.Load()
	stableHits := reader.effectCache.hits.Load()

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range repeats {
				// Completed DAG observations and duplicate deliveries must
				// also skip dependency lookup and solver work.
				reader.observeTemporal(newest, eff, false)

				if err := reader.storeWireData(newest, wire); err != nil {
					t.Error(err)

					return
				}
			}
		})
	}

	wg.Wait()

	if reader.effectCache.hits.Load() != stableHits || clockReads.Load() != stableReceipts || reader.clockEvidence() != stableEvidence {
		t.Fatal("completed observation repeated dependency/receipt work")
	}
}
