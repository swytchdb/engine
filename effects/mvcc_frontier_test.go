package effects

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
	"github.com/swytchdb/engine/stn"
)

func TestMVCCCompletedBindSelectsOnlyAccessedFrontiers(t *testing.T) {
	for _, width := range []int{2, 128} {
		t.Run(fmt.Sprint(width), func(t *testing.T) {
			e := NewEngine(EngineConfig{DefaultMode: SafeMode, BuildVersion: "dev"})

			t.Cleanup(func() { _ = e.Close() })

			writer := e.NewContext()
			writer.BeginTx()

			for i := range width {
				if err := writer.Emit(scalarSet(fmt.Sprintf("key-%04d", i), fmt.Sprint(i))); err != nil {
					t.Fatal(err)
				}
			}

			if err := writer.Flush(); err != nil {
				t.Fatal(err)
			}

			reader := e.NewContext()

			reader.BeginTx()
			defer reader.Abort()

			for n, i := range []int{width - 1, 0} {
				got, _, err := reader.GetSnapshot(fmt.Sprintf("key-%04d", i))
				if err != nil || string(got.GetScalar().GetRaw()) != fmt.Sprint(i) {
					t.Fatalf("read %d: value=%v err=%v", i, got, err)
				}

				if got := len(reader.txSnapshot.decided); got != n+1 {
					t.Fatalf("selected %d effects for %d accessed frontiers in a %d-key bind", got, n+1, width)
				}
			}
		})
	}
}

// Both reads must use the normal reconstruction snapshot boundary, even after
// another key has been frozen and even without bind evidence for old data.
func TestMVCCReconstructsCompactedHistoricalFrontiers(t *testing.T) {
	for _, future := range []bool{false, true} {
		t.Run(fmt.Sprintf("newer-snapshot=%t", future), func(t *testing.T) {
			log := newSnapshotLog()
			e := newSnapshotEngine(log)

			t.Cleanup(func() { _ = e.Close() })

			s := &transactionSnapshot{engine: e, at: stn.Sample{Domain: 1, Tick: 500}, expiryAt: time.Now(), clocks: e.clockEvidence()}
			for _, key := range []string{"a", "b"} {
				preSubscribe(e, key)

				makeSnapshot := func(stamp int64, value string, deps ...Tip) Tip {
					return log.putEffect(&pb.Effect{
						Key: []byte(key), NodeId: 1, Hlc: sTs(stamp), Deps: toPbRefs(deps),
						Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
							Collection: pb.CollectionKind_SCALAR,
							State:      &pb.ReducedEffect{Collection: pb.CollectionKind_SCALAR, Op: pb.EffectOp_INSERT_OP, Scalar: scalarInsertRaw([]byte(value))},
						}},
					})
				}
				old := makeSnapshot(100, "old-"+key, Tip{99, 99})

				current := old
				if future {
					current = makeSnapshot(600, "new-"+key, old)
				}

				e.index.Insert(key, nil, keytrie.NewTipSet(current))

				internal := logicalAddress(key)

				tips, err := s.tips(context.Background(), internal)
				if err != nil || !slices.Equal(tips.Tips(), []Tip{old}) {
					t.Fatalf("historical tips for %s: %v, %v", key, tips, err)
				}

				got, _, err := e.reconstructWithView(internal, tips.Tips(), "reader", true, s.views[internal])
				if err != nil || string(got.GetScalar().GetRaw()) != "old-"+key {
					t.Fatalf("reconstructed %s: %v, %v", key, got, err)
				}
			}
		})
	}
}

func TestMVCCHistoricalFrontierIgnoresCrossKeyObservations(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)

	t.Cleanup(func() { _ = e.Close() })

	put := func(key, value string, stamp int64, deps ...Tip) Tip {
		eff := scalarSet(key, value)
		eff.NodeId, eff.Hlc, eff.Deps = 1, sTs(stamp), toPbRefs(deps)

		return log.putEffect(eff)
	}
	old := put("a", "old-a", 100)
	other := put("b", "not-a", 200)
	current := put("a", "new-a", 600, old, other)
	s := &transactionSnapshot{engine: e, at: stn.Sample{Domain: 1, Tick: 500}, clocks: e.clockEvidence(), decided: make(map[Tip]bool)}

	tips, err := e.historicalFrontier(context.Background(), logicalAddress("a"), []Tip{current}, func(tip Tip, eff *pb.Effect) (bool, error) {
		return s.member(context.Background(), tip, eff)
	})
	if err != nil || !slices.Equal(tips, []Tip{old}) {
		t.Fatalf("historical tips contain another key: %v, %v", tips, err)
	}

	got, _, err := e.reconstructWithView(logicalAddress("a"), tips, "reader", false, nil)
	if err != nil || string(got.GetScalar().GetRaw()) != "old-a" {
		t.Fatalf("reconstructed: %v, %v", got, err)
	}
}

func TestMVCCReconstructionStillRequiresValueHistory(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)

	t.Cleanup(func() { _ = e.Close() })

	eff := scalarSet("a", "value")
	eff.NodeId, eff.Hlc, eff.Deps = 1, sTs(100), toPbRefs([]Tip{{99, 99}})
	tip := log.putEffect(eff)
	s := &transactionSnapshot{engine: e, at: stn.Sample{Domain: 1, Tick: 500}, clocks: e.clockEvidence(), decided: make(map[Tip]bool)}

	tips, err := e.historicalFrontier(context.Background(), logicalAddress("a"), []Tip{tip}, func(tip Tip, eff *pb.Effect) (bool, error) {
		return s.member(context.Background(), tip, eff)
	})
	if err != nil || !slices.Equal(tips, []Tip{tip}) {
		t.Fatalf("frontier selection demanded value history: %v, %v", tips, err)
	}

	if _, _, err := e.reconstructWithView(logicalAddress("a"), tips, "reader", false, nil); err == nil {
		t.Fatalf("reconstruction hid required missing history: %v", err)
	}
}

func TestMVCCHistoricalBindRetreatsToConsumedTips(t *testing.T) {
	for _, first := range []string{"a", "b"} {
		t.Run(first, func(t *testing.T) {
			e := NewEngine(EngineConfig{DefaultMode: SafeMode, BuildVersion: "dev"})

			t.Cleanup(func() { _ = e.Close() })

			historicalTestWrite(t, e, map[string]string{"a": "old-a", "b": "old-b"}, true)

			oldTips := map[string][]Tip{"a": e.index.Contains("a").Tips(), "b": e.index.Contains("b").Tips()}
			reader := e.NewContext()

			reader.BeginTx()
			defer reader.Abort()

			historicalTestWrite(t, e, map[string]string{"a": "new-a", "b": "new-b"}, true)

			keys := []string{"a", "b"}
			if first == "b" {
				slices.Reverse(keys)
			}

			for range 2 {
				for _, key := range keys {
					got, tips, err := reader.GetSnapshot(key)
					if err != nil || string(got.GetScalar().GetRaw()) != "old-"+key || !slices.Equal(tips, oldTips[key]) {
						t.Fatalf("historical %s: value=%v tips=%v err=%v", key, got, tips, err)
					}
				}
			}
		})
	}
}

func TestMVCCTemporalChoiceSurvivesEvidenceTightening(t *testing.T) {
	for _, included := range []bool{false, true} {
		t.Run(fmt.Sprint(included), func(t *testing.T) {
			e := &Engine{}
			s := &transactionSnapshot{at: stn.Sample{Domain: 1, Tick: 500}, clocks: e.clockEvidence(), decided: make(map[Tip]bool)}
			eff := &pb.Effect{NodeId: 2, Hlc: sTs(100)}
			tip := Tip{2, 1}

			if !included {
				eff.NodeId, eff.Hlc = 1, sTs(600)
			}

			if got, err := s.member(context.Background(), tip, eff); err != nil || got != included {
				t.Fatalf("initial selection: included=%t err=%v", got, err)
			}

			if included {
				var err error

				s.clocks, err = s.clocks.Before(stn.Sample{Domain: 1, Tick: 600}, effectSample(eff))
				if err != nil {
					t.Fatal(err)
				}
			}

			if got, err := s.member(context.Background(), tip, eff); err != nil || got != included {
				t.Fatalf("reversed selection: included=%t err=%v", got, err)
			}
		})
	}
}
