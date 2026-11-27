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

// Historical membership can include an ambiguously timed remote bind while
// excluding its local competitor. Value selection must not discard the live
// evidence that determines which transaction survives.
func TestHistoricalReadRetainsLiveCompetition(t *testing.T) {
	for _, chain := range []bool{false, true} {
		for _, cached := range []bool{false, true} {
			t.Run(fmt.Sprintf("chain=%t/cached=%t", chain, cached), func(t *testing.T) {
				log := newSnapshotLog()
				e := newSnapshotEngine(log)
				preSubscribe(e, "winner")
				preSubscribe(e, "other")

				root := scalarSet("winner", "base")
				root.NodeId, root.Hlc = 1, sTs(0)
				base := log.putEffect(root)
				log.nodeID = 2
				a := closureBind(log, "A", 3, []string{"winner"}, map[string]Tip{"winner": base})

				var b, excluded Tip

				frontiers := make(map[string][]Tip)
				verdicts := map[string]pb.Verdict{"A": pb.Verdict_LOST, "B": pb.Verdict_WON}
				loser, wantValue, verdictKey := "A", "base", "winner"
				boundary := int64(450)

				if chain {
					// A conflicts with B, B conflicts with C, A and C are
					// disjoint. C's higher priority eliminates B, allowing A.
					log.nodeID = 3
					b = closureBind(log, "B", 2, []string{"winner", "other"}, map[string]Tip{"winner": base})
					log.nodeID = 1
					excluded = closureBind(log, "C", 1, []string{"other"}, nil)
					frontiers["other"] = []Tip{b, excluded}
					verdicts = map[string]pb.Verdict{"B": pb.Verdict_LOST, "C": pb.Verdict_WON}
					loser, wantValue, verdictKey, boundary = "B", "A", "other", 750
				} else {
					log.nodeID = 1
					b = closureBind(log, "B", 1, []string{"winner"}, map[string]Tip{"winner": base})
					excluded = b
				}

				frontiers["winner"] = []Tip{a, b}
				if cached {
					v := log.putEffect(&pb.Effect{
						Key: []byte(verdictKey), NodeId: 1, Hlc: sTs(int64(log.nextOff)),
						Deps: toPbRefs(frontiers[verdictKey]),
						Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{TxnVerdicts: verdicts}},
					})
					frontiers[verdictKey] = []Tip{v}
				}

				for key, tips := range frontiers {
					e.index.Insert(key, nil, keytrie.NewTipSet(tips...))
				}

				s := &transactionSnapshot{
					engine: e, at: stn.Sample{Domain: 1, Tick: boundary},
					expiryAt: time.Now(), clocks: e.clockEvidence(),
				}
				if err := s.prepare(context.Background(), []string{logicalAddress("winner")}); err != nil {
					t.Fatal(err)
				}

				if !s.decided[a] || s.decided[excluded] {
					t.Fatalf("fixture must select A and exclude its local competitor: A=%v excluded=%v", s.decided[a], s.decided[excluded])
				}

				v := s.views[logicalAddress("winner")]
				if _, lost := v.lost[loser]; !lost || len(v.lost) != 1 {
					t.Errorf("lost=%v, want only %s", v.lost, loser)
				}

				for key := range v.anchors {
					if slices.Contains(v.frontiers[key].resolved, excluded) || slices.Contains(v.anchors[key], excluded) {
						t.Errorf("live competitor leaked into historical value history on %q", key)
					}
				}

				tips := slices.Concat(v.frontiers[logicalAddress("winner")].resolved, v.anchors[logicalAddress("winner")])

				result, _, err := e.reconstructWithView(logicalAddress("winner"), tips, "reader", true, v)
				if err != nil || string(result.GetScalar().GetRaw()) != wantValue {
					t.Fatalf("value=%q error=%v, want %q", result.GetScalar().GetRaw(), err, wantValue)
				}
			})
		}
	}
}

// Equivalent live proof must not hide the source already present in the frozen
// history: membership needs that source after value ancestry has been reclaimed.
func TestHistoricalMembershipPreservesEquivalentFrozenProof(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)

	t.Cleanup(func() { _ = e.Close() })

	bind := closureBind(log, "old", 1, []string{"a", "b"}, nil)

	proof, err := e.bindProof(bind, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	frozenEffect := bindEvidenceSnapshot("a", proof)
	frozenEffect.GetSnapshot().State = &pb.ReducedEffect{}
	frozenEffect.Deps = toPbRefs([]Tip{{99, 99}}) // Reclaimed value ancestry.
	frozen := log.putEffect(frozenEffect)
	liveEffect := bindEvidenceSnapshot("a", proof)
	liveEffect.GetSnapshot().State = &pb.ReducedEffect{}
	liveEffect.Deps = toPbRefs([]Tip{frozen})
	newer := log.putEffect(liveEffect)

	e.effectCache.releaseEvidence(evidenceTips(proof))

	e.updateIndex(logicalAddress("a"), nil, newer)

	live := newSnapshotReadView(e, context.Background())
	if err := live.discover([]string{logicalAddress("a")}, live.capture); err != nil {
		t.Fatal(err)
	}

	if entry := live.verdicts["old"]; !entry.proven || entry.snapshotTip != newer {
		t.Fatalf("live fixture must use newer proof: %+v", entry)
	}

	view, err := e.historicalReadView(context.Background(), []string{logicalAddress("a")}, func(key string) ([]Tip, error) {
		if key != logicalAddress("a") {
			t.Fatalf("unexpected value history requested: %s", key)
		}

		return []Tip{frozen}, nil
	}, live)
	if err != nil {
		t.Fatal(err)
	}

	if entry := view.verdicts["old"]; !entry.proven || entry.snapshotTip != frozen {
		t.Errorf("equivalent live proof displaced frozen source: %+v, want %v", entry, frozen)
	}

	if entry := live.verdicts["old"]; entry.snapshotTip != newer {
		t.Error("historical merge mutated the live view")
	}

	// Exercise ordinary reconstruction at the selected historical frontier.
	// The state snapshot and its retained proof must not reopen reclaimed data.
	tips := slices.Concat(view.frontiers[logicalAddress("a")].resolved, view.anchors[logicalAddress("a")])
	if _, _, err := e.reconstructWithView(logicalAddress("a"), tips, "reader", true, view); err != nil {
		t.Fatalf("historical reconstruction reopened reclaimed history: %v", err)
	}
}
