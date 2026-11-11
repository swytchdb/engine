package effects

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
	"github.com/swytchdb/engine/stn"
)

// A retained proof can account for a bind without retaining the old value
// ancestry. Cached labels and proofs outside the frozen cut cannot do so.
func TestHistoricalMembershipConsultsFrozenEvidence(t *testing.T) {
	for _, evidence := range []string{"frozen", "cached-label", "live-only"} {
		t.Run(evidence, func(t *testing.T) {
			log := newSnapshotLog()
			e := newSnapshotEngine(log)

			t.Cleanup(func() { _ = e.Close() })

			bind := closureBind(log, "old", 1, []string{"a", "b"}, nil)

			proof, err := e.bindProof(bind, nil, nil)
			if err != nil {
				t.Fatal(err)
			}

			owner := log.putEffect(bindEvidenceSnapshot("a", proof))
			e.effectCache.releaseEvidence(evidenceTips(proof))
			ownerEffect, _ := e.effectCache.Get(owner)

			verdicts, err := e.proofVerdicts(owner, ownerEffect)
			if err != nil || !verdicts["old"].proven {
				t.Fatalf("expected evaluated proof: %v, %v", verdicts, err)
			}

			// Value reconstruction stops at this state snapshot. The missing
			// connector models the already reclaimed path to the old bind.
			missing := Tip{99, 99}
			state := log.putEffect(&pb.Effect{
				Key: []byte("a"), Deps: toPbRefs([]Tip{missing}),
				Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{State: &pb.ReducedEffect{}}},
			})
			frontier := state

			if evidence != "live-only" {
				// Make the proof source part of the selected value history.
				snapshot := bindEvidenceSnapshot("a", proof)
				snapshot.Deps = toPbRefs([]Tip{state})
				owner = log.putEffect(snapshot)
				ownerEffect, _ = e.effectCache.Get(owner)

				verdicts, err = e.proofVerdicts(owner, ownerEffect)
				if err != nil {
					t.Fatal(err)
				}

				frontier = owner
			}

			if evidence == "cached-label" {
				entry := verdicts["old"]
				entry.proven = false
				verdicts["old"] = entry
			}

			view := newSnapshotReadView(e, context.Background())
			view.frontiers["a"] = snapshotFrontier{resolved: []Tip{frontier}}
			view.verdicts = verdicts
			s := &transactionSnapshot{
				engine: e, views: map[string]*snapshotReadView{"a": view},
				decided: make(map[Tip]bool),
			}
			eff, _ := e.effectCache.Get(bind)

			included, err := s.member(context.Background(), bind, eff, make(map[Tip]bool))
			if evidence == "frozen" {
				if err != nil || !included {
					t.Fatalf("retained proof reopened reclaimed history: included=%t error=%v", included, err)
				}
				// Evidence must not reverse an earlier exclusion.
				s.decided[bind] = false
				if included, err := s.member(context.Background(), bind, eff, make(map[Tip]bool)); err != nil || included {
					t.Fatalf("proof reversed frozen membership: included=%t error=%v", included, err)
				}
			} else if !errors.Is(err, errEffectNotResident) {
				t.Fatalf("unusable proof bypassed the history check: included=%t error=%v", included, err)
			}
		})
	}
}

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
				if err := s.prepare(context.Background(), []string{"winner"}); err != nil {
					t.Fatal(err)
				}

				if !s.decided[a] || s.decided[excluded] {
					t.Fatalf("fixture must select A and exclude its local competitor: A=%v excluded=%v", s.decided[a], s.decided[excluded])
				}

				v := s.views["winner"]
				if _, lost := v.lost[loser]; !lost || len(v.lost) != 1 {
					t.Errorf("lost=%v, want only %s", v.lost, loser)
				}

				for key := range v.anchors {
					if slices.Contains(v.frontiers[key].resolved, excluded) || slices.Contains(v.anchors[key], excluded) {
						t.Errorf("live competitor leaked into historical value history on %q", key)
					}
				}

				tips := slices.Concat(v.frontiers["winner"].resolved, v.anchors["winner"])

				result, _, err := e.reconstructWithView("winner", tips, "reader", true, v)
				if err != nil || string(result.GetScalar().GetRaw()) != wantValue {
					t.Fatalf("value=%q error=%v, want %q", result.GetScalar().GetRaw(), err, wantValue)
				}
			})
		}
	}
}
