package effects

import (
	"fmt"
	"slices"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

// A snapshot covers its own frontier, not a competing transaction discovered
// through another key. Adding that bind as an anchor moves the common boundary
// into the snapshot's ancestry. No cached verdict is required for fork choice.
func TestBindEvidence_CompetitorBelowSnapshot(t *testing.T) {
	for _, snapshotFirst := range []bool{false, true} {
		name := "competitor-first"
		if snapshotFirst {
			name = "snapshot-first"
		}
		t.Run(name, func(t *testing.T) {
			log := newSnapshotLog()
			e := newSnapshotEngine(log)
			base := log.putEffect(&pb.Effect{
				Key: []byte("winner"),
				// History below the actual common snapshot may be reclaimed.
				Deps: toPbRefs([]Tip{{99, 1}}),
				Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
					State: &pb.ReducedEffect{Scalar: scalarInsertRaw([]byte("base"))},
				}},
			})
			alice := closureBind(log, "alice", 1, []string{"winner", "payout:alice"}, map[string]Tip{"winner": base})
			bob := closureBind(log, "bob", 2, []string{"winner", "payout:bob"}, map[string]Tip{"winner": base})
			snapshot := log.putEffect(&pb.Effect{
				Key: []byte("winner"), Deps: toPbRefs([]Tip{alice}),
				Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
					State: &pb.ReducedEffect{Scalar: scalarInsertRaw([]byte("alice"))},
				}},
			})

			// Prove the distinction independently of the evidence walks.
			for _, tc := range []struct {
				tips []Tip
				lca  Tip
			}{{[]Tip{snapshot}, snapshot}, {[]Tip{snapshot, bob}, base}} {
				d := newDag(e, "winner", "")
				if err := d.prepare(tc.tips); err != nil {
					t.Fatal(err)
				}
				if d.lcaTip != tc.lca {
					t.Fatalf("tips %v: LCA = %v, want %v", tc.tips, d.lcaTip, tc.lca)
				}
			}

			frontiers := map[string][]Tip{
				"winner":       {snapshot},
				"payout:alice": {alice},
				"payout:bob":   {bob},
			}
			keys := []string{"winner", "payout:bob"}
			if snapshotFirst {
				// bindKeyClosureFrom processes its key queue from the end.
				slices.Reverse(keys)
			}
			anchors, verdicts, err := e.bindKeyClosureFrom(keys, func(key string) ([]Tip, error) {
				return frontiers[key], nil
			}, true)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Contains(anchors["payout:alice"], alice) {
				t.Errorf("did not discover Alice below the former LCA: anchors=%v", anchors)
			}
			if len(verdicts) != 0 {
				t.Fatalf("fixture unexpectedly supplies cached verdicts: %v", verdicts)
			}
			lost, _ := e.losersOnKeysAt(anchors, func(key string) []Tip {
				return frontiers[key]
			}, verdicts, nil, false)
			if _, ok := lost["bob"]; !ok || len(lost) != 1 {
				t.Errorf("fork choice = %v, want only Bob to lose", lost)
			}
			view := &snapshotReadView{anchors: anchors, verdicts: verdicts, lost: lost, historical: true}
			result, _, err := e.reconstructWithView("payout:bob", []Tip{bob}, "", false, view)
			if err != nil {
				t.Fatal(err)
			}
			if result != nil {
				t.Errorf("reconstructed losing transaction's payout: %v", result)
			}
		})
	}
}

func TestBindEvidence_ConcurrentSnapshots(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(fmt.Sprintf("cached-verdicts=%t", cached), func(t *testing.T) {
			log := newSnapshotLog()
			e := newSnapshotEngine(log)
			state := func(deps ...Tip) Tip {
				return log.putEffect(&pb.Effect{
					Key: []byte("winner"), Deps: toPbRefs(deps),
					Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
						State: &pb.ReducedEffect{Scalar: scalarInsertRaw([]byte("alice"))},
					}},
				})
			}
			base := state(Tip{99, 1}) // reclaimed history
			alice := closureBind(log, "alice", 1, []string{"winner"}, map[string]Tip{"winner": base})
			bob := closureBind(log, "bob", 2, []string{"winner"}, map[string]Tip{"winner": base})
			branch := state(alice, bob)
			if cached {
				branch = log.putEffect(&pb.Effect{
					Key: []byte("winner"), Deps: toPbRefs([]Tip{branch}),
					Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
						TxnVerdicts:  map[string]pb.Verdict{"alice": pb.Verdict_WON, "bob": pb.Verdict_LOST},
						PrevSnapshot: toPbRef(branch),
					}},
				})
			}
			noop := log.putEffect(&pb.Effect{
				Key: []byte("winner"), Deps: toPbRefs([]Tip{branch}),
				Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
			})
			tips := []Tip{state(noop), state(base)}
			anchors, verdicts, err := e.bindKeyClosureFrom([]string{"winner"}, func(string) ([]Tip, error) {
				return tips, nil
			}, true)
			if err != nil {
				t.Fatal(err)
			}
			lost, _ := e.losersOnKeysAt(anchors, func(string) []Tip { return tips }, verdicts, nil, false)
			if _, ok := lost["bob"]; !ok || len(lost) != 1 {
				t.Fatalf("losers=%v, want only Bob", lost)
			}
			view := &snapshotReadView{anchors: anchors, verdicts: verdicts, lost: lost, historical: true}
			result, _, err := e.reconstructWithView("winner", tips, "", false, view)
			if err != nil || string(result.GetScalar().GetRaw()) != "alice" {
				t.Fatalf("reconstructed %v, error=%v; want Alice", result, err)
			}
		})
	}
}

func TestBindEvidence_VerdictShortcutDoesNotExpandFoldedData(t *testing.T) {
	for _, resident := range []bool{false, true} {
		t.Run(fmt.Sprintf("resident=%t", resident), func(t *testing.T) {
			e := newSnapshotEngine(nil)
			old, verdict, snapshot := Tip{7, 1}, Tip{7, 2}, Tip{7, 3}
			metadata := &pb.Effect{
				Key: []byte("k"), Deps: toPbRefs([]Tip{old}),
				Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
					TxnVerdicts: map[string]pb.Verdict{"lost": pb.Verdict_LOST},
				}},
			}
			state := &pb.Effect{
				Key: []byte("k"), Deps: toPbRefs([]Tip{verdict}),
				Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
					State: &pb.ReducedEffect{}, PrevSnapshot: toPbRef(verdict),
				}},
			}
			putCutEffects(t, e, map[Tip]*pb.Effect{verdict: metadata})
			if resident {
				putCutEffects(t, e, map[Tip]*pb.Effect{snapshot: state})
			}
			e.broadcaster = &cutBroadcaster{load: func(tip Tip) ([]byte, error) {
				if tip != snapshot {
					t.Fatalf("fetched folded data at %v", tip)
				}
				return wireEffect(state), nil
			}}
			_, verdicts, err := e.bindKeyClosureFrom([]string{"k"}, func(string) ([]Tip, error) {
				return []Tip{snapshot}, nil
			}, true)
			if err != nil || verdicts["lost"].verdict != pb.Verdict_LOST {
				t.Fatalf("verdicts=%v, error=%v; want cached loss without folded data", verdicts, err)
			}
		})
	}
}
