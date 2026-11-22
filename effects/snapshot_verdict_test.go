/*
 * Copyright 2026 Swytch Labs BV
 *
 * This file is part of Swytch.
 *
 * Swytch is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Swytch is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Swytch. If not, see <https://www.gnu.org/licenses/>.
 *
 * Additional permissions under GNU AGPL version 3 section 7 apply to
 * this file. See the NOTICE.md file distributed with this source code
 * for the current set of additional permissions.
 */

package effects

import (
	"context"
	"fmt"
	"slices"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
)

func TestReconstruct_ConflictingLegacyVerdictsReevaluate(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprintf("reverse=%t", reverse), func(t *testing.T) {
			log := newSnapshotLog()
			e := newSnapshotEngine(log)
			winner := closureBind(log, "winner", 1, []string{"winner"}, nil)
			loser := closureBind(log, "loser", 2, []string{"winner"}, nil)

			tips := make([]Tip, 0, 3)
			for _, verdict := range []pb.Verdict{pb.Verdict_LOST, pb.Verdict_WON, pb.Verdict_LOST} {
				tips = append(tips, log.putEffect(&pb.Effect{
					Key: []byte("winner"), Deps: toPbRefs([]Tip{winner, loser}),
					Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{TxnVerdicts: map[string]pb.Verdict{"winner": verdict}}},
				}))
			}

			if reverse {
				slices.Reverse(tips)
			}

			preSubscribe(e, "winner")
			e.index.Insert("winner", nil, keytrie.NewTipSet(tips...))

			got, _, _, err := e.GetSnapshot("winner")
			if err != nil || string(got.GetScalar().GetRaw()) != "winner" {
				t.Fatalf("conflicting cached labels changed bind arbitration: %v, %v", got, err)
			}
		})
	}
}

// A loses to B in one package, but B loses to C on another key. Reconcile
// inputs, not labels: A and C must survive. C may arrive without a snapshot.
// The shared key's LCA has reclaimed its data ancestry, so opening an old bind
// there would be both unnecessary and incorrect.
func TestReconstruct_VerdictPackagesReconcilePriorityChain(t *testing.T) {
	for _, cSnapshot := range []bool{false, true} {
		for _, payoutFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("c-snapshot=%t/payout-first=%t", cSnapshot, payoutFirst), func(t *testing.T) {
				log := newSnapshotLog()
				e := newSnapshotEngine(log)
				a := closureBind(log, "A", 3, []string{"winner", "payout:A"}, nil)
				b := closureBind(log, "B", 2, []string{"winner", "other"}, nil)
				c := closureBind(log, "C", 1, []string{"other"}, nil)

				ab, err := e.bindProof(b, map[string]Tip{"A": a}, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer e.effectCache.releaseEvidence(evidenceTips(ab))

				bc, err := e.bindProof(c, map[string]Tip{"B": b}, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer e.effectCache.releaseEvidence(evidenceTips(bc))

				payout := bindEvidenceSnapshot("payout:A", ab)
				payout.Deps = toPbRefs([]Tip{a})
				payoutTip := log.putEffect(payout)
				folded := bindEvidenceSnapshot("winner", ab)
				folded.GetSnapshot().State = &pb.ReducedEffect{Scalar: scalarInsertRaw([]byte("B"))}
				folded.Deps = toPbRefs([]Tip{{99, 99}})
				foldedTip := log.putEffect(folded)
				otherTips := []Tip{b, c}

				if cSnapshot {
					other := bindEvidenceSnapshot("other", bc)
					other.Deps = toPbRefs(otherTips)
					otherTips = []Tip{log.putEffect(other)}
				}

				preSubscribe(e, "winner", "payout:A", "other")
				e.index.Insert("winner", nil, keytrie.NewTipSet(foldedTip))
				e.index.Insert("payout:A", nil, keytrie.NewTipSet(payoutTip))
				e.index.Insert("other", nil, keytrie.NewTipSet(otherTips...))
				e.broadcaster = &cutBroadcaster{load: func(tip Tip) ([]byte, error) {
					// Optional conflict diagnostics may probe absent ancestry;
					// value reconstruction must not require it.
					return nil, ErrEffectNotFound
				}}

				keys := []string{"other", "payout:A"}
				if payoutFirst {
					slices.Reverse(keys)
				}

				for _, key := range keys {
					got, _, _, err := e.GetSnapshot(key)
					if err != nil || string(got.GetScalar().GetRaw()) != "100" {
						t.Fatalf("%s=%v err=%v; A and C should survive", key, got, err)
					}
				}

				got, err := e.GetSnapshots(context.Background(), keys)
				if err != nil {
					t.Fatal(err)
				}

				for i, result := range got {
					if string(result.GetScalar().GetRaw()) != "100" {
						t.Errorf("batch %s=%v", keys[i], result)
					}
				}
			})
		}
	}
}

// A local WON package and a later-discovered package containing the competing
// winner must be reconciled. The first visited snapshot cannot select which
// verdict for the same transaction is authoritative.
func TestReconstruct_ConflictingVerdictEvidenceOrderIndependent(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		name := "old-first"
		if reverse {
			name = "new-first"
		}

		t.Run(name, func(t *testing.T) {
			log := newSnapshotLog()
			e := newSnapshotEngine(log)
			loser := closureBind(log, "loser", 2, []string{"winner", "payout:loser"}, nil)
			winner := closureBind(log, "winner", 1, []string{"winner", "payout:winner"}, nil)

			oldProof, err := e.bindProof(loser, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer e.effectCache.releaseEvidence(evidenceTips(oldProof))

			newProof, err := e.bindProof(winner, map[string]Tip{"loser": loser}, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer e.effectCache.releaseEvidence(evidenceTips(newProof))

			tips := make([]Tip, 0, 2)

			for _, proof := range []*pb.BindEvidence{oldProof, newProof} {
				snapshot := bindEvidenceSnapshot("payout:loser", proof)
				snapshot.Deps = toPbRefs([]Tip{loser})
				tips = append(tips, log.putEffect(snapshot))
			}

			if reverse {
				slices.Reverse(tips)
			}

			preSubscribe(e, "winner", "payout:loser", "payout:winner")
			e.index.Insert("winner", nil, keytrie.NewTipSet(loser, winner))
			e.index.Insert("payout:loser", nil, keytrie.NewTipSet(tips...))
			e.index.Insert("payout:winner", nil, keytrie.NewTipSet(winner))

			got, _, _, err := e.GetSnapshot("payout:loser")
			if err != nil {
				t.Fatal(err)
			}

			if got != nil {
				t.Fatalf("losing payout survived despite available competing verdict evidence: %v", got)
			}
		})
	}
}

// A bind first discovered as an ancestor on winner must also become reachable
// from payout. Reinstalling an ancestor must not replace a newer payout value.
func TestReconstruct_InstallsAssociatedBindHistory(t *testing.T) {
	for _, state := range []string{"unseen", "already_tip", "descendant", "descendant_preinstalled", "missing_branch", "transaction_snapshot"} {
		t.Run(state, func(t *testing.T) {
			log := newSnapshotLog()

			e := newSnapshotEngine(log)
			for _, key := range []string{"winner", "payout"} {
				if err := e.ensureSubscribed(key); err != nil {
					t.Fatal(err)
				}
			}
			// Cache the initially empty payout, as on the partitioned peer.
			if got, _, _, err := e.GetSnapshot("payout"); err != nil || got != nil {
				t.Fatalf("initial payout = %v, %v", got, err)
			}

			winner := log.putEffect(&pb.Effect{
				Key: []byte("winner"), NodeId: 1, Hlc: sTs(10), TxnId: "draw",
				Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("bob"))},
			})

			payout := log.putEffect(&pb.Effect{
				Key: []byte("payout"), NodeId: 1, Hlc: sTs(11), TxnId: "draw",
				Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("100"))},
			})
			if state == "missing_branch" {
				payout = Tip{999, 999}
			}

			bind := log.putEffect(&pb.Effect{
				Key: []byte("winner"), NodeId: 1, Hlc: sTs(12), TxnId: "draw",
				Deps: []*pb.EffectRef{toPbRef(winner), toPbRef(payout)},
				Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
					TxnHlc: sTs(12), OriginatorNodeId: 1,
					Keys: []*pb.TransactionalBindEffect_KeyBind{
						{Key: []byte("winner"), NewTip: toPbRef(winner)},
						{Key: []byte("payout"), NewTip: toPbRef(payout)},
					},
				}},
			})
			want := "100"

			if state == "already_tip" {
				e.updateIndex("payout", nil, bind)
			}

			if state == "descendant" || state == "descendant_preinstalled" {
				newer := log.putEffect(&pb.Effect{
					Key: []byte("payout"), NodeId: 1, Hlc: sTs(20),
					Deps: []*pb.EffectRef{toPbRef(bind)},
					Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("200"))},
				})
				e.updateIndex("payout", nil, newer)

				want = "200"
			}

			if state == "descendant_preinstalled" {
				e.updateIndex("payout", nil, bind)
			}

			payoutTips := e.index.Contains("payout").Tips()
			// The middle node writes only winner; its ancestry carries draw.
			middle := log.putEffect(&pb.Effect{
				Key: []byte("winner"), NodeId: 1, Hlc: sTs(30),
				Deps: []*pb.EffectRef{toPbRef(bind)},
				Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("charlie"))},
			})
			e.updateIndex("winner", nil, middle)

			if state == "transaction_snapshot" {
				before := e.index.Contains("payout").Tips()
				if _, _, err := e.reconstruct("winner", e.index.Contains("winner").Tips(), "active", true); err != nil {
					t.Fatal(err)
				}

				if !tipsEqual(before, e.index.Contains("payout").Tips()) {
					t.Fatal("transaction snapshot read changed the other key's live frontier")
				}
			}

			if got, _, _, err := e.GetSnapshot("winner"); err != nil || got == nil || string(got.GetScalar().GetRaw()) != "charlie" {
				t.Fatalf("winner = %v, %v; want charlie", got, err)
			}

			if state == "descendant" && !tipsEqual(payoutTips, e.index.Contains("payout").Tips()) {
				t.Fatal("discovery reintroduced an already-reachable ancestor into the live frontier")
			}

			got, _, _, err := e.GetSnapshot("payout")
			if err != nil {
				t.Fatal(err)
			}

			if state == "missing_branch" {
				if got != nil || slices.Contains(e.index.Contains("payout").Tips(), bind) {
					t.Fatalf("unwalkable payout bind installed: %v", got)
				}

				return
			}

			if got == nil || string(got.GetScalar().GetRaw()) != want {
				t.Fatalf("payout = %v; want %s", got, want)
			}

			if (state == "descendant" || state == "descendant_preinstalled") && slices.Contains(e.index.Contains("payout").Tips(), bind) {
				t.Fatal("ancestor bind was not pruned from payout tips")
			}
		})
	}
}

// Independent partitions can both publish WON before discovering a competing
// transaction. Adding those verdicts must not change the reconstruction of the
// same transaction DAG or let a losing transaction's other keys remain visible.
func TestReconstruct_WonSnapshotsPreserveConflictChecks(t *testing.T) {
	for _, tc := range []struct {
		name          string
		aliceSnapshot bool
		bobSnapshot   bool
		sequential    bool
		warmPayout    bool
		ownedEvidence bool
	}{
		{name: "no_snapshots"},
		{name: "loser_snapshot", aliceSnapshot: true},
		{name: "winner_snapshot", bobSnapshot: true},
		{name: "both_snapshots", aliceSnapshot: true, bobSnapshot: true},
		// Real subscriber-backed commits attach a proof, not just WON labels.
		// Independent proofs must not hide a newly discovered cross-key loss.
		{name: "both_snapshots_with_evidence", aliceSnapshot: true, bobSnapshot: true, ownedEvidence: true},
		{name: "sequential_snapshots", aliceSnapshot: true, bobSnapshot: true, sequential: true},
		{name: "cached_payout_new_competitor", aliceSnapshot: true, bobSnapshot: true, warmPayout: true},
	} {
		for _, payoutFirst := range []bool{false, true} {
			order := "winner_first"
			if payoutFirst {
				order = "payout_first"
			}

			t.Run(tc.name+"/"+order, func(t *testing.T) {
				log := newSnapshotLog()
				e := newSnapshotEngine(log)

				addDraw := func(node uint64, name string, hashByte byte, snapshot bool, parent *pb.EffectRef) map[string]Tip {
					log.nodeID = node
					txnID := "draw-" + name
					payoutKey := "payout:" + name

					var deps []*pb.EffectRef
					if parent != nil {
						deps = []*pb.EffectRef{parent}
					}

					winnerData := log.putEffect(&pb.Effect{
						Key: []byte("winner"), Hlc: sTs(int64(log.nextOff)), NodeId: node, TxnId: txnID,
						Deps: deps,
						Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte(name))},
					})
					payoutData := log.putEffect(&pb.Effect{
						Key: []byte(payoutKey), Hlc: sTs(int64(log.nextOff)), NodeId: node, TxnId: txnID,
						Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("100"))},
					})
					hash := make([]byte, 32)
					hash[0] = hashByte
					bind := log.putEffect(&pb.Effect{
						Key: []byte("winner"), Hlc: sTs(int64(log.nextOff)), NodeId: node, TxnId: txnID,
						ForkChoiceHash: hash,
						Deps:           []*pb.EffectRef{toPbRef(winnerData), toPbRef(payoutData)},
						Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
							TxnHlc: sTs(int64(log.nextOff)), OriginatorNodeId: node,
							Keys: []*pb.TransactionalBindEffect_KeyBind{
								{Key: []byte("winner"), ConsumedTips: deps, NewTip: toPbRef(winnerData)},
								{Key: []byte(payoutKey), NewTip: toPbRef(payoutData)},
							},
						}},
					})

					tips := map[string]Tip{"winner": bind, payoutKey: bind}

					if snapshot {
						var proof *pb.BindEvidence

						if tc.ownedEvidence {
							var err error

							proof, err = e.bindProof(bind, nil, nil)
							if err != nil {
								t.Fatal(err)
							}
							defer e.effectCache.releaseEvidence(evidenceTips(proof))
						}

						for _, key := range []string{"winner", payoutKey} {
							tips[key] = log.putEffect(&pb.Effect{
								Key: []byte(key), Hlc: sTs(int64(log.nextOff)), NodeId: node,
								Deps: []*pb.EffectRef{toPbRef(bind)},
								Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
									TxnVerdicts: map[string]pb.Verdict{txnID: pb.Verdict_WON},
									Evidence:    proof,
								}},
							})
						}
					}

					return tips
				}

				alice := addDraw(101, "alice", 0xff, tc.aliceSnapshot, nil)

				var payoutTips []Tip

				if tc.warmPayout {
					e.index.Insert("winner", nil, keytrie.NewTipSet(alice["winner"]))
					e.index.Insert("payout:alice", nil, keytrie.NewTipSet(alice["payout:alice"]))

					got, _, _, err := e.GetSnapshot("payout:alice")
					if err != nil || got == nil || string(got.GetScalar().GetRaw()) != "100" {
						t.Fatalf("payout before discovering competitor = %v, %v; want 100", got, err)
					}

					payoutTips = e.index.Contains("payout:alice").Tips()
				}

				var parent *pb.EffectRef
				if tc.sequential {
					parent = toPbRef(alice["winner"])
				}

				bob := addDraw(202, "bob", 0x01, tc.bobSnapshot, parent)

				winnerTips := []Tip{alice["winner"], bob["winner"]}
				if tc.sequential {
					winnerTips = []Tip{bob["winner"]}
				}

				e.index.Insert("winner", e.index.Contains("winner"), keytrie.NewTipSet(winnerTips...))

				if !tc.warmPayout {
					e.index.Insert("payout:alice", nil, keytrie.NewTipSet(alice["payout:alice"]))
				}

				e.index.Insert("payout:bob", nil, keytrie.NewTipSet(bob["payout:bob"]))

				keys := []string{"winner", "payout:alice", "payout:bob"}
				if payoutFirst {
					keys = []string{"payout:alice", "payout:bob", "winner"}
				}

				for _, key := range keys {
					if tc.warmPayout && key == "payout:alice" && !tipsEqual(payoutTips, e.index.Contains(key).Tips()) {
						t.Fatal("regression requires unchanged payout tips when the competitor is discovered")
					}

					got, _, _, err := e.GetSnapshot(key)
					if err != nil {
						t.Fatalf("GetSnapshot(%q): %v", key, err)
					}

					if key == "payout:alice" && !tc.sequential {
						if got != nil {
							t.Errorf("losing transaction's payout survived: %v", got)
						}

						continue
					}

					want := "100"
					if key == "winner" {
						want = "bob"
					}

					if got == nil || string(got.GetScalar().GetRaw()) != want {
						t.Errorf("GetSnapshot(%q) = %v, want %q", key, got, want)
					}
				}
			})
		}
	}
}
