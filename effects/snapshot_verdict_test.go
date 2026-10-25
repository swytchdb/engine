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
	"slices"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
)

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
	}{
		{name: "no_snapshots"},
		{name: "loser_snapshot", aliceSnapshot: true},
		{name: "winner_snapshot", bobSnapshot: true},
		{name: "both_snapshots", aliceSnapshot: true, bobSnapshot: true},
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
						for _, key := range []string{"winner", payoutKey} {
							tips[key] = log.putEffect(&pb.Effect{
								Key: []byte(key), Hlc: sTs(int64(log.nextOff)), NodeId: node,
								Deps: []*pb.EffectRef{toPbRef(bind)},
								Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
									TxnVerdicts: map[string]pb.Verdict{txnID: pb.Verdict_WON},
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
