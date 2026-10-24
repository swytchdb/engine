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
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
)

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
