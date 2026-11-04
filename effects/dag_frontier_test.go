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
	"fmt"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
)

// S correctly summarizes A+B, but L bypasses it:
//
//	base -> A(+1) -> B(+1) -> S(state=base+2) -> R(+1)
//	         \-> L(+1)
//
// Finishing the A path before processing S must not make S authoritative.
// Exercise both discovery orders and both fork-choice replay orders. A real
// common snapshot must remain usable even when its own history is unavailable.
func TestDAGSnapshotDependencyFrontier(t *testing.T) {
	for _, baseKind := range []string{"data-root", "subscription-root", "common-snapshot", "root-snapshot-first"} {
		for _, reverseTips := range []bool{false, true} {
			for _, snapshotFirst := range []bool{false, true} {
				for _, duplicateTips := range []bool{false, true} {
					name := fmt.Sprintf("%s/reverse-%t/snapshot-first-%t/duplicates-%t", baseKind, reverseTips, snapshotFirst, duplicateTips)
					t.Run(name, func(t *testing.T) {
						log := newSnapshotLog()
						e := newSnapshotEngine(log)
						add := func(hash byte, deps ...Tip) Tip {
							return log.putEffect(&pb.Effect{
								Key: []byte("k"), Hlc: sTs(int64(log.nextOff)), NodeId: 1,
								ForkChoiceHash: []byte{hash}, Deps: toPbRefs(deps),
								Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
							})
						}
						snapshot := func(value int64, deps ...Tip) Tip {
							return log.putEffect(&pb.Effect{
								Key: []byte("k"), Hlc: sTs(int64(log.nextOff)), NodeId: 1, Deps: toPbRefs(deps),
								Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
									Collection: pb.CollectionKind_SCALAR,
									State: &pb.ReducedEffect{
										Op: pb.EffectOp_INSERT_OP, Merge: pb.MergeRule_ADDITIVE_INT,
										Collection: pb.CollectionKind_SCALAR, Commutative: true,
										Scalar: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, value),
									},
								}},
							})
						}

						var (
							baseValue                 int64
							baseDeps                  []Tip
							expectedLCA, subscription Tip
						)

						switch baseKind {
						case "subscription-root":
							subscription = log.putEffect(&pb.Effect{
								Key: []byte("k"), Hlc: sTs(1), NodeId: 1,
								Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{SubscriberNodeId: 1}},
							})
							baseDeps = []Tip{subscription}
						case "common-snapshot":
							baseValue = 10
							// No broadcaster or cached bytes for this old dependency:
							// walking below the common snapshot would fail the test.
							expectedLCA = snapshot(baseValue, Tip{99, 1})
							baseDeps = []Tip{expectedLCA}
						case "root-snapshot-first":
							baseValue = 10
							expectedLCA = snapshot(baseValue)
							baseDeps = []Tip{expectedLCA}
						}

						a := add(1, baseDeps...)
						b := add(2, a)
						s := snapshot(baseValue+2, b)

						leftHash, rightHash := byte(1), byte(2)
						if snapshotFirst {
							leftHash, rightHash = rightHash, leftHash
						}

						left := add(leftHash, a)

						rightBase := s
						if baseKind == "root-snapshot-first" {
							// The shorter branch reaches the common snapshot first.
							// It must remain a candidate after its work item is done.
							rightBase = log.putEffect(&pb.Effect{
								Key: []byte("k"), Hlc: sTs(int64(log.nextOff)), NodeId: 1, Deps: toPbRefs([]Tip{s}),
								Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 0)},
							})
						}

						right := add(rightHash, rightBase)

						tips := []Tip{left, right}
						if reverseTips {
							tips[0], tips[1] = tips[1], tips[0]
						}

						if subscription != (Tip{}) {
							// A subscription first seen as a standalone tip must
							// become a frontier entry when A references it.
							tips = append([]Tip{subscription}, tips...)
						}

						if duplicateTips {
							tips = append(tips, tips...)
						}

						d := newDag(e, "k", "")
						if err := d.prepare(tips); err != nil {
							t.Fatal(err)
						}

						if d.lcaTip != expectedLCA {
							t.Errorf("selected snapshot %v, want %v", d.lcaTip, expectedLCA)
						}

						got, _, err := e.reconstruct("k", tips, "", false)
						if err != nil {
							t.Fatal(err)
						}

						if value, want := got.GetScalar().GetIntVal(), baseValue+4; value != want {
							t.Fatalf("got %d, want %d; replay=%v", value, want, d.topoOrder)
						}
					})
				}
			}
		}
	}
}

func TestReconstructionPrunesOnlyCommittedAncestors(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprintf("committed=%t", committed), func(t *testing.T) {
			log := newSnapshotLog()
			e := newSnapshotEngine(log)
			root := log.putEffect(&pb.Effect{
				Key: []byte("k"), Hlc: sTs(1), NodeId: 1,
				Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
			})
			child := log.putEffect(&pb.Effect{
				Key: []byte("k"), Hlc: sTs(2), NodeId: 1, TxnId: "pending", Deps: toPbRefs([]Tip{root}),
				Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
			})
			unknown := Tip{99, 99}
			indexed := []Tip{root, unknown}

			if committed {
				indexed = append(indexed, child)
			}

			e.index.Insert("k", nil, keytrie.NewTipSet(indexed...))

			state, _, err := e.reconstruct("k", []Tip{root, child, root}, "pending", false)
			if err != nil {
				t.Fatal(err)
			}

			if state.GetScalar().GetIntVal() != 2 {
				t.Fatal("duplicate frontier replayed an ancestor twice")
			}

			current := e.index.Contains("k")
			if current.Contains(root) == committed || !current.Contains(unknown) || current.Contains(child) != committed {
				t.Fatalf("pruning changed an uncommitted or unwalked frontier: %v", current.Tips())
			}
		})
	}
}
