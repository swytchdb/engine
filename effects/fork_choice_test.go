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
	"bytes"
	"testing"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func ts(nanos int64) *timestamppb.Timestamp {
	return timestamppb.New(time.Unix(0, nanos))
}

func TestComputeForkChoiceHash_Deterministic(t *testing.T) {
	h1 := ComputeForkChoiceHash(pb.NodeID(1), ts(100))

	h2 := ComputeForkChoiceHash(pb.NodeID(1), ts(100))
	if !bytes.Equal(h1, h2) {
		t.Fatal("same inputs must produce same hash")
	}

	if len(h1) != 32 {
		t.Fatalf("expected 32-byte hash, got %d", len(h1))
	}
}

func TestComputeForkChoiceHash_DifferentInputs(t *testing.T) {
	h1 := ComputeForkChoiceHash(pb.NodeID(1), ts(100))
	h2 := ComputeForkChoiceHash(pb.NodeID(2), ts(100))
	h3 := ComputeForkChoiceHash(pb.NodeID(1), ts(101))

	if bytes.Equal(h1, h2) {
		t.Fatal("different nodeIDs should produce different hashes")
	}

	if bytes.Equal(h1, h3) {
		t.Fatal("different HLCs should produce different hashes")
	}
}

func TestForkChoiceLess_Ordering(t *testing.T) {
	a := ComputeForkChoiceHash(pb.NodeID(1), ts(100))
	b := ComputeForkChoiceHash(pb.NodeID(2), ts(200))

	// One must be less than the other
	if !ForkChoiceLess(a, b) && !ForkChoiceLess(b, a) {
		t.Fatal("different hashes must have a strict ordering")
	}

	// Reflexivity: neither is less than itself
	if ForkChoiceLess(a, a) {
		t.Fatal("hash should not be less than itself")
	}
}

func TestForkChoiceLess_EliminatesHLCBias(t *testing.T) {
	wins := 0

	trials := 20
	for i := range trials {
		h1 := ComputeForkChoiceHash(pb.NodeID(1), ts(int64(100+i)))

		h2 := ComputeForkChoiceHash(pb.NodeID(2), ts(int64(200+i)))
		if ForkChoiceLess(h1, h2) {
			wins++
		}
	}

	if wins == 0 || wins == trials {
		t.Fatalf("expected mixed winners, got %d/%d for node1", wins, trials)
	}
}

func TestLosersOnKeyAt_SharedRootFork(t *testing.T) {
	for _, shape := range []string{"fork", "merge", "snapshot", "snapshot-descendant"} {
		for _, reverse := range []bool{false, true} {
			name := shape
			if reverse {
				name += "/reversed"
			}

			t.Run(name, func(t *testing.T) {
				log := newSnapshotLog()
				e := newSnapshotEngine(log)
				addBind := func(txn string, hash byte, consumed ...Tip) Tip {
					data := log.putEffect(&pb.Effect{
						Key: []byte("k"), NodeId: 1, Hlc: sTs(100), TxnId: txn,
						Deps: toPbRefs(consumed), Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
					})

					return log.putEffect(&pb.Effect{
						Key: []byte("k"), NodeId: 1, Hlc: sTs(100), TxnId: txn, ForkChoiceHash: []byte{hash},
						Deps: toPbRefs([]Tip{data}),
						Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
							Keys: []*pb.TransactionalBindEffect_KeyBind{{
								Key: []byte("k"), NewTip: toPbRef(data), ConsumedTips: toPbRefs(consumed),
							}},
						}},
					})
				}

				// A common root does not make the siblings a chain. Adding a
				// merge or snapshot must not change which sibling loses.
				root := addBind("root", 0x40)
				winner := addBind("winner", 0x10, root)
				loser := addBind("loser", 0x20, root)

				tips := []Tip{winner, loser}
				if reverse {
					tips[0], tips[1] = tips[1], tips[0]
				}

				switch shape {
				case "merge":
					tips = []Tip{addBind("descendant", 0x30, tips...)}
				case "snapshot", "snapshot-descendant":
					snapshot := log.putEffect(&pb.Effect{
						Key: []byte("k"), NodeId: 1, Hlc: sTs(100), Deps: toPbRefs(tips),
						Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{}},
					})

					tips = []Tip{snapshot}
					if shape == "snapshot-descendant" {
						tips = []Tip{addBind("descendant", 0x30, snapshot)}
					}
				}

				losers, _ := e.losersOnKeyAt("k", tips, nil, nil, nil, false)
				if _, found := losers["loser"]; !found {
					t.Fatalf("concurrent sibling escaped fork choice: %v", losers)
				}

				if len(losers) != 1 {
					t.Fatalf("unexpected other losers: %v", losers)
				}
			})
		}
	}
}
