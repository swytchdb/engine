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
	"github.com/swytchdb/engine/keytrie"
	"google.golang.org/protobuf/proto"
)

func graphInitialEffects() []*pb.Effect {
	effects := pb.NewGraphEffects([]byte("graph"), []byte("avl"))
	for i, effect := range effects {
		effect.Hlc = rTs(int64(i + 1))
		effect.NodeId = 1
	}

	return effects
}

func assertGraphDescriptor(t *testing.T, state *pb.ReducedEffect) {
	t.Helper()

	if state.GetTypeTag() != pb.ValueType_TYPE_GRAPH {
		t.Fatalf("graph type lost: %v", state)
	}

	if got := state.GetNetAdds()[pb.GraphAlgorithmKey].GetData().GetRaw(); string(got) != "avl" {
		t.Fatalf("algorithm = %q, want avl", got)
	}

	for _, field := range []struct {
		key string
		tag pb.ValueType
	}{
		{pb.GraphRegionSubscriptionsKey, pb.ValueType_TYPE_JSON_OBJECT},
		{pb.GraphEntryPointKey, pb.ValueType_TYPE_JSON_ARRAY},
	} {
		child := state.GetNetAdds()[field.key].GetData().GetChild()
		if string(child) != field.key || state.GetPartitions()[string(child)].GetTypeTag() != field.tag {
			t.Fatalf("missing nested container %q: %v", field.key, state)
		}
	}
}

func TestGraphDescriptorReduction(t *testing.T) {
	seed := ReduceBranch(graphInitialEffects())
	assertGraphDescriptor(t, seed)

	if filterSnapshot(seed) == nil {
		t.Fatal("empty graph was auto-deleted")
	}

	node := []byte{0xff, 0, 1} // An opaque node identity, not a causal reference.
	state := ReduceChain(seed, []*pb.Effect{
		virt(makeDataEffect(10, keyedInsertRaw("peer-a/sub-1", []byte("mammals"))), pb.GraphRegionSubscriptionsKey),
		virt(makeDataEffect(11, keyedInsertRaw("peer-b/sub-1", []byte("reptiles"))), pb.GraphRegionSubscriptionsKey),
		virt(makeDataEffect(12, orderedInsert(pb.Placement_PLACE_TAIL, "first", node)), pb.GraphEntryPointKey),
		virt(makeDataEffect(13, orderedInsert(pb.Placement_PLACE_TAIL, "last", []byte("node/z"))), pb.GraphEntryPointKey),
		virt(makeDataEffect(14, orderedInsertRef(pb.Placement_PLACE_AFTER, "middle", []byte("node/m"), []byte("first"))), pb.GraphEntryPointKey),
	})
	assertGraphDescriptor(t, state)

	entries := state.GetPartitions()[pb.GraphEntryPointKey].GetOrderedElements()
	if len(entries) != 3 || !bytes.Equal(entries[0].GetData().GetRaw(), node) || string(entries[1].GetData().GetId()) != "middle" || string(entries[2].GetData().GetId()) != "last" {
		t.Fatalf("entry-point order or identity lost: %v", entries)
	}

	if state.GetPartitions()[pb.GraphRegionSubscriptionsKey].GetCollection() != pb.CollectionKind_KEYED {
		t.Fatal("subscription directory did not reduce as a keyed collection")
	}

	if len(seed.GetPartitions()[pb.GraphRegionSubscriptionsKey].GetNetAdds()) != 0 || len(seed.GetPartitions()[pb.GraphEntryPointKey].GetOrderedElements()) != 0 {
		t.Fatal("graph update mutated the original snapshot")
	}

	state = ReduceChain(state, []*pb.Effect{
		virt(makeDataEffect(15, keyedRemove("peer-a/sub-1")), pb.GraphRegionSubscriptionsKey),
		virt(makeDataEffect(16, orderedRemove("first")), pb.GraphEntryPointKey),
		virt(makeDataEffect(17, orderedRemove("middle")), pb.GraphEntryPointKey),
		virt(makeDataEffect(18, orderedRemove("last")), pb.GraphEntryPointKey),
	})
	assertGraphDescriptor(t, state)

	if directory := state.GetPartitions()[pb.GraphRegionSubscriptionsKey]; len(directory.GetNetAdds()) != 1 || directory.GetNetAdds()["peer-b/sub-1"] == nil {
		t.Fatal("unsubscribe removed another peer's entry")
	}

	if len(state.GetPartitions()[pb.GraphEntryPointKey].GetOrderedElements()) != 0 || filterSnapshot(state) == nil {
		t.Fatal("removing the final entry point removed the graph")
	}

	gone := ReduceChain(state, []*pb.Effect{makeDataEffect(19, scalarRemove())})
	if len(gone.GetPartitions()) != 0 || filterSnapshot(gone) != nil {
		t.Fatal("explicit graph deletion did not remove its metadata")
	}
}

func TestGraphDescriptorConcurrentBranchesAndSnapshot(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})

	var base Tip
	for _, effect := range graphInitialEffects() {
		if base != (Tip{}) {
			effect.Deps = toPbRefs([]Tip{base})
		}

		base = log.putEffect(effect)
	}

	branches := make([]Tip, 0, 2)

	for i, peer := range []string{"peer-a", "peer-b"} {
		sub := virt(makeDataEffect(int64(10+i), keyedInsertRaw(peer, []byte("mammals"))), pb.GraphRegionSubscriptionsKey)
		sub.Key, sub.NodeId, sub.Deps = []byte("graph"), uint64(i+2), toPbRefs([]Tip{base})
		subTip := log.putEffect(sub)
		entry := virt(makeDataEffect(int64(20+i), orderedInsert(pb.Placement_PLACE_TAIL, peer, []byte("absent-node/"+peer))), pb.GraphEntryPointKey)
		entry.Key, entry.NodeId, entry.Deps = []byte("graph"), uint64(i+2), toPbRefs([]Tip{subTip})
		branches = append(branches, log.putEffect(entry))
	}

	state, _, err := e.reconstruct(logicalAddress("graph"), branches, "", false)
	if err != nil {
		t.Fatal(err)
	}

	assertGraphDescriptor(t, state)

	if len(state.GetPartitions()[pb.GraphRegionSubscriptionsKey].GetNetAdds()) != 2 || len(state.GetPartitions()[pb.GraphEntryPointKey].GetOrderedElements()) != 2 {
		t.Fatal("concurrent metadata edits lost a peer or entry point")
	}

	reversed, _, err := e.reconstruct(logicalAddress("graph"), []Tip{branches[1], branches[0]}, "", false)
	if err != nil {
		t.Fatal(err)
	}

	if !proto.Equal(state, reversed) {
		t.Fatal("graph reduction depends on frontier order")
	}

	for _, key := range []string{"absent-node/peer-a", "absent-node/peer-b"} {
		if _, subscribed := e.subscriptions.Load(key); subscribed || e.index.Contains(key) != nil {
			t.Fatalf("graph reconstruction subscribed to referenced node %q", key)
		}
	}

	wire, err := proto.Marshal(&pb.SnapshotEffect{Collection: state.GetCollection(), State: state})
	if err != nil {
		t.Fatal(err)
	}

	decoded := new(pb.SnapshotEffect)
	if err := proto.Unmarshal(wire, decoded); err != nil {
		t.Fatal(err)
	}

	// Reconstruct from a snapshot on a fresh engine with none of the original
	// history or referenced graph nodes installed.
	freshLog := newSnapshotLog()
	fresh := newSnapshotEngine(freshLog)

	t.Cleanup(func() {
		if err := fresh.Close(); err != nil {
			t.Error(err)
		}
	})

	snapshotTip := freshLog.putEffect(&pb.Effect{
		Key: []byte("graph"), Hlc: rTs(30), NodeId: 1,
		Kind: &pb.Effect_Snapshot{Snapshot: decoded},
	})
	fresh.index.Insert("graph", nil, keytrie.NewTipSet(snapshotTip))

	restored, _, _, err := fresh.reconstructLocal(logicalAddress("graph"))
	if err != nil {
		t.Fatal(err)
	}

	assertGraphDescriptor(t, restored)

	if !proto.Equal(restored, state) {
		t.Fatal("graph snapshot round trip changed metadata")
	}

	update := virt(makeDataEffect(31, keyedInsertRaw("peer-c", []byte("birds"))), pb.GraphRegionSubscriptionsKey)
	update.Key, update.Deps = []byte("graph"), toPbRefs([]Tip{snapshotTip})

	updated, _, err := fresh.reconstruct(logicalAddress("graph"), []Tip{freshLog.putEffect(update)}, "", false)
	if err != nil {
		t.Fatal(err)
	}

	if len(updated.GetPartitions()[pb.GraphRegionSubscriptionsKey].GetNetAdds()) != 3 {
		t.Fatal("update after snapshot lost directory entries")
	}

	if !proto.Equal(updated.GetPartitions()[pb.GraphEntryPointKey], state.GetPartitions()[pb.GraphEntryPointKey]) {
		t.Fatal("directory update changed entry points")
	}
}

func TestGraphDescriptorEngineReads(t *testing.T) {
	e := NewEngine(EngineConfig{DefaultMode: UnsafeMode})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})

	ctx := e.NewContext()
	for _, effect := range pb.NewGraphEffects([]byte("graph"), []byte("avl")) {
		if err := ctx.Emit(effect); err != nil {
			t.Fatal(err)
		}
	}

	if err := ctx.Flush(); err != nil {
		t.Fatal(err)
	}

	reader := e.NewContext()

	state, _, err := reader.GetSnapshot("graph")
	if err != nil {
		t.Fatal(err)
	}

	assertGraphDescriptor(t, state)

	if err := reader.Flush(); err != nil {
		t.Fatal(err)
	}

	// A declared but not yet populated graph must also survive both read paths.
	ctx = e.NewContext()
	if err := ctx.Emit(&pb.Effect{Key: []byte("empty"), Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{TypeTag: pb.ValueType_TYPE_GRAPH}}}); err != nil {
		t.Fatal(err)
	}

	if err := ctx.Flush(); err != nil {
		t.Fatal(err)
	}

	raw, _, _, err := e.GetSnapshot("empty")
	if err != nil || raw.GetTypeTag() != pb.ValueType_TYPE_GRAPH {
		t.Fatalf("metadata-only graph missing: %v, %v", raw, err)
	}

	if filterSnapshot(raw) == nil {
		t.Fatal("metadata-only graph filtered out")
	}

	// Graph emptiness does not bypass explicit key expiration.
	expiring := proto.Clone(state).(*pb.ReducedEffect)

	expiring.ExpiresAt = rTs(50)
	if filterSnapshotAt(expiring, time.Unix(0, 51)) != nil {
		t.Fatal("expired graph remained visible")
	}
}
