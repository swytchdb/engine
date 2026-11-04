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
)

func TestDependencyTraversal(t *testing.T) {
	ordinary := &pb.EffectRef{NodeId: 1, Offset: 1}
	left := &pb.EffectRef{NodeId: 1, Offset: 2}
	right := &pb.EffectRef{NodeId: 1, Offset: 3}
	eff := &pb.Effect{
		Deps: []*pb.EffectRef{ordinary},
		Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{Keys: []*pb.TransactionalBindEffect_KeyBind{
			{Key: []byte("left"), NewTip: left},
			{Key: []byte("right"), NewTip: right},
		}}},
	}

	if got := slices.Collect(allDeps(eff)); !slices.Equal(got, []*pb.EffectRef{ordinary, left, right}) {
		t.Fatalf("global dependencies: %v", got)
	}

	if got := slices.Collect(keyDeps("left", eff)); !slices.Equal(got, []*pb.EffectRef{left}) {
		t.Fatalf("key-scoped dependencies: %v", got)
	}

	if got := slices.Collect(keyDeps("absent", eff)); len(got) != 0 {
		t.Fatalf("unrelated key inherited dependencies: %v", got)
	}

	for stopAfter := 1; stopAfter <= 3; stopAfter++ {
		count := 0
		for range allDeps(eff) {
			count++
			if count == stopAfter {
				break
			}
		}
	}

	eff.Kind = nil
	if got := slices.Collect(keyDeps("left", eff)); !slices.Equal(got, []*pb.EffectRef{ordinary}) {
		t.Fatalf("ordinary dependencies changed: %v", got)
	}

	if got := slices.Collect(allDeps(nil)); len(got) != 0 {
		t.Fatalf("nil effect has dependencies: %v", got)
	}
}

func TestDAGPrepareReplacesPreviousTraversal(t *testing.T) {
	e := newSnapshotEngine(newSnapshotLog())
	tip := Tip{1, 1}

	eff := &pb.Effect{Key: []byte("key"), Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{State: &pb.ReducedEffect{}}}}
	if err := e.effectCache.Put(tip, eff); err != nil {
		t.Fatal(err)
	}

	d := newDag(e, "key", "")
	if err := d.prepare([]Tip{tip}); err != nil {
		t.Fatal(err)
	}

	if d.lcaTip != tip || len(d.topoOrder) != 1 {
		t.Fatal("snapshot was not collected")
	}

	if err := d.prepare(nil); err != nil {
		t.Fatal(err)
	}

	if d.lcaTip != (Tip{}) || len(d.nodes) != 0 || len(d.topoOrder) != 0 {
		t.Fatal("empty traversal retained the previous DAG")
	}
}
