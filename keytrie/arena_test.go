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

package keytrie

import "testing"

func TestEmptyArenaReusesDirectory(t *testing.T) {
	var arena critbitArena[int]
	arena.init(2, nil)

	empty := arena.list.Load()
	arena.alloc()
	arena.clear()

	if arena.list.Load() != empty || len(empty.chunks) != 0 {
		t.Fatal("clear did not restore the unchanged empty directory")
	}

	if arena.residentBytes() != 0 {
		t.Fatal("embedded empty directory was charged as a separate allocation")
	}
}

func TestArenaGrowthPreservesPublishedDirectories(t *testing.T) {
	var arena critbitArena[int]
	arena.init(1, nil)

	snapshots := make([]*arenaChunkList[int], 0, 100)

	for i := range 100 {
		node, _ := arena.alloc()
		*node = i + 1

		snapshots = append(snapshots, arena.list.Load())
	}

	// Neither appending into spare capacity nor recycling may change entries
	// visible to readers of an older directory.
	arena.recordReaped(0)

	for i, snapshot := range snapshots {
		if len(snapshot.chunks) != i+1 {
			t.Fatalf("snapshot %d grew to %d entries", i, len(snapshot.chunks))
		}

		for j, chunk := range snapshot.chunks {
			if chunk.nodes[0] != j+1 {
				t.Fatalf("snapshot %d entry %d changed", i, j)
			}
		}
	}
}

func TestArenaRecycleKeepsGenerationAccountingSeparate(t *testing.T) {
	var arena critbitArena[int]
	arena.init(2, nil)
	arena.alloc()
	arena.alloc()

	old := arena.list.Load().chunks[0]
	arena.recordReaped(0)
	arena.recordReaped(0)

	fresh := arena.list.Load().chunks[0]
	if old == fresh || old.reaped.Load() != 2 || fresh.reaped.Load() != 0 {
		t.Fatal("recycling changed the old generation's accounting")
	}

	if fresh.outstanding.Load() != 2 || arena.freeCount.Load() != 2 {
		t.Fatal("recycled slots were not accounted before publication")
	}

	arena.alloc()

	if fresh.outstanding.Load() != 1 || old.outstanding.Load() != 0 {
		t.Fatal("allocation modified the wrong generation")
	}
}

func TestArenaDropsChunkWhenRecycleQueueIsFull(t *testing.T) {
	var arena critbitArena[int]
	arena.init(2, nil)
	arena.freeCap = 0
	arena.alloc()
	arena.alloc()
	arena.recordReaped(0)
	arena.recordReaped(0)

	if arena.list.Load().chunks[0] != nil || arena.bytes() != 0 {
		t.Fatal("full recycle queue retained the dead chunk")
	}

	if _, chunk := arena.alloc(); chunk != 1 {
		t.Fatalf("allocation reused a dropped chunk: %d", chunk)
	}
}

func TestArenaClearDiscardsRecycledSlots(t *testing.T) {
	var arena critbitArena[int]
	arena.init(2, nil)

	for range 2 {
		node, _ := arena.alloc()
		*node = 42
	}

	arena.recordReaped(0)
	arena.recordReaped(0)
	arena.clear()

	for i := range 5 {
		node, chunk := arena.alloc()
		if chunk != uint32(i/2) || *node != 0 {
			t.Fatalf("allocation after clear returned chunk %d, value %d", chunk, *node)
		}

		*node = i + 1
	}

	if arena.freeCount.Load() != 0 {
		t.Fatal("clear retained recycled slots")
	}
}
