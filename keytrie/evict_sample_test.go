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

import (
	"fmt"
	"testing"
)

// Keep enough ghosts to exceed the stack scratch capacity, with pinned live
// leaves mixed into the same window. Background reclamation stays disabled so
// the test can inspect the queue and the benchmark can restore its population.
func ghostWindowTrie(tb testing.TB) *Critbit[struct{}] {
	tb.Helper()

	c := NewCritbit[struct{}]()

	tb.Cleanup(func() { closeTestTrie(tb, c) })
	c.reapRunning.Store(true)
	c.SetEvictHooks(nil, nil, nil)

	tips := NewTipSet(tip(1))

	for i := range 4096 {
		key := fmt.Sprintf("key:%04d", i)
		c.Insert(key, nil, tips)
		c.liveLeaf(key).lastAccess.Store(uint64(i + 1))
	}

	if got := c.EvictBatch(512); got != 512 {
		tb.Fatalf("initial eviction = %d, want 512", got)
	}

	if c.ghostCount.Load() <= minSweep {
		tb.Fatal("setup did not exceed stack scratch capacity")
	}

	// Discard the initial full deletes from the queue without unlinking them.
	// The sweep skips these slots; the next queued leaf must be a ghost reclaim.
	if queue := c.reapQueue.Load(); queue != nil {
		for {
			if _, ok := queue.TryDequeue(); !ok {
				break
			}
		}
	}

	c.Range(func(key string) bool {
		if !c.Pin(key) {
			tb.Fatalf("could not pin %q", key)
		}

		return true
	})

	return c
}

func TestEvictGhostWindowPreservesPinnedLeaves(t *testing.T) {
	c := ghostWindowTrie(t)
	ghosts, live := c.ghostCount.Load(), c.Size()

	// Every call must choose the next oldest ghost, leaving the pinned keys
	// intact. This exercises sorted ghost consumption beyond the stack buffer.
	for i := range 3 {
		c.evictHand = 0

		if got := c.EvictBatch(512); got != 1 {
			t.Fatalf("ghost reclaim = %d, want 1", got)
		}

		leaf, ok := c.reapQueue.Load().TryDequeue()
		if !ok || leaf.key != fmt.Sprintf("key:%04d", i) {
			t.Fatalf("reclaim %d did not select the oldest ghost", i)
		}

		if c.Size() != live || c.ghostCount.Load() != ghosts-int64(i+1) {
			t.Fatal("ghost reclaim changed the live set or miscounted ghosts")
		}
	}
}

func BenchmarkEvictGhostWindow(b *testing.B) {
	c := ghostWindowTrie(b)
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		c.evictHand = 0

		if got := c.EvictBatch(512); got != 1 {
			b.Fatalf("ghost reclaim = %d, want 1", got)
		}

		leaf, ok := c.reapQueue.Load().TryDequeue()
		if !ok {
			b.Fatal("reclaimed ghost was not queued")
		}

		// Restore the same ghost after the batch, without allocating a new leaf
		// or changing the sample's recency and occupancy distribution.
		leaf.freq.Store(-1)
		c.ghostCount.Add(1)
		c.deletedCount.Add(-1)
	}
}
