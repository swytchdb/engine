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

func TestSweepBudgetIncludesGhosts(t *testing.T) {
	c := NewCritbit[struct{}]()

	t.Cleanup(func() { closeTestTrie(t, c) })
	c.SetEvictHooks(func(string) bool { return true }, nil, func() bool { return true })

	const count = 4096
	for i := range count {
		c.Insert(fmt.Sprintf("ghost-budget:%d", i), nil, NewTipSet(tip(uint64(i+1))))
	}

	// Leave one live key beyond the first window. All other occupied slots
	// are ghosts; they must consume the budget even though they hold no value.
	liveKey := fmt.Sprintf("ghost-budget:%d", count-1)

	for _, chunk := range c.leafArena.list.Load().chunks {
		if chunk == nil {
			continue
		}

		for i := range chunk.nodes {
			n := &chunk.nodes[i]
			if n.key != "" && n.key != liveKey {
				n.deleted.Store(true)
				n.freq.Store(-1)
				c.clearLeaf(n, n.tips.Swap(nil))
				c.ghostCount.Add(1)
			}
		}
	}

	c.evictMu.Lock()
	c.reapMu.RLock()

	seen := 0

	c.sweepLeaves(minSweep, func(*critNode[struct{}]) { seen++ })
	c.reapMu.RUnlock()
	c.evictMu.Unlock()

	if seen != minSweep {
		t.Fatalf("sweep visited %d occupied leaves, want budget %d", seen, minSweep)
	}

	if c.EvictBatch(1) != 1 {
		t.Fatal("ghost-only window made no progress")
	}

	if c.Contains(liveKey) == nil {
		t.Fatal("ghost reclaim evicted the unrelated live key")
	}
}
