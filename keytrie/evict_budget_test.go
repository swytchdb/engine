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
				c.releaseLeafMemory(n, n.tips.Swap(nil), nil)
				c.size.Add(-1)
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
