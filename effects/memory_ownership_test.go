package effects

import (
	"fmt"
	"strings"
	"testing"
	"unsafe"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
)

func TestMemorySharedPrimaryKeyOwners(t *testing.T) {
	e := NewEngine(EngineConfig{DefaultMode: UnsafeMode})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})

	key := strings.Repeat("shared", 100)
	owned := e.retainKey(key)
	e.subscriptions.Store(owned, &subscriptionState{ownedKey: true})
	e.index.Insert(key, nil, keytrie.NewTipSet(Tip{1, 1}))
	e.index.Range(func(indexKey string) bool {
		if unsafe.StringData(indexKey) != unsafe.StringData(owned) {
			t.Fatal("subscription and index did not share key allocation")
		}

		return true
	})
	before := e.keyBytes
	e.dropSubscription(key)

	if e.keyBytes != before {
		t.Fatal("subscription release credited the trie's key")
	}

	e.index.SetEvictHooks(func(string) bool { return true }, nil, nil)

	if e.index.EvictBatch(1) != 1 {
		t.Fatal("no eviction")
	}

	if e.keyBytes != before {
		t.Fatal("ghost lost its key charge")
	}

	if err := e.index.Close(); err != nil {
		t.Fatal(err)
	}

	if e.keyBytes != 0 || len(e.keyOwners) != 0 {
		t.Fatal("last key owner was not released")
	}
}

func TestMemoryClusterFiltersHaveIndependentBudget(t *testing.T) {
	e := NewEngine(EngineConfig{DefaultMode: UnsafeMode})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})

	before := e.MemoryStats()

	for i := range 5000 {
		key := fmt.Sprintf("filter-only:%d", i)
		e.ownFilterAdd(key)
		e.peerFilterAdd(pb.NodeID(123), key)
	}

	e.OwnKeyFilterSnapshot()

	after := e.MemoryStats()
	if after.ResidentBytes != before.ResidentBytes {
		t.Fatal("independent filters reduced primary capacity")
	}

	if after.KeyFilterBytes <= before.KeyFilterBytes {
		t.Fatal("filter observation did not grow")
	}
}
