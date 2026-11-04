package keytrie

import (
	"strings"
	"testing"
	"unsafe"

	"github.com/swytchdb/engine/internal/memsize"
)

func TestMemoryFrontiersPayloadsAndDeadKeys(t *testing.T) {
	type payload [80]byte

	c := NewCritbit[payload]()
	// Measure a deleted-but-still-linked slot before physical unlink clears it.
	c.reapRunning.Store(true)
	defer c.reapRunning.Store(false)

	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	})

	key := strings.Repeat("key", 200)

	first := NewTipSet(tip(1))
	if _, ok := c.Insert(key, nil, first); !ok {
		t.Fatal("insert failed")
	}

	leaf := c.findBestMatch(c.root.Load(), key)
	charge := func() int64 { return c.leafArena.list.Load().extra[leaf.chunkIdx].Load() }

	initial := memsize.Alloc(int64(len(key))) + tipSetBytes(first)
	if charge() != initial {
		t.Fatalf("key/frontier charge = %d, want %d", charge(), initial)
	}

	if _, ok := c.Insert(key, NewTipSet(tip(999)), NewTipSet(tip(2))); ok {
		t.Fatal("stale CAS succeeded")
	}

	if charge() != initial {
		t.Fatal("failed CAS changed accounting")
	}

	refs := make([]EffectRef, 100)
	for i := range refs {
		refs[i] = tip(uint64(i + 10))
	}

	large := NewTipSet(refs...)
	if _, ok := c.Insert(key, first, large); !ok {
		t.Fatal("frontier replacement failed")
	}

	data := &payload{}
	c.LoadOrStoreData(key, data)

	expect := memsize.Alloc(int64(len(key))) + tipSetBytes(large) + memsize.Alloc(int64(unsafe.Sizeof(*data)))
	if charge() != expect {
		t.Fatalf("growing frontier/payload charge=%d want=%d", charge(), expect)
	}

	c.LoadOrStoreData(key, &payload{})

	if charge() != expect {
		t.Fatal("duplicate payload charged twice")
	}

	if c.DeleteAndSnapshot(key, large) == nil {
		t.Fatal("delete failed")
	}

	if charge() != memsize.Alloc(int64(len(key))) {
		t.Fatal("dead slot lost key charge or retained removed frontier/payload charge")
	}

	if c.ResidentBytes() < c.ArenaBytes()+charge() {
		t.Fatal("resident budget omitted allocated arena capacity")
	}
}

func TestMemoryGhostRetainsKeyCapacity(t *testing.T) {
	c := NewCritbit[struct{}]()

	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	})
	c.SetEvictHooks(func(string) bool { return true }, nil, nil)

	key := strings.Repeat("ghost", 100)
	c.Insert(key, nil, NewTipSet(tip(1)))

	leaf := c.findBestMatch(c.root.Load(), key)
	if c.EvictBatch(1) != 1 {
		t.Fatal("no victim")
	}

	got := c.leafArena.list.Load().extra[leaf.chunkIdx].Load()
	if got != memsize.Alloc(int64(len(key))) {
		t.Fatalf("ghost retained %d bytes, want key capacity", got)
	}
}

func TestMemoryRecycledChunkDropsOldKeyCharges(t *testing.T) {
	c := NewCritbit[int]()

	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	})
	// Small chunks exercise whole-chunk reclamation without changing global
	// constants or relying on background reaper scheduling.
	c.leafArena.chunkSize, c.internalArena.chunkSize = 2, 2

	c.reapRunning.Store(true)
	defer c.reapRunning.Store(false)

	for _, key := range []string{"a", "b", "c"} {
		c.Insert(key, nil, NewTipSet(tip(1)))
	}

	stale := c.findBestMatch(c.root.Load(), "a")
	// Public readers escape immutable results, never the arena node itself.
	escapedKey, escapedTips := stale.key, c.Contains("a")
	for _, key := range []string{"a", "b"} {
		c.Delete(key, c.Contains(key))
	}

	for c.drainReap() > 0 {
	}

	if got := c.leafArena.list.Load().extra[0].Load(); got != 0 {
		t.Fatalf("recycled chunk retained %d bytes of old keys", got)
	}

	if escapedKey != "a" || !escapedTips.Contains(tip(1)) {
		t.Fatal("reclamation mutated an escaped reader result")
	}

	c.Insert("fresh", nil, NewTipSet(tip(2)))

	if got := c.leafArena.list.Load().extra[0].Load(); got != memsize.Alloc(5)+tipSetBytes(NewTipSet(tip(2))) {
		t.Fatalf("fresh recycled slot charge=%d", got)
	}
}
