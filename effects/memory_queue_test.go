package effects

import (
	"runtime/debug"
	"strings"
	"testing"
	"unsafe"

	"github.com/swytchdb/engine/memsize"
)

func TestMemoryMaintenanceQueuesCannotCausePrimaryEviction(t *testing.T) {
	previous := debug.SetMemoryLimit(1 << 30)

	t.Cleanup(func() { debug.SetMemoryLimit(previous) })

	// The trie's maintenance queues allocate over 14 MB independently of the
	// primary budget. A small value and its indexes still fit in 1 MiB.
	const budget = int64(1 << 20)

	e := NewEngine(EngineConfig{DefaultMode: UnsafeMode, MemoryLimit: budget})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})

	close(e.memGovStop)
	e.memGovWg.Wait()
	e.memGovStop = nil

	value := strings.Repeat("v", 64<<10)

	c := e.NewContext()
	if err := c.Emit(scalarSet("small-primary", value)); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	before := e.residentMemory()
	if before.ResidentBytes >= budget {
		t.Fatalf("maintenance storage consumed the primary budget: %+v", before)
	}

	e.evictToTarget(budget)

	if e.KeyCount() != 1 || e.effectCache.ColdEvictions() != 0 {
		t.Fatal("maintenance queues caused primary eviction")
	}

	result, _, _, err := e.GetSnapshot("small-primary")
	if err != nil || string(result.GetScalar().GetRaw()) != value {
		t.Fatalf("cached value changed: %v", err)
	}
}

func TestMemoryReleaseBufferCapacityExcludedButOwnedStateCharged(t *testing.T) {
	e := NewEngine(EngineConfig{DefaultMode: UnsafeMode})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})

	before := e.residentMemory()
	e.releaseItems = make([]evictedKey, 0, 1<<16)
	e.releaseSpare = make([]evictedKey, 0, 1<<16)

	if after := e.residentMemory(); after.ResidentBytes != before.ResidentBytes {
		t.Fatalf("empty maintenance buffers changed primary occupancy: before=%+v after=%+v", before, after)
	}

	key := e.retainKey("queued-primary")
	tips := make([]Tip, 1, 32)
	ls := &leafState{}
	e.releaseItems = append(e.releaseItems, evictedKey{key: key, tips: tips, ls: ls, ownedKey: true})

	want := memsize.Alloc(int64(cap(tips))*int64(unsafe.Sizeof(Tip{}))) + memsize.Alloc(int64(unsafe.Sizeof(*ls)))
	if got := e.residentMemory().DeferredBytes; got != want {
		t.Fatalf("deferred primary state charge=%d want=%d", got, want)
	}

	e.drainPendingReleases()

	if got := e.residentMemory().DeferredBytes; got != 0 {
		t.Fatalf("drained maintenance buffers retained primary charge: %d", got)
	}

	if len(e.keyOwners) != 0 {
		t.Fatal("draining retained ownership of the key")
	}
}
