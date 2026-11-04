package effects

import (
	"maps"
	"runtime/metrics"
	"strings"
	"unsafe"

	"github.com/swytchdb/engine/memsize"
)

// MemoryStats separates cache ownership from GC/OS residency. Capacity estimates
// include allocator rounding and conservative map overhead; they are not an RSS
// measurement. Maintenance queue buffers are excluded; primary state awaiting
// deferred release remains charged. Snapshots during mutations may span several
// transitions.
type MemoryStats struct {
	ResidentBytes, EffectBytes, IndexBytes, CachedDagBytes           int64
	SubscriberBytes, MetadataBytes, DeferredBytes, KeyBytes          int64
	KeyFilterBytes                                                   int64 // independent cluster hints; excluded from ResidentBytes
	CacheTargetBytes, GoMemoryLimitBytes                             int64
	LiveHeapBytes, HeapObjectsBytes, HeapFreeBytes, RuntimeUsedBytes int64
	GCCycles                                                         uint64
	UnderPressure                                                    bool
}

type runtimeMemory struct {
	live, objects, free, used int64
	cycles                    uint64
}

func readRuntimeMemory() runtimeMemory {
	s := [...]metrics.Sample{
		{Name: "/gc/heap/live:bytes"},
		{Name: "/memory/classes/heap/objects:bytes"},
		{Name: "/memory/classes/heap/free:bytes"},
		{Name: "/memory/classes/total:bytes"},
		{Name: "/memory/classes/heap/released:bytes"},
		{Name: "/gc/cycles/total:gc-cycles"},
	}
	metrics.Read(s[:])

	return runtimeMemory{
		live: int64(s[0].Value.Uint64()), objects: int64(s[1].Value.Uint64()),
		free: int64(s[2].Value.Uint64()),
		used: int64(s[3].Value.Uint64()) - int64(s[4].Value.Uint64()), cycles: s[5].Value.Uint64(),
	}
}

func (e *Engine) residentMemory() MemoryStats {
	s := MemoryStats{CacheTargetBytes: e.cacheTarget, GoMemoryLimitBytes: e.goMemoryLimit}
	if e.effectCache != nil {
		s.EffectBytes = e.effectCache.Bytes()
	}

	if e.index != nil {
		s.IndexBytes = e.index.ResidentBytes()
	}

	s.CachedDagBytes = e.cachedDagBytes.Load()
	s.SubscriberBytes = e.subscriberBytes.Load()
	// These maps shrink with xsync. Charge per-entry bucket slack plus state/
	// channel headers. Pending transaction contents and transport/read buffers
	// use transient headroom and Go's collector, rather than cache eviction.
	s.MetadataBytes = int64(unsafe.Sizeof(*e)) + int64(e.subscriptions.Size())*256 + int64(e.serializationState.Size())*128
	e.releaseMu.Lock()

	// Queue storage is maintenance overhead. Only primary state still owned by
	// queued work contributes here; shared keys/DAGs/effects remain charged by
	// their existing owners until drainPendingReleases relinquishes them.
	for _, item := range e.releaseItems {
		s.DeferredBytes += memsize.Alloc(int64(cap(item.tips)) * 16)
		if item.ls != nil {
			s.DeferredBytes += memsize.Alloc(int64(unsafe.Sizeof(leafState{})))
		}

		if !item.ownedKey {
			s.DeferredBytes += memsize.Alloc(int64(len(item.key)))
		}
	}
	e.releaseMu.Unlock()

	e.keyMemoryMu.Lock()
	s.KeyBytes = e.keyBytes + memsize.Map(e.keyHighWater, 40)
	e.keyMemoryMu.Unlock()

	s.ResidentBytes = s.KeyBytes + s.EffectBytes + s.IndexBytes + s.CachedDagBytes + s.SubscriberBytes + s.MetadataBytes + s.DeferredBytes
	s.UnderPressure = e.memTarget > 0 && s.ResidentBytes > e.cacheTarget

	return s
}

// MemoryStats exposes both signals without materializing effects or walking DAGs.
func (e *Engine) MemoryStats() MemoryStats {
	s := e.residentMemory()
	r := readRuntimeMemory()
	s.LiveHeapBytes, s.HeapObjectsBytes, s.HeapFreeBytes, s.RuntimeUsedBytes, s.GCCycles = r.live, r.objects, r.free, r.used, r.cycles
	s.KeyFilterBytes = e.keyFilterBytes()

	return s
}

func (e *Engine) wakeMemoryGovernor() {
	if e.memWake != nil {
		select {
		case e.memWake <- struct{}{}:
		default:
		}
	}
}

func (e *Engine) dropSubscription(key string) {
	if state, ok := e.subscriptions.LoadAndDelete(key); ok && state.ownedKey {
		e.releaseKey(key)
	}
}

func (e *Engine) configureIndexKeyOwnership() {
	e.index.SetKeyOwnership(e.retainKey, e.releaseKey)
}

// A subscription, trie leaf (including ghosts), and pending release share one
// key allocation. Keep it charged until the last primary owner releases it.
func (e *Engine) retainKey(key string) string {
	if key == "" {
		return ""
	}

	e.keyMemoryMu.Lock()
	defer e.keyMemoryMu.Unlock()

	if e.keyOwners == nil {
		e.keyOwners = make(map[string]keyOwner)
	}
	// Return the canonical string, not the caller's potentially borrowed buffer.
	entry, ok := e.keyOwners[key]
	if !ok {
		entry.key = strings.Clone(key)
		e.keyBytes += memsize.Alloc(int64(len(key)))
	}

	entry.refs++
	e.keyOwners[entry.key] = entry
	e.keyHighWater = max(e.keyHighWater, len(e.keyOwners))

	return entry.key
}

func (e *Engine) releaseKey(key string) {
	if key == "" {
		return
	}

	e.keyMemoryMu.Lock()
	defer e.keyMemoryMu.Unlock()

	if entry := e.keyOwners[key]; entry.refs > 1 {
		entry.refs--
		e.keyOwners[entry.key] = entry
	} else if entry.refs == 1 {
		delete(e.keyOwners, key)

		e.keyBytes -= memsize.Alloc(int64(len(key)))
		if len(e.keyOwners)*4 < e.keyHighWater {
			shrunk := make(map[string]keyOwner, len(e.keyOwners))
			maps.Copy(shrunk, e.keyOwners)

			e.keyOwners = shrunk
			e.keyHighWater = len(e.keyOwners)
		}
	}
}

// Cluster filters are monotonic hints, with a lifetime independent of local
// primary-cache eviction. Report them separately, outside the governor path.
func (e *Engine) keyFilterBytes() int64 {
	e.keyFilterMu.RLock()
	defer e.keyFilterMu.RUnlock()

	n := e.ownKeyFilter.residentBytes() + memsize.Alloc(int64(cap(e.ownFilterBytes))) + memsize.Alloc(int64(cap(e.peerKeyFilters))*int64(unsafe.Sizeof(peerKeyFilter{})))
	for i := range e.peerKeyFilters {
		filter := &e.peerKeyFilters[i]
		n += filter.bulk.residentBytes() + filter.realtime.residentBytes()
	}

	return n
}

func (cc *CuckooChain) residentBytes() int64 {
	n := memsize.Alloc(int64(cap(cc.sealed)) * 8)
	for _, filter := range cc.sealed {
		n += memsize.Alloc(int64(unsafe.Sizeof(*filter))) + memsize.Alloc(int64(cap(filter.buckets))*2)
	}

	if cc.active != nil {
		n += memsize.Alloc(int64(unsafe.Sizeof(*cc.active))) + memsize.Alloc(int64(cap(cc.active.buckets))*2)
	}

	return n
}

type keyOwner struct {
	key  string
	refs int
}
