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
	"slices"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/puzpuzpuz/xsync/v4"
)

// freeSlotCapacity bounds the per-arena recycled-slot free list. Recycling that
// would overflow it falls back to dropping the dead chunk to nil (the old
// behavior), so this only caps how much eviction churn the allocator can buffer,
// never correctness. 64Ki slots ≈ 32 chunks.
const freeSlotCapacity = 1 << 16

// arenaChunkList is an immutable directory. Growth and recycling publish a new
// directory; existing entries share their nodes and counters until retired.
type arenaChunkList[T any] struct {
	chunks []*arenaChunk[T]
}

// arenaChunk owns one generation of slots and its accounting. Recycling creates
// a new chunk, so an old directory never observes a new generation's counters.
type arenaChunk[T any] struct {
	nodes       []T
	reaped      atomic.Int64
	outstanding atomic.Int64 // recycled slots still waiting in the free list
	extra       atomic.Int64 // keys, tip sets and payload headers
}

type critbitArena[T any] struct {
	next      atomic.Uint64
	chunkSize uint64
	growMu    sync.Mutex // serializes directory growth and chunk recycling
	list      atomic.Pointer[arenaChunkList[T]]
	// freeList recycles the slots of fully-reaped chunks so the arena's footprint
	// tracks the live working set instead of the all-time high-water mark — without
	// it, monotonic next climbs forever under key churn and the eviction sweep
	// walks an ever-larger arena of dead slots. It holds reusable slot indices;
	// alloc drains it before bumping next. freeCount mirrors its occupancy (it has
	// no Size) so dropChunk can pre-check room for a whole chunk before committing
	// a recycle. MPMC: many allocators dequeue, dropChunk enqueues under growMu.
	// Reuse is safe because recycling installs a brand-new chunk array — its slots
	// have never been published, so no reader can hold a pointer into
	// them, while the dead array goes to GC exactly as the nil path does.
	freeList  *xsync.MPMCQueue[uint64]
	freeCount atomic.Int64
	freeCap   uint64
	// initNode, if set, runs on every node of a freshly-grown chunk before that
	// chunk is published in list. The leaf arena uses it to stamp deleted=true on
	// every slot, so the eviction sweep (which scans the arena directly) skips a
	// slot until allocLeafNode + Insert have written and published a real leaf.
	initNode func(*T)
}

func (a *critbitArena[T]) init(chunkSize int, initNode func(*T)) {
	if chunkSize < 1 {
		chunkSize = 1
	}

	a.chunkSize = uint64(chunkSize)
	a.initNode = initNode
	a.freeCap = freeSlotCapacity
	a.freeList = xsync.NewMPMCQueue[uint64](freeSlotCapacity)
	a.list.Store(&arenaChunkList[T]{})
}

func (a *critbitArena[T]) alloc() (*T, uint32) {
	// Reuse a recycled slot first, so the arena's footprint tracks live keys
	// rather than the all-time high-water mark. A recycled index points into a
	// fresh chunk dropChunk installed (and published in list before enqueuing the
	// index), so the load below sees it. Every allocator holds the trie's reap
	// lock (read or write) from claim through publish, so a compaction pass —
	// which skips chunks with outstanding recycled slots — can never evacuate the
	// chunk out from under a half-initialized slot.
	if idx, ok := a.freeList.TryDequeue(); ok {
		a.freeCount.Add(-1)
		chunkIdx := idx / a.chunkSize
		list := a.list.Load()
		chunk := list.chunks[chunkIdx]
		chunk.outstanding.Add(-1)

		return &chunk.nodes[idx%a.chunkSize], uint32(chunkIdx)
	}

	// Fast path: atomically claim a fresh slot index. No lock needed.
	idx := a.next.Add(1) - 1
	chunkIdx := idx / a.chunkSize
	offset := idx % a.chunkSize

	list := a.list.Load()
	if chunkIdx >= uint64(len(list.chunks)) {
		// Slow path: need a new chunk. Only one goroutine allocates;
		// others wait on the mutex then find the chunk already exists.
		a.grow(chunkIdx)
		list = a.list.Load()
	}

	return &list.chunks[chunkIdx].nodes[offset], uint32(chunkIdx)
}

// recordReaped notes that one node in chunkIdx is permanently dead — unlinked
// from the tree by the reaper (write lock) or orphaned by a lost insert race
// (read lock). Each slot is accounted at most once: unlink is identity-checked
// and an orphan is counted exactly where it is abandoned. When every slot in
// the chunk is accounted, the chunk is dropped so GC can reclaim it. The drop
// is safe from either lock: the counter is shared across list snapshots, grow
// and drop serialize on growMu, and a concurrent sweep iterating a dropped
// chunk's memory sees only deleted slots (a droppable chunk holds no ghosts —
// ghosts are never accounted).
func (a *critbitArena[T]) recordReaped(chunkIdx uint32) {
	list := a.list.Load()
	if int(chunkIdx) >= len(list.chunks) || list.chunks[chunkIdx] == nil {
		return
	}

	if list.chunks[chunkIdx].reaped.Add(1) == int64(a.chunkSize) {
		a.dropChunk(chunkIdx)
	}
}

// dropChunk handles a fully-unlinked chunk. The dead array is always released to
// GC after its detached fields are cleared and list releases the array.
//
// When the free list has room for a whole chunk, it RECYCLES: a fresh empty chunk
// takes the slot's place and its pristine slots return to the free list for
// reuse, so the arena stops growing under churn. The fresh array's slots have
// never been published, so no reader can be mid-read on them — reuse is safe. The
// reaped counter is reset for the new generation, and outstanding is armed so
// compaction skips the chunk until every recycled slot has been re-allocated.
// Otherwise it falls back to the old behavior: a nil placeholder (indices stay
// stable; monotonic next never revisits a nil'd chunk).
//
// Its sole caller is recordReaped's counter hitting chunkSize — every drop is
// counter-driven, and per-slot at-most-once accounting means it fires exactly
// once per chunk generation. growMu serializes concurrent drops of different
// chunks, so the free list has a single logical enqueuer and the room pre-check
// below guarantees every TryEnqueue succeeds (freeCount is decremented by
// allocators only after removal, so it never understates occupancy).
func (a *critbitArena[T]) dropChunk(chunkIdx uint32) {
	a.growMu.Lock()
	defer a.growMu.Unlock()

	list := a.list.Load()
	if int(chunkIdx) >= len(list.chunks) || list.chunks[chunkIdx] == nil {
		return
	}

	newChunks := slices.Clone(list.chunks)

	if a.freeCount.Load()+int64(a.chunkSize) <= int64(a.freeCap) {
		fresh := a.newChunk()
		newChunks[chunkIdx] = fresh
		fresh.outstanding.Store(int64(a.chunkSize))
		// Publish the fresh chunk before its slots become reachable via the free
		// list, so a concurrent alloc that dequeues an index sees the new array.
		a.list.Store(&arenaChunkList[T]{chunks: newChunks})

		a.freeCount.Add(int64(a.chunkSize))

		base := uint64(chunkIdx) * a.chunkSize
		for i := uint64(0); i < a.chunkSize; i++ {
			a.freeList.TryEnqueue(base + i) // room pre-checked: cannot fail
		}

		return
	}

	newChunks[chunkIdx] = nil
	a.list.Store(&arenaChunkList[T]{chunks: newChunks})
}

func (a *critbitArena[T]) newChunk() *arenaChunk[T] {
	chunk := &arenaChunk[T]{nodes: make([]T, a.chunkSize)}
	if a.initNode != nil {
		for i := range chunk.nodes {
			a.initNode(&chunk.nodes[i])
		}
	}

	return chunk
}

func (a *critbitArena[T]) grow(needed uint64) {
	a.growMu.Lock()
	defer a.growMu.Unlock()

	list := a.list.Load()
	if needed < uint64(len(list.chunks)) {
		return
	}

	chunks := make([]*arenaChunk[T], needed+1)
	copy(chunks, list.chunks)

	for i := len(list.chunks); i < len(chunks); i++ {
		chunks[i] = a.newChunk()
	}

	a.list.Store(&arenaChunkList[T]{chunks: chunks})
}

// clear runs under the trie's exclusive reap lock, which already excludes all
// arena writers. Taking growMu here would protect the same ownership twice.
func (a *critbitArena[T]) clear() {
	a.list.Store(&arenaChunkList[T]{})
	a.next.Store(0)
	// Drain stale recycled indices: they point into chunks the reset just dropped,
	// so a later alloc must not hand them out. clear assumes no concurrent alloc
	// (same as the list/next reset above).
	if a.freeList != nil {
		for {
			if _, ok := a.freeList.TryDequeue(); !ok {
				break
			}
		}

		a.freeCount.Store(0)
	}
}

// bytes returns the backing-array footprint of the arena: the sum of the
// node-struct bytes across every live (non-dropped) chunk. This is the
// contiguous slot memory only — the key strings and TipSets the nodes point
// at live on the heap and are accounted elsewhere. A dropped chunk is a nil
// entry and contributes nothing, so this shrinks exactly when dropChunk fires.
func (a *critbitArena[T]) bytes() int64 {
	list := a.list.Load()
	if list == nil {
		return 0
	}

	var node T

	nodeSize := int64(unsafe.Sizeof(node))

	var total int64

	for _, chunk := range list.chunks {
		if chunk != nil {
			total += int64(len(chunk.nodes)) * nodeSize
		}
	}

	return total
}
