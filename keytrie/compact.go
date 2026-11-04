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
	"log/slog"
	"sync/atomic"
	"unsafe"
)

// Arena compaction. Arena slots are never reused in place — a chunk is
// reclaimed (and its slot range recycled through the free list) only when every
// one of its 2048 slots has individually died. Under evict/reinsert churn
// survivors scatter, so one live leaf (or ghost) pins 160KB indefinitely and
// the arena high-water ratchets at the churn rate. Compaction evacuates sparse
// chunks: live nodes are copied to fresh slots and spliced in under the reap
// write lock, dead ones are unlinked, and every disposed slot is accounted
// through recordReaped so the chunk's counter — the single drop trigger —
// fires exactly once when the last slot dies. Compaction never drops a chunk
// directly: a force-drop racing the counter would recycle a chunk while its
// old generation was still being accounted, corrupting the new generation's
// counter and double-enqueuing free-list indices.
//
// Relocation safety rests on the existing lock discipline:
//   - Every leaf-field mutator (Insert, RemoveTips, LoadOrStoreData,
//     DeleteAndSnapshot, the eviction sweep) holds the reap read-lock for its
//     whole critical section, so the write lock excludes all of them.
//   - Splices are identity-checked by descent, exactly like unlinkLeaf: a slot
//     whose key path resolves to a different node is an orphan husk and is
//     skipped, and stale reapQueue entries no-op the same way.
//   - Readers copy their result under the reap read lock. The write lock gives
//     relocation a grace period to clear the detached slot's references. Range
//     cursors restart on readRevision changes before reading an old path.
//   - TipSets and leaf payloads move by pointer, so escaped *T references and
//     tip refcounts are untouched; no refDelta fires for a relocation.
const (
	// compactOccupancyPct: a chunk whose linked slots are at or below this
	// percentage of chunkSize is an evacuation candidate. Evacuating costs one
	// fresh slot per survivor, so the threshold is the worst-case copy overhead
	// (25% → move ≤512 nodes to free 2048 slots).
	compactOccupancyPct = 25

	// compactMaxChunksPerPass bounds one pass's evacuation work per arena so a
	// badly fragmented trie converges over several reap cycles instead of one
	// long stall.
	compactMaxChunksPerPass = 256
)

// compactMinWasteBytes gates compaction entirely while the reclaimable waste
// is small — scanning the arenas for victims is not free. A var so tests can
// lower it.
var compactMinWasteBytes = int64(32 << 20)

// maybeCompact runs a bounded compaction pass when the arenas hold
// substantially more slots than the tree has linked nodes. Called from the
// reap goroutine (serialized by reapRunning), so it never runs concurrently
// with itself, drainReap, or the full-tree reap.
func (c *Critbit[T]) maybeCompact() {
	if c.closed.Load() {
		return
	}

	var node critNode[T]

	nodeSize := int64(unsafe.Sizeof(node))
	arenaBytes := c.leafArena.bytes() + c.internalArena.bytes()
	// Linked estimate: live + deleted-pending + ghost leaves, and one internal
	// per leaf (a crit-bit tree has exactly leaves-1 internals).
	linked := 2 * (c.size.Load() + c.deletedCount.Load() + c.ghostCount.Load()) * nodeSize

	waste := arenaBytes - linked
	if waste < compactMinWasteBytes || waste*4 < arenaBytes {
		return
	}

	leafDropped := c.compactLeafChunks(compactMaxChunksPerPass)

	internalDropped := c.compactInternalChunks(compactMaxChunksPerPass)
	if leafDropped+internalDropped > 0 {
		slog.Debug("keytrie: compacted arenas",
			"leaf_chunks", leafDropped, "internal_chunks", internalDropped,
			"arena_bytes", c.leafArena.bytes()+c.internalArena.bytes())
	}
}

// compactLeafChunks evacuates up to maxChunks sparse leaf chunks and returns
// how many were dropped. Victim selection is slot-driven (a leaf's liveness is
// readable off the slot) and advisory — occupancy is re-read under the write
// lock by the per-slot handling, so a chunk that gained live leaves since the
// scan is still evacuated correctly, just less profitably.
func (c *Critbit[T]) compactLeafChunks(maxChunks int) int {
	list := c.leafArena.list.Load()
	chunks, chunkSize := list.chunks, c.leafArena.chunkSize
	// The tail chunk is still receiving allocations (and is where evacuated
	// survivors land); never evacuate it.
	tail := int(c.leafArena.next.Load() / chunkSize)
	threshold := int(chunkSize) * compactOccupancyPct / 100

	evacuated := 0
	for ci := 0; ci < len(chunks) && ci < tail && evacuated < maxChunks; ci++ {
		chunk := chunks[ci]
		if chunk == nil {
			continue
		}
		// A recycled chunk with slots still waiting in the free list looks empty
		// (pristine slots are born deleted) but is about to be refilled by
		// allocations — evacuating it would relocate its few residents for
		// nothing, and its counter can't fill until every recycled slot has been
		// handed out anyway.
		if chunk.outstanding.Load() > 0 {
			continue
		}

		live := 0

		for i := range chunk.nodes {
			if !chunk.nodes[i].deleted.Load() {
				live++
			}
		}

		if live > threshold {
			continue
		}

		if c.evacuateLeafChunk(chunk.nodes) {
			evacuated++
		}
	}

	return evacuated
}

// evacuateLeafChunk empties one leaf chunk under a single reap write-lock hold.
// Live leaves are relocated, dead-pending leaves are unlinked, ghosts forfeit
// their warm-restart hint (a ghost stranded in a near-empty chunk is stale
// anyway), and orphan husks need nothing — the identity-checked descent skips
// them, and their slots were already accounted at abandonment. Every disposal
// here flows through recordReaped (unlinkLeaf does it on success, relocateLeaf
// on splice), so the chunk's counter reaches chunkSize exactly when the last
// slot dies and dropChunk fires from inside recordReaped — possibly mid-loop,
// which is safe: a full counter means every remaining slot is an
// already-accounted husk whose disposal below is a no-op.
func (c *Critbit[T]) evacuateLeafChunk(chunk []critNode[T]) bool {
	c.reapMu.Lock()
	defer c.reapMu.Unlock()

	if c.closed.Load() {
		return false
	}

	for i := range chunk {
		n := &chunk[i]
		if n.deleted.Load() {
			if n.freq.Load() < 0 {
				// Ghost: shed ghost status (unlinkLeaf refuses freq < 0), then
				// unlink. Ghosts were never counted in deletedCount, so balance
				// unlinkLeaf's decrement up front and revert if it wasn't linked.
				c.deletedCount.Add(1)
				n.freq.Store(0)

				if c.unlinkLeaf(n) {
					c.ghostCount.Add(-1)
				} else {
					c.deletedCount.Add(-1)
				}
			} else {
				// Deleted-pending (its reapQueue entry will no-op later) or an
				// orphan (identity check fails, no-op now).
				c.unlinkLeaf(n)
			}

			continue
		}

		c.relocateLeaf(n)
	}

	return true
}

// relocateLeaf copies a live leaf into a fresh slot and splices it in place of
// the original. Caller holds the reap write lock. The detached slot's references
// are cleared after the splice; readers have finished copying their result.
// Its slot is accounted right here so
// the chunk's counter can fire the drop. Returns false when the slot isn't
// actually linked (an orphan husk, accounted at abandonment).
func (c *Critbit[T]) relocateLeaf(old *critNode[T]) bool {
	root := c.root.Load()
	if root == nil {
		return false
	}

	var (
		p    *critNode[T]
		pDir int
	)

	cur := root
	for !cur.isLeaf {
		dir := getDirection(old.key, cur.bytePos, cur.otherbits)
		p, pDir = cur, dir

		next := cur.child[dir].Load()
		if next == nil {
			return false
		}

		cur = next
	}

	if cur != old {
		return false // an orphan husk; a different leaf owns this key path
	}

	n2, ci := c.leafArena.alloc() // born deleted=true via the arena init hook
	n2.chunkIdx = ci

	n2.key = old.key
	if c.retainKey != nil {
		n2.key = c.retainKey(n2.key)
	}

	n2.pinned = old.pinned
	n2.pins.Store(old.pins.Load())
	n2.tips.Store(old.tips.Load())
	n2.data.Store(old.data.Load())
	c.leafArena.addExtra(ci, c.keyBytes(n2.key)+tipSetBytes(n2.tips.Load())+payloadBytes(n2.data.Load()))
	n2.freq.Store(old.freq.Load())
	n2.lastAccess.Store(old.lastAccess.Load())
	n2.lastDecayEpoch.Store(old.lastDecayEpoch.Load())
	// Publish before splicing: unreachable until the splice, and the write lock
	// excludes scanners until the relocated leaf is ready.
	n2.deleted.Store(false)

	if p == nil {
		c.root.Store(n2)
	} else {
		p.child[pDir].Store(n2)
	}

	c.readRevision++
	c.retireLeaf(old)

	return true
}

// compactInternalChunks evacuates up to maxChunks sparse internal chunks.
// The arena's retired-slot counters identify sparse, fully allocated chunks.
// One write-lock DFS relocates selected nodes before visiting their children.
// Children therefore always splice through their current parent. Each
// relocated husk is accounted through recordReaped, which fires the drop when
// the victim's last slot dies — compaction never drops a chunk directly.
func (c *Critbit[T]) compactInternalChunks(maxChunks int) int {
	c.reapMu.Lock()
	defer c.reapMu.Unlock()

	if c.closed.Load() || maxChunks <= 0 || c.root.Load() == nil {
		return 0
	}

	list := c.internalArena.list.Load()
	chunks, chunkSize := list.chunks, c.internalArena.chunkSize

	tail := int(c.internalArena.next.Load() / chunkSize)
	if tail == 0 {
		return 0
	}

	threshold := int(chunkSize) * compactOccupancyPct / 100

	var selection [64]bool

	selected := selection[:min(len(chunks), len(selection))]
	if len(chunks) > len(selection) {
		selected = make([]bool, len(chunks))
	}

	victims := 0
	for ci := 0; ci < len(chunks) && ci < tail && victims < maxChunks; ci++ {
		if chunks[ci] == nil {
			continue
		}
		// Recycled chunks awaiting re-allocation look sparse but are refilling;
		// see the same skip in compactLeafChunks.
		if chunks[ci].outstanding.Load() > 0 {
			continue
		}

		// All slots before tail have been allocated. With writers excluded,
		// every slot is either linked or accounted by recordReaped.
		if int(chunkSize)-int(chunks[ci].reaped.Load()) <= threshold {
			selected[ci] = true
			victims++
		}
	}

	if victims == 0 {
		return 0
	}

	// A link is either the root or a parent's child slot. Relocating through
	// the link avoids separate root handling and parent/direction bookkeeping.
	stack := make([]*atomic.Pointer[critNode[T]], 0, 64)

	stack = append(stack, &c.root)
	for len(stack) > 0 {
		link := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		node := link.Load()
		if node.isLeaf {
			continue
		}

		if int(node.chunkIdx) < len(selected) && selected[node.chunkIdx] {
			n2 := c.allocInternalNode(node.bytePos, node.otherbits)
			n2.child[0].Store(node.child[0].Load())
			n2.child[1].Store(node.child[1].Load())
			link.Store(n2)

			c.readRevision++
			c.retireInternal(node)
			node = n2
		}

		for dir := range 2 {
			if ch := node.child[dir].Load(); ch != nil && !ch.isLeaf {
				stack = append(stack, &node.child[dir])
			}
		}
	}

	return victims
}
