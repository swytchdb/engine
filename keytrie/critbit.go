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

	"github.com/puzpuzpuz/xsync/v4"
)

const (
	defaultCritbitArenaChunkSize = 2048

	// maxLeafFreq caps the per-leaf access-frequency counter. The eviction
	// policy protects leaves whose freq exceeds the shard's adaptive
	// threshold; saturating keeps a hot key from running away.
	maxLeafFreq = 15

	// reapQueueCapacity bounds the pending-unlink queue. Deleted/evicted leaves
	// are pushed here at delete time (we already know which node went dead, so
	// there is no need to BFS the tree to rediscover them); a background worker
	// drains it in small batches under brief write-lock holds. On overflow a push
	// is dropped and a full-tree reap is scheduled as a backstop, so nothing leaks.
	reapQueueCapacity = 1 << 16

	// reapBatchSize is how many queued leaves the worker unlinks per write-lock
	// acquisition — small, so inserts interleave between batches instead of
	// stalling behind a whole-tree pass.
	reapBatchSize = 64
)

// critNode is a flattened node that serves as both internal and leaf.
// Hot-path fields (child, bytePos, otherbits, isLeaf) are in the first 22 bytes.
// Leaf fields start at offset 24.
//
// The type parameter T is the consumer-owned leaf payload. The trie itself
// never inspects it — it only stores the pointer on each leaf so a consumer
// (the effects engine) can attach a per-key cached subdag without leaking its
// types into keytrie. Plain tip indexes use T = struct{} and never touch it.
type critNode[T any] struct {
	child     [2]atomic.Pointer[critNode[T]] // 16 bytes, offset 0  (internal)
	bytePos   uint32                         // 4 bytes,  offset 16 (internal)
	otherbits uint8                          // 1 byte,   offset 20 (internal)
	isLeaf    bool                           // 1 byte,   offset 21
	// pinned caches the eviction decider's verdict (the system-key pin), computed
	// once when the leaf is created, so the sweep reads a bit instead of calling
	// the decider — a func-pointer indirect plus a per-key prefix check — on every
	// leaf it visits. The verdict is a pure function of the immutable key; key
	// updates reuse the leaf, so it never needs recomputing. Written once before
	// publish, so no atomic is needed (same discipline as key).
	pinned  bool                   // 1 byte,   offset 22 (leaf)
	_       [1]byte                //           offset 23
	key     string                 // 16 bytes, offset 24 (leaf)
	tips    atomic.Pointer[TipSet] // 8 bytes,  offset 40 (leaf)
	deleted atomic.Bool            // 4 bytes,  offset 44 (leaf)
	// Eviction metadata (leaf). A leaf spans a second cache line now;
	// these are written on the hot read path (freq bump) so they sit
	// apart from the structural fields above. freq < 0 marks a ghost:
	// a soft-deleted leaf retaining |freq| so a returning key warms back.
	freq       atomic.Int32  // 4 bytes,  offset 48
	chunkIdx   uint32        // 4 bytes,  offset 52 (arena chunk, for reclamation)
	lastAccess atomic.Uint64 // 8 bytes,  offset 56 (LRU tiebreak)
	// pins counts dynamic do-not-evict holds (Pin/Unpin). The cloud outbox
	// holds one while any of the key's effects await a durability ack:
	// evicting then would free almost nothing — the outbox already pins the
	// effect bytes — while making the key invisible cluster-wide (this node
	// unsubscribes and drops its tips, and the cloud has no tip markers yet),
	// so every read of the key anywhere misses data the cluster durably
	// holds. Unlike the static pinned bit this changes at runtime, so the
	// sweep pays one atomic load per visited leaf.
	pins atomic.Int32      // 4 bytes,  offset 64
	data atomic.Pointer[T] // 8 bytes,  offset 72 (leaf payload, 8-aligned)
	// lastDecayEpoch is the eviction-decay epoch this leaf last absorbed; the
	// sweep steps freq down by the elapsed epochs on visit (see decayLeaf).
	// Atomic because ghost promotion and the sweep can share the read lock
	// while the sweep writes it under evictMu.
	lastDecayEpoch atomic.Uint64 // 8 bytes,  offset 80
}

func noop() {}

func (n *critNode[T]) isDeleted() bool {
	return n.deleted.Load()
}

// Critbit is a crit-bit trie for storing string keys, generic over the
// per-leaf payload type T (see critNode).
type Critbit[T any] struct {
	root   atomic.Pointer[critNode[T]]
	size   atomic.Int64
	closed atomic.Bool

	deletedCount atomic.Int64 // deleted-but-linked leaves still in the tree
	reapRunning  atomic.Bool  // prevents concurrent reap goroutines

	// reapQueue holds leaves that have gone dead (deleted/evicted) and need to be
	// unlinked from the tree. Populated at delete time, drained by a background
	// worker (see drainReap) so the unlink never rides the hot path. reapDropped
	// counts pushes lost to a full queue; a non-zero value schedules a full-tree
	// reap as a backstop.
	reapQueue   *xsync.MPMCQueue[*critNode[T]]
	reapDropped atomic.Int64

	// Eviction policy (lifted from CloxCache as a single bounded domain;
	// see evict.go). Sweeps run serialized under evictMu — a cold path —
	// while the read path only bumps per-leaf freq/lastAccess + the window
	// counters. A sweep advances a CLOCK hand over the leaf arena's slots
	// (CloxCache's slot scan, adapted to the trie), reading them sequentially
	// rather than by per-sample tree descent. evictHand is the hand position,
	// mutated only under evictMu (no atomics needed).
	evictMu        sync.Mutex
	evictHand      uint64
	evictClock     uint64        // reclaims since the last decay-epoch step (under evictMu)
	decayEvery     atomic.Uint64 // reclaims per freq-decay step (0 = auto-scale, MaxUint64 = disabled)
	decayEpochN    atomic.Uint64 // completed decay intervals; leaves absorb the delta on sweep visit
	evictK         atomic.Int32  // protected-freq threshold (adaptive)
	ghostCount     atomic.Int64  // soft-deleted leaves retained for warm restart
	windowHits     windowCounter // live accesses in the current adapt window (sharded)
	windowOps      windowCounter // total accesses (live + ghost) in the window (sharded)
	evictedProt    atomic.Uint64 // evicted with freq > k (protected fallback)
	evictedUnprot  atomic.Uint64 // evicted with freq <= k (unprotected)
	reachedProt    atomic.Uint64 // leaves whose freq crossed k under pressure
	prevHitRate    atomic.Uint64 // last window hit rate * 10000 (gradient input)
	rateLow        atomic.Uint32 // adaptive low graduation threshold * 10000
	rateHigh       atomic.Uint32 // adaptive high graduation threshold * 10000
	lastKDir       atomic.Int32  // direction of the last k change (+1/-1/0)
	lastAdaptCheck atomic.Uint64 // eviction count at the last adapt check
	evictDecider   func(key string) bool
	evictNotify    func(key string, tips []EffectRef, data *T)
	// underPressure reports whether the consumer is currently over its eviction
	// target — the keytrie analogue of CloxCache's instantaneous
	// entryCount >= capacity check. A graduation (a leaf's freq crossing k) is
	// counted only while this is true, so adaptive-k measures the current
	// eviction episode instead of accumulating graduations across idle pauses.
	underPressure func() bool
	evictActive   atomic.Bool // true once eviction hooks are installed

	// refDelta, if set, is called on every leaf tip-set transition so the
	// consumer can maintain per-tip refcounts without re-deriving them at each
	// call site. added/removed are the tip-set delta; droppedData is the leaf
	// payload released by a delete or eviction (nil for a plain tip change), so
	// the consumer can release references the payload held. The trie owns the
	// "when"; the consumer owns the "what". Runs without any trie lock the
	// consumer could re-enter.
	refDelta func(added, removed []EffectRef, droppedData *T)

	// Optional shared key ownership, configured before publishing the trie.
	// The owner accounts for interned key storage; each leaf holds one reference.
	retainKey  func(string) string
	releaseKey func(string)

	initOnce sync.Once
	// Two arenas of the same node type — leaves and internal nodes never migrate
	// between roles (isLeaf is set at alloc and never flipped), so a node born in
	// one arena dies there. Splitting them lets the eviction sweep scan only the
	// leaf arena (dense, no internal-node skipping) and lets the sweep reclaim a
	// whole leaf chunk once every slot in it is dead. Individual slots are never
	// reused in place; reclamation is at chunk granularity, and a
	// reclaimed chunk's slot range is recycled through a fresh, never-published
	// array via the arena free list (see dropChunk).
	leafArena     critbitArena[critNode[T]]
	internalArena critbitArena[critNode[T]]

	// reapMu protects node lifetimes. Readers and inserts share the read lock;
	// the reaper takes the write lock to detach nodes and clear their fields.
	reapMu sync.RWMutex
	// Incremented under the write lock when existing paths are detached. Range
	// cursors restart after their last key instead of following retired nodes.
	readRevision uint64
}

// SetKeyOwnership shares key allocations with an external primary-cache owner.
// Set it only on an empty trie before concurrent use. Relocations transfer a
// reference; deletion releases it when the leaf is physically unlinked.
func (c *Critbit[T]) SetKeyOwnership(retain func(string) string, release func(string)) {
	if retain == nil || release == nil {
		panic("keytrie: key ownership requires retain and release")
	}

	c.retainKey, c.releaseKey = retain, release
}

// NewCritbit creates a new crit-bit trie with leaf payload type T.
func NewCritbit[T any]() *Critbit[T] {
	c := &Critbit[T]{}
	c.ensureInit()

	return c
}

func (c *Critbit[T]) ensureInit() {
	c.initOnce.Do(func() {
		// Leaf slots are born deleted=true (and isLeaf=true) so the eviction sweep
		// skips a slot until allocLeafNode has written it and Insert has published
		// it (deleted=false) after linking. See the sweep in evict.go.
		c.leafArena.init(defaultCritbitArenaChunkSize, func(n *critNode[T]) {
			n.isLeaf = true
			n.deleted.Store(true)
		})
		c.internalArena.init(defaultCritbitArenaChunkSize, nil)
		c.reapQueue = xsync.NewMPMCQueue[*critNode[T]](reapQueueCapacity)
		c.evictK.Store(defaultProtectedFreqThreshold)
		c.rateLow.Store(defaultRateLow)
		c.rateHigh.Store(defaultRateHigh)
	})
}

// allocLeafNode returns a fresh leaf from the leaf arena. The node is born
// deleted=true (the arena init hook) and stays so while its fields are written,
// so the eviction sweep — which scans the leaf arena directly — skips it; the
// caller flips deleted=false only after linking it into the tree, which is also
// when its fields become visible to readers (release).
func (c *Critbit[T]) allocLeafNode(key string, ts *TipSet) *critNode[T] {
	n, ci := c.leafArena.alloc() // born deleted=true via the arena init hook
	n.chunkIdx = ci

	if c.retainKey != nil {
		key = c.retainKey(key)
	}

	n.key = key
	c.leafArena.addExtra(ci, c.keyBytes(key)+tipSetBytes(ts))
	// A decider returning false means "do not evict" → pinned. When no decider is
	// installed, no sweep ever runs, so the default false is inert.
	n.pinned = c.evictDecider != nil && !c.evictDecider(key)
	n.tips.Store(ts)
	n.freq.Store(1) // start warm enough to survive one sweep
	n.lastAccess.Store(monotonic())
	// Start the decay epoch at "now": a recycled arena slot retains the epoch
	// of its previous occupant, and a stale epoch would make the sweep charge
	// this leaf for decay it never lived through.
	n.lastDecayEpoch.Store(c.decayEpoch())

	return n
}

func (c *Critbit[T]) allocInternalNode(bytePos uint32, otherbits uint8) *critNode[T] {
	n, ci := c.internalArena.alloc()
	n.chunkIdx = ci
	n.bytePos = bytePos
	n.otherbits = otherbits

	return n
}

func highestBit(b byte) uint8 {
	if b == 0 {
		return 0
	}

	b |= b >> 1
	b |= b >> 2
	b |= b >> 4

	return b ^ (b >> 1)
}

func findCritBit(a, b string) (bytePos uint32, otherbits uint8) {
	minLen := min(len(b), len(a))

	for i := range minLen {
		if a[i] != b[i] {
			return uint32(i), highestBit(a[i]^b[i]) ^ 0xFF
		}
	}

	if len(a) != len(b) {
		bytePos = uint32(minLen)

		var diffByte byte
		if len(a) > minLen {
			diffByte = a[minLen]
		} else {
			diffByte = b[minLen]
		}

		hb := highestBit(diffByte)
		if hb == 0 {
			return bytePos, 0xFF // length-discrimination sentinel
		}

		return bytePos, hb ^ 0xFF
	}

	return 0, 0
}

// getDirection uses branchless arithmetic matching the reference C implementation.
// otherbits has all bits set except the critical bit.
// (1 + uint16(otherbits | c)) >> 8 yields 1 when the critical bit is set in c, 0 otherwise.
func getDirection(key string, bytePos uint32, otherbits uint8) int {
	if int(bytePos) >= len(key) {
		return 0
	}

	c := key[bytePos]

	return int(1+uint16(otherbits|c)) >> 8
}

// Insert attempts to store a TipSet for the given key using CAS.
//
// For new keys (old is nil): CAS retry loop for tree structural insertion.
// For existing keys: single CAS comparing old to the current pointer.
//
// On success: returns (nil, true).
// On CAS failure: returns (currentTips, false) — the conflicting tip set.
func (c *Critbit[T]) Insert(key string, old *TipSet, newTips *TipSet) (*TipSet, bool) {
	if c.closed.Load() {
		return nil, false
	}

	c.ensureInit()

	for {
		c.reapMu.RLock()

		if c.closed.Load() {
			c.reapMu.RUnlock()

			return nil, false
		}

		// Load the root under the read lock so reclamation cannot detach and
		// clear a path before insertion finishes following it. A structural
		// CAS on a detached node would link a leaf into an unreachable subtree.
		rootNode := c.root.Load()

		if rootNode == nil {
			newNode := c.allocLeafNode(key, newTips) // deleted=true, pop under the read lock
			if c.root.CompareAndSwap(nil, newNode) {
				newNode.deleted.Store(false) // publish only after linking
				c.size.Add(1)
				c.reapMu.RUnlock()
				c.fireTipDelta(old, newTips, nil)

				return nil, true
			}

			// Lost the CAS: newNode is an unlinked orphan, still deleted=true, so
			// the sweep skips it. Its slot is never reused; account it reaped now,
			// or its chunk could never reach a full count and would be pinned from
			// reclamation forever by this one dead slot.
			c.retireLeaf(newNode)
			c.reapMu.RUnlock()

			continue
		}

		bestLeaf := c.findBestMatch(rootNode, key)
		if bestLeaf == nil {
			c.reapMu.RUnlock()

			continue
		}

		if bestLeaf.key == key {
			// Existing key — single CAS attempt.
			// If the leaf is deleted, the caller may pass old=nil (from Contains
			// returning nil). Accept that by swapping old to the actual current
			// pointer so the CAS can succeed.
			current := bestLeaf.tips.Load()
			if old == nil && bestLeaf.isDeleted() {
				old = current
			}

			if bestLeaf.tips.CompareAndSwap(old, newTips) {
				c.leafArena.addExtra(bestLeaf.chunkIdx, tipSetBytes(newTips)-tipSetBytes(old))

				if bestLeaf.deleted.CompareAndSwap(true, false) {
					c.size.Add(1)
					// A ghost (freq < 0) was never counted as a deleted leaf;
					// promote it instead of decrementing deletedCount.
					if bestLeaf.freq.Load() < 0 {
						c.promoteIfGhost(bestLeaf)
					} else {
						c.deletedCount.Add(-1)
					}
				}

				c.reapMu.RUnlock()
				c.fireTipDelta(old, newTips, nil)

				return nil, true
			}

			tips := bestLeaf.tips.Load()

			c.reapMu.RUnlock()

			return tips, false
		}

		bytePos, otherbits := findCritBit(key, bestLeaf.key)

		newLeafNode := c.allocLeafNode(key, newTips) // deleted=true
		newInternal := c.allocInternalNode(bytePos, otherbits)

		newDir := getDirection(key, bytePos, otherbits)
		oldDir := 1 - newDir

		ok := c.insertNode(rootNode, newInternal, newLeafNode, newDir, oldDir, bytePos, otherbits)
		if ok {
			newLeafNode.deleted.Store(false) // publish after linking, before releasing the read lock
			c.size.Add(1)
			c.reapMu.RUnlock()
			c.fireTipDelta(old, newTips, nil)

			return nil, true
		}

		// Lost: newLeafNode/newInternal are unlinked orphans (leaf still
		// deleted=true, so the sweep skips it). Account both slots reaped so
		// they don't pin their chunks from reclamation, then retry.
		c.retireLeaf(newLeafNode)
		c.retireInternal(newInternal)
		c.reapMu.RUnlock()
	}
}

func (c *Critbit[T]) findBestMatch(node *critNode[T], key string) *critNode[T] {
	if node == nil {
		return nil
	}

	current := node
	for !current.isLeaf {
		dir := getDirection(key, current.bytePos, current.otherbits)

		next := current.child[dir].Load()
		if next == nil {
			return nil
		}

		current = next
	}

	return current
}

func (c *Critbit[T]) insertNode(rootNode *critNode[T], newInternal *critNode[T], newLeafNode *critNode[T], newDir, oldDir int, bytePos uint32, otherbits uint8) bool {
	if rootNode.isLeaf || shouldInsertBefore(bytePos, otherbits, rootNode.bytePos, rootNode.otherbits) {
		newInternal.child[newDir].Store(newLeafNode)
		newInternal.child[oldDir].Store(rootNode)

		return c.root.CompareAndSwap(rootNode, newInternal)
	}

	current := rootNode

	for {
		// The new node's bit must be strictly less significant than the
		// current parent's bit. A concurrent insert may have spliced a
		// new internal node with the same (or more-significant) bit into
		// the path we are walking, which would create a duplicate or
		// out-of-order bit position if we insert here.
		if !shouldInsertBefore(current.bytePos, current.otherbits, bytePos, otherbits) {
			return false
		}

		dir := getDirection(newLeafNode.key, current.bytePos, current.otherbits)
		childNode := current.child[dir].Load()

		if childNode == nil {
			return false
		}

		if childNode.isLeaf {
			// Verify the precomputed crit bit is still valid for this leaf.
			// A concurrent insert may have replaced the original leaf with a
			// different one, making the crit bit (computed against the old
			// best-match leaf) incorrect for this pairing.
			actualBytePos, actualOtherbits := findCritBit(newLeafNode.key, childNode.key)
			if actualBytePos != bytePos || actualOtherbits != otherbits {
				return false // tree changed; retry from Insert's outer loop
			}
		}

		if childNode.isLeaf || shouldInsertBefore(bytePos, otherbits, childNode.bytePos, childNode.otherbits) {
			newInternal.child[newDir].Store(newLeafNode)
			newInternal.child[oldDir].Store(childNode)

			return current.child[dir].CompareAndSwap(childNode, newInternal)
		}

		current = childNode
	}
}

func shouldInsertBefore(newBytePos uint32, newOtherbits uint8, curBytePos uint32, curOtherbits uint8) bool {
	// Lower otherbits means a more significant bit within the same byte.
	return newBytePos < curBytePos || newBytePos == curBytePos && newOtherbits < curOtherbits
}

// fireTipDelta reports a leaf tip-set transition to the refDelta hook (if any).
// droppedData is the leaf payload released by a delete/eviction, else nil.
func (c *Critbit[T]) fireTipDelta(old, newTips *TipSet, droppedData *T) {
	if c.refDelta == nil {
		return
	}

	var added, removed []EffectRef

	switch {
	case old == newTips:
		// No tip change; the payload may still need releasing.
	case old.Len() == 0:
		added = newTips.Tips()
	case newTips.Len() == 0:
		removed = old.Tips()
	default:
		for _, t := range newTips.tips {
			if !old.Contains(t) {
				added = append(added, t)
			}
		}

		for _, t := range old.tips {
			if !newTips.Contains(t) {
				removed = append(removed, t)
			}
		}
	}

	if len(added) > 0 || len(removed) > 0 || droppedData != nil {
		c.refDelta(added, removed, droppedData)
	}
}

// RemoveTips drops refs from key's tip set (CAS retry). The CAS runs under the
// reap read-lock like every other leaf-field mutation: compaction (write lock)
// relocates leaves, and a CAS landing on a relocated husk would be silently
// lost. The delta fires outside the lock (the hook may re-enter the trie).
func (c *Critbit[T]) RemoveTips(key string, refs []EffectRef) {
	if len(refs) == 0 {
		return
	}

	c.reapMu.RLock()

	leaf := c.liveLeaf(key)
	if leaf == nil {
		c.reapMu.RUnlock()

		return
	}

	for {
		current := leaf.tips.Load()
		if current == nil {
			c.reapMu.RUnlock()

			return
		}

		remaining := 0

		for _, tip := range current.tips {
			if !slices.Contains(refs, tip) {
				remaining++
			}
		}

		if remaining == len(current.tips) {
			c.reapMu.RUnlock()

			return // nothing to remove
		}

		// Allocate only retained capacity. The source is already deduplicated,
		// so the owned slice can be published without another NewTipSet copy.
		kept := make([]EffectRef, 0, remaining)

		for _, tip := range current.tips {
			if !slices.Contains(refs, tip) {
				kept = append(kept, tip)
			}
		}

		newTips := &TipSet{tips: kept}
		if leaf.tips.CompareAndSwap(current, newTips) {
			c.leafArena.addExtra(leaf.chunkIdx, tipSetBytes(newTips)-tipSetBytes(current))
			c.reapMu.RUnlock()
			c.fireTipDelta(current, newTips, nil)

			return
		}
		// CAS failed, retry
	}
}

func (c *Critbit[T]) Delete(key string, old *TipSet) bool {
	return c.DeleteAndSnapshot(key, old) != nil
}

// DeleteAndSnapshot removes a key only if its current tips match old
// (CAS). Returns the previous tip set on success, nil on failure.
func (c *Critbit[T]) DeleteAndSnapshot(key string, old *TipSet) *TipSet {
	c.reapMu.RLock()

	leaf := c.liveLeaf(key)
	if leaf == nil {
		c.reapMu.RUnlock()

		return nil
	}

	if !leaf.deleted.CompareAndSwap(false, true) {
		c.reapMu.RUnlock()

		return nil
	}

	if !leaf.tips.CompareAndSwap(old, nil) {
		leaf.deleted.Store(false)
		c.reapMu.RUnlock()

		return nil
	}
	// Drop the leaf payload too — a deleted key's cached state is stale.
	droppedData := leaf.data.Load()
	leaf.data.Store(nil)
	c.releaseLeafMemory(leaf, old, droppedData)
	c.size.Add(-1)

	c.reapMu.RUnlock()

	// Release the tips and the payload's references — DeleteAndSnapshot bypasses
	// the eviction notify, so this is the only refcount signal for FlushIndex.
	c.fireTipDelta(old, nil, droppedData)

	deleted := c.deletedCount.Add(1)
	c.enqueueReap(leaf)

	live := c.size.Load()
	if deleted > 0 && deleted > (live+deleted)/10 {
		c.maybeReap()
	}

	return old
}

func (c *Critbit[T]) Size() int64 {
	if c.closed.Load() {
		return 0
	}

	return c.size.Load()
}

// ArenaBytes returns the combined slot-array footprint of the leaf and internal
// arenas. Append-only growth means this tracks the high-water mark of allocated
// nodes and only drops when a whole chunk is reclaimed — so it is the direct
// measure of trie-skeleton memory, distinct from the vertex pool's effect bytes.
func (c *Critbit[T]) ArenaBytes() int64 {
	if c.closed.Load() {
		return 0
	}

	return c.leafArena.bytes() + c.internalArena.bytes()
}

func (c *Critbit[T]) Range(fn func(key string) bool) {
	c.rangeKeys("", "", false, fn)
}

func (c *Critbit[T]) RangeFrom(after string, fn func(key string) bool) {
	c.rangeKeys("", after, after != "", fn)
}

func (c *Critbit[T]) RangePrefix(prefix string, fn func(key string) bool) {
	c.rangeKeys(prefix, "", false, fn)
}

func (c *Critbit[T]) findPrefixSubtree(node *critNode[T], prefix string) *critNode[T] {
	current := node
	for current != nil && !current.isLeaf {
		if int(current.bytePos) >= len(prefix) {
			return current
		}

		dir := getDirection(prefix, current.bytePos, current.otherbits)
		current = current.child[dir].Load()
	}

	return current
}

func (c *Critbit[T]) Keys() []string {
	if c.closed.Load() {
		return nil
	}

	result := make([]string, 0, c.size.Load())
	c.Range(func(key string) bool {
		result = append(result, key)

		return true
	})

	return result
}

func (c *Critbit[T]) MatchPattern(pattern string) []string {
	if c.closed.Load() {
		return nil
	}

	if pattern == "*" {
		return c.Keys()
	}

	result := make([]string, 0)

	c.Range(func(key string) bool {
		if matchGlob(key, pattern) {
			result = append(result, key)
		}

		return true
	})

	return result
}

// liveLeaf resolves an exact, live key. The caller holds reapMu until it has
// finished using the node, including any mutation and memory accounting.
func (c *Critbit[T]) liveLeaf(key string) *critNode[T] {
	if c.closed.Load() {
		return nil
	}

	leaf := c.findBestMatch(c.root.Load(), key)
	if leaf == nil || leaf.key != key || leaf.isDeleted() {
		return nil
	}

	return leaf
}

func (c *Critbit[T]) TryClaimKey(key string) (exists bool, release ReleaseClaimFunc) {
	c.reapMu.RLock()
	defer c.reapMu.RUnlock()

	if c.liveLeaf(key) == nil {
		return false, nil
	}

	return true, noop
}

func (c *Critbit[T]) GetHeadHint(prefix string) string      { return "" }
func (c *Critbit[T]) SetHeadHint(prefix string, key string) {}
func (c *Critbit[T]) GetTailHint(prefix string) string      { return "" }
func (c *Critbit[T]) SetTailHint(prefix string, key string) {}

// Snapshot returns a frozen copy of the index. The new Critbit is independent —
// mutations to either copy don't affect the other. TipSets are immutable so
// only pointers are copied. Leaf payloads (T) are NOT copied: a snapshot is a
// tip-only view used for SSI reads. O(n) in key count.
func (c *Critbit[T]) Snapshot() KeyIndex {
	snap := NewCritbit[T]()

	c.Range(func(key string) bool {
		tips := c.Contains(key)
		if tips != nil {
			snap.Insert(key, nil, tips)
		}

		return true
	})

	return snap
}

// Closed reports whether Close has been called. Mutations on a closed trie
// fail unconditionally, so CAS retry loops must check this to avoid spinning
// forever during shutdown.
func (c *Critbit[T]) Closed() bool {
	return c.closed.Load()
}

func (c *Critbit[T]) Close() error {
	c.reapMu.Lock()
	defer c.reapMu.Unlock()

	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}

	c.root.Store(nil)
	c.readRevision++
	c.size.Store(0)

	if c.releaseKey != nil {
		for _, chunk := range c.leafArena.list.Load().chunks {
			if chunk == nil {
				continue
			}

			for i := range chunk.nodes {
				if key := chunk.nodes[i].key; key != "" {
					c.releaseKey(key)
				}
			}
		}
	}

	c.leafArena.clear()
	c.internalArena.clear()

	return nil
}
