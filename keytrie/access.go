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
	"unsafe"
)

// Pin adds a dynamic do-not-evict hold on key's live leaf, reporting whether
// one was taken (false: no live leaf — nothing to protect). Taken under the
// reap read lock, writer discipline: chunk compaction copies leaf fields under
// the write lock, so a hold can never land on a husk the copy already left.
func (c *Critbit[T]) Pin(key string) bool {
	c.reapMu.RLock()
	defer c.reapMu.RUnlock()

	leaf := c.liveLeaf(key)
	if leaf == nil {
		return false
	}

	leaf.pins.Add(1)
	// Dekker partner of EvictBatch's post-claim pins re-check: if the sweep
	// claimed this leaf between our liveness check and the Add, exactly one of
	// us observes the other — undo so the hold is never stranded on an evicted
	// leaf (a ghost promotion would resurrect it as a permanent pin).
	if leaf.isDeleted() {
		leaf.pins.Add(-1)

		return false
	}

	return true
}

// Unpin releases one dynamic hold on key's live leaf. A missing or deleted
// leaf is a no-op (the key was explicitly deleted or the index flushed while
// held — the hold died with the leaf). A negative count on a live leaf is an
// unpin without a matching pin: a protocol bug that would let the sweep evict
// a key another holder still protects, so it panics like a refcount underflow.
func (c *Critbit[T]) Unpin(key string) bool {
	c.reapMu.RLock()
	defer c.reapMu.RUnlock()

	leaf := c.liveLeaf(key)
	if leaf == nil {
		return false
	}

	if n := leaf.pins.Add(-1); n < 0 {
		panic(fmt.Sprintf("keytrie: pin underflow on %q (unpin without matching pin)", key))
	}

	return true
}

// Contains checks if a key exists and returns its TipSet.
// Returns nil if the key doesn't exist or is deleted.
func (c *Critbit[T]) Contains(key string) *TipSet {
	c.reapMu.RLock()
	defer c.reapMu.RUnlock()

	leaf := c.liveLeaf(key)
	if leaf == nil {
		return nil
	}

	return leaf.tips.Load()
}

// LoadData returns the existing leaf payload without installing one. Like
// Contains, it holds the structural read lock until it has copied the payload
// pointer. The returned payload remains valid after the leaf is retired.
// A successful lookup records the access for eviction policy.
func (c *Critbit[T]) LoadData(key string) *T {
	c.reapMu.RLock()
	defer c.reapMu.RUnlock()

	leaf := c.liveLeaf(key)
	if leaf == nil {
		return nil
	}

	c.bumpAccess(leaf)

	return leaf.data.Load()
}

// bumpAccess records an access to leaf: it raises the frequency counter
// (saturating at maxLeafFreq) and stamps lastAccess from the monotonic clock.
// Lock-free; called on the hot read path. A ghost (freq < 0) is left alone —
// it is promoted explicitly when its key is re-inserted.
func (c *Critbit[T]) bumpAccess(leaf *critNode[T]) {
	// When eviction is inactive (no memory governor) nothing consumes the
	// freq/LRU/window data, so skip all of it — this keeps the read path free
	// of trie-global atomic contention in the default, no-eviction case.
	if !c.evictActive.Load() {
		return
	}

	for {
		f := leaf.freq.Load()
		if f < 0 || f >= maxLeafFreq {
			break
		}

		if leaf.freq.CompareAndSwap(f, f+1) {
			// Count a leaf crossing into protected status, but only while
			// instantaneously over the eviction target — CloxCache's
			// entryCount >= capacity guard. The graduation rate is
			// graduated/evictions; counting graduations while not under pressure
			// (cold start, or an idle pause after the governor brought us back
			// under target) inflates the numerator against a frozen denominator
			// and rails k. underPressure must be the *current* over-target
			// condition, not a "have we ever evicted" latch — a latch never falls
			// back to false, so the rate accumulates across pauses. The predicate
			// runs only at the crossing (once per leaf lifetime), not on every
			// read — except at k=0 (protection off), where every successful
			// bump counts: the question there is "does enough recurrence exist
			// to pay for protection at all", and once-per-residency crossings
			// can't push the rate past rateHigh even on a hot-set workload
			// (measured ≈0.25 on scan-burst — k never climbed and the policy
			// stayed LRU). Resident hits per eviction can.
			if k := c.evictK.Load(); (k == 0 || f == k) && c.underPressure != nil && c.underPressure() {
				c.reachedProt.Add(1)
			}

			break
		}
	}

	leaf.lastAccess.Store(monotonic())
	// Window accounting for adaptive-k: a live-leaf access is a hit. Sharded by
	// leaf address so the per-access increment doesn't serialize on one line.
	p := unsafe.Pointer(leaf)
	c.windowHits.add(p, 1)
	c.windowOps.add(p, 1)
}

// LoadOrStoreData returns the leaf payload for key, installing def if none is
// present yet (CAS, so concurrent callers agree on a single payload). Returns
// (nil, false) if the key is missing/deleted. The bool reports whether an
// existing payload was loaded (true) rather than def being stored (false).
//
// Locating the payload counts as an access: it bumps the leaf's frequency and
// last-access stamp, which is the signal the eviction policy learns from. The
// read path (reconstruct) calls this on every read, hit or miss.
//
// The install CAS runs under the reap read-lock like every other leaf-field
// mutation: compaction (write lock) relocates leaves, and a CAS landing on a
// relocated husk would be silently lost.
func (c *Critbit[T]) LoadOrStoreData(key string, def *T) (*T, bool) {
	c.reapMu.RLock()
	defer c.reapMu.RUnlock()

	leaf := c.liveLeaf(key)
	if leaf == nil {
		return nil, false
	}

	c.bumpAccess(leaf)

	if cur := leaf.data.Load(); cur != nil {
		return cur, true
	}

	if leaf.data.CompareAndSwap(nil, def) {
		c.leafArena.addExtra(leaf.chunkIdx, payloadBytes(def))

		return def, false
	}

	return leaf.data.Load(), true
}
