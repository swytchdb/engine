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

import "sync/atomic"

// enqueueReap queues a leaf that just went dead for background unlinking. A drop
// (full queue) bumps reapDropped so maybeReap falls back to a full traversal, which
// rediscovers stragglers — nothing is leaked, the fast path is just bypassed.
func (c *Critbit[T]) enqueueReap(leaf *critNode[T]) {
	if c.reapQueue == nil || !c.reapQueue.TryEnqueue(leaf) {
		c.reapDropped.Add(1)
	}
}

func (c *Critbit[T]) maybeReap() {
	if !c.reapRunning.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer c.reapRunning.Store(false)
		// Fast path: unlink the leaves we already know are dead, in small batches.
		for c.drainReap() > 0 {
		}
		// Backstop: if any pushes were dropped on a full queue, a full traversal
		// rediscovers those stragglers so nothing lingers linked forever.
		if c.reapDropped.Swap(0) > 0 {
			c.reap()
		}
		// Unlinking alone frees a chunk only when every one of its slots died;
		// under churn, survivors fragment across chunks and the arenas ratchet.
		// Compaction evacuates sparse chunks so they can drop (see compact.go).
		c.maybeCompact()
	}()
}

// drainReap pops up to reapBatchSize queued leaves and unlinks them under a
// single short write-lock hold. Returns the number popped (0 when the queue is
// empty), so the caller can loop until drained. Each unlink re-validates the
// leaf is still dead and still the leaf for its key, so a revived or
// already-unlinked entry is harmlessly skipped.
func (c *Critbit[T]) drainReap() int {
	if c.closed.Load() || c.reapQueue == nil {
		return 0
	}

	var batch [reapBatchSize]*critNode[T]

	n := 0
	for n < reapBatchSize {
		leaf, ok := c.reapQueue.TryDequeue()
		if !ok {
			break
		}

		batch[n] = leaf
		n++
	}

	if n == 0 {
		return 0
	}

	c.reapMu.Lock()
	for i := range n {
		c.unlinkLeaf(batch[i])
	}
	c.reapMu.Unlock()

	return n
}

// unlinkLeaf removes one dead leaf (and the internal node it collapses) from the
// tree, given the leaf directly — no search. The caller must hold the reap write
// lock. It re-validates under the lock: the leaf must still be deleted, non-ghost
// (freq >= 0), and still reachable as the leaf for its key; otherwise it was
// revived or already unlinked and is skipped. Returns true if it unlinked.
func (c *Critbit[T]) unlinkLeaf(leaf *critNode[T]) bool {
	if leaf == nil || !leaf.isDeleted() || leaf.freq.Load() < 0 {
		return false
	}

	// Track the link to the parent so pruning needs no special case for an
	// internal root and no separate grandparent/direction bookkeeping.
	var (
		parentLink *atomic.Pointer[critNode[T]]
		parent     *critNode[T]
		dir        int
	)

	link := &c.root

	cur := link.Load()
	for cur != nil && !cur.isLeaf {
		parentLink, parent = link, cur
		dir = getDirection(leaf.key, cur.bytePos, cur.otherbits)
		link = &cur.child[dir]
		cur = link.Load()
	}

	if cur != leaf {
		return false // a different leaf occupies this key path now
	}

	if parent == nil {
		c.root.Store(nil)
		c.readRevision++
		c.deletedCount.Add(-1)
		c.retireLeaf(leaf)
	} else {
		c.pruneLeaf(parentLink, parent, dir)
	}

	return true
}

// pruneLeaf replaces parent with the dead leaf's sibling. Both reclamation
// paths use this operation under reapMu's write lock, so retirement and path
// invalidation have one owner. link points to parent, dir to the dead leaf.
func (c *Critbit[T]) pruneLeaf(link *atomic.Pointer[critNode[T]], parent *critNode[T], dir int) {
	leaf := parent.child[dir].Load()
	link.Store(parent.child[1-dir].Load())
	c.deletedCount.Add(-1)
	c.readRevision++
	c.retireInternal(parent)
	c.retireLeaf(leaf)
}

// reap prunes all dead leaves in one depth-first pass. The stack holds links
// into reachable parents, using space proportional to tree depth rather than
// key count. Children are processed before their parent, so a dead subtree
// collapses all the way to the root without another pass.
func (c *Critbit[T]) reap() int {
	if c.closed.Load() {
		return 0
	}

	c.reapMu.Lock()
	defer c.reapMu.Unlock()

	root := c.root.Load()
	if root == nil {
		return 0
	}

	reaped := 0

	type frame struct {
		link    *atomic.Pointer[critNode[T]]
		visited bool
	}

	stack := make([]frame, 0, 64)
	stack = append(stack, frame{link: &c.root})

	for len(stack) > 0 {
		f := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		node := f.link.Load()

		if node.isLeaf {
			continue
		}

		if !f.visited {
			stack = append(stack, frame{link: f.link, visited: true})

			for dir := range 2 {
				if child := node.child[dir].Load(); !child.isLeaf {
					stack = append(stack, frame{link: &node.child[dir]})
				}
			}

			continue
		}

		for dir := range 2 {
			child := node.child[dir].Load()
			if child.isLeaf && child.isDeleted() && child.freq.Load() >= 0 {
				c.pruneLeaf(f.link, node, dir)

				reaped++

				break
			}
		}
	}

	if root = c.root.Load(); root.isLeaf && c.unlinkLeaf(root) {
		reaped++
	}

	return reaped
}
