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

// enqueueReap queues a leaf that just went dead for background unlinking. A drop
// (full queue) bumps reapDropped so maybeReap falls back to a full BFS, which
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
		// Backstop: if any pushes were dropped on a full queue, a full BFS pass
		// rediscovers those stragglers so nothing lingers linked forever.
		if c.reapDropped.Swap(0) > 0 {
			for c.reap() > 0 {
			}
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

	root := c.root.Load()
	if root == nil {
		return false
	}

	if root == leaf {
		c.root.Store(nil)
		c.readRevision++
		c.deletedCount.Add(-1)
		c.retireLeaf(leaf)

		return true
	}

	// Descend to leaf.key, tracking the parent (p) and grandparent (gp) so we can
	// splice the leaf's sibling up into p's slot, dropping both leaf and p.
	key := leaf.key

	var (
		gp, p       *critNode[T]
		gpDir, pDir int
	)

	cur := root
	for !cur.isLeaf {
		dir := getDirection(key, cur.bytePos, cur.otherbits)
		gp, gpDir = p, pDir
		p, pDir = cur, dir

		next := cur.child[dir].Load()
		if next == nil {
			return false
		}

		cur = next
	}

	if cur != leaf {
		return false // a different leaf occupies this key path now
	}

	sibling := p.child[1-pDir].Load()
	if sibling == nil {
		return false
	}

	if gp == nil {
		c.root.Store(sibling)
	} else {
		gp.child[gpDir].Store(sibling)
	}

	c.deletedCount.Add(-1)
	c.readRevision++
	c.retireInternal(p)
	c.retireLeaf(leaf)

	return true
}

// reap does a single BFS walk pruning every deleted leaf it finds.
// Takes the write-lock on reapMu so no structural insert can race.
// Returns the number of deleted leaves unlinked.
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

	if root.isLeaf {
		// Ghosts (deleted, freq < 0) are retained for warm restart.
		if root.isDeleted() && root.freq.Load() >= 0 {
			c.root.Store(nil)
			c.readRevision++
			c.deletedCount.Add(-1)
			c.retireLeaf(root)

			return 1
		}

		return 0
	}

	type bfsEntry struct {
		node          *critNode[T]
		parent        *critNode[T]
		dirFromParent int
	}

	reaped := 0
	queue := make([]bfsEntry, 1, 64)
	queue[0] = bfsEntry{node: root}

	for i := 0; i < len(queue); i++ {
		e := queue[i]
		node := e.node

		pruned := false

		for dir := range 2 {
			child := node.child[dir].Load()
			// Skip live leaves, internal nodes, and ghosts (freq < 0).
			if child == nil || !child.isLeaf || !child.isDeleted() || child.freq.Load() < 0 {
				continue
			}

			sibling := node.child[1-dir].Load()
			if sibling == nil {
				continue
			}

			if e.parent == nil {
				c.root.Store(sibling)
			} else {
				e.parent.child[e.dirFromParent].Store(sibling)
			}

			c.deletedCount.Add(-1)

			reaped++
			pruned = true

			// Both the deleted leaf and the internal node it collapsed are now
			// unlinked; tell each arena so it can drop a fully-dead chunk.
			c.readRevision++
			c.retireInternal(node)
			c.retireLeaf(child)

			if !sibling.isLeaf {
				queue = append(queue, bfsEntry{
					node:          sibling,
					parent:        e.parent,
					dirFromParent: e.dirFromParent,
				})
			}

			break
		}

		if !pruned {
			for dir := range 2 {
				child := node.child[dir].Load()
				if child != nil && !child.isLeaf {
					queue = append(queue, bfsEntry{
						node:          child,
						parent:        node,
						dirFromParent: dir,
					})
				}
			}
		}
	}

	return reaped
}
