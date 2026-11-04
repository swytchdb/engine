package keytrie

// rangeKeys copies small batches under the structural read lock, then invokes
// callbacks without a lock. Callbacks may re-enter or mutate the trie. A cursor
// may retain a detached node between batches, but never reads it after a path
// revision: it restarts from the current root, skipping keys already delivered.
// Concurrent mutations have the same weak consistency as the other trie scans.
func (c *Critbit[T]) rangeKeys(prefix, after string, hasAfter bool, fn func(string) bool) {
	if fn == nil {
		return
	}

	var storage [64]*critNode[T]

	stack := storage[:0]

	var revision uint64

	initialized := false

	for {
		var keys [64]string

		n := 0

		c.reapMu.RLock()

		if c.closed.Load() {
			c.reapMu.RUnlock()

			return
		}

		if !initialized || revision != c.readRevision {
			clear(stack)
			stack = stack[:0]

			if root := c.root.Load(); root != nil {
				if subtree := c.findPrefixSubtree(root, prefix); subtree != nil {
					stack = append(stack, subtree)
				}
			}

			revision, initialized = c.readRevision, true
		}

		for len(stack) > 0 && n < len(keys) {
			last := len(stack) - 1
			node := stack[last]
			stack[last] = nil
			stack = stack[:last]

			if node.isLeaf {
				if !node.isDeleted() && (!hasAfter || node.key > after) && len(node.key) >= len(prefix) && node.key[:len(prefix)] == prefix {
					keys[n] = node.key
					n++
				}

				continue
			}

			if right := node.child[1].Load(); right != nil {
				stack = append(stack, right)
			}

			if left := node.child[0].Load(); left != nil {
				stack = append(stack, left)
			}
		}

		done := len(stack) == 0

		c.reapMu.RUnlock()

		for i := range n {
			after, hasAfter = keys[i], true
			if !fn(keys[i]) {
				return
			}

			keys[i] = ""
		}

		if done {
			return
		}
	}
}
