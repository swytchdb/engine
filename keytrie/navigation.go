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
	"strings"
)

func (c *Critbit[T]) FirstWithPrefix(prefix string, claim bool) (string, bool, ReleaseClaimFunc) {
	return c.seekPrefix(prefix, "", false, 0, claim)
}

func (c *Critbit[T]) LastWithPrefix(prefix string, claim bool) (string, bool, ReleaseClaimFunc) {
	return c.seekPrefix(prefix, "", false, 1, claim)
}

func (c *Critbit[T]) NextWithPrefix(prefix, after string, claim bool) (string, bool, ReleaseClaimFunc) {
	return c.seekPrefix(prefix, after, strings.HasPrefix(after, prefix), 0, claim)
}

func (c *Critbit[T]) PrevWithPrefix(prefix, before string, claim bool) (string, bool, ReleaseClaimFunc) {
	return c.seekPrefix(prefix, before, strings.HasPrefix(before, prefix), 1, claim)
}

// extremeLeaf finds the first live leaf in direction order: 0 is ascending,
// 1 is descending. The caller holds reapMu while reading the returned node.
func (c *Critbit[T]) extremeLeaf(node *critNode[T], prefix string, direction int) *critNode[T] {
	if node == nil {
		return nil
	}

	if node.isLeaf {
		if !node.isDeleted() && strings.HasPrefix(node.key, prefix) {
			return node
		}

		return nil
	}

	if leaf := c.extremeLeaf(node.child[direction].Load(), prefix, direction); leaf != nil {
		return leaf
	}

	return c.extremeLeaf(node.child[1-direction].Load(), prefix, direction)
}

// seekPrefix shares the lock and traversal for all four ordered lookups.
// A missing boundary is located at its hypothetical insertion point; following
// only its search path can skip neighbours when it differs at an earlier bit.
func (c *Critbit[T]) seekPrefix(prefix, boundary string, bounded bool, direction int, claim bool) (string, bool, ReleaseClaimFunc) {
	c.reapMu.RLock()
	defer c.reapMu.RUnlock()

	if c.closed.Load() {
		return "", false, nil
	}

	subtree := c.findPrefixSubtree(c.root.Load(), prefix)

	var leaf *critNode[T]

	if !bounded {
		leaf = c.extremeLeaf(subtree, prefix, direction)
	} else {
		type pathEntry struct {
			node *critNode[T]
			dir  int
		}

		path := make([]pathEntry, 0, 64)
		current := subtree

		for current != nil && !current.isLeaf {
			dir := getDirection(boundary, current.bytePos, current.otherbits)
			path = append(path, pathEntry{node: current, dir: dir})
			current = current.child[dir].Load()
		}

		if current != nil && current.key != boundary {
			bytePos, otherbits := findCritBit(boundary, current.key)
			insertion := current

			for len(path) > 0 {
				entry := path[len(path)-1]
				if shouldInsertBefore(entry.node.bytePos, entry.node.otherbits, bytePos, otherbits) {
					break
				}

				insertion = entry.node
				path = path[:len(path)-1]
			}

			if getDirection(current.key, bytePos, otherbits) == 1-direction {
				leaf = c.extremeLeaf(insertion, prefix, direction)
			}
		}

		for i := len(path) - 1; leaf == nil && i >= 0; i-- {
			entry := path[i]
			if entry.dir == direction {
				leaf = c.extremeLeaf(entry.node.child[1-direction].Load(), prefix, direction)
			}
		}
	}

	if leaf == nil {
		return "", false, nil
	}

	if claim {
		return leaf.key, true, noop
	}

	return leaf.key, true, nil
}
