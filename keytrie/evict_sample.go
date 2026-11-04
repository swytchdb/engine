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

import "cmp"

type evictionCandidate[T any] struct {
	leaf   *critNode[T]
	access uint64
}

// evictionSample classifies one bounded sweep. Its node pointers are valid only
// while reapMu is held. Storage belongs to the calling batch, never to the trie.
type evictionSample[T any] struct {
	// Cold leaves fill the front, ghosts fill the back. Their combined count
	// cannot exceed the sweep budget, so one buffer suffices for any mixture.
	candidates          []evictionCandidate[T]
	coldEnd, ghostStart int
	oldest              *evictionCandidate[T]
	threshold           int32
	epoch               uint64
}

func (s *evictionSample[T]) consider(leaf *critNode[T]) {
	candidate := evictionCandidate[T]{leaf: leaf, access: leaf.lastAccess.Load()}
	if leaf.deleted.Load() {
		// The arena sweep only forwards deleted slots with ghost frequency.
		s.ghostStart--
		s.candidates[s.ghostStart] = candidate

		return
	}

	// Ghosts do not decay; pinned leaves do, so their frequency remains honest
	// if they are later unpinned. Decay precedes the protection decision.
	if s.epoch > 0 {
		decayLeaf(leaf, s.epoch)
	}

	if leaf.pinned || leaf.pins.Load() != 0 {
		return
	}

	if candidate.access < s.oldest.access {
		*s.oldest = candidate
	}

	// Zero disables frequency protection: every unpinned live leaf is eligible.
	if s.threshold == 0 || leaf.freq.Load() <= s.threshold {
		s.candidates[s.coldEnd] = candidate
		s.coldEnd++
	}
}

func oldestCandidateFirst[T any](a, b evictionCandidate[T]) int {
	return cmp.Compare(a.access, b.access)
}

// Consume sampled ghosts in recency order, skipping concurrent promotions.
// Both capacity trimming and protected-leaf fallback use this same cursor.
func (c *Critbit[T]) reclaimOldestGhost(sample *evictionSample[T]) bool {
	for sample.ghostStart < len(sample.candidates) {
		leaf := sample.candidates[sample.ghostStart].leaf
		sample.ghostStart++

		if c.reclaimGhost(leaf) {
			return true
		}
	}

	return false
}
