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

import "testing"

func TestReapQueueAllocatedOnlyForDeletion(t *testing.T) {
	c := NewCritbit[struct{}]()

	t.Cleanup(func() { closeTestTrie(t, c) })
	c.reapRunning.Store(true) // inspect the queue before maintenance consumes it

	tips := NewTipSet(tip(1))
	c.Insert("key", nil, tips)

	if c.reapQueue.Load() != nil {
		t.Fatal("inserting a live key allocated the deletion queue")
	}

	if !c.Delete("key", tips) || c.reapQueue.Load() == nil {
		t.Fatal("deletion did not publish its pending unlink")
	}

	if c.drainReap() != 1 || c.root.Load() != nil {
		t.Fatal("pending deletion was not unlinked")
	}

	closeTestTrie(t, c)

	if c.reapQueue.Load() != nil {
		t.Fatal("close retained the deletion queue")
	}
}

func TestDeleteCallbackCanClose(t *testing.T) {
	c := NewCritbit[struct{}]()

	t.Cleanup(func() { closeTestTrie(t, c) })
	c.SetRefDelta(func(_, removed []EffectRef, _ *struct{}) {
		if len(removed) > 0 {
			closeTestTrie(t, c)
		}
	})

	tips := NewTipSet(tip(1))
	c.Insert("key", nil, tips)

	if !c.Delete("key", tips) || !c.Closed() {
		t.Fatal("deletion callback did not close the trie")
	}

	if c.reapQueue.Load() != nil {
		t.Fatal("deletion queued work after the callback closed the trie")
	}
}
