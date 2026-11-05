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
	"sync"
	"testing"
)

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

func TestDeleteAndSnapshotConcurrentInsert(t *testing.T) {
	for _, useCurrent := range []bool{false, true} {
		name := "absent"
		if useCurrent {
			name = "current"
		}

		t.Run(name, func(t *testing.T) {
			c := NewCritbit[struct{}]()

			t.Cleanup(func() { closeTestTrie(t, c) })
			c.reapRunning.Store(true) // keep completed deletions linked for revival

			for i := range 10000 {
				base, replacement := NewTipSet(tip(1)), NewTipSet(tip(2))
				c.Insert("key", c.Contains("key"), base)

				var old, removed *TipSet
				if useCurrent {
					old = base
				}

				var (
					inserted bool
					wg       sync.WaitGroup
				)

				start := make(chan struct{})

				wg.Go(func() {
					<-start

					removed = c.DeleteAndSnapshot("key", base)
				})
				wg.Go(func() {
					<-start

					_, inserted = c.Insert("key", old, replacement)
				})

				close(start)
				wg.Wait()

				if !useCurrent && inserted && removed == nil {
					t.Fatalf("iteration %d: insert with old=nil replaced live tips while deletion failed", i)
				}

				wantSize := int64(1)
				wantTips := base

				if inserted {
					wantTips = replacement
				} else if removed != nil {
					wantSize, wantTips = 0, nil
				}

				if got := c.Contains("key"); got != wantTips {
					t.Fatalf("iteration %d: tips = %p, want %p", i, got, wantTips)
				}

				if live, dead := c.Size(), c.deletedCount.Load(); live != wantSize || dead != 1-wantSize {
					t.Fatalf("iteration %d: live=%d deleted=%d, want live=%d deleted=%d", i, live, dead, wantSize, 1-wantSize)
				}
			}
		})
	}
}

func TestEvictionConcurrentInsert(t *testing.T) {
	c := NewCritbit[struct{}]()

	t.Cleanup(func() { closeTestTrie(t, c) })
	c.reapRunning.Store(true)
	c.SetEvictHooks(func(string) bool { return true }, nil, nil)

	for i := range 10000 {
		base, replacement := NewTipSet(tip(1)), NewTipSet(tip(2))
		c.Insert("key", c.Contains("key"), base)

		var (
			inserted bool
			wg       sync.WaitGroup
		)

		start := make(chan struct{})

		wg.Go(func() {
			<-start

			c.EvictBounded()
		})
		wg.Go(func() {
			<-start

			_, inserted = c.Insert("key", nil, replacement)
		})

		close(start)
		wg.Wait()

		wantSize := int64(0)

		var wantTips *TipSet

		if inserted {
			wantSize, wantTips = 1, replacement
		}

		if got := c.Contains("key"); got != wantTips {
			t.Fatalf("iteration %d: tips = %p, want %p", i, got, wantTips)
		}

		if live, ghosts, dead := c.Size(), c.ghostCount.Load(), c.deletedCount.Load(); live != wantSize || ghosts != 1-wantSize || dead != 0 {
			t.Fatalf("iteration %d: live=%d ghosts=%d deleted=%d, want live=%d ghosts=%d deleted=0", i, live, ghosts, dead, wantSize, 1-wantSize)
		}
	}
}
