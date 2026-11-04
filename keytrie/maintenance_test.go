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
	"slices"
	"testing"
)

func TestReapRemovesDeadSubtreesInOnePass(t *testing.T) {
	for _, survivors := range []int{0, 1, 17} {
		t.Run(fmt.Sprint(survivors), func(t *testing.T) {
			c := NewCritbit[struct{}]()

			t.Cleanup(func() { closeTestTrie(t, c) })

			c.reapRunning.Store(true)
			defer c.reapRunning.Store(false)

			var want []string

			for i := range 128 {
				key := fmt.Sprintf("key:%03d", i)
				c.Insert(key, nil, NewTipSet(tip(1)))

				if i < survivors {
					want = append(want, key)
				} else {
					c.Delete(key, c.Contains(key))
				}
			}

			if got := c.reap(); got != 128-survivors {
				t.Fatalf("one pass removed %d leaves, want %d", got, 128-survivors)
			}

			if got := c.Keys(); !slices.Equal(got, want) {
				t.Fatalf("survivors = %v, want %v", got, want)
			}

			if c.deletedCount.Load() != 0 {
				t.Fatal("dead leaves remain linked")
			}

			drainAllReaps(c) // queued pointers to retired leaves must be harmless

			if c.deletedCount.Load() != 0 || c.Size() != int64(survivors) {
				t.Fatal("stale queued leaves changed accounting")
			}
		})
	}
}

func TestTipDeltaTransitions(t *testing.T) {
	first := NewTipSet(tip(1), tip(2))
	second := NewTipSet(tip(2), tip(3))

	data := 42
	for _, tt := range []struct {
		name           string
		old, next      *TipSet
		data           *int
		added, removed []EffectRef
	}{
		{name: "insert", next: first, added: first.Tips()},
		{name: "delete", old: first, removed: first.Tips()},
		{name: "overlap", old: first, next: second, added: []EffectRef{tip(3)}, removed: []EffectRef{tip(1)}},
		{name: "disjoint", old: first, next: NewTipSet(tip(3), tip(4)), added: []EffectRef{tip(3), tip(4)}, removed: first.Tips()},
		{name: "subset", old: first, next: NewTipSet(tip(2)), removed: []EffectRef{tip(1)}},
		{name: "superset", old: NewTipSet(tip(2)), next: first, added: []EffectRef{tip(1)}},
		{name: "same", old: first, next: first},
		{name: "equal", old: first, next: NewTipSet(first.Tips()...)},
		{name: "payload", old: first, next: first, data: &data},
		{name: "empty", old: NewTipSet(), next: first, added: first.Tips()},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var c Critbit[int]

			calls := 0

			c.SetRefDelta(func(added, removed []EffectRef, dropped *int) {
				calls++

				if !slices.Equal(added, tt.added) || !slices.Equal(removed, tt.removed) || dropped != tt.data {
					t.Fatalf("delta = %v / %v / %v", added, removed, dropped)
				}
			})
			c.fireTipDelta(tt.old, tt.next, tt.data)

			wantCalls := 0
			if len(tt.added)+len(tt.removed) > 0 || tt.data != nil {
				wantCalls = 1
			}

			if calls != wantCalls {
				t.Fatalf("hook called %d times, want %d", calls, wantCalls)
			}
		})
	}
}

func TestTipDeltaStorageSurvivesLaterMutations(t *testing.T) {
	c := NewCritbit[struct{}]()

	t.Cleanup(func() { closeTestTrie(t, c) })

	type delta struct{ added, removed []EffectRef }

	var deltas []delta

	c.SetRefDelta(func(added, removed []EffectRef, _ *struct{}) {
		// Consumers may retain these immutable slices without copying them.
		deltas = append(deltas, delta{added, removed})
	})

	first := NewTipSet(tip(1), tip(2), tip(3))
	second := NewTipSet(tip(3), tip(4), tip(5))
	third := NewTipSet(tip(6), tip(7))

	c.Insert("key", nil, first)
	c.Insert("key", first, second)
	c.Insert("key", second, third)
	c.RemoveTips("key", []EffectRef{tip(6)})
	c.Delete("key", c.Contains("key"))

	want := []delta{
		{added: []EffectRef{tip(1), tip(2), tip(3)}},
		{added: []EffectRef{tip(4), tip(5)}, removed: []EffectRef{tip(1), tip(2)}},
		{added: []EffectRef{tip(6), tip(7)}, removed: []EffectRef{tip(3), tip(4), tip(5)}},
		{removed: []EffectRef{tip(6)}},
		{removed: []EffectRef{tip(7)}},
	}
	if len(deltas) != len(want) {
		t.Fatalf("got %d deltas, want %d", len(deltas), len(want))
	}

	for i, got := range deltas {
		if !slices.Equal(got.added, want[i].added) || !slices.Equal(got.removed, want[i].removed) {
			t.Fatalf("delta %d changed: got %v, want %v", i, got, want[i])
		}
	}
}

func BenchmarkTipDeltaInsertDelete(b *testing.B) {
	var c Critbit[struct{}]

	refs := 0

	c.SetRefDelta(func(added, removed []EffectRef, _ *struct{}) {
		refs += len(added) - len(removed)
	})

	tips := NewTipSet(tip(1), tip(2))

	b.ReportAllocs()

	for b.Loop() {
		c.fireTipDelta(nil, tips, nil)
		c.fireTipDelta(tips, nil, nil)
	}

	if refs != 0 {
		b.Fatal("unbalanced references")
	}
}

func BenchmarkEvictBatchReinsert(b *testing.B) {
	for _, want := range []int{1, 16, 128, 512} {
		b.Run(fmt.Sprint(want), func(b *testing.B) {
			c := NewCritbit[struct{}]()

			b.Cleanup(func() { closeTestTrie(b, c) })

			c.reapRunning.Store(true)
			defer c.reapRunning.Store(false)

			tips := NewTipSet(tip(1))

			c.SetEvictHooks(nil, func(key string, _ []EffectRef, _ *struct{}) {
				c.Insert(key, nil, tips)
			}, nil)

			for i := range 1024 {
				c.Insert(fmt.Sprintf("key:%04d", i), nil, tips)
			}

			b.ReportAllocs()

			for b.Loop() {
				if got := c.EvictBatch(want); got != want {
					b.Fatalf("evicted %d, want %d", got, want)
				}
			}
		})
	}
}

func BenchmarkReapLiveTree(b *testing.B) {
	c := NewCritbit[struct{}]()

	b.Cleanup(func() { closeTestTrie(b, c) })

	tips := NewTipSet(tip(1))
	for i := range 4096 {
		c.Insert(fmt.Sprintf("key:%04d", i), nil, tips)
	}

	b.ReportAllocs()

	for b.Loop() {
		c.reap()
	}
}

func BenchmarkCompactDenseInternal(b *testing.B) {
	c := NewCritbit[struct{}]()

	b.Cleanup(func() { closeTestTrie(b, c) })

	tips := NewTipSet(tip(1))
	for i := range 8192 {
		c.Insert(fmt.Sprintf("key:%04d", i), nil, tips)
	}

	b.ReportAllocs()

	for b.Loop() {
		c.compactInternalChunks(16)
	}
}

func BenchmarkCritbitLifecycle(b *testing.B) {
	b.ReportAllocs()

	for b.Loop() {
		c := NewCritbit[struct{}]()
		closeTestTrie(b, c)
	}
}
