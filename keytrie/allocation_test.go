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

func TestRemoveTipsPreservesImmutableSource(t *testing.T) {
	c := NewCritbit[struct{}]()

	t.Cleanup(func() { closeTestTrie(t, c) })

	source := NewTipSet(tip(1), tip(2), tip(3))
	c.Insert("key", nil, source)
	c.RemoveTips("key", []EffectRef{tip(9)})

	if c.Contains("key") != source {
		t.Fatal("no-op removal replaced the tip set")
	}

	c.RemoveTips("key", []EffectRef{tip(2), tip(2), tip(9)})

	if got := c.Contains("key").Tips(); !slices.Equal(got, []EffectRef{tip(1), tip(3)}) {
		t.Fatalf("remaining tips = %v", got)
	}

	if !slices.Equal(source.Tips(), []EffectRef{tip(1), tip(2), tip(3)}) {
		t.Fatal("removal changed the escaped source")
	}

	c.RemoveTips("key", []EffectRef{tip(1), tip(3)})

	if got := c.Contains("key"); got == nil || got.Len() != 0 || c.Size() != 1 {
		t.Fatal("removing all tips must preserve the live key with an empty tip set")
	}

	if cap(c.Contains("key").tips) != 0 {
		t.Fatal("empty tip set retains the removed references' backing capacity")
	}
}

func BenchmarkRemoveTips(b *testing.B) {
	for _, missing := range []bool{false, true} {
		b.Run(fmt.Sprintf("missing=%v", missing), func(b *testing.B) {
			c := NewCritbit[struct{}]()

			b.Cleanup(func() { closeTestTrie(b, c) })

			source := NewTipSet(tip(1), tip(2), tip(3), tip(4))

			ref := tip(2)
			if missing {
				ref = tip(9)
			}

			c.Insert("key", nil, source)
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				c.RemoveTips("key", []EffectRef{ref})
				c.Insert("key", c.Contains("key"), source)
			}
		})
	}
}

func BenchmarkMatchPatternSparse(b *testing.B) {
	c := NewCritbit[struct{}]()

	b.Cleanup(func() { closeTestTrie(b, c) })

	for i := range 4096 {
		c.Insert(fmt.Sprintf("key:%04d", i), nil, NewTipSet(tip(1)))
	}

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		c.MatchPattern("*000*")
	}
}

func BenchmarkArenaGrow(b *testing.B) {
	for b.Loop() {
		var arena critbitArena[uint64]
		arena.init(64, nil)

		for range 4096 {
			arena.alloc()
		}
	}
}

func BenchmarkTipDeltaReplace(b *testing.B) {
	for _, overlap := range []int{0, 8, 16} {
		b.Run(fmt.Sprintf("overlap=%d", overlap), func(b *testing.B) {
			var c Critbit[struct{}]

			c.SetRefDelta(func(_, _ []EffectRef, _ *struct{}) {})

			var before, after [16]EffectRef

			for i := range 16 {
				before[i] = tip(uint64(i))
				after[i] = tip(uint64(i + 16 - overlap))
			}

			old, next := NewTipSet(before[:]...), NewTipSet(after[:]...)

			b.ReportAllocs()

			for b.Loop() {
				c.fireTipDelta(old, next, nil)
			}
		})
	}
}
