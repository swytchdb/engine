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
	"strings"
	"testing"
	"unsafe"
)

func TestLogicalKeysAndRawKeysCoexist(t *testing.T) {
	c := NewCritbit[int]()

	t.Cleanup(func() { closeTestTrie(t, c) })

	logical, raw := NewTipSet(tip(1), tip(2)), NewTipSet(tip(3))
	c.Insert("animals", nil, logical)
	c.InsertInternal("animals", nil, raw)

	if c.Contains("animals") != logical || c.ContainsInternal("animals") != raw || c.ContainsInternal(LogicalKeyPrefix+"animals") != logical {
		t.Fatal("logical and raw identities collided")
	}

	if c.Size() != 1 || c.SizeInternal() != 2 {
		t.Fatal("incorrect namespace counts")
	}

	if got, ok := c.Insert("animals", raw, raw); ok || got != logical {
		t.Fatal("logical CAS used the raw key's tips")
	}

	if got, ok := c.InsertInternal("animals", logical, logical); ok || got != raw {
		t.Fatal("raw CAS used the logical key's tips")
	}

	one, two := 1, 2
	c.LoadOrStoreData("animals", &one)
	c.LoadOrStoreDataInternal("animals", &two)

	if c.LoadData("animals") != &one || c.LoadDataInternal("animals") != &two {
		t.Fatal("payloads crossed namespaces")
	}

	if !c.Pin("animals") || !c.PinInternal("animals") || !c.Unpin("animals") || !c.UnpinInternal("animals") {
		t.Fatal("pin/unpin failed")
	}

	for _, claim := range []func(string) (bool, ReleaseClaimFunc){c.TryClaimKey, c.TryClaimKeyInternal} {
		exists, release := claim("animals")
		if !exists || release == nil {
			t.Fatal("claim failed")
		}

		release()
	}

	c.RemoveTips("animals", []EffectRef{tip(1)})

	if !slices.Equal(c.Contains("animals").Tips(), []EffectRef{tip(2)}) || c.ContainsInternal("animals") != raw {
		t.Fatal("logical tip removal touched raw state")
	}

	c.RemoveTipsInternal("animals", []EffectRef{tip(3)})

	if c.ContainsInternal("animals").Len() != 0 || c.Contains("animals").Len() != 1 {
		t.Fatal("raw tip removal touched logical state")
	}

	logicalSnapshot := c.Snapshot()

	t.Cleanup(func() { closeTestTrie(t, logicalSnapshot) })

	rawSnapshot := c.SnapshotInternal().(*Critbit[int])

	t.Cleanup(func() { closeTestTrie(t, rawSnapshot) })

	if logicalSnapshot.Size() != 1 || logicalSnapshot.(*Critbit[int]).SizeInternal() != 1 || rawSnapshot.SizeInternal() != 2 || rawSnapshot.Size() != 1 {
		t.Fatal("snapshot scope is wrong")
	}

	if !c.Delete("animals", c.Contains("animals")) || c.ContainsInternal("animals") == nil || c.Size() != 0 || c.SizeInternal() != 1 {
		t.Fatal("logical deletion affected raw key")
	}

	if c.DeleteAndSnapshotInternal("animals", c.ContainsInternal("animals")) == nil || c.SizeInternal() != 0 {
		t.Fatal("raw deletion failed")
	}

	if rawSnapshot.ContainsInternal("animals") == nil || logicalSnapshot.Contains("animals") == nil {
		t.Fatal("mutations changed snapshot")
	}
}

func TestLogicalTraversalWithInternalKeys(t *testing.T) {
	c := NewCritbit[struct{}]()

	t.Cleanup(func() { closeTestTrie(t, c) })

	keys := make([]string, 0, 157)
	keys = append(keys, "", "\x00", "\x00animals", "a\x00b", "\xff", "*", "[a]")

	for i := range 150 { // Cross the traversal batch boundary.
		keys = append(keys, fmt.Sprintf("animal:%03d", i))
	}

	slices.Sort(keys)

	for _, key := range keys {
		c.Insert(key, nil, NewTipSet(tip(1)))
	}

	for _, key := range []string{"", "animal:000", "animal:149", "\xffinternal"} {
		c.InsertInternal(key, nil, NewTipSet(tip(2)))
	}

	if got := c.Keys(); !slices.Equal(got, keys) {
		t.Fatalf("logical keys = %q, want %q", got, keys)
	}

	if got := c.MatchPattern("*"); !slices.Equal(got, keys) {
		t.Fatal("glob exposed internal keys")
	}

	for _, key := range keys {
		if c.Contains(key) != c.ContainsInternal(LogicalKeyPrefix+key) {
			t.Fatalf("binary key %q was not prefixed exactly once", key)
		}
	}

	for _, prefix := range []string{"", "animal:", "\x00", "\xff", "absent"} {
		var want, got []string

		for _, key := range keys {
			if strings.HasPrefix(key, prefix) {
				want = append(want, key)
			}
		}

		c.RangePrefix(prefix, func(key string) bool {
			got = append(got, key)

			return true
		})

		if !slices.Equal(got, want) {
			t.Fatalf("prefix %q: got %q, want %q", prefix, got, want)
		}

		first, ok, release := c.FirstWithPrefix(prefix, true)
		if ok != (len(want) > 0) || ok && (first != want[0] || release == nil) {
			t.Fatalf("first for prefix %q: %q, %v", prefix, first, ok)
		}

		if release != nil {
			release()
		}

		last, ok, _ := c.LastWithPrefix(prefix, false)
		if ok != (len(want) > 0) || ok && last != want[len(want)-1] {
			t.Fatalf("last for prefix %q: %q, %v", prefix, last, ok)
		}
	}

	var after []string

	c.RangeFrom("animal:100", func(key string) bool {
		after = append(after, key)

		return true
	})

	start, _ := slices.BinarySearch(keys, "animal:101")
	if !slices.Equal(after, keys[start:]) {
		t.Fatal("RangeFrom leaked or skipped keys")
	}

	if next, ok, _ := c.NextWithPrefix("animal:", "animal:100", false); !ok || next != "animal:101" {
		t.Fatal("incorrect next key")
	}

	if prev, ok, _ := c.PrevWithPrefix("animal:", "animal:100", false); !ok || prev != "animal:099" {
		t.Fatal("incorrect previous key")
	}

	if got := c.MatchPattern("animal:00?"); len(got) != 10 {
		t.Fatalf("glob returned %q", got)
	}

	if got := c.MatchPatternInternal("animal:*"); !slices.Equal(got, []string{"animal:000", "animal:149"}) {
		t.Fatalf("raw glob returned %q", got)
	}

	if first, ok, _ := c.FirstWithPrefixInternal("animal:", false); !ok || first != "animal:000" {
		t.Fatal("raw navigation failed")
	}

	if next, ok, _ := c.NextWithPrefixInternal("animal:", "animal:000", false); !ok || next != "animal:149" {
		t.Fatal("raw next failed")
	}

	if prev, ok, _ := c.PrevWithPrefixInternal("animal:", "animal:149", false); !ok || prev != "animal:000" {
		t.Fatal("raw previous failed")
	}

	if last, ok, _ := c.LastWithPrefixInternal("animal:", false); !ok || last != "animal:149" {
		t.Fatal("raw last failed")
	}

	c.Range(nil)
	c.RangePrefix("", nil)
	c.RangeFrom("", nil)
	c.RangeInternal(nil)
	c.RangePrefixInternal("", nil)
	c.RangeFromInternal("", nil)
	closeTestTrie(t, c)

	if c.Size() != 0 || c.SizeInternal() != 0 || c.Keys() != nil || c.KeysInternal() != nil {
		t.Fatal("closed trie retained keys")
	}
}

func TestLogicalCallbacksKeepOriginalKeys(t *testing.T) {
	c := NewCritbit[int]()

	t.Cleanup(func() { closeTestTrie(t, c) })

	canonical := strings.Clone("__swytch:keep")
	refs := map[string]int{}

	c.SetKeyOwnership(func(key string) string {
		refs[key]++
		if key == canonical {
			return canonical
		}

		return key
	}, func(key string) { refs[key]-- })

	var evicted []string

	c.SetEvictHooks(func(key string) bool {
		return !strings.HasPrefix(key, "__swytch:")
	}, func(key string, _ []EffectRef, _ *int) {
		evicted = append(evicted, key)
		if c.Contains(key) != nil {
			t.Error("callback key is still live")
		}
	}, nil)
	c.Insert(canonical, nil, NewTipSet(tip(1)))
	c.Insert("victim", nil, NewTipSet(tip(2)))
	c.InsertInternal("node/123", nil, NewTipSet(tip(3)))
	c.RangePrefix("__swytch:", func(key string) bool {
		if unsafe.StringData(key) != unsafe.StringData(canonical) {
			t.Error("range lost shared allocation")
		}

		return true
	})

	key, _, _ := c.FirstWithPrefix("__swytch:", false)
	if unsafe.StringData(key) != unsafe.StringData(canonical) {
		t.Fatal("navigation lost shared allocation")
	}

	if c.EvictBatch(3) != 1 || !slices.Equal(evicted, []string{"victim"}) {
		t.Fatalf("evicted = %q", evicted)
	}

	if c.Size() != 1 || c.SizeInternal() != 2 {
		t.Fatal("eviction miscounted namespaces")
	}

	c.Insert("victim", nil, NewTipSet(tip(4)))

	if c.Size() != 2 || c.SizeInternal() != 3 {
		t.Fatal("revival miscounted namespaces")
	}

	if refs[canonical] != 1 || refs["victim"] != 1 || refs["node/123"] != 1 || len(refs) != 3 {
		t.Fatalf("ownership callbacks saw wrong keys: %v", refs)
	}

	closeTestTrie(t, c)

	for key, count := range refs {
		if count != 0 {
			t.Errorf("%q retains %d owners", key, count)
		}
	}
}

func TestInternalEvictionWithSharedKeyOwnership(t *testing.T) {
	c := NewCritbit[struct{}]()

	t.Cleanup(func() { closeTestTrie(t, c) })

	refs := map[string]int{}

	c.SetKeyOwnership(func(key string) string {
		refs[key]++

		return key
	}, func(key string) { refs[key]-- })

	var evicted []string

	c.SetEvictHooksInternal(func(string) bool { return true }, func(key string, _ []EffectRef, _ *struct{}) { evicted = append(evicted, key) }, nil)
	c.Insert("animal", nil, NewTipSet(tip(1)))
	c.InsertInternal("node", nil, NewTipSet(tip(2)))
	c.InsertInternal("", nil, NewTipSet(tip(3)))
	c.Insert("", nil, NewTipSet(tip(4)))

	if len(refs) != 3 || refs["animal"] != 1 || refs["node"] != 1 || refs[""] != 2 {
		t.Fatalf("key owners = %v", refs)
	}

	if c.EvictBatch(4) != 4 || c.Size() != 0 || c.SizeInternal() != 0 {
		t.Fatal("raw eviction failed")
	}

	slices.Sort(evicted)

	if !slices.Equal(evicted, []string{"", LogicalKeyPrefix, LogicalKeyPrefix + "animal", "node"}) {
		t.Fatalf("raw callback keys = %q", evicted)
	}

	closeTestTrie(t, c)

	for key, count := range refs {
		if count != 0 {
			t.Errorf("%q retains %d owners", key, count)
		}
	}
}
