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
	"runtime"
	"slices"
	"sync"
	"testing"
	"weak"
)

// Reproduces the old retaining path: detached internal slots in a partially
// occupied current chunk pointed into an old leaf array after it was recycled.
func TestReclaimDetachedInternalDoesNotRetainLeafChunk(t *testing.T) {
	c := NewCritbit[struct{}]()

	t.Cleanup(func() { closeTestTrie(t, c) })

	c.leafArena.chunkSize, c.internalArena.chunkSize = 2, 8

	c.reapRunning.Store(true)
	defer c.reapRunning.Store(false)

	for _, key := range []string{"a", "b", "c", "d", "e"} {
		c.Insert(key, nil, NewTipSet(tip(1)))
	}

	old := weak.Make(c.findBestMatch(c.root.Load(), "a"))
	for _, key := range []string{"a", "b"} {
		c.Delete(key, c.Contains(key))
	}

	drainAllReaps(c)
	runtime.GC()
	runtime.GC()

	if weakNodeAlive(old) {
		t.Fatal("detached internal slots retain the recycled leaf array")
	}

	for _, key := range []string{"c", "d", "e"} {
		if c.Contains(key) == nil {
			t.Fatalf("surviving key %q lost", key)
		}
	}

	runtime.KeepAlive(c)
}

//go:noinline
func weakNodeAlive[T any](p weak.Pointer[critNode[T]]) bool { return p.Value() != nil }

func TestReclaimRelocationReleasesOldChunkAndPreservesEscapedValues(t *testing.T) {
	c := NewCritbit[int]()

	t.Cleanup(func() { closeTestTrie(t, c) })

	c.leafArena.chunkSize, c.internalArena.chunkSize = 4, 4

	c.reapRunning.Store(true)
	defer c.reapRunning.Store(false)

	for _, key := range []string{"a", "b", "c", "d", "e"} {
		c.Insert(key, nil, NewTipSet(tip(1)))
	}

	value := 42
	c.LoadOrStoreData("a", &value)
	escapedTips, escapedData := c.Contains("a"), c.LoadData("a")
	c.findBestMatch(c.root.Load(), "a").lastDecayEpoch.Store(7)

	old := weak.Make(c.findBestMatch(c.root.Load(), "a"))
	for _, key := range []string{"b", "c", "d"} {
		c.Delete(key, c.Contains(key))
	}

	drainAllReaps(c)

	if c.compactLeafChunks(1) != 1 {
		t.Fatal("fixture did not evacuate the sparse chunk")
	}

	runtime.GC()
	runtime.GC()

	if weakNodeAlive(old) {
		t.Fatal("relocated leaf chunk remains cache-owned")
	}

	if !escapedTips.Contains(tip(1)) || escapedData != &value || *escapedData != 42 {
		t.Fatal("relocation invalidated escaped immutable values")
	}

	if c.LoadData("a") != escapedData || c.Contains("a") != escapedTips {
		t.Fatal("relocated leaf did not preserve value identity")
	}

	if c.findBestMatch(c.root.Load(), "a").lastDecayEpoch.Load() != 7 {
		t.Fatal("relocation lost the leaf's decay history")
	}

	runtime.KeepAlive(c)
}

func TestReclaimInternalRelocationReleasesOldChunk(t *testing.T) {
	c := NewCritbit[struct{}]()

	t.Cleanup(func() { closeTestTrie(t, c) })

	c.leafArena.chunkSize, c.internalArena.chunkSize = 4, 4

	c.reapRunning.Store(true)
	defer c.reapRunning.Store(false)

	for _, key := range []string{"a", "b", "c", "d", "e"} {
		c.Insert(key, nil, NewTipSet(tip(1)))
	}

	for _, key := range []string{"b", "c", "d"} {
		c.Delete(key, c.Contains(key))
	}

	drainAllReaps(c)

	old := weak.Make(c.root.Load())
	if c.compactInternalChunks(1) != 1 {
		t.Fatal("fixture did not evacuate the sparse internal chunk")
	}

	runtime.GC()
	runtime.GC()

	if weakNodeAlive(old) {
		t.Fatal("relocated internal chunk remains cache-owned")
	}

	if got := c.Keys(); !slices.Equal(got, []string{"a", "e"}) {
		t.Fatalf("internal relocation changed surviving keys: %v", got)
	}

	runtime.KeepAlive(c)
}

func TestRangeCallbacksCanReclaimPaths(t *testing.T) {
	for _, kind := range []string{"all", "from", "prefix"} {
		t.Run(kind, func(t *testing.T) {
			c := NewCritbit[struct{}]()

			t.Cleanup(func() { closeTestTrie(t, c) })

			c.leafArena.chunkSize, c.internalArena.chunkSize = 4, 4

			c.reapRunning.Store(true)
			defer c.reapRunning.Store(false)

			var expected []string

			for i := range 200 {
				key := fmt.Sprintf("k:%03d", i)
				c.Insert(key, nil, NewTipSet(tip(1)))

				if kind != "from" || key > "k:050" {
					expected = append(expected, key)
				}
			}

			c.Insert("other", nil, NewTipSet(tip(1)))

			if kind != "prefix" {
				expected = append(expected, "other")
			}

			var got []string

			visit := func(key string) bool {
				got = append(got, key)
				c.Delete(key, c.Contains(key))
				drainAllReaps(c)
				c.compactLeafChunks(4)
				c.compactInternalChunks(4)

				return true
			}

			switch kind {
			case "all":
				c.Range(visit)
			case "from":
				c.RangeFrom("k:050", visit)
			case "prefix":
				c.RangePrefix("k:", visit)
			}

			if !slices.Equal(got, expected) {
				t.Fatalf("scan omitted or repeated keys across reclamation: got %v want %v", got, expected)
			}
		})
	}
}

func TestReclaimConcurrentReadersKeepStableKeys(t *testing.T) {
	c := NewCritbit[int]()

	t.Cleanup(func() { closeTestTrie(t, c) })

	c.leafArena.chunkSize, c.internalArena.chunkSize = 16, 16

	const stable = 64
	for i := range stable {
		key := fmt.Sprintf("stable:%03d", i)
		c.Insert(key, nil, NewTipSet(tip(1)))

		value := i
		c.LoadOrStoreData(key, &value)
	}

	stop := make(chan struct{})

	var readers sync.WaitGroup
	for range 4 {
		readers.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}

				for i := range stable {
					key := fmt.Sprintf("stable:%03d", i)
					if tips := c.Contains(key); tips == nil || !tips.Contains(tip(1)) {
						t.Errorf("stable key %q missing during reclamation", key)

						return
					}

					if data := c.LoadData(key); data == nil || *data != i {
						t.Errorf("stable payload %q changed", key)

						return
					}

					if exists, _ := c.TryClaimKey(key); !exists {
						t.Errorf("stable key %q could not be claimed", key)

						return
					}
				}

				first, firstOK, _ := c.FirstWithPrefix("stable:", false)
				last, lastOK, _ := c.LastWithPrefix("stable:", false)
				next, nextOK, _ := c.NextWithPrefix("stable:", "stable:000", false)

				prev, prevOK, _ := c.PrevWithPrefix("stable:", "stable:063", false)
				if !firstOK || !lastOK || !nextOK || !prevOK ||
					first != "stable:000" || last != "stable:063" || next != "stable:001" || prev != "stable:062" {
					t.Error("ordered lookup lost stable keys during reclamation")

					return
				}

				count := 0

				c.RangePrefix("stable:", func(string) bool {
					count++

					return true
				})

				if count != stable {
					t.Errorf("scan lost stable keys: %d", count)

					return
				}
			}
		})
	}

	for cycle := range 20 {
		for i := range 128 {
			c.Insert(fmt.Sprintf("churn:%d:%d", cycle, i), nil, NewTipSet(tip(2)))
		}

		for i := range 128 {
			key := fmt.Sprintf("churn:%d:%d", cycle, i)
			c.Delete(key, c.Contains(key))
		}

		drainAllReaps(c)
		c.compactLeafChunks(8)
		c.compactInternalChunks(8)
	}

	close(stop)
	readers.Wait()
}

func TestReclaimReleasesSharedKeysOnce(t *testing.T) {
	c := NewCritbit[int]()

	t.Cleanup(func() { closeTestTrie(t, c) })

	c.leafArena.chunkSize, c.internalArena.chunkSize = 4, 4

	c.reapRunning.Store(true)
	defer c.reapRunning.Store(false)

	owners := make(map[string]int)

	c.SetKeyOwnership(func(key string) string {
		owners[key]++

		return key
	}, func(key string) {
		if owners[key] <= 0 {
			t.Errorf("key %q released without ownership", key)
		}

		owners[key]--
	})

	for _, key := range []string{"a", "b", "c", "d", "e"} {
		c.Insert(key, nil, NewTipSet(tip(1)))
	}

	// Retiring one leaf must release its key even while survivors keep the
	// rest of the chunk resident. Replaying its stale queue entry is harmless.
	old := c.findBestMatch(c.root.Load(), "b")
	c.Delete("b", c.Contains("b"))
	drainAllReaps(c)

	if owners["b"] != 0 {
		t.Fatal("retired key remains owned by a partially occupied chunk")
	}

	c.reapMu.Lock()
	unlinked := c.unlinkLeaf(old)
	c.reapMu.Unlock()

	if unlinked || owners["b"] != 0 {
		t.Fatal("stale reap entry released a retired leaf twice")
	}

	for _, key := range []string{"c", "d"} {
		c.Delete(key, c.Contains(key))
	}

	drainAllReaps(c)

	if c.compactLeafChunks(1) != 1 {
		t.Fatal("fixture did not relocate the surviving leaf")
	}

	if owners["a"] != 1 || owners["e"] != 1 || owners["c"] != 0 || owners["d"] != 0 {
		t.Fatalf("relocation changed shared key ownership: %v", owners)
	}

	closeTestTrie(t, c)

	for key, count := range owners {
		if count != 0 {
			t.Errorf("closed trie retains %d references to %q", count, key)
		}
	}
}
