package keytrie

import (
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
)

func TestOrderedLookupMatchesSortedKeys(t *testing.T) {
	c := NewCritbit[struct{}]()

	t.Cleanup(func() { closeTestTrie(t, c) })

	rng := rand.New(rand.NewPCG(17, 29))
	keys := []string{"", "a", "a\x00", "a\x00\x00", "a\xff", "b", "b\x00"}

	for range 300 {
		key := make([]byte, 1+rng.IntN(8))
		for i := range key {
			key[i] = byte(rng.IntN(256))
		}

		keys = append(keys, string(key))
	}

	slices.Sort(keys)

	keys = slices.Compact(keys)
	for _, key := range keys {
		c.Insert(key, nil, NewTipSet(tip(1)))
	}

	// Include present, deleted, and never-inserted boundaries. Deletions stay
	// linked so both navigation and its live-leaf filtering are exercised.
	c.reapRunning.Store(true)
	defer c.reapRunning.Store(false)

	var live []string

	boundaries := make([]string, 0, 3*len(keys))
	for i, key := range keys {
		boundaries = append(boundaries, key, key+"\x00", key+"\xff")
		if i%3 == 0 {
			c.Delete(key, c.Contains(key))
		} else {
			live = append(live, key)
		}
	}

	for _, prefix := range []string{"", "a", "a\x00", "b", "\x00", "\xff", "absent"} {
		var matches []string

		for _, key := range live {
			if strings.HasPrefix(key, prefix) {
				matches = append(matches, key)
			}
		}

		for _, boundary := range boundaries {
			for _, reverse := range []bool{false, true} {
				index := 0
				if reverse {
					index = len(matches) - 1
				}

				if strings.HasPrefix(boundary, prefix) {
					index, _ = slices.BinarySearch(matches, boundary)
					if reverse {
						index--
					} else if index < len(matches) && matches[index] == boundary {
						index++
					}
				}

				var (
					got     string
					found   bool
					release ReleaseClaimFunc
				)
				if reverse {
					got, found, release = c.PrevWithPrefix(prefix, boundary, true)
				} else {
					got, found, release = c.NextWithPrefix(prefix, boundary, true)
				}

				wantFound := index >= 0 && index < len(matches)
				if found != wantFound || found && got != matches[index] {
					t.Fatalf("prefix=%q boundary=%q reverse=%v: got %q/%v, expected index %d in %q", prefix, boundary, reverse, got, found, index, matches)
				}

				if found {
					if release == nil {
						t.Fatal("successful claim has no release")
					}

					release()
				}
			}
		}
	}
}
