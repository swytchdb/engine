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

import "strings"

// LogicalKeyPrefix is the trie namespace for application keys. Existing methods
// add it to inputs and remove it from outputs; callers keep passing their keys
// unchanged, including binary keys and keys that themselves start with this byte.
// Methods ending in Internal operate on raw trie keys without adding a prefix.
const LogicalKeyPrefix = "\x00"

// Insert stores tips for a logical key using CAS. See InsertInternal for CAS semantics.
func (c *Critbit[T]) Insert(key string, old, newTips *TipSet) (*TipSet, bool) {
	return c.InsertInternal(LogicalKeyPrefix+key, old, newTips)
}

// Delete removes a logical key only if its current tips match old.
func (c *Critbit[T]) Delete(key string, old *TipSet) bool {
	return c.DeleteInternal(LogicalKeyPrefix+key, old)
}

// DeleteAndSnapshot removes a logical key using CAS and returns its previous tips.
func (c *Critbit[T]) DeleteAndSnapshot(key string, old *TipSet) *TipSet {
	return c.DeleteAndSnapshotInternal(LogicalKeyPrefix+key, old)
}

// Contains returns a logical key's tips, or nil if missing or deleted.
func (c *Critbit[T]) Contains(key string) *TipSet {
	return c.ContainsInternal(LogicalKeyPrefix + key)
}

// RemoveTips atomically removes refs from a logical key's tip set.
func (c *Critbit[T]) RemoveTips(key string, refs []EffectRef) {
	c.RemoveTipsInternal(LogicalKeyPrefix+key, refs)
}

// Size returns the number of live logical keys.
func (c *Critbit[T]) Size() int64 {
	if c.closed.Load() {
		return 0
	}

	return c.logicalSize.Load()
}

// Range iterates over logical keys in lexicographic order, without their namespace prefix.
func (c *Critbit[T]) Range(fn func(key string) bool) {
	c.RangePrefix("", fn)
}

// RangeFrom iterates over logical keys after after; an empty after starts at the beginning.
func (c *Critbit[T]) RangeFrom(after string, fn func(key string) bool) {
	c.rangeKeys(LogicalKeyPrefix, LogicalKeyPrefix+after, after != "", true, fn)
}

// RangePrefix iterates over logical keys starting with prefix.
func (c *Critbit[T]) RangePrefix(prefix string, fn func(key string) bool) {
	c.rangeKeys(LogicalKeyPrefix+prefix, "", false, true, fn)
}

// Keys returns all logical keys in lexicographic order.
func (c *Critbit[T]) Keys() []string {
	if c.closed.Load() {
		return nil
	}

	keys := make([]string, 0, c.Size())
	c.Range(func(key string) bool {
		keys = append(keys, key)

		return true
	})

	return keys
}

// MatchPattern returns logical keys matching a Redis-style glob pattern.
func (c *Critbit[T]) MatchPattern(pattern string) []string {
	if c.closed.Load() {
		return nil
	}

	if pattern == "*" {
		return c.Keys()
	}

	keys := make([]string, 0)

	c.Range(func(key string) bool {
		if MatchGlob(key, pattern) {
			keys = append(keys, key)
		}

		return true
	})

	return keys
}

// FirstWithPrefix returns the smallest logical key starting with prefix.
func (c *Critbit[T]) FirstWithPrefix(prefix string, claim bool) (string, bool, ReleaseClaimFunc) {
	return c.seekPrefix(LogicalKeyPrefix+prefix, "", false, 0, claim, true)
}

// LastWithPrefix returns the largest logical key starting with prefix.
func (c *Critbit[T]) LastWithPrefix(prefix string, claim bool) (string, bool, ReleaseClaimFunc) {
	return c.seekPrefix(LogicalKeyPrefix+prefix, "", false, 1, claim, true)
}

// NextWithPrefix returns the next logical key after after starting with prefix.
func (c *Critbit[T]) NextWithPrefix(prefix, after string, claim bool) (string, bool, ReleaseClaimFunc) {
	return c.seekPrefix(LogicalKeyPrefix+prefix, LogicalKeyPrefix+after, strings.HasPrefix(after, prefix), 0, claim, true)
}

// PrevWithPrefix returns the previous logical key before before starting with prefix.
func (c *Critbit[T]) PrevWithPrefix(prefix, before string, claim bool) (string, bool, ReleaseClaimFunc) {
	return c.seekPrefix(LogicalKeyPrefix+prefix, LogicalKeyPrefix+before, strings.HasPrefix(before, prefix), 1, claim, true)
}

// TryClaimKey attempts to claim a logical key. Claims currently provide no exclusion.
func (c *Critbit[T]) TryClaimKey(key string) (bool, ReleaseClaimFunc) {
	return c.TryClaimKeyInternal(LogicalKeyPrefix + key)
}

// GetHeadHint returns the head hint for a logical prefix (currently always empty).
func (c *Critbit[T]) GetHeadHint(prefix string) string {
	return strings.TrimPrefix(c.GetHeadHintInternal(LogicalKeyPrefix+prefix), LogicalKeyPrefix)
}

// SetHeadHint sets a logical prefix's head hint (currently a no-op).
func (c *Critbit[T]) SetHeadHint(prefix, key string) {
	c.SetHeadHintInternal(LogicalKeyPrefix+prefix, LogicalKeyPrefix+key)
}

// GetTailHint returns the tail hint for a logical prefix (currently always empty).
func (c *Critbit[T]) GetTailHint(prefix string) string {
	return strings.TrimPrefix(c.GetTailHintInternal(LogicalKeyPrefix+prefix), LogicalKeyPrefix)
}

// SetTailHint sets a logical prefix's tail hint (currently a no-op).
func (c *Critbit[T]) SetTailHint(prefix, key string) {
	c.SetTailHintInternal(LogicalKeyPrefix+prefix, LogicalKeyPrefix+key)
}

// Snapshot copies the logical key tip index. Payloads are not copied, and
// concurrent mutations have the same weak consistency as Range.
func (c *Critbit[T]) Snapshot() KeyIndex {
	snap := NewCritbit[T]()

	c.Range(func(key string) bool {
		if tips := c.Contains(key); tips != nil {
			snap.Insert(key, nil, tips)
		}

		return true
	})

	return snap
}

// Pin adds a do-not-evict hold to a live logical key. See PinInternal.
func (c *Critbit[T]) Pin(key string) bool {
	return c.PinInternal(LogicalKeyPrefix + key)
}

// Unpin releases a hold on a logical key. See UnpinInternal.
func (c *Critbit[T]) Unpin(key string) bool {
	return c.UnpinInternal(LogicalKeyPrefix + key)
}

// LoadData returns the logical key's payload without installing one. See LoadDataInternal.
func (c *Critbit[T]) LoadData(key string) *T {
	return c.LoadDataInternal(LogicalKeyPrefix + key)
}

// LoadOrStoreData loads or installs a logical key's payload. See LoadOrStoreDataInternal.
func (c *Critbit[T]) LoadOrStoreData(key string, def *T) (*T, bool) {
	return c.LoadOrStoreDataInternal(LogicalKeyPrefix+key, def)
}

// SetKeyOwnership installs ownership callbacks for unprefixed logical keys.
// Returned logical keys preserve the owner's canonical allocation. The trie
// accounts separately for the prefixed key used for navigation. Configure only
// on an empty trie before concurrent use. Raw keys outside the logical namespace
// are owned by the trie and do not invoke these callbacks.
func (c *Critbit[T]) SetKeyOwnership(retain func(string) string, release func(string)) {
	if retain == nil || release == nil {
		panic("keytrie: key ownership requires retain and release")
	}

	c.retainKey, c.releaseKey = retain, release
}

// SetEvictHooks installs eviction hooks receiving unprefixed logical keys.
// Raw keys outside the logical namespace are pinned; use SetEvictHooksInternal
// to manage eviction across namespaces. See it for callback lifetime rules.
func (c *Critbit[T]) SetEvictHooks(decider func(string) bool, notify func(string, []EffectRef, *T), pressure func() bool) {
	var notifyRaw func(string, []EffectRef, *T)
	if notify != nil {
		notifyRaw = func(key string, tips []EffectRef, data *T) {
			if logical, ok := strings.CutPrefix(key, LogicalKeyPrefix); ok {
				notify(logical, tips, data)
			}
		}
	}

	c.SetEvictHooksInternal(func(key string) bool {
		logical, ok := strings.CutPrefix(key, LogicalKeyPrefix)

		return ok && (decider == nil || decider(logical))
	}, notifyRaw, pressure)
}

func (c *Critbit[T]) addSize(key string, delta int64) {
	c.size.Add(delta)

	if strings.HasPrefix(key, LogicalKeyPrefix) {
		c.logicalSize.Add(delta)
	}
}

func (c *Critbit[T]) retainLeafKey(leaf *critNode[T], key string) {
	leaf.key = key

	if c.retainKey == nil {
		return
	}

	if logical, ok := strings.CutPrefix(key, LogicalKeyPrefix); ok {
		leaf.ownedKey = c.retainKey(logical)
	}
}

func (c *Critbit[T]) releaseLeafKey(leaf *critNode[T]) {
	if c.releaseKey != nil && strings.HasPrefix(leaf.key, LogicalKeyPrefix) {
		c.releaseKey(leaf.ownedKey)
	}

	leaf.ownedKey = ""
}

// Caller holds reapMu and has already restricted traversal to logical keys.
func (c *Critbit[T]) logicalLeafKey(leaf *critNode[T]) string {
	if c.retainKey != nil {
		return leaf.ownedKey
	}

	return leaf.key[len(LogicalKeyPrefix):]
}
