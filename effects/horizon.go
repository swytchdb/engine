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

package effects

import (
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	pb "github.com/swytchdb/engine/cluster/proto"
)

// horizonEntry tracks a single Bind that is in its horizon wait period.
type horizonEntry struct {
	group   *horizonGroup
	txnID   string
	bind    *pb.TransactionalBindEffect // immutable; owns keys and new tips
	waiters xsync.RBMutex
}

// horizonGroup tracks a group of competing Binds that share at least one
// consumed Tip on an overlapping key. Visibility is driven externally:
// flushTx calls MakeVisible / Abort on the originator side; handleRemoteBind
// schedules MakeVisible via ScheduleMakeVisible on the remote-arrival side
// (crash-fallback timer).
type horizonGroup struct {
	entries  []*horizonEntry
	bindKeys map[string][]Tip // key → consumed tips (for overlap detection)
	timer    *time.Timer      // scheduled MakeVisible (remote-arrival path)
}

// HorizonSet tracks Binds in their horizon wait period. Effects from
// invisible Binds are excluded from reconstruction until the horizon
// completes.
type HorizonSet struct {
	entries *xsync.Map[string, *horizonEntry] // txnID → entry (fast lookup)
	mu      sync.Mutex                        // owns groups, entry.group, and all lifecycle mutations
	groups  []*horizonGroup                   // all active groups
	engine  *Engine

	// Test hook: if non-nil, called instead of time.AfterFunc.
	afterFunc func(d time.Duration, f func()) *time.Timer
}

func newHorizonSet(engine *Engine) *HorizonSet {
	return &HorizonSet{
		entries: xsync.NewMap[string, *horizonEntry](),
		engine:  engine,
	}
}

// horizonFallbackWait is the crash-fallback duration used when the
// originator's verdict-snapshot never arrives (e.g., originator died
// between bind broadcast and snapshot emit). The primary signal is the
// snapshot itself via applySnapshotVerdicts; this timer only fires when
// that signal is lost. Generous on purpose — racing the snapshot risks
// premature visibility, holding longer just delays the rare crash case.
const horizonFallbackWait = 5 * time.Second

// Add registers a Bind in the invisible set. The bind stays invisible
// until MakeVisible, Abort, or a timer scheduled via ScheduleMakeVisible
// fires. If the bind's keys overlap with an existing group's consumed tips,
// it joins that group; otherwise a new group is created.
func (h *HorizonSet) Add(txnID string, bindOffset Tip, bind *pb.TransactionalBindEffect) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// A redelivered bind must not replace an entry with blocked readers.
	if _, exists := h.entries.Load(txnID); exists {
		return
	}

	var target *horizonGroup

	for _, group := range h.groups {
		if !group.overlaps(bind) {
			continue
		}

		if target == nil {
			target = group

			continue
		}

		for _, entry := range group.entries {
			entry.group = target
		}

		target.entries = append(target.entries, group.entries...)
		for key, tips := range group.bindKeys {
			target.bindKeys[key] = append(target.bindKeys[key], tips...)
		}

		group.entries = nil
	}

	if target == nil {
		target = &horizonGroup{bindKeys: make(map[string][]Tip, len(bind.GetKeys()))}
		h.groups = append(h.groups, target)
	} else {
		h.pruneGroups()
	}

	entry := &horizonEntry{group: target, txnID: txnID, bind: bind}
	entry.waiters.Lock()
	target.entries = append(target.entries, entry)

	for _, kb := range bind.GetKeys() {
		key := string(kb.GetKey())
		target.bindKeys[key] = append(target.bindKeys[key], fromPbRefs(kb.GetConsumedTips())...)
	}

	h.entries.Store(txnID, entry)

	slog.Debug("HorizonSet.Add", "txnID", txnID, "bindOffset", bindOffset,
		"group_size", len(target.entries))
}

// HorizonWait is a handle returned by WaitForClear. It carries the
// horizonEntry pointer and the RToken so Release can RUnlock the same
// mutex even after the entry has been removed from h.entries by
// MakeVisible or Abort.
type HorizonWait struct {
	entry *horizonEntry
	tok   *xsync.RToken
}

// WaitForClear blocks until the bind for txnID has been resolved
// (MakeVisible or Abort releases the entry's waiter lock). Returns nil if
// the bind is not in the invisible set — the caller may proceed without
// waiting. Returns a *HorizonWait the caller must Release when done.
func (h *HorizonSet) WaitForClear(txnID string) *HorizonWait {
	if entry, ok := h.entries.Load(txnID); ok {
		return &HorizonWait{entry: entry, tok: entry.waiters.RLock()}
	}

	return nil
}

// Release returns the read-lock token to the horizonEntry. Safe on nil.
func (w *HorizonWait) Release() {
	if w == nil {
		return
	}

	w.entry.waiters.RUnlock(w.tok)
}

// ScheduleMakeVisible schedules MakeVisible(txnID) to fire after `wait`.
// Used by handleRemoteBind as a crash-fallback for the remote-arrival
// wait — the primary signal is the originator's verdict-snapshot
// arriving via applySnapshotVerdicts, which short-circuits this timer.
// The timer only fires when the originator died between broadcasting
// the bind and broadcasting the verdict; we keep it generous (~5s) to
// avoid racing the snapshot under normal conditions. Resets any
// existing timer on the group; a later-joining bind extends the wait.
func (h *HorizonSet) ScheduleMakeVisible(txnID string, wait time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()

	entry, ok := h.entries.Load(txnID)
	if !ok {
		return
	}

	group := entry.group
	if group.timer != nil {
		group.timer.Stop()
	}

	fn := func() { h.MakeVisible(txnID) }
	if h.afterFunc != nil {
		group.timer = h.afterFunc(wait, fn)
	} else {
		group.timer = time.AfterFunc(wait, fn)
	}
}

// MakeVisible promotes an entire group: removes all txnIDs from the invisible
// set, cleans up pendingTxTips, evicts cache, and fires OnKeyDataAdded callbacks.
func (h *HorizonSet) MakeVisible(txnID string) {
	h.mu.Lock()

	entry, ok := h.entries.Load(txnID)
	if !ok {
		h.mu.Unlock()

		return
	}

	group := entry.group
	count := len(group.entries)

	var keys []string

	for _, member := range group.entries {
		for _, kb := range member.bind.GetKeys() {
			keys = append(keys, string(kb.GetKey()))
		}

		h.resolve(member)
	}

	group.entries = nil

	h.pruneGroups()
	h.mu.Unlock()

	// User callbacks run outside the lifecycle lock and once per live key.
	slices.Sort(keys)

	keys = slices.Compact(keys)
	for _, k := range keys {
		h.engine.ownFilterAdd(k)

		if h.engine.OnKeyDataAdded != nil {
			h.engine.OnKeyDataAdded(k)
		}
	}

	slog.Debug("HorizonSet.MakeVisible", "txnID", txnID,
		"promoted_count", count, "keys", len(keys))
}

// Abort removes a single entry from the horizon set without promoting it.
// Used by flushTx when the originator decides to abort after NACK
// processing. The bind effect is still in the local DAG and at peers;
// reconstruct's cross-key reachability is what skips it on read.
//
// Other entries in the same group are unaffected — they continue waiting
// on their own visibility trigger (timer or explicit MakeVisible).
func (h *HorizonSet) Abort(txnID string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	entry, ok := h.entries.Load(txnID)
	if !ok {
		return
	}

	group := entry.group
	i := slices.Index(group.entries, entry)
	group.entries = shrinkSlice(slices.Delete(group.entries, i, i+1))

	h.resolve(entry)
	h.pruneGroups()

	slog.Debug("HorizonSet.Abort", "txnID", txnID)
}

// IsInvisible returns true if the given txnID is in the invisible set.
func (h *HorizonSet) IsInvisible(txnID string) bool {
	_, ok := h.entries.Load(txnID)

	return ok
}

// Empty reports whether no txn is currently held invisible. When true,
// reconstruct can skip its per-read invisibility precompute: any bind
// reachable from a read's captured tips had its horizon entry added during
// ingest (before the index update that made it visible to the read), so an
// empty horizon at reconstruct setup proves no walked bind is invisible.
func (h *HorizonSet) Empty() bool {
	return h.entries.Size() == 0
}

// overlaps checks shared consumed tips without building a temporary set.
// The owning HorizonSet's mutex protects the group.
func (g *horizonGroup) overlaps(bind *pb.TransactionalBindEffect) bool {
	for _, kb := range bind.GetKeys() {
		for _, ref := range kb.GetConsumedTips() {
			if slices.Contains(g.bindKeys[string(kb.GetKey())], r(ref)) {
				return true
			}
		}
	}

	return false
}

// resolve releases one bind's bookkeeping before waking readers. Caller holds h.mu.
func (h *HorizonSet) resolve(entry *horizonEntry) {
	for _, kb := range entry.bind.GetKeys() {
		h.engine.pendingTxTips.Delete(r(kb.GetNewTip()))
	}

	h.entries.Delete(entry.txnID)
	entry.waiters.Unlock()
}

// pruneGroups releases completed/merged groups and their timer and key storage.
// Caller holds h.mu.
func (h *HorizonSet) pruneGroups() {
	h.groups = slices.DeleteFunc(h.groups, func(group *horizonGroup) bool {
		if len(group.entries) != 0 {
			return false
		}

		if group.timer != nil {
			group.timer.Stop()
			group.timer = nil
		}

		group.bindKeys = nil

		return true
	})
	h.groups = shrinkSlice(h.groups)
}
