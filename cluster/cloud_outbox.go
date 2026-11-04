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

package cluster

import (
	"fmt"
	"log/slog"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
)

// pendingKeyEntry is one key's slice of the outbox: its un-acked tips and
// whether the entry holds the key's do-not-evict pin.
type pendingKeyEntry struct {
	tips   map[effects.Tip]struct{}
	pinned bool
}

type pendingUpload struct {
	effect *pb.Effect // keeps the effect reachable while its pool reference is held
	// Only locally-authored uploads expand ancestry. Fetch replies upload
	// exactly the requested effect; Cloud must ask for any deeper holes.
	expand bool
}

// cloudUploadable reports whether an effect belongs in the cloud at all.
// Wire-only kinds never persist, so they are never a mint the hook sees and
// never a dep the ancestry walk reaches; the guard keeps a future mint-path
// change from silently uploading them.
func cloudUploadable(eff *pb.Effect) bool {
	if sub := eff.GetSubscription(); sub != nil && (sub.GetDiscovery() || sub.GetEphemeral()) {
		return false
	}

	return eff.GetPubsubMessage() == nil
}

// handleLocalEffect is the engine's OnLocalEffect hook: enqueue the mint with
// whatever locally-authored history the cloud is still missing, and return.
// Runs inside the emit path, so it must not block — the walk is one dep level
// here and continues off-path as each entry is sent.
func (cs *CloudSync) handleLocalEffect(offset effects.Tip, eff *pb.Effect) {
	if !cloudUploadable(eff) {
		return
	}
	// Queue the first ancestry level before the new head. The sender continues
	// the walk off-path and keeps moving each parent behind any newly discovered
	// deps, producing root-to-head wire order instead of advertising the head
	// first and trying to fill its hole afterwards.
	cs.expandAncestry(eff)
	// OnLocalEffect fires synchronously in emit, after PutSized installed the
	// vertex with its creation ref — the incref cannot find it missing or
	// claimed, and a freshly-allocated offset cannot already be queued. If it
	// ever is, the ref protocol is broken and retire's Decref would steal
	// another holder's reference (premature reclaim of a vertex someone still
	// reads); fail at the violation, same policy as decref's underflow panic.
	if !cs.enqueueUpload(offset, eff, true, true) {
		panic(fmt.Sprintf("cloud sync: outbox enqueue failed for just-emitted %v — vertex missing, claimed, or already queued at emit time", offset))
	}

	cs.wakeSender()
}

// enqueueUpload adds one effect to the outbox: the pool Incref that is the
// outbox's DAG reference (the effect stays resident across evictions until the
// cloud's ack releases it), the per-key index entry, and the send queue slot.
//
// frontier marks the effect as one of this node's own un-acked writes, the
// tips pendingTipsFor answers reads with and the reason the key holds an
// eviction pin. Ancestry and fetch replies are interior uploads rather than
// this node's new frontier, so they take neither: naming one as a tip could hand
// a reader a frontier containing an effect and its own ancestor.
//
// Reports false when the tip is already queued or its vertex could not be
// referenced — evicted, or claimed for reclamation. For an ancestor that
// simply means the cloud keeps whatever hole it has there; nothing local can
// fill it.
func (cs *CloudSync) enqueueUpload(tip effects.Tip, eff *pb.Effect, frontier, expand bool) bool {
	// Anything cloud-bound is filter-positive from the moment it's enqueued,
	// even if it's later evicted locally before the cloud's push reflects it.
	// Except subscriptions: ensureSubscribed mints one on every read of an
	// absent key, moments before that same read reaches the cloudMayHold gate
	// — letting it into the filter would open the gate for the very key being
	// missed, turning every first-touch read into a wasted consult of a
	// stateless chain.
	if eff.GetSubscription() == nil {
		name := CloudKeyName(cs.keyNameKey, eff.GetKey())
		cs.filterMu.Lock()
		cs.filterOwn.add(name)
		cs.filterMu.Unlock()
	}

	// Reference before claiming the slot, so a duplicate loses its own ref
	// rather than the ref the existing entry is holding.
	if !cs.engine.EffectCache().Incref(tip) {
		return false
	}

	cs.mu.Lock()
	if _, dup := cs.pending[tip]; dup {
		cs.mu.Unlock()
		cs.engine.EffectCache().Decref(tip)

		return false
	}

	if cs.pending == nil {
		cs.pending = make(map[effects.Tip]pendingUpload)
	}

	cs.pending[tip] = pendingUpload{effect: eff, expand: expand}

	if frontier {
		k := string(eff.GetKey())

		entry := cs.pendingKeys[k]
		if entry == nil {
			if cs.pendingKeys == nil {
				cs.pendingKeys = make(map[string]*pendingKeyEntry)
			}

			// First un-acked upload on this key: hold it in the index until the
			// cloud acks everything. Evicting it early frees almost nothing (the
			// outbox pins the bytes) but unsubscribes and drops the tips, making
			// data only this node holds invisible to every reader in the cluster
			// until the upload drains. Pin/Unpin ride cs.mu so the first-effect /
			// last-ack transitions can't interleave their index calls. PinKey
			// reports false when the key has no live leaf (the eviction path's own
			// unsubscribe mint) — nothing to keep findable, nothing to unpin later.
			entry = &pendingKeyEntry{
				tips:   make(map[effects.Tip]struct{}),
				pinned: cs.engine.PinKey(k),
			}
			cs.pendingKeys[k] = entry
		}

		entry.tips[tip] = struct{}{}
	}

	cs.sendQ = append(cs.sendQ, tip)
	backlog := len(cs.pending)
	cs.mu.Unlock()
	cloudOutboxEnqueuedTotal.Inc()
	cloudOutboxPending.Set(float64(backlog))

	if backlog%backlogWarnEvery == 0 {
		slog.Warn("cloud sync: upload backlog growing", "pending", backlog)
	}

	return true
}

// expandAncestry queues the locally-authored dep-ancestry of eff that the cloud
// has not seen.
//
// The cloud only ever receives what some node uploads, and a node uploads only
// what it mints. An effect whose author died with an un-acked outbox — or that
// predates this node's cloud attach — never reaches the cloud, and every
// descendant we upload then names a dep the cloud cannot resolve. That hole is
// permanent: no retry heals it, and a bootstrapping node walking the cloud
// frontier dies on the missing blob with no local state to fall back on.
//
// So a local upload carries its own node's unsent history, not just its tip.
// Each same-author dep we do not know the cloud holds joins the outbox, and
// expanding again as that dep is sent walks the local closure out to the first
// effect the cloud already has (or the first one eviction already took from us
// — a hole nothing local can fill). Foreign refs are Cloud's responsibility:
// they enter the outbox only through an explicit FetchRequest.
//
// Referencing each dep before walking past it is what keeps the walk alive
// across a stream outage: the frontier it stopped at is pinned in the pool, so
// the closure resumes on reconnect instead of restarting into evicted ancestry.
func (cs *CloudSync) expandAncestry(eff *pb.Effect) []effects.Tip {
	var queued []effects.Tip

	for _, dep := range eff.GetDeps() {
		// A node owes Cloud only the history it authored. Peer effects enter this
		// outbox exclusively through an explicit FetchRequest; merely naming one
		// as a dependency is not authority to upload it.
		if dep.GetNodeId() != uint64(cs.engine.NodeID()) {
			continue
		}

		tip := effects.Tip{dep.GetNodeId(), dep.GetOffset()}
		if cs.cloudHolds(tip) {
			continue
		}

		anc, ok := cs.engine.EffectCache().Get(tip)
		if !ok {
			continue
		}

		if !cloudUploadable(anc) {
			continue
		}

		if cs.enqueueUpload(tip, anc, false, true) {
			cloudAncestryUploadedTotal.Inc()

			queued = append(queued, tip)
		}
	}

	return queued
}

// deferUploadBehind moves tip behind deps that its ancestry walk just queued,
// ahead of unrelated work already in the FIFO. Repeating this as each dep is
// considered turns the incremental one-level walk into root-to-head send order
// without putting an unbounded traversal on the emit path.
func (cs *CloudSync) deferUploadBehind(tip effects.Tip, deps []effects.Tip) {
	depSet := make(map[effects.Tip]struct{}, len(deps))
	for _, dep := range deps {
		depSet[dep] = struct{}{}
	}

	cs.mu.Lock()
	deferred := make([]effects.Tip, 0, len(cs.sendQ)+1)
	deferred = append(deferred, deps...)

	deferred = append(deferred, tip)
	for _, queued := range cs.sendQ {
		if queued == tip {
			continue
		}

		if _, moved := depSet[queued]; moved {
			continue
		}

		deferred = append(deferred, queued)
	}

	cs.sendQ = deferred
	cs.mu.Unlock()
}

// tipGenSet is a bounded exact set of tips: two generations, the older dropped
// wholesale once the newer fills. Membership must be exact — a false positive
// would silently skip an ancestor the cloud is missing, which is the hole the
// walk exists to close — so an approximate filter is not usable here.
type tipGenSet struct {
	cur, prev map[effects.Tip]struct{}
}

func (s *tipGenSet) add(t effects.Tip) {
	if s.cur == nil {
		s.cur = make(map[effects.Tip]struct{})
	}

	if len(s.cur) >= cloudHasGenSize {
		s.prev, s.cur = s.cur, make(map[effects.Tip]struct{})
	}

	s.cur[t] = struct{}{}
}

func (s *tipGenSet) has(t effects.Tip) bool {
	if _, ok := s.cur[t]; ok {
		return true
	}

	_, ok := s.prev[t]

	return ok
}

// markCloudHolds records that the cloud holds tip: it acked our upload, or it
// served the effect to us in the first place.
func (cs *CloudSync) markCloudHolds(tip effects.Tip) {
	cs.knownMu.Lock()
	cs.knownInCloud.add(tip)
	cs.knownMu.Unlock()
}

func (cs *CloudSync) cloudHolds(tip effects.Tip) bool {
	cs.knownMu.Lock()
	defer cs.knownMu.Unlock()

	return cs.knownInCloud.has(tip)
}

func (cs *CloudSync) wakeSender() {
	select {
	case cs.wake <- struct{}{}:
	default:
	}
}

// retire removes an outbox entry and releases its pool reference. Returns
// whether the tip was actually held — false for duplicate or stale acks after
// the outbox entry was already retired.
func (cs *CloudSync) retire(tip effects.Tip) bool {
	cs.mu.Lock()
	upload, held := cs.pending[tip]
	delete(cs.pending, tip)

	if held {
		k := string(upload.effect.GetKey())
		if entry := cs.pendingKeys[k]; entry != nil {
			delete(entry.tips, tip)

			if len(entry.tips) == 0 {
				delete(cs.pendingKeys, k)
				// Last un-acked upload on this key drained: the key is
				// cloud-durable, eviction may take it again.
				if entry.pinned {
					cs.engine.UnpinKey(k)
				}
			}
		}
	}

	backlog := len(cs.pending)
	if backlog == 0 {
		// Release peak backlog storage once the outbox drains. clear/delete
		// alone would leave the maps and queued retry storage allocated.
		cs.pending = nil
		cs.pendingKeys = nil
		cs.sendQ = nil
	}

	cs.mu.Unlock()

	if held {
		cloudOutboxPending.Set(float64(backlog))
		cs.engine.EffectCache().Decref(tip)
	}

	return held
}

// pendingTipsFor returns the outbox's un-acked tips on key — the frontier the
// cloud will eventually hold. Their bytes are refcount-pinned in the pool, so
// a walk over them resolves locally.
func (cs *CloudSync) pendingTipsFor(key string) []effects.Tip {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	entry := cs.pendingKeys[key]
	if entry == nil || len(entry.tips) == 0 {
		return nil
	}

	tips := make([]effects.Tip, 0, len(entry.tips))
	for tip := range entry.tips {
		tips = append(tips, tip)
	}

	return tips
}
