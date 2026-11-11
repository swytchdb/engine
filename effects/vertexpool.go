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
	"fmt"
	"log/slog"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/puzpuzpuz/xsync/v4"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/durability"
	"github.com/swytchdb/engine/memsize"
)

// vertexOverhead accounts for the wrapper and conservative concurrent-map slack.
var vertexOverhead = memsize.Alloc(int64(unsafe.Sizeof(vertex{}))) + 96

// vertex wraps an immutable protobuf effect. Shared message allocations are
// tracked separately; size accounts for this wrapper and its map entry.
// refs counts creation ownership and cached read-DAG pins. Whole-key eviction
// releases creation ownership; displacing a read DAG releases its pins.
type vertex struct {
	eff  *pb.Effect
	size int64
	refs atomic.Int32
	// isRoot means missing ancestry beyond this vertex need not be fetched.
	// Resident dependencies still participate in traversal and consumption.
	// Protected by creationMu.
	isRoot bool
	// Temporal completion belongs to this resident copy, so eviction also
	// discards the bookkeeping. Bits are set only after evidence publication.
	temporal atomic.Uint32
	// Creation references belong to individual keys. A positive refs count
	// alone cannot distinguish ownership from another subdag's adoption pin.
	creationMu    sync.Mutex
	creationKeys  []string // sorted, unique; usually just the effect's own key
	creationBytes int64
	// Reverse navigation is not ownership. Identities do not pin descendant
	// payloads, and this storage disappears with the ancestor. creationMu
	// protects the slice and its memory accounting, not descendant lifetimes.
	parents []struct {
		tip Tip
		key string
	}
}

// VertexPool is the engine's deserialized effect store: a map from Tip to
// its effect, replacing the per-offset CloxCache. Presence in the pool means
// "resident locally"; absence means getEffect must fetch the bytes from a
// peer. The pool is dumb storage — it has no eviction policy of its own.
// Eviction is driven by the engine memory governor, which is the only component
// that knows a key's active DAG path and can therefore choose safe victims.
//
// Bytes reports owned protobuf graphs and pool bookkeeping. The governor also
// accounts for the trie, cached DAGs and subscription metadata.
//
// DAG-reference counting lets eviction drop whole keys and reclaim only the
// vertices no other key or read still needs; a vertex is referenced by every
// tipset that reaches it and every cached subdag that adopted it.
type VertexPool struct {
	owned       protoMemory
	vertexBytes atomic.Int64
	// Set before serving; nil during recovery so restored records are not rewritten.
	mirror      *durability.Store
	mirrorLocks [256]sync.Mutex
	m           *xsync.Map[Tip, *vertex]

	// reclaimQueue holds tips whose refcount just transitioned to zero — the
	// candidates the governor's per-tick drainReclaim frees without ever scanning
	// the whole map. This is the sole reclaim mechanism: there is no full-map
	// fallback, at any pressure level, because every path to refs==0 enqueues
	// here — the two decrement sites (decref, decrefChainNode) and born-at-zero
	// cache puts (PutSizedCache) — so the queue is by construction the complete
	// set of reclaimable vertices. Completeness is enforced by
	// TestRefcount_EvictAllReclaimsPool (evict every key, then the pool must be
	// empty); a future decrement site that forgets to enqueue fails that test
	// rather than silently leaking. The single governor goroutine consumes;
	// reclaimPending counts enqueued-but-undrained entries so the consumer
	// dequeues exactly the available set (UMPSCQueue.Dequeue blocks on empty, has
	// no Len). A stale or duplicate entry is harmless: drainReclaim's
	// CAS(0→tombstone) fails on a re-referenced vertex and delete no-ops on an
	// absent one. UMPSCQueue's slot retention (it never zeroes dequeued items) is
	// safe here, unlike for evictedKey: Tip is a pointer-free value, so a retained
	// slot pins nothing.
	reclaimQueue   *xsync.UMPSCQueue[Tip]
	reclaimPending atomic.Int64

	hits   atomic.Uint64
	misses atomic.Uint64
	// reclaimed counts vertices freed by reclaimUnreferenced (refs-0 below-LCA
	// history and orphans) — pure storage churn, not cache eviction. delete()
	// is its only increment site. coldEvictions counts keys the index's bounded
	// sweep actually evicted under memory pressure (the real "evicted_keys"),
	// bumped once per victim in onLeafEvicted. These were conflated before: the
	// old single "evictions" counter only ever saw reclaim churn, so cold-key
	// eviction was invisible in telemetry.
	reclaimed     atomic.Uint64
	coldEvictions atomic.Uint64
}

func newVertexPool() *VertexPool {
	return &VertexPool{
		m:            xsync.NewMap[Tip, *vertex](),
		reclaimQueue: xsync.NewUMPSCQueue[Tip](),
	}
}

// Get returns the effect at tip.
func (p *VertexPool) Get(tip Tip) (*pb.Effect, bool) {
	v, ok := p.m.Load(tip)
	if !ok {
		p.misses.Add(1)

		return nil, false
	}

	p.hits.Add(1)

	return v.eff, true
}

func (p *VertexPool) getVertex(tip Tip) *vertex {
	v, ok := p.m.Load(tip)
	if !ok || v.refs.Load() < 0 {
		p.misses.Add(1)

		return nil
	}

	p.hits.Add(1)

	return v
}

func (v *vertex) root() bool {
	v.creationMu.Lock()
	defer v.creationMu.Unlock()

	return v.isRoot
}

func (v *vertex) markRoot() {
	v.creationMu.Lock()
	defer v.creationMu.Unlock()

	v.isRoot = true
}

func (p *VertexPool) addParent(v *vertex, parent Tip, key string) {
	v.creationMu.Lock()
	defer v.creationMu.Unlock()

	if v.refs.Load() < 0 {
		return
	}

	for _, link := range v.parents {
		if link.tip == parent && link.key == key {
			return
		}
	}
	// Reclamation removes payloads, not evidence of an established join.
	// Keep the key as well: a shared bind can have parents on different keys,
	// and the parent's payload may be gone when this edge is traversed.
	v.parents = append(v.parents, struct {
		tip Tip
		key string
	}{parent, key})
	p.accountCreation(v)
}

// firstParent follows one established chain; cut discovery never enumerates
// all reverse paths. A reclaimed parent's identity remains valid join evidence.
func (v *vertex) firstParent(key string) (Tip, bool) {
	v.creationMu.Lock()
	defer v.creationMu.Unlock()

	for _, link := range v.parents {
		if link.key == key {
			return link.tip, true
		}
	}

	return Tip{}, false
}

// Put stores eff at tip and accounts for its resident protobuf allocations.
//
// A re-Put of an already-resident tip keeps the existing vertex so its
// accumulated refcount survives: effects are immutable (a Tip is written once),
// so the payload and byte cost are identical. Replacing the vertex would reset
// refs to 0, letting reclaimUnreferenced free an effect the index still holds
// as a frontier tip — a premature free surfacing as a missing read. The
// resident fast-path reuses the known size on a re-delivery while still taking
// any missing creation ownership.
func (p *VertexPool) Put(tip Tip, eff *pb.Effect) error {
	return p.PutSized(tip, eff, 0)
}

// PutSized preserves the serialized-length ingestion API. Owned allocations,
// rather than wire length, determine the memory charge.
//
// A new vertex is born with its creation refcount equal to the number of tipsets
// it occupies — its "walkable from a tipset" references, one per reaching tip.
// An ordinary effect is the tip of its single key, so it starts at 1; a bind
// commits a whole keyset and is the tip of each key, so it starts at len(Keys).
// This pins the vertex (interior chain nodes included, local or backfilled from a
// peer) until each occupying key is evicted, where the eviction reachable-walk
// decrefs it once per key (releaseChainRefs). The count is stored before the
// vertex is published via LoadOrStore, so reclaimUnreferenced never observes a
// fresh vertex at 0 (a birth-race free). Read adoption layers further refs on top
// in publishSubdag.
//
// Re-putting adds only missing key ownership, even if an adoption pin already
// holds the vertex. Creation ownership is tracked separately from total refs.
func (p *VertexPool) PutSized(tip Tip, eff *pb.Effect, protoLen int) error {
	return p.putSized(tip, eff, protoLen, nil)
}

// putSized takes creation references only for keys accepted by owns. Local
// emission owns the whole keyset (nil); remote ingestion owns served keys.
func (p *VertexPool) putSized(tip Tip, eff *pb.Effect, _ int, owns func([]byte) bool) error {
	return p.put(tip, eff, true, owns)
}

// missingCreationKeys returns only new owners. Ordinary effects own one key;
// binds own their distinct keys. Re-delivery of an owned effect allocates nothing.
func missingCreationKeys(eff *pb.Effect, existing []string, owns func([]byte) bool) []string {
	var keys []string

	add := func(key []byte) {
		if owns != nil && !owns(key) {
			return
		}

		if _, found := slices.BinarySearch(existing, string(key)); !found {
			keys = append(keys, string(key))
		}
	}

	if bind := eff.GetTxnBind(); bind != nil && len(bind.GetKeys()) > 0 {
		for _, kb := range bind.GetKeys() {
			add(kb.GetKey())
		}
	} else {
		add(eff.GetKey())
	}

	slices.Sort(keys)

	return slices.Compact(keys)
}

// put owns persistence, publication, and accounting for both owned and cache-only
// effects. Creation references are initialized before a vertex is published.
func (p *VertexPool) put(tip Tip, eff *pb.Effect, takeOwnership bool, owns func([]byte) bool) error {
	return p.putWithPin(tip, eff, takeOwnership, owns, false)
}

// pin reserves an independent reference before a new vertex is visible. Proof
// package installation uses it to protect cache-only records until their owner
// has acquired its references, even with reclamation running concurrently.
func (p *VertexPool) putWithPin(tip Tip, eff *pb.Effect, takeOwnership bool, owns func([]byte) bool, pin bool) error {
	if len(eff.GetEvidencePackage()) != 0 {
		clean, release, err := p.installEvidencePackage(tip, eff)
		if err != nil {
			return err
		}
		defer release()

		eff = clean
	}
	// Package members are durable inside their owning snapshot. Writing them
	// as independent records would promote proof-only history into key
	// frontiers during restart. An ordinary ingestion persists them normally.
	if p.mirror != nil && !pin {
		lock := &p.mirrorLocks[(tip[0]^tip[1])%256]
		lock.Lock()
		defer lock.Unlock()

		data, err := p.MarshalEffect(eff)
		if err != nil {
			return err
		}

		if err := p.mirror.Put(tip, data); err != nil {
			return err
		}
	}

	v, loaded := p.m.Load(tip)
	if !loaded {
		evidence, err := p.pinEvidence(tip, eff)
		if err != nil {
			return err
		}

		nv := &vertex{eff: eff, size: vertexOverhead}
		if takeOwnership {
			nv.creationKeys = missingCreationKeys(eff, nil, owns)
		}

		owners := len(nv.creationKeys)
		if pin {
			owners++
		}

		nv.refs.Store(int32(owners))
		p.owned.change(eff, 1)
		p.vertexBytes.Add(nv.size)
		p.accountCreation(nv)

		v, loaded = p.m.LoadOrStore(tip, nv)
		if loaded {
			p.releaseEvidence(evidence)
			p.owned.change(eff, -1)
			p.vertexBytes.Add(-nv.size - nv.creationBytes)
		} else if owners == 0 {
			p.enqueueReclaim(tip)
		}
	}

	if loaded && (takeOwnership || pin) {
		v.creationMu.Lock()
		defer v.creationMu.Unlock()

		var keys []string
		if takeOwnership {
			keys = missingCreationKeys(eff, v.creationKeys, owns)
		}

		added := len(keys)
		if pin {
			added++
		}

		if added == 0 {
			return nil
		}

		for {
			n := v.refs.Load()
			if n < 0 {
				if pin {
					return fmt.Errorf("pin effect %v: reclamation already claimed it", tip)
				}

				return nil // reclaim already claimed this vertex; do not resurrect it
			}

			if v.refs.CompareAndSwap(n, n+int32(added)) {
				break
			}
		}

		v.creationKeys = append(v.creationKeys, keys...)
		slices.Sort(v.creationKeys)
		p.accountCreation(v)
	}

	return nil
}

// PutSizedCache stores a fetched effect on a key we do NOT serve as a reclaimable
// cache entry: refs start at 0, so it carries no creation ref and reclaim frees
// it the moment no subdag adopts it. These are cross-key bind adjudication
// fetches (and remote effects on unsubscribed keys): a read that walks them
// holds them via the reconstruct dag for its duration, and they are refetchable
// from a peer, so they must not pin memory the way an owned effect's creation
// ref does. An already-resident owned tip is kept (LoadOrStore), never
// downgraded. protoLen semantics match PutSized.
//
// A freshly-stored entry is born at refs==0 and will never see a refs→0
// transition unless first adopted (publishSubdag) and then dropped, so it is
// enqueued as a reclaim candidate at birth — without this an un-adopted cache
// fetch would never be reclaimed (there is no full-map scan to catch it).
func (p *VertexPool) PutSizedCache(tip Tip, eff *pb.Effect, _ int) error {
	return p.put(tip, eff, false, nil)
}

// delete removes tip from the pool and counts a reclaim. It does NOT touch the
// effect's deps: a vertex is kept alive by the tipsets and DAGs that reach it
// (its own creation/adoption refs), not by its parents, so freeing a vertex
// never cascades to its deps — a shared dep stays as long as another reacher
// still references it (which is why the reclaim queue, not a re-scan, is what
// catches each newly-zero vertex). Returns the per-vertex size estimate freed
// (telemetry only), or 0 if tip was absent. Its only caller is claimAndDelete.
func (p *VertexPool) delete(tip Tip) int64 {
	if p.mirror != nil {
		if err := p.mirror.Delete(tip); err != nil {
			slog.Error("durability: failed to reclaim effect", "tip", tip, "error", err)

			if v, ok := p.m.Load(tip); ok {
				v.refs.Store(0)
				p.enqueueReclaim(tip)
			}

			return 0
		}
	}

	prev, loaded := p.m.LoadAndDelete(tip)
	if !loaded {
		return 0
	}

	p.reclaimed.Add(1)
	// claimAndDelete already changed refs from zero to the tombstone. Every
	// creation release updates accounting before dropping its reference, and
	// new owners must acquire a reference before changing it. The successful
	// CAS therefore publishes the final accounting and prevents further writes.
	p.vertexBytes.Add(-prev.size - prev.creationBytes)
	p.owned.change(prev.eff, -1)
	// Each snapshot vertex owns its proof once, independently of the number
	// of keys, read DAGs, or outbox entries holding the snapshot itself.
	p.releaseEvidence(uniqueEvidence(prev.eff))

	return prev.size
}

// decrefChainNode releases one creation reference on tip and returns its effect
// so the eviction reachable-walk (releaseChainRefs) can follow its deps. ok is
// false when tip is not resident — already reclaimed or never fetched — so there
// is nothing below it to release. A cache node or an already released key has
// no creation reference to drop, even when other keys or subdags still pin it.
func (p *VertexPool) decrefChainNode(key string, tip Tip) (*pb.Effect, bool) {
	v, ok := p.m.Load(tip)
	if !ok {
		return nil, false
	}

	v.creationMu.Lock()
	if i, owned := slices.BinarySearch(v.creationKeys, key); owned {
		v.creationKeys = shrinkSlice(slices.Delete(v.creationKeys, i, i+1))
		p.accountCreation(v)
		p.decref(tip)
	}
	v.creationMu.Unlock()

	return v.eff, true
}

// recordColdEviction counts one key evicted by the index's bounded sweep under
// memory pressure. Called once per victim from onLeafEvicted — the cold-key
// eviction the old conflated counter never saw.
func (p *VertexPool) recordColdEviction() { p.coldEvictions.Add(1) }

// enqueueReclaim records a tip whose refcount just reached zero (or a
// born-at-zero cache put) as a reclaim candidate. Enqueue precedes the pending
// increment so the consumer's Swap-then-dequeue-n never observes a count for an
// item not yet in the queue.
func (p *VertexPool) enqueueReclaim(tip Tip) {
	p.reclaimQueue.Enqueue(tip)
	p.reclaimPending.Add(1)
}

// claimAndDelete frees the vertex at tip iff it is still a reclaimable refs==0
// non-system vertex, returning the bytes freed (0 if it survived). The
// CAS(0→vertexTombstone) is the membership oracle: it fails if a concurrent
// incref or owned re-Put bumped refs above 0 since the candidate was enqueued —
// in which case the vertex is referenced again and must survive. incref treats a
// successful claim as "absent" and refuses to resurrect it, so delete after the
// claim is safe. Without the CAS this check-then-delete races incref and frees a
// referenced vertex. System keys are pinned defensively.
func (p *VertexPool) claimAndDelete(tip Tip, v *vertex) int64 {
	if p.mirror != nil {
		// Claim and removal are one operation relative to re-ingestion. Taking
		// this lock only in delete lets a successful Put see the tombstone and
		// return before delete removes its supposedly re-acquired effect.
		lock := &p.mirrorLocks[(tip[0]^tip[1])%256]
		lock.Lock()
		defer lock.Unlock()
	}

	if v.eff != nil && isSystemKey(v.eff.GetKey()) {
		return 0
	}

	// Finish navigation/accounting updates before retiring their storage.
	v.creationMu.Lock()
	defer v.creationMu.Unlock()

	if !v.refs.CompareAndSwap(0, vertexTombstone) {
		return 0
	}

	return p.delete(tip)
}

// drainReclaim frees every queued reclaim candidate, returning the bytes freed.
// This is the whole reclaim mechanism — the governor calls it every tick at any
// pressure level (via reclaimUnreferenced): O(transitions since the last tick),
// touching only refs==0 candidates, with no full-map scan, so it never pins a
// core or stalls the request path the way a rangeVertices sweep would.
// Single-consumer (the governor goroutine): the Swap-then-dequeue is not safe to
// run from two goroutines at once. A stale candidate (already reclaimed,
// re-referenced, or re-put as owned) is a cheap no-op via claimAndDelete. It
// dequeues exactly reclaimPending entries — the count enqueued so far — so the
// blocking UMPSCQueue.Dequeue never waits on an empty queue; entries enqueued
// concurrently after the Swap are counted into the next drain.
func (p *VertexPool) drainReclaim() int64 {
	n := p.reclaimPending.Swap(0)

	var total int64

	for ; n > 0; n-- {
		tip := p.reclaimQueue.Dequeue()
		if v, ok := p.m.Load(tip); ok {
			total += p.claimAndDelete(tip, v)
		}
	}

	return total
}

// vertexTombstone marks a vertex reclaimUnreferenced has claimed for deletion.
// It is deeply negative so the refcount can never climb back to a live (>=0)
// value through ordinary increfs, and so incref can detect the claim and refuse
// to resurrect it. Reclaim CAS-claims 0 -> vertexTombstone, then deletes.
const vertexTombstone = math.MinInt32 / 2

// incref bumps the key-membership refcount of the vertex at tip and reports
// whether the reference was taken. It fails (false) for a non-resident tip and
// for a vertex reclaimUnreferenced has already claimed for deletion (refs < 0):
// the claim-then-delete is atomic with this CAS, so resurrecting a claimed
// vertex would let the pool free a vertex the caller still references.
//
// Callers that retain the effect beyond the current operation (subdag pins,
// the cloud outbox) MUST honor a false return — retaining a pointer without
// the ref recreates exactly the off-books residency this protocol exists to
// prevent: memory that is heap-live but invisible to the pool's accounting
// and unreachable by eviction.
func (p *VertexPool) incref(tip Tip) bool {
	if v, ok := p.m.Load(tip); ok {
		for {
			r := v.refs.Load()
			if r < 0 {
				return false // claimed for reclamation; do not resurrect
			}

			if v.refs.CompareAndSwap(r, r+1) {
				return true
			}
		}
	}

	return false
}

// Incref adds a DAG reference to the vertex at tip and reports whether the
// reference was taken. See incref for the failure contract.
func (p *VertexPool) Incref(tip Tip) bool { return p.incref(tip) }

// Decref releases a DAG reference on the vertex at tip.
func (p *VertexPool) Decref(tip Tip) { p.decref(tip) }

// decref lowers the key-membership refcount of the vertex at tip, if resident.
// A refcount that goes negative means a decref had no matching incref — a
// refcount-protocol bug that would let reclaimUnreferenced free a vertex the
// index or a cache still references (a missing read), or strand it forever
// (a leak). It is never correct, so it panics rather than silently underflowing.
func (p *VertexPool) decref(tip Tip) {
	if v, ok := p.m.Load(tip); ok {
		n := v.refs.Add(-1)
		if n < 0 {
			panic(fmt.Sprintf("vertex refcount underflow: tip=%v refs=%d (decref without matching incref)", tip, n))
		}

		if n == 0 {
			p.enqueueReclaim(tip) // last ref released — reclaim candidate
		}
	}
}

// Bytes estimates the pool's owned capacity, including shared protobuf memory
// charged once. It changes on ownership transitions without waiting for GC.
func (p *VertexPool) Bytes() int64 {
	return p.owned.residentBytes() + p.vertexBytes.Load() + int64(unsafe.Sizeof(*p))
}

// Call with creationMu held, or before publishing a new vertex.
func (p *VertexPool) accountCreation(v *vertex) {
	cost := memsize.Alloc(int64(cap(v.creationKeys))*16) + memsize.Alloc(int64(cap(v.parents))*int64(unsafe.Sizeof(v.parents[0])))
	for _, key := range v.creationKeys {
		cost += memsize.Alloc(int64(len(key)))
	}

	for _, link := range v.parents {
		cost += memsize.Alloc(int64(len(link.key)))
	}

	p.vertexBytes.Add(cost - v.creationBytes)
	v.creationBytes = cost
}

// EntryCount returns the number of resident effects.
func (p *VertexPool) EntryCount() int { return p.m.Size() }

// Stats returns cumulative hit, miss, and reclaim counts. The third value is
// reclaimUnreferenced churn (vertices freed), not cache eviction — use
// ColdEvictions for keys dropped under memory pressure.
func (p *VertexPool) Stats() (hits, misses, reclaimed uint64) {
	return p.hits.Load(), p.misses.Load(), p.reclaimed.Load()
}

// ColdEvictions returns the number of keys the index's bounded sweep evicted
// under memory pressure — the real "evicted_keys", distinct from reclaim churn.
func (p *VertexPool) ColdEvictions() uint64 { return p.coldEvictions.Load() }

// Reclaimed returns the number of vertices freed by reclaimUnreferenced
// (below-LCA history and orphans) — storage churn, not cache eviction.
func (p *VertexPool) Reclaimed() uint64 { return p.reclaimed.Load() }
