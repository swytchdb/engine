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
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/puzpuzpuz/xsync/v4"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/durability"
	"github.com/swytchdb/engine/internal/memsize"
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
	// Temporal completion belongs to this resident copy, so eviction also
	// discards the bookkeeping. Bits are set only after evidence publication.
	temporal atomic.Uint32
	// Creation references belong to individual keys. A positive refs count
	// alone cannot distinguish ownership from another subdag's adoption pin.
	creationMu        sync.Mutex
	creationKeys      map[string]struct{}
	creationHighWater int
	creationBytes     int64
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
func (p *VertexPool) putSized(tip Tip, eff *pb.Effect, protoLen int, owns func([]byte) bool) error {
	keys := make(map[string]struct{})
	add := func(key []byte) {
		if owns == nil || owns(key) {
			keys[string(key)] = struct{}{}
		}
	}

	if bind := eff.GetTxnBind(); bind != nil && len(bind.GetKeys()) > 0 {
		for _, kb := range bind.GetKeys() {
			add(kb.GetKey())
		}
	} else {
		add(eff.GetKey())
	}

	if len(keys) == 0 {
		return p.PutSizedCache(tip, eff, protoLen)
	}

	if p.mirror != nil {
		lock := &p.mirrorLocks[(tip[0]^tip[1])%256]
		lock.Lock()
		defer lock.Unlock()

		if err := p.persist(tip, eff); err != nil {
			return err
		}
	}

	v, loaded := p.m.Load(tip)
	if !loaded {
		nv := &vertex{eff: eff, size: vertexOverhead, creationKeys: keys}
		nv.refs.Store(int32(len(keys)))
		p.owned.change(eff, 1)
		p.vertexBytes.Add(nv.size)
		p.accountCreation(nv)

		v, loaded = p.m.LoadOrStore(tip, nv)
		if loaded {
			p.owned.change(eff, -1)
			p.vertexBytes.Add(-nv.size - nv.creationBytes)
		}
	}

	if loaded {
		v.creationMu.Lock()
		defer v.creationMu.Unlock()

		for key := range v.creationKeys {
			delete(keys, key)
		}

		if len(keys) == 0 {
			return nil
		}

		for {
			n := v.refs.Load()
			if n < 0 {
				return nil // reclaim already claimed this vertex; do not resurrect it
			}

			if v.refs.CompareAndSwap(n, n+int32(len(keys))) {
				break
			}
		}

		if v.creationKeys == nil {
			v.creationKeys = keys
		} else {
			for key := range keys {
				v.creationKeys[key] = struct{}{}
			}
		}

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
	if p.mirror != nil {
		lock := &p.mirrorLocks[(tip[0]^tip[1])%256]
		lock.Lock()
		defer lock.Unlock()

		if err := p.persist(tip, eff); err != nil {
			return err
		}
	}

	if _, loaded := p.m.Load(tip); loaded {
		return nil
	}

	nv := &vertex{eff: eff, size: vertexOverhead}
	p.owned.change(eff, 1)
	p.vertexBytes.Add(nv.size)

	if _, loaded := p.m.LoadOrStore(tip, nv); !loaded {
		p.enqueueReclaim(tip)
	} else {
		p.owned.change(eff, -1)
		p.vertexBytes.Add(-nv.size)
	}

	return nil
}

func (p *VertexPool) persist(tip Tip, eff *pb.Effect) error {
	data, err := MarshalEffect(eff)
	if err != nil {
		return err
	}

	return p.mirror.Put(tip, data)
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
	prev.creationMu.Lock()
	p.vertexBytes.Add(-prev.size - prev.creationBytes)
	prev.creationMu.Unlock()
	p.owned.change(prev.eff, -1)

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
	if _, owned := v.creationKeys[key]; owned {
		delete(v.creationKeys, key)
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

// Call with creationMu held, or before publishing a new vertex. Map capacity
// survives deletions, while removed ownership strings no longer belong to it.
func (p *VertexPool) accountCreation(v *vertex) {
	v.creationHighWater = max(v.creationHighWater, len(v.creationKeys))

	var cost int64
	if v.creationKeys != nil {
		cost = memsize.Map(v.creationHighWater, 16)
		for key := range v.creationKeys {
			cost += memsize.Alloc(int64(len(key)))
		}
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

// startMemoryGovernor evicts owned capacity above the configured budget.
// Go handles collection/scavenging independently of cache policy.
// targetBytes <= 0 leaves the existing unbounded-cache mode unchanged.
func (e *Engine) startMemoryGovernor(targetBytes int64) {
	if targetBytes <= 0 {
		return
	}

	if targetBytes > 0 {
		// Eviction is only ever driven by this governor, so install the hooks here
		// rather than unconditionally: the bounded sweep pins system keys (never a
		// victim) and hands each evicted cold key's tips + leaf payload to
		// onLeafEvicted (which releases refs and unsubscribes). Installing the hooks
		// also activates the trie's per-read eviction bookkeeping, so a node without
		// a memory target pays none of that read-path cost.
		e.index.SetEvictHooks(
			func(key string) bool { return !isSystemKey([]byte(key)) },
			e.onLeafEvicted,
			// Graduations count only during an active capacity-eviction batch.
			func() bool { return e.evictBudget.Load() > 0 },
		)

		// Preserve the existing default collector setting when GOMEMLIMIT is unset.
		// Collector pacing must not change the configured eviction threshold.
		defaultGoLimit := targetBytes / 100 * 95

		e.goMemoryLimit = debug.SetMemoryLimit(-1)
		if e.goMemoryLimit == math.MaxInt64 {
			e.goMemoryLimit = defaultGoLimit
			debug.SetMemoryLimit(e.goMemoryLimit)
		}

		e.cacheTarget = targetBytes
	}

	e.memWake = make(chan struct{}, 1)

	e.memGovStop = make(chan struct{})

	e.memGovWg.Add(1)
	go e.memoryGovernorLoop(targetBytes)
}

func (e *Engine) memoryGovernorLoop(targetBytes int64) {
	defer e.memGovWg.Done()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	// Coalesce write notifications; never scan accounting or walk references
	// once per write. All reference draining has one background consumer.
	wakeTimer := time.NewTimer(time.Hour)

	wakeTimer.Stop()
	defer wakeTimer.Stop()

	var wake <-chan time.Time

	for {
		select {
		case <-e.memGovStop:
			return
		case <-e.memWake:
			if wake == nil {
				wakeTimer.Reset(50 * time.Millisecond)
				wake = wakeTimer.C
			}

			continue
		case <-wake:
			wake = nil
		case <-ticker.C:
		}

		if r := e.recovery.Load(); r != nil {
			r.checkMemory(targetBytes)

			continue
		}

		if targetBytes > 0 {
			e.evictToTarget(e.cacheTarget)
		} else {
			e.reclaimUnreferenced()
		}
	}
}

// evictToTarget uses ownership/capacity accounting only. Each bounded batch
// drains deferred releases before measuring again; GC timing cannot repeat a
// deficit that logical reclamation has already satisfied.
func (e *Engine) evictToTarget(target int64) MemoryStats {
	e.reclaimUnreferenced()

	s := e.residentMemory()
	defer e.evictBudget.Store(0)

	if s.ResidentBytes <= target {
		return s
	}

	for remaining := maxDrainPerTick; remaining > 0 && s.ResidentBytes > target; {
		keys := e.index.Size()
		if keys == 0 {
			break
		}

		avg := max(s.ResidentBytes/keys, 64)
		want := min(max((s.ResidentBytes-target)/avg, 1), int64(evictBatchSize), int64(remaining))
		e.evictBudget.Store(want)
		e.drainEvictBudget(int(want))

		drained := want - e.evictBudget.Load()
		if drained <= 0 {
			break // pinned state or the bounded CLOCK window needs another pass
		}

		remaining -= int(drained)

		e.reclaimUnreferenced()
		s = e.residentMemory()
	}

	return s
}

// evictBatchSize bounds one sweep before the governor releases references and
// remeasures. It is deliberately independent of the GC cycle.
const evictBatchSize = 4096

// maxDrainPerTick caps the deficit the governor sets (and bulk-drains) per tick so
// a runaway overage can't try to evict the whole cache at once; the remainder is
// re-measured and drained on following ticks.
const maxDrainPerTick = 1 << 18 // 262144

// backpressure coalesces a governor wake after ingest/flush. Eviction and its
// deferred walks run together in the background. Writes do not wait for GC or
// for the operating system to reclaim pages.
func (e *Engine) backpressure() {
	e.wakeMemoryGovernor()
}

// drainEvictBudget evicts up to maxKeys keys, decrementing the shared evictBudget
// the governor sets from resident capacity. Only the governor drains in serving
// code, alternating bounded batches with reference release and remeasurement.
//
// Stops early when the budget is exhausted (at/under target) or when a sweep finds
// nothing evictable (all pinned / empty window this pass); the persistent clock
// hand means the next call resumes elsewhere in the arena.
func (e *Engine) drainEvictBudget(maxKeys int) {
	if r := e.recovery.Load(); r != nil {
		if e.evictBudget.Load() > 0 {
			r.cancel(errRecoveryMemory)
		}

		return
	}

	for maxKeys > 0 {
		b := e.evictBudget.Load()
		if b <= 0 {
			return
		}

		want := int(min(b, int64(maxKeys), evictBatchSize))

		n := e.index.EvictBatch(want)
		if n == 0 {
			return // nothing evictable right now; leave the budget for next time
		}

		e.evictBudget.Add(-int64(n))
		maxKeys -= n
	}
	// The caller drains deferred chain walks and reclaim candidates before
	// deciding whether another batch is needed.
}

// ReleaseQueueDepth returns the number of cold-evicted keys whose deferred
// ref-release walk has not yet run. Each pending entry pins its key's cached
// subdag and resident effect chain, so a persistently deep queue means the
// governor's reclaim pass is falling behind eviction and live heap will read
// high for memory eviction can't actually free yet.
func (e *Engine) ReleaseQueueDepth() int {
	e.releaseMu.Lock()
	n := len(e.releaseItems)
	e.releaseMu.Unlock()

	return n
}

// drainPendingReleases runs the deferred creation-ref chain walks
// (releaseChainRefs) for keys cold-evicted since the last call. Single-consumer:
// only reclaimUnreferenced (on the governor goroutine) calls it. It swaps the
// buffer out under the lock (entries enqueued during the walks land in the
// spare and are drained next call), then zeroes every processed slot before
// reusing the buffer — an evictedKey's leafState pins the key's whole cached
// subdag → effect → payload chain, so a drained-but-unzeroed slot would keep
// gigabytes of evicted state GC-reachable.
func (e *Engine) drainPendingReleases() {
	e.releaseMu.Lock()
	batch := e.releaseItems
	e.releaseItems = e.releaseSpare[:0]
	e.releaseMu.Unlock()

	if len(batch) == 0 {
		e.releaseSpare = batch

		return
	}

	for _, ek := range batch {
		e.releaseChainRefs(ek.key, ek.tips, ek.ls)

		if ek.ownedKey {
			e.releaseKey(ek.key)
		}
	}

	clear(batch)
	e.releaseSpare = batch[:0]
}

// reclaimUnreferenced runs the deferred creation-ref chain walks for keys
// cold-evicted since the last call, then frees every pool vertex now at refcount
// zero by draining the candidate queue — no full-map scan. A vertex held by no
// tipset that reaches it and no cached subdag that adopted it is a released
// chain node, a reclaimable cache fetch, or a true orphan, and safe to free
// (getEffect re-fetches on the rare miss); every path to refs==0 enqueued it
// (decref, decrefChainNode, born-at-zero PutSizedCache), so the queue is the
// complete set of reclaimable vertices.
//
// Like drainReclaim it is single-consumer: only the governor goroutine (and
// tests, which never start a governor — it requires a memory target) may call
// it.
func (e *Engine) reclaimUnreferenced() {
	e.reclaimMu.Lock()
	defer e.reclaimMu.Unlock()

	if e.effectCache == nil {
		return
	}

	e.drainPendingReleases()

	total := e.effectCache.drainReclaim()
	if total > 0 {
		slog.Debug("vertex pool: reclaimed unreferenced effects",
			"vertex_overhead_bytes", total, "pool_resident_bytes", e.effectCache.Bytes())
	}
}
