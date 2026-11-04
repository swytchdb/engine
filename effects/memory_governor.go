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
	"math"
	"runtime/debug"
	"time"
)

// startMemoryGovernor evicts owned capacity above the configured budget.
// Go handles collection/scavenging independently of cache policy.
// targetBytes <= 0 leaves the existing unbounded-cache mode unchanged.
func (e *Engine) startMemoryGovernor(targetBytes int64) {
	if targetBytes <= 0 {
		return
	}

	// Only bounded engines need eviction hooks and per-read eviction tracking.
	e.index.SetEvictHooks(
		func(key string) bool { return !isSystemKey([]byte(key)) },
		e.onLeafEvicted,
		func() bool { return e.evictBudget.Load() > 0 },
	)

	// Collector pacing does not change the configured eviction threshold.
	e.goMemoryLimit = debug.SetMemoryLimit(-1)
	if e.goMemoryLimit == math.MaxInt64 {
		e.goMemoryLimit = targetBytes / 100 * 95
		debug.SetMemoryLimit(e.goMemoryLimit)
	}

	e.cacheTarget = targetBytes
	e.memWake = make(chan struct{}, 1)
	e.memGovStop = make(chan struct{})

	e.memGovWg.Add(1)
	go e.memoryGovernorLoop()
}

func (e *Engine) memoryGovernorLoop() {
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
			r.checkMemory(e.cacheTarget)

			continue
		}

		e.evictToTarget(e.cacheTarget)
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
// Like drainReclaim it is single-consumer: the governor owns this operation.
// Close may drain after joining the governor; tests drain only without one.
func (e *Engine) reclaimUnreferenced() {
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
