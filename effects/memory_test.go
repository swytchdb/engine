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
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"math"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
)

func TestMemoryPacing(t *testing.T) {
	for _, end := range []string{"relief", "cancellation", "shutdown", "timeout"} {
		t.Run(end, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				e := &Engine{memGovStop: make(chan struct{})}
				e.setMemoryPressure(true)

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				done := make(chan struct{})

				go func() {
					e.backpressure(ctx)
					close(done)
				}()

				synctest.Wait()

				select {
				case <-done:
					t.Fatal("writer did not wait for reclamation")
				default:
				}

				switch end {
				case "relief":
					e.setMemoryPressure(false)
				case "cancellation":
					cancel()
				case "shutdown":
					close(e.memGovStop)
				case "timeout":
					time.Sleep(maxMemoryPacing)
				}

				synctest.Wait()

				select {
				case <-done:
				default:
					t.Fatal("writer remained blocked")
				}
			})
		})
	}
}

func TestMemoryPacingAfterPublication(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := NewEngine(EngineConfig{DefaultMode: UnsafeMode})

		t.Cleanup(func() {
			if err := e.Close(); err != nil {
				t.Error(err)
			}
		})
		e.setMemoryPressure(true)

		c := e.NewContext()
		if err := c.Emit(scalarSet("paced", "committed")); err != nil {
			t.Fatal(err)
		}

		result := make(chan error, 1)
		go func() { result <- c.Flush() }()

		synctest.Wait()

		select {
		case err := <-result:
			t.Fatalf("write returned before relief: %v", err)
		default:
		}

		state, _, _, err := e.GetSnapshot("paced")
		if err != nil || string(state.GetScalar().GetRaw()) != "committed" {
			t.Fatalf("pacing held up publication: %v", err)
		}
		// Read-only/empty flushes must not join the pressure wait.
		started := time.Now()

		if err := e.NewReadOnlyContext().Flush(); err != nil {
			t.Fatal(err)
		}

		if time.Since(started) != 0 {
			t.Fatal("read-only flush was paced")
		}

		e.evictToTarget(e.residentMemory().ResidentBytes)
		synctest.Wait()

		if err := <-result; err != nil {
			t.Fatalf("pacing changed the commit result: %v", err)
		}
	})
}

func TestMemoryEvictionStopsWithoutGC(t *testing.T) {
	e := NewEngine(EngineConfig{DefaultMode: UnsafeMode})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})
	e.index.SetEvictHooks(func(string) bool { return true }, e.onLeafEvicted, func() bool { return e.evictBudget.Load() > 0 })

	value := strings.Repeat("v", 32<<10)

	for i := range 256 {
		c := e.NewContext()
		if err := c.Emit(scalarSet(fmt.Sprintf("memory:%d", i), value)); err != nil {
			t.Fatal(err)
		}

		if err := c.Flush(); err != nil {
			t.Fatal(err)
		}
	}

	runtime.GC()

	oldGC := debug.SetGCPercent(-1)

	t.Cleanup(func() { debug.SetGCPercent(oldGC) })

	before := e.MemoryStats()
	target := before.ResidentBytes - 3<<20

	var after MemoryStats
	for range 8 {
		after = e.evictToTarget(target)
		if after.ResidentBytes <= target {
			break
		}
	}

	if after.ResidentBytes > target {
		t.Fatalf("did not reclaim to target: before=%d after=%d target=%d", before.ResidentBytes, after.ResidentBytes, target)
	}

	if e.memPressure.Load() != nil {
		t.Fatal("writers still paced after logical reclamation without GC")
	}

	keys, victims := e.KeyCount(), e.effectCache.ColdEvictions()
	if keys == 0 || victims == 0 {
		t.Fatalf("wanted a partial eviction, keys=%d victims=%d", keys, victims)
	}

	for range 8 {
		e.evictToTarget(target)
	}

	if e.KeyCount() != keys || e.effectCache.ColdEvictions() != victims {
		t.Fatal("repeated eviction after accounting already met the target")
	}

	if live := readRuntimeMemory(); live.cycles != before.GCCycles || live.live != before.LiveHeapBytes {
		t.Fatal("test did not preserve the stale GC measurement")
	}

	e.index.Range(func(key string) bool {
		result, _, _, err := e.GetSnapshot(key)
		if err != nil || string(result.GetScalar().GetRaw()) != value {
			t.Errorf("remaining key %q changed: %v", key, err)
		}

		return true
	})
}

func TestMemoryRuntimeFootprintDoesNotBlockWrites(t *testing.T) {
	previousLimit := debug.SetMemoryLimit(1 << 30)
	previousGC := debug.SetGCPercent(-1)

	t.Cleanup(func() {
		debug.SetMemoryLimit(previousLimit)
		debug.SetGCPercent(previousGC)
	})

	e := NewEngine(EngineConfig{DefaultMode: UnsafeMode, MemoryLimit: 32 << 20})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})

	marker := Tip{123456789, 1}
	if err := e.effectCache.PutSizedCache(marker, &pb.Effect{Key: []byte("reclaim-marker")}, 0); err != nil {
		t.Fatal(err)
	}
	// Retain unrelated Go memory above the engine target, with a higher
	// explicit Go limit. This reproduces alpha's mismatched runtime/cache
	// thresholds without relying on collection or scavenging to unblock writes.
	external := make([]byte, 64<<20)
	for i := 0; i < len(external); i += 4096 {
		external[i] = 1
	}

	runtime.GC()
	e.wakeMemoryGovernor()

	deadline := time.Now().Add(2 * time.Second)

	for {
		if _, present := e.effectCache.m.Load(marker); !present {
			break
		}

		if time.Now().After(deadline) {
			t.Fatal("governor did not run its reclamation pass")
		}

		time.Sleep(time.Millisecond)
	}
	// Join the completed pass to exclude a race with its final observations.
	// The private fixture fits the cache budget, so no further eviction is due.
	close(e.memGovStop)
	e.memGovWg.Wait()
	e.memGovStop = nil

	before := e.MemoryStats()
	if before.RuntimeUsedBytes-before.HeapFreeBytes <= e.MemoryTarget() {
		t.Fatal("fixture did not exceed the process target")
	}

	value := strings.Repeat("v", 4096)

	for i := range 64 {
		c := e.NewContext()
		if err := c.Emit(scalarSet(fmt.Sprintf("runtime:%d", i), value)); err != nil {
			t.Fatalf("runtime memory blocked a write: %v", err)
		}

		if err := c.Flush(); err != nil {
			t.Fatal(err)
		}
	}

	c := e.NewContext()
	c.BeginTx()

	for _, key := range []string{"runtime:tx:a", "runtime:tx:b"} {
		if err := c.Emit(scalarSet(key, value)); err != nil {
			t.Fatal(err)
		}
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	for _, key := range []string{"runtime:0", "runtime:63", "runtime:tx:a", "runtime:tx:b"} {
		state, _, _, err := e.GetSnapshot(key)
		if err != nil || string(state.GetScalar().GetRaw()) != value {
			t.Fatalf("write did not remain readable for %q: %v", key, err)
		}
	}

	after := e.MemoryStats()
	if after.GCCycles != before.GCCycles || after.UnderPressure || e.effectCache.ColdEvictions() != 0 {
		t.Fatalf("runtime footprint affected collection/eviction: before=%+v after=%+v", before, after)
	}

	// Now require actual eviction, while the same runtime sample stays high.
	// Freed ownership must permit the next write without a GC or scavenger pass.
	e.cacheTarget = after.ResidentBytes - 128<<10

	reclaimed := e.evictToTarget(e.cacheTarget)
	if reclaimed.ResidentBytes > e.cacheTarget || e.effectCache.ColdEvictions() == 0 || e.KeyCount() == 0 {
		t.Fatal("fixture did not perform a partial capacity eviction")
	}

	c = e.NewContext()
	if err := c.Emit(scalarSet("runtime:after-eviction", value)); err != nil {
		t.Fatalf("write waited for runtime memory after logical reclamation: %v", err)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	state, _, _, err := e.GetSnapshot("runtime:after-eviction")
	if err != nil || string(state.GetScalar().GetRaw()) != value {
		t.Fatalf("post-eviction write was not readable: %v", err)
	}

	e.evictToTarget(e.cacheTarget)

	keys := e.KeyCount()
	for range 8 {
		e.evictToTarget(e.cacheTarget)
	}

	if e.KeyCount() != keys || readRuntimeMemory().cycles != before.GCCycles {
		t.Fatal("stale runtime memory caused another eviction or collection")
	}

	runtime.KeepAlive(external)
}

func TestMemoryConcurrentPublicationAndEviction(t *testing.T) {
	e := NewEngine(EngineConfig{})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})

	for round := range 100 {
		key := fmt.Sprintf("race:%d", round)

		id := Tip{7, uint64(round + 1)}
		if err := e.effectCache.Put(id, &pb.Effect{Key: []byte(key)}); err != nil {
			t.Fatal(err)
		}

		view, _ := e.effectCache.Get(id)
		ls := &leafState{}
		d := &dag{topoOrder: []Tip{id}, nodes: map[Tip]*pb.Effect{id: view}}

		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() { e.publishSubdag(ls, d, []Tip{id}) })
		}

		wg.Go(func() { e.releaseChainRefs(key, []Tip{id}, ls) })
		wg.Wait()
		e.releaseChainRefs(key, []Tip{id}, ls)
		e.reclaimUnreferenced()

		if e.cachedDagBytes.Load() != 0 || e.effectCache.vertexBytes.Load() != 0 {
			t.Fatal("racing publication/eviction leaked accounting")
		}
	}
}

func TestMemorySubscribersReleasedOnce(t *testing.T) {
	e := NewEngine(EngineConfig{})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})
	e.index.Insert("key", nil, keytrie.NewTipSet())
	e.addPeerSubscriber("key", 1)
	charged := e.subscriberBytes.Load()
	e.addPeerSubscriber("key", 1)
	e.removePeerSubscriber("key", 1)

	if charged == 0 || e.subscriberBytes.Load() != 0 {
		t.Fatal("last unsubscribe must release subscriber capacity")
	}

	ls := e.index.LoadData("key")
	e.releaseChainRefs("key", nil, ls)
	e.releaseChainRefs("key", nil, ls)
	e.addPeerSubscriber("key", 2)

	if e.subscriberBytes.Load() != 0 {
		t.Fatal("evicted subscribers remained or were resurrected")
	}
}

func TestMemoryTargetsRespectGoLimit(t *testing.T) {
	const budget = int64(8 << 30)

	for _, limit := range []int64{math.MaxInt64, 256 << 20, 2 << 30, budget, 14 << 30} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			previous := debug.SetMemoryLimit(limit)

			t.Cleanup(func() { debug.SetMemoryLimit(previous) })

			var logs bytes.Buffer

			previousLogger := slog.Default()

			slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
			t.Cleanup(func() { slog.SetDefault(previousLogger) })

			e := NewEngine(EngineConfig{MemoryLimit: budget})

			t.Cleanup(func() {
				if err := e.Close(); err != nil {
					t.Error(err)
				}
			})

			actual := debug.SetMemoryLimit(-1)
			if actual != limit {
				t.Fatalf("overrode process Go limit: %d -> %d", limit, actual)
			}

			stats := e.MemoryStats()
			if stats.GoMemoryLimitBytes != actual || stats.CacheTargetBytes != budget || e.MemoryTarget() != budget {
				t.Fatalf("collector settings changed the configured eviction threshold: %+v", stats)
			}

			if err := e.Close(); err != nil {
				t.Fatal(err)
			}

			wantWarning := limit != math.MaxInt64 && limit <= budget
			if got := strings.Contains(logs.String(), "level=WARN"); got != wantWarning {
				t.Fatalf("memory-limit warning = %v, want %v; logs: %s", got, wantWarning, logs.String())
			}

			if wantWarning && (!strings.Contains(logs.String(), fmt.Sprintf("go_memory_limit_bytes=%d", limit)) ||
				!strings.Contains(logs.String(), fmt.Sprintf("cache_target_bytes=%d", budget))) {
				t.Fatalf("warning omitted the conflicting memory budgets: %s", logs.String())
			}
		})
	}
}

func TestMemoryDoesNotEvictBelowConfiguredBudget(t *testing.T) {
	previous := debug.SetMemoryLimit(1 << 30)

	t.Cleanup(func() { debug.SetMemoryLimit(previous) })

	const budget = int64(32 << 20)

	e := NewEngine(EngineConfig{DefaultMode: UnsafeMode, MemoryLimit: budget})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})
	// Drive passes synchronously so the fixture observes the eviction boundary,
	// independently of the background timer or workload throughput.
	close(e.memGovStop)
	e.memGovWg.Wait()
	e.memGovStop = nil
	value := strings.Repeat("v", 64<<10)

	for i := 0; e.residentMemory().ResidentBytes < budget*9/10; i++ {
		c := e.NewContext()
		if err := c.Emit(scalarSet(fmt.Sprintf("threshold:%d", i), value)); err != nil {
			t.Fatal(err)
		}

		if err := c.Flush(); err != nil {
			t.Fatal(err)
		}
	}

	before := e.residentMemory()
	if before.ResidentBytes >= budget {
		t.Fatal("fixture crossed the configured budget")
	}

	keys := e.KeyCount()
	e.evictToTarget(e.cacheTarget)

	if e.KeyCount() != keys || e.effectCache.ColdEvictions() != 0 {
		t.Fatalf("evicted below configured budget: owned=%d target=%d", before.ResidentBytes, budget)
	}

	c := e.NewContext()
	if err := c.Emit(scalarSet("threshold:over", strings.Repeat("v", 4<<20))); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	if e.residentMemory().ResidentBytes <= budget {
		t.Fatal("fixture did not cross the configured budget")
	}

	after := e.evictToTarget(e.cacheTarget)
	if after.ResidentBytes > budget || e.effectCache.ColdEvictions() == 0 {
		t.Fatalf("did not evict actual overage: owned=%d target=%d", after.ResidentBytes, budget)
	}
}

func TestMemoryRecoveryPreservesConfiguredCacheBudget(t *testing.T) {
	previous := debug.SetMemoryLimit(-1)

	t.Cleanup(func() { debug.SetMemoryLimit(previous) })

	e := newEngine(EngineConfig{MemoryLimit: 512 << 20})

	_, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	e.recovery.Store(&recoveryRun{cancel: cancel})
	e.startMemoryGovernor(e.memTarget)
	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})

	budget := e.cacheTarget
	close(e.memGovStop)
	e.memGovWg.Wait()
	e.memGovStop = nil
	e.recovery.Store(nil)

	restoreScratch := make([]byte, 4<<20)

	e.startMemoryGovernor(e.memTarget)

	if e.cacheTarget != budget {
		t.Fatal("restore scratch changed the configured cache budget")
	}

	runtime.KeepAlive(restoreScratch)
}
