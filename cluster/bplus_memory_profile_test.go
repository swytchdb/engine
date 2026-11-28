package cluster

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"runtime/metrics"
	"runtime/pprof"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/swytchdb/engine/effects"
)

// Opt-in process-wide observations. Periodic samples do not request GC; explicit
// GC checkpoints run outside operation timing with the fixture still reachable.
type bplusMemoryTimeline struct {
	started     time.Time
	dir         string
	file        *os.File
	encoder     *json.Encoder
	phase       atomic.Value
	mu          sync.Mutex
	err         error
	stop        chan struct{}
	done        chan struct{}
	tb          testing.TB
	engines     []*effects.Engine
	profileStop func()
}

func (m *bplusMemoryTimeline) attach(engines []*effects.Engine) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.engines = append([]*effects.Engine(nil), engines...)
}

func (m *bplusMemoryTimeline) detach() {
	if m == nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.engines = nil
}

func startBplusMemoryTimeline(tb testing.TB, dir string, started time.Time) *bplusMemoryTimeline {
	if os.Getenv("BPLUS_MEMORY_TIMELINE") == "" {
		return nil
	}

	f, err := os.Create(filepath.Join(dir, "memory.jsonl"))
	if err != nil {
		tb.Fatal(err)
	}

	m := &bplusMemoryTimeline{started: started, dir: dir, file: f, encoder: json.NewEncoder(f), stop: make(chan struct{}), done: make(chan struct{}), tb: tb}
	m.phase.Store("start")
	m.profileStop = startBplusWindowProfiles(tb, dir)

	tb.Cleanup(m.postCleanup)
	go func() {
		defer close(m.done)

		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				m.sample()
			case <-m.stop:
				return
			}
		}
	}()

	return m
}

func (m *bplusMemoryTimeline) sample() {
	if m == nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	gcCPU := []metrics.Sample{
		{Name: "/cpu/classes/gc/total:cpu-seconds"},
		{Name: "/cpu/classes/gc/mark/assist:cpu-seconds"},
		{Name: "/gc/heap/live:bytes"},
		{Name: "/gc/scan/heap:bytes"},
	}
	metrics.Read(gcCPU)

	values := make(map[string]any, len(gcCPU))
	for _, sample := range gcCPU {
		switch sample.Value.Kind() {
		case metrics.KindFloat64:
			values[sample.Name] = sample.Value.Float64()
		case metrics.KindUint64:
			values[sample.Name] = sample.Value.Uint64()
		case metrics.KindBad, metrics.KindFloat64Histogram:
			// This timeline records available scalar metrics only.
			continue
		}
	}

	peers := make([]map[string]any, 0, len(m.engines))
	for i, e := range m.engines {
		stats := e.MemoryStats()
		_, _, reclaimed := e.EffectCache().Stats()
		peers = append(peers, map[string]any{
			"peer": i, "pool_entries": e.EffectCache().EntryCount(), "pool_reclaimed": reclaimed,
			"resident_bytes": stats.ResidentBytes, "effect_bytes": stats.EffectBytes,
			"index_bytes": stats.IndexBytes, "cached_dag_bytes": stats.CachedDagBytes,
			"subscriber_bytes": stats.SubscriberBytes, "metadata_bytes": stats.MetadataBytes,
			"deferred_bytes": stats.DeferredBytes, "key_bytes": stats.KeyBytes,
		})
	}

	err := m.encoder.Encode(map[string]any{
		"unix_ns": time.Now().UnixNano(), "elapsed_ns": time.Since(m.started).Nanoseconds(), "phase": m.phase.Load(),
		"heap_alloc": mem.HeapAlloc, "heap_inuse": mem.HeapInuse, "heap_idle": mem.HeapIdle,
		"heap_released": mem.HeapReleased, "heap_sys": mem.HeapSys, "sys": mem.Sys,
		"stack_inuse": mem.StackInuse, "heap_objects": mem.HeapObjects,
		"total_alloc": mem.TotalAlloc, "mallocs": mem.Mallocs, "frees": mem.Frees,
		"num_gc": mem.NumGC, "num_forced_gc": mem.NumForcedGC, "next_gc": mem.NextGC,
		"pause_total_ns": mem.PauseTotalNs, "gc_cpu_fraction": mem.GCCPUFraction,
		"goroutines": runtime.NumGoroutine(), "runtime_metrics": values, "peers": peers,
	})
	if err != nil && m.err == nil {
		m.err = err
	}
}

func (m *bplusMemoryTimeline) stage(name string) {
	if m == nil {
		return
	}

	m.phase.Store(name)
	m.sample()

	if name == "flush_failed_before_abort" || name == "idle_after_gc" || name == "idle_final_after_gc" || name == "post_cleanup_after_gc" || name == "post_cleanup_idle_after_gc" {
		f, err := os.Create(filepath.Join(m.dir, name+".goroutine.pprof"))
		if err != nil {
			m.tb.Fatal(err)
		}

		err = pprof.Lookup("goroutine").WriteTo(f, 0)
		closeErr := f.Close()

		if err != nil {
			m.tb.Fatal(err)
		}

		if closeErr != nil {
			m.tb.Fatal(closeErr)
		}
	}

	if name == "ready_after_gc" || name == "finished_after_gc" || name == "idle_after_gc" || name == "idle_final_after_gc" || name == "post_cleanup_after_gc" || name == "post_cleanup_idle_after_gc" {
		f, err := os.Create(filepath.Join(m.dir, name+".heap.pprof"))
		if err != nil {
			m.tb.Fatal(err)
		}

		err = pprof.WriteHeapProfile(f)
		closeErr := f.Close()

		if err != nil {
			m.tb.Fatal(err)
		}

		if closeErr != nil {
			m.tb.Fatal(closeErr)
		}
	}
}

// testing cleanup runs after the benchmark frame and its fixture-cleanup
// defers unwind. The observer has detached engine references by this point.
func (m *bplusMemoryTimeline) postCleanup() {
	f, err := os.OpenFile(filepath.Join(m.dir, "memory.jsonl"), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		m.tb.Error(err)

		return
	}

	m.file, m.encoder = f, json.NewEncoder(f)
	defer func() {
		if err := f.Close(); err != nil {
			m.tb.Error(err)
		}
	}()

	m.stage("post_cleanup_before_gc")
	runtime.GC()
	runtime.GC()
	m.stage("post_cleanup_after_gc")
	time.Sleep(10 * time.Second)
	runtime.GC()
	runtime.GC()
	m.stage("post_cleanup_idle_after_gc")
}

// Preserve the ordinary error/Abort behavior, but keep peers alive long enough
// to observe background work after failure. Abort does not undo publication;
// an unknown transaction outcome remains unknown throughout this diagnostic.
func (m *bplusMemoryTimeline) flushFailure(err error, c *effects.Context) {
	if writeErr := os.WriteFile(filepath.Join(m.dir, "flush-error.txt"), []byte(err.Error()+"\n"), 0o644); writeErr != nil {
		m.tb.Fatal(writeErr)
	}

	m.stage("flush_failed_before_abort")
	c.Abort()
	m.stage("flush_failed_after_abort")
	m.observeIdle(m.tb)
}

func (m *bplusMemoryTimeline) observeIdle(tb testing.TB) {
	if m == nil {
		return
	}

	seconds := 60

	if value := os.Getenv("BPLUS_MEMORY_IDLE_SECONDS"); value != "" {
		var err error

		seconds, err = strconv.Atoi(value)
		if err != nil || seconds < 2 {
			tb.Fatal("BPLUS_MEMORY_IDLE_SECONDS must be >=2")
		}
	}

	m.stage("idle_natural")
	time.Sleep(time.Duration(seconds/2) * time.Second)
	m.stage("idle_before_gc")
	runtime.GC()
	runtime.GC()
	m.stage("idle_after_gc")
	time.Sleep(time.Duration(seconds-seconds/2) * time.Second)
	m.stage("idle_final_before_gc")
	runtime.GC()
	runtime.GC()
	m.stage("idle_final_after_gc")
}

func (m *bplusMemoryTimeline) close() {
	if m == nil {
		return
	}

	if m.profileStop != nil {
		m.profileStop()
	}

	close(m.stop)
	<-m.done
	m.phase.Store("after_fixture_cleanup")
	m.sample()

	if err := m.file.Close(); err != nil {
		m.tb.Error(err)
	}

	if m.err != nil {
		m.tb.Error(m.err)
	}
}
