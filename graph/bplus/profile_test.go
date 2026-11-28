package bplus

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"testing"
	"time"
)

// Opt-in diagnostic, never part of normal tests or benchmark timing. Heap
// profiles are cumulative; analyze after with -base before and alloc_space or
// alloc_objects, not inuse_space. The two GCs flush delayed heap-profile samples.
func TestBPlusLookupProfile(t *testing.T) {
	dir := os.Getenv("BPLUS_PROFILE_DIR")
	if dir == "" {
		t.Skip("set BPLUS_PROFILE_DIR for opt-in profiling")
	}

	build := os.Getenv("BPLUS_BUILD")
	if build != "SingleTx" && build != "Incremental" {
		t.Fatal("BPLUS_BUILD must be SingleTx or Incremental")
	}

	entries, err := strconv.Atoi(os.Getenv("BPLUS_ENTRIES"))
	if err != nil || entries < 16 {
		t.Fatal("BPLUS_ENTRIES must be >= 16")
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	e, tree, keys, value := benchmarkReadFixture(t, entries, build)
	writeMemory := func(name string) {
		runtime.GC()
		runtime.GC()

		f, err := os.Create(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}

		if err := pprof.Lookup("allocs").WriteTo(f, 0); err != nil {
			t.Fatal(err)
		}

		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	writeMemory("alloc-before.pprof")

	f, err := os.Create(filepath.Join(dir, "cpu.pprof"))
	if err != nil {
		t.Fatal(err)
	}

	if err := pprof.StartCPUProfile(f); err != nil {
		t.Fatal(err)
	}

	defer func() {
		pprof.StopCPUProfile()

		if err := f.Close(); err != nil {
			t.Error(err)
		}
	}()

	const requests = 1000

	observed := make(map[int]int)
	start := time.Now()

	pprof.Do(context.Background(), pprof.Labels("phase", "lookup"), func(ctx context.Context) {
		for i := range requests {
			c := e.NewContext()
			c.SetTraceCtx(ctx)
			got, found, err := tree.Get(c, keys[i%entries])
			observed[len(c.PendingKeysInternal())]++
			c.Abort()

			if err != nil || !found || !bytes.Equal(got, value) {
				t.Fatalf("lookup: %v %v", found, err)
			}
		}
	})

	elapsed := time.Since(start)

	pprof.StopCPUProfile()
	writeMemory("alloc-after.pprof")

	data, err := json.MarshalIndent(map[string]any{"build": build, "entries": entries, "requests": requests, "wall_ns": elapsed.Nanoseconds(), "observed_address_count_histogram": observed, "mem_profile_rate": runtime.MemProfileRate}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(dir, "lookup.json"), data, 0o644); err != nil {
		t.Fatal(err)
	}
}
