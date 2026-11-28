package cluster

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/engine/graph/bplus"
	"github.com/swytchdb/engine/internal/compatversion"
)

type bplusScalePhase struct {
	Name              string `json:"name"`
	Requests          int    `json:"requests"`
	Reads             int    `json:"reads"`
	Writes            int    `json:"writes"`
	TotalNS           int64  `json:"total_ns"`
	PrepareNS         int64  `json:"prepare_ns"`
	FlushNS           int64  `json:"flush_ns"`
	P50NS             int64  `json:"p50_ns"`
	P95NS             int64  `json:"p95_ns"`
	AllocatedBytes    uint64 `json:"allocated_bytes"`
	Allocations       uint64 `json:"allocations"`
	HeapBytes         uint64 `json:"heap_bytes"`
	GCs               uint32 `json:"gcs"`
	NewPages          uint64 `json:"new_pages"`
	DeletedPages      uint64 `json:"deleted_pages"`
	PageWrites        uint64 `json:"page_writes"`
	Snapshots         uint64 `json:"snapshots"`
	EvidenceSnapshots uint64 `json:"evidence_snapshots"`
}

// Scale is opt-in: a single fixture can take much longer than the small suite.
// Each process runs one fixed-count sample; all phases share its growing history.
func BenchmarkBPlusScale(b *testing.B) {
	if os.Getenv("BPLUS_SCALE_ENTRIES") == "" {
		b.Skip("set BPLUS_SCALE_ENTRIES, BPLUS_SCALE_PEERS, BPLUS_SCALE_BUILD and BPLUS_SCALE_DIR; use -benchtime=1x")
	}

	integer := func(name string) int {
		n, err := strconv.Atoi(os.Getenv(name))
		if err != nil || n <= 0 {
			b.Fatalf("invalid %s", name)
		}

		return n
	}
	entries, peers := integer("BPLUS_SCALE_ENTRIES"), integer("BPLUS_SCALE_PEERS")
	build := os.Getenv("BPLUS_SCALE_BUILD")

	buildVersion := os.Getenv("BPLUS_SCALE_VERSION")
	if buildVersion != "" {
		if compatversion.Normalize(buildVersion) == "" {
			b.Fatal("invalid BPLUS_SCALE_VERSION")
		}

		previous := Version
		Version = buildVersion

		b.Cleanup(func() { Version = previous })
	}

	if entries < 32 || (build != "SingleTx" && build != "Incremental") {
		b.Fatal("entries must be >=32; build must be SingleTx or Incremental")
	}

	dir := os.Getenv("BPLUS_SCALE_DIR")
	if dir == "" {
		b.Fatal("BPLUS_SCALE_DIR is required")
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		b.Fatal(err)
	}

	logger := slog.Default()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
	b.Cleanup(func() { slog.SetDefault(logger) })

	b.Run(fmt.Sprintf("Peers%d/Entries%d/Order32/Build%s", peers, entries, build), func(b *testing.B) {
		if b.N != 1 {
			b.Fatal("scale benchmarks require -benchtime=1x")
		}

		b.StopTimer()
		b.ReportAllocs()

		file, err := os.Create(filepath.Join(dir, "events.jsonl"))
		if err != nil {
			b.Fatal(err)
		}

		defer func() { _ = file.Close() }()

		encoder := json.NewEncoder(file)
		started := time.Now()

		memory := startBplusMemoryTimeline(b, dir, started)
		defer memory.close()

		emit := func(value any) {
			if err := encoder.Encode(value); err != nil {
				b.Fatal(err)
			}
		}
		progress := func(stage string, completed int) {
			memory.stage(stage)

			var mem runtime.MemStats
			runtime.ReadMemStats(&mem)
			emit(map[string]any{
				"stage": stage, "completed": completed, "elapsed_ns": time.Since(started).Nanoseconds(),
				"heap_bytes": mem.HeapAlloc, "total_allocated_bytes": mem.TotalAlloc, "allocations": mem.Mallocs, "gcs": mem.NumGC,
			})
			fmt.Fprintf(os.Stderr, "SCALE %s %d/%d %.3fs heap=%.1fMiB\n", stage, completed, entries, time.Since(started).Seconds(), float64(mem.HeapAlloc)/(1<<20))
		}
		progress("start", 0)

		scope := &bplusBenchScope{TB: b, memory: memory}
		defer func() {
			memory.detach()
			scope.close()
		}()

		engines, tree := bplusBenchFixtureWithOptions(scope, peers, entries, build, nil, 32, progress)
		// Force GC only outside operation timing, to measure reachable fixture heap.
		runtime.GC()
		progress("ready_after_gc", entries)

		want := make(map[string][]byte, entries)
		for i := range entries {
			want[string(bplusBenchKey(i))] = bytes.Repeat([]byte{7}, 32)
		}

		var (
			newPages, deletedPages, pageWrites atomic.Uint64
			snapshots, evidenceSnapshots       atomic.Uint64
		)

		e := engines[0]

		e.SetOnLocalEffect(func(_ effects.Tip, eff *pb.Effect) {
			if snap := eff.GetSnapshot(); snap != nil {
				snapshots.Add(1)

				if snap.GetEvidence() != nil {
					evidenceSnapshots.Add(1)
				}
			}

			data := eff.GetData()
			if len(eff.GetIKey()) == 0 || data == nil {
				return
			}

			if string(eff.GetVirtual()) == pb.GraphRegionsKey && string(data.GetId()) == bplus.RegionLabel {
				newPages.Add(1)
			} else if len(eff.GetVirtual()) == 0 {
				pageWrites.Add(1)

				if data.GetOp() == pb.EffectOp_REMOVE_OP {
					deletedPages.Add(1)
				}
			}
		})
		defer e.SetOnLocalEffect(nil)

		phaseNames := []string{"Get", "Scan16", "ReadHeavy", "Mixed", "Replace", "Structural"}
		phases := make([]bplusScalePhase, 0, len(phaseNames))

		for _, name := range phaseNames {
			trace := bplusScaleTrace(entries, name, want)
			progress(name+"_start", 0)
			newPages.Store(0)
			deletedPages.Store(0)
			pageWrites.Store(0)
			snapshots.Store(0)
			evidenceSnapshots.Store(0)

			samples := make([]int64, 0, len(trace))
			phase := bplusScalePhase{Name: name, Requests: len(trace)}

			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			b.StartTimer()

			for _, request := range trace {
				start := time.Now()
				c := e.NewContext()

				switch request.kind {
				case 'g':
					got, found, err := tree.Get(c, request.key)
					c.Abort()

					if err != nil || !found || !bytes.Equal(got, request.value) {
						b.Fatalf("get: found=%v err=%v", found, err)
					}

					phase.Reads++
				case 's':
					got, err := tree.Scan(c, request.key, nil, 16)
					c.Abort()

					if err != nil || len(got) != 16 {
						b.Fatalf("scan: len=%d err=%v", len(got), err)
					}

					for i, entry := range got {
						if !bytes.Equal(entry.Value, want[string(entry.Key)]) || (i > 0 && bytes.Compare(got[i-1].Key, entry.Key) >= 0) {
							b.Fatal("incorrect scan result")
						}
					}

					phase.Reads++
				default:
					var err error
					if request.kind == 'p' {
						err = tree.Put(c, request.key, request.value)
					} else {
						var found bool

						found, err = tree.Delete(c, request.key)
						if err == nil && !found {
							err = fmt.Errorf("missing deletion key %x", request.key)
						}
					}

					if err != nil {
						c.Abort()
						b.Fatal(err)
					}

					phase.PrepareNS += time.Since(start).Nanoseconds()
					flushStart := time.Now()

					bplusBenchFlush(b, c)

					phase.FlushNS += time.Since(flushStart).Nanoseconds()
					phase.Writes++
				}

				elapsed := time.Since(start).Nanoseconds()
				phase.TotalNS += elapsed
				samples = append(samples, elapsed)
			}

			b.StopTimer()
			runtime.ReadMemStats(&after)
			slices.Sort(samples)
			phase.P50NS, phase.P95NS = samples[(len(samples)-1)/2], samples[(len(samples)-1)*95/100]
			phase.AllocatedBytes, phase.Allocations = after.TotalAlloc-before.TotalAlloc, after.Mallocs-before.Mallocs
			phase.HeapBytes, phase.GCs = after.HeapAlloc, after.NumGC-before.NumGC
			phase.NewPages, phase.DeletedPages, phase.PageWrites = newPages.Load(), deletedPages.Load(), pageWrites.Load()

			phase.Snapshots, phase.EvidenceSnapshots = snapshots.Load(), evidenceSnapshots.Load()
			if compatversion.AtLeast(buildVersion, "1.8.0") && phase.EvidenceSnapshots != 0 {
				b.Fatal("modern fixture emitted compatibility evidence")
			}

			phases = append(phases, phase)
			emit(phase)
			progress(name+"_done", len(trace))
			b.ReportMetric(float64(phase.TotalNS)/float64(phase.Requests), name+"-ns/request")
			b.ReportMetric(float64(phase.AllocatedBytes)/float64(phase.Requests), name+"-B/request")
			// Verification and subscriber catch-up are separate from Flush latency.
			bplusBenchContents(b, engines, tree, want)
			progress(name+"_verified", len(trace))
		}

		runtime.GC()
		progress("finished_after_gc", entries)

		minimumVersions := make([]string, len(engines))
		for i, engine := range engines {
			minimumVersions[i] = engine.MinimumVersion()
		}

		raw, err := json.MarshalIndent(map[string]any{
			"entries": entries, "peers": peers, "build": build, "max_keys": 32, "phases": phases,
			"build_version": buildVersion, "minimum_versions": minimumVersions,
		}, "", "  ")
		if err != nil {
			b.Fatal(err)
		}

		if err := os.WriteFile(filepath.Join(dir, "result.json"), raw, 0o644); err != nil {
			b.Fatal(err)
		}

		memory.observeIdle(b)
		runtime.KeepAlive(engines)
		runtime.KeepAlive(tree)
	})
}

// Reads and replacements sample the whole key range, independently of size.
// Structural phases insert then delete the right edge; 64 inserts force splits
// at the production default page capacity even when the initial edge has room.
func bplusScaleTrace(entries int, name string, want map[string][]byte) []bplusRequest {
	count, reads, replacements := 40, 0, 0

	switch name {
	case "Get":
		count, reads = 64, 20
	case "Scan16":
		count, reads = 16, 20
	case "ReadHeavy":
		reads, replacements = 18, 1
	case "Mixed":
		reads, replacements = 10, 5
	case "Replace":
		replacements = 20
	case "Structural":
		count = 128
	}

	structural := 0

	for i := range count {
		if i%20 >= reads+replacements {
			structural++
		}
	}

	rng := rand.New(rand.NewPCG(17, 29))
	trace := make([]bplusRequest, count)
	next := 0

	for i := range trace {
		key := bplusBenchKey(rng.IntN(entries))
		request := bplusRequest{kind: 'g', key: key, value: want[string(key)]}

		switch {
		case name == "Scan16":
			request.kind, request.key = 's', bplusBenchKey(rng.IntN(entries-15))
		case i%20 < reads:
		case i%20 < reads+replacements:
			request.kind, request.value = 'p', bytes.Repeat([]byte{byte(20 + i)}, 32)
			want[string(key)] = request.value
		default:
			if next < structural/2 {
				request.kind, request.key, request.value = 'p', bplusBenchKey(entries+next), bytes.Repeat([]byte{9}, 32)
				want[string(request.key)] = request.value
			} else {
				request.kind, request.key = 'd', bplusBenchKey(entries+structural-next-1)
				delete(want, string(request.key))
			}

			next++
		}

		trace[i] = request
	}

	return trace
}
