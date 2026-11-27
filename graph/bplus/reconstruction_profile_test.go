package bplus

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"slices"
	"sort"
	"strconv"
	"testing"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
)

// This opt-in diagnostic changes only its disposable fixture. It compares the
// same transactions before/after the existing nontransactional compaction path.
func TestBPlusReconstructionDiagnostic(t *testing.T) {
	dir := os.Getenv("BPLUS_RECON_DIR")
	if dir == "" {
		t.Skip("set BPLUS_RECON_DIR for the reconstruction diagnostic")
	}

	entries, err := strconv.Atoi(os.Getenv("BPLUS_ENTRIES"))
	if err != nil || entries < 32 {
		t.Fatal("invalid BPLUS_ENTRIES")
	}

	build := os.Getenv("BPLUS_BUILD")
	if build != "SingleTx" && build != "Incremental" {
		t.Fatal("invalid BPLUS_BUILD")
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	started := time.Now()
	e, tree, keys, value := benchmarkReadFixture(t, entries, build)
	setup := time.Since(started)
	scan := e.NewContext()
	contents, err := tree.Scan(scan, nil, nil, 0)
	addresses := scan.PendingKeysInternal()
	scan.Abort()

	if err != nil || len(contents) != entries {
		t.Fatalf("scan: %d %v", len(contents), err)
	}

	sort.Strings(addresses)

	descriptor := ""

	for _, key := range addresses {
		if key != tree.RootAddress() {
			state, _, _, err := e.GetSnapshotInternal(key)
			if err != nil {
				t.Fatal(err)
			}

			if state.GetTypeTag() == pb.ValueType_TYPE_GRAPH {
				descriptor = key
			}
		}
	}

	if descriptor == "" {
		t.Fatal("descriptor not observed")
	}

	audit := func() map[string]any {
		rows := make([]map[string]any, 0, len(addresses))
		for i, key := range addresses {
			_, tips, chain, err := e.GetSnapshotInternal(key)
			if err != nil {
				t.Fatal(err)
			}

			counts := map[string]int{}

			stack := append([]effects.Tip(nil), tips...)
			seen := map[effects.Tip]bool{}

			for len(stack) > 0 {
				tip := stack[len(stack)-1]
				stack = stack[:len(stack)-1]

				if seen[tip] {
					continue
				}

				seen[tip] = true

				eff, ok := e.EffectCache().Get(tip)
				if !ok {
					counts["missing"]++

					continue
				}

				if bind := eff.GetTxnBind(); bind != nil {
					for _, kb := range bind.GetKeys() {
						if string(kb.GetKey()) == key {
							counts["bind"]++

							if ref := kb.GetNewTip(); ref != nil {
								stack = append(stack, effects.Tip{ref.GetNodeId(), ref.GetOffset()})
							}

							break
						}
					}

					continue
				}

				if string(eff.GetIKey()) != key {
					counts["cross_key_boundary"]++

					continue
				}

				switch {
				case eff.GetSnapshot() != nil:
					if eff.GetSnapshot().GetState() != nil {
						counts["state_snapshot"]++
					} else {
						counts["verdict_snapshot"]++
					}
				case eff.GetNoop() != nil:
					counts["noop"]++
				case eff.GetData() != nil:
					counts["data"]++
				case eff.GetSubscription() != nil:
					counts["subscription"]++
				default:
					counts["other"]++
				}

				for _, ref := range eff.GetDeps() {
					stack = append(stack, effects.Tip{ref.GetNodeId(), ref.GetOffset()})
				}
			}

			role := "page"
			if key == descriptor {
				role = "descriptor"
			}

			if key == tree.RootAddress() {
				role = "root"
			}

			rows = append(rows, map[string]any{"index": i, "role": role, "counts": counts, "reported_chain_length": chain})
		}

		return map[string]any{"addresses": len(addresses), "keys": rows, "resident_pool_entries": e.EffectCache().EntryCount()}
	}
	query := func(kind string) map[string]any {
		const requests = 256

		runtime.GC()

		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)

		h0, m0, _ := e.EffectCache().Stats()
		samples := make([]int64, 0, requests)
		observed := map[int]int{}

		start := time.Now()
		for i := range requests {
			one := time.Now()

			c := e.NewContext()
			if kind == "Get" {
				got, found, err := tree.Get(c, keys[(i*7919)%entries])
				if err != nil || !found || !bytes.Equal(got, value) {
					c.Abort()
					t.Fatalf("get: %v %v", found, err)
				}
			} else {
				c.BeginTx()

				if kind == "Descriptor" {
					got, _, err := c.GetSnapshot(tree.Key())
					if err != nil || got.GetTypeTag() != pb.ValueType_TYPE_GRAPH {
						c.Abort()
						t.Fatalf("descriptor: %v", err)
					}
				} else {
					got, _, err := c.GetSnapshotInternal(tree.RootAddress())
					if err != nil || got.GetScalar() == nil {
						c.Abort()
						t.Fatalf("root: %v", err)
					}
				}
			}

			observed[len(c.PendingKeysInternal())]++
			c.Abort()

			samples = append(samples, time.Since(one).Nanoseconds())
		}

		elapsed := time.Since(start)
		h1, m1, _ := e.EffectCache().Stats()

		runtime.ReadMemStats(&after)
		slices.Sort(samples)

		return map[string]any{"kind": kind, "requests": requests, "elapsed_ns": elapsed.Nanoseconds(), "p50_ns": samples[127], "p95_ns": samples[242], "allocated_bytes": after.TotalAlloc - before.TotalAlloc, "allocations": after.Mallocs - before.Mallocs, "vertex_pool_hits": h1 - h0, "vertex_pool_misses": m1 - m0, "observed_addresses": observed}
	}
	result := map[string]any{"entries": entries, "build": build, "max_keys": 8, "setup_ns": setup.Nanoseconds(), "minimum_version": e.MinimumVersion(), "before_history": audit()}
	measure := func() []map[string]any { return []map[string]any{query("Descriptor"), query("Root"), query("Get")} }
	result["before_queries"] = measure()
	stateSnapshots, verdictSnapshots := 0, 0

	e.SetOnLocalEffect(func(_ effects.Tip, eff *pb.Effect) {
		if s := eff.GetSnapshot(); s != nil {
			if s.GetState() != nil {
				stateSnapshots++
			} else {
				verdictSnapshots++
			}
		}
	})

	compactStart := time.Now()

	for _, key := range addresses {
		c := e.NewContext() // Deliberately no BeginTx: exercise ordinary auto-compaction.

		_, _, err := c.GetSnapshotInternal(key)
		if err != nil {
			c.Abort()
			t.Fatal(err)
		}

		if err := c.Flush(); err != nil {
			c.Abort()
			t.Fatal(err)
		}
	}

	e.SetOnLocalEffect(nil)

	result["compaction"] = map[string]any{"elapsed_ns": time.Since(compactStart).Nanoseconds(), "state_snapshots_emitted": stateSnapshots, "verdict_snapshots_emitted": verdictSnapshots}
	verify := e.NewContext()
	stats, err := checkTree(tree, verify)
	verify.Abort()

	if err != nil || stats.Entries != entries {
		t.Fatalf("after compaction: %+v %v", stats, err)
	}

	result["after_history"] = audit()
	result["after_queries"] = measure()

	if os.Getenv("BPLUS_RECON_PROFILE") != "" {
		writeAlloc := func(name string) {
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
		writeAlloc("alloc-before.pprof")

		f, err := os.Create(filepath.Join(dir, "cpu.pprof"))
		if err != nil {
			t.Fatal(err)
		}

		if err := pprof.StartCPUProfile(f); err != nil {
			t.Fatal(err)
		}

		result["after_profile_queries"] = query("Get")

		pprof.StopCPUProfile()

		if err := f.Close(); err != nil {
			t.Fatal(err)
		}

		writeAlloc("alloc-after.pprof")
	}

	final := e.NewContext()
	actual, err := tree.Scan(final, nil, nil, 0)
	final.Abort()

	if err != nil || len(actual) != len(contents) {
		t.Fatalf("final scan: %v", err)
	}

	for i := range actual {
		if !bytes.Equal(actual[i].Key, contents[i].Key) || !bytes.Equal(actual[i].Value, contents[i].Value) {
			t.Fatal("compaction changed content")
		}
	}

	raw, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(dir, "diagnostic.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}

	fmt.Fprintf(os.Stderr, "RECONSTRUCTION_DIAGNOSTIC entries=%d build=%s state_snapshots=%d\n", entries, build, stateSnapshots)
}
