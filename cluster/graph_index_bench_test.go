package cluster

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/engine/graph"
	"github.com/swytchdb/engine/graph/bplus"
	"github.com/swytchdb/engine/graph/critbit"
	"github.com/swytchdb/engine/internal/compatversion"
)

// These adapters normalize only API shape. Both implementations use fresh,
// contexts; graph builders never survive an operation. Transactions are the
// default; critbit can also run with GRAPH_INDEX_TRANSACTION_MODE=non-transactional.
// The key/value bytes, request traces, QUIC fixture and measurement boundaries
// are shared. The existing B+ benchmarks remain available as historical controls.
type graphIndexOperation interface {
	get([]byte) ([]byte, bool, error)
	put([]byte, []byte) error
	delete([]byte) (bool, error)
	scan([]byte, []byte, int) ([]bplus.Entry, error)
	emit() error
}

const graphIndexBenchKey = "bench-index"

type graphIndexSpec struct {
	name             string
	order            int
	nonTransactional bool
}

func graphIndexBenchmarkSpec(t testing.TB, algorithm string, order int) graphIndexSpec {
	t.Helper()

	mode := os.Getenv("GRAPH_INDEX_TRANSACTION_MODE")
	if mode != "" && mode != "caller-owned" && mode != "non-transactional" {
		t.Fatal("invalid GRAPH_INDEX_TRANSACTION_MODE")
	}

	if mode == "non-transactional" && algorithm != "Critbit" {
		t.Fatal("non-transactional mode is supported only for Critbit; set GRAPH_INDEX_ALGORITHM=Critbit")
	}

	return graphIndexSpec{name: algorithm, order: order, nonTransactional: mode == "non-transactional"}
}

func (s graphIndexSpec) begin(c *effects.Context) {
	if !s.nonTransactional {
		c.BeginTx()
	}
}

func (s graphIndexSpec) transactionMode() string {
	if s.nonTransactional {
		return "non-transactional"
	}

	return "caller-owned"
}

func (s graphIndexSpec) label() string {
	if s.nonTransactional {
		return s.name + "NonTransactional"
	}

	return s.name
}

// Neutral metrics: pages and routing nodes are different units. Count actual
// writer-local effects and addressed objects instead of inventing page counts.
type graphIndexPhase struct {
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
	DataEffects       uint64 `json:"data_effects"`
	WrittenAddresses  int    `json:"written_addresses"`
	BindKeys          uint64 `json:"bind_keys"`
	MaxBindKeys       int    `json:"max_bind_keys"`
	Snapshots         uint64 `json:"snapshots"`
	EvidenceSnapshots uint64 `json:"evidence_snapshots"`
}

type graphIndexCounters struct {
	mu                                  sync.Mutex
	addresses                           map[string]struct{}
	data, bindKeys, snapshots, evidence uint64
	maxBindKeys                         int
}

func (s *graphIndexCounters) observe(_ effects.Tip, effect *pb.Effect) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if effect.GetData() != nil {
		s.data++
		s.addresses[string(effect.GetIKey())] = struct{}{}
	}

	if bind := effect.GetTxnBind(); bind != nil {
		s.bindKeys += uint64(len(bind.GetKeys()))
		s.maxBindKeys = max(s.maxBindKeys, len(bind.GetKeys()))
	}

	if snap := effect.GetSnapshot(); snap != nil {
		s.snapshots++
		if snap.GetEvidence() != nil {
			s.evidence++
		}
	}
}

func (s *graphIndexCounters) finish(p *graphIndexPhase) {
	s.mu.Lock()
	defer s.mu.Unlock()

	p.DataEffects, p.WrittenAddresses = s.data, len(s.addresses)
	p.BindKeys, p.MaxBindKeys = s.bindKeys, s.maxBindKeys
	p.Snapshots, p.EvidenceSnapshots = s.snapshots, s.evidence
}

func (s graphIndexSpec) create(c *effects.Context) error {
	if s.name == "BPlus" {
		_, err := bplus.Create(c, graphIndexBenchKey, bplus.Options{MaxKeys: s.order})

		return err
	}

	return critbit.Create(graphIndexBenchKey, graph.BytesType).Flush(c)
}

func (s graphIndexSpec) open(c *effects.Context) (graphIndexOperation, error) {
	if s.name == "BPlus" {
		return &bplusIndexOperation{tree: bplus.Open(graphIndexBenchKey), ctx: c}, nil
	}

	tree, err := critbit.Open(graphIndexBenchKey, c, graph.BytesType)

	return &critbitIndexOperation{tree: tree, ctx: c}, err
}

type bplusIndexOperation struct {
	tree *bplus.Tree
	ctx  *effects.Context
}

func (o *bplusIndexOperation) get(k []byte) ([]byte, bool, error) { return o.tree.Get(o.ctx, k) }
func (o *bplusIndexOperation) put(k, v []byte) error              { return o.tree.Put(o.ctx, k, v) }
func (o *bplusIndexOperation) delete(k []byte) (bool, error)      { return o.tree.Delete(o.ctx, k) }
func (o *bplusIndexOperation) scan(l, u []byte, n int) ([]bplus.Entry, error) {
	return o.tree.Scan(o.ctx, l, u, n)
}
func (o *bplusIndexOperation) emit() error { return nil }

type critbitIndexOperation struct {
	tree *critbit.Crit[*graph.Bytes]
	ctx  *effects.Context
}

func (o *critbitIndexOperation) get(k []byte) ([]byte, bool, error) {
	v, err := o.tree.Find(string(k))
	if err != nil || v == nil {
		return nil, false, err
	}

	return v.Value(), true, nil
}

func (o *critbitIndexOperation) put(k, v []byte) error {
	return o.tree.Insert(string(k), graph.NewBytes(v))
}
func (o *critbitIndexOperation) delete(k []byte) (bool, error) { return o.tree.Delete(string(k)) }
func (o *critbitIndexOperation) scan(l, u []byte, n int) ([]bplus.Entry, error) {
	entries, err := o.tree.Scan(string(l), string(u), n)
	if err != nil {
		return nil, err
	}

	result := make([]bplus.Entry, len(entries))
	for i, entry := range entries {
		result[i] = bplus.Entry{Key: []byte(entry.Key), Value: entry.Value.Value()}
	}

	return result, nil
}
func (o *critbitIndexOperation) emit() error { return o.tree.Flush(o.ctx) }

func graphIndexVersion(t testing.TB) string {
	t.Helper()

	v := os.Getenv("BPLUS_SCALE_VERSION")
	if v == "" {
		v = "1.8.0"
	}

	if compatversion.Normalize(v) == "" {
		t.Fatal("invalid BPLUS_SCALE_VERSION")
	}

	return v
}

func graphIndexFixture(t testing.TB, spec graphIndexSpec, peers, entries int, progress func(string, int)) []*effects.Engine {
	t.Helper()

	report := func(stage string, n int) {
		if progress != nil {
			progress(stage, n)
		}
	}
	version := graphIndexVersion(t)
	previous := Version
	Version = version

	t.Cleanup(func() { Version = previous })

	engines := make([]*effects.Engine, peers)
	handlers := make([]EffectHandler, peers)
	readers := make([]LogReader, peers)

	ids := make([]NodeId, peers)
	for i := range peers {
		e := effects.NewEngine(effects.EngineConfig{DefaultMode: effects.SafeMode, BuildVersion: version})
		engines[i], ids[i] = e, e.NodeID()
		handlers[i], readers[i] = NewEngineEffectHandler(e), NewEngineLogReader(e.EffectCache())

		t.Cleanup(func() {
			if err := e.Close(); err != nil {
				t.Error(err)
			}
		})
	}

	if scope, ok := t.(*bplusBenchScope); ok && scope.memory != nil {
		scope.memory.attach(engines)
	}

	if peers > 1 {
		managers := startNNodeCluster(t, handlers, readers, ids...)
		for i, e := range engines {
			e.SetBroadcaster(managers[i])
		}
	}

	for _, e := range engines {
		bplusBenchWait(t, "missing version announcements", func() bool { return e.MinimumVersion() == compatversion.Normalize(version) })

		if err := e.Subscribe(graphIndexBenchKey); err != nil {
			t.Fatal(err)
		}
	}

	c := engines[0].NewContext()
	if err := spec.create(c); err != nil {
		c.Abort()
		t.Fatal(err)
	}

	bplusBenchFlush(t, c)
	c.Abort()

	for _, e := range engines[1:] {
		bplusBenchWait(t, "missing descriptor", func() bool {
			state, _, _, err := e.GetSnapshot(graphIndexBenchKey)
			if err != nil {
				t.Fatal(err)
			}

			return state.GetTypeTag() == pb.ValueType_TYPE_GRAPH
		})
	}

	buildCounters := &graphIndexCounters{addresses: make(map[string]struct{})}

	engines[0].SetOnLocalEffect(func(tip effects.Tip, effect *pb.Effect) {
		if effect.GetTxnBind() != nil {
			buildCounters.observe(tip, effect)
		}
	})
	defer engines[0].SetOnLocalEffect(nil)

	report("descriptor_ready", 0)

	for i := range entries {
		c = engines[0].NewContext()
		spec.begin(c)

		op, err := spec.open(c)
		if err != nil {
			c.Abort()
			t.Fatal(err)
		}

		if i == 0 && spec.name == "BPlus" {
			if err := bplus.Open(graphIndexBenchKey).Initialize(c); err != nil {
				c.Abort()
				t.Fatal(err)
			}
		}

		if err := op.put(bplusBenchKey(i), bytes.Repeat([]byte{7}, 32)); err != nil {
			c.Abort()
			t.Fatal(err)
		}

		if err := op.emit(); err != nil {
			c.Abort()
			t.Fatal(err)
		}

		bplusBenchFlush(t, c)
		c.Abort()

		if (i+1)%256 == 0 || i+1 == entries {
			report("inserted", i+1)
		}
	}

	report("flush_start", entries)

	if spec.nonTransactional {
		var observed graphIndexPhase
		buildCounters.finish(&observed)

		if observed.BindKeys != 0 {
			t.Fatal("non-transactional construction emitted transaction binds")
		}
	}

	engines[0].SetOnLocalEffect(nil)
	report("flushed", entries)
	graphIndexContents(t, spec, engines, graphIndexWant(entries))
	report("verified", entries)

	return engines
}

func graphIndexWant(entries int) map[string][]byte {
	want := make(map[string][]byte, entries)
	for i := range entries {
		want[string(bplusBenchKey(i))] = bytes.Repeat([]byte{7}, 32)
	}

	return want
}

func graphIndexContents(t testing.TB, spec graphIndexSpec, engines []*effects.Engine, want map[string][]byte) {
	t.Helper()

	for i, e := range engines {
		bplusBenchWait(t, fmt.Sprintf("%s peer %d contents did not converge", spec.name, i), func() bool {
			c := e.NewContext()
			defer c.Abort()

			spec.begin(c)

			op, err := spec.open(c)
			if err != nil {
				t.Fatal(err)
			}

			entries, err := op.scan(nil, nil, 0)
			if err != nil {
				t.Fatal(err)
			}

			if len(entries) != len(want) {
				return false
			}

			for j, entry := range entries {
				v, exists := want[string(entry.Key)]
				if !exists || !bytes.Equal(v, entry.Value) {
					return false
				}

				if j > 0 && bytes.Compare(entries[j-1].Key, entry.Key) >= 0 {
					t.Fatal("scan not strictly ordered")
				}
			}

			return true
		})
	}
}

// Preparation includes Open, traversal, mutation, and graph emission. Only the
// engine Context.Flush goes in flush_ns; Abort is included in total latency.
func graphIndexRequest(t testing.TB, spec graphIndexSpec, e *effects.Engine, r bplusRequest, want map[string][]byte) (prepare, flush int64) {
	t.Helper()

	start := time.Now()

	c := e.NewContext()
	defer c.Abort()

	spec.begin(c)

	op, err := spec.open(c)
	if err != nil {
		t.Fatal(err)
	}

	switch r.kind {
	case 'g':
		v, found, err := op.get(r.key)
		if err != nil || !found || !bytes.Equal(v, r.value) {
			t.Fatalf("get %x: found=%v err=%v", r.key, found, err)
		}
	case 's':
		entries, err := op.scan(r.key, nil, 16)
		if err != nil || len(entries) != 16 {
			t.Fatalf("scan: len=%d err=%v", len(entries), err)
		}

		for i, entry := range entries {
			if !bytes.Equal(entry.Value, want[string(entry.Key)]) || bytes.Compare(entry.Key, r.key) < 0 || (i > 0 && bytes.Compare(entries[i-1].Key, entry.Key) >= 0) {
				t.Fatal("incorrect bounded scan")
			}
		}
	default:
		if r.kind == 'p' {
			err = op.put(r.key, r.value)
		} else {
			var found bool

			found, err = op.delete(r.key)
			if err == nil && !found {
				err = fmt.Errorf("missing deletion key %x", r.key)
			}
		}

		if err != nil {
			t.Fatal(err)
		}

		if err = op.emit(); err != nil {
			t.Fatal(err)
		}

		prepare = time.Since(start).Nanoseconds()
		start = time.Now()

		bplusBenchFlush(t, c)

		flush = time.Since(start).Nanoseconds()
	}

	return prepare, flush
}

func graphIndexQuiet(t testing.TB) {
	logger := slog.Default()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(logger) })
}

// Same complete 40-request traces as BenchmarkBPlusEndToEnd, with fresh fixtures.
func BenchmarkGraphIndexEndToEnd(b *testing.B) {
	graphIndexQuiet(b)

	for _, algorithm := range []string{"BPlus", "Critbit"} {
		if selected := os.Getenv("GRAPH_INDEX_ALGORITHM"); selected != "" && selected != algorithm {
			continue
		}

		spec := graphIndexBenchmarkSpec(b, algorithm, 8)

		for _, peers := range []int{1, 3, 5} {
			for _, entries := range []int{32, 128} {
				for _, workload := range []bplusWorkload{{"ReadHeavy", 18, 1}, {"Mixed", 10, 5}, {"Replace", 0, 20}, {"Structural", 0, 0}} {
					b.Run(fmt.Sprintf("%s/Peers%d/Entries%d/BuildIncremental/%s", spec.label(), peers, entries, workload.name), func(b *testing.B) {
						b.StopTimer()
						b.ReportAllocs()

						trace, want := bplusTrace(entries, workload)

						var (
							samples              []int64
							prepare, flush, read int64
							writes, reads        int
						)

						for range b.N {
							func() {
								scope := &bplusBenchScope{TB: b}
								defer scope.close()

								engines := graphIndexFixture(scope, spec, peers, entries, nil)

								b.StartTimer()

								for _, r := range trace {
									start := time.Now()
									p, f := graphIndexRequest(b, spec, engines[0], r, want)
									elapsed := time.Since(start).Nanoseconds()
									samples = append(samples, elapsed)
									prepare += p
									flush += f

									if r.kind == 'g' {
										reads++
										read += elapsed
									} else {
										writes++
									}
								}

								b.StopTimer()
								graphIndexContents(b, spec, engines, want)
							}()
						}

						slices.Sort(samples)
						b.ReportMetric(40, "requests/trace")
						b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(len(samples)), "ns/request")
						b.ReportMetric(float64(len(samples))/b.Elapsed().Seconds(), "requests/s")
						b.ReportMetric(float64(samples[(len(samples)-1)/2]), "p50-ns/request")
						b.ReportMetric(float64(samples[(len(samples)-1)*95/100]), "p95-ns/request")

						if reads > 0 {
							b.ReportMetric(float64(read)/float64(reads), "read-ns/read")
						}

						if writes > 0 {
							b.ReportMetric(float64(prepare)/float64(writes), "prepare-ns/write")
							b.ReportMetric(float64(flush)/float64(writes), "flush-ns/write")
						}
					})
				}
			}
		}
	}
}

func BenchmarkGraphIndexRead(b *testing.B) {
	graphIndexQuiet(b)

	for _, algorithm := range []string{"BPlus", "Critbit"} {
		if selected := os.Getenv("GRAPH_INDEX_ALGORITHM"); selected != "" && selected != algorithm {
			continue
		}

		spec := graphIndexBenchmarkSpec(b, algorithm, 8)

		for _, entries := range []int{32, 128} {
			for _, kind := range []string{"Get", "Scan16"} {
				b.Run(fmt.Sprintf("%s/Entries%d/BuildIncremental/%s", spec.label(), entries, kind), func(b *testing.B) {
					engines := graphIndexFixture(b, spec, 1, entries, nil)
					want := graphIndexWant(entries)
					requests := bplusScaleTrace(entries, kind, want)
					i := 0

					b.ReportAllocs()

					for b.Loop() {
						graphIndexRequest(b, spec, engines[0], requests[i%len(requests)], want)
						i++
					}
				})
			}
		}
	}
}

func TestGraphIndexFivePeers(t *testing.T) {
	graphIndexQuiet(t)

	for _, spec := range []graphIndexSpec{{name: "BPlus", order: 8}, {name: "Critbit", order: 8}, {name: "Critbit", order: 8, nonTransactional: true}} {
		t.Run(spec.label(), func(t *testing.T) {
			engines := graphIndexFixture(t, spec, 5, 32, nil)
			counters := &graphIndexCounters{addresses: make(map[string]struct{})}

			engines[0].SetOnLocalEffect(counters.observe)
			defer engines[0].SetOnLocalEffect(nil)

			want := graphIndexWant(32)
			for _, phase := range []string{"Get", "Scan16", "Mixed", "Structural"} {
				for _, r := range bplusScaleTrace(32, phase, want) {
					graphIndexRequest(t, spec, engines[0], r, want)
				}

				graphIndexContents(t, spec, engines, want)
			}

			var observed graphIndexPhase
			counters.finish(&observed)

			if spec.nonTransactional && observed.BindKeys != 0 {
				t.Fatal("non-transactional operations emitted transaction binds")
			}
		})
	}
}

func BenchmarkGraphIndexScale(b *testing.B) {
	if os.Getenv("GRAPH_INDEX_ALGORITHM") == "" {
		b.Skip("set GRAPH_INDEX_ALGORITHM and BPLUS_SCALE_{ENTRIES,PEERS,DIR}")
	}

	algorithm := os.Getenv("GRAPH_INDEX_ALGORITHM")
	if algorithm != "BPlus" && algorithm != "Critbit" {
		b.Fatal("invalid algorithm")
	}

	integer := func(name string) int {
		n, err := strconv.Atoi(os.Getenv(name))
		if err != nil || n <= 0 {
			b.Fatalf("invalid %s", name)
		}

		return n
	}

	entries, peers := integer("BPLUS_SCALE_ENTRIES"), integer("BPLUS_SCALE_PEERS")
	if entries < 32 {
		b.Fatal("at least 32 entries required")
	}

	dir := os.Getenv("BPLUS_SCALE_DIR")
	if dir == "" {
		b.Fatal("missing BPLUS_SCALE_DIR")
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		b.Fatal(err)
	}

	graphIndexQuiet(b)
	spec := graphIndexBenchmarkSpec(b, algorithm, 32)
	b.Run(fmt.Sprintf("%s/Peers%d/Entries%d/BuildIncremental", spec.label(), peers, entries), func(b *testing.B) {
		if b.N != 1 {
			b.Fatal("use -benchtime=1x")
		}

		b.StopTimer()
		b.ReportAllocs()

		file, err := os.Create(filepath.Join(dir, "events.jsonl"))
		if err != nil {
			b.Fatal(err)
		}

		defer func() {
			if err := file.Close(); err != nil {
				b.Error(err)
			}
		}()

		enc := json.NewEncoder(file)
		emit := func(v any) {
			if err := enc.Encode(v); err != nil {
				b.Fatal(err)
			}
		}
		started := time.Now()

		memory := startBplusMemoryTimeline(b, dir, started)
		defer memory.close()

		progress := func(stage string, n int) {
			memory.stage(stage)

			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			emit(map[string]any{"stage": stage, "completed": n, "elapsed_ns": time.Since(started).Nanoseconds(), "heap_bytes": m.HeapAlloc, "total_allocated_bytes": m.TotalAlloc, "allocations": m.Mallocs, "gcs": m.NumGC})
			fmt.Fprintf(os.Stderr, "SCALE %s %d/%d %.3fs\n", stage, n, entries, time.Since(started).Seconds())
		}
		progress("start", 0)

		scope := &bplusBenchScope{TB: b, memory: memory}
		defer func() { memory.detach(); scope.close() }()

		engines := graphIndexFixture(scope, spec, peers, entries, progress)

		runtime.GC()
		progress("ready_after_gc", entries)
		want := graphIndexWant(entries)

		phaseNames := []string{"Get", "Scan16", "ReadHeavy", "Mixed", "Replace", "Structural"}
		phases := make([]graphIndexPhase, 0, len(phaseNames))

		for _, name := range phaseNames {
			trace := bplusScaleTrace(entries, name, want)
			progress(name+"_start", 0)
			phase := graphIndexPhase{Name: name, Requests: len(trace)}
			counters := &graphIndexCounters{addresses: make(map[string]struct{})}
			engines[0].SetOnLocalEffect(counters.observe)

			samples := make([]int64, 0, len(trace))

			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			b.StartTimer()

			for _, r := range trace {
				start := time.Now()
				p, f := graphIndexRequest(b, spec, engines[0], r, want)
				elapsed := time.Since(start).Nanoseconds()
				phase.TotalNS += elapsed
				phase.PrepareNS += p
				phase.FlushNS += f

				samples = append(samples, elapsed)

				if r.kind == 'g' || r.kind == 's' {
					phase.Reads++
				} else {
					phase.Writes++
				}
			}

			b.StopTimer()
			runtime.ReadMemStats(&after)
			engines[0].SetOnLocalEffect(nil)
			counters.finish(&phase)

			if spec.nonTransactional && phase.BindKeys != 0 {
				b.Fatal("non-transactional phase emitted transaction binds")
			}

			if phase.EvidenceSnapshots != 0 {
				b.Fatal("modern fixture emitted compatibility evidence")
			}

			slices.Sort(samples)
			phase.P50NS, phase.P95NS = samples[(len(samples)-1)/2], samples[(len(samples)-1)*95/100]
			phase.AllocatedBytes, phase.Allocations = after.TotalAlloc-before.TotalAlloc, after.Mallocs-before.Mallocs
			phase.HeapBytes, phase.GCs = after.HeapAlloc, after.NumGC-before.NumGC
			phases = append(phases, phase)
			emit(phase)
			progress(name+"_done", len(trace))
			b.ReportMetric(float64(phase.TotalNS)/float64(phase.Requests), name+"-ns/request")
			b.ReportMetric(float64(phase.AllocatedBytes)/float64(phase.Requests), name+"-B/request")
			graphIndexContents(b, spec, engines, want)
			progress(name+"_verified", len(trace))
		}

		runtime.GC()
		progress("finished_after_gc", entries)

		versions := make([]string, len(engines))
		for i, e := range engines {
			versions[i] = e.MinimumVersion()
		}

		data, err := json.MarshalIndent(map[string]any{"algorithm": algorithm, "entries": entries, "peers": peers, "build": "Incremental", "bplus_max_keys": 32, "phases": phases, "build_version": graphIndexVersion(b), "minimum_versions": versions, "transaction_mode": spec.transactionMode()}, "", "  ")
		if err != nil {
			b.Fatal(err)
		}

		if err := os.WriteFile(filepath.Join(dir, "result.json"), data, 0o644); err != nil {
			b.Fatal(err)
		}

		memory.observeIdle(b)
		runtime.KeepAlive(engines)
	})
}
