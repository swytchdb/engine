package cluster

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/engine/graph/bplus"
	"github.com/swytchdb/engine/internal/compatversion"
)

const bplusTraceLength = 40

// Scope fixture cleanup to one benchmark iteration. Fresh engines keep both
// tree shape and history length independent of Go's calibration of b.N.
type bplusBenchScope struct {
	testing.TB

	cleanups []func()
	memory   *bplusMemoryTimeline
}

func (s *bplusBenchScope) Cleanup(f func()) { s.cleanups = append(s.cleanups, f) }
func (s *bplusBenchScope) close() {
	for _, v := range slices.Backward(s.cleanups) {
		v()
	}
}

type bplusWorkload struct {
	name                string
	reads, replacements int // per twenty requests; the remainder insert/delete
}

type bplusRequest struct {
	kind       byte // g(et), p(ut), d(elete)
	key, value []byte
}

func bplusBenchKey(i int) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(i))

	return key
}

func bplusTrace(entries int, workload bplusWorkload) ([]bplusRequest, map[string][]byte) {
	want := make(map[string][]byte, entries)
	for i := range entries {
		want[string(bplusBenchKey(i))] = bytes.Repeat([]byte{7}, 32)
	}

	structural := bplusTraceLength - (workload.reads+workload.replacements)*2
	next := 0

	trace := make([]bplusRequest, bplusTraceLength)
	for i := range trace {
		r := bplusRequest{key: bplusBenchKey((i * 17) % entries)}
		switch {
		case i%20 < workload.reads:
			r.kind = 'g'
			r.value = want[string(r.key)]
		case i%20 < workload.reads+workload.replacements:
			r.kind, r.value = 'p', bytes.Repeat([]byte{8}, 32)
			want[string(r.key)] = r.value
		default:
			if next < structural/2 {
				// Grow the right edge, then remove these keys in reverse order.
				// With Order8 and these fixture sizes this causes actual splits
				// and (for longer structural runs) merges, not just replacements.
				r.kind, r.key, r.value = 'p', bplusBenchKey(entries+next), bytes.Repeat([]byte{9}, 32)
			} else {
				r.kind, r.key = 'd', bplusBenchKey(entries+structural-next-1)
			}

			next++
		}

		trace[i] = r
	}

	return trace, want
}

func bplusBenchFlush(t testing.TB, c *effects.Context) {
	t.Helper()

	if err := c.Flush(); err != nil {
		if scope, ok := t.(*bplusBenchScope); ok && scope.memory != nil {
			scope.memory.flushFailure(err, c)
			t.Fatal(err)
		}

		c.Abort()
		t.Fatal(err)
	}
}

func bplusBenchWait(t testing.TB, description string, f func() bool) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatal(description)
		}

		time.Sleep(time.Millisecond)
	}
}

func bplusBenchContents(t testing.TB, engines []*effects.Engine, tree *bplus.Tree, want map[string][]byte) {
	t.Helper()

	for i, e := range engines {
		bplusBenchWait(t, fmt.Sprintf("peer %d did not converge", i), func() bool {
			c := e.NewContext()
			got, err := tree.Scan(c, nil, nil, 0)
			c.Abort()

			if err != nil {
				t.Fatal(err)
			}

			if len(got) != len(want) {
				return false
			}

			for j, entry := range got {
				value, ok := want[string(entry.Key)]
				if !ok || !bytes.Equal(entry.Value, value) {
					return false
				}

				if j > 0 && bytes.Compare(got[j-1].Key, entry.Key) >= 0 {
					t.Fatal("scan is not strictly ordered")
				}
			}

			return true
		})
	}
}

func bplusBenchFixture(t testing.TB, peers, entries int, build string, observer *bplusProfileObserver) ([]*effects.Engine, *bplus.Tree) {
	return bplusBenchFixtureWithOptions(t, peers, entries, build, observer, 8, nil)
}

func bplusBenchFixtureWithOptions(t testing.TB, peers, entries int, build string, observer *bplusProfileObserver, maxKeys int, progress func(string, int)) ([]*effects.Engine, *bplus.Tree) {
	t.Helper()

	buildVersion := os.Getenv("BPLUS_SCALE_VERSION")
	if buildVersion != "" {
		if compatversion.Normalize(buildVersion) == "" {
			t.Fatal("invalid BPLUS_SCALE_VERSION")
		}

		previous := Version
		Version = buildVersion

		t.Cleanup(func() { Version = previous })
	}

	report := func(stage string, completed int) {
		if progress != nil {
			progress(stage, completed)
		}
	}

	engines := make([]*effects.Engine, peers)
	handlers := make([]EffectHandler, peers)
	readers := make([]LogReader, peers)

	ids := make([]NodeId, peers)
	for i := range peers {
		e := effects.NewEngine(effects.EngineConfig{DefaultMode: effects.SafeMode, BuildVersion: buildVersion})
		engines[i], ids[i] = e, e.NodeID()

		handlers[i], readers[i] = NewEngineEffectHandler(e), NewEngineLogReader(e.EffectCache())
		if observer != nil {
			handlers[i] = &bplusProfileHandler{EngineEffectHandler: NewEngineEffectHandler(e), observer: observer, peer: uint64(e.NodeID())}
			readers[i] = &bplusProfileReader{EngineLogReader: NewEngineLogReader(e.EffectCache()), observer: observer}
		}

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

	if buildVersion != "" {
		for i, e := range engines {
			bplusBenchWait(t, fmt.Sprintf("peer %d missing version announcements", i), func() bool {
				return e.MinimumVersion() == compatversion.Normalize(buildVersion)
			})
			fmt.Fprintf(os.Stderr, "SCALE_VERSION peer=%d minimum=%s\n", i, e.MinimumVersion())
		}
	}

	const key = "bench-index"
	for _, e := range engines {
		if err := e.Subscribe(key); err != nil {
			t.Fatal(err)
		}
	}

	c := engines[0].NewContext()
	defer c.Abort()

	tree, err := bplus.Create(c, key, bplus.Options{MaxKeys: maxKeys})
	if err != nil {
		t.Fatal(err)
	}

	bplusBenchFlush(t, c)

	for _, e := range engines[1:] {
		bplusBenchWait(t, "missing descriptor", func() bool {
			state, _, _, err := e.GetSnapshot(key)
			if err != nil {
				t.Fatal(err)
			}

			return state.GetTypeTag() == pb.ValueType_TYPE_GRAPH
		})
	}

	report("descriptor_ready", 0)

	c = engines[0].NewContext()
	defer func() { c.Abort() }()

	if err := tree.Initialize(c); err != nil {
		t.Fatal(err)
	}

	want := make(map[string][]byte, entries)
	for i := range entries {
		key, value := bplusBenchKey(i), bytes.Repeat([]byte{7}, 32)
		if err := tree.Put(c, key, value); err != nil {
			t.Fatal(err)
		}

		want[string(key)] = value

		if build == "Incremental" {
			bplusBenchFlush(t, c)
			c = engines[0].NewContext()
		}

		if (i+1)%256 == 0 || i+1 == entries {
			report("inserted", i+1)
		}
	}

	report("flush_start", entries)
	bplusBenchFlush(t, c)
	report("flushed", entries)
	// Establish warm exact-address subscriptions and verify the fixture outside
	// timing. Region registration alone must not masquerade as bootstrapping.
	bplusBenchContents(t, engines, tree, want)
	report("verified", entries)

	return engines, tree
}

// One Go iteration is a complete fixed 40-request trace. The one-peer case
// uses the same SafeMode API without a network; 3/5 peers use real loopback QUIC.
// The completion boundary is each Context.Flush, not visibility on every peer.
func BenchmarkBPlusEndToEnd(b *testing.B) {
	logger := slog.Default()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
	b.Cleanup(func() { slog.SetDefault(logger) })

	for _, peers := range []int{1, 3, 5} {
		for _, entries := range []int{32, 128} {
			for _, build := range []string{"SingleTx", "Incremental"} {
				for _, workload := range []bplusWorkload{
					{"ReadHeavy", 18, 1}, {"Mixed", 10, 5}, {"Replace", 0, 20}, {"Structural", 0, 0},
				} {
					b.Run(fmt.Sprintf("Peers%d/Entries%d/Order8/Build%s/%s", peers, entries, build, workload.name), func(b *testing.B) {
						benchmarkBPlusTrace(b, peers, entries, build, workload)
					})
				}
			}
		}
	}
}

func benchmarkBPlusTrace(b *testing.B, peers, entries int, build string, workload bplusWorkload) {
	b.StopTimer()

	trace, want := bplusTrace(entries, workload)
	samples := make([]int64, 0, b.N*bplusTraceLength)

	var (
		readTime, prepareTime, flushTime   time.Duration
		reads, writes                      int
		newPages, deletedPages, pageWrites atomic.Uint64
	)

	b.ReportAllocs()

	for range b.N {
		func() {
			scope := &bplusBenchScope{TB: b}
			defer scope.close()

			engines, tree := bplusBenchFixture(scope, peers, entries, build, nil)
			e := engines[0]
			e.SetOnLocalEffect(func(_ effects.Tip, eff *pb.Effect) {
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
			b.StartTimer()

			for _, request := range trace {
				start := time.Now()

				c := e.NewContext()
				if request.kind == 'g' {
					got, found, err := tree.Get(c, request.key)
					c.Abort()

					if err != nil || !found || !bytes.Equal(got, request.value) {
						b.Fatalf("lookup: %v %v", found, err)
					}

					readTime += time.Since(start)
					reads++
				} else {
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

					prepareTime += time.Since(start)
					flushStart := time.Now()

					bplusBenchFlush(b, c)

					flushTime += time.Since(flushStart)
					writes++
				}

				samples = append(samples, time.Since(start).Nanoseconds())
			}

			b.StopTimer()
			e.SetOnLocalEffect(nil)
			// Correctness and eventual delivery checks are deliberately untimed.
			bplusBenchContents(b, engines, tree, want)
		}()
	}

	slices.Sort(samples)
	requests := float64(len(samples))

	b.ReportMetric(bplusTraceLength, "requests/trace")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/requests, "ns/request")
	b.ReportMetric(requests/b.Elapsed().Seconds(), "requests/s")
	b.ReportMetric(float64(samples[(len(samples)-1)/2]), "p50-ns/request")
	b.ReportMetric(float64(samples[(len(samples)-1)*95/100]), "p95-ns/request")

	if reads > 0 {
		b.ReportMetric(float64(readTime.Nanoseconds())/float64(reads), "read-ns/read")
	}

	if writes > 0 {
		b.ReportMetric(float64(prepareTime.Nanoseconds())/float64(writes), "prepare-ns/write")
		b.ReportMetric(float64(flushTime.Nanoseconds())/float64(writes), "flush-ns/write")
	}

	b.ReportMetric(float64(newPages.Load())/float64(b.N), "new-pages/trace")
	b.ReportMetric(float64(deletedPages.Load())/float64(b.N), "deleted-pages/trace")
	b.ReportMetric(float64(pageWrites.Load())/requests, "page-writes/request")
}
