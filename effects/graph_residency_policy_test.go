package effects

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/graphstore"
	"github.com/swytchdb/engine/keytrie"
)

func TestDiskPolicyDemotesThenForgetsRAM(t *testing.T) {
	e := tierEngine(t, t.TempDir())
	durableSet(t, e, "x", "retained")
	before := e.index.Contains("x")
	selectOne := func() {
		t.Helper()

		if n := e.index.EvictBatchWith(1, e.graphs.evictCandidate); n != 1 {
			t.Fatalf("completed transitions: %d", n)
		}

		e.reclaimUnreferenced()
	}
	selectOne()

	if e.index.Contains("x") != before || e.DiskStats().Demotions != 1 {
		t.Fatal("first selection must preserve the frontier and demote")
	}

	if _, ok := e.subscriptions.Load("x"); !ok {
		t.Fatal("demotion unsubscribed the key")
	}

	selectOne()

	if e.index.Contains("x") != nil {
		t.Fatal("second selection retained RAM frontier")
	}

	if _, ok := e.subscriptions.Load("x"); ok {
		t.Fatal("RAM eviction retained subscription")
	}

	e.graphs.mu.Lock()
	_, tracked := e.graphs.keys["x"]
	e.graphs.mu.Unlock()

	if tracked {
		t.Fatal("RAM eviction retained graph metadata")
	}

	if _, err := e.graphs.store.Read("x"); err != nil {
		t.Fatalf("unlimited disk lost package: %v", err)
	}

	if got := durableValue(t, e, "x"); got != "retained" {
		t.Fatalf("disk-only read: %q", got)
	}

	if _, ok := e.subscriptions.Load("x"); !ok {
		t.Fatal("rehydration did not resubscribe")
	}
}

func TestDiskHydrationDoesNotRetainBorrowedLookupKey(t *testing.T) {
	for _, forget := range []bool{false, true} {
		t.Run(fmt.Sprint(forget), func(t *testing.T) {
			e := tierEngine(t, t.TempDir())
			durableSet(t, e, "x", "retained")
			demoteTest(t, e, "x")

			if forget && !e.graphs.evictCandidate("x", e.index.Contains("x"), nil) {
				t.Fatal("failed to forget cold key")
			}

			// Redis lookups borrow command-buffer bytes for the duration of a call.
			buf := []byte{'x'}

			key := unsafe.String(unsafe.SliceData(buf), len(buf))
			if err := e.hydrateDisk(key); err != nil {
				t.Fatal(err)
			}

			buf[0] = 'y'

			g := e.graphs
			g.mu.Lock()
			defer g.mu.Unlock()

			if g.keys["x"] == nil {
				t.Fatal("hydration retained the borrowed key as a map key")
			}

			for _, owners := range g.owners {
				for _, owner := range owners {
					if owner != "x" {
						t.Fatalf("hydration retained borrowed owner key: %q", owner)
					}
				}
			}
		})
	}
}

func BenchmarkDiskTierPressure(b *testing.B) {
	limit := int64(0)

	e, err := OpenEngine(context.Background(), EngineConfig{DataDir: b.TempDir(), MemoryLimit: 16 << 20, DiskLimit: &limit, DefaultMode: UnsafeMode})
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		if err := e.Close(); err != nil {
			b.Error(err)
		}
	}()

	keys := make([]string, 512)
	value := strings.Repeat("x", 64<<10)

	for i := range keys {
		keys[i] = fmt.Sprint(i)
		durableSet(b, e, keys[i], value)
	}

	var next atomic.Uint64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := keys[next.Add(1)%uint64(len(keys))]

			state, _, _, err := e.GetSnapshot(key)
			if err != nil || state == nil {
				b.Errorf("disk-retained key %s: state=%v err=%v", key, state, err)

				return
			}
		}
	})
	b.StopTimer()

	stats := e.DiskStats()
	b.ReportMetric(float64(stats.Demotions), "demotions")
	b.ReportMetric(float64(stats.Hydrations), "hydrations")
}

func TestDiskMaintenanceDoesNotStopUnrelatedReads(t *testing.T) {
	e := tierEngine(t, t.TempDir())
	durableSet(t, e, "busy", "busy")

	other := "other"
	for e.graphs.residencyLock(other) == e.graphs.residencyLock("busy") {
		other += "x"
	}

	durableSet(t, e, other, "available")
	mu := e.graphs.residencyLock("busy")
	mu.Lock()
	done := make(chan error, 1)

	go func() {
		_, _, _, err := e.GetSnapshot(other)
		done <- err
	}()

	select {
	case err := <-done:
		mu.Unlock()

		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		mu.Unlock()

		if err := <-done; err != nil {
			t.Error(err)
		}

		t.Fatal("one key's maintenance blocked another key's read")
	}
}

func TestDiskPolicyConcurrentReadsAndWrites(t *testing.T) {
	e := tierEngine(t, t.TempDir())

	const keys = 24
	for i := range keys {
		durableSet(t, e, fmt.Sprint(i), "initial")
	}

	var wg sync.WaitGroup

	done := make(chan struct{})

	wg.Go(func() {
		defer close(done)

		for range 80 {
			e.index.EvictBatchWith(4, e.graphs.evictCandidate)
			e.reclaimUnreferenced()
		}
	})

	for worker := range 4 {
		wg.Go(func() {
			for i := range 60 {
				key := fmt.Sprint((i*4 + worker) % keys)

				value, _, _, err := e.GetSnapshot(key)
				if err != nil || value == nil {
					t.Errorf("retained key %s: value=%v err=%v", key, value, err)

					return
				}

				c := e.NewContext()
				if err := c.Emit(scalarSet(key, "updated")); err != nil {
					t.Error(err)

					return
				}

				if err := c.Flush(); err != nil {
					t.Error(err)

					return
				}
			}
		})
	}

	wg.Wait()
	<-done
}

func TestDiskPolicySkipsActiveKey(t *testing.T) {
	e := tierEngine(t, t.TempDir())
	durableSet(t, e, "x", "value")

	release := e.holdDisk("x")
	if n := e.index.EvictBatchWith(1, e.graphs.evictCandidate); n != 0 {
		t.Fatal("demoted a key with an active reader")
	}

	release()

	if !e.PinKey("x") {
		t.Fatal("pin failed")
	}

	if n := e.index.EvictBatchWith(1, e.graphs.evictCandidate); n != 0 {
		t.Fatal("selected a pinned key")
	}

	e.UnpinKey("x")
}

func BenchmarkDiskCandidateSelection(b *testing.B) {
	for _, count := range []int{1024, 65536} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			index := keytrie.NewCritbit[struct{}]()
			defer func() { _ = index.Close() }()

			index.SetEvictHooks(nil, nil, nil)

			for i := range count {
				index.Insert(fmt.Sprint(i), nil, keytrie.NewTipSet(keytrie.EffectRef{1, uint64(i)}))
			}

			visit := func(key string, _ *keytrie.TipSet, _ *struct{}) bool {
				index.LoadData(key)

				return true
			}

			b.ResetTimer()

			for b.Loop() {
				index.EvictBatchWith(16, visit)
			}
		})
	}
}

// A peer unsubscribe must not reinstall the local package it is announcing
// as evicted. The hook still needs a live independently retained vertex.
type evictionBroadcaster struct {
	mockBroadcaster

	sent chan struct{}
}

func (b *evictionBroadcaster) BroadcastWithData(n *pb.OffsetNotify, data []byte) {
	b.mockBroadcaster.BroadcastWithData(n, data)
	close(b.sent)
}

func TestDiskEvictionBroadcastDoesNotRehydrate(t *testing.T) {
	e := tierEngine(t, t.TempDir())
	durableSet(t, e, "x", "retained")

	b := &evictionBroadcaster{sent: make(chan struct{})}

	e.broadcaster = b
	for range 2 {
		if n := e.index.EvictBatchWith(1, e.graphs.evictCandidate); n != 1 {
			t.Fatalf("transitions: %d", n)
		}
	}

	select {
	case <-b.sent:
	case <-time.After(3 * time.Second):
		t.Fatal("unsubscribe was not broadcast")
	}

	e.reclaimUnreferenced()

	if e.index.Contains("x") != nil {
		t.Fatal("unsubscribe reinstalled evicted index entry")
	}

	e.graphs.mu.Lock()
	_, tracked := e.graphs.keys["x"]
	e.graphs.mu.Unlock()

	if tracked {
		t.Fatal("unsubscribe reinstalled graph metadata")
	}

	if !e.graphs.store.Has("x") {
		t.Fatal("unsubscribe removed retained package")
	}
}

func TestDiskHydrateWaitsForFrontierPublication(t *testing.T) {
	e := tierEngine(t, t.TempDir())
	// Model the publication window after the loader restored metadata and
	// before it installed the index frontier.
	load := &graphLoad{done: make(chan struct{})}

	e.graphs.mu.Lock()
	e.graphs.keys["x"] = &diskGraph{cold: false}
	e.graphs.loading = map[string]*graphLoad{"x": load}
	e.graphs.mu.Unlock()

	done := make(chan error, 1)
	go func() { done <- e.hydrateDisk("x") }()

	select {
	case err := <-done:
		close(load.done)
		t.Fatalf("hydration returned before frontier publication: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	e.graphs.mu.Lock()
	delete(e.graphs.loading, "x")
	close(load.done)
	e.graphs.mu.Unlock()

	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestDiskWriterBackpressureDoesNotBlockResidentRead(t *testing.T) {
	e := tierEngine(t, t.TempDir())
	durableSet(t, e, "resident", "available")

	if err := e.graphs.store.Barrier(); err != nil {
		t.Fatal(err)
	}

	entered, resume := make(chan struct{}), make(chan struct{})

	var once sync.Once

	release := func() { once.Do(func() { close(resume) }) }
	defer release()

	if err := e.graphs.store.Update(0, func() []graphstore.Mutation {
		close(entered)
		<-resume

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	<-entered

	// Fill the bounded segment writer queue while its consumer is paused.
	for range 4096 {
		if err := e.graphs.store.Update(0, func() []graphstore.Mutation { return nil }); err != nil {
			t.Fatal(err)
		}
	}

	produced := make(chan error, 1)
	go func() { produced <- e.graphs.putEffect(Tip{90, 1}, scalarSet("blocked", "pending"), nil) }()

	deadline := time.Now().Add(3 * time.Second)
	blocked := false

	for time.Now().Before(deadline) {
		buf := make([]byte, 1<<20)

		buf = buf[:runtime.Stack(buf, true)]
		for _, stack := range strings.Split(string(buf), "\n\n") {
			if strings.Contains(stack, "(*Store).submit") && strings.Contains(stack, "(*graphMirror).putEffect") {
				blocked = true

				break
			}
		}

		if blocked {
			break
		}

		runtime.Gosched()
	}

	if !blocked {
		release()
		t.Fatal("producer did not reach full writer queue")
	}

	read := make(chan error, 1)

	go func() {
		v, _, _, err := e.GetSnapshot("resident")
		if err == nil && v == nil {
			err = fmt.Errorf("resident key missing")
		}

		read <- err
	}()

	select {
	case err := <-read:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(time.Second):
		release()

		if err := <-read; err != nil {
			t.Error(err)
		}

		t.Error("writer backpressure blocked unrelated resident read")
	}

	release()

	if err := <-produced; err != nil {
		t.Fatal(err)
	}

	if err := e.graphs.store.Barrier(); err != nil {
		t.Fatal(err)
	}
}

func TestDiskConcurrentPublishRetainsLatestFrontier(t *testing.T) {
	e := tierEngine(t, t.TempDir())
	durableSet(t, e, "shared", "initial")

	var wg sync.WaitGroup
	for worker := range 4 {
		wg.Go(func() {
			for i := range 20 {
				c := e.NewContext()
				if err := c.Emit(scalarSet("shared", fmt.Sprintf("%d/%d", worker, i))); err != nil {
					t.Error(err)

					return
				}

				if err := c.Flush(); err != nil {
					t.Error(err)

					return
				}
			}
		})
	}

	wg.Wait()

	if err := e.graphs.store.Barrier(); err != nil {
		t.Fatal(err)
	}

	graph, err := e.graphs.store.Read("shared")
	if err != nil {
		t.Fatal(err)
	}

	if !tipsEqual(graph.Frontier, e.index.Contains("shared").Tips()) {
		t.Fatalf("disk frontier %v differs from live frontier %v", graph.Frontier, e.index.Contains("shared").Tips())
	}

	before := durableValue(t, e, "shared")
	demoteTest(t, e, "shared")

	if after := durableValue(t, e, "shared"); after != before {
		t.Fatalf("rehydration changed value: %q -> %q", before, after)
	}
}

func TestDiskQueuedReclamationDoesNotDeleteRehydratedVertex(t *testing.T) {
	e := tierEngine(t, t.TempDir())
	durableSet(t, e, "x", "retained")

	tips := e.index.Contains("x")
	if !e.graphs.evictCandidate("x", tips, nil) {
		t.Fatal("failed to demote")
	}

	entered, resume := make(chan struct{}), make(chan struct{})

	var once sync.Once

	release := func() { once.Do(func() { close(resume) }) }
	defer release()

	if err := e.graphs.store.Update(0, func() []graphstore.Mutation {
		close(entered)
		<-resume

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	<-entered
	e.reclaimUnreferenced()

	if err := e.hydrateDisk("x"); err != nil {
		t.Fatal(err)
	}

	release()

	if err := e.graphs.store.Barrier(); err != nil {
		t.Fatal(err)
	}

	graph, err := e.graphs.store.Read("x")
	if err != nil {
		t.Fatal(err)
	}

	for _, tip := range tips.Tips() {
		found := false

		for _, node := range graph.Nodes {
			if node.ID == tip {
				found = true

				break
			}
		}

		if !found {
			t.Fatalf("queued reclamation deleted rehydrated tip %v", tip)
		}
	}
}

func TestDiskObsoletePublicationDoesNotRecreateDroppedKey(t *testing.T) {
	e := tierEngine(t, t.TempDir())
	durableSet(t, e, "x", "retained")

	tips := e.index.Contains("x")
	if !e.graphs.evictCandidate("x", tips, nil) {
		t.Fatal("failed to demote")
	}

	entered, resume := make(chan struct{}), make(chan struct{})

	var once sync.Once

	release := func() { once.Do(func() { close(resume) }) }
	defer release()

	if err := e.graphs.store.Update(0, func() []graphstore.Mutation {
		close(entered)
		<-resume

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	<-entered

	if err := e.graphs.queueDrop("x"); err != nil {
		t.Fatal(err)
	}

	if err := e.graphs.publish("x"); err != nil {
		t.Fatal(err)
	}

	if err := e.graphs.forgetResident("x", tips, false); err != nil {
		t.Fatal(err)
	}

	release()

	if err := e.graphs.store.Barrier(); err != nil {
		t.Fatal(err)
	}

	if e.graphs.store.Has("x") {
		t.Fatal("obsolete publication recreated deleted package")
	}

	e.graphs.mu.RLock()
	_, tracked := e.graphs.keys["x"]
	e.graphs.mu.RUnlock()

	if tracked {
		t.Fatal("obsolete publication recreated resident metadata")
	}
}
