package effects

import (
	"fmt"
	"testing"
	"time"
)

// A writer updating another graph's membership must not stop resident reads.
func TestDiskResidentReadBypassesMetadataLock(t *testing.T) {
	e := tierEngine(t, t.TempDir())
	durableSet(t, e, "resident", "available")

	if got := durableValue(t, e, "resident"); got != "available" {
		t.Fatal(got)
	}

	if err := e.hydrateDisk("resident"); err != nil {
		t.Fatal(err)
	}

	e.graphs.mu.Lock()
	done := make(chan error, 1)

	go func() {
		for range 10 {
			value, _, _, err := e.GetSnapshot("resident")
			if err != nil {
				done <- err

				return
			}

			if value == nil {
				done <- fmt.Errorf("resident value disappeared")

				return
			}
		}

		done <- nil
	}()

	select {
	case err := <-done:
		e.graphs.mu.Unlock()

		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		e.graphs.mu.Unlock()
		t.Fatal("resident read waited for unrelated graph metadata")
	}
}

func TestDiskResidentReadyDemotionAndRecreation(t *testing.T) {
	e := tierEngine(t, t.TempDir())
	durableSet(t, e, "x", "retained")

	_ = durableValue(t, e, "x")
	if err := e.hydrateDisk("x"); err != nil {
		t.Fatal(err)
	}

	leaf := e.index.LoadData("x")
	if leaf == nil || !leaf.diskResidentReady.Load() {
		t.Fatal("resident graph was not certified")
	}

	demoteTest(t, e, "x")

	if leaf.diskResidentReady.Load() {
		t.Fatal("demotion retained the ready certificate")
	}

	before := e.DiskStats().Hydrations
	if got := durableValue(t, e, "x"); got != "retained" {
		t.Fatal(got)
	}

	if !leaf.diskResidentReady.Load() || e.DiskStats().Hydrations != before+1 {
		t.Fatal("cold read bypassed hydration or failed to recertify")
	}

	demoteTest(t, e, "x")

	if !e.graphs.evictCandidate("x", e.index.Contains("x"), nil) {
		t.Fatal("failed to forget cold leaf")
	}

	if err := e.hydrateDisk("x"); err != nil {
		t.Fatal(err)
	}

	newLeaf, _ := e.index.LoadOrStoreData("x", &leafState{})
	if newLeaf == leaf || newLeaf.diskResidentReady.Load() {
		t.Fatal("recreated leaf inherited a previous certificate")
	}

	if got := durableValue(t, e, "x"); got != "retained" {
		t.Fatal(got)
	}
}

// Measure the normal resident hydration check with concurrent callers; writer
// and persistence overhead are outside this benchmark's timed region.
func BenchmarkDiskResidentHydration(b *testing.B) {
	e := tierEngine(b, b.TempDir())
	durableSet(b, e, "resident", "available")

	_ = durableValue(b, e, "resident")
	if err := e.graphs.store.Barrier(); err != nil {
		b.Fatal(err)
	}

	if err := e.hydrateDisk("resident"); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := e.hydrateDisk("resident"); err != nil {
				b.Error(err)

				return
			}
		}
	})
}
