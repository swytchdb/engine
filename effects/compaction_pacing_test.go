package effects

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCompactionPacing(t *testing.T) {
	var leaf leafState

	first := int64(time.Second)
	if !leaf.claimCompaction(first) {
		t.Fatal("first compaction was delayed")
	}

	if leaf.claimCompaction(first + int64(compactionInterval) - 1) {
		t.Fatal("compaction ran before the minimum interval")
	}

	if !leaf.claimCompaction(first + int64(compactionInterval)) {
		t.Fatal("compaction did not become eligible at the interval boundary")
	}

	if leaf.claimCompaction(first) {
		t.Fatal("an older clock sample claimed a compaction")
	}
}

func TestConcurrentReadersClaimOneCompaction(t *testing.T) {
	var (
		leaf    leafState
		winners atomic.Int32
		wg      sync.WaitGroup
	)
	for range 32 {
		wg.Go(func() {
			if leaf.claimCompaction(int64(time.Second)) {
				winners.Add(1)
			}
		})
	}

	wg.Wait()

	if got := winners.Load(); got != 1 {
		t.Fatalf("concurrent readers claimed %d compactions, want 1", got)
	}
}

func TestAutomaticCompactionPacingIsPerKey(t *testing.T) {
	e := newTestEngine(nil)
	defer func() { _ = e.Close() }()

	preSubscribe(e, "a", "b")

	for _, step := range []struct {
		key  string
		want bool
	}{{"a", true}, {"a", false}, {"b", true}} {
		for range 8 {
			historicalTestWrite(t, e, map[string]string{step.key: "value"}, false)
		}

		c := e.NewContext()

		got, _, err := c.GetSnapshot(step.key)
		if err != nil || string(got.GetScalar().GetRaw()) != "value" {
			c.Abort()
			t.Fatalf("read while pacing: value=%v error=%v", got, err)
		}

		if emitted := c.keys[step.key] != nil; emitted != step.want {
			c.Abort()
			t.Fatalf("key %q: snapshot emitted=%t, want %t", step.key, emitted, step.want)
		}

		if err := c.Flush(); err != nil {
			t.Fatal(err)
		}
	}
}
