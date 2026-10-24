package effects

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"testing"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/durability"
)

func durableTestEngine(t testing.TB, dir string) *Engine {
	t.Helper()

	e, err := OpenEngine(context.Background(), EngineConfig{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})

	return e
}

func durableSet(t testing.TB, e *Engine, key, value string) {
	t.Helper()

	c := e.NewContext()
	if err := c.Emit(scalarSet(key, value)); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
}

func durableValue(t testing.TB, e *Engine, key string) string {
	t.Helper()

	s, _, _, err := e.GetSnapshot(key)
	if err != nil {
		t.Fatal(err)
	}

	return string(s.GetScalar().GetRaw())
}

func TestDurabilityRestoreChainAndTransaction(t *testing.T) {
	dir := t.TempDir()

	e := durableTestEngine(t, dir)
	for i := range 200 {
		durableSet(t, e, "winner", fmt.Sprint(i))
	}

	c := e.NewContext()
	c.BeginTx()

	if _, _, err := c.GetSnapshot("winner"); err != nil {
		t.Fatal(err)
	}

	if err := c.Emit(scalarSet("winner", "alice")); err != nil {
		t.Fatal(err)
	}

	if err := c.Emit(scalarSet("payout:alice", "100")); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	oldID := e.NodeID()
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e = durableTestEngine(t, dir)
	if e.NodeID() == oldID {
		t.Fatal("restart reused incarnation")
	}

	for key, want := range map[string]string{"winner": "alice", "payout:alice": "100"} {
		if got := durableValue(t, e, key); got != want {
			t.Fatalf("%s=%q, want %q", key, got, want)
		}
	}

	durableSet(t, e, "winner", "bob")

	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e = durableTestEngine(t, dir)
	if got := durableValue(t, e, "winner"); got != "bob" {
		t.Fatal(got)
	}
}

func TestDurabilityRecoveryOrderIndependentFrontier(t *testing.T) {
	dir := t.TempDir()

	s, err := durability.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	// Child deliberately stored before parent, with a competing branch and a
	// transaction envelope whose canonical key differs from its second key.
	parent := Tip{3, 64}
	left := Tip{3, 128}
	right := Tip{4, 1}
	bindID := Tip{5, 2}

	records := []struct {
		id  Tip
		eff *pb.Effect
	}{
		{left, &pb.Effect{Key: []byte("x"), Deps: toPbRefs([]Tip{parent})}},
		{bindID, &pb.Effect{Key: []byte("x"), Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{Keys: []*pb.TransactionalBindEffect_KeyBind{
			{Key: []byte("x"), NewTip: toPbRefs([]Tip{left})[0], ConsumedTips: toPbRefs([]Tip{parent})},
			{Key: []byte("y"), NewTip: toPbRefs([]Tip{right})[0]},
		}}}}},
		{right, &pb.Effect{Key: []byte("y")}},
		{parent, &pb.Effect{Key: []byte("x")}},
	}
	for _, r := range records {
		data, err := MarshalEffect(r.eff)
		if err != nil {
			t.Fatal(err)
		}

		if err := s.Put(r.id, data); err != nil {
			t.Fatal(err)
		}
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	e := durableTestEngine(t, dir)
	for _, key := range []string{"x", "y"} {
		tips := e.index.Contains(key)
		if tips.Len() != 1 || !tips.Contains(bindID) {
			t.Fatalf("%s frontier=%v", key, tips)
		}
	}

	if e.VertexCount() != 4 {
		t.Fatalf("effects=%d", e.VertexCount())
	}
}

func TestDurabilityEvictionRemovesDiskRecord(t *testing.T) {
	dir := t.TempDir()
	e := durableTestEngine(t, dir)
	durableSet(t, e, "evict-me", "value")
	tips := e.index.Contains("evict-me")
	e.releaseChainRefs("evict-me", tips.Tips(), nil)
	e.index.Delete("evict-me", tips)
	e.reclaimUnreferenced()

	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e = durableTestEngine(t, dir)
	if e.index.Contains("evict-me").Len() != 0 {
		t.Fatal("evicted key recovered")
	}
}

func TestDurabilityLowMemoryFallback(t *testing.T) {
	previous := debug.SetMemoryLimit(1 << 30)
	defer debug.SetMemoryLimit(previous)

	for _, cancelled := range []bool{true, false} {
		t.Run(fmt.Sprint(cancelled), func(t *testing.T) {
			dir := t.TempDir()
			e := durableTestEngine(t, dir)
			durableSet(t, e, "saved", "value")

			if err := e.Close(); err != nil {
				t.Fatal(err)
			}

			e = newEngine(EngineConfig{})
			// Tiny target deterministically exercises the governor's recovery
			// cancellation without setting the process-wide GOMEMLIMIT to one byte.
			e.memTarget = 1

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			if cancelled {
				time.AfterFunc(20*time.Millisecond, cancel)
			}

			err := e.openDurability(ctx, dir, 100*time.Millisecond)
			if cancelled && !errors.Is(err, context.Canceled) {
				t.Fatalf("err=%v", err)
			}

			if !cancelled && err != nil {
				t.Fatal(err)
			}

			if e.VertexCount() != 0 || e.KeyCount() != 0 {
				t.Fatal("partial recovery retained")
			}

			if err := e.Close(); err != nil {
				t.Fatal(err)
			}

			e = durableTestEngine(t, dir)
			if cancelled {
				if got := durableValue(t, e, "saved"); got != "value" {
					t.Fatal(got)
				}
			} else if e.KeyCount() != 0 {
				t.Fatal("discarded storage recovered")
			}
		})
	}
}

func TestDurabilityOpenFailureAndWriteFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(path, []byte("file"), 0600); err != nil {
		t.Fatal(err)
	}

	if e, err := OpenEngine(context.Background(), EngineConfig{DataDir: path}); err == nil {
		_ = e.Close()

		t.Fatal("bad path accepted")
	}

	e := durableTestEngine(t, t.TempDir())
	if err := e.effectCache.mirror.Close(); err != nil {
		t.Fatal(err)
	}

	c := e.NewContext()
	if err := c.Emit(scalarSet("x", "lost")); !errors.Is(err, durability.ErrClosed) {
		t.Fatalf("storage error hidden: %v", err)
	}

	if e.index.Contains("x").Len() != 0 {
		t.Fatal("failed write published")
	}
}

func TestDurabilityFlushDoesNotResurrect(t *testing.T) {
	dir := t.TempDir()
	e := durableTestEngine(t, dir)
	durableSet(t, e, "old", "value")

	if err := e.FlushIndex(); err != nil {
		t.Fatal(err)
	}

	durableSet(t, e, "new", "kept")

	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e = durableTestEngine(t, dir)
	if e.index.Contains("old").Len() != 0 {
		t.Fatal("flushed key recovered")
	}

	if durableValue(t, e, "new") != "kept" {
		t.Fatal("post-flush write lost")
	}
}

func TestDurabilityConflictingWonHistories(t *testing.T) {
	source, _, _ := conflictLottery(t, "alice", "bob")
	dir := t.TempDir()

	s, err := durability.Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	source.effectCache.m.Range(func(id Tip, v *vertex) bool {
		data, err := MarshalEffect(v.eff)
		if err != nil {
			t.Error(err)

			return false
		}

		if err := s.Put(id, data); err != nil {
			t.Error(err)

			return false
		}

		return true
	})

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	e := durableTestEngine(t, dir)
	for _, key := range []string{"winner", "payout:alice", "payout:bob"} {
		if got, want := durableValue(t, e, key), durableValue(t, source, key); got != want {
			t.Fatalf("%s: got %q want %q", key, got, want)
		}
	}

	for _, txn := range []string{"alice", "bob"} {
		conflictDocument(t, e, txn)
	}

	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e = durableTestEngine(t, dir)
	for _, txn := range []string{"alice", "bob"} {
		conflictDocument(t, e, txn)
	}
}

func BenchmarkDurabilityEngineRecovery(b *testing.B) {
	dir := b.TempDir()

	e := durableTestEngine(b, dir)
	for i := range 10000 {
		durableSet(b, e, fmt.Sprintf("key:%d", i%1000), fmt.Sprint(i))
	}

	if err := e.Close(); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for range b.N {
		e, err := OpenEngine(context.Background(), EngineConfig{DataDir: dir})
		if err != nil {
			b.Fatal(err)
		}

		if err := e.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func TestDurabilityReingestionWinsConcurrentReclaim(t *testing.T) {
	e := durableTestEngine(t, t.TempDir())
	p := e.effectCache

	for i := range 256 {
		tip := Tip{71, uint64(i) * 64}

		eff := scalarSet("race", "value")
		if err := p.PutSizedCache(tip, eff, 10); err != nil {
			t.Fatal(err)
		}

		v, _ := p.m.Load(tip)
		start := make(chan struct{})
		done := make(chan error, 2)

		go func() { <-start; p.claimAndDelete(tip, v); done <- nil }()
		go func() { <-start; done <- p.Put(tip, eff) }()

		close(start)

		for range 2 {
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		}

		v, ok := p.m.Load(tip)
		if !ok || v.refs.Load() < 1 {
			t.Fatalf("successful re-ingestion lost to reclaim: %v", tip)
		}
	}
}
