package effects

import (
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

func TestWatchCompactsObservationHistory(t *testing.T) {
	for _, readOnly := range []bool{false, true} {
		name := "normal-context"
		if readOnly {
			name = "read-only-context"
		}

		t.Run(name, func(t *testing.T) {
			e := newTestEngine(nil)

			t.Cleanup(func() { _ = e.Close() })
			preSubscribe(e, "key")
			historicalTestWrite(t, e, map[string]string{"key": "value"}, false)

			c := e.NewContext()
			if readOnly {
				c = e.NewReadOnlyContext()
			}
			defer c.Abort()

			var compacted bool

			for range 12 {
				if err := c.Watch("key"); err != nil {
					t.Fatal(err)
				}

				tip := c.watchedKeys["key"].noopOffset

				noop, ok := e.effectCache.Get(tip)
				if !ok || noop.GetNoop() == nil {
					t.Fatal("observation NOOP is missing")
				}

				for _, dep := range noop.GetDeps() {
					eff, ok := e.effectCache.Get(r(dep))
					if ok && eff.GetSnapshot().GetState() != nil {
						compacted = true
					}
				}

				got, _, _, err := e.GetSnapshot("key")
				if err != nil || string(got.GetScalar().GetRaw()) != "value" {
					t.Fatalf("read after observation: value=%v error=%v", got, err)
				}

				if compacted {
					c.BeginTx()

					if !c.CheckWatches() {
						t.Fatal("own compaction invalidated the observation")
					}

					if err := c.Flush(); err != nil {
						t.Fatal(err)
					}

					return
				}

				c.ClearWatches()
			}

			t.Fatal("observation history grew without producing a state snapshot")
		})
	}
}

func TestReconstructionCountsFilteredHistory(t *testing.T) {
	e := newTestEngine(nil)

	t.Cleanup(func() { _ = e.Close() })
	preSubscribe(e, "key")
	historicalTestWrite(t, e, map[string]string{"key": "value"}, false)

	_, _, before, err := e.GetSnapshot("key")
	if err != nil {
		t.Fatal(err)
	}

	c := e.NewContext()
	if err := c.Emit(&pb.Effect{Key: []byte("key"), TxnId: "observation", Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}}}); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	got, _, after, err := e.GetSnapshot("key")
	if err != nil || string(got.GetScalar().GetRaw()) != "value" {
		t.Fatalf("value=%v error=%v", got, err)
	}

	if after != before+1 {
		t.Fatalf("walked count before=%d after=%d: filtered NOOP was not counted", before, after)
	}
}

func TestNoopEmissionMakesContextWritable(t *testing.T) {
	e := newTestEngine(nil)

	t.Cleanup(func() { _ = e.Close() })
	preSubscribe(e, "key")
	historicalTestWrite(t, e, map[string]string{"key": "value"}, false)

	c := e.NewReadOnlyContext()
	defer c.Abort()

	for range 6 {
		if err := c.Emit(&pb.Effect{Key: []byte("key"), Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}}}); err != nil {
			t.Fatal(err)
		}

		if err := c.Flush(); err != nil {
			t.Fatal(err)
		}
	}

	if _, _, err := c.GetSnapshot("key"); err != nil {
		t.Fatal(err)
	}

	pending := c.keys["key"]
	if pending == nil {
		t.Fatal("context that emitted NOOPs still skips compaction")
	}

	eff, ok := e.effectCache.Get(pending.lastOffset)
	if !ok || eff.GetSnapshot().GetState() == nil {
		t.Fatal("compaction did not produce a state snapshot")
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
}
