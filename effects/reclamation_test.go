package effects

import (
	"fmt"
	"testing"
	"testing/synctest"
	"time"
)

// Tests that drain reclamation themselves must own its single-consumer queue.
func stopMemoryGovernorForTest(e *Engine) {
	close(e.memGovStop)
	e.memGovWg.Wait()
	e.memGovStop = nil
}

func TestUnboundedEngineReclaimsCompactedHistory(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := NewEngine(EngineConfig{DefaultMode: UnsafeMode})
		defer func() { _ = e.Close() }()

		for i := range 40 {
			c := e.NewContext()
			c.BeginTx()

			if _, _, err := c.GetSnapshot("key"); err != nil {
				t.Fatal(err)
			}

			if err := c.Emit(scalarSet("key", fmt.Sprint(i))); err != nil {
				t.Fatal(err)
			}

			if err := c.Flush(); err != nil {
				t.Fatal(err)
			}
		}

		before := e.VertexCount()

		time.Sleep(2 * time.Second)
		synctest.Wait()

		if e.effectCache.Reclaimed() == 0 || e.VertexCount() >= before {
			t.Fatalf("history was not physically reclaimed: before=%d after=%d reclaimed=%d", before, e.VertexCount(), e.effectCache.Reclaimed())
		}

		if e.effectCache.ColdEvictions() != 0 || e.memPressure.Load() != nil {
			t.Fatal("unbounded engine evicted keys or applied memory pressure")
		}

		got, _, _, err := e.GetSnapshot("key")
		if err != nil || string(got.GetScalar().GetRaw()) != "39" {
			t.Fatalf("value after reclamation: %v, %v", got, err)
		}
	})
}
