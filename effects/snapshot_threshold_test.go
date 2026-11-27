package effects

import "testing"

// Snapshot creation is driven by new history, independently of how quickly
// writes arrive. Repeated reads of the resulting snapshot need no new snapshot.
func TestStateSnapshotsFollowHistoryCount(t *testing.T) {
	e := newTestEngine(nil)
	defer func() { _ = e.Close() }()

	preSubscribe(e, "a", "b")

	for _, key := range []string{"a", "a", "b", "a"} {
		for range 8 {
			historicalTestWrite(t, e, map[string]string{key: "value"}, false)
		}

		c := e.NewContext()

		got, _, err := c.GetSnapshot(key)
		if err != nil || string(got.GetScalar().GetRaw()) != "value" {
			t.Fatalf("read: value=%v err=%v", got, err)
		}

		ck := c.keys[logicalAddress(key)]
		if ck == nil {
			t.Fatalf("key %q: accumulated history did not create a snapshot", key)
		}

		eff, ok := e.effectCache.Get(ck.lastOffset)
		if !ok || eff.GetSnapshot().GetState() == nil {
			t.Fatal("expected a state snapshot")
		}

		if err := c.Flush(); err != nil {
			t.Fatal(err)
		}

		for range 3 {
			if _, _, err := c.GetSnapshot(key); err != nil {
				t.Fatal(err)
			}

			if len(c.keys) != 0 {
				t.Fatal("unchanged state created another snapshot")
			}
		}

		c.Abort()
	}
}
