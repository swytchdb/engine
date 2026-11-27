package effects

import (
	"errors"
	"fmt"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

func TestTransactionReadsCreateStateSnapshots(t *testing.T) {
	for _, earlyRead := range []bool{false, true} {
		t.Run(fmt.Sprint(earlyRead), func(t *testing.T) {
			e := newTestEngine(nil)
			preSubscribe(e, "key")
			historicalTestWrite(t, e, map[string]string{"key": "old"}, false)
			older := e.NewContext()

			older.BeginTx()
			defer older.Abort()

			if earlyRead {
				if _, _, err := older.GetSnapshot("key"); err != nil {
					t.Fatal(err)
				}
			}

			snapshots := 0

			e.SetOnLocalEffect(func(_ Tip, eff *pb.Effect) {
				if eff.GetSnapshot().GetState() != nil {
					snapshots++

					if eff.GetTxnId() != "" {
						t.Error("maintenance snapshot belongs to the transaction")
					}
				}
			})

			for i := range 20 {
				tx := e.NewContext()
				tx.BeginTx()

				if _, _, err := tx.GetSnapshot("key"); err != nil {
					t.Fatal(err)
				}

				beforeFlush := snapshots

				if err := tx.Emit(scalarSet("key", fmt.Sprint(i))); err != nil {
					t.Fatal(err)
				}

				if err := tx.Flush(); err != nil {
					t.Fatal(err)
				}

				if snapshots != beforeFlush {
					t.Fatal("commit reconstructed another state snapshot")
				}
			}

			if snapshots < 2 {
				t.Fatalf("transaction-only writes emitted %d state snapshots", snapshots)
			}

			got, _, _, err := e.GetSnapshot("key")
			if err != nil || string(got.GetScalar().GetRaw()) != "19" {
				t.Fatalf("live value=%v err=%v", got, err)
			}

			e.reclaimUnreferenced()

			got, _, err = older.GetSnapshot("key")
			if err != nil || string(got.GetScalar().GetRaw()) != "old" {
				t.Fatalf("frozen value=%v err=%v", got, err)
			}
		})
	}
}

func TestTransactionBuildsOnReadSnapshot(t *testing.T) {
	for _, concurrentWrite := range []bool{false, true} {
		for _, abort := range []bool{false, true} {
			t.Run(fmt.Sprintf("concurrent=%t/abort=%t", concurrentWrite, abort), func(t *testing.T) {
				e := newTestEngine(nil)
				defer func() { _ = e.Close() }()

				preSubscribe(e, "key")

				for range 8 {
					historicalTestWrite(t, e, map[string]string{"key": "before"}, false)
				}

				oldFrontier := e.index.Contains("key").Tips()
				tx := e.NewContext()

				tx.BeginTx()
				defer tx.Abort()

				if concurrentWrite {
					historicalTestWrite(t, e, map[string]string{"key": "concurrent"}, false)
				}

				savepoint := tx.TakeSavepoint()

				value, frontier, err := tx.GetSnapshot("key")
				if err != nil || string(value.GetScalar().GetRaw()) != "before" {
					t.Fatalf("historical read: value=%v err=%v", value, err)
				}

				if len(frontier) != 1 {
					t.Fatalf("read frontier=%v, want one state snapshot", frontier)
				}

				if concurrentWrite {
					if !tipsEqual(frontier, oldFrontier) {
						t.Fatal("historical read published a snapshot beside newer live data")
					}
				} else {
					snapshot, err := e.getEffect(logicalAddress("key"), frontier[0])
					if err != nil || snapshot.GetTxnId() != "" || snapshot.GetSnapshot().GetState() == nil {
						t.Fatalf("read did not publish a committed state snapshot: %v, %v", snapshot, err)
					}

					if !e.index.Contains("key").Contains(frontier[0]) {
						t.Fatal("read snapshot was not published before the transaction")
					}
				}

				// Rollback removes the read marker, but the committed read snapshot
				// remains the transaction's equivalent frontier and needs no rebuild.
				tx.RestoreSavepoint(savepoint)

				value, again, err := tx.GetSnapshot("key")
				if err != nil || string(value.GetScalar().GetRaw()) != "before" || !tipsEqual(again, frontier) {
					t.Fatalf("read after rollback rebuilt or changed its frontier: %v, %v, %v", value, again, err)
				}

				pending := tx.keys[logicalAddress("key")]

				observation, err := e.getEffect(logicalAddress("key"), pending.lastOffset)
				if err != nil || !tipsEqual(pending.initialTips.Tips(), frontier) ||
					!tipsEqual(fromPbRefs(observation.GetDeps()), frontier) {
					t.Fatalf("transaction did not build on the read snapshot: %v, %v", observation, err)
				}

				if err := tx.Emit(scalarSet("key", "mine")); err != nil {
					t.Fatal(err)
				}

				value, _, err = tx.GetSnapshot("key")
				if err != nil || string(value.GetScalar().GetRaw()) != "mine" {
					t.Fatalf("read own write from snapshot: value=%v err=%v", value, err)
				}

				want := "mine"

				if abort {
					tx.Abort()

					want = "before"
				} else if err := tx.Flush(); concurrentWrite {
					if !errors.Is(err, ErrTxnAborted) {
						t.Fatalf("concurrent write was hidden by the snapshot: %v", err)
					}
				} else if err != nil {
					t.Fatal(err)
				}

				if concurrentWrite {
					want = "concurrent"
				}

				value, _, _, err = e.GetSnapshot("key")
				if err != nil || string(value.GetScalar().GetRaw()) != want {
					t.Fatalf("committed state: value=%v want=%q err=%v", value, want, err)
				}
			})
		}
	}
}
