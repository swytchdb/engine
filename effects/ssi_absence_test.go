package effects

import (
	"errors"
	"slices"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

func TestSSIAbsentReadRecordsDependency(t *testing.T) {
	for _, createdBeforeRead := range []bool{false, true} {
		name := "never-created"
		if createdBeforeRead {
			name = "created-after-snapshot"
		}

		t.Run(name, func(t *testing.T) {
			e := newTxnTestEngine(nil)
			tx := e.NewContext()
			tx.BeginTx()

			write := func() {
				historicalTestWrite(t, e, map[string]string{"x": "concurrent"}, false)
			}
			if createdBeforeRead {
				write()
			}

			result, tips, err := tx.GetSnapshot("x")
			if err != nil || result != nil || len(tips) != 0 {
				t.Fatalf("snapshot must preserve absence: result=%v tips=%v err=%v", result, tips, err)
			}

			ck := tx.keys["x"]
			if ck == nil || !ck.readOnly {
				t.Fatalf("absent read must record a read-only dependency, got %+v", ck)
			}

			noop, err := e.getEffect("x", ck.lastOffset)
			if err != nil || noop.GetNoop() == nil || noop.GetTxnId() != tx.txnID {
				t.Fatalf("expected transactional NOOP: effect=%v err=%v", noop, err)
			}

			if !createdBeforeRead && len(noop.GetDeps()) == 0 {
				t.Fatal("absence read orphaned the subscription established for this key")
			}

			for _, dep := range noop.GetDeps() {
				parent, err := e.getEffect("x", r(dep))
				if err != nil || parent.GetSubscription() == nil {
					t.Fatalf("absence read adopted newer data: parent=%v err=%v", parent, err)
				}
			}

			if !createdBeforeRead {
				write()
			}

			// Recording a dependency must not make a later write visible or
			// turn the second read into a live-index read.
			for range 2 {
				result, _, err = tx.GetSnapshot("x")
				if err != nil || result != nil {
					t.Fatalf("repeat read escaped snapshot: result=%v err=%v", result, err)
				}
			}

			if err := tx.Emit(scalarSet("unrelated", "must not commit")); err != nil {
				t.Fatal(err)
			}

			if err := tx.Flush(); !errors.Is(err, ErrTxnAborted) {
				t.Fatalf("concurrent creation must invalidate the absence read, got %v", err)
			}

			result, _, _, err = e.GetSnapshot("unrelated")
			if err != nil || result != nil {
				t.Fatalf("aborted transaction leaked a write: result=%v err=%v", result, err)
			}
		})
	}
}

func TestSSIAbsentReadIncludedInBind(t *testing.T) {
	e := newTxnTestEngine(nil)

	var boundKeys []string

	e.SetOnLocalEffect(func(_ Tip, eff *pb.Effect) {
		if bind := eff.GetTxnBind(); bind != nil {
			for _, kb := range bind.GetKeys() {
				boundKeys = append(boundKeys, string(kb.GetKey()))
			}
		}
	})

	tx := e.NewContext()
	tx.BeginTx()

	if result, _, err := tx.GetSnapshot("absent"); err != nil || result != nil {
		t.Fatalf("expected absence: result=%v err=%v", result, err)
	}

	if err := tx.Emit(scalarSet("written", "value")); err != nil {
		t.Fatal(err)
	}

	if err := tx.Flush(); err != nil {
		t.Fatal(err)
	}

	if !slices.Equal(boundKeys, []string{"absent", "written"}) {
		t.Fatalf("bind dropped the read-only key: %v", boundKeys)
	}

	if result, _, _, err := e.GetSnapshot("absent"); err != nil || result != nil {
		t.Fatalf("read marker must not create a user value: result=%v err=%v", result, err)
	}
}

func TestSSIWriteSkewOnAbsentKeys(t *testing.T) {
	for _, initial := range []string{"absent", "existing", "deleted"} {
		for _, first := range []string{"a", "b"} {
			t.Run(initial+"/commit-"+first+"-first", func(t *testing.T) {
				e := newTxnTestEngine(nil)
				if initial != "absent" {
					historicalTestWrite(t, e, map[string]string{"x": "initial", "y": "initial"}, false)
				}

				if initial == "deleted" {
					c := e.NewContext()

					for _, key := range []string{"x", "y"} {
						eff := scalarSet(key, "")

						eff.GetData().Op = pb.EffectOp_REMOVE_OP
						if err := c.Emit(eff); err != nil {
							t.Fatal(err)
						}
					}

					if err := c.Flush(); err != nil {
						t.Fatal(err)
					}
				}

				a, b := e.NewContext(), e.NewContext()
				a.BeginTx()
				b.BeginTx()

				for _, read := range []struct {
					ctx *Context
					key string
				}{{a, "x"}, {b, "y"}} {
					result, _, err := read.ctx.GetSnapshot(read.key)
					if err != nil || (result == nil) != (initial != "existing") {
						t.Fatalf("unexpected initial read: result=%v err=%v", result, err)
					}
				}

				if err := a.Emit(scalarSet("y", "A")); err != nil {
					t.Fatal(err)
				}

				if err := b.Emit(scalarSet("x", "B")); err != nil {
					t.Fatal(err)
				}

				winner, loser := a, b
				if first == "b" {
					winner, loser = b, a
				}

				if err := winner.Flush(); err != nil {
					t.Fatalf("first transaction should commit: %v", err)
				}

				if err := loser.Flush(); !errors.Is(err, ErrTxnAborted) {
					t.Fatalf("write skew must abort the second transaction, got %v", err)
				}
			})
		}
	}
}
