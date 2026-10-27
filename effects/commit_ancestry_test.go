package effects

import (
	"errors"
	"sync"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

func commitAncestryFixture(t *testing.T) (*Engine, Tip, Tip, *pb.Effect) {
	t.Helper()

	e := newTxnTestEngine(nil)
	historicalTestWrite(t, e, map[string]string{"k": "P"}, false)
	p := e.index.Contains("k").Tips()[0]

	var (
		bindTip Tip
		bind    *pb.Effect
	)

	e.SetOnLocalEffect(func(tip Tip, eff *pb.Effect) {
		if eff.GetTxnBind() != nil {
			bindTip, bind = tip, eff
		}
	})
	historicalTestWrite(t, e, map[string]string{"a": "B", "k": "B"}, true)
	e.SetOnLocalEffect(nil)

	if bind == nil {
		t.Fatal("missing fixture bind")
	}

	// Record the same observation as remote bind ingestion, and retain P
	// alongside its descendant, as additional-key indexing can do.
	e.handleRemoteBind(bind.GetTxnBind(), bindTip, bind.GetTxnId())
	e.updateIndex("k", nil, p)

	return e, p, bindTip, bind
}

func TestFlushTxRedundantConsumedAncestor(t *testing.T) {
	for _, write := range []bool{false, true} {
		name := "read-only"
		if write {
			name = "read-write"
		}

		t.Run(name, func(t *testing.T) {
			e, p, _, _ := commitAncestryFixture(t)
			for range 3 {
				tx := e.NewContext()
				tx.BeginTx()

				result, _, err := tx.GetSnapshot("k")
				if err != nil || string(result.GetScalar().GetRaw()) != "B" {
					t.Fatalf("read: result=%v err=%v", result, err)
				}

				if !tx.keys["k"].initialTips.Contains(p) {
					t.Fatal("fixture lost the redundant ancestor before commit")
				}

				if write {
					if err := tx.Emit(scalarSet("other", "committed")); err != nil {
						t.Fatal(err)
					}
				}

				if err := tx.Flush(); err != nil {
					t.Fatalf("redundant ancestor prevented progress: %v", err)
				}

				e.updateIndex("k", nil, p)
			}
		})
	}
}

func TestCheckCompetingBindsAncestry(t *testing.T) {
	for _, dependency := range []string{"shared-parent", "bind", "descendant"} {
		t.Run(dependency, func(t *testing.T) {
			e, p, b, prior := commitAncestryFixture(t)
			consumed := []Tip{p}

			switch dependency {
			case "bind":
				consumed = append(consumed, b)
			case "descendant":
				historicalTestWrite(t, e, map[string]string{"k": "C"}, false)
				consumed = append(consumed, e.index.Contains("k").Tips()...)
			}

			candidate := &pb.TransactionalBindEffect{Keys: []*pb.TransactionalBindEffect_KeyBind{
				{Key: []byte("k"), ConsumedTips: toPbRefs(consumed)},
			}}

			want := ""
			if dependency == "shared-parent" {
				want = prior.GetTxnId()
			}

			if got := e.checkCompetingBinds(candidate, "candidate"); got != want {
				t.Fatalf("competing bind: got %q, want %q", got, want)
			}
		})
	}
}

func TestFlushTxRequiresEverySpokenBind(t *testing.T) {
	e, p, _, _ := commitAncestryFixture(t)
	// Another bind also consumed P, but is not an ancestor of our snapshot.
	// It may have been recorded before its index transition completed.
	other := Tip{99, 1}
	e.handleRemoteBind(&pb.TransactionalBindEffect{Keys: []*pb.TransactionalBindEffect_KeyBind{
		{Key: []byte("k"), ConsumedTips: []*pb.EffectRef{toPbRef(p)}, NewTip: toPbRef(Tip{99, 2})},
	}}, other, "other")

	tx := e.NewContext()
	tx.BeginTx()

	if _, _, err := tx.GetSnapshot("k"); err != nil {
		t.Fatal(err)
	}

	if err := tx.Emit(scalarSet("unrelated", "must not commit")); err != nil {
		t.Fatal(err)
	}

	if err := tx.Flush(); !errors.Is(err, ErrTxnAborted) {
		t.Fatalf("unproven consuming bind should abort, got %v", err)
	}

	if result, _, _, err := e.GetSnapshot("unrelated"); err != nil || result != nil {
		t.Fatalf("aborted transaction leaked writes: result=%v err=%v", result, err)
	}
}

func TestSpokenBindsRetainsConcurrentConsumers(t *testing.T) {
	e := newTxnTestEngine(nil)
	p := Tip{99, 1}
	bind := &pb.TransactionalBindEffect{Keys: []*pb.TransactionalBindEffect_KeyBind{
		{Key: []byte("k"), ConsumedTips: []*pb.EffectRef{toPbRef(p)}},
	}}
	e.handleRemoteBind(bind, Tip{99, 2}, "first")
	history, _ := e.spokenBinds.Get(p, 0)
	before := history.snapshot()

	var writers sync.WaitGroup
	for i := range 16 {
		writers.Go(func() {
			for range 2 { // Re-delivery must not grow the list.
				e.handleRemoteBind(bind, Tip{99, uint64(i + 3)}, "later")
			}
		})
	}

	writers.Wait()

	if got, _ := e.spokenBinds.Get(p, 0); len(got.snapshot()) != 17 {
		t.Fatalf("lost or duplicated a consuming bind: %v", got)
	}

	if len(before) != 1 || before[0] != (Tip{99, 2}) {
		t.Fatalf("mutated an already-published list: %v", before)
	}
}
