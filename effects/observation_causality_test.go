package effects

import (
	"errors"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

func ingestObservation(t *testing.T, e *Engine, tip Tip, deps []Tip) {
	t.Helper()

	eff := &pb.Effect{
		Key: []byte("k"), NodeId: tip[0], Hlc: sTs(int64(tip[1])),
		ForkChoiceHash: ComputeForkChoiceHash(pb.NodeID(tip[0]), sTs(int64(tip[1]))),
		TxnId:          "remote-reader", Deps: toPbRefs(deps),
		Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
	}

	data, err := MarshalEffect(eff)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := e.HandleRemote(BuildOffsetNotify(pb.NodeID(tip[0]), tip, eff, data, nil)); err != nil {
		t.Fatal(err)
	}
}

func TestObservationsRemainCausalDependencies(t *testing.T) {
	for _, kind := range []string{"noop", "snapshot", "write", "verdict"} {
		t.Run(kind, func(t *testing.T) {
			e := newTxnTestEngine(nil)

			t.Cleanup(func() { _ = e.Close() })
			preSubscribe(e, "k")
			historicalTestWrite(t, e, map[string]string{"k": "value"}, false)

			observation := Tip{7, 100}
			ingestObservation(t, e, observation, e.index.Contains("k").Tips())

			var emitted *pb.Effect

			e.SetOnLocalEffect(func(_ Tip, eff *pb.Effect) { emitted = eff })

			c := e.NewContext()
			defer c.Abort()

			var err error

			switch kind {
			case "noop":
				err = c.Emit(&pb.Effect{Key: []byte("k"), TxnId: "next-reader", Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}}})
			case "snapshot":
				value, _, _, readErr := e.GetSnapshot("k")
				if readErr != nil {
					t.Fatal(readErr)
				}

				err = c.Emit(&pb.Effect{Key: []byte("k"), Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{State: value}}})
			case "write":
				c.BeginTx()
				err = c.Emit(scalarSet("k", "next"))
			case "verdict":
				err = e.emitSnapshot("k", nil)
			}

			if err != nil {
				t.Fatal(err)
			}

			if emitted == nil || !refsContain(emitted.GetDeps(), observation) {
				t.Fatalf("%s bypassed observed NOOP: %v", kind, emitted)
			}
		})
	}
}

func TestObservationDoesNotInvalidateTransaction(t *testing.T) {
	for _, watched := range []bool{false, true} {
		t.Run(map[bool]string{false: "read", true: "watch"}[watched], func(t *testing.T) {
			e := newTxnTestEngine(nil)

			t.Cleanup(func() { _ = e.Close() })
			preSubscribe(e, "k")
			historicalTestWrite(t, e, map[string]string{"k": "value"}, false)

			c := e.NewContext()
			defer c.Abort()

			if watched {
				if err := c.Watch("k"); err != nil {
					t.Fatal(err)
				}
			} else {
				c.BeginTx()

				if _, _, err := c.GetSnapshot("k"); err != nil {
					t.Fatal(err)
				}
			}

			ingestObservation(t, e, Tip{7, 100}, e.index.Contains("k").Tips())

			if watched {
				c.BeginTx()

				if !c.CheckWatches() {
					t.Fatal("another observation invalidated WATCH")
				}
			}

			if err := c.Emit(scalarSet("other", "committed")); err != nil {
				t.Fatal(err)
			}

			if err := c.Flush(); err != nil {
				t.Fatalf("observation prevented commit: %v", err)
			}
		})
	}
}

func TestCompactionConsumesRemoteObservations(t *testing.T) {
	e := newTxnTestEngine(nil)

	t.Cleanup(func() { _ = e.Close() })
	preSubscribe(e, "k")
	historicalTestWrite(t, e, map[string]string{"k": "value"}, false)

	first := Tip{7, 100}

	for i := range 12 {
		ingestObservation(t, e, Tip{7, uint64(100 + i)}, e.index.Contains("k").Tips())
	}

	c := e.NewContext()
	defer c.Abort()

	if _, _, err := c.GetSnapshot("k"); err != nil {
		t.Fatal(err)
	}

	if c.keys["k"] == nil {
		t.Fatal("remote observation history did not trigger compaction")
	}

	snapshot := c.keys["k"].lastOffset
	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
	// Reconciliation must recognize the old observation as consumed, rather
	// than restoring it as a competing branch beside the snapshot.
	e.installIngestedTips("k", first)

	tips := e.index.Contains("k").Tips()
	if len(tips) != 1 || tips[0] != snapshot {
		t.Fatalf("compacted observations returned to the frontier: %v", tips)
	}

	value, _, walked, err := e.GetSnapshot("k")
	if err != nil || string(value.GetScalar().GetRaw()) != "value" || walked != 1 {
		t.Fatalf("compaction did not bound reconstruction: value=%v walked=%d err=%v", value, walked, err)
	}
}

func TestWriteBehindObservationInvalidatesTransaction(t *testing.T) {
	e := newTxnTestEngine(nil)

	t.Cleanup(func() { _ = e.Close() })
	preSubscribe(e, "k")
	historicalTestWrite(t, e, map[string]string{"k": "before"}, false)

	c := e.NewContext()
	defer c.Abort()

	c.BeginTx()

	if _, _, err := c.GetSnapshot("k"); err != nil {
		t.Fatal(err)
	}

	historicalTestWrite(t, e, map[string]string{"k": "after"}, false)
	ingestObservation(t, e, Tip{7, 100}, e.index.Contains("k").Tips())

	if err := c.Emit(scalarSet("other", "must abort")); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); !errors.Is(err, ErrTxnAborted) {
		t.Fatalf("write hidden by NOOP did not abort: %v", err)
	}
}

func TestObservingNoopDoesNotOrderBinds(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)
	base := log.putEffect(scalarSet("k", "base"))
	observation := log.putEffect(&pb.Effect{
		Key: []byte("k"), TxnId: "reader", Deps: toPbRefs([]Tip{base}),
		Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
	})
	write := scalarSet("k", "writer")
	write.TxnId, write.Deps = "writer", toPbRefs([]Tip{observation})
	writerTip := log.putEffect(write)
	makeBind := func(txn string, tip Tip, consumed Tip, hash byte) (*pb.Effect, Tip) {
		eff := &pb.Effect{
			Key: []byte("k"), TxnId: txn, Deps: toPbRefs([]Tip{tip}), ForkChoiceHash: []byte{hash},
			Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{Keys: []*pb.TransactionalBindEffect_KeyBind{
				{Key: []byte("k"), NewTip: toPbRef(tip), ConsumedTips: toPbRefs([]Tip{consumed})},
			}}},
		}

		return eff, log.putEffect(eff)
	}
	reader, readerBind := makeBind("reader", observation, base, 2)
	writer, writerBind := makeBind("writer", writerTip, observation, 1)

	e.updateIndex("k", nil, readerBind)
	e.updateIndex("k", nil, writerBind)

	if got := e.checkCompetingBinds(writer.GetTxnBind(), "writer"); got != "reader" {
		t.Errorf("observing reader's NOOP excused its competing bind: %q", got)
	}

	ptxn := &pendingTxn{txnID: "reader", bindOffset: readerBind, forkChoiceHash: reader.GetForkChoiceHash(), keys: []pendingTxnKey{{key: "k", newTip: observation, readOnly: true}}}
	detail := nackTipDetail(writerBind, writer)

	detail.BindForkChoiceHash = writer.GetForkChoiceHash()
	if !e.isRealConflict(ptxn, "k", detail) {
		t.Error("writer observed our NOOP, not our bind; its winning bind must conflict")
	}

	losers, _ := e.losersOnKeyAt("k", []Tip{readerBind, writerBind}, nil, nil, nil, false)
	if _, lost := losers["reader"]; !lost {
		t.Errorf("reconstruction treated observation as committed ordering: losers=%v", losers)
	}
}

func TestBindAncestryIsKeyScoped(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)
	first := closureBind(log, "first", 1, []string{"k", "other"}, nil)
	second := closureBind(log, "second", 2, []string{"k", "other"}, map[string]Tip{"other": first})

	ancestry := newCausalIndex(e, "k")
	if ancestry.reaches(second, first) {
		t.Fatal("observing a bind on another key ordered concurrent branches on k")
	}

	if !newCausalIndex(e, "other").reaches(second, first) {
		t.Fatal("lost the bind dependency on the key where it was observed")
	}
}
