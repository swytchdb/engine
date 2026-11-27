package effects

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

func contradictoryVerdictNack(t *testing.T, e *Engine) *pb.NackNotify {
	t.Helper()

	nack := &pb.NackNotify{Key: []byte("evidence"), Conflicting: toPbRef(Tip{42, 100})}

	for i, outcome := range []pb.Verdict{pb.Verdict_WON, pb.Verdict_LOST} {
		owner := Tip{99, uint64(i + 1)}

		err := e.effectCache.Put(owner, &pb.Effect{Key: []byte("evidence"), Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
			TxnVerdicts: map[string]pb.Verdict{"settled": outcome}, OutstandingSubscribers: &pb.SubscriberSet{},
		}}})
		if err != nil {
			t.Fatal(err)
		}

		nack.TipDetails = append(nack.TipDetails, &pb.NackTipDetail{Ref: toPbRef(owner)})
	}

	return nack
}

func TestHandleNackReturnsIngestionError(t *testing.T) {
	for _, late := range []bool{false, true} {
		e := newSnapshotEngine(nil)

		nack := contradictoryVerdictNack(t, e)
		if late {
			e.pendingTxns.Store(r(nack.GetConflicting()), &pendingTxn{nackBatchClosed: true})
		}

		if err := e.HandleNack(nack); !errors.Is(err, ErrTxnAmbiguous) {
			t.Fatalf("late = %t: error = %v, want contradictory verdict error", late, err)
		}
	}
}

func TestFlushTxLogsDeferredNackErrorWithoutChangingCommit(t *testing.T) {
	e := newSnapshotEngine(nil)
	nack := contradictoryVerdictNack(t, e)

	var logs bytes.Buffer

	previous := slog.Default()

	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	var pending *pendingTxn

	e.SetOnLocalEffect(func(tip Tip, effect *pb.Effect) {
		if effect.GetTxnBind() != nil {
			nack.Conflicting = toPbRef(tip)
		}

		if effect.GetSnapshot() != nil && pending == nil {
			pending, _ = e.pendingTxns.Load(r(nack.GetConflicting()))
			if pending == nil {
				t.Fatal("snapshot published without a pending transaction")
			}

			if err := e.HandleNack(nack); err != nil {
				t.Fatalf("could not queue reply: %v", err)
			}
		}
	})
	defer e.SetOnLocalEffect(nil)

	c := e.NewContext()
	defer c.Abort()

	c.BeginTx()

	if err := c.Emit(scalarSet("k", "committed")); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		t.Fatalf("history recovery changed the commit result: %v", err)
	}

	if pending == nil || pending.state.Load() != txnStateCommitted {
		t.Fatal("transaction did not commit")
	}

	if !strings.Contains(logs.String(), "deferred NACK ingestion failed") || !strings.Contains(logs.String(), ErrTxnAmbiguous.Error()) {
		t.Fatalf("ingestion failure was not logged: %s", logs.String())
	}

	select {
	case <-pending.resolved:
	default:
		t.Fatal("ingestion failure prevented transaction cleanup")
	}
}
