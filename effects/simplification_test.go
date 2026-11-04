package effects

import (
	"context"
	"sync"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

func TestPendingTxnConcurrentDecision(t *testing.T) {
	for range 100 {
		txn := &pendingTxn{done: make(chan struct{})}
		start := make(chan struct{})
		winners := make(chan uint32, 3)

		var wg sync.WaitGroup
		for _, state := range []uint32{txnStateCommitted, txnStateAborted, txnStateAmbiguous} {
			wg.Go(func() {
				<-start

				if txn.finish(state) {
					winners <- state
				}
			})
		}

		close(start)
		wg.Wait()

		if len(winners) != 1 {
			t.Fatalf("got %d terminal decisions", len(winners))
		}

		select {
		case <-txn.done:
			if got, want := txn.state.Load(), <-winners; got != want {
				t.Fatalf("published state %d differs from winning decision %d", got, want)
			}
		default:
			t.Fatal("terminal decision did not wake waiters")
		}
	}
}

func TestSnapshotReadViewCapturedAbsence(t *testing.T) {
	e := newSnapshotEngine(newSnapshotLog())

	v := newSnapshotReadView(e, context.Background())
	if _, err := v.capture("absent"); err != nil {
		t.Fatal(err)
	}

	e.updateIndex("absent", nil, Tip{1, 1})

	tips, err := v.capture("absent")
	if err != nil || len(tips) != 0 {
		t.Fatalf("captured absence changed: %v, %v", tips, err)
	}

	if v.valid() {
		t.Fatal("a newly created key did not invalidate captured absence")
	}
}

func TestNackBindDecision(t *testing.T) {
	for _, tt := range []struct {
		name     string
		txn      string
		hash     byte
		conflict bool
		defeated bool
	}{
		{name: "we win", txn: "theirs", hash: 3, defeated: true},
		{name: "they win", txn: "theirs", hash: 1, conflict: true},
		{name: "equal hashes", txn: "theirs", hash: 2},
		{name: "own bind", txn: "ours", hash: 1},
		{name: "unknown transaction", hash: 3},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e := newTxnTestEngine(nil)
			tip := Tip{2, 1}

			eff := &pb.Effect{TxnId: tt.txn, ForkChoiceHash: []byte{tt.hash}, Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{}}}
			if err := e.effectCache.Put(tip, eff); err != nil {
				t.Fatal(err)
			}

			ours := &pendingTxn{txnID: "ours", forkChoiceHash: []byte{2}, bindOffset: Tip{1, 1}}
			detail := &pb.NackTipDetail{Ref: toPbRef(tip), IsBind: true, BindForkChoiceHash: []byte{tt.hash}}

			conflict, defeated := e.checkNackConflict(ours, "key", detail)
			if conflict != tt.conflict || (len(defeated) == 1 && defeated[tt.txn] == tip) != tt.defeated {
				t.Fatalf("conflict=%t, defeated=%v", conflict, defeated)
			}
		})
	}
}
