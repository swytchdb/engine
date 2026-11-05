package effects

import (
	"context"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

func BenchmarkSnapshotBatch(b *testing.B) {
	e := newTxnTestEngine(nil)

	c := e.NewContext()
	if err := c.Emit(&pb.Effect{Key: []byte("key"), Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("value"))}}); err != nil {
		b.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()

	for b.Loop() {
		values, err := e.GetSnapshots(context.Background(), []string{"key", "key"})
		if err != nil || len(values) != 2 || string(values[0].GetScalar().GetRaw()) != "value" {
			b.Fatalf("snapshot batch: %v, %v", values, err)
		}
	}
}

func BenchmarkNackWinningBind(b *testing.B) {
	e := newTxnTestEngine(nil)
	tip := Tip{2, 1}

	eff := &pb.Effect{TxnId: "theirs", ForkChoiceHash: []byte{2}, Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{}}}
	if err := e.effectCache.Put(tip, eff); err != nil {
		b.Fatal(err)
	}

	ours := &pendingTxn{txnID: "ours", forkChoiceHash: []byte{1}, bindOffset: Tip{1, 1}, keys: []pendingTxnKey{{key: "key", newTip: Tip{1, 2}}}}
	detail := nackTipDetail(tip, eff)

	b.ReportAllocs()

	for b.Loop() {
		conflict, defeated := e.checkNackConflict(ours, "key", detail)
		if conflict || defeated["theirs"] != tip {
			b.Fatalf("NACK decision: conflict=%t, defeated=%v", conflict, defeated)
		}
	}
}
