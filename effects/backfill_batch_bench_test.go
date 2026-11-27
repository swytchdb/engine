package effects

import (
	"fmt"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

// backfillHistory supplies resident, valid multi-key transactions without
// installing their frontiers. Every bind consumes its predecessor on each key.
func backfillHistory(count int, keys []string) (*Engine, []Tip, map[Tip]*pb.Effect) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)
	preSubscribe(e, keys...)
	e.broadcaster = &mockBroadcaster{}
	binds := make([]Tip, 0, count)
	records := make(map[Tip]*pb.Effect)

	put := func(eff *pb.Effect) Tip {
		tip := log.putEffect(eff)
		records[tip], _ = e.effectCache.Get(tip)

		return tip
	}

	for i := range count {
		txn := fmt.Sprintf("txn-%d", i)

		var consumed []Tip
		if i != 0 {
			consumed = []Tip{binds[i-1]}
		}

		kb := make([]*pb.TransactionalBindEffect_KeyBind, 0, len(keys))

		deps := make([]Tip, 0, len(keys))
		for _, key := range keys {
			eff := scalarSet(key, txn)
			eff.TxnId, eff.Deps = txn, toPbRefs(consumed)
			tip := put(eff)
			deps = append(deps, tip)
			kb = append(kb, &pb.TransactionalBindEffect_KeyBind{
				Key: []byte(key), NewTip: toPbRef(tip), ConsumedTips: toPbRefs(consumed),
			})
		}

		binds = append(binds, put(&pb.Effect{
			Key: []byte(keys[0]), NodeId: 1, Hlc: sTs(int64(log.nextOff)), TxnId: txn, Deps: toPbRefs(deps),
			Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
				Keys: kb, OriginatorNodeId: 1, TxnHlc: sTs(int64(log.nextOff)),
			}},
		}))
	}

	return e, binds, records
}

func BenchmarkNackBackfillCrossKey(b *testing.B) {
	for _, count := range []int{16, 64} {
		for _, keyCount := range []int{2, 4} {
			b.Run(fmt.Sprintf("binds=%d/keys=%d", count, keyCount), func(b *testing.B) {
				keys := make([]string, keyCount)
				for i := range keys {
					keys[i] = fmt.Sprintf("key-%d", i)
				}

				e, binds, _ := backfillHistory(count, keys)

				b.Cleanup(func() { _ = e.Close() })

				nacks := []*pb.NackNotify{{Key: []byte(keys[0]), TipDetails: []*pb.NackTipDetail{{Ref: toPbRef(binds[count-1])}}}}
				// Measure repeated recovery of resident history, the case where
				// extra closure walks cannot be attributed to network fetches.
				_, _ = e.ingestNackBatch(nacks)

				b.ReportAllocs()
				b.ResetTimer()

				for range b.N {
					_, _ = e.ingestNackBatch(nacks)
				}
			})
		}
	}
}
