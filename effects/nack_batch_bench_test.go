package effects

import (
	"fmt"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
)

func BenchmarkIngestRepeatedNacks(b *testing.B) {
	for _, peers := range []int{1, 4} {
		b.Run(fmt.Sprintf("peers=%d", peers), func(b *testing.B) {
			e := newSnapshotEngine(nil)
			preSubscribe(e, "k")
			e.broadcaster = &mockBroadcaster{}

			for i := uint64(1); i <= 128; i++ {
				eff := frontierEffect(i)
				if i > 1 {
					eff.Deps = toPbRefs([]Tip{{7, i - 1}})
				}

				if err := e.effectCache.Put(Tip{7, i}, eff); err != nil {
					b.Fatal(err)
				}
			}

			nacks := make([]*pb.NackNotify, peers)
			for i := range nacks {
				nacks[i] = &pb.NackNotify{Key: []byte("k"), TipDetails: []*pb.NackTipDetail{{Ref: toPbRef(Tip{7, 128})}}}
			}

			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				_, _ = e.ingestNackBatch(nacks)
			}
		})
	}
}

func BenchmarkEvaluateRepeatedNacks(b *testing.B) {
	for _, peers := range []int{1, 4} {
		b.Run(fmt.Sprintf("peers=%d", peers), func(b *testing.B) {
			log := newSnapshotLog()
			e := newSnapshotEngine(log)
			bind := closureBind(log, "competitor", 2, []string{"k"}, nil)

			root := bind
			for range 128 {
				root = log.putEffect(&pb.Effect{Key: []byte("k"), Deps: toPbRefs([]Tip{root}), Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}}})
			}

			ptxn := &pendingTxn{txnID: "local", forkChoiceHash: []byte{0}, keys: []pendingTxnKey{{key: logicalAddress("k")}}}
			rootEff, _ := e.effectCache.Get(root)

			nacks := make([]*pb.NackNotify, peers)
			for i := range nacks {
				nacks[i] = &pb.NackNotify{Key: []byte("k"), TipDetails: []*pb.NackTipDetail{nackTipDetail(root, rootEff)}}
			}

			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				defeated, err := e.evaluateNacks(ptxn, nacks)
				if err != nil || defeated["competitor"] != bind {
					b.Fatalf("evaluation: %v %v", defeated, err)
				}
			}
		})
	}
}

func BenchmarkHasWritesSinceSharedHistory(b *testing.B) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)
	tips := make([]Tip, 0, 512)

	for i := range 512 {
		eff := scalarSet("k", "old")
		if i > 0 {
			eff.Deps = toPbRefs([]Tip{tips[i-1]})
		}

		tips = append(tips, log.putEffect(eff))
	}

	e.index.Insert("k", nil, keytrie.NewTipSet(tips[:64]...))
	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		if e.hasWritesSince(logicalAddress("k"), tips[511:], "local") {
			b.Fatal("observed ancestors were treated as new writes")
		}
	}
}
