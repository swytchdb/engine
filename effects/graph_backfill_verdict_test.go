package effects

import (
	"fmt"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
)

// Recovering a bind on an owned key does not acquire its other keys.
func TestGraphBackfillDoesNotAcquireOtherKeys(t *testing.T) {
	for _, tc := range []struct {
		name        string
		outcome     pb.Verdict
		closed      bool
		adjudicated bool
	}{
		{"closed-win", pb.Verdict_WON, true, true},
		{"lost", pb.Verdict_LOST, false, true},
		{"open-win", pb.Verdict_WON, false, false},
		{"unresolved", pb.Verdict_VERDICT_UNSPECIFIED, false, false},
	} {
		for _, resident := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/resident=%t", tc.name, resident), func(t *testing.T) {
				log := newSnapshotLog()
				e := newSnapshotEngine(log)

				t.Cleanup(func() { _ = e.Close() })
				preSubscribe(e, "animals")

				key := logicalAddress("animals")
				other := keytrie.InternalKey("animals", "unserved")
				missing := Tip{99, 1}
				base := log.putEffect(&pb.Effect{Key: []byte("animals"), Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{State: &pb.ReducedEffect{TypeTag: pb.ValueType_TYPE_GRAPH}}}})
				bind := log.putEffect(&pb.Effect{Key: []byte("animals"), IKey: []byte(key), NodeId: 1, Hlc: sTs(100), TxnId: "historical", Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{Keys: []*pb.TransactionalBindEffect_KeyBind{
					{Key: []byte(key), NewTip: toPbRef(base)},
					{Key: []byte(other), NewTip: toPbRef(missing)},
				}}}})

				snap := &pb.SnapshotEffect{}
				if tc.outcome != pb.Verdict_VERDICT_UNSPECIFIED {
					snap.TxnVerdicts = map[string]pb.Verdict{"historical": tc.outcome}
				}

				if tc.closed {
					snap.OutstandingSubscribers = &pb.SubscriberSet{}
				}

				owner := log.putEffect(&pb.Effect{Key: []byte("animals"), NodeId: 1, Hlc: sTs(101), Deps: toPbRefs([]Tip{bind}), Kind: &pb.Effect_Snapshot{Snapshot: snap}})
				bindEffect, _ := e.effectCache.Get(bind)
				bindWire := wireEffect(bindEffect)

				if !resident {
					e.effectCache.decrefChainNode(key, bind)
					e.effectCache.decrefChainNode(other, bind)
					e.reclaimUnreferenced()

					if _, ok := e.effectCache.Get(bind); ok {
						t.Fatal("fixture retained bind")
					}
				}

				foreignFetches := 0
				e.broadcaster = &cutBroadcaster{load: func(tip Tip) ([]byte, error) {
					if tip == missing {
						foreignFetches++

						return nil, ErrEffectNotFound
					}

					if tip == bind {
						return bindWire, nil
					}

					t.Errorf("unexpected fetch %v", tip)

					return nil, ErrEffectNotFound
				}}

				verdicts, err := e.ingestNackBatch([]*pb.NackNotify{{Key: []byte("animals"), TipDetails: []*pb.NackTipDetail{{Ref: toPbRef(owner)}}}})
				if err != nil {
					t.Fatal(err)
				}

				if verdicts.adjudicated("historical") != tc.adjudicated {
					t.Fatalf("verdict classification: %v", verdicts)
				}

				if tc.adjudicated && foreignFetches != 0 {
					t.Fatalf("adjudicated replay requested sibling history %d times", foreignFetches)
				}

				if tc.adjudicated {
					if _, ok := e.effectCache.Get(bind); !ok {
						t.Fatal("recovery did not install the bind")
					}

					requireCutTips(t, e.index.Contains("animals").Tips(), []Tip{owner})
				}

				if _, subscribed := e.subscription(other); subscribed || e.index.ContainsInternal(other).Len() != 0 {
					t.Fatal("historical recovery admitted the sibling key")
				}
			})
		}
	}
}
