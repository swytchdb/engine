package effects

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

func TestNackVerdictsPrecedeCandidateAncestry(t *testing.T) {
	for _, tc := range []struct {
		name    string
		outcome pb.Verdict
		closed  bool
		skip    bool
	}{
		{"lost", pb.Verdict_LOST, false, true},
		{"closed-win", pb.Verdict_WON, true, true},
		{"open-win", pb.Verdict_WON, false, false},
		{"unresolved", pb.Verdict_VERDICT_UNSPECIFIED, false, false},
	} {
		for _, reverse := range []bool{false, true} {
			for _, wrapped := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/reverse-%t/wrapped-%t", tc.name, reverse, wrapped), func(t *testing.T) {
					log := newSnapshotLog()
					e := newSnapshotEngine(log)
					base := log.putEffect(scalarSet("k", "base"))
					ours := newTestPendingTxn(pendingTxnKey{key: logicalAddress("k"), newTip: base, collection: pb.CollectionKind_SCALAR})
					ours.txnID = "ours"
					ours.bindOffset = log.putEffect(&pb.Effect{
						Key: []byte("k"), TxnId: "ours", Deps: toPbRefs([]Tip{base}),
						Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
							Keys: []*pb.TransactionalBindEffect_KeyBind{{Key: []byte("k"), NewTip: toPbRef(base)}},
						}},
					})

					node := pb.NodeID(2)
					for !ForkChoiceLess(ComputeForkChoiceHash(node, sTs(100)), ours.forkChoiceHash) {
						node++
					}

					missing := Tip{99, 1}
					bind := &pb.Effect{
						Key: []byte("k"), TxnId: "rival", NodeId: uint64(node), Hlc: sTs(100), Deps: toPbRefs([]Tip{missing}),
						Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
							OriginatorNodeId: uint64(node), TxnHlc: sTs(100),
							Keys: []*pb.TransactionalBindEffect_KeyBind{{Key: []byte("k"), NewTip: toPbRef(missing)}},
						}},
					}

					candidate := log.putEffect(bind)
					if wrapped {
						candidate = log.putEffect(&pb.Effect{
							Key: []byte("k"), Deps: toPbRefs([]Tip{candidate}),
							Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{}},
						})
					}

					snap := &pb.SnapshotEffect{}
					if tc.outcome != pb.Verdict_VERDICT_UNSPECIFIED {
						snap.TxnVerdicts = map[string]pb.Verdict{"rival": tc.outcome}
					}

					if tc.closed {
						snap.OutstandingSubscribers = &pb.SubscriberSet{}
					}

					owner := log.putEffect(&pb.Effect{Key: []byte("evidence"), Kind: &pb.Effect_Snapshot{Snapshot: snap}})

					responses := []*pb.NackNotify{
						e.buildEnrichedNack(logicalAddress("k"), toPbRef(ours.bindOffset), []Tip{candidate}),
						e.buildEnrichedNack(logicalAddress("evidence"), toPbRef(ours.bindOffset), []Tip{owner}),
					}
					if reverse {
						slices.Reverse(responses)
					}

					loads := 0
					e.broadcaster = &cutBroadcaster{load: func(tip Tip) ([]byte, error) {
						loads++

						return nil, ErrEffectNotFound
					}}

					defeated, err := e.evaluateNacks(ours, responses)
					if tc.skip {
						if err != nil || len(defeated) != 0 || loads != 0 {
							t.Fatalf("adjudicated candidate reopened: defeated=%v err=%v fetches=%d", defeated, err, loads)
						}

						if !slices.Contains(ours.proofSources, owner) {
							t.Fatal("reused verdict lost its legacy evidence source")
						}
					} else {
						want := ErrTxnAborted
						if tc.outcome == pb.Verdict_VERDICT_UNSPECIFIED {
							want = ErrTxnAmbiguous
						}

						if !errors.Is(err, want) || loads == 0 {
							t.Fatalf("unsettled competitor skipped: err=%v want=%v fetches=%d", err, want, loads)
						}
					}
				})
			}
		}
	}
}

func TestNackVerdictForCurrentTransaction(t *testing.T) {
	for _, contradict := range []bool{false, true} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("contradict-%t/reverse-%t", contradict, reverse), func(t *testing.T) {
				log := newSnapshotLog()
				e := newSnapshotEngine(log)
				ours := newTestPendingTxn(pendingTxnKey{key: logicalAddress("k")})
				ours.txnID = "ours"

				outcomes := []pb.Verdict{pb.Verdict_LOST}
				if contradict {
					outcomes = append(outcomes, pb.Verdict_WON)
				}

				responses := make([]*pb.NackNotify, 1, 1+len(outcomes))
				responses[0] = &pb.NackNotify{Key: []byte("k"), TipDetails: []*pb.NackTipDetail{{Ref: toPbRef(Tip{99, 1}), IsBind: true}}}

				for _, outcome := range outcomes {
					owner := log.putEffect(&pb.Effect{Key: []byte("evidence"), Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
						TxnVerdicts: map[string]pb.Verdict{"ours": outcome}, OutstandingSubscribers: &pb.SubscriberSet{},
					}}})
					responses = append(responses, e.buildEnrichedNack(logicalAddress("evidence"), toPbRef(ours.bindOffset), []Tip{owner}))
				}

				if reverse {
					slices.Reverse(responses)
				}

				e.broadcaster = &cutBroadcaster{load: func(tip Tip) ([]byte, error) {
					t.Fatalf("known outcome reopened unrelated bind %v", tip)

					return nil, ErrEffectNotFound
				}}
				_, err := e.evaluateNacks(ours, responses)

				want := ErrTxnAborted
				if contradict {
					want = ErrTxnAmbiguous
				}

				if !errors.Is(err, want) {
					t.Fatalf("got %v, want %v", err, want)
				}
			})
		}
	}
}

func TestNackAdjudicatedReplayKeepsOtherFrontiers(t *testing.T) {
	for _, outcome := range []pb.Verdict{pb.Verdict_LOST, pb.Verdict_WON} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/reverse-%t", outcome, reverse), func(t *testing.T) {
				e, binds, _ := backfillHistory(1, []string{"a", "b"})
				defer func() { _ = e.Close() }()

				snap := &pb.SnapshotEffect{TxnVerdicts: map[string]pb.Verdict{"txn-0": outcome}}
				if outcome == pb.Verdict_WON {
					snap.OutstandingSubscribers = &pb.SubscriberSet{}
				}

				owner := Tip{99, 2}
				if err := e.effectCache.Put(owner, &pb.Effect{Key: []byte("evidence"), Kind: &pb.Effect_Snapshot{Snapshot: snap}}); err != nil {
					t.Fatal(err)
				}

				e.broadcaster = &cutBroadcaster{load: func(tip Tip) ([]byte, error) {
					t.Fatalf("unexpected history fetch: %v", tip)

					return nil, ErrEffectNotFound
				}}

				responses := []*pb.NackNotify{
					{Key: []byte("a"), TipDetails: []*pb.NackTipDetail{{Ref: toPbRef(binds[0]), IsBind: true}}},
					{Key: []byte("evidence"), TipDetails: []*pb.NackTipDetail{{Ref: toPbRef(owner)}}},
				}
				if reverse {
					slices.Reverse(responses)
				}

				verdicts, err := e.ingestNackBatch(responses)
				if err != nil || !verdicts.adjudicated("txn-0") {
					t.Fatalf("response verdict was lost during closure: %v, %v", verdicts, err)
				}

				requireCutTips(t, e.index.Contains("a").Tips(), []Tip{binds[0]})

				if e.index.Contains("b").Len() != 0 {
					t.Fatal("adjudicated replay published another key")
				}
			})
		}
	}
}

func TestClosureLostBindDoesNotReopenOtherKeys(t *testing.T) {
	e, key, owner := completedClosureFixture(t, 2)
	snapshot, _ := e.effectCache.Get(owner)
	// The winner may still be open, but this bind's loss is already known.
	snapshot.GetSnapshot().TxnVerdicts["txn-0"] = pb.Verdict_LOST
	snapshot.GetSnapshot().OutstandingSubscribers = &pb.SubscriberSet{NodeIds: []uint64{9}}

	op := newClosureOperation(e, make(map[Tip]error))
	defer op.release()

	_, complete, err := op.run(key, []Tip{owner}, false, false)
	if err != nil || !complete || len(op.work) != 1 || len(op.binds) != 0 {
		t.Fatalf("lost bind reopened: complete=%t error=%v keys=%d projections=%d", complete, err, len(op.work), len(op.binds))
	}
}
