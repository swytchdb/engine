package effects

import (
	"errors"
	"fmt"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

type admissionArrivalBroadcaster struct {
	mockBroadcaster

	arrive func()
}

func (b *admissionArrivalBroadcaster) BroadcastWithData(*pb.OffsetNotify, []byte) {
	if arrive := b.arrive; arrive != nil {
		b.arrive = nil
		arrive()
	}
}

// Observing an unbound reader's NOOP does not observe its later bind. Its
// consumed base differs from our observation, so the spoken-tip cache cannot
// substitute for checking the bound observation at the publication boundary.
func TestAdmissionHonorsReadBind(t *testing.T) {
	for _, late := range []bool{false, true} {
		for _, wrapped := range []bool{false, true} {
			t.Run(fmt.Sprintf("late=%t/wrapped=%t", late, wrapped), func(t *testing.T) {
				e := newTxnTestEngine(nil)
				defer func() { _ = e.Close() }()

				historicalTestWrite(t, e, map[string]string{"k": "base"}, false)
				base := e.index.Contains("k").Tips()[0]
				receive := func(tip Tip, eff *pb.Effect) {
					eff.NodeId = tip[0]
					eff.Hlc = sTs(int64(tip[1]))
					eff.ForkChoiceHash = ComputeForkChoiceHash(pb.NodeID(tip[0]), eff.GetHlc())

					data, err := MarshalEffect(eff)
					if err != nil {
						t.Fatal(err)
					}

					if _, err = e.HandleRemote(BuildOffsetNotify(pb.NodeID(tip[0]), tip, eff, data, nil)); err != nil {
						t.Fatal(err)
					}
				}
				observation := Tip{7, 100}
				receive(observation, &pb.Effect{Key: []byte("k"), TxnId: "reader", Deps: toPbRefs([]Tip{base}), Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}}})

				writer := e.NewContext()
				writer.BeginTx()

				if err := writer.Emit(scalarSet("k", "writer")); err != nil {
					t.Fatal(err)
				}

				arrived := false

				arrive := func() {
					arrived = true
					reader := Tip{7, 101}
					receive(reader, &pb.Effect{Key: []byte("k"), TxnId: "reader", Deps: toPbRefs([]Tip{observation}), Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
						OriginatorNodeId: 7, TxnHlc: sTs(101), Keys: []*pb.TransactionalBindEffect_KeyBind{{Key: []byte("k"), NewTip: toPbRef(observation), ConsumedTips: toPbRefs([]Tip{base})}},
					}}})

					if wrapped {
						receive(Tip{7, 102}, &pb.Effect{Key: []byte("k"), Deps: toPbRefs([]Tip{reader}), Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
							TxnVerdicts: map[string]pb.Verdict{"reader": pb.Verdict_WON}, OutstandingSubscribers: &pb.SubscriberSet{},
						}}})
					}

					if e.hasWritesSince(logicalAddress("k"), []Tip{observation}, writer.txnID) {
						t.Fatal("read-only bind dirtied write-only validation")
					}

					if _, hit := e.spokenBinds.Get(observation, 0); hit {
						t.Fatal("fixture must exercise the different-base lookup gap")
					}
				}
				if late {
					// Legacy transport publishes effects after preflight and before taking
					// nodeMu. Deliver the read bind precisely in that allowed interval.
					e.broadcaster = &admissionArrivalBroadcaster{arrive: arrive}
				} else {
					arrive()
				}

				published := false

				e.SetOnLocalEffect(func(_ Tip, eff *pb.Effect) {
					if eff.GetTxnBind() != nil {
						published = true
					}
				})

				err := writer.Flush()
				if !arrived || !errors.Is(err, ErrTxnAborted) || published {
					t.Fatalf("arrival=%t flush=%v published=%t", arrived, err, published)
				}
			})
		}
	}
}

func TestAdmissionStopsAtConsumedFrontier(t *testing.T) {
	for _, depth := range []int{16, 256, 2048} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			e, binds, _ := backfillHistory(depth, []string{"k"})
			defer func() { _ = e.Close() }()

			past := binds[len(binds)-1]
			e.updateIndex(logicalAddress("k"), nil, past)

			ours := Tip{99, 1}
			check := e.newAdmissionCheck(logicalAddress("k"), []Tip{past}, "ours", ours)
			loads := 0

			check.load = func(tip Tip) (*pb.Effect, error) {
				loads++

				if tip != ours {
					t.Fatalf("admission opened consumed history at %v", tip)
				}

				return e.getEffect(logicalAddress("k"), tip)
			}
			if check.changed() || loads != 0 {
				t.Fatal("unchanged frontier required history")
			}

			local := scalarSet("k", "ours")

			local.TxnId, local.Deps = "ours", toPbRefs([]Tip{past})
			if err := e.effectCache.Put(ours, local); err != nil {
				t.Fatal(err)
			}

			e.updateIndex(logicalAddress("k"), e.index.Contains("k"), ours)

			if check.changed() || loads != 1 {
				t.Fatalf("publication recheck loaded %d effects", loads)
			}

			if check.changed() || loads != 1 {
				t.Fatal("repeated check reopened validated history")
			}
		})
	}
}

func TestAdmissionDisjointReadBindDoesNotHideEarlierCompetitor(t *testing.T) {
	for _, earlier := range []bool{false, true} {
		t.Run(fmt.Sprint(earlier), func(t *testing.T) {
			log := newSnapshotLog()

			e := newSnapshotEngine(log)
			defer func() { _ = e.Close() }()

			base := log.putEffect(scalarSet("k", "base"))
			readBind := func(txn string, row int64, dep Tip) Tip {
				observation := log.putEffect(&pb.Effect{Key: []byte("k"), TxnId: txn, Deps: toPbRefs([]Tip{dep}), Kind: &pb.Effect_Observation{Observation: &pb.ObservationEffect{
					Predicate: &pb.Predicate{Kind: pb.Predicate_CMP, Col: 0, Op: pb.PredicateCmpOp_CMP_EQ, Literal: &pb.TypedValue{Kind: pb.TypedValue_INT, IntVal: row}},
				}}})

				return log.putEffect(&pb.Effect{Key: []byte("k"), TxnId: txn, Deps: toPbRefs([]Tip{observation}), Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
					Keys: []*pb.TransactionalBindEffect_KeyBind{{Key: []byte("k"), NewTip: toPbRef(observation), ConsumedTips: toPbRefs([]Tip{dep})}},
				}}})
			}

			dep := base
			if earlier {
				dep = readBind("earlier", 2, base)
			}

			current := readBind("disjoint", 1, dep)
			ours := log.putEffect(&pb.Effect{Key: []byte("k"), TxnId: "ours", Deps: toPbRefs([]Tip{base}), Kind: &pb.Effect_RowWrite{RowWrite: &pb.RowWriteEffect{
				Kind: pb.RowWriteEffect_INSERT, Columns: []*pb.TypedValue{{Kind: pb.TypedValue_INT, IntVal: 2}},
			}}})

			e.updateIndex(logicalAddress("k"), nil, current)

			check := e.newAdmissionCheck(logicalAddress("k"), []Tip{base}, "ours", ours)
			if got := check.changed(); got != earlier {
				t.Fatalf("rejected=%t want=%t competitor=%s", got, earlier, check.competingTxn)
			}
		})
	}
}
