package effects

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
)

// An unreachable configured peer is still required when it is a known
// subscriber. Unsafe bootstrap on a new key, however, can finish without
// discovering any remote subscriptions. These must remain distinct cases.
func TestPartitionKnownSubscriberUnreachable(t *testing.T) {
	for _, known := range []bool{false, true} {
		t.Run(fmt.Sprintf("known-subscriber=%t", known), func(t *testing.T) {
			bc := &mockBroadcaster{
				peerIDs: []pb.NodeID{99}, replicateErr: errors.New("peer unreachable"),
			}

			e := newTxnTestEngine(bc)
			if known {
				preSubscribe(e, "seat")

				setup := e.NewContext()
				if err := setup.Emit(&pb.Effect{
					Key:  []byte("seat"),
					Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{SubscriberNodeId: 99}},
				}); err != nil {
					t.Fatal(err)
				}

				if err := setup.Flush(); err != nil {
					t.Fatal(err)
				}
			}

			ctx := e.NewContext()
			ctx.BeginTx()

			if err := ctx.Emit(scalarSet("seat", "passenger")); err != nil {
				t.Fatal(err)
			}

			subscribers := ctx.collectSubscribers()
			if (len(subscribers) == 1) != known {
				t.Fatalf("subscribers=%v, known=%t", subscribers, known)
			}

			before := bc.replicateCount()

			err := ctx.Flush()
			if known && !errors.Is(err, ErrTxnAmbiguous) || !known && err != nil {
				t.Fatalf("Flush=%v, known=%t", err, known)
			}

			wantCalls := 0
			if known {
				wantCalls = 1
			}

			if calls := bc.replicateCount() - before; calls != wantCalls {
				t.Fatalf("commit replication attempts=%d, want %d", calls, wantCalls)
			}

			t.Logf("configured peer 99 unreachable; known subscribers=%v; Flush=%v", subscribers, err)
		})
	}
}

// Reproduce the inferred cancellation mechanism separately from the partition
// experiments. Swytch's command wrapper restores this savepoint on a failed
// queued command. Without that rollback, the engine rejects Flush.
func TestPartitionCommandFailureCharacterization(t *testing.T) {
	for _, rollbackCommand := range []bool{false, true} {
		t.Run(fmt.Sprintf("rollback-command=%t", rollbackCommand), func(t *testing.T) {
			e := newTxnTestEngine(nil)
			preSubscribe(e, "seat", "booking")
			ctx := e.NewContext()
			ctx.BeginTx()

			if err := ctx.Emit(scalarSet("seat", "passenger")); err != nil {
				t.Fatal(err)
			}

			sp := ctx.TakeSavepoint()
			cancelled, cancel := context.WithCancel(context.Background())
			cancel()
			ctx.SetTraceCtx(cancelled)

			if _, _, err := ctx.GetSnapshot("booking"); !errors.Is(err, context.Canceled) {
				t.Fatalf("booking read: %v, want cancellation", err)
			}

			if rollbackCommand {
				ctx.RestoreSavepoint(sp)
			}

			err := ctx.Flush()
			if rollbackCommand && err != nil || !rollbackCommand && !errors.Is(err, context.Canceled) {
				t.Fatalf("Flush: %v, rollback=%t", err, rollbackCommand)
			}

			seat, _, _, err := e.GetSnapshot("seat")
			if err != nil || (seat.GetScalar() != nil) != rollbackCommand {
				t.Fatalf("seat=%v err=%v, rollback=%t", seat, err, rollbackCommand)
			}

			booking, _, _, err := e.GetSnapshot("booking")
			if err != nil || booking != nil {
				t.Fatalf("booking=%v err=%v, want absent", booking, err)
			}
		})
	}
}

// These exercise the dashboard findings through the commit path. Both variants
// must hide the losing transaction atomically after discovering the competitor.
// Transport ACKs are mocked; transaction emission, commit, proof construction,
// and reconstruction are real. A shared pool makes all history resident after
// the heal, while independent indexes keep the pre-heal frontiers separate.
func TestPartitionCommitCharacterization(t *testing.T) {
	for _, subscriber := range []bool{false, true} {
		t.Run(fmt.Sprintf("subscriber=%t", subscriber), func(t *testing.T) {
			log := newSnapshotLog()
			reader := newSnapshotEngine(log)

			writers := make([]*Engine, 0, 2)
			txns := make([]string, 0, 2)
			binds := make([]Tip, 0, 2)

			for i := range 2 {
				bc := &txnMockBroadcaster{}
				bc.peerIDs = []pb.NodeID{99}
				// Deliberately report a minority partition: UnsafeMode admits it.
				writer := newTxnTestEngine(bc)
				writer.nodeID = pb.NodeID(i + 10)
				writer.effectCache = log.effectCache
				booking := fmt.Sprintf("booking:%d", i)
				preSubscribe(writer, "seat", booking)

				if subscriber {
					ctx := writer.NewContext()
					if err := ctx.Emit(&pb.Effect{
						Key:  []byte("seat"),
						Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{SubscriberNodeId: 99}},
					}); err != nil {
						t.Fatal(err)
					}

					if err := ctx.Flush(); err != nil {
						t.Fatal(err)
					}
				}

				ctx := writer.NewContext()
				ctx.BeginTx()

				txns = append(txns, ctx.txnID)
				for _, key := range []string{"seat", booking} {
					if _, _, err := ctx.GetSnapshot(key); err != nil {
						t.Fatal(err)
					}

					data := scalarInsertRaw([]byte(fmt.Sprintf("passenger:%d", i)))
					if key == booking {
						data = keyedInsertRaw("seat", []byte("seat"))
					}

					if err := ctx.Emit(&pb.Effect{Key: []byte(key), Kind: &pb.Effect_Data{Data: data}}); err != nil {
						t.Fatal(err)
					}
				}

				if err := ctx.Flush(); err != nil {
					t.Fatalf("partition %d commit: %v", i, err)
				}

				wantCalls := 0
				if subscriber {
					wantCalls = 1
				}

				if len(bc.replicateToCalls) != wantCalls {
					t.Fatalf("subscriber calls = %v, want %d", bc.replicateToCalls, wantCalls)
				}

				var bind Tip
				if err := writer.walkBindEvidence("seat", writer.index.Contains("seat").Tips(), func(tip Tip, eff *pb.Effect) {
					if eff.GetTxnBind() != nil && eff.GetTxnId() == txns[i] {
						bind = tip
					}
				}); err != nil || bind == (Tip{}) {
					t.Fatalf("find emitted bind: %v, %v", bind, err)
				}

				binds = append(binds, bind)
				writers = append(writers, writer)
			}

			// Heal by making both committed frontiers visible to a fresh reader.
			for i, writer := range writers {
				for _, key := range []string{"seat", fmt.Sprintf("booking:%d", i)} {
					preSubscribe(reader, key)

					for _, tip := range writer.index.Contains(key).Tips() {
						reader.updateIndex(key, nil, tip)
					}
				}
			}

			left, _ := log.effectCache.Get(binds[0])
			right, _ := log.effectCache.Get(binds[1])

			winner, loser := 0, 1
			if ForkChoiceLess(right.GetForkChoiceHash(), left.GetForkChoiceHash()) {
				winner, loser = loser, winner
			}

			seat, _, _, err := reader.GetSnapshot("seat")
			if err != nil || string(seat.GetScalar().GetRaw()) != fmt.Sprintf("passenger:%d", winner) {
				t.Fatalf("seat=%v err=%v winner=%d", seat, err, winner)
			}

			booking, _, _, err := reader.GetSnapshot(fmt.Sprintf("booking:%d", loser))
			if err != nil {
				t.Fatal(err)
			}

			visible := len(booking.GetNetAdds()) != 0
			if visible {
				t.Error("losing booking survived cross-key transaction arbitration")
			}

			reports := conflictList(t, reader, conflictIndexPrefix+"seat")

			wantReports := 0
			if subscriber {
				wantReports = 2
			}

			if len(reports) != wantReports {
				t.Errorf("reports=%v, want %d", reports, wantReports)
			}

			t.Logf("both Flush calls succeeded; winner=%d; losing booking visible=%t; reports=%d", winner, visible, len(reports))
		})
	}
}

// Hold bind priority and every data effect fixed, and vary only the hashes of
// the verdict snapshots at the frontier. This isolates value replay order from
// transaction arbitration. The plain write on the losing branch survives the
// losing bind and overwrites the winner when that branch is traversed last.
func TestPartitionReplayCharacterization(t *testing.T) {
	for _, evidence := range []bool{false, true} {
		for _, loserLast := range []bool{false, true} {
			for _, reverseTips := range []bool{false, true} {
				t.Run(fmt.Sprintf("evidence=%t/loser-last=%t/reverse=%t", evidence, loserLast, reverseTips), func(t *testing.T) {
					log := newSnapshotLog()
					e := newSnapshotEngine(log)

					var seatTips []Tip

					for i, name := range []string{"alice", "bob"} {
						log.nodeID = uint64(i + 10)
						base := log.putEffect(scalarSet("winner", "free:"+name))
						bind := closureBind(log, name, byte(i+1), []string{"winner", "payout:" + name}, map[string]Tip{"winner": base})

						var proof *pb.BindEvidence

						if evidence {
							var err error

							proof, err = e.bindProof(bind, nil, nil)
							if err != nil {
								t.Fatal(err)
							}
						}

						for _, key := range []string{"winner", "payout:" + name} {
							hash := byte(10 + i)
							if !loserLast {
								hash = byte(11 - i)
							}

							tip := log.putEffect(&pb.Effect{
								Key: []byte(key), NodeId: log.nodeID, ForkChoiceHash: []byte{hash},
								Deps: toPbRefs([]Tip{bind}),
								Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
									TxnVerdicts: map[string]pb.Verdict{name: pb.Verdict_WON}, Evidence: proof,
								}},
							})
							preSubscribe(e, key)

							if key == "winner" {
								seatTips = append(seatTips, tip)
							} else {
								e.updateIndex(key, nil, tip)
							}
						}

						e.effectCache.releaseEvidence(evidenceTips(proof))
					}

					if reverseTips {
						slices.Reverse(seatTips)
					}

					e.index.Insert("winner", nil, keytrie.NewTipSet(seatTips...))
					got, _, _, err := e.GetSnapshot("winner")

					want := "alice"
					if loserLast {
						want = "free:bob"
					}

					if err != nil || string(got.GetScalar().GetRaw()) != want {
						t.Fatalf("seat=%v err=%v, want %q", got, err, want)
					}

					anchors, verdicts, err := e.bindKeyClosure("payout:bob")
					if err != nil {
						t.Fatal(err)
					}

					wantKeys := 3
					if len(anchors) != wantKeys || verdicts["bob"].proven != evidence {
						t.Fatalf("closure=%v verdicts=%v", anchors, verdicts)
					}

					t.Logf("Alice wins bind priority; seat=%q; Bob's booking closure has %d keys", want, len(anchors))
				})
			}
		}
	}
}
