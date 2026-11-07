package effects

import (
	"fmt"
	"slices"
	"sync"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
)

func frontierEffect(id uint64, deps ...uint64) *pb.Effect {
	eff := &pb.Effect{
		Key: []byte("k"), NodeId: 7, Hlc: sTs(int64(id)),
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	}

	eff.ForkChoiceHash = ComputeForkChoiceHash(7, eff.GetHlc())
	for _, dep := range deps {
		eff.Deps = append(eff.Deps, toPbRef(Tip{7, dep}))
	}

	return eff
}

func TestBackfillFrontierMissingLink(t *testing.T) {
	e := newSnapshotEngine(nil)
	preSubscribe(e, "k")

	for _, id := range []uint64{1, 3} {
		eff := frontierEffect(id)
		if id == 3 {
			eff = frontierEffect(id, 2)
		}

		if err := e.handleBackfill(&pb.OffsetNotify{Origin: toPbRef(Tip{7, id}), EffectData: wireEffect(eff)}); err != nil {
			t.Fatal(err)
		}
	}

	requireFrontier(t, e, Tip{7, 1}, Tip{7, 3})

	if err := e.handleBackfill(&pb.OffsetNotify{Origin: toPbRef(Tip{7, 2}), EffectData: wireEffect(frontierEffect(2, 1))}); err != nil {
		t.Fatal(err)
	}

	requireFrontier(t, e, Tip{7, 3})
}

func TestBackfillFrontierConcurrentDelivery(t *testing.T) {
	for _, remote := range []bool{false, true} {
		t.Run(fmt.Sprintf("remote=%t", remote), func(t *testing.T) {
			e := newSnapshotEngine(nil)
			preSubscribe(e, "k")

			const writers, length = 4, 32

			want := make([]Tip, 0, writers)

			var wg sync.WaitGroup

			for writer := range writers {
				want = append(want, Tip{7, uint64(writer*length + length)})

				wg.Go(func() {
					for i := length; i > 0; i-- {
						id := uint64(writer*length + i)

						eff := frontierEffect(id)
						if i > 1 {
							eff = frontierEffect(id, id-1)
						}

						notify := &pb.OffsetNotify{Origin: toPbRef(Tip{7, id}), EffectData: wireEffect(eff)}

						var err error
						if remote {
							_, err = e.HandleRemote(notify)
						} else {
							err = e.handleBackfill(notify)
						}

						if err != nil {
							t.Error(err)

							return
						}
					}
				})
			}

			wg.Wait()
			requireFrontier(t, e, want...)

			state, _, _, err := e.GetSnapshot("k")
			if err != nil || state.GetScalar().GetIntVal() != writers*length {
				t.Fatalf("concurrent recovered value = %v, error = %v", state, err)
			}
		})
	}
}

func TestBackfillFrontierBindKeys(t *testing.T) {
	for _, canonical := range []string{"k", "other"} {
		for _, advanced := range []bool{false, true} {
			t.Run(fmt.Sprintf("canonical=%s/advanced=%t", canonical, advanced), func(t *testing.T) {
				e := newSnapshotEngine(nil)
				preSubscribe(e, "k", "other")
				e.broadcaster = &mockBroadcaster{}
				other := frontierEffect(2)
				other.Key = []byte("other")
				bind := frontierEffect(3)
				bind.Key = []byte(canonical)
				bind.Kind = &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
					Keys: []*pb.TransactionalBindEffect_KeyBind{
						{Key: []byte("k"), NewTip: toPbRef(Tip{7, 1})},
						{Key: []byte("other"), NewTip: toPbRef(Tip{7, 2})},
					},
				}}
				child := frontierEffect(4, 3)

				child.Key = []byte("other")
				for i, eff := range []*pb.Effect{frontierEffect(1), other, bind, child} {
					if err := e.effectCache.Put(Tip{7, uint64(i + 1)}, eff); err != nil {
						t.Fatal(err)
					}
				}

				otherWant := Tip{7, 3}
				if advanced {
					otherWant = Tip{7, 4}
					e.index.Insert("other", nil, keytrie.NewTipSet(otherWant))
				}

				e.ingestNackTips(&pb.NackNotify{Key: []byte("k"), TipDetails: []*pb.NackTipDetail{{Ref: toPbRef(Tip{7, 3})}}})
				requireFrontier(t, e, Tip{7, 3})

				got := e.index.Contains("other")
				if got.Len() != 1 || !got.Contains(otherWant) {
					t.Fatalf("other-key frontier = %v, want %v", got.Tips(), otherWant)
				}
			})
		}
	}
}

func requireFrontier(t *testing.T, e *Engine, want ...Tip) {
	t.Helper()

	got := e.index.Contains("k")
	if got.Len() != len(want) || !got.ContainsAll(want) {
		t.Fatalf("frontier = %v, want %v", got.Tips(), want)
	}
}

// Recovery must cache ancestry without turning it into independent tips. Check
// the index and the outgoing advertisement before a read can prune stale tips.
func TestBackfillFrontierSingleAdvertisedTip(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(fmt.Sprintf("cached=%t", cached), func(t *testing.T) {
			e := newSnapshotEngine(nil)
			preSubscribe(e, "k")

			bc := &frontierBroadcaster{fetchable: make(map[Tip][]byte), fetched: make(map[Tip]bool)}
			e.broadcaster = bc

			const n = 128
			for id := uint64(1); id <= n; id++ {
				eff := frontierEffect(id)
				if id > 1 {
					eff.Deps = toPbRefs([]Tip{{7, id - 1}})
				}

				tip := Tip{7, id}

				bc.fetchable[tip] = wireEffect(eff)
				if cached {
					if err := e.effectCache.Put(tip, eff); err != nil {
						t.Fatal(err)
					}
				}
			}

			if cached {
				e.index.Insert("k", nil, keytrie.NewTipSet(Tip{7, n}))
			}

			nack := &pb.NackNotify{Key: []byte("k"), TipDetails: []*pb.NackTipDetail{{Ref: toPbRef(Tip{7, n})}}}
			for range 3 {
				e.ingestNackTips(nack)
				requireFrontier(t, e, Tip{7, n})

				out := e.buildEnrichedNack("k", nil, e.index.Contains("k").Tips())
				if len(out.GetTips()) != 1 || len(out.GetTipDetails()) != 1 || r(out.GetTips()[0]) != (Tip{7, n}) {
					t.Fatalf("recovery advertised ancestors as current tips: %v", out.GetTips())
				}
			}

			for id := uint64(1); id <= n; id++ {
				if _, ok := e.effectCache.Get(Tip{7, id}); !ok {
					t.Fatalf("recovery omitted ancestor %d", id)
				}
			}

			e.broadcaster = nil

			state, _, _, err := e.GetSnapshot("k")
			if err != nil || state.GetScalar().GetIntVal() != n {
				t.Fatalf("recovered value = %v, error = %v", state, err)
			}
		})
	}
}

// Two concurrent branches share a root: 1 -> 2 -> 4 and 1 -> 3.
// Even when a child arrives before its missing parent, or an ancestor is
// redelivered after its descendants, only 3 and 4 belong in the frontier.
func TestBackfillFrontierDeliveryOrder(t *testing.T) {
	effects := []*pb.Effect{frontierEffect(1), frontierEffect(2, 1), frontierEffect(3, 1), frontierEffect(4, 2)}

	for _, remote := range []bool{false, true} {
		var visit func([]int, []int)

		visit = func(order, remaining []int) {
			if len(remaining) > 0 {
				for i, id := range remaining {
					visit(append(slices.Clone(order), id), slices.Delete(slices.Clone(remaining), i, i+1))
				}

				return
			}

			t.Run(fmt.Sprintf("remote=%t/order=%v", remote, order), func(t *testing.T) {
				e := newSnapshotEngine(nil)
				preSubscribe(e, "k")

				deliver := func(id int) {
					t.Helper()

					notify := &pb.OffsetNotify{Key: []byte("k"), Origin: toPbRef(Tip{7, uint64(id + 1)}), EffectData: wireEffect(effects[id])}

					var err error
					if remote {
						_, err = e.HandleRemote(notify)
					} else {
						err = e.handleBackfill(notify)
					}

					if err != nil {
						t.Fatal(err)
					}
				}
				for _, id := range order {
					deliver(id)
				}

				requireFrontier(t, e, Tip{7, 3}, Tip{7, 4})

				for _, id := range []int{3, 2, 1, 0} {
					deliver(id)
					requireFrontier(t, e, Tip{7, 3}, Tip{7, 4})
				}

				state, _, _, err := e.GetSnapshot("k")
				if err != nil || state.GetScalar().GetIntVal() != 4 {
					t.Fatalf("recovered branches = %v, error = %v", state, err)
				}
			})
		}
		visit(nil, []int{0, 1, 2, 3})
	}
}
