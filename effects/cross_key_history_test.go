package effects

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"google.golang.org/protobuf/proto"
)

// A synthetic version of the el-0/el-4 incident, not a replay of its logs.
// el-4 has two state snapshots above a shared bind. el-0 still needs the
// bind and its own data to compare it with a concurrent, losing transaction.
// No verdict cache is supplied: reconstruction must derive the outcome.
type crossKeyHistory struct {
	engine             *Engine
	shared, competitor Tip
	data0, data4       Tip
	previous, latest   Tip
}

func newCrossKeyHistory(t *testing.T, gap bool) crossKeyHistory {
	t.Helper()
	log := newSnapshotLog()
	e := newSnapshotEngine(log)
	t.Cleanup(func() { _ = e.Close() })
	preSubscribe(e, "el-0", "el-4")
	f := crossKeyHistory{engine: e}
	f.shared = closureBind(log, "shared", 1, []string{"el-0", "el-4"}, nil)
	bind, _ := e.effectCache.Get(f.shared)
	f.data0 = r(bind.GetTxnBind().GetKeys()[0].GetNewTip())
	f.data4 = r(bind.GetTxnBind().GetKeys()[1].GetNewTip())
	otherData := log.putEffect(&pb.Effect{
		Key: []byte("el-0"), TxnId: "competitor",
		Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("wrong"))},
	})
	f.competitor = log.putEffect(&pb.Effect{
		Key: []byte("el-0"), TxnId: "competitor", ForkChoiceHash: []byte{2},
		Deps: toPbRefs([]Tip{otherData}),
		Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
			Keys: []*pb.TransactionalBindEffect_KeyBind{{Key: []byte("el-0"), NewTip: toPbRef(otherData)}},
		}},
	})
	state := func(parent Tip) Tip {
		return log.putEffect(&pb.Effect{
			Key: []byte("el-4"), Deps: toPbRefs([]Tip{parent}),
			Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
				State: &pb.ReducedEffect{Scalar: scalarInsertRaw([]byte("100"))},
			}},
		})
	}
	parent := f.shared
	if gap {
		parent = log.putEffect(&pb.Effect{Key: []byte("el-4"), Deps: toPbRefs([]Tip{parent}), Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}}})
	}
	f.previous = state(parent)
	f.latest = state(f.previous)
	e.installIngestedTips("el-0", f.shared, f.competitor)
	e.installIngestedTips("el-4", f.latest)
	return f
}

func (f crossKeyHistory) compact(t *testing.T) {
	t.Helper()
	d := newDag(f.engine, "el-4", "")
	d.compact = true
	if err := d.prepare([]Tip{f.latest}); err != nil {
		t.Fatal(err)
	}
	if d.lcaTip != f.latest {
		t.Fatalf("el-4 LCA = %v, want %v", d.lcaTip, f.latest)
	}
	f.engine.reclaimUnreferenced()
}

func TestCompactionPreservesOtherKeyBindOwnership(t *testing.T) {
	f := newCrossKeyHistory(t, false)
	p := f.engine.effectCache
	shared := p.getVertex(f.shared)
	if got := shared.refs.Load(); got != 2 {
		t.Fatalf("shared bind has %d refs before compaction, want one per key", got)
	}
	f.compact(t)
	if p.getVertex(f.shared) != shared || shared.refs.Load() != 1 {
		t.Fatal("el-4 compaction did not preserve el-0's reference to the shared bind")
	}
	shared.creationMu.Lock()
	owners := slices.Clone(shared.creationKeys)
	shared.creationMu.Unlock()
	if !slices.Equal(owners, []string{"el-0"}) {
		t.Fatalf("bind owners = %v, want only el-0", owners)
	}
	if _, ok := p.Get(f.data4); ok {
		t.Fatal("el-4 data below both snapshots was not reclaimed")
	}
	for _, tip := range []Tip{f.data0, f.competitor, f.previous, f.latest} {
		if _, ok := p.Get(tip); !ok {
			t.Fatalf("compaction reclaimed required vertex %v", tip)
		}
	}
	// el-0's own DAG remains complete after the other key's compaction.
	d := newDag(f.engine, "el-0", "")
	if err := d.prepare(f.engine.index.Contains("el-0").Tips()); err != nil {
		t.Fatal(err)
	}
	if d.nodes[f.shared] == nil || d.nodes[f.data0] == nil {
		t.Fatal("el-0 no longer reaches its bind and data")
	}
	losers, _ := f.engine.losersOnKey("el-0", nil, nil)
	if _, lost := losers["competitor"]; !lost || len(losers) != 1 {
		t.Fatalf("retained el-0 evidence gives losers %v, want only competitor", losers)
	}
	// Releasing the final key must release the shared envelope too.
	f.engine.releaseChainRefs("el-0", f.engine.index.Contains("el-0").Tips(), nil)
	f.engine.reclaimUnreferenced()
	if _, ok := p.Get(f.shared); ok {
		t.Fatal("shared bind leaked after both keys released their ownership")
	}
}

func TestBindReconstructionAfterOtherKeyCompaction(t *testing.T) {
	for _, gap := range []bool{false, true} {
		var baseline []*pb.ReducedEffect
		for _, compact := range []bool{false, true} {
			t.Run(fmt.Sprintf("gap=%t/compacted=%t", gap, compact), func(t *testing.T) {
				f := newCrossKeyHistory(t, gap)
				// The committing key retains the resolution proof before any
				// other key can compact it. The old fixture omitted this owner.
				proof, err := f.engine.bindProof(f.shared, map[string]Tip{"competitor": f.competitor}, nil)
				if err != nil {
					t.Fatal(err)
				}
				if err := f.engine.emitSnapshot("el-0", nil, proof); err != nil {
					t.Fatal(err)
				}
				f.engine.effectCache.releaseEvidence(evidenceTips(proof))
				if compact {
					f.compact(t)
				}
				// A strict batch exercises evidence discovery as well as value
				// reconstruction. Returning the competitor's value is also wrong.
				got, err := f.engine.GetSnapshots(context.Background(), []string{"el-0", "el-4"})
				if err != nil {
					t.Fatalf("reconstruct retained cross-key history: %v", err)
				}
				if len(got) != 2 {
					t.Fatalf("returned %d values, want 2", len(got))
				}
				for i, value := range got {
					if string(value.GetScalar().GetRaw()) != "100" {
						t.Errorf("key %d reconstructed %v, want shared transaction value 100", i, value)
					}
					if compact && (len(baseline) != len(got) || !proto.Equal(value, baseline[i])) {
						t.Errorf("key %d changed after reclamation: got %v, unreclaimed %v", i, value, baseline)
					}
				}
				if !compact {
					baseline = got
				}
			})
		}
	}
}

func TestCrossKeyClosureEquivalentDiscovery(t *testing.T) {
	transport := errors.New("peer unavailable")
	for _, tc := range []struct {
		name       string
		subscribed bool
		failure    error
	}{
		{name: "available", subscribed: true},
		{name: "unsubscribed", failure: transport},
		{name: "peer-absent", subscribed: true, failure: ErrEffectNotFound},
		{name: "cloud-absent", subscribed: true, failure: ErrCDNBlobMissing},
		{name: "transport-failure", subscribed: true, failure: transport},
	} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stream=%t", tc.name, stream), func(t *testing.T) {
				source := newCrossKeyHistory(t, false)
				pool := source.engine.effectCache
				noop := Tip{9, 1}
				putCutEffects(t, source.engine, map[Tip]*pb.Effect{
					noop: {Key: []byte("el-0"), Deps: toPbRefs([]Tip{source.shared}), Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}}},
				})
				var direct []Tip
				for _, ancestor := range []bool{false, true} {
					e := newTestEngine(nil)
					t.Cleanup(func() { _ = e.Close() })
					preSubscribe(e, "el-0")
					if tc.subscribed {
						preSubscribe(e, "el-4")
					}
					load := func(tip Tip) ([]byte, error) {
						if tip == source.data4 {
							if !tc.subscribed {
								t.Fatal("fetched the unsubscribed branch")
							}
							if tc.failure != nil {
								return nil, tc.failure
							}
						}
						eff, ok := pool.Get(tip)
						if !ok {
							t.Fatalf("unexpected fetch %v", tip)
						}
						return wireEffect(eff), nil
					}
					e.broadcaster = &cutBroadcaster{load: load}
					if stream {
						e.broadcaster = &ancestryBroadcaster{load: func(key string, root Tip, _ []pb.NodeID, have func(Tip) bool, receive func(Tip, []byte, bool) error) error {
							var err error
							pool.VisitAncestry(key, root, func(tip Tip, _ *pb.Effect, isRoot bool) bool {
								if have(tip) {
									return true
								}
								var data []byte
								data, err = load(tip)
								if err != nil && !errors.Is(err, ErrEffectNotFound) && !errors.Is(err, ErrCDNBlobMissing) {
									return false
								}
								err = receive(tip, data, isRoot)
								return err == nil
							})
							return err
						}}
					}
					incoming := source.shared
					if ancestor {
						incoming = noop
					}
					eff, _ := pool.Get(incoming)
					if err := e.putIngested(incoming, eff, proto.Size(eff)); err != nil {
						t.Fatal(err)
					}
					_, complete, err := e.closeIngestedFrontier("el-0", []Tip{incoming}, make(map[Tip]error))
					wantFailure := tc.subscribed && tc.failure == transport
					if complete == wantFailure || errors.Is(err, transport) != wantFailure || (!wantFailure && err != nil) {
						t.Fatalf("ancestor=%t: complete=%t error=%v, want transport failure=%t", ancestor, complete, err, wantFailure)
					}
					var available []Tip
					for _, tip := range []Tip{source.shared, source.data0, source.data4} {
						_, resident := e.effectCache.Get(tip)
						want := tip != source.data4 || tc.subscribed && tc.failure == nil
						if resident != want {
							t.Errorf("ancestor=%t: vertex %v resident=%t, want %t", ancestor, tip, resident, want)
						}
						if resident {
							available = append(available, tip)
						}
					}
					if ancestor && !slices.Equal(available, direct) {
						t.Errorf("ancestry discovery closure %v differs from direct receipt %v", available, direct)
					}
					direct = available
				}
			})
		}
	}
}

// Folding a bind into another key's state does not prove that it won. The
// shared bind beats competitor on el-0, but loses to winner on el-4; therefore
// competitor must survive on el-0. No cached verdicts supply this evidence.
func TestHistoricalBindRetainsOtherKeyLoss(t *testing.T) {
	f := newCrossKeyHistory(t, false)
	data, bind, snapshot, latest := Tip{2, 1}, Tip{2, 2}, Tip{2, 3}, Tip{2, 4}
	state := &pb.ReducedEffect{Scalar: scalarInsertRaw([]byte("other"))}
	putCutEffects(t, f.engine, map[Tip]*pb.Effect{
		data: {Key: []byte("el-4"), TxnId: "winner", Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("other"))}},
		bind: {Key: []byte("el-4"), TxnId: "winner", ForkChoiceHash: []byte{0}, Deps: toPbRefs([]Tip{data}),
			Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
				Keys: []*pb.TransactionalBindEffect_KeyBind{{Key: []byte("el-4"), NewTip: toPbRef(data)}},
			}}},
		snapshot: {Key: []byte("el-4"), Deps: toPbRefs([]Tip{f.latest, bind}), Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{State: state}}},
		latest:   {Key: []byte("el-4"), Deps: toPbRefs([]Tip{snapshot}), Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{State: state}}},
	})
	f.engine.installIngestedTips("el-4", latest)
	got, err := f.engine.GetSnapshots(context.Background(), []string{"el-0", "el-4"})
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"wrong", "other"} {
		if value := string(got[i].GetScalar().GetRaw()); value != want {
			t.Errorf("key %d reconstructed %q, want %q", i, value, want)
		}
	}
}

// Ingestion must close each subscribed key's branch of a shared bind without
// merging their value DAGs. Exercise both a directly received bind and a bind
// discovered in ancestry, with individual fetches and streamed replication.
func TestBindIngestionClosesAllSubscribedKeys(t *testing.T) {
	for _, ancestor := range []bool{false, true} {
		for _, stream := range []bool{false, true} {
			for _, remote := range []bool{false, true} {
				t.Run(fmt.Sprintf("ancestor=%t/stream=%t/remote=%t", ancestor, stream, remote), func(t *testing.T) {
					log := newSnapshotLog()
					base0 := log.putEffect(&pb.Effect{Key: []byte("el-0"), Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("old0"))}})
					base4 := log.putEffect(&pb.Effect{Key: []byte("el-4"), Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("old4"))}})
					var keys []*pb.TransactionalBindEffect_KeyBind
					var deps []Tip
					for i, key := range []string{"el-0", "el-4"} {
						consumed := toPbRefs([]Tip{[]Tip{base0, base4}[i]})
						data := log.putEffect(&pb.Effect{
							Key: []byte(key), TxnId: "shared", Deps: consumed,
							Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("100"))},
						})
						deps = append(deps, data)
						keys = append(keys, &pb.TransactionalBindEffect_KeyBind{
							Key: []byte(key), NewTip: toPbRef(data), ConsumedTips: consumed,
						})
					}
					bindTip := log.putEffect(&pb.Effect{
						Key: []byte("el-0"), TxnId: "shared", Deps: toPbRefs(deps),
						Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{Keys: keys}},
					})
					bind, _ := log.effectCache.Get(bindTip)
					incomingTip, incoming := bindTip, bind
					if ancestor {
						incomingTip = log.putEffect(&pb.Effect{
							Key: []byte("el-0"), Deps: toPbRefs([]Tip{bindTip}),
							Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
						})
						incoming, _ = log.effectCache.Get(incomingTip)
					}
					loads := make(map[Tip]int)
					var b Broadcaster = &cutBroadcaster{load: func(tip Tip) ([]byte, error) {
						loads[tip]++
						eff, ok := log.effectCache.Get(tip)
						if !ok {
							t.Fatalf("unexpected dependency fetch %v", tip)
						}
						return wireEffect(eff), nil
					}}
					if stream {
						b = &ancestryBroadcaster{load: func(key string, root Tip, _ []pb.NodeID, have func(Tip) bool, receive func(Tip, []byte, bool) error) error {
							var err error
							log.effectCache.VisitAncestry(key, root, func(tip Tip, eff *pb.Effect, isRoot bool) bool {
								if have(tip) {
									return true
								}
								if eff == nil {
									t.Fatalf("unexpected missing source vertex %v", tip)
								}
								loads[tip]++
								err = receive(tip, wireEffect(eff), isRoot)
								return err == nil
							})
							return err
						}}
					}
					e := newTestEngine(b)
					t.Cleanup(func() { _ = e.Close() })
					preSubscribe(e, "el-0", "el-4")
					notify := &pb.OffsetNotify{Origin: toPbRef(incomingTip), EffectData: wireEffect(incoming)}
					if remote {
						if _, err := e.HandleRemote(notify); err != nil {
							t.Fatal(err)
						}
					} else if err := e.handleBackfill(notify); err != nil {
						t.Fatal(err)
					}
					for i, key := range []string{"el-0", "el-4"} {
						if !ancestor || i == 0 {
							requireCutTips(t, e.index.Contains(key).Tips(), []Tip{incomingTip})
						}
						data := r(bind.GetTxnBind().GetKeys()[i].GetNewTip())
						base := []Tip{base0, base4}[i]
						for _, tip := range []Tip{data, base} {
							if _, ok := e.effectCache.Get(tip); !ok || loads[tip] != 1 {
								t.Errorf("%s dependency %v resident=%t fetches=%d, want true/1", key, tip, ok, loads[tip])
							}
						}
						d := newDag(e, key, "")
						if err := d.prepare([]Tip{bindTip}); err != nil {
							t.Fatal(err)
						}
						other := r(bind.GetTxnBind().GetKeys()[1-i].GetNewTip())
						if d.nodes[other] != nil {
							t.Errorf("%s DAG includes the other key's data %v", key, other)
						}
					}
				})
			}
		}
	}
}
