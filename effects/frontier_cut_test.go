package effects

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
)

type cutBroadcaster struct {
	mockBroadcaster

	load func(Tip) ([]byte, error)
}

func (b *cutBroadcaster) FetchFromAny(ref *pb.EffectRef, _ FetchHint) ([]byte, error) {
	return b.load(r(ref))
}

func cutEffect(tip Tip, deps ...Tip) *pb.Effect {
	return &pb.Effect{
		Key: []byte("k"), NodeId: tip[0], Deps: toPbRefs(deps),
		ForkChoiceHash: []byte{byte(tip[0]), byte(tip[1])},
		Kind:           &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	}
}

func putCutEffects(t *testing.T, e *Engine, effects map[Tip]*pb.Effect) {
	t.Helper()

	for tip, eff := range effects {
		if err := e.effectCache.Put(tip, eff); err != nil {
			t.Fatal(err)
		}
	}
}

func requireCutTips(t *testing.T, got, want []Tip) {
	t.Helper()

	set := keytrie.NewTipSet(got...)
	if len(got) != len(want) || !set.ContainsAll(want) {
		t.Fatalf("frontier = %v, want %v", got, want)
	}
}

func TestFrontierCutDominanceStillResolvesBoundary(t *testing.T) {
	e := newSnapshotEngine(nil)
	defer func() { _ = e.Close() }()

	old, middle, newest, gone := Tip{7, 1}, Tip{8, 1}, Tip{9, 1}, Tip{6, 1}
	putCutEffects(t, e, map[Tip]*pb.Effect{
		old: cutEffect(old, gone), middle: cutEffect(middle, old), newest: cutEffect(newest, middle),
	})

	transport := errors.New("peer unavailable")
	for _, failure := range []error{ErrEffectNotFound, ErrCDNBlobMissing, transport} {
		loads := 0
		e.broadcaster = &cutBroadcaster{load: func(tip Tip) ([]byte, error) {
			loads++

			if tip != gone {
				t.Fatalf("unexpected boundary %v", tip)
			}

			return nil, failure
		}}

		for _, tips := range [][]Tip{{old, newest, middle}, {newest, middle, old}} {
			got, complete, err := e.closeIngestedFrontier("k", tips, make(map[Tip]error))
			requireCutTips(t, got, []Tip{newest})

			if complete != (failure != transport) || errors.Is(err, transport) != (failure == transport) {
				t.Fatalf("boundary %v: complete=%t error=%v", failure, complete, err)
			}
		}

		if loads != 2 {
			t.Fatalf("boundary %v fetched %d times, want once per closure", failure, loads)
		}
	}
}

func TestFrontierCutSingleTipLoadsRequiredHistory(t *testing.T) {
	for _, remote := range []bool{false, true} {
		t.Run(fmt.Sprintf("remote=%t", remote), func(t *testing.T) {
			e := newSnapshotEngine(nil)
			defer func() { _ = e.Close() }()

			preSubscribe(e, "k")

			loads := make(map[Tip]int)
			e.broadcaster = &cutBroadcaster{load: func(tip Tip) ([]byte, error) {
				loads[tip]++
				switch tip {
				case (Tip{7, 2}):
					return wireEffect(frontierEffect(2, 1)), nil
				case (Tip{7, 1}):
					return wireEffect(frontierEffect(1)), nil
				default:
					t.Fatalf("unexpected fetch %v", tip)

					return nil, ErrEffectNotFound
				}
			}}
			notify := &pb.OffsetNotify{Origin: toPbRef(Tip{7, 3}), EffectData: wireEffect(frontierEffect(3, 2))}

			var err error
			if remote {
				_, err = e.HandleRemote(notify)
			} else {
				err = e.handleBackfill(notify)
			}

			if err != nil {
				t.Fatal(err)
			}

			requireFrontier(t, e, Tip{7, 3})

			for _, tip := range []Tip{{7, 1}, {7, 2}} {
				if _, resident := e.effectCache.Get(tip); !resident || loads[tip] != 1 {
					t.Fatalf("after ingestion: dependency %v resident=%t loads=%d", tip, resident, loads[tip])
				}
			}

			e.broadcaster = &cutBroadcaster{load: func(tip Tip) ([]byte, error) {
				t.Fatalf("reconstruction fetched %v after ingestion", tip)

				return nil, ErrEffectNotFound
			}}

			state, _, err := e.reconstruct("k", []Tip{{7, 3}}, "", false)
			if err != nil || state.GetScalar().GetIntVal() != 3 {
				t.Fatalf("reconstruction: state=%v error=%v", state, err)
			}
		})
	}
}

func TestFrontierCutDiscoversUnrelatedRemoteHistories(t *testing.T) {
	e := newSnapshotEngine(nil)
	defer func() { _ = e.Close() }()

	a, b := Tip{7, 3}, Tip{8, 3}
	putCutEffects(t, e, map[Tip]*pb.Effect{
		a: cutEffect(a, Tip{7, 2}), b: cutEffect(b, Tip{8, 2}),
	})

	fetched := make(map[Tip]int)
	e.broadcaster = &cutBroadcaster{load: func(tip Tip) ([]byte, error) {
		fetched[tip]++
		if tip[1] == 0 {
			return nil, ErrEffectNotFound
		}

		return wireEffect(cutEffect(tip, Tip{tip[0], tip[1] - 1})), nil
	}}

	got, complete, err := e.closeIngestedFrontier("k", []Tip{b, a}, make(map[Tip]error))
	requireCutTips(t, got, []Tip{a, b})

	if !complete || err != nil {
		t.Fatalf("exhausted separate regions: complete=%t error=%v", complete, err)
	}

	for _, node := range []uint64{7, 8} {
		for id := range uint64(3) {
			if got := fetched[Tip{node, id}]; got != 1 {
				t.Fatalf("boundary %v fetched %d times, want once", Tip{node, id}, got)
			}
		}
	}
}

func TestFrontierCutResidentPhasePrecedesRemoteLoads(t *testing.T) {
	e := newSnapshotEngine(nil)
	defer func() { _ = e.Close() }()

	a, b, x, y := Tip{7, 3}, Tip{8, 3}, Tip{7, 2}, Tip{8, 2}
	putCutEffects(t, e, map[Tip]*pb.Effect{
		a: cutEffect(a, x), x: cutEffect(x, Tip{7, 1}),
		b: cutEffect(b, y), y: cutEffect(y, Tip{8, 1}),
	})
	e.broadcaster = &cutBroadcaster{load: func(Tip) ([]byte, error) {
		for dep, parent := range map[Tip]Tip{x: a, y: b} {
			v := e.effectCache.getVertex(dep)
			if got, ok := v.firstParent("k"); !ok || got != parent {
				t.Fatalf("remote load began before resident edge %v -> %v was visited", parent, dep)
			}
		}

		return nil, ErrEffectNotFound
	}}
	e.installIngestedTips("k", a, b)
	requireFrontier(t, e, a, b)
}

func TestFrontierCutDownwardEncounterDiscoversEstablishedCut(t *testing.T) {
	e := newSnapshotEngine(nil)
	defer func() { _ = e.Close() }()

	x, middle, established, separate := Tip{7, 1}, Tip{7, 2}, Tip{7, 3}, Tip{9, 1}
	putCutEffects(t, e, map[Tip]*pb.Effect{
		x: cutEffect(x), middle: cutEffect(middle, x),
		established: cutEffect(established, middle), separate: cutEffect(separate),
	})
	// Reverse links arise by establishing this region, not by fabricating
	// arbitrary upward edges in the fixture.
	e.installIngestedTips("k", established, separate)
	requireFrontier(t, e, established, separate)

	incoming, gap := Tip{8, 2}, Tip{8, 1}
	putCutEffects(t, e, map[Tip]*pb.Effect{incoming: cutEffect(incoming, gap)})

	loads := 0
	e.broadcaster = &cutBroadcaster{load: func(tip Tip) ([]byte, error) {
		loads++

		if tip != gap {
			t.Fatalf("unexpected fetch %v", tip)
		}

		return wireEffect(cutEffect(gap, x)), nil
	}}

	// Reconcile the supplied unknown region. Reaching X downward discovers
	// the previously established tip through its resident reverse chain.
	got, complete, err := e.closeIngestedFrontier("k", []Tip{incoming, separate}, make(map[Tip]error))
	requireCutTips(t, got, []Tip{incoming, established, separate})

	if !complete || err != nil || loads != 1 {
		t.Fatalf("joined history: complete=%t error=%v loads=%d", complete, err, loads)
	}
}

func TestFrontierCutReclaimedParentIsAnEstablishedJoin(t *testing.T) {
	e := newSnapshotEngine(nil)
	defer func() { _ = e.Close() }()

	x, middle, old, current, separate := Tip{7, 1}, Tip{7, 2}, Tip{7, 3}, Tip{7, 4}, Tip{9, 1}
	putCutEffects(t, e, map[Tip]*pb.Effect{
		x: cutEffect(x), middle: cutEffect(middle, x), old: cutEffect(old, middle),
		separate: cutEffect(separate),
	})
	e.installIngestedTips("k", old, separate)
	putCutEffects(t, e, map[Tip]*pb.Effect{current: cutEffect(current, old)})
	e.installIngestedTips("k", current)
	requireFrontier(t, e, current, separate)

	// Retire payloads from the established region while X survives. Its
	// reverse link remains evidence; it does not own the retired parent.
	for _, tip := range []Tip{middle, old} {
		e.effectCache.decref(tip)
	}

	e.effectCache.drainReclaim()

	incoming := Tip{8, 1}
	putCutEffects(t, e, map[Tip]*pb.Effect{incoming: cutEffect(incoming, x)})
	e.broadcaster = &cutBroadcaster{load: func(tip Tip) ([]byte, error) {
		if tip != old {
			t.Fatalf("reverse navigation fetched reclaimed parent %v", tip)
		}

		return nil, ErrEffectNotFound
	}}

	got, complete, err := e.closeIngestedFrontier("k", []Tip{incoming, current, separate}, make(map[Tip]error))
	requireCutTips(t, got, []Tip{incoming, current, separate})

	if !complete || err != nil {
		t.Fatalf("reclaimed join treated as unresolved: complete=%t error=%v", complete, err)
	}
}

func TestFrontierCutSharedBindNavigationStaysOnItsKey(t *testing.T) {
	e := newSnapshotEngine(nil)
	defer func() { _ = e.Close() }()

	kRoot, otherRoot, bind := Tip{7, 1}, Tip{8, 1}, Tip{9, 1}
	kTip, otherTip, separate := Tip{7, 2}, Tip{8, 2}, Tip{10, 1}
	shared := cutEffect(bind)
	shared.Kind = &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
		Keys: []*pb.TransactionalBindEffect_KeyBind{
			{Key: []byte("k"), NewTip: toPbRef(kRoot)},
			{Key: []byte("other"), NewTip: toPbRef(otherRoot)},
		},
	}}
	otherBase, otherParent := cutEffect(otherRoot), cutEffect(otherTip, bind)
	otherBase.Key, otherParent.Key = []byte("other"), []byte("other")
	putCutEffects(t, e, map[Tip]*pb.Effect{
		kRoot: cutEffect(kRoot), otherRoot: otherBase, bind: shared,
		kTip: cutEffect(kTip, bind), otherTip: otherParent, separate: cutEffect(separate),
	})
	// Establish the other key first, so its reverse link is stored first.
	e.installIngestedTips("other", otherTip, otherRoot)
	e.installIngestedTips("k", kTip, separate)
	e.effectCache.decref(otherTip)
	e.effectCache.drainReclaim()

	incoming := Tip{11, 1}
	putCutEffects(t, e, map[Tip]*pb.Effect{incoming: cutEffect(incoming, bind)})
	e.broadcaster = &cutBroadcaster{load: func(tip Tip) ([]byte, error) {
		t.Fatalf("key-scoped resident closure unexpectedly fetched %v", tip)

		return nil, ErrEffectNotFound
	}}
	got, complete, err := e.closeIngestedFrontier("k", []Tip{incoming, separate}, make(map[Tip]error))
	requireCutTips(t, got, []Tip{incoming, kTip, separate})

	if !complete || err != nil {
		t.Fatalf("shared-bind closure: complete=%t error=%v", complete, err)
	}
}

func TestFrontierCutSnapshotLabelsPreserveDirectedClosure(t *testing.T) {
	for mask := range 8 {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("snapshots=%03b/reverse=%t", mask, reverse), func(t *testing.T) {
				e := newSnapshotEngine(nil)
				defer func() { _ = e.Close() }()

				x, a, middle, b := Tip{7, 1}, Tip{8, 1}, Tip{9, 1}, Tip{9, 2}

				nodes := map[Tip]*pb.Effect{
					x: cutEffect(x), a: cutEffect(a, x), middle: cutEffect(middle, x), b: cutEffect(b, middle),
				}
				for bit, tip := range []Tip{x, middle, b} {
					if mask&(1<<bit) != 0 {
						nodes[tip].Kind = &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{State: &pb.ReducedEffect{}}}
					}
				}

				putCutEffects(t, e, nodes)

				tips := []Tip{a, middle, b}
				if reverse {
					slices.Reverse(tips)
				}

				e.installIngestedTips("k", tips...)
				// B consumes middle. Meeting at X does not consume A or B.
				requireFrontier(t, e, a, b)
			})
		}
	}
}

func TestFrontierCutAbsenceClosesBoundaryButTransportDoesNot(t *testing.T) {
	transport := errors.New("peer unavailable")
	for _, failure := range []error{ErrEffectNotFound, ErrCDNBlobMissing, transport} {
		t.Run(failure.Error(), func(t *testing.T) {
			e := newSnapshotEngine(nil)
			defer func() { _ = e.Close() }()

			a, b, shared, other := Tip{7, 2}, Tip{8, 2}, Tip{7, 1}, Tip{8, 1}
			putCutEffects(t, e, map[Tip]*pb.Effect{
				a: cutEffect(a, shared), b: cutEffect(b, shared, other),
			})

			loads := make(map[Tip]int)
			e.broadcaster = &cutBroadcaster{load: func(tip Tip) ([]byte, error) {
				loads[tip]++
				if tip == shared {
					return nil, ErrCDNBlobMissing
				}

				return nil, failure
			}}
			got, complete, err := e.closeIngestedFrontier("k", []Tip{a, b}, make(map[Tip]error))
			requireCutTips(t, got, []Tip{a, b})

			if complete != (failure != transport) || errors.Is(err, transport) != (failure == transport) {
				t.Fatalf("complete=%t error=%v for %v", complete, err, failure)
			}

			if errors.Is(err, ErrCDNBlobMissing) || loads[shared] != 1 || loads[other] != 1 {
				t.Fatalf("boundary outcomes not preserved: error=%v loads=%v", err, loads)
			}
		})
	}
}

func TestFrontierCutRetryResolvesUnavailableConnection(t *testing.T) {
	e := newSnapshotEngine(nil)
	defer func() { _ = e.Close() }()

	a, gap, b := Tip{7, 1}, Tip{8, 1}, Tip{8, 2}
	putCutEffects(t, e, map[Tip]*pb.Effect{a: cutEffect(a), b: cutEffect(b, gap)})

	available, calls := false, 0
	e.broadcaster = &cutBroadcaster{load: func(tip Tip) ([]byte, error) {
		calls++

		if tip != gap {
			t.Fatalf("unexpected fetch %v", tip)
		}

		if !available {
			return nil, errors.New("partitioned")
		}

		return wireEffect(cutEffect(gap, a)), nil
	}}
	e.installIngestedTips("k", a, b)
	requireFrontier(t, e, a, b)

	available = true

	e.installIngestedTips("k", b)
	requireFrontier(t, e, b)

	if calls != 2 {
		t.Fatalf("loads=%d, want one in each closure attempt", calls)
	}
}

func TestFrontierCutPublicationRetryIncludesConcurrentTip(t *testing.T) {
	e := newSnapshotEngine(nil)
	defer func() { _ = e.Close() }()

	old, gap, incoming, concurrent := Tip{7, 1}, Tip{8, 1}, Tip{8, 2}, Tip{9, 1}
	putCutEffects(t, e, map[Tip]*pb.Effect{
		old: cutEffect(old), incoming: cutEffect(incoming, gap), concurrent: cutEffect(concurrent),
	})
	e.installIngestedTips("k", old)

	loads := 0
	e.broadcaster = &cutBroadcaster{load: func(tip Tip) ([]byte, error) {
		loads++

		if tip != gap {
			t.Fatalf("unexpected fetch %v", tip)
		}

		before := e.index.Contains("k")
		if _, ok := e.index.Insert("k", before, keytrie.NewTipSet(old, concurrent)); !ok {
			t.Fatal("concurrent publication failed")
		}

		return wireEffect(cutEffect(gap, old)), nil
	}}
	e.installIngestedTips("k", incoming)
	requireFrontier(t, e, incoming, concurrent)

	if loads != 1 {
		t.Fatalf("CAS retry loaded the same immutable dependency %d times", loads)
	}

	before := e.index.Contains("k")
	e.installIngestedTips("k", incoming)

	if e.index.Contains("k") != before {
		t.Fatal("unchanged frontier publication invalidated its identity")
	}
}

func TestFrontierCutSharedHistoryWorkRemainsLinear(t *testing.T) {
	e := newSnapshotEngine(nil)
	defer func() { _ = e.Close() }()

	const length = 1000
	for id := uint64(1); id <= length; id++ {
		tip := Tip{7, id}
		putCutEffects(t, e, map[Tip]*pb.Effect{tip: cutEffect(tip, Tip{7, id - 1})})
	}

	a, b := Tip{8, 1}, Tip{9, 1}
	putCutEffects(t, e, map[Tip]*pb.Effect{
		a: cutEffect(a, Tip{7, length}), b: cutEffect(b, Tip{7, length}),
	})

	loads := 0

	e.broadcaster = &cutBroadcaster{load: func(tip Tip) ([]byte, error) {
		loads++

		if tip != (Tip{7, 0}) {
			t.Fatalf("unexpected fetch %v", tip)
		}

		return nil, ErrEffectNotFound
	}}
	for attempt := range 3 {
		before, _, _ := e.effectCache.Stats()
		e.installIngestedTips("k", a, b)
		requireFrontier(t, e, a, b)
		after, _, _ := e.effectCache.Stats()
		// A repeated closure sees established reverse links throughout the
		// chain. Following each one to the tip would make work quadratic.
		if after-before > 30*length || loads != attempt+1 {
			t.Fatalf("attempt=%d lookups=%d loads=%d", attempt, after-before, loads)
		}
	}
}

func TestFrontierCutIngestionMatchesReachability(t *testing.T) {
	// Each graph is ingested in two phases. The first publication establishes
	// reverse links naturally; the second reconciles another advertised region.
	// The oracle uses only immutable forward edges and candidate reachability.
	const n = 5
	for graph := range 1 << (n * (n - 1) / 2) {
		var (
			reach [n][n]bool
			deps  [n][]Tip
			tips  [n]Tip
		)

		bit := 0

		for i := range n {
			// Distinct origins avoid asserting any cross-vertex ordering from
			// one origin's monotonic counters. Only the edges prove ancestry.
			tips[i] = Tip{uint64(i + 7), 1}
			for j := range i {
				if graph&(1<<bit) != 0 {
					reach[i][j] = true
					deps[i] = append(deps[i], tips[j])
				}

				bit++
			}
		}

		for k := range n {
			for i := range n {
				for j := range n {
					reach[i][j] = reach[i][j] || reach[i][k] && reach[k][j]
				}
			}
		}

		for _, reverseFork := range []bool{false, true} {
			e := newSnapshotEngine(nil)

			for i, tip := range tips {
				eff := cutEffect(tip, deps[i]...)

				fork := i
				if reverseFork {
					fork = n - i
				}

				eff.ForkChoiceHash = []byte{byte(fork)}
				putCutEffects(t, e, map[Tip]*pb.Effect{tip: eff})
			}

			e.installIngestedTips("k", tips[0], tips[2])

			for phase, candidates := range [][]int{{0, 2}, {0, 2, 4}} {
				if phase == 1 {
					e.installIngestedTips("k", tips[4])
				}

				var want []Tip

				for _, candidate := range candidates {
					consumed := false
					for _, other := range candidates {
						consumed = consumed || reach[other][candidate]
					}

					if !consumed {
						want = append(want, tips[candidate])
					}
				}

				got := e.index.Contains("k")
				if got.Len() != len(want) || !got.ContainsAll(want) {
					_ = e.Close()

					t.Fatalf("graph=%010b reverseFork=%t phase=%d frontier=%v want=%v", graph, reverseFork, phase, got.Tips(), want)
				}
			}

			_ = e.Close()
		}
	}
}
