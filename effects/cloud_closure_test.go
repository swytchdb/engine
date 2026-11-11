package effects

import (
	"errors"
	"slices"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

// Synthetic stream ordering: X fails first, then Y supplies newer boundary
// evidence for X. A usable snapshot alone must not erase availability errors.
func TestInstallCloudTipsRetiresResolvedBoundaryErrors(t *testing.T) {
	for _, mode := range []string{"absent", "root", "resident", "unresolved", "interrupted", "unrelated-failure"} {
		t.Run(mode, func(t *testing.T) {
			e := newSnapshotEngine(nil)
			defer func() { _ = e.Close() }()

			preSubscribe(e, "k")

			x, unrelated, y := Tip{7, 1}, Tip{8, 1}, Tip{9, 1}
			xFailure := errors.New("X transport unavailable")
			otherFailure := errors.New("unrelated transport unavailable")
			tips := []Tip{x, y}
			wantCalls := []Tip{x, y}

			if mode == "unrelated-failure" {
				tips = append(tips, unrelated)
				wantCalls = []Tip{x, unrelated, y}
			}

			snapshot := cutEffect(y, x)

			snapshot.Kind = &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{State: &pb.ReducedEffect{
				Scalar: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 10),
			}}}
			if mode == "unresolved" {
				// Y's complete history supplies no evidence about concurrent X.
				snapshot.Deps = nil
			}

			var calls []Tip

			e.broadcaster = &ancestryBroadcaster{load: func(_ string, root Tip, _ []pb.NodeID, _ func(Tip) bool, receive func(Tip, []byte, bool) error) error {
				calls = append(calls, root)
				switch root {
				case x:
					return xFailure
				case unrelated:
					return otherFailure
				case y:
					if err := receive(y, wireEffect(snapshot), mode == "root"); err != nil {
						return err
					}
				default:
					t.Fatalf("unexpected ancestry root: %v", root)
				}

				switch mode {
				case "absent", "unrelated-failure":
					return receive(x, nil, false)
				case "resident":
					return receive(x, wireEffect(cutEffect(x)), false)
				case "interrupted":
					// No missing-boundary report is published by an incomplete stream.
					return errors.New("Y stream interrupted")
				default:
					return nil
				}
			}}

			err := e.InstallCloudTips("k", tips, nil)

			if !slices.Equal(calls, wantCalls) {
				t.Fatalf("fetch order = %v, want %v", calls, wantCalls)
			}

			switch mode {
			case "unresolved", "interrupted":
				if !errors.Is(err, xFailure) || !errors.Is(err, ErrCloudUnavailable) {
					t.Fatalf("unresolved transport failure was lost: %v", err)
				}
			case "unrelated-failure":
				if errors.Is(err, xFailure) || !errors.Is(err, otherFailure) || !errors.Is(err, ErrCloudUnavailable) {
					t.Fatalf("only the resolved boundary error should be retired: %v", err)
				}
			default:
				if err != nil {
					t.Fatalf("resolved boundary prevented publication: %v", err)
				}

				requireFrontier(t, e, y)
			}

			if err != nil && e.index.Contains("k").Len() != 0 {
				t.Fatal("cloud published despite unresolved availability")
			}
		})
	}
}

func TestInstallCloudTipsRetainsClosureWithoutReconstruction(t *testing.T) {
	e := newSnapshotEngine(nil)
	defer func() { _ = e.Close() }()

	preSubscribe(e, "k")
	e.broadcaster = &holedFetchBroadcaster{}
	tip := Tip{7, 2}
	eff := frontierEffect(2, 1)

	err := e.InstallCloudTips("k", []Tip{tip}, []CloudEffect{{Tip: tip, Eff: eff}})
	if err != nil {
		t.Fatalf("closure should exhaust the absent dependency: %v", err)
	}

	requireFrontier(t, e, tip)

	if err := newDag(e, "k", "").prepare([]Tip{tip}); !errors.Is(err, errEffectNotResident) {
		t.Fatalf("reconstruction must detect its missing dependency: %v", err)
	}
}

func TestInstallCloudTipsConsumesLocalAncestor(t *testing.T) {
	e := newSnapshotEngine(nil)
	defer func() { _ = e.Close() }()

	preSubscribe(e, "k")

	ancestor, tip := Tip{7, 1}, Tip{7, 2}
	if err := e.effectCache.Put(ancestor, frontierEffect(1)); err != nil {
		t.Fatal(err)
	}

	e.installTips("k", []Tip{ancestor})

	err := e.InstallCloudTips("k", []Tip{tip}, []CloudEffect{{Tip: tip, Eff: frontierEffect(2, 1)}})
	if err != nil {
		t.Fatal(err)
	}

	requireFrontier(t, e, tip)
}

func TestInstallCloudTipsFetchesBeyondSnapshot(t *testing.T) {
	e := newSnapshotEngine(nil)
	defer func() { _ = e.Close() }()

	preSubscribe(e, "k")

	bc := &frontierBroadcaster{
		fetchable: map[Tip][]byte{{7, 2}: wireEffect(frontierEffect(2, 1))},
		fetched:   make(map[Tip]bool),
	}
	e.broadcaster = bc
	tip := Tip{7, 3}
	eff := frontierEffect(3, 2)
	eff.Kind = &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{State: &pb.ReducedEffect{
		Scalar: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 10),
	}}}

	err := e.InstallCloudTips("k", []Tip{tip}, []CloudEffect{{Tip: tip, Eff: eff}})
	if err != nil {
		t.Fatal(err)
	}

	if !bc.fetched[Tip{7, 2}] || !bc.fetched[Tip{7, 1}] {
		t.Fatalf("closure did not exhaust snapshot ancestry: %v", bc.fetched)
	}

	requireFrontier(t, e, tip)
}

func TestInstallCloudTipsRetriesStreamingClosure(t *testing.T) {
	e := newSnapshotEngine(nil)
	defer func() { _ = e.Close() }()

	preSubscribe(e, "k")

	tip, dep := Tip{7, 3}, Tip{7, 2}
	failure := errors.New("ancestry stream interrupted")
	calls := 0
	e.broadcaster = &ancestryBroadcaster{load: func(key string, root Tip, _ []pb.NodeID, _ func(Tip) bool, receive func(Tip, []byte, bool) error) error {
		calls++

		if key != "k" || root != dep {
			t.Fatalf("unexpected ancestry request: %q %v", key, root)
		}

		if calls == 1 {
			return failure
		}

		return receive(dep, wireEffect(frontierEffect(2, 1)), true)
	}}

	sidecar := []CloudEffect{{Tip: tip, Eff: frontierEffect(3, 2)}}
	if err := e.InstallCloudTips("k", []Tip{tip}, sidecar); !errors.Is(err, failure) || !errors.Is(err, ErrCloudUnavailable) {
		t.Fatalf("transport failure was lost: %v", err)
	}

	if err := e.InstallCloudTips("k", []Tip{tip}, sidecar); err != nil {
		t.Fatal(err)
	}

	if calls != 2 || !e.effectCache.getVertex(dep).root() {
		t.Fatalf("stream retry did not retain the remote boundary: calls=%d", calls)
	}

	requireFrontier(t, e, tip)
}
