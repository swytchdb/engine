package effects

import (
	"errors"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

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
