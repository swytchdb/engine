package effects

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
)

type ancestryBroadcaster struct {
	mockBroadcaster

	load func(string, Tip, []pb.NodeID, func(Tip) bool, func(Tip, []byte, bool) error) error
}

func (b *ancestryBroadcaster) FetchAncestry(key string, root Tip, peers []pb.NodeID, have func(Tip) bool, receive func(Tip, []byte, bool) error) error {
	return b.load(key, root, peers, have, receive)
}

func TestAncestryConcurrentRootsJoinActiveDownload(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newSnapshotEngine(nil)
		defer func() { _ = e.Close() }()

		preSubscribe(e, "k")

		root, dep := Tip{7, 2}, Tip{7, 1}
		release := make(chan struct{})

		var calls atomic.Int32

		e.broadcaster = &ancestryBroadcaster{load: func(_ string, tip Tip, _ []pb.NodeID, _ func(Tip) bool, receive func(Tip, []byte, bool) error) error {
			calls.Add(1)

			if tip != root {
				t.Errorf("duplicate download for %v", tip)
			}

			<-release

			if err := receive(root, wireEffect(cutEffect(root, dep)), false); err != nil {
				return err
			}

			return receive(dep, wireEffect(cutEffect(dep)), false)
		}}

		var wg sync.WaitGroup

		fetch := func(tip Tip) {
			wg.Go(func() {
				if _, err := e.fetchFrontierAncestry("k", tip); err != nil {
					t.Error(err)
				}
			})
		}
		fetch(root)
		synctest.Wait()

		for i := range 32 {
			if i%2 == 0 {
				fetch(root)
			} else {
				fetch(dep)
			}
		}

		synctest.Wait()

		if calls.Load() != 1 {
			t.Fatalf("concurrent requests opened %d downloads", calls.Load())
		}

		close(release)
		wg.Wait()

		if calls.Load() != 1 {
			t.Fatalf("joined dependency redownloaded: %d calls", calls.Load())
		}
	})
}

func TestAncestryFailedDownloadCanRetry(t *testing.T) {
	e := newSnapshotEngine(nil)
	defer func() { _ = e.Close() }()

	preSubscribe(e, "k")

	root := Tip{7, 1}
	failure := errors.New("transport unavailable")
	calls := 0

	e.broadcaster = &ancestryBroadcaster{load: func(_ string, _ Tip, _ []pb.NodeID, _ func(Tip) bool, receive func(Tip, []byte, bool) error) error {
		calls++
		if calls == 1 {
			return failure
		}

		return receive(root, wireEffect(cutEffect(root)), false)
	}}
	if _, err := e.fetchFrontierAncestry("k", root); !errors.Is(err, failure) {
		t.Fatal(err)
	}

	if _, err := e.fetchFrontierAncestry("k", root); err != nil {
		t.Fatal(err)
	}

	if calls != 2 {
		t.Fatalf("failed transfer was cached: %d calls", calls)
	}
}

func TestAncestryPeersUseKeyEvidenceAndMembership(t *testing.T) {
	e := newSnapshotEngine(nil)
	defer func() { _ = e.Close() }()

	preSubscribe(e, "k")
	e.index.Insert("k", nil, keytrie.NewTipSet(Tip{1, 1}))

	e.broadcaster = &mockBroadcaster{peerIDs: []pb.NodeID{10, 11, 12, 13, 15}}
	for _, peer := range []pb.NodeID{10, 11, 13, 15} {
		e.getOrCreatePeerFilter(peer).hasBulk = true
	}

	e.peerFilterAdd(13, "k")
	e.addPeerSubscriber("k", 11)
	e.addPeerSubscriber("k", 14) // removed member, stale leaf subscriber

	got := e.ancestryPeers("k", Tip{15, 1})
	if len(got) != 2 || got[0] == got[1] || !slices.Contains([]pb.NodeID{15, 11, 12, 13}, got[0]) || !slices.Contains([]pb.NodeID{15, 11, 12, 13}, got[1]) {
		t.Fatalf("holders = %v", got)
	}
}

func TestAncestryStreamFeedsExistingCutDiscoveryAndBoundaries(t *testing.T) {
	e := newSnapshotEngine(nil)
	defer func() { _ = e.Close() }()

	preSubscribe(e, "k")

	a, b := Tip{7, 4}, Tip{8, 1}
	putCutEffects(t, e, map[Tip]*pb.Effect{a: cutEffect(a, Tip{7, 3}), b: cutEffect(b, Tip{7, 1})})

	requests := []Tip{}
	e.broadcaster = &ancestryBroadcaster{load: func(_ string, root Tip, _ []pb.NodeID, _ func(Tip) bool, receive func(Tip, []byte, bool) error) error {
		requests = append(requests, root)
		for i := root[1]; i > 0; i-- {
			tip := Tip{7, i}

			var deps []Tip
			if i > 1 {
				deps = []Tip{{7, i - 1}}
			} else {
				deps = []Tip{{9, 1}}
			}

			if err := receive(tip, wireEffect(cutEffect(tip, deps...)), false); err != nil {
				return err
			}
		}

		return receive(Tip{9, 1}, nil, false)
	}}
	got, complete, err := e.closeIngestedFrontier("k", []Tip{a, b}, make(map[Tip]error))
	requireCutTips(t, got, []Tip{a, b})

	if err != nil || !complete {
		t.Fatalf("closure complete=%t error=%v", complete, err)
	}

	if len(requests) != 1 || requests[0] != (Tip{7, 3}) {
		t.Fatalf("ancestry requests = %v", requests)
	}
}

func TestAncestryRootEvidenceSuppressesSubsequentFetches(t *testing.T) {
	for _, alreadyResident := range []bool{false, true} {
		t.Run(fmt.Sprintf("resident=%t", alreadyResident), func(t *testing.T) {
			e := newSnapshotEngine(nil)
			defer func() { _ = e.Close() }()

			preSubscribe(e, "k")

			source := newVertexPool()
			cut, middle, tip := Tip{7, 2}, Tip{7, 3}, Tip{7, 4}
			snapshot := cutEffect(cut, Tip{7, 1})

			snapshot.Kind = &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{State: &pb.ReducedEffect{}}}
			for id, eff := range map[Tip]*pb.Effect{cut: snapshot, middle: cutEffect(middle, cut)} {
				if err := source.Put(id, eff); err != nil {
					t.Fatal(err)
				}
			}

			source.getVertex(cut).markRoot()
			putCutEffects(t, e, map[Tip]*pb.Effect{tip: cutEffect(tip, middle)})

			if alreadyResident {
				putCutEffects(t, e, map[Tip]*pb.Effect{cut: snapshot})
			}

			calls, records := 0, 0

			e.broadcaster = &ancestryBroadcaster{load: func(key string, root Tip, _ []pb.NodeID, have func(Tip) bool, receive func(Tip, []byte, bool) error) error {
				calls++

				var err error

				source.VisitAncestry(key, root, func(id Tip, eff *pb.Effect, isRoot bool) bool {
					records++

					var data []byte
					if eff != nil && !have(id) {
						data = wireEffect(eff)
					}

					err = receive(id, data, isRoot)

					return err == nil
				})

				return err
			}}
			for range 2 {
				got, complete, err := e.closeIngestedFrontier("k", []Tip{tip}, make(map[Tip]error))
				requireCutTips(t, got, []Tip{tip})

				if err != nil || !complete {
					t.Fatalf("complete=%t err=%v", complete, err)
				}
			}

			if calls != 1 || records != 3 {
				t.Fatalf("requests=%d records=%d", calls, records)
			}

			if !e.effectCache.getVertex(cut).root() {
				t.Fatal("remote root evidence was lost")
			}

			if e.effectCache.getVertex(tip).root() || e.effectCache.getVertex(middle).root() {
				t.Fatal("marked history above the cut as root")
			}
		})
	}
}

// A reclamation boundary suppresses fetches, not resident causal evidence.
func TestAncestryRootStillProcessesResidentHistory(t *testing.T) {
	for _, rootFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("rootFirst=%t", rootFirst), func(t *testing.T) {
			e := newSnapshotEngine(nil)
			defer func() { _ = e.Close() }()

			cut, middle, old, missing := Tip{7, 4}, Tip{7, 3}, Tip{7, 2}, Tip{7, 1}
			snapshot := cutEffect(cut, middle)

			snapshot.Kind = &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{State: &pb.ReducedEffect{}}}
			if rootFirst {
				snapshot.ForkChoiceHash = []byte{0}
			}

			putCutEffects(t, e, map[Tip]*pb.Effect{
				cut: snapshot, middle: cutEffect(middle, old), old: cutEffect(old, missing),
			})
			e.effectCache.getVertex(cut).markRoot()

			e.broadcaster = &cutBroadcaster{load: func(tip Tip) ([]byte, error) {
				t.Errorf("fetched below root: %v", tip)

				return nil, ErrEffectNotFound
			}}
			for range 2 {
				got, complete, err := e.closeIngestedFrontier("k", []Tip{cut, old}, make(map[Tip]error))
				requireCutTips(t, got, []Tip{cut})

				if err != nil || !complete {
					t.Fatalf("complete=%t err=%v", complete, err)
				}
			}

			seen := make(map[Tip]bool)

			e.effectCache.VisitAncestry("k", cut, func(tip Tip, eff *pb.Effect, _ bool) bool {
				if eff != nil {
					seen[tip] = true
				}

				return true
			})

			if !seen[cut] || !seen[middle] || !seen[old] {
				t.Fatalf("stream omitted resident history: %v", seen)
			}
		})
	}
}
