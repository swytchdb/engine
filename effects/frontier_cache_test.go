package effects

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/cespare/xxhash/v2"
	clox "github.com/swytchdb/engine/cache"
	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
)

func enableClosureCache(e *Engine) {
	if e.closedFrontiers != nil {
		e.closedFrontiers.Close()
	}

	e.closedFrontiers = clox.NewCloxCache[uint64, closedFrontier](clox.ConfigFromCapacity(64))
}

func publishCountedClosure(t *testing.T, e *Engine, tips ...Tip) (int, error) {
	t.Helper()

	op := newClosureOperation(e, make(map[Tip]error))
	defer op.release()

	n := 0
	op.observe = func(_ Tip, ref *pb.EffectRef) {
		if ref == nil {
			n++
		}
	}
	_, _, err := op.publish(logicalAddress("k"), tips, true)

	return n, err
}

func TestClosureCacheIncrementalHistory(t *testing.T) {
	for _, n := range []int{64, 1024} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			e := newSnapshotEngine(nil)

			enableClosureCache(e)
			defer func() { _ = e.Close() }()

			base := Tip{7, 1}
			putCutEffects(t, e, map[Tip]*pb.Effect{base: cutEffect(base)})

			if _, err := publishCountedClosure(t, e, base); err != nil {
				t.Fatal(err)
			}

			total := 0

			for i := 1; i <= n; i++ {
				tip := Tip{7, uint64(i + 1)}

				eff := cutEffect(tip, Tip{7, uint64(i)})
				if i%8 == 0 {
					eff.Kind = &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{State: &pb.ReducedEffect{}}}
				}

				putCutEffects(t, e, map[Tip]*pb.Effect{tip: eff})

				walked, err := publishCountedClosure(t, e, tip)
				if err != nil {
					t.Fatal(err)
				}

				total += walked
			}

			if total != n {
				t.Fatalf("%d arrivals expanded %d vertices", n, total)
			}

			walked, err := publishCountedClosure(t, e, Tip{7, uint64(n + 1)})
			if err != nil || walked != 0 {
				t.Fatalf("unchanged closure: walked=%d err=%v", walked, err)
			}
		})
	}
}

func TestClosureCacheEvictionAndBranchFallback(t *testing.T) {
	e := newSnapshotEngine(nil)

	enableClosureCache(e)
	defer func() { _ = e.Close() }()

	base, left, right, merge := Tip{7, 1}, Tip{7, 2}, Tip{8, 1}, Tip{8, 2}
	putCutEffects(t, e, map[Tip]*pb.Effect{base: cutEffect(base), left: cutEffect(left, base), right: cutEffect(right, base), merge: cutEffect(merge, left, right)})

	for _, tip := range []Tip{base, left, right} {
		if _, err := publishCountedClosure(t, e, tip); err != nil {
			t.Fatal(err)
		}
	}

	requireFrontier(t, e, left, right)

	if _, err := publishCountedClosure(t, e, merge); err != nil {
		t.Fatal(err)
	}

	requireFrontier(t, e, merge)
	e.closedFrontiers.Evict(xxhash.Sum64String(logicalAddress("k")))

	walked, err := publishCountedClosure(t, e, merge)
	if err != nil || walked != 1 {
		t.Fatalf("eviction should reuse parent links: walked=%d err=%v", walked, err)
	}

	if _, err := publishCountedClosure(t, e, base); err != nil {
		t.Fatal(err)
	}

	requireFrontier(t, e, merge)
}

func TestClosureCacheDoesNotCertifyMissingHistory(t *testing.T) {
	for _, failure := range []error{ErrEffectNotFound, errors.New("offline")} {
		t.Run(failure.Error(), func(t *testing.T) {
			e := newSnapshotEngine(nil)

			enableClosureCache(e)
			defer func() { _ = e.Close() }()

			tip, missing := Tip{7, 2}, Tip{7, 1}
			putCutEffects(t, e, map[Tip]*pb.Effect{tip: cutEffect(tip, missing)})
			e.broadcaster = &cutBroadcaster{load: func(Tip) ([]byte, error) { return nil, failure }}

			_, _ = publishCountedClosure(t, e, tip)
			if _, ok := e.closedFrontiers.Get(xxhash.Sum64String(logicalAddress("k")), 0); ok {
				t.Fatal("missing history certified as established closure")
			}

			putCutEffects(t, e, map[Tip]*pb.Effect{missing: cutEffect(missing)})

			if _, err := publishCountedClosure(t, e, tip); err != nil {
				t.Fatal(err)
			}

			if _, ok := e.closedFrontiers.Get(xxhash.Sum64String(logicalAddress("k")), 0); !ok {
				t.Fatal("repaired closure was not cached")
			}
		})
	}
}

func TestClosureCacheReclamationAndFlushInvalidateProof(t *testing.T) {
	for _, flush := range []bool{false, true} {
		t.Run(fmt.Sprint(flush), func(t *testing.T) {
			e := newSnapshotEngine(nil)

			enableClosureCache(e)
			defer func() { _ = e.Close() }()

			base, tip := Tip{7, 1}, Tip{7, 2}
			putCutEffects(t, e, map[Tip]*pb.Effect{base: cutEffect(base), tip: cutEffect(tip, base)})

			if _, err := publishCountedClosure(t, e, tip); err != nil {
				t.Fatal(err)
			}

			if flush {
				if err := e.FlushIndex(); err != nil {
					t.Fatal(err)
				}
			} else {
				e.effectCache.decref(base)
				e.effectCache.drainReclaim()

				if _, ok := e.effectCache.Get(base); ok {
					t.Fatal("fixture did not reclaim ancestry")
				}
			}

			e.broadcaster = &cutBroadcaster{load: func(Tip) ([]byte, error) { return nil, errors.New("offline") }}

			walked, err := publishCountedClosure(t, e, tip)
			if flush && walked > 0 {
				return
			}

			if err == nil {
				t.Fatal("stale proof hid unavailable history")
			}
		})
	}
}

func TestClosureCacheRetainsCrossKeyFailure(t *testing.T) {
	e := newSnapshotEngine(nil)

	enableClosureCache(e)
	defer func() { _ = e.Close() }()

	preSubscribe(e, "k", "other")

	base, bind, missing := Tip{7, 1}, Tip{8, 1}, Tip{9, 1}
	putCutEffects(t, e, map[Tip]*pb.Effect{base: cutEffect(base)})

	if _, err := publishCountedClosure(t, e, base); err != nil {
		t.Fatal(err)
	}

	putCutEffects(t, e, map[Tip]*pb.Effect{bind: {
		Key: []byte("k"), TxnId: "cross-key", Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
			Keys: []*pb.TransactionalBindEffect_KeyBind{
				{Key: []byte("k"), NewTip: toPbRef(base)},
				{Key: []byte("other"), NewTip: toPbRef(missing)},
			},
		}},
	}})

	failure := errors.New("other key unavailable")

	e.broadcaster = &cutBroadcaster{load: func(Tip) ([]byte, error) { return nil, failure }}
	if _, err := publishCountedClosure(t, e, bind); !errors.Is(err, failure) {
		t.Fatalf("cached local closure hid cross-key failure: %v", err)
	}

	requireFrontier(t, e, base)
}

func TestClosureCacheHashCollisionIsAMiss(t *testing.T) {
	e := newSnapshotEngine(nil)

	enableClosureCache(e)
	defer func() { _ = e.Close() }()

	tip := Tip{7, 1}
	putCutEffects(t, e, map[Tip]*pb.Effect{tip: cutEffect(tip)})
	e.closedFrontiers.Put(xxhash.Sum64String(logicalAddress("k")), closedFrontier{key: "another key", tip: tip})

	if walked, err := publishCountedClosure(t, e, tip); err != nil || walked != 1 {
		t.Fatalf("hash collision reused another key's proof: walked=%d err=%v", walked, err)
	}
}

func TestClosureCacheNewReverseLinkFallsBack(t *testing.T) {
	e := newSnapshotEngine(nil)

	enableClosureCache(e)
	defer func() { _ = e.Close() }()

	base, successor, incoming := Tip{7, 1}, Tip{8, 1}, Tip{9, 1}
	putCutEffects(t, e, map[Tip]*pb.Effect{
		base: cutEffect(base), successor: cutEffect(successor, base), incoming: cutEffect(incoming, base),
	})

	if _, err := publishCountedClosure(t, e, base); err != nil {
		t.Fatal(err)
	}

	e.effectCache.addParent(e.effectCache.getVertex(base), successor, logicalAddress("k"))

	if walked, err := publishCountedClosure(t, e, incoming); err != nil || walked != 1 {
		t.Fatalf("new reverse link should avoid reopening history: walked=%d err=%v", walked, err)
	}

	requireFrontier(t, e, successor, incoming)
}

func TestClosureCacheReclamationDuringPublicationRetry(t *testing.T) {
	e := newSnapshotEngine(nil)

	enableClosureCache(e)
	defer func() { _ = e.Close() }()

	base, tip, incoming := Tip{7, 1}, Tip{7, 2}, Tip{7, 3}
	putCutEffects(t, e, map[Tip]*pb.Effect{
		base: cutEffect(base), tip: cutEffect(tip, base), incoming: cutEffect(incoming, tip),
	})

	if _, err := publishCountedClosure(t, e, tip); err != nil {
		t.Fatal(err)
	}

	failure := errors.New("reclaimed history unavailable")
	e.broadcaster = &cutBroadcaster{load: func(Tip) ([]byte, error) { return nil, failure }}

	op := newClosureOperation(e, make(map[Tip]error))
	defer op.release()

	attempts := 0

	op.cas = func(key string, old, next *keytrie.TipSet) (*keytrie.TipSet, bool) {
		attempts++
		if attempts == 1 {
			e.effectCache.decref(base)
			e.effectCache.drainReclaim()

			return old, false
		}

		return e.index.InsertInternal(key, old, next)
	}
	if _, complete, err := op.publish(logicalAddress("k"), []Tip{incoming}, true); !complete || err != nil {
		t.Fatalf("parent links did not survive cache invalidation: complete=%t err=%v", complete, err)
	}

	if !op.cacheDisabled || attempts != 2 {
		t.Fatalf("cache invalidation not exercised: disabled=%t attempts=%d", op.cacheDisabled, attempts)
	}

	requireFrontier(t, e, incoming)
}

func TestClosureCacheSuffixPreservesReverseNavigation(t *testing.T) {
	e := newSnapshotEngine(nil)

	enableClosureCache(e)
	defer func() { _ = e.Close() }()

	base, middle, successor, incoming := Tip{7, 1}, Tip{7, 2}, Tip{8, 1}, Tip{9, 1}
	putCutEffects(t, e, map[Tip]*pb.Effect{
		base: cutEffect(base), middle: cutEffect(middle, base),
		successor: cutEffect(successor, middle), incoming: cutEffect(incoming, middle),
	})
	// A shared vertex's sole reverse hint may belong to another key. The
	// same-key successor is discoverable only through the new suffix.
	e.effectCache.addParent(e.effectCache.getVertex(base), Tip{10, 1}, logicalAddress("other"))

	if _, err := publishCountedClosure(t, e, base); err != nil {
		t.Fatal(err)
	}

	e.effectCache.addParent(e.effectCache.getVertex(middle), successor, logicalAddress("k"))

	if _, err := publishCountedClosure(t, e, incoming); err != nil {
		t.Fatal(err)
	}

	requireFrontier(t, e, successor, incoming)
}

func TestClosureCacheMatchesUncachedFrontiers(t *testing.T) {
	for seed := range uint64(16) {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			cached, ordinary := newSnapshotEngine(nil), newSnapshotEngine(nil)
			if ordinary.closedFrontiers != nil {
				ordinary.closedFrontiers.Close()
				ordinary.closedFrontiers = nil
			}

			enableClosureCache(cached)
			defer func() { _ = cached.Close(); _ = ordinary.Close() }()

			random := rand.New(rand.NewPCG(seed, seed+1))

			for n := range 128 {
				tip := Tip{7, uint64(n + 1)}

				var deps []Tip
				if n != 0 {
					deps = append(deps, Tip{7, uint64(random.IntN(n) + 1)})
					if random.IntN(2) == 0 {
						deps = append(deps, Tip{7, uint64(random.IntN(n) + 1)})
					}
				}

				eff := cutEffect(tip, deps...)
				if n%7 == 0 {
					eff.Kind = &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{State: &pb.ReducedEffect{}}}
				}

				for _, e := range []*Engine{cached, ordinary} {
					putCutEffects(t, e, map[Tip]*pb.Effect{tip: eff})

					if _, err := publishCountedClosure(t, e, tip); err != nil {
						t.Fatal(err)
					}
				}

				if !tipsEqual(cached.index.Contains("k").Tips(), ordinary.index.Contains("k").Tips()) {
					t.Fatalf("arrival %d: cached frontier=%v ordinary=%v", n, cached.index.Contains("k").Tips(), ordinary.index.Contains("k").Tips())
				}
			}
		})
	}
}
