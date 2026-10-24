package effects

import (
	"sync"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

func TestReleaseChainRefs_SharedBindPins(t *testing.T) {
	for _, owned := range []bool{false, true} {
		name := "cache"
		if owned {
			name = "owned"
		}

		t.Run(name, func(t *testing.T) {
			e := newTestEngine(&mockBroadcaster{})
			tip := Tip{1, 10}

			eff := &pb.Effect{Key: []byte("a"), Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
				Keys: []*pb.TransactionalBindEffect_KeyBind{{Key: []byte("a")}, {Key: []byte("b")}},
			}}}
			if owned {
				e.effectCache.PutSized(tip, eff, 10)
			} else {
				e.effectCache.PutSizedCache(tip, eff, 10)
			}

			a, b := &leafState{}, &leafState{}
			d := &dag{topoOrder: []Tip{tip}}
			e.publishSubdag(a, d, []Tip{tip})
			e.publishSubdag(b, d, []Tip{tip})
			e.releaseChainRefs("a", []Tip{tip}, a)
			// A later eviction walk may reach the same resident ancestor.
			// It must not consume b's creation reference or cached DAG pin.
			e.releaseChainRefs("a", []Tip{tip}, &leafState{})
			e.reclaimUnreferenced()

			if _, ok := e.materializeSubdag(b.subdag.Load()); !ok {
				t.Fatal("evicting a reclaimed a vertex still pinned by b")
			}

			if owned {
				// Dropping b's read cache must still leave its creation ref.
				e.publishSubdag(b, &dag{}, nil)
				e.reclaimUnreferenced()

				if _, ok := e.effectCache.Get(tip); !ok {
					t.Fatal("repeated eviction of a consumed b's creation reference")
				}
			}

			e.releaseChainRefs("b", []Tip{tip}, b)
			e.reclaimUnreferenced()

			if e.effectCache.EntryCount() != 0 {
				t.Fatal("released bind leaked")
			}
		})
	}
}

func TestPutIngested_BindOwnsOnlySubscribedKeys(t *testing.T) {
	for _, key := range []string{"a", "b"} {
		t.Run(key, func(t *testing.T) {
			e := newTestEngine(&mockBroadcaster{})
			tip := Tip{1, 10}
			eff := &pb.Effect{Key: []byte("a"), Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
				Keys: []*pb.TransactionalBindEffect_KeyBind{{Key: []byte("a")}, {Key: []byte("b")}},
			}}}

			e.subscriptions.Store(key, &subscriptionState{})
			e.putIngested(tip, eff, 10)
			e.reclaimUnreferenced()

			if _, ok := e.effectCache.Get(tip); !ok {
				t.Fatal("bind on subscribed noncanonical key was reclaimed")
			}

			e.releaseChainRefs(key, []Tip{tip}, nil)
			e.reclaimUnreferenced()

			if e.effectCache.EntryCount() != 0 {
				t.Fatal("bind retained ownership for an unsubscribed key")
			}
		})
	}
}

func TestCreationRefs_ConcurrentReacquireAndRelease(t *testing.T) {
	e := newTestEngine(&mockBroadcaster{})
	tip := Tip{1, 10}
	eff := &pb.Effect{Key: []byte("a"), Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
		Keys: []*pb.TransactionalBindEffect_KeyBind{{Key: []byte("a")}, {Key: []byte("b")}},
	}}}
	e.effectCache.PutSizedCache(tip, eff, 10)

	if !e.effectCache.Incref(tip) {
		t.Fatal("failed to pin shared vertex")
	}

	var wg sync.WaitGroup
	for _, key := range []string{"a", "b"} {
		wg.Go(func() {
			owns := func(k []byte) bool { return string(k) == key }
			for range 1000 {
				e.effectCache.putSized(tip, eff, 10, owns)
				e.effectCache.putSized(tip, eff, 10, owns)
				e.effectCache.decrefChainNode(key, tip)
				e.effectCache.decrefChainNode(key, tip)
			}
		})
	}

	wg.Wait()
	e.reclaimUnreferenced()

	if _, ok := e.effectCache.Get(tip); !ok {
		t.Fatal("creation releases consumed the independent pin")
	}

	e.effectCache.Decref(tip)
	e.reclaimUnreferenced()

	if e.effectCache.EntryCount() != 0 {
		t.Fatal("repeated ownership changes leaked references")
	}
}

func TestPutSized_PromotesPinnedCacheEntry(t *testing.T) {
	e := newTestEngine(&mockBroadcaster{})
	tip := Tip{1, 10}
	eff := &pb.Effect{Key: []byte("a")}
	e.effectCache.PutSizedCache(tip, eff, 10)

	if !e.effectCache.Incref(tip) {
		t.Fatal("failed to pin cache entry")
	}

	e.effectCache.PutSized(tip, eff, 10)
	e.effectCache.PutSized(tip, eff, 10) // duplicate delivery must not add ownership twice
	e.effectCache.Decref(tip)
	e.reclaimUnreferenced()

	if _, ok := e.effectCache.Get(tip); !ok {
		t.Fatal("promotion failed while a cache pin was present")
	}

	e.releaseChainRefs("a", []Tip{tip}, nil)
	e.reclaimUnreferenced()

	if e.effectCache.EntryCount() != 0 {
		t.Fatal("duplicate delivery leaked a creation reference")
	}
}
