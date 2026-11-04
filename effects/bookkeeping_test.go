package effects

import (
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
)

func TestHorizonDuplicatePreservesWaiters(t *testing.T) {
	e := newHorizonTestEngine()
	bind := testBind([]*pb.EffectRef{{NodeId: 1, Offset: 10}}, &pb.EffectRef{NodeId: 1, Offset: 20})
	e.horizon.Add("txn", Tip{1, 30}, bind)
	original, _ := e.horizon.entries.Load("txn")
	e.horizon.Add("txn", Tip{1, 30}, bind)

	current, _ := e.horizon.entries.Load("txn")
	if current != original {
		t.Fatal("duplicate bind replaced the entry on which readers wait")
	}

	done := make(chan struct{})

	go func() {
		token := original.waiters.RLock()
		original.waiters.RUnlock(token)
		close(done)
	}()

	e.horizon.MakeVisible("txn")

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reader remained blocked after promotion")
	}
}

func TestHorizonConcurrentLifecycle(t *testing.T) {
	e := newHorizonTestEngine()
	a := testBind([]*pb.EffectRef{{NodeId: 1, Offset: 10}}, &pb.EffectRef{NodeId: 1, Offset: 20})
	b := testBind([]*pb.EffectRef{{NodeId: 1, Offset: 40}}, &pb.EffectRef{NodeId: 1, Offset: 50})
	b.Keys[0].Key = []byte("k2")
	bridge := &pb.TransactionalBindEffect{Keys: []*pb.TransactionalBindEffect_KeyBind{a.GetKeys()[0], b.GetKeys()[0]}}

	for range 100 {
		e.horizon.Add("a", Tip{1, 30}, a)
		e.horizon.Add("b", Tip{1, 60}, b)

		start := make(chan struct{})

		var wg sync.WaitGroup
		for _, action := range []func(){
			func() { e.horizon.Add("bridge", Tip{1, 70}, bridge) },
			func() { e.horizon.ScheduleMakeVisible("b", time.Hour) },
			func() { e.horizon.MakeVisible("a") },
			func() { e.horizon.Abort("b") },
			func() { e.horizon.Abort("b") },
			func() { e.horizon.WaitForClear("a").Release() },
		} {
			wg.Go(func() {
				<-start
				action()
			})
		}

		close(start)
		wg.Wait()
		e.horizon.MakeVisible("bridge")

		if !e.horizon.Empty() || len(e.horizon.groups) != 0 {
			t.Fatal("resolved horizon retained entries or groups")
		}
	}
}

func TestHorizonMergedGroupNotifiesLiveKeysOnce(t *testing.T) {
	e := newHorizonTestEngine()
	a := testBind([]*pb.EffectRef{{NodeId: 1, Offset: 10}}, &pb.EffectRef{NodeId: 1, Offset: 20})
	b := testBind([]*pb.EffectRef{{NodeId: 1, Offset: 40}}, &pb.EffectRef{NodeId: 1, Offset: 50})
	b.Keys[0].Key = []byte("k2")
	bridge := &pb.TransactionalBindEffect{Keys: []*pb.TransactionalBindEffect_KeyBind{a.GetKeys()[0], b.GetKeys()[0]}}
	e.horizon.Add("a", Tip{1, 30}, a)
	e.horizon.Add("b", Tip{1, 60}, b)
	e.horizon.Add("bridge", Tip{1, 70}, bridge)
	e.horizon.Abort("b")

	var notified []string

	e.OnKeyDataAdded = func(key string) {
		notified = append(notified, key)
		// Callbacks must be able to reenter the horizon lifecycle.
		e.horizon.Abort("unknown")
	}
	done := make(chan struct{})

	go func() {
		e.horizon.MakeVisible("a")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("promotion blocked in a reentrant callback")
	}

	if !slices.Equal(notified, []string{"k1", "k2"}) || !e.horizon.Empty() {
		t.Fatalf("merged promotion: notified %v, empty %v", notified, e.horizon.Empty())
	}
}

func TestCreationOwnershipReleasesCapacity(t *testing.T) {
	p := newVertexPool()

	bind := &pb.TransactionalBindEffect{}
	for i := range 128 {
		bind.Keys = append(bind.Keys, &pb.TransactionalBindEffect_KeyBind{Key: fmt.Appendf(nil, "key-%d", i)})
	}
	// Repeated key declarations must still take only one ownership reference.
	bind.Keys = append(bind.Keys, bind.GetKeys()[0])
	eff := &pb.Effect{Kind: &pb.Effect_TxnBind{TxnBind: bind}}

	tip := Tip{1, 1}
	if err := p.Put(tip, eff); err != nil {
		t.Fatal(err)
	}

	if !p.Incref(tip) {
		t.Fatal("failed to pin vertex")
	}

	v, _ := p.m.Load(tip)
	if v.refs.Load() != 129 {
		t.Fatalf("duplicate key acquired ownership: %d refs", v.refs.Load())
	}

	for _, key := range bind.GetKeys()[:127] {
		p.decrefChainNode(string(key.GetKey()), tip)
	}

	if len(v.creationKeys) != 1 || cap(v.creationKeys) > 8 {
		t.Fatalf("one remaining owner retained capacity %d", cap(v.creationKeys))
	}

	p.decrefChainNode(string(bind.GetKeys()[127].GetKey()), tip)

	if v.creationKeys != nil || v.creationBytes != 0 || v.refs.Load() != 1 {
		t.Fatal("released ownership retained storage or consumed the independent pin")
	}

	p.Decref(tip)
	p.drainReclaim()

	if p.EntryCount() != 0 || p.vertexBytes.Load() != 0 {
		t.Fatal("released vertex retained storage or accounting")
	}
}

func TestSubscribersShrinkAndReturnIndependentSnapshots(t *testing.T) {
	e := newHorizonTestEngine()
	e.index.Insert("key", nil, keytrie.NewTipSet())

	for id := pb.NodeID(2); id < 130; id++ {
		e.addPeerSubscriber("key", id)
	}

	before := e.PeerSubscribers("key")
	for id := pb.NodeID(2); id < 129; id++ {
		e.removePeerSubscriber("key", id)
	}

	ls := e.index.LoadData("key")
	if cap(ls.subscribers) > 8 || len(before) != 128 {
		t.Fatal("unsubscribe retained excess capacity or changed a returned snapshot")
	}

	after := e.PeerSubscribers("key")
	after[0] = 99

	if got := e.PeerSubscribers("key"); !slices.Equal(got, []pb.NodeID{129}) {
		t.Fatalf("caller mutation changed subscribers: %v", got)
	}

	e.removePeerSubscriber("key", 129)

	if ls.subscribers != nil || e.subscriberBytes.Load() != 0 {
		t.Fatal("empty subscriber set retained storage")
	}
}
