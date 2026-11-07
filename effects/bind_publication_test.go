package effects

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
)

// Queue asynchronous data/verdict broadcasts, but deliver synchronous bind
// requests and dependency fetches. Independent transport streams permit this
// ordering; all queued messages are delivered before the test finishes.
type bindPublicationBroadcaster struct {
	mockBroadcaster

	peer     *Engine
	bindSent chan struct{}
}

func (b *bindPublicationBroadcaster) ReplicateTo(n *pb.OffsetNotify, _ []byte, _ pb.NodeID) ([]*pb.NackNotify, error) {
	return b.peer.HandleRemote(n)
}

func (b *bindPublicationBroadcaster) ReplicateMarshalled(n *pb.OffsetNotify, _ []byte, _ pb.NodeID) ([]*pb.NackNotify, error) {
	b.bindSent <- struct{}{}

	return b.peer.HandleRemote(n)
}

func (b *bindPublicationBroadcaster) FetchFromAny(ref *pb.EffectRef, _ FetchHint) ([]byte, error) {
	if eff, ok := b.peer.effectCache.Get(r(ref)); ok {
		return wireEffect(eff), nil
	}

	return nil, fmt.Errorf("missing effect %v", ref)
}

// Both transactions pass preflight before either bind is published. The weaker
// bind arrives while the stronger originator is between checking and publishing.
// That receiver must not respond with a view that omits its forthcoming bind,
// then publish that competitor after allowing the weaker transaction to commit.
func TestBindPublicationSerializesIncomingResponse(t *testing.T) {
	var (
		engines [2]*Engine
		links   [2]*bindPublicationBroadcaster
	)
	for i := range engines {
		links[i] = &bindPublicationBroadcaster{bindSent: make(chan struct{}, 1)}
		links[i].peerIDs = []pb.NodeID{pb.NodeID(2 - i)}
		links[i].allRegionPeersReachable = true
		engines[i] = newTxnTestEngine(links[i])
		engines[i].nodeID = pb.NodeID(i + 1)
		engines[i].UpdateSafetyRules(SafeMode, nil)
		engines[i].horizon = newHorizonSet(engines[i])
	}

	links[0].peer, links[1].peer = engines[1], engines[0]

	tx := [2]*Context{engines[0].NewContext(), engines[1].NewContext()}
	for _, c := range tx {
		c.BeginTx()
	}

	for i, key := range []string{"x", "y"} {
		value, _, err := tx[i].GetSnapshot(key)
		if err != nil || value != nil {
			t.Fatalf("absence read: value=%v err=%v", value, err)
		}
	}

	for i, key := range []string{"y", "x"} {
		if err := tx[i].Emit(scalarSet(key, fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
	}

	var (
		paused      [2]chan *pb.Effect
		release     [2]chan struct{}
		releaseOnce [2]sync.Once
		done        [2]chan error
	)

	resume := func(i int) { releaseOnce[i].Do(func() { close(release[i]) }) }
	for i, e := range engines {
		paused[i] = make(chan *pb.Effect, 1)
		release[i] = make(chan struct{})
		done[i] = make(chan error, 1)

		t.Cleanup(func() { resume(i) })
		e.SetOnLocalEffect(func(_ Tip, eff *pb.Effect) {
			if eff.GetTxnBind() != nil {
				paused[i] <- eff

				<-release[i]
			}
		})

		go func() { done[i] <- tx[i].Flush() }()
	}

	var binds [2]*pb.Effect
	for i := range binds {
		select {
		case binds[i] = <-paused[i]:
		case err := <-done[i]:
			t.Fatalf("transaction %d finished before publication pause: %v", i, err)
		case <-time.After(5 * time.Second):
			t.Fatal("bind emission did not reach hook")
		}
	}

	stronger, weaker := 0, 1
	if ForkChoiceLess(binds[1].GetForkChoiceHash(), binds[0].GetForkChoiceHash()) {
		stronger, weaker = 1, 0
	}

	resume(weaker)

	select {
	case <-links[weaker].bindSent:
	case <-time.After(5 * time.Second):
		t.Fatal("weaker bind was not sent")
	}

	var (
		results  [2]error
		finished [2]bool
	)

	select {
	case results[weaker] = <-done[weaker]:
		finished[weaker] = true
		if results[weaker] == nil {
			t.Error("competing transaction committed while the receiver's bind was still unpublished")
		}
	case <-time.After(100 * time.Millisecond):
		// The incoming bind waits for the receiver's publication boundary.
	}

	resume(stronger)

	for i := range done {
		if finished[i] {
			continue
		}

		select {
		case results[i] = <-done[i]:
		case <-time.After(5 * time.Second):
			t.Fatalf("transaction %d did not finish after publication resumed", i)
		}
	}

	if results[stronger] != nil || !errors.Is(results[weaker], ErrTxnAborted) {
		t.Errorf("expected stronger commit and weaker abort; stronger=%v weaker=%v", results[stronger], results[weaker])
	}

	for i, link := range links {
		link.mu.Lock()
		queued := append([]*pb.OffsetNotify(nil), link.broadcasts...)
		link.mu.Unlock()

		for _, n := range queued {
			if _, err := engines[1-i].HandleRemote(n); err != nil {
				t.Fatal(err)
			}
		}
	}
}
