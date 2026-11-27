package effects

import (
	"fmt"
	"slices"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
)

func TestCompactionReclaimsCoveredSubscriptions(t *testing.T) {
	for _, unsubscribe := range []bool{false, true} {
		for _, protection := range []string{"none", "partition", "overlay"} {
			for _, metadataFirst := range []bool{false, true} {
				t.Run(fmt.Sprintf("unsubscribe=%t/protection=%s/metadataFirst=%t", unsubscribe, protection, metadataFirst), func(t *testing.T) {
					b := &historyBroadcaster{progress: []HistoryProgress{{Node: 2, Processed: 3}}}

					b.peerIDs = []pb.NodeID{2}
					if protection == "partition" {
						b.progress = nil // Still a member, with no evidence of progress.
					}

					e := newTestEngine(b)
					defer func() { _ = e.Close() }()

					sub, old, cut, concurrent, latest := Tip{42, 1}, Tip{42, 2}, Tip{42, 3}, Tip{42, 4}, Tip{42, 5}

					nodes := map[Tip]*pb.Effect{
						sub: cutEffect(sub), old: cutEffect(old, sub), cut: cutEffect(cut, old),
						concurrent: cutEffect(concurrent), latest: cutEffect(latest, cut, concurrent),
					}
					for _, tip := range []Tip{sub, concurrent} {
						nodes[tip].Kind = &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{SubscriberNodeId: 7, Unsubscribe: unsubscribe}}
					}

					for _, tip := range []Tip{cut, latest} {
						nodes[tip].Kind = &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{State: &pb.ReducedEffect{Subscribers: map[uint64]bool{2: true}}}}
					}

					if metadataFirst {
						nodes[concurrent].ForkChoiceHash = []byte{0}
					}

					putCutEffects(t, e, nodes)
					e.index.Insert("k", nil, keytrie.NewTipSet(latest))
					e.addPeerSubscriber(logicalAddress("k"), 2)

					var overlay historyOverlay

					if protection == "overlay" {
						e.nextOff.Store(2)

						e.pinHistory(&overlay)
						defer e.unpinHistory(&overlay)
					}

					d := newDag(e, logicalAddress("k"), "")

					d.compact = true
					if err := d.prepare([]Tip{latest}); err != nil {
						t.Fatal(err)
					}

					e.reclaimUnreferenced()

					for _, tip := range []Tip{sub, old} {
						if _, present := e.effectCache.Get(tip); present != (protection != "none") {
							t.Fatalf("covered ancestor %v resident=%t, protection=%s", tip, present, protection)
						}
					}

					for _, tip := range []Tip{cut, concurrent, latest} {
						if _, present := e.effectCache.Get(tip); !present {
							t.Fatalf("reclaimed retained snapshot or concurrent subscription %v", tip)
						}
					}

					if err := e.FlushIndex(); err != nil {
						t.Fatal(err)
					}

					e.reclaimUnreferenced()

					for tip := range nodes {
						if _, present := e.effectCache.Get(tip); present {
							t.Fatalf("flush left orphaned effect %v", tip)
						}
					}
				})
			}
		}
	}
}

func TestSubscriberDeparturePrunesResidentKeys(t *testing.T) {
	e := newTestEngine(nil)
	defer func() { _ = e.Close() }()

	for _, key := range []string{"hot", "idle"} {
		e.index.Insert(key, nil, keytrie.NewTipSet())
		e.addPeerSubscriber(logicalAddress(key), 2)
	}

	baseline := e.subscriberBytes.Load()

	for peer := pb.NodeID(100); peer < 300; peer++ {
		for _, key := range []string{"hot", "idle"} {
			e.addPeerSubscriber(logicalAddress(key), peer)
		}

		e.DropPeer(peer)
	}

	for _, key := range []string{"hot", "idle"} {
		if got := e.PeerSubscribers(key); !slices.Equal(got, []pb.NodeID{2}) {
			t.Fatalf("%s retained departed subscribers: %v", key, got)
		}
	}

	if got := e.subscriberBytes.Load(); got > 2*baseline {
		t.Fatalf("subscriber storage grew with churn: got %d, limit %d", got, 2*baseline)
	}

	e.DropPeer(2)

	if got := e.subscriberBytes.Load(); got != 0 {
		t.Fatalf("last departure retained %d bytes", got)
	}
}

func TestSubscriptionSnapshotsBoundChurn(t *testing.T) {
	b := &mockBroadcaster{peerIDs: []pb.NodeID{2}}
	// The remaining member is unreachable, but must remain a subscriber.
	e := newTestEngine(b)
	defer func() { _ = e.Close() }()

	preSubscribe(e, "k")
	historicalTestWrite(t, e, map[string]string{"k": "value"}, false)
	e.addPeerSubscriber(logicalAddress("k"), 2)

	subscriptions := make([]Tip, 0, 12)

	for round := range 12 {
		peer := pb.NodeID(100 + round)

		c := e.NewContext()
		if err := c.Emit(&pb.Effect{Key: []byte("k"), Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{SubscriberNodeId: uint64(peer)}}}); err != nil {
			t.Fatal(err)
		}

		subscriptions = append(subscriptions, c.keys[logicalAddress("k")].lastOffset)
		if err := c.Flush(); err != nil {
			t.Fatal(err)
		}

		c.Abort()
		// Simulate old bootstrap state or a late announcement after departure.
		// Snapshot production must prune it even without another DropPeer call.
		for range 16 {
			historicalTestWrite(t, e, map[string]string{"k": "value"}, false)
		}

		compactor := e.NewContext()
		if _, _, err := compactor.GetSnapshot("k"); err != nil {
			t.Fatal(err)
		}

		ck := compactor.keys[logicalAddress("k")]
		if ck == nil {
			t.Fatal("read did not emit a compaction snapshot")
		}

		eff, ok := e.effectCache.Get(ck.lastOffset)
		if !ok || eff.GetSnapshot().GetState() == nil {
			t.Fatal("missing compaction snapshot")
		}

		if got := eff.GetSnapshot().GetState().GetSubscribers(); len(got) != 1 || !got[2] {
			t.Fatalf("snapshot retained churn or dropped unreachable member: %v", got)
		}

		if got := e.PeerSubscribers("k"); !slices.Equal(got, []pb.NodeID{2}) {
			t.Fatalf("resident subscriber set retained churn: %v", got)
		}

		if err := compactor.Flush(); err != nil {
			t.Fatal(err)
		}

		compactor.Abort()
	}

	// Once the last member departs, the existing progress boundary permits
	// reclamation. Two snapshots are retained; only the latest round's sub
	// can remain above the older boundary.
	b.peerIDs = nil

	e.DropPeer(2)

	if _, _, _, err := e.GetSnapshot("k"); err != nil {
		t.Fatal(err)
	}

	e.reclaimUnreferenced()

	for _, tip := range subscriptions[:len(subscriptions)-1] {
		if _, ok := e.effectCache.Get(tip); ok {
			t.Fatalf("long-lived key retained covered subscription %v", tip)
		}
	}
}

// A new holder must recover membership from the retained snapshots after the
// subscribe/unsubscribe effects that established it have actually been freed.
func TestSubscriptionBootstrapAfterCompaction(t *testing.T) {
	e := newTestEngine(nil)
	defer func() { _ = e.Close() }()

	sub, unsub, cut, latest := Tip{42, 1}, Tip{42, 2}, Tip{42, 3}, Tip{42, 4}
	nodes := map[Tip]*pb.Effect{
		sub: cutEffect(sub), unsub: cutEffect(unsub, sub),
		cut: cutEffect(cut, unsub), latest: cutEffect(latest, cut),
	}
	nodes[sub].Kind = &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{SubscriberNodeId: 7}}

	nodes[unsub].Kind = &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{SubscriberNodeId: 7, Unsubscribe: true}}
	for _, tip := range []Tip{cut, latest} {
		nodes[tip].Kind = &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{State: &pb.ReducedEffect{Subscribers: map[uint64]bool{2: true}}}}
	}

	putCutEffects(t, e, nodes)
	e.index.Insert("k", nil, keytrie.NewTipSet(latest))
	d := newDag(e, logicalAddress("k"), "")

	d.compact = true
	if err := d.prepare([]Tip{latest}); err != nil {
		t.Fatal(err)
	}

	e.reclaimUnreferenced()

	for _, tip := range []Tip{sub, unsub} {
		if _, ok := e.effectCache.Get(tip); ok {
			t.Fatalf("covered membership effect %v remains resident", tip)
		}
	}

	joiner := newTestEngine(&mockBroadcaster{peerIDs: []pb.NodeID{2}})
	defer func() { _ = joiner.Close() }()

	e.effectCache.VisitAncestry(logicalAddress("k"), latest, func(tip Tip, eff *pb.Effect, _ bool) bool {
		if eff != nil {
			if err := joiner.effectCache.Put(tip, eff); err != nil {
				t.Fatal(err)
			}
		}

		return true
	})
	joiner.index.Insert("k", nil, keytrie.NewTipSet(latest))
	joiner.seedSubscribersFromDag(logicalAddress("k"))

	if got := joiner.PeerSubscribers("k"); !slices.Equal(got, []pb.NodeID{2}) {
		t.Fatalf("bootstrap lost the live subscriber or resurrected the unsubscribed peer: %v", got)
	}
}
