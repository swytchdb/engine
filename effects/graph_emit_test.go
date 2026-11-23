package effects

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"slices"
	"testing"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
	"google.golang.org/protobuf/proto"
)

func seedGraph(t *testing.T, e *Engine, key string) []Tip {
	t.Helper()

	c := e.NewContext()
	for _, eff := range pb.NewGraphEffects([]byte(key), []byte("avl")) {
		if err := c.Emit(eff); err != nil {
			t.Fatal(err)
		}
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	return slices.Clone(e.index.Contains(key).Tips())
}

func addGraphPredicate(t *testing.T, e *Engine, key, id string, peer uint64, predicate map[string]string) []Tip {
	t.Helper()

	state, _, err := e.reconstruct(logicalAddress(key), e.index.Contains(key).Tips(), "", false)
	if err != nil {
		t.Fatal(err)
	}

	c := e.NewContext()
	if err := c.writeGraphPredicate(key, pb.NodeID(peer), id, predicate, state); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	return slices.Clone(e.index.Contains(key).Tips())
}

func graphNode(key, address string, value int64) *pb.Effect {
	return &pb.Effect{Key: []byte(keytrie.InternalKey(key, address)), IKey: []byte(keytrie.InternalKey(key, address)), Kind: &pb.Effect_Data{Data: &pb.DataEffect{Op: pb.EffectOp_INSERT_OP, Collection: pb.CollectionKind_SCALAR, Merge: pb.MergeRule_LAST_WRITE_WINS, Value: &pb.DataEffect_IntVal{IntVal: value}}}}
}

// The graph library constructs ordinary collection effects. EmitGraph does
// not synthesize or replicate a second region-label representation.
func emitGraphNode(c *Context, deps GraphTip, value *pb.Effect) error {
	region := &pb.Effect{Key: value.GetKey(), IKey: value.GetIKey(), Virtual: []byte(pb.GraphRegionsKey), Kind: &pb.Effect_Data{Data: &pb.DataEffect{
		Op: pb.EffectOp_INSERT_OP, Collection: pb.CollectionKind_KEYED, Merge: pb.MergeRule_LAST_WRITE_WINS,
		Id: []byte("region"), Value: &pb.DataEffect_Raw{Raw: []byte("mammals")},
	}}}
	if err := c.EmitGraph(deps, region); err != nil {
		return err
	}

	return c.EmitGraph(deps, value)
}

func TestGraphEmissionIndependentFrontiers(t *testing.T) {
	e := NewEngine(EngineConfig{DefaultMode: UnsafeMode})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})

	for _, key := range []string{"animals", "animals\x00", "\x01\x07animalsn"} {
		seedGraph(t, e, key)
		graphTips := addGraphPredicate(t, e, key, "local", uint64(e.nodeID), map[string]string{"region": "mammals"})

		c := e.NewContext()
		for i, address := range []string{"n", "m"} {
			if err := emitGraphNode(c, GraphTip{Key: key, GraphTips: graphTips}, graphNode(key, address, int64(i+1))); err != nil {
				t.Fatal(err)
			}
		}

		if err := c.Flush(); err != nil {
			t.Fatal(err)
		}

		if !tipsEqual(e.index.Contains(key).Tips(), graphTips) {
			t.Fatal("node advanced graph descriptor")
		}

		for i, address := range []string{"n", "m"} {
			tips := e.index.ContainsInternal(keytrie.InternalKey(key, address)).Tips()
			if len(tips) != 1 {
				t.Fatalf("node %q has tips %v", address, tips)
			}

			state, _, err := e.reconstruct(keytrie.InternalKey(key, address), tips, "", false)
			if err != nil {
				t.Fatal(err)
			}

			if state.GetScalar().GetIntVal() != int64(i+1) || state.GetTypeTag() == pb.ValueType_TYPE_GRAPH {
				t.Fatalf("mixed node and descriptor: %v", state)
			}

			eff, _ := e.effectCache.Get(tips[0])
			if !refsContain(eff.GetDeps(), graphTips[0]) {
				t.Fatal("missing descriptor causal dependency")
			}

			wire, err := MarshalEffect(eff)
			if err != nil {
				t.Fatal(err)
			}

			notice := BuildOffsetNotify(e.nodeID, tips[0], eff, wire, nil)
			if !bytes.Equal(eff.GetIKey(), []byte(keytrie.InternalKey(key, address))) || string(notice.GetKey()) != keytrie.InternalKey(key, address) {
				t.Fatal("transport lost identity")
			}
		}
	}

	if e.index.Size() != 3 || e.index.SizeInternal() != 9 {
		t.Fatalf("logical enumeration leaked nodes: %d/%d", e.index.Size(), e.index.SizeInternal())
	}
}

type graphDelivery struct {
	peer   pb.NodeID
	notice *pb.OffsetNotify
}
type graphRecorder struct {
	mockBroadcaster

	deliveries  chan graphDelivery
	historySent map[pb.NodeID][]uint64
}

func (b *graphRecorder) HistoryProgress() []HistoryProgress       { return nil }
func (b *graphRecorder) RecordHistoryProcessed(pb.NodeID, uint64) {}
func (b *graphRecorder) RecordHistorySent(peer pb.NodeID, offset uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.historySent == nil {
		b.historySent = make(map[pb.NodeID][]uint64)
	}

	b.historySent[peer] = append(b.historySent[peer], offset)
}

func (b *graphRecorder) ReplicateTo(n *pb.OffsetNotify, _ []byte, peer pb.NodeID) ([]*pb.NackNotify, error) {
	if eff, err := parseWireEffect(n.GetEffectData()); err == nil && eff.GetSubscription() != nil {
		return nil, nil
	}
	err := b.replicateErr
	b.deliveries <- graphDelivery{peer, n}

	return nil, err
}

func (b *graphRecorder) ReplicateMarshalled(n *pb.OffsetNotify, _ []byte, peer pb.NodeID) ([]*pb.NackNotify, error) {
	return b.ReplicateTo(n, nil, peer)
}

// Ordinary subscription discovery is protocol traffic, not a graph publication.
func (b *graphRecorder) BroadcastWithData(n *pb.OffsetNotify, data []byte) {
	if eff, err := parseWireEffect(n.GetEffectData()); err == nil && eff.GetSubscription() != nil {
		return
	}
	b.mockBroadcaster.BroadcastWithData(n, data)
}

func receiveGraphDelivery(t *testing.T, b *graphRecorder) graphDelivery {
	t.Helper()

	select {
	case d := <-b.deliveries:
		return d
	case <-time.After(time.Second):
		t.Fatal("no graph delivery")

		return graphDelivery{}
	}
}

func TestGraphRoutingUsesObservedDirectoryAndOrdinaryMembership(t *testing.T) {
	e := NewEngine(EngineConfig{DefaultMode: SafeMode})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})
	seedGraph(t, e, "animals")
	old := addGraphPredicate(t, e, "animals", "a", 2, map[string]string{"region": "(mammals|reptiles)"})
	addGraphPredicate(t, e, "animals", "b", 3, map[string]string{"region": "mammals"})

	b := &graphRecorder{mockBroadcaster: mockBroadcaster{peerIDs: []pb.NodeID{2, 3}, allRegionPeersReachable: true}, deliveries: make(chan graphDelivery, 16)}
	e.SetBroadcaster(b)
	e.addPeerSubscriber(logicalAddress("animals"), 2)
	e.addPeerSubscriber(logicalAddress("animals"), 3)

	c := e.NewContext()
	if err := emitGraphNode(c, GraphTip{Key: "animals", GraphTips: old}, graphNode("animals", "n", 7)); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	d := receiveGraphDelivery(t, b)
	if d.peer != 2 {
		t.Fatalf("routed using current directory: %d", d.peer)
	}

	if second := receiveGraphDelivery(t, b); second.peer != 2 {
		t.Fatal("region and value selected different subscribers")
	}

	if len(b.deliveries) != 0 {
		t.Fatal("sent historical observation to later subscriber")
	}

	b.mu.Lock()
	if len(b.historySent) != 1 || len(b.historySent[2]) != 2 {
		t.Errorf("history obligations include nonrecipients: %v", b.historySent)
	}
	b.mu.Unlock()
	// Removing ordinary authority overrides the historical predicate match.
	e.removePeerSubscriber(logicalAddress("animals"), 2)

	c = e.NewContext()
	if err := emitGraphNode(c, GraphTip{Key: "animals", GraphTips: old}, graphNode("animals", "m", 8)); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	if len(b.deliveries) != 0 {
		t.Fatal("sent to peer no longer subscribed to main key")
	}

	if len(b.broadcasts) != 0 {
		t.Fatal("graph fell back to ordinary broadcast")
	}
	// A configured but unreachable matching peer remains a coordination obligation.
	e.addPeerSubscriber(logicalAddress("animals"), 2)

	b.replicateErr = errors.New("offline")

	route := &graphRoute{peers: map[pb.NodeID]bool{2: true}, descriptor: logicalAddress("animals")}
	if err := c.engine.publishGraphNotice(keytrie.InternalKey("animals", "n"), d.notice, route, SafeMode); err == nil {
		t.Fatal("unreachable matching subscriber was silently removed")
	}

	receiveGraphDelivery(t, b)
	b.peerIDs = []pb.NodeID{3}

	if got := e.contextSubscribers(keytrie.InternalKey("animals", "n"), &contextKey{routes: []*graphRoute{route}}); len(got) != 0 {
		t.Fatalf("departed member retained: %v", got)
	}
}

type graphSource struct {
	mockBroadcaster

	source *Engine
}

func (b *graphSource) FetchFromAny(ref *pb.EffectRef, _ FetchHint) ([]byte, error) {
	eff, ok := b.source.effectCache.Get(r(ref))
	if !ok {
		return nil, ErrEffectNotFound
	}

	data, err := MarshalEffect(eff)
	if err != nil {
		return nil, err
	}

	return BuildOffsetNotify(b.source.nodeID, r(ref), eff, data, nil).GetEffectData(), nil
}

func TestGraphRemoteAndBackfillUseInternalIdentity(t *testing.T) {
	sender := NewEngine(EngineConfig{DefaultMode: UnsafeMode})
	receiver := NewEngine(EngineConfig{DefaultMode: UnsafeMode})

	t.Cleanup(func() {
		if err := sender.Close(); err != nil {
			t.Error(err)
		}

		if err := receiver.Close(); err != nil {
			t.Error(err)
		}
	})
	seedGraph(t, sender, "animals")
	graphTips := addGraphPredicate(t, sender, "animals", "receiver", uint64(receiver.nodeID), map[string]string{"region": "mammals"})
	receiverTips := seedGraph(t, receiver, "animals")
	receiver.SetBroadcaster(&graphSource{source: sender})

	c := sender.NewContext()
	if err := emitGraphNode(c, GraphTip{Key: "animals", GraphTips: graphTips}, graphNode("animals", "n", 9)); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	tips := sender.index.ContainsInternal(keytrie.InternalKey("animals", "n")).Tips()
	eff, _ := sender.effectCache.Get(tips[0])

	data, err := MarshalEffect(eff)
	if err != nil {
		t.Fatal(err)
	}

	notice := BuildOffsetNotify(sender.nodeID, tips[0], eff, data, nil)
	if _, err := receiver.HandleRemote(notice); err != nil {
		t.Fatal(err)
	}

	sibling := keytrie.InternalKey("animals", "unseen")
	if _, subscribed := receiver.subscription(sibling); subscribed || receiver.isServed([]byte(sibling)) || receiver.index.ContainsInternal(sibling) != nil {
		t.Fatal("selected receipt granted ownership of an unseen sibling")
	}

	if !tipsEqual(receiver.index.Contains("animals").Tips(), receiverTips) {
		t.Fatal("remote node replaced descriptor")
	}

	if err := receiver.handleBackfill(notice); err != nil {
		t.Fatal(err)
	}

	state, _, err := receiver.reconstruct(keytrie.InternalKey("animals", "n"), receiver.index.ContainsInternal(keytrie.InternalKey("animals", "n")).Tips(), "", false)
	if err != nil {
		t.Fatal(err)
	}

	if state.GetScalar().GetIntVal() != 9 {
		t.Fatalf("remote node state: %v", state)
	}
}

func TestGraphTransactionBindsDistinctInternalNodes(t *testing.T) {
	e := NewEngine(EngineConfig{DefaultMode: UnsafeMode})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})
	graphTips := seedGraph(t, e, "animals")
	c := e.NewContext()
	c.BeginTx()

	for i, address := range []string{"left", "right"} {
		if err := emitGraphNode(c, GraphTip{Key: "animals", GraphTips: graphTips}, graphNode("animals", address, int64(i+1))); err != nil {
			t.Fatal(err)
		}
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	if !tipsEqual(e.index.Contains("animals").Tips(), graphTips) {
		t.Fatal("transaction advanced descriptor")
	}

	for i, address := range []string{"left", "right"} {
		tips := e.index.ContainsInternal(keytrie.InternalKey("animals", address)).Tips()

		eff, _ := e.effectCache.Get(tips[0])
		if len(eff.GetTxnBind().GetKeys()) != 2 {
			t.Fatal("collapsed internal key binds")
		}

		state, _, err := e.reconstruct(keytrie.InternalKey("animals", address), tips, "", false)
		if err != nil {
			t.Fatal(err)
		}

		if state.GetScalar().GetIntVal() != int64(i+1) {
			t.Fatalf("bind reconstructed wrong node %q: %v", address, state)
		}
	}
}

func TestRegionPredicateMatching(t *testing.T) {
	for _, tt := range []struct {
		predicate, regions map[string]string
		want               bool
		bad                bool
	}{
		{map[string]string{"region": "(mammals|reptiles)"}, map[string]string{"region": "mammals"}, true, false},
		{map[string]string{"region": "mammals"}, map[string]string{"region": "small mammals"}, false, false},
		{map[string]string{"region": ".*"}, nil, false, false},
		{nil, nil, true, false},
		{map[string]string{"region": "mammals", "color": "blue"}, map[string]string{"region": "mammals", "color": "red"}, false, false},
		{map[string]string{"region": "["}, nil, false, true},
	} {
		got, err := matchRegions(tt.predicate, tt.regions)
		if (err != nil) != tt.bad || got != tt.want {
			t.Fatalf("match %v %v = %v %v", tt.predicate, tt.regions, got, err)
		}
	}
}

func TestGraphSnapshotPrunesDirectoryWithoutChangingHistory(t *testing.T) {
	e := NewEngine(EngineConfig{DefaultMode: UnsafeMode})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})
	seedGraph(t, e, "animals")

	for _, peer := range []uint64{uint64(e.nodeID), 2, 3, 4} {
		addGraphPredicate(t, e, "animals", string(rune(peer)), peer, map[string]string{"region": "mammals"})
	}

	tips := slices.Clone(e.index.Contains("animals").Tips())

	before, _, err := e.reconstruct(logicalAddress("animals"), tips, "", false)
	if err != nil {
		t.Fatal(err)
	}

	saved := proto.Clone(before).(*pb.ReducedEffect)

	e.SetBroadcaster(&mockBroadcaster{peerIDs: []pb.NodeID{2, 3}})
	e.addPeerSubscriber(logicalAddress("animals"), 2)
	e.addPeerSubscriber(logicalAddress("animals"), 4)

	snapshot := cloneReducedFlat(before)

	snapshot.Partitions = maps.Clone(before.GetPartitions())
	if err := e.stampSnapshotSubscriptions(logicalAddress("animals"), snapshot); err != nil {
		t.Fatal(err)
	}

	if !proto.Equal(saved, before) {
		t.Fatal("pruning mutated historical descriptor")
	}

	peers, err := graphPredicatePeers(snapshot, map[string]string{"region": "mammals"})
	if err != nil {
		t.Fatal(err)
	}

	if len(peers) != 2 || !peers[e.nodeID] || !peers[2] {
		t.Fatalf("pruned directory: %v", peers)
	}
}

func TestGraphTransactionRoutingAndVerdicts(t *testing.T) {
	e := NewEngine(EngineConfig{DefaultMode: UnsafeMode})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})
	seedGraph(t, e, "animals")
	tips := addGraphPredicate(t, e, "animals", "match", 2, map[string]string{"region": "mammals"})
	b := &graphRecorder{mockBroadcaster: mockBroadcaster{peerIDs: []pb.NodeID{2, 3}, allRegionPeersReachable: true}, deliveries: make(chan graphDelivery, 32)}
	e.SetBroadcaster(b)
	e.addPeerSubscriber(logicalAddress("animals"), 2)
	e.addPeerSubscriber(logicalAddress("animals"), 3)
	c := e.NewContext()
	c.BeginTx()

	for _, address := range []string{"left", "right"} {
		if err := emitGraphNode(c, GraphTip{Key: "animals", GraphTips: tips}, graphNode("animals", address, 3)); err != nil {
			t.Fatal(err)
		}
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
	// Region/value writes and the complete bind use the explicit selected recipients.
	// Verdict snapshots use ordinary publication and carry complete evidence.
	kinds := map[string]int{}

	for range 5 {
		d := receiveGraphDelivery(t, b)
		if d.peer != 2 {
			t.Fatalf("nonmatching subscriber received transaction: %d", d.peer)
		}

		eff, err := parseWireEffect(d.notice.GetEffectData())
		if err != nil {
			t.Fatal(err)
		}

		if len(eff.GetIKey()) == 0 {
			t.Fatal("graph transaction published on descriptor")
		}

		switch {
		case eff.GetData() != nil:
			kinds["data"]++
		case eff.GetTxnBind() != nil:
			kinds["bind"]++
		case eff.GetSnapshot() != nil:
			kinds["snapshot"]++
		}
	}

	for _, n := range b.broadcasts {
		eff, err := parseWireEffect(n.GetEffectData())
		if err != nil {
			t.Fatal(err)
		}
		if eff.GetSnapshot() == nil {
			t.Fatal("unexpected ordinary publication")
		}
		kinds["snapshot"]++
	}
	if kinds["data"] != 4 || kinds["bind"] != 1 || kinds["snapshot"] != 2 {
		t.Fatalf("publication kinds: %v", kinds)
	}

	if len(b.broadcasts) != 2 {
		t.Fatal("missing ordinary verdict publications")
	}

	if !tipsEqual(tips, e.index.Contains("animals").Tips()) {
		t.Fatal("verdict changed descriptor frontier")
	}

	for _, address := range []string{"left", "right"} {
		state, _, err := e.reconstruct(keytrie.InternalKey("animals", address), e.index.ContainsInternal(keytrie.InternalKey("animals", address)).Tips(), "", false)
		if err != nil || state.GetScalar().GetIntVal() != 3 {
			t.Fatalf("node after verdict: %v, %v", state, err)
		}
	}
}

func TestGraphSharedDescriptorAndNodeBindRemainsANodeDependency(t *testing.T) {
	e := NewEngine(EngineConfig{DefaultMode: UnsafeMode})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})
	tips := seedGraph(t, e, "animals")
	c := e.NewContext()
	c.BeginTx()

	if err := emitGraphNode(c, GraphTip{Key: "animals", GraphTips: tips}, graphNode("animals", "n", 1)); err != nil {
		t.Fatal(err)
	}
	// The same transaction also changes descriptor metadata.
	entry := virt(makeDataEffect(1, orderedInsert(pb.Placement_PLACE_TAIL, "root", []byte("n"))), pb.GraphEntryPointKey)

	entry.Key = []byte("animals")
	if err := c.Emit(entry); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	shared := slices.Clone(e.index.Contains("animals").Tips())
	if !tipsEqual(shared, e.index.ContainsInternal(keytrie.InternalKey("animals", "n")).Tips()) {
		t.Fatal("expected one shared bind frontier")
	}

	c = e.NewContext()
	// A metadata-only update needs the earlier value through the shared bind.
	meta := &pb.Effect{Key: []byte(keytrie.InternalKey("animals", "n")), IKey: []byte(keytrie.InternalKey("animals", "n")), Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{TypeTag: pb.ValueType_TYPE_STRING}}}
	if err := c.EmitGraph(GraphTip{Key: "animals", GraphTips: shared, Tips: shared}, meta); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	state, _, err := e.reconstruct(keytrie.InternalKey("animals", "n"), e.index.ContainsInternal(keytrie.InternalKey("animals", "n")).Tips(), "", false)
	if err != nil || state.GetScalar().GetIntVal() != 1 {
		t.Fatalf("descriptor filtering discarded the node's shared dependency: %v %v", state, err)
	}

	if !tipsEqual(shared, e.index.Contains("animals").Tips()) {
		t.Fatal("node metadata advanced descriptor")
	}
}

func TestGraphPredicateFieldsMergeAsCollectionData(t *testing.T) {
	e := NewEngine(EngineConfig{DefaultMode: UnsafeMode})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})
	seedGraph(t, e, "animals")
	base := addGraphPredicate(t, e, "animals", "sub", 2, map[string]string{"region": "mammals", "color": "blue"})
	entryID := string(graphSubscriptionID(2, "sub"))
	partition := pb.GraphRegionSubscriptionsKey + "/" + entryID
	// Two writers observe the same descriptor and edit different predicate fields.
	for field, pattern := range map[string]string{"region": "(mammals|reptiles)", "color": "(blue|green)"} {
		c := e.NewContext()

		eff := &pb.Effect{Key: []byte("animals"), Virtual: []byte(partition), Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op: pb.EffectOp_INSERT_OP, Collection: pb.CollectionKind_KEYED, Merge: pb.MergeRule_LAST_WRITE_WINS,
			Id: []byte(field), Value: &pb.DataEffect_Raw{Raw: []byte(pattern)},
		}}}
		if err := c.Emit(eff, base); err != nil {
			t.Fatal(err)
		}

		if err := c.Flush(); err != nil {
			t.Fatal(err)
		}
	}

	state, _, err := e.reconstruct(logicalAddress("animals"), e.index.Contains("animals").Tips(), "", false)
	if err != nil {
		t.Fatal(err)
	}

	entry := state.GetPartitions()[pb.GraphRegionSubscriptionsKey].GetNetAdds()[entryID]
	if string(entry.GetData().GetChild()) != partition {
		t.Fatal("subscription is not a child collection")
	}

	peers, err := graphPredicatePeers(state, map[string]string{"region": "reptiles", "color": "green"})
	if err != nil || !peers[2] {
		t.Fatalf("concurrent predicate field updates did not merge: %v %v", peers, err)
	}
	// The prior descriptor observation still means the original predicate.
	view, err := e.historicalReadView(context.Background(), []string{logicalAddress("animals")}, func(string) ([]Tip, error) { return base, nil }, nil)
	if err != nil {
		t.Fatal(err)
	}

	old, _, err := e.reconstructWithView(logicalAddress("animals"), base, "", false, view)
	if err != nil {
		t.Fatal(err)
	}

	peers, err = graphPredicatePeers(old, map[string]string{"region": "reptiles", "color": "green"})
	if err != nil || peers[2] {
		t.Fatalf("historical predicate was rewritten: %v %v", peers, err)
	}
}

func TestGraphRoutingReconstructsCollectionRegionsAndDescriptorDeps(t *testing.T) {
	e := NewEngine(EngineConfig{DefaultMode: UnsafeMode})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})
	seedGraph(t, e, "animals")
	observed := addGraphPredicate(t, e, "animals", "match", 2, map[string]string{"region": "mammals"})

	c := e.NewContext()
	if err := emitGraphNode(c, GraphTip{Key: "animals", GraphTips: observed}, graphNode("animals", "n", 1)); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	nodeTips := slices.Clone(e.index.ContainsInternal(keytrie.InternalKey("animals", "n")).Tips())
	// This later directory entry must not affect an emission using observed.
	addGraphPredicate(t, e, "animals", "later", 3, map[string]string{"region": "mammals"})

	c = e.NewContext()
	if err := c.EmitGraph(GraphTip{Key: "animals", Tips: nodeTips, GraphTips: observed}, graphNode("animals", "n", 2)); err != nil {
		t.Fatal(err)
	}

	identity := keytrie.InternalKey("animals", "n")
	route := c.keys[identity].routes[0]
	if !route.peers[2] || route.peers[3] {
		t.Fatalf("emission lost observed route: %v", route)
	}
	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	node, err := e.graphNodeState(identity, e.index.ContainsInternal(identity).Tips(), "")
	if err != nil || graphRegions(node)["region"] != "mammals" || node.GetScalar().GetIntVal() != 2 {
		t.Fatalf("ordinary value update did not retain collection regions: %v %v", node, err)
	}
}

func TestSubscribeGraphUsesNestedCollections(t *testing.T) {
	e := NewEngine(EngineConfig{DefaultMode: UnsafeMode})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})
	seedGraph(t, e, "animals")

	for _, predicate := range []map[string]string{{"region": "mammals", "color": "blue"}, {"region": "mammals"}, {}} {
		c := e.NewContext()
		if err := c.SubscribeGraph("animals", "local", predicate); err != nil {
			t.Fatal(err)
		}

		if err := c.Flush(); err != nil {
			t.Fatal(err)
		}

		state, _, err := e.reconstruct(logicalAddress("animals"), e.index.Contains("animals").Tips(), "", false)
		if err != nil {
			t.Fatal(err)
		}

		id := graphSubscriptionID(e.nodeID, "local")
		child := state.GetPartitions()[pb.GraphRegionSubscriptionsKey].GetNetAdds()[string(id)].GetData().GetChild()

		fields := state.GetPartitions()[string(child)].GetNetAdds()
		if len(fields) != len(predicate) {
			t.Fatalf("stale predicate fields: %v", fields)
		}

		peers, err := graphPredicatePeers(state, map[string]string{"region": "mammals", "color": "blue"})
		if err != nil || !peers[e.nodeID] {
			t.Fatalf("subscription did not match: %v %v", peers, err)
		}
	}

	c := e.NewContext()
	if err := c.UnsubscribeGraph("animals", "local"); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	state, _, err := e.reconstruct(logicalAddress("animals"), e.index.Contains("animals").Tips(), "", false)
	if err != nil {
		t.Fatal(err)
	}

	peers, err := graphPredicatePeers(state, map[string]string{"region": "mammals"})
	if err != nil || len(peers) != 0 {
		t.Fatalf("removed subscription still matched: %v %v", peers, err)
	}
}

func TestGraphNodeDeletionUsesObservedRegions(t *testing.T) {
	e := NewEngine(EngineConfig{DefaultMode: SafeMode})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})
	seedGraph(t, e, "animals")
	observed := addGraphPredicate(t, e, "animals", "match", 2, map[string]string{"region": "mammals"})

	c := e.NewContext()
	if err := emitGraphNode(c, GraphTip{Key: "animals", GraphTips: observed}, graphNode("animals", "n", 1)); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	nodeTips := slices.Clone(e.index.ContainsInternal(keytrie.InternalKey("animals", "n")).Tips())
	b := &graphRecorder{mockBroadcaster: mockBroadcaster{peerIDs: []pb.NodeID{2}, allRegionPeersReachable: true}, deliveries: make(chan graphDelivery, 8)}
	e.SetBroadcaster(b)
	e.addPeerSubscriber(logicalAddress("animals"), 2)
	c = e.NewContext()

	deletion := &pb.Effect{Key: []byte(keytrie.InternalKey("animals", "n")), IKey: []byte(keytrie.InternalKey("animals", "n")), Kind: &pb.Effect_Data{Data: &pb.DataEffect{Op: pb.EffectOp_REMOVE_OP, Collection: pb.CollectionKind_SCALAR}}}
	if err := c.EmitGraph(GraphTip{Key: "animals", Tips: nodeTips, GraphTips: observed}, deletion); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	if delivery := receiveGraphDelivery(t, b); delivery.peer != 2 {
		t.Fatal("deletion did not reach the node subscriber")
	}

}

// Delay ordinary node/snapshot deliveries. A bind must bring in all validation
// dependencies even when the recipient's predicate matches only one of its nodes.
type graphTransactionRecorder struct {
	graphRecorder

	receivers map[pb.NodeID]*Engine
}

func (b *graphTransactionRecorder) ReplicateMarshalled(n *pb.OffsetNotify, _ []byte, peer pb.NodeID) ([]*pb.NackNotify, error) {
	eff, err := parseWireEffect(n.GetEffectData())
	if err != nil {
		return nil, err
	}

	if eff.GetSubscription() != nil {
		return nil, nil
	}
	b.deliveries <- graphDelivery{peer, n}
	if eff.GetTxnBind() != nil {
		return b.receivers[peer].HandleRemote(n)
	}

	return nil, nil
}

func TestGraphCrossRegionTransactionCarriesCompleteEvidence(t *testing.T) {
	sender := NewEngine(EngineConfig{DefaultMode: UnsafeMode})

	t.Cleanup(func() {
		if err := sender.Close(); err != nil {
			t.Error(err)
		}
	})
	seedGraph(t, sender, "animals")
	addGraphPredicate(t, sender, "animals", "red", 2, map[string]string{"color": "red"})
	observed := addGraphPredicate(t, sender, "animals", "blue", 3, map[string]string{"color": "blue"})
	b := &graphTransactionRecorder{
		graphRecorder: graphRecorder{mockBroadcaster: mockBroadcaster{peerIDs: []pb.NodeID{2, 3, 4}, allRegionPeersReachable: true}, deliveries: make(chan graphDelivery, 32)},
		receivers:     make(map[pb.NodeID]*Engine),
	}

	for _, peer := range []pb.NodeID{2, 3} {
		receiver := NewEngine(EngineConfig{DefaultMode: UnsafeMode})
		receiver.nodeID = peer

		t.Cleanup(func() {
			if err := receiver.Close(); err != nil {
				t.Error(err)
			}
		})
		seedGraph(t, receiver, "animals")
		receiver.SetBroadcaster(&graphSource{source: sender})
		b.receivers[peer] = receiver
	}

	sender.SetBroadcaster(b)

	for _, peer := range []pb.NodeID{2, 3, 4} {
		sender.addPeerSubscriber(logicalAddress("animals"), peer)
	}

	c := sender.NewContext()
	c.BeginTx()
	txnID := c.txnID
	nodeTips := make(map[string]Tip)

	for _, color := range []string{"red", "blue"} {
		value := graphNode("animals", color, 7)
		label := &pb.Effect{Key: value.GetKey(), IKey: value.GetIKey(), Virtual: []byte(pb.GraphRegionsKey), Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op: pb.EffectOp_INSERT_OP, Collection: pb.CollectionKind_KEYED, Merge: pb.MergeRule_LAST_WRITE_WINS, Id: []byte("color"), Value: &pb.DataEffect_Raw{Raw: []byte(color)},
		}}}

		deps := GraphTip{Key: "animals", GraphTips: observed}
		if err := c.EmitGraph(deps, label); err != nil {
			t.Fatal(err)
		}

		if err := c.EmitGraph(deps, value); err != nil {
			t.Fatal(err)
		}

		nodeTips[color] = c.keys[string(value.GetIKey())].lastOffset
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	binds, verdicts := 0, 0

	for range 6 {
		d := receiveGraphDelivery(t, &b.graphRecorder)
		if d.peer != 2 && d.peer != 3 {
			t.Fatalf("uninterested peer received publication: %d", d.peer)
		}

		eff, err := parseWireEffect(d.notice.GetEffectData())
		if err != nil {
			t.Fatal(err)
		}

		if bind := eff.GetTxnBind(); bind != nil {
			binds++

			if len(bind.GetKeys()) != 2 {
				t.Fatalf("predicate truncated bind: %v", bind)
			}

			for color, tip := range nodeTips {
				if _, ok := b.receivers[d.peer].effectCache.Get(tip); !ok {
					t.Fatalf("peer %d lacks %s validation dependency", d.peer, color)
				}
			}
		} else {
			color := "red"
			if d.peer == 3 {
				color = "blue"
			}

			if string(eff.GetIKey()) != keytrie.InternalKey("animals", color) {
				t.Fatalf("standalone publication reached wrong region: %d %v", d.peer, eff)
			}

			if eff.GetSnapshot() != nil {
				verdicts++

				if eff.GetSnapshot().GetTxnVerdicts()[txnID] != pb.Verdict_WON || eff.GetSnapshot().GetEvidence() == nil {
					t.Fatal("missing complete transaction decision")
				}

				if _, err := b.receivers[d.peer].HandleRemote(d.notice); err != nil {
					t.Fatal(err)
				}
			}
		}
	}

	for _, notice := range b.broadcasts {
		eff, err := parseWireEffect(notice.GetEffectData())
		if err != nil || eff.GetSnapshot().GetTxnVerdicts()[txnID] != pb.Verdict_WON || eff.GetSnapshot().GetEvidence() == nil {
			t.Fatalf("incomplete verdict: %v %v", eff, err)
		}
		verdicts++
		for _, receiver := range b.receivers {
			if _, err := receiver.HandleRemote(notice); err != nil {
				t.Fatal(err)
			}
		}
	}
	if binds != 2 || verdicts != 2 {
		t.Fatalf("binds/verdicts = %d/%d", binds, verdicts)
	}

	for peer, receiver := range b.receivers {
		if receiver.horizon.IsInvisible(txnID) {
			t.Fatalf("peer %d did not receive the transaction decision", peer)
		}
	}

	if len(b.broadcasts) != 2 {
		t.Fatal("missing ordinary verdict publications")
	}
}

func TestGraphCompactionUsesExactNodeSubscribers(t *testing.T) {
	e := NewEngine(EngineConfig{DefaultMode: UnsafeMode})
	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})
	observed := seedGraph(t, e, "animals")
	address := keytrie.InternalKey("animals", "n")
	c := e.NewContext()
	for range 24 {
		if err := emitGraphNode(c, GraphTip{Key: "animals", GraphTips: observed}, graphNode("animals", "n", 2)); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
	e.addPeerSubscriber(address, 2)
	e.addPeerSubscriber(logicalAddress("animals"), 3)
	c = e.NewContext()
	state, _, err := c.GetSnapshotInternal(address)
	if err != nil || state.GetScalar().GetIntVal() != 2 {
		t.Fatalf("read: %v %v", state, err)
	}
	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
	tip := e.index.ContainsInternal(address).Tips()[0]
	eff, ok := e.effectCache.Get(tip)
	if !ok || eff.GetSnapshot() == nil {
		t.Fatal("expected compaction snapshot")
	}
	subs := eff.GetSnapshot().GetState().GetSubscribers()
	if !subs[2] || subs[3] {
		t.Fatalf("snapshot subscribers: %v", subs)
	}
	c = e.NewContext()
	defer c.Abort()
	if err := c.Emit(&pb.Effect{Key: []byte(address), IKey: []byte(address), Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}}}); err != nil {
		t.Fatal(err)
	}
	if c.keys[address].routes != nil {
		t.Fatal("ordinary observation inferred graph routing")
	}
	if got := c.collectSubscribers(); !slices.Equal(got, []pb.NodeID{2}) {
		t.Fatalf("participants: %v", got)
	}
}
