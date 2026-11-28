package effects

import (
	"bytes"
	"slices"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
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
		graphTips := seedGraph(t, e, key)

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
			for dep := range keyDeps(string(eff.GetIKey()), eff) {
				parent, err := e.getEffect(string(eff.GetIKey()), r(dep))
				if err != nil || !effectHasAddress(parent, string(eff.GetIKey())) {
					t.Fatalf("node depends on another key: %v, %v", dep, err)
				}
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

type graphRecorder struct {
	mockBroadcaster

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

// Ordinary subscription discovery is protocol traffic, not a graph publication.
func (b *graphRecorder) BroadcastWithData(n *pb.OffsetNotify, data []byte) {
	if eff, err := parseWireEffect(n.GetEffectData()); err == nil && eff.GetSubscription() != nil {
		return
	}

	b.mockBroadcaster.BroadcastWithData(n, data)
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
	graphTips := seedGraph(t, sender, "animals")
	seedGraph(t, receiver, "animals")
	receiverTips := slices.Clone(receiver.index.Contains("animals").Tips())
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
	// Descriptor ownership alone does not admit effects for another key.
	nacks, err := receiver.HandleRemote(notice)
	if err != nil || len(nacks) != 1 || !nacks[0].GetNotSubscribed() {
		t.Fatalf("unowned internal key was admitted: %v %v", nacks, err)
	}

	if err := receiver.SubscribeInternal(keytrie.InternalKey("animals", "n")); err != nil {
		t.Fatal(err)
	}

	nacks, err = receiver.HandleRemote(notice)
	if err != nil {
		t.Fatal(err)
	}

	for _, nack := range nacks {
		if nack.GetNotSubscribed() {
			t.Fatalf("remote receipt rejected: %v", nack)
		}
	}

	sibling := keytrie.InternalKey("animals", "unseen")
	if _, subscribed := receiver.subscription(sibling); subscribed || receiver.isServed([]byte(sibling)) || receiver.index.ContainsInternal(sibling) != nil {
		t.Fatal("receipt granted ownership of an unseen sibling")
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

		eff := bindAtTip(t, e, tips[0])
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

	// Model the shared frontier before verdict snapshots arrive on a peer.
	// Local commits now emit a separate verdict snapshot for each address.
	descriptorTips := e.index.Contains("animals")

	snapshot, err := e.getEffect("", descriptorTips.Tips()[0])
	if err != nil {
		t.Fatal(err)
	}

	binds := snapshot.GetSnapshot().GetEvidence().GetBinds()
	if len(binds) != 1 {
		t.Fatalf("expected evidence for one shared bind, got %v", binds)
	}

	bindTip := r(binds[0])
	for _, key := range []string{logicalAddress("animals"), keytrie.InternalKey("animals", "n")} {
		e.updateIndex(key, e.index.ContainsInternal(key), bindTip)
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

	if got := c.collectSubscribers(); !slices.Equal(got, []pb.NodeID{2}) {
		t.Fatalf("participants: %v", got)
	}
}
