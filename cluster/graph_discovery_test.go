package cluster

import (
	"testing"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/engine/keytrie"
)

// A B+ tree reader can know a child's address without having received its
// publication. Region registration intentionally does not bootstrap that child;
// reading its explicit address must discover its existing state.
func TestGraphNodeDiscoveryFivePeers(t *testing.T) {
	const (
		count = 5
		key   = "graph-discovery"
	)

	engines := make([]*effects.Engine, count)
	handlers := make([]EffectHandler, count)
	readers := make([]LogReader, count)

	identities := make([]NodeId, count)
	for i := range count {
		e := effects.NewEngine(effects.EngineConfig{DefaultMode: effects.SafeMode})
		engines[i] = e
		identities[i] = e.NodeID()
		handlers[i] = NewEngineEffectHandler(e)
		readers[i] = NewEngineLogReader(e.EffectCache())

		t.Cleanup(func() {
			if err := e.Close(); err != nil {
				t.Error(err)
			}
		})
	}

	peers := startNNodeCluster(t, handlers, readers, identities...)
	for i, e := range engines {
		e.SetBroadcaster(peers[i])
	}
	// All peers already subscribe to the descriptor before the node is created.
	for _, e := range engines {
		if err := e.Subscribe(key); err != nil {
			t.Fatal(err)
		}
	}

	c := engines[0].NewContext()
	for _, eff := range pb.NewGraphEffects([]byte(key), []byte("bplus")) {
		if err := c.Emit(eff); err != nil {
			t.Fatal(err)
		}
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	for i, e := range engines {
		deadline := time.Now().Add(5 * time.Second)

		for {
			state, _, _, err := e.GetSnapshot(key)
			if err != nil {
				t.Fatal(err)
			}

			if state.GetTypeTag() == pb.ValueType_TYPE_GRAPH {
				break
			}

			if time.Now().After(deadline) {
				t.Fatalf("peer %d did not receive descriptor", i)
			}

			time.Sleep(time.Millisecond)
		}
	}

	_, descriptorTips, _, err := engines[0].GetSnapshot(key)
	if err != nil {
		t.Fatal(err)
	}

	address := keytrie.InternalKey(key, "root")
	deps := effects.GraphTip{Key: key, GraphTips: descriptorTips}

	c = engines[0].NewContext()
	for _, eff := range []*pb.Effect{
		{Key: []byte(address), IKey: []byte(address), Virtual: []byte(pb.GraphRegionsKey), Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op: pb.EffectOp_INSERT_OP, Collection: pb.CollectionKind_KEYED, Merge: pb.MergeRule_LAST_WRITE_WINS,
			Id: []byte("region"), Value: &pb.DataEffect_Raw{Raw: []byte("r/")},
		}}},
		{Key: []byte(address), IKey: []byte(address), Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op: pb.EffectOp_INSERT_OP, Collection: pb.CollectionKind_SCALAR, Merge: pb.MergeRule_LAST_WRITE_WINS,
			Value: &pb.DataEffect_Raw{Raw: []byte("existing root")},
		}}},
	} {
		if err := c.EmitGraph(deps, eff); err != nil {
			t.Fatal(err)
		}
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	expected, _, _, err := engines[0].GetSnapshotInternal(address)
	if err != nil || string(expected.GetScalar().GetRaw()) != "existing root" {
		t.Fatalf("writer state: %v, %v", expected, err)
	}

	for i := 1; i < count; i++ {
		t.Run(string(rune('0'+i)), func(t *testing.T) {
			e := engines[i]

			c := e.NewContext()
			if err := c.SubscribeGraph(key, "tree", map[string]string{"region": "r/.*"}); err != nil {
				t.Fatal(err)
			}

			if err := c.Flush(); err != nil {
				t.Fatal(err)
			}

			if e.PinKeyInternal(address) {
				e.UnpinKeyInternal(address)
				t.Fatal("predicate registration bootstrapped node state")
			}

			read := e.NewReadOnlyContext()
			defer read.Abort()

			state, tips, err := read.GetSnapshotInternal(address)
			if err != nil {
				t.Fatal(err)
			}

			if string(state.GetScalar().GetRaw()) != "existing root" {
				t.Fatalf("explicit node read returned state=%v tips=%v; writer holds existing root", state, tips)
			}
		})
	}
	// Registration also selects future emissions. Wait for the writer to observe
	// those directory entries, then deliver a fresh address without reader discovery.
	deadline := time.Now().Add(5 * time.Second)

	for {
		state, tips, _, err := engines[0].GetSnapshot(key)
		if err != nil {
			t.Fatal(err)
		}

		if len(state.GetPartitions()[pb.GraphRegionSubscriptionsKey].GetNetAdds()) == count-1 {
			descriptorTips = tips

			break
		}

		if time.Now().After(deadline) {
			t.Fatal("writer did not observe region subscriptions")
		}

		time.Sleep(time.Millisecond)
	}

	fresh := keytrie.InternalKey(key, "child")

	c = engines[0].NewContext()
	for _, eff := range []*pb.Effect{
		{Key: []byte(fresh), IKey: []byte(fresh), Virtual: []byte(pb.GraphRegionsKey), Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op: pb.EffectOp_INSERT_OP, Collection: pb.CollectionKind_KEYED, Id: []byte("region"), Value: &pb.DataEffect_Raw{Raw: []byte("r/child")},
		}}},
		{Key: []byte(fresh), IKey: []byte(fresh), Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op: pb.EffectOp_INSERT_OP, Collection: pb.CollectionKind_SCALAR, Value: &pb.DataEffect_Raw{Raw: []byte("future child")},
		}}},
	} {
		if err := c.EmitGraph(effects.GraphTip{Key: key, GraphTips: descriptorTips}, eff); err != nil {
			t.Fatal(err)
		}
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	for i := 1; i < count; i++ {
		deadline := time.Now().Add(5 * time.Second)
		for !engines[i].PinKeyInternal(fresh) {
			if time.Now().After(deadline) {
				t.Fatalf("peer %d did not receive selected future node", i)
			}

			time.Sleep(time.Millisecond)
		}

		engines[i].UnpinKeyInternal(fresh)
		// Ownership was installed by selected receipt before this read could subscribe.
		for {
			state, _, _, err := engines[i].GetSnapshotInternal(fresh)
			if err != nil {
				t.Fatal(err)
			}

			if string(state.GetScalar().GetRaw()) == "future child" {
				break
			}

			if time.Now().After(deadline) {
				t.Fatalf("peer %d did not receive the value", i)
			}

			time.Sleep(time.Millisecond)
		}
	}
}
