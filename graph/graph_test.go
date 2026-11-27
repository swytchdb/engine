package graph

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/engine/keytrie"
	"google.golang.org/protobuf/proto"
)

func seedAPIGraph(t *testing.T, e *effects.Engine, key string) (*pb.ReducedEffect, []effects.Tip) {
	t.Helper()

	c := e.NewContext()
	defer c.Abort()

	if err := NewDescriptor(key, "tree").Emit(key, c, nil); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	snap, tips, err := c.GetSnapshot(key)
	if err != nil {
		t.Fatal(err)
	}

	return snap, tips
}

func TestGraphConstructorsAndNewNodeTips(t *testing.T) {
	tips := []effects.Tip{{1, 10}, {2, 20}}
	snap := descriptorSnapshot("tree")

	g, err := GraphFromSnapshot("catalog", snap, tips)
	if err != nil {
		t.Fatal(err)
	}

	address := keytrie.InternalKey("catalog", "node")

	n := g.CreateNode("node", ScalarInt(10).AsEffect())
	if n.tips.Key != "catalog" || len(n.tips.Tips) != 0 || !slices.Equal(n.tips.GraphTips, tips) || string(n.Address()) != address {
		t.Fatalf("new-node observation = %+v", n.tips)
	}

	if g.pending[address] != n || n.Value().Value() != 10 {
		t.Fatal("new node was not registered")
	}

	wrapped := GraphFromDescriptor(g.GetDescriptor())
	if wrapped.GetDescriptor() != g.GetDescriptor() || len(wrapped.pending) != 0 {
		t.Fatal("wrapping must share descriptor, not pending node cache")
	}

	created := CreateGraph("new", "tree")
	if created.GetDescriptor().getKey() != "new" || len(created.GetDescriptor().getTips()) != 0 {
		t.Fatal("new graph must not fabricate observed tips")
	}
}

func TestGraphRelationshipOperations(t *testing.T) {
	g := CreateGraph("catalog", "tree")
	a := g.CreateNode("a", ScalarString("a").AsEffect())

	b := g.CreateNode("b", NewInt64(2))
	for _, direction := range []Direction{Left, Right, Up, Down} {
		g.Link(a, b, direction)
		g.Link(a, b, direction)

		addresses, count := g.NeighborAddresses(a, direction)
		if count != 2 || !slices.Equal(slices.Collect(addresses), []string{string(b.Address()), string(b.Address())}) {
			t.Fatalf("direction %v lost repeated slots", direction)
		}

		reverse, n := g.NeighborAddresses(b, direction)
		if n != 0 || len(slices.Collect(reverse)) != 0 {
			t.Fatal("Link created reciprocal reference")
		}

		neighbors, err := g.GetNeighbors(a, nil, direction, Int64Type)
		if err != nil || len(neighbors) != 2 || neighbors[0] != b || neighbors[1] != b {
			t.Fatalf("cached neighbors: %v", err)
		}

		g.Unlink(a, b, direction)

		_, count = g.NeighborAddresses(a, direction)
		if count != 0 {
			t.Fatal("Unlink retained reference")
		}
	}

	g.AttachChild(a, b, false)

	_, up := g.NeighborAddresses(b, Up)
	if up != 0 {
		t.Fatal("nonreciprocal child attachment added parent")
	}

	g.DetachChild(a, b)
	g.AttachChild(a, b, true)
	_, up = g.NeighborAddresses(b, Up)

	_, down := g.NeighborAddresses(a, Down)
	if up != 1 || down != 1 {
		t.Fatal("reciprocal attachment missing a direction")
	}

	g.DetachChild(a, b)

	if len(a.inner)+len(b.inner) != 0 {
		t.Fatal("detachment deleted a node")
	}

	invalid, count := g.NeighborAddresses(a, Direction(-1))
	if count != 0 || len(slices.Collect(invalid)) != 0 {
		t.Fatal("invalid direction was not empty")
	}
}

func TestGraphGetNodeCacheAndTypeFactory(t *testing.T) {
	e := scalarTestEngine(t)
	snap, tips := seedAPIGraph(t, e, "catalog")
	address := keytrie.InternalKey("catalog", "node")

	writer := e.NewContext()
	defer writer.Abort()

	if err := emitNodeSeed(writer, address, 10); err != nil {
		t.Fatal(err)
	}

	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}

	c := e.NewContext()
	defer c.Abort()

	g, err := GraphFromSnapshot("catalog", snap, tips)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := g.GetNode(address, c, Type[*Int64]{}); err == nil || g.pending[address] != nil {
		t.Fatal("missing type factory must fail without caching a node")
	}

	calls := 0
	typ := Type[*Int64]{New: func() *Int64 {
		calls++

		return &Int64{}
	}}

	n, err := g.GetNode(address, c, typ)
	if err != nil {
		t.Fatal(err)
	}

	if calls != 0 {
		t.Fatal("loading should bind the value lazily")
	}

	value := n.Value()
	if value.Value() != 10 || value != n.Value() || calls != 1 {
		t.Fatal("Value must retain its bound runtime instance")
	}

	cached, err := g.GetNode(address, nil, typ)
	if err != nil || cached != n || calls != 1 {
		t.Fatal("cached GetNode constructed a value or read storage again")
	}

	if _, err := g.GetNode(address, nil, StringType); err == nil {
		t.Fatal("cached node accepted an incompatible runtime type")
	}

	missing, err := g.GetNode(keytrie.InternalKey("catalog", "missing"), c, typ)
	if err != nil || missing.tips.Key != "catalog" {
		t.Fatalf("missing node: %v", err)
	}

	if missing.Value().Value() != 0 || len(missing.fields.getEffects()) != 0 {
		t.Fatal("an absent value must bind empty without queuing effects")
	}

	missing.Value().Add(7)

	if missing.Value().Value() != 7 {
		t.Fatal("empty bound value did not retain edits")
	}

	if err := g.Flush(c); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	reader := e.NewContext()
	defer reader.Abort()

	stored, _, err := reader.GetSnapshotInternal(string(missing.Address()))
	if err != nil || FieldSnapshot(stored, valueKey).GetScalar().GetIntVal() != 7 {
		t.Fatalf("edits to an absent bound value were not published: %v", err)
	}
}

func TestGraphNodeTipsMatchObservedReads(t *testing.T) {
	for _, tx := range []bool{false, true} {
		t.Run(fmt.Sprintf("tx=%t", tx), func(t *testing.T) {
			e := scalarTestEngine(t)
			seedAPIGraph(t, e, "catalog")

			address := keytrie.InternalKey("catalog", "node")

			seed := e.NewContext()
			defer seed.Abort()

			if err := emitNodeSeed(seed, address, 10); err != nil {
				t.Fatal(err)
			}

			if err := seed.Flush(); err != nil {
				t.Fatal(err)
			}

			c := e.NewContext()
			defer c.Abort()

			if tx {
				c.BeginTx()
			}

			desc, graphTips, err := c.GetSnapshot("catalog")
			if err != nil {
				t.Fatal(err)
			}

			_, nodeTips, err := c.GetSnapshotInternal(address)
			if err != nil {
				t.Fatal(err)
			}

			g, err := GraphFromSnapshot("catalog", desc, graphTips)
			if err != nil {
				t.Fatal(err)
			}

			n, err := g.GetNode(address, c, Int64Type)
			if err != nil {
				t.Fatal(err)
			}

			if n.tips.Key != "catalog" || !slices.Equal(n.tips.Tips, nodeTips) || !slices.Equal(n.tips.GraphTips, graphTips) || len(nodeTips) == 0 || len(graphTips) == 0 || slices.Equal(nodeTips, graphTips) {
				t.Fatalf("node/descriptor frontiers confused: %+v", n.tips)
			}

			oldGraph := slices.Clone(graphTips)
			oldNode := slices.Clone(nodeTips)

			other := e.NewContext()
			defer other.Abort()

			if err := other.Emit(&pb.Effect{Key: []byte("catalog"), Virtual: []byte("unrelated"), Kind: &pb.Effect_Data{Data: &pb.DataEffect{Op: pb.EffectOp_INSERT_OP, Value: &pb.DataEffect_IntVal{IntVal: 9}}}}); err != nil {
				t.Fatal(err)
			}

			if err := other.Flush(); err != nil {
				t.Fatal(err)
			}

			_, newGraph, err := other.GetSnapshot("catalog")
			if err != nil {
				t.Fatal(err)
			}

			if slices.Equal(oldGraph, newGraph) {
				t.Fatal("test failed to advance live descriptor")
			}

			n.Value().Add(2).Add(3)
			// Explicit address preparation isolates GraphTip construction; a separate
			// public-API regression below verifies that Graph.Flush must supply it.
			addressNodeEffects(n)

			var (
				mu          sync.Mutex
				emitted     []*pb.Effect
				emittedTips []effects.Tip
			)

			e.SetOnLocalEffect(func(tip effects.Tip, eff *pb.Effect) {
				if string(eff.GetIKey()) == address && eff.GetData() != nil {
					mu.Lock()

					emitted = append(emitted, proto.Clone(eff).(*pb.Effect))
					emittedTips = append(emittedTips, tip)
					mu.Unlock()
				}
			})
			defer e.SetOnLocalEffect(nil)

			if err := g.Flush(c); err != nil {
				t.Fatal(err)
			}

			e.SetOnLocalEffect(nil)
			mu.Lock()
			observed := slices.Clone(emitted)
			observedTips := slices.Clone(emittedTips)
			mu.Unlock()

			if len(observed) != 2 {
				t.Fatalf("emitted %d data effects, want two deltas only", len(observed))
			}

			firstDeps := make([]effects.Tip, 0, len(observed[0].GetDeps()))
			for _, ref := range observed[0].GetDeps() {
				firstDeps = append(firstDeps, effects.Tip{ref.GetNodeId(), ref.GetOffset()})
			}

			for _, tip := range oldNode {
				if !scalarTestDependsOn(t, e, firstDeps, tip) {
					t.Fatal("emission lost observed node ancestry")
				}
			}

			previous := observedTips[0]

			secondDeps := make([]effects.Tip, 0, len(observed[1].GetDeps()))
			for _, ref := range observed[1].GetDeps() {
				secondDeps = append(secondDeps, effects.Tip{ref.GetNodeId(), ref.GetOffset()})
			}

			if !slices.Contains(secondDeps, previous) {
				t.Fatal("same-node effects failed to chain")
			}

			if len(g.pending) != 0 || !slices.Equal(n.tips.Tips, oldNode) || !slices.Equal(n.tips.GraphTips, oldGraph) {
				t.Fatal("Flush changed observed frontiers or retained pending nodes")
			}

			reader := e.NewContext()
			defer reader.Abort()

			before, _, err := reader.GetSnapshotInternal(address)
			if err != nil {
				t.Fatal(err)
			}

			if FieldSnapshot(before, valueKey).GetScalar().GetIntVal() != 10 {
				t.Fatal("Graph.Flush committed the context")
			}

			if err := c.Flush(); tx {
				// The descriptor changed after the transaction's read. The
				// observed tips must remain old and validation must reject it.
				if !errors.Is(err, effects.ErrTxnAborted) {
					t.Fatalf("stale descriptor transaction: %v", err)
				}

				return
			} else if err != nil {
				t.Fatal(err)
			}

			after, _, err := reader.GetSnapshotInternal(address)
			if err != nil {
				t.Fatal(err)
			}

			if FieldSnapshot(after, valueKey).GetScalar().GetIntVal() != 15 {
				t.Fatal("node update lost after context publication")
			}
		})
	}
}

func TestGraphCommitDescriptorBeforeCreatingNodes(t *testing.T) {
	for _, transactional := range []bool{false, true} {
		t.Run(fmt.Sprintf("transaction=%t", transactional), func(t *testing.T) {
			e := scalarTestEngine(t)

			creation := e.NewContext()
			defer creation.Abort()

			if transactional {
				creation.BeginTx()
			}

			initial := CreateGraph("catalog", "tree")
			if err := initial.Flush(creation); err != nil {
				t.Fatal(err)
			}

			if err := creation.Flush(); err != nil {
				t.Fatal(err)
			}

			// Node creation is a separate operation observing the committed descriptor.
			operation := e.NewContext()
			defer operation.Abort()

			if transactional {
				operation.BeginTx()
			}

			descriptor, tips, err := operation.GetSnapshot("catalog")
			if err != nil {
				t.Fatal(err)
			}

			if descriptor.GetTypeTag() != pb.ValueType_TYPE_GRAPH || len(tips) == 0 {
				t.Fatal("descriptor commit did not establish an observable graph")
			}

			g, err := GraphFromSnapshot("catalog", descriptor, tips)
			if err != nil {
				t.Fatal(err)
			}

			node := g.CreateNode("node", ScalarInt(10).AsEffect()).SetRegion("zone", "west")
			if len(node.tips.Tips) != 0 || !slices.Equal(node.tips.GraphTips, tips) {
				t.Fatal("new node must depend on the committed descriptor observation")
			}

			g.GetDescriptor().AddEntryPoint("root", string(node.Address()))

			if err := g.Flush(operation); err != nil {
				t.Fatal(err)
			}

			if err := operation.Flush(); err != nil {
				t.Fatal(err)
			}

			reader := e.NewContext()
			defer reader.Abort()

			snap, _, err := reader.GetSnapshotInternal(string(node.Address()))
			if err != nil {
				t.Fatal(err)
			}

			if FieldSnapshot(snap, valueKey).GetScalar().GetIntVal() != 10 ||
				string(snap.GetPartitions()[regionKey].GetNetAdds()["zone"].GetData().Decompress()) != "west" {
				t.Fatal("node value or region missing after publication")
			}

			desc, _, err := reader.GetSnapshot("catalog")
			if err != nil {
				t.Fatal(err)
			}

			if string(desc.GetPartitions()[pb.GraphEntryPointKey].GetNetAdds()["root"].GetData().Decompress()) != string(node.Address()) {
				t.Fatal("entry point missing after node operation")
			}
		})
	}
}

func TestGraphRejectsNodesBeforeDescriptorCommit(t *testing.T) {
	for _, transactional := range []bool{false, true} {
		t.Run(fmt.Sprintf("transaction=%t", transactional), func(t *testing.T) {
			e := scalarTestEngine(t)

			c := e.NewContext()
			defer c.Abort()

			if transactional {
				c.BeginTx()
			}

			g := CreateGraph("catalog", "tree")

			node := g.CreateNode("node", ScalarInt(10).AsEffect())
			if len(node.tips.GraphTips) != 0 {
				t.Fatal("creation fabricated descriptor tips")
			}

			err := g.Flush(c)
			if err == nil || !strings.Contains(err.Error(), "observed graph frontier") {
				t.Fatalf("node emission without a committed descriptor should be rejected: %v", err)
			}
			// Graph.Flush may have emitted the descriptor before the node failed.
			// Aborting the failed operation must leave neither descriptor nor node.
			c.Abort()

			reader := e.NewContext()
			defer reader.Abort()

			descriptor, _, err := reader.GetSnapshot("catalog")
			if err != nil {
				t.Fatal(err)
			}

			snap, _, err := reader.GetSnapshotInternal(string(node.Address()))
			if err != nil {
				t.Fatal(err)
			}

			if descriptor != nil || snap != nil {
				t.Fatal("failed creation leaked published state")
			}
		})
	}
}

func TestGraphFlushAddressesLoadedNodes(t *testing.T) {
	e := scalarTestEngine(t)
	snap, tips := seedAPIGraph(t, e, "catalog")

	g, err := GraphFromSnapshot("catalog", snap, tips)
	if err != nil {
		t.Fatal(err)
	}

	g.CreateNode("node", ScalarInt(10).AsEffect())

	c := e.NewContext()
	defer c.Abort()

	if err := g.Flush(c); err != nil {
		t.Fatalf("Graph.Flush must address node effects: %v", err)
	}
}

func TestGraphNewNodesKeepIndependentFrontiers(t *testing.T) {
	e := scalarTestEngine(t)
	snap, tips := seedAPIGraph(t, e, "catalog")

	g, err := GraphFromSnapshot("catalog", snap, tips)
	if err != nil {
		t.Fatal(err)
	}

	a := g.CreateNode("a", ScalarInt(1).AsEffect())
	b := g.CreateNode("b", ScalarString("b").AsEffect())
	g.Link(a, b, Down)
	addressNodeEffects(a)
	addressNodeEffects(b)

	type recorded struct {
		key  string
		tip  effects.Tip
		deps []effects.Tip
	}

	var (
		mu      sync.Mutex
		records []recorded
	)

	e.SetOnLocalEffect(func(tip effects.Tip, eff *pb.Effect) {
		if eff.GetData() == nil {
			return
		}

		r := recorded{key: string(eff.GetIKey()), tip: tip}
		for _, dep := range eff.GetDeps() {
			r.deps = append(r.deps, effects.Tip{dep.GetNodeId(), dep.GetOffset()})
		}

		mu.Lock()

		records = append(records, r)
		mu.Unlock()
	})
	defer e.SetOnLocalEffect(nil)

	c := e.NewContext()
	defer c.Abort()

	if err := g.Flush(c); err != nil {
		t.Fatal(err)
	}

	e.SetOnLocalEffect(nil)
	mu.Lock()
	observed := slices.Clone(records)
	mu.Unlock()

	if len(observed) != 3 {
		t.Fatalf("emitted %d effects, want two values and one link", len(observed))
	}

	for _, r := range observed {
		for _, other := range observed {
			if other.key != r.key && slices.Contains(r.deps, other.tip) {
				t.Fatal("relationship or iteration order added a dependency on another node")
			}
		}
	}

	if len(a.tips.Tips) != 0 || len(b.tips.Tips) != 0 {
		t.Fatal("new-node observations acquired node tips")
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
}

func TestGraphGetNodePreservesObservedDeletionTips(t *testing.T) {
	e := scalarTestEngine(t)
	desc, graphTips := seedAPIGraph(t, e, "catalog")
	address := keytrie.InternalKey("catalog", "deleted")

	c := e.NewContext()
	defer c.Abort()

	if err := c.Emit(&pb.Effect{Key: []byte(address), IKey: []byte(address), Kind: &pb.Effect_Data{Data: &pb.DataEffect{Op: pb.EffectOp_REMOVE_OP, Collection: pb.CollectionKind_SCALAR}}}); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	reader := e.NewContext()
	defer reader.Abort()

	snap, nodeTips, err := reader.GetSnapshotInternal(address)
	if err != nil {
		t.Fatal(err)
	}

	if snap != nil || len(nodeTips) == 0 {
		t.Fatal("test requires observed tombstone tips")
	}

	g, err := GraphFromSnapshot("catalog", desc, graphTips)
	if err != nil {
		t.Fatal(err)
	}

	n, err := g.GetNode(address, reader, Int64Type)
	if err != nil {
		t.Fatal(err)
	}

	if !slices.Equal(n.tips.Tips, nodeTips) || !slices.Equal(n.tips.GraphTips, graphTips) {
		t.Fatal("missing value must not discard observed deletion ancestry")
	}
}

// Test fixture for an existing node, independent of the public Graph creation path.
func emitNodeSeed(c *effects.Context, address string, value int64) error {
	return c.Emit(&pb.Effect{Key: []byte(address), IKey: []byte(address), Kind: &pb.Effect_Data{Data: &pb.DataEffect{Op: pb.EffectOp_INSERT_OP, Collection: pb.CollectionKind_KEYED, Id: []byte(valueKey), Value: &pb.DataEffect_IntVal{IntVal: value}}}})
}
