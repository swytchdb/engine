package graph

import (
	"slices"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"google.golang.org/protobuf/proto"
)

func compositeInt(value int64) *pb.DataEffect {
	return &pb.DataEffect{Op: pb.EffectOp_INSERT_OP, Value: &pb.DataEffect_IntVal{IntVal: value}}
}

func TestCompositeSharedOrderAndRelativeEmission(t *testing.T) {
	var root Composite

	left, right := root.Child("left"), root.Child("right")
	leaf := left.Child("leaf")
	left.ApplyField("x", compositeInt(1))
	root.ApplyField("x", compositeInt(2))
	right.ApplyField("x", compositeInt(3))
	leaf.ApplyField("x", compositeInt(4))
	left.ApplyField("x", compositeInt(5))

	generated := root.getEffects()
	paths := make([]string, 0, len(generated))
	values := make([]int64, 0, len(generated))

	for _, effect := range generated {
		paths = append(paths, string(effect.GetVirtual()))
		values = append(values, effect.GetData().GetIntVal())
	}

	if !slices.Equal(paths, []string{"left", "", "right", "left/leaf", "left"}) || !slices.Equal(values, []int64{1, 2, 3, 4, 5}) {
		t.Fatalf("order = %v/%v", paths, values)
	}
	// Emitting a child mounts only that subtree, with its own path stripped.
	e := scalarTestEngine(t)

	c := e.NewContext()
	defer c.Abort()

	if err := left.EmitPartition("key", "mounted", c, nil); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	reader := e.NewContext()
	defer reader.Abort()

	snap, _, err := reader.GetSnapshot("key")
	if err != nil {
		t.Fatal(err)
	}

	if len(snap.GetPartitions()) != 2 || snap.GetPartitions()["mounted"].GetNetAdds()["x"].GetData().GetIntVal() != 5 || snap.GetPartitions()["mounted/leaf"].GetNetAdds()["x"].GetData().GetIntVal() != 4 {
		t.Fatal("child emission included siblings or lost relative paths")
	}
}

func TestCompositePathBoundariesAndProjection(t *testing.T) {
	var root Composite

	literal := root.Child("a/b")
	nested := root.Child("a")
	nested = nested.Child("b")
	tilde := root.Child("a~1b")

	literal.ApplyField("x", compositeInt(1))
	nested.ApplyField("x", compositeInt(2))
	tilde.ApplyField("x", compositeInt(3))

	if literal.Field("x").GetIntVal() != 1 || nested.Field("x").GetIntVal() != 2 || tilde.Field("x").GetIntVal() != 3 {
		t.Fatal("partition name collision")
	}

	snap := root.Snapshot()
	before := proto.Clone(snap)

	projected := PartitionSnapshot(snap, "a")
	if len(projected.GetPartitions()) != 1 || projected.GetPartitions()["b"].GetNetAdds()["x"].GetData().GetIntVal() != 2 {
		t.Fatal("subtree projection included similarly named siblings")
	}

	loaded := CompositeFromSnap(projected)

	child := loaded.Child("b")
	if child.Field("x").GetIntVal() != 2 || len(loaded.getEffects()) != 0 {
		t.Fatal("snapshot read queued effects")
	}

	child.ApplyField("x", compositeInt(4))

	if !proto.Equal(snap, before) {
		t.Fatal("subtree view mutated original snapshot")
	}
}

func TestCompositeMountedClearPreservesSiblings(t *testing.T) {
	e := scalarTestEngine(t)

	c := e.NewContext()
	defer c.Abort()

	var root Composite
	root.ApplyField("x", compositeInt(1))
	child := root.Child("nested")
	child.ApplyField("x", compositeInt(2))

	if err := root.EmitPartition("key", "object", c, nil); err != nil {
		t.Fatal(err)
	}

	if err := ScalarString("keep").AsEffect().EmitPartition("key", "objectSibling", c, nil); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	update := e.NewContext()
	defer update.Abort()

	snap, tips, err := update.GetSnapshot("key")
	if err != nil {
		t.Fatal(err)
	}

	loaded := CompositeFromSnap(PartitionSnapshot(snap, "object"))
	loaded.Clear()

	if err := loaded.EmitPartition("key", "object", update, tips); err != nil {
		t.Fatal(err)
	}

	if err := update.Flush(); err != nil {
		t.Fatal(err)
	}

	reader := e.NewContext()
	defer reader.Abort()

	snap, _, err = reader.GetSnapshot("key")
	if err != nil {
		t.Fatal(err)
	}

	if snap.GetPartitions()["object"].GetOp() != pb.EffectOp_REMOVE_OP || snap.GetPartitions()["object/nested"].GetOp() != pb.EffectOp_REMOVE_OP || string(snap.GetPartitions()["objectSibling"].GetScalar().Decompress()) != "keep" {
		t.Fatal("mounted clear missed descendants or removed a sibling")
	}
}
