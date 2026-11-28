package graph

import (
	"slices"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/engine/keytrie"
	"google.golang.org/protobuf/proto"
)

func TestNodeFromSnap(t *testing.T) {
	value := (&MapEffect[*StringEffect]{}).Set(valueKey, ScalarString("value").AsEffect()).inner[0]
	region := (&MapEffect[*StringEffect]{}).Set("zone", ScalarString("west").AsEffect()).inner[0]
	region.Virtual = []byte(regionKey)
	link := (&ListEffect[*StringEffect]{}).Append(ScalarString("child").AsEffect()).inner[0]
	link.Virtual = []byte(downKey)
	snap := effects.ReduceChain(nil, []*pb.Effect{value, region, link})
	before := proto.Clone(snap)
	tips := effects.GraphTip{Key: "graph", Tips: []effects.Tip{{1, 2}}, GraphTips: []effects.Tip{{1, 1}}}

	n, err := FromSnap(snap, tips, StringType)
	if err != nil {
		t.Fatal(err)
	}

	if n.fields.Runtime().owner().base == nil || n.Value().Value() != "value" || len(n.fields.Runtime().getEffects()) != 0 || len(n.inner) != 0 {
		t.Fatal("snapshot should seed components without creating effects")
	}

	if !slices.Contains(n.fields.Down().Values(), "child") {
		t.Fatal("existing relationship missing")
	}

	n.SetValue(n.Value().Append("!")).SetRegion("zone", "east").AttachDown(Address("next"))

	if n.Value().Value() != "value!" || len(n.fields.Runtime().getEffects()) != 3 || !slices.Contains(n.fields.Down().Values(), "next") {
		t.Fatal("pending changes not retained")
	}

	if !proto.Equal(snap, before) {
		t.Fatal("modified borrowed snapshot")
	}

	if !slices.Equal(n.tips.Tips, tips.Tips) || !slices.Equal(n.tips.GraphTips, tips.GraphTips) {
		t.Fatal("lost observations")
	}

	if _, err := FromSnap(snap, tips, Type[*String]{}); err == nil {
		t.Fatal("missing factory must return an error")
	}

	empty, err := FromSnap(nil, tips, StringType)
	if err != nil || empty.Value().Value() != "" || len(empty.fields.getEffects()) != 0 {
		t.Fatal("nil snapshot must bind an empty value without effects")
	}

	if _, ok := any(n).(Emits); ok {
		t.Fatal("Node must not implement Emits")
	}

	if _, ok := any(n).(Value); ok {
		t.Fatal("Node must not implement Value")
	}
}

func TestNodeRelationshipsAndDeleteEffects(t *testing.T) {
	n := CreateGraph("test", "test").CreateNode("self", ScalarInt(10).AsEffect())
	target := CreateGraph("test", "test").CreateNode("target", ScalarInt(20).AsEffect())
	n.AttachUp(target.address).AttachDown(target.address).AttachLeft(n.address).AttachRight(target.address).AttachDown(target.address)

	if len(n.fields.Down().Values()) != 2 || len(n.fields.Up().Values()) != 1 || len(n.fields.Left().Values()) != 1 || len(n.fields.Right().Values()) != 1 {
		t.Fatal("relationship lists must be independent and retain repeated addresses")
	}

	if len(target.fields.Up().Values())+len(target.fields.Down().Values())+len(target.fields.Left().Values())+len(target.fields.Right().Values()) != 0 {
		t.Fatal("attach implicitly changed target")
	}

	n.Delete()
	n.SetValue(ScalarInt(30).AsEffect()).AttachDown(Address("another"))

	if len(n.inner) != 1 || n.inner[0].GetData().GetOp() != pb.EffectOp_REMOVE_OP || n.inner[0].GetData().GetCollection() != pb.CollectionKind_SCALAR || len(n.inner[0].GetVirtual()) != 0 {
		t.Fatal("Delete must create a root removal")
	}

	if n.Value().Value() != 30 || string(n.Address()) != keytrie.InternalKey("test", "self") {
		t.Fatal("Delete must not permanently invalidate the node handle")
	}

	n.Delete()

	if len(n.inner) != 2 || len(target.inner) != 0 {
		t.Fatal("deletion must be local and retained as effects")
	}
}

func TestNodeGeneratedBodyRegionAliases(t *testing.T) {
	e := scalarTestEngine(t)
	desc, tips := seedAPIGraph(t, e, "regions")

	g, err := GraphFromSnapshot("regions", desc, tips)
	if err != nil {
		t.Fatal(err)
	}

	value := NewString("hello")
	n := g.CreateNode("node", value).SetRegion("zone", "west")

	regions := n.fields.Regions()
	if regions != n.fields.Regions() || value != n.Value() {
		t.Fatal("accessors must retain their runtime values")
	}

	zone, found := regions.Get("zone")
	if !found || zone != "west" {
		t.Fatalf("region view missed generated write: %q", zone)
	}

	regions.Set("zone", "east")
	n.SetRegion("zone", "north")

	zone, found = regions.Get("zone")
	if !found || zone != "north" {
		t.Fatalf("interleaved region writes lost call order: %q", zone)
	}

	regions.Replace(map[string]string{"rack": "one"})
	n.SetRegion("zone", "south")
	value.Append("!")
	n.AttachDown(Address("child"))

	// Delete remains terminal even if more body mutations follow it.
	deleted := g.CreateNode("deleted", NewString("old"))
	deleted.Delete()
	deleted.Value().Set("new")
	deleted.AttachRight(Address("peer"))

	c := e.NewContext()
	defer c.Abort()

	if err := g.Flush(c); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	reader := e.NewContext()
	defer reader.Abort()

	snap, _, err := reader.GetSnapshotInternal(string(n.Address()))
	if err != nil {
		t.Fatal(err)
	}

	loaded, err := FromSnap(snap, effects.GraphTip{}, StringType)
	if err != nil {
		t.Fatal(err)
	}

	if loaded.Value().Value() != "hello!" || len(loaded.fields.Runtime().getEffects()) != 0 {
		t.Fatal("region replacement affected the value or loading queued effects")
	}

	for key, want := range map[string]string{"rack": "one", "zone": "south"} {
		got, found := loaded.fields.Regions().Get(key)
		if !found || got != want {
			t.Fatalf("reloaded region %q = %q, found %t; want %q", key, got, found, want)
		}
	}

	neighbors, count := g.NeighborAddresses(loaded, Down)
	if count != 1 || !slices.Equal(slices.Collect(neighbors), []string{"child"}) {
		t.Fatal("region replacement affected relationships")
	}

	snap, _, err = reader.GetSnapshotInternal(string(deleted.Address()))
	if err != nil || snap != nil {
		t.Fatalf("body effects resurrected a deleted node: %v", err)
	}
}

// Prepare addresses explicitly for tests that isolate graph frontier behavior.
func addressNodeEffects[T Value](n *Node[T]) {
	for _, batch := range [][]*pb.Effect{n.fields.Runtime().getEffects(), n.inner} {
		for _, eff := range batch {
			eff.Key = n.address
			eff.IKey = n.address
		}
	}
}

func TestNodeEmissionDeleteAndReuseAddress(t *testing.T) {
	for _, tx := range []bool{false, true} {
		name := "context"
		if tx {
			name = "transaction"
		}

		t.Run(name, func(t *testing.T) {
			e := scalarTestEngine(t)

			init := e.NewContext()
			defer init.Abort()

			if err := NewDescriptor("nodes", "test").Emit("nodes", init, nil); err != nil {
				t.Fatal(err)
			}

			if err := init.Flush(); err != nil {
				t.Fatal(err)
			}

			_, graphTips, err := init.GetSnapshot("nodes")
			if err != nil {
				t.Fatal(err)
			}

			address := Address(keytrie.InternalKey("nodes", "reusable"))

			c := e.NewContext()
			defer c.Abort()

			if tx {
				c.BeginTx()
			}

			deps := effects.GraphTip{Key: "nodes", GraphTips: graphTips}
			first := CreateGraph("nodes", "test").CreateNode("reusable", ScalarString("old").AsEffect())
			first.tips = deps
			first.AttachDown(Address("old-child")).SetRegion("zone", "west")
			addressNodeEffects(first)

			if err := first.EmitGraphNode(c); err != nil {
				t.Fatal(err)
			}

			deletion := CreateGraph("nodes", "test").CreateNode("reusable", &StringEffect{})
			deletion.tips = deps
			deletion.Delete()
			addressNodeEffects(deletion)

			if err := deletion.EmitGraphNode(c); err != nil {
				t.Fatal(err)
			}

			recreated := CreateGraph("nodes", "test").CreateNode("reusable", ScalarString("new").AsEffect())
			recreated.tips = deps
			recreated.AttachRight(Address("new-peer")).SetRegion("zone", "east")
			custom := recreated.fields.Child("custom")
			custom.ApplyField("payload", &pb.DataEffect{Op: pb.EffectOp_INSERT_OP, Value: &pb.DataEffect_Raw{Raw: []byte("custom")}})
			addressNodeEffects(recreated)

			if err := recreated.EmitGraphNode(c); err != nil {
				t.Fatal(err)
			}

			if err := c.Flush(); err != nil {
				t.Fatal(err)
			}

			reader := e.NewContext()
			defer reader.Abort()

			snap, _, err := reader.GetSnapshotInternal(string(address))
			if err != nil {
				t.Fatal(err)
			}

			if snap == nil {
				t.Fatal("engine returned nil for the recreated node")
			}

			if string(FieldSnapshot(snap, valueKey).GetScalar().Decompress()) != "new" || string(snap.GetPartitions()["custom"].GetNetAdds()["payload"].GetData().Decompress()) != "custom" {
				t.Fatal("recreated payload or explicit partition missing")
			}

			if len(snap.GetPartitions()[downKey].GetOrderedElements()) != 0 {
				t.Fatal("old relationships survived deletion")
			}

			if string(snap.GetPartitions()[rightKey].GetOrderedElements()[0].GetData().Decompress()) != "new-peer" {
				t.Fatal("recreated relationship missing")
			}

			if string(snap.GetPartitions()[regionKey].GetNetAdds()["zone"].GetData().Decompress()) != "east" {
				t.Fatal("recreated region missing")
			}
		})
	}
}
