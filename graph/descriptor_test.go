package graph

import (
	"fmt"
	"slices"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"google.golang.org/protobuf/proto"
)

func descriptorSnapshot(algorithm string) *pb.ReducedEffect {
	algorithmEffect := (&MapEffect[*StringEffect]{}).Set(pb.GraphAlgorithmKey, ScalarString(algorithm).AsEffect()).inner[0]
	snap := effects.ReduceChain(nil, []*pb.Effect{algorithmEffect})
	snap.TypeTag = pb.ValueType_TYPE_GRAPH
	snap.Partitions = map[string]*pb.ReducedEffect{
		pb.GraphEntryPointKey: effects.ReduceChain(nil, (&MapEffect[*StringEffect]{}).Set("root", ScalarString("external-address").AsEffect()).getEffects()),
	}

	return snap
}

func TestDescriptorSnapshot(t *testing.T) {
	snap := descriptorSnapshot("tree")
	before := proto.Clone(snap)
	tips := []effects.Tip{{4, 10}, {7, 20}}

	d, err := DescriptorFromSnap("catalog", snap, tips)
	if err != nil {
		t.Fatal(err)
	}

	if d.getKey() != "catalog" || !slices.Equal(d.getTips(), tips) || d.fields.Algorithm() != "tree" {
		t.Fatal("descriptor lost identity, algorithm, or observed tips")
	}

	if len(d.fields.getEffects()) != 0 || len(d.inner) != 0 {
		t.Fatal("snapshot queued effects")
	}

	d.AddEntryPoint("second", "second-address")

	if string(d.GetEntryPoint("second")) != "second-address" || string(d.GetEntryPoint("root")) != "external-address" {
		t.Fatal("entry-point view lost pending or observed values")
	}

	if !proto.Equal(snap, before) || !slices.Equal(tips, []effects.Tip{{4, 10}, {7, 20}}) {
		t.Fatal("edits changed borrowed observation")
	}

	empty, err := DescriptorFromSnap("empty", nil, nil)
	if err != nil || empty.GetEntryPoint("root") != nil || len(empty.fields.getEffects()) != 0 {
		t.Fatalf("nil snapshot should seed an empty descriptor: %v", err)
	}
}

func TestDescriptorEntryPointsUseRawTags(t *testing.T) {
	d := NewDescriptor("catalog", "tree")
	for _, tag := range []string{"", "root", "\x00\xff"} {
		d.AddEntryPoint(tag, "first")
		d.AddEntryPoint(tag, "second")
	}

	d.RemoveEntryPoint("root")
	d.RemoveEntryPoint("missing")

	entries := d.fields.EntryPoint().Values()
	if len(entries) != 2 || entries[""] != "second" || entries["\x00\xff"] != "second" {
		t.Fatalf("entry points = %v", entries)
	}

	address := d.GetEntryPoint("")
	address[0] = 'X'

	if string(d.GetEntryPoint("")) != "second" || d.GetEntryPoint("root") != nil {
		t.Fatal("entry-point reads must return independent bytes or nil when absent")
	}

	if len(d.getTips()) != 0 {
		t.Fatal("constructing edits must not fabricate a frontier")
	}
}

func TestDescriptorCompositionBoundary(t *testing.T) {
	var d Descriptor = NewDescriptor("catalog", "tree")
	if _, ok := d.(Emits); ok {
		t.Fatal("descriptor must not implement Emits")
	}

	if _, ok := d.(Value); ok {
		t.Fatal("descriptor must not implement Value")
	}

	if _, ok := d.(interface {
		EmitPartition(string, string, *effects.Context, []effects.Tip) error
	}); ok {
		t.Fatal("descriptor must not expose partition emission")
	}

	if _, ok := d.(interface{ getEffects() []*pb.Effect }); ok {
		t.Fatal("descriptor must not expose ordinary value effects")
	}
}

func TestDescriptorRoundTrip(t *testing.T) {
	for _, tx := range []bool{false, true} {
		t.Run(fmt.Sprintf("transaction=%t", tx), func(t *testing.T) {
			e := scalarTestEngine(t)
			newContext := func() *effects.Context {
				c := e.NewContext()
				t.Cleanup(c.Abort)

				if tx {
					c.BeginTx()
				}

				return c
			}
			c := newContext()
			d := NewDescriptor("catalog", "tree")
			d.AddEntryPoint("root", "external-address")

			if err := GraphFromDescriptor(d).Flush(c); err != nil {
				t.Fatal(err)
			}

			if err := c.Flush(); err != nil {
				t.Fatal(err)
			}

			update := newContext()

			snap, tips, err := update.GetSnapshot("catalog")
			if err != nil {
				t.Fatal(err)
			}

			if snap.GetTypeTag() != pb.ValueType_TYPE_GRAPH || len(tips) == 0 {
				t.Fatal("descriptor must establish a TYPE_GRAPH frontier")
			}

			if string(snap.GetNetAdds()["algorithm"].GetData().Decompress()) != "tree" ||
				string(snap.GetPartitions()["entry_point"].GetNetAdds()["root"].GetData().Decompress()) != "external-address" ||
				snap.GetPartitions()["entryPoint"] != nil {
				t.Fatal("Go accessor naming changed descriptor storage")
			}

			loaded, err := DescriptorFromSnap("catalog", snap, tips)
			if err != nil {
				t.Fatal(err)
			}

			if len(loaded.fields.getEffects()) != 0 || len(loaded.inner) != 0 {
				t.Fatal("loading queued snapshot or initialization effects")
			}

			loaded.AddEntryPoint("root", "second")
			loaded.RemoveEntryPoint("root")
			loaded.AddEntryPoint("root", "final")

			if err := GraphFromDescriptor(loaded).Flush(update); err != nil {
				t.Fatal(err)
			}

			if !slices.Equal(loaded.getTips(), tips) {
				t.Fatal("emission advanced observed tips")
			}

			if err := update.Flush(); err != nil {
				t.Fatal(err)
			}

			reader := newContext()

			snap, _, err = reader.GetSnapshot("catalog")
			if err != nil {
				t.Fatal(err)
			}

			if snap.GetTypeTag() != pb.ValueType_TYPE_GRAPH ||
				string(snap.GetNetAdds()["algorithm"].GetData().Decompress()) != "tree" ||
				string(snap.GetPartitions()["entry_point"].GetNetAdds()["root"].GetData().Decompress()) != "final" {
				t.Fatal("entry-point mutation order or untouched descriptor fields were lost")
			}
		})
	}
}
