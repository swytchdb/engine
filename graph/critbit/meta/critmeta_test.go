package meta

import (
	"fmt"
	"math"
	"slices"
	"sync"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/engine/graph"
	"google.golang.org/protobuf/proto"
)

func TestCritMetaLocalValues(t *testing.T) {
	m := NewCritMeta()
	if m.IsLeaf() || m.BytePos() != 0 || m.OtherBits() != 0 || m.Key() != "" {
		t.Fatal("new metadata did not start with zero values")
	}

	input := []byte{0, 255, 128}
	m.SetIsLeaf(true).SetBytePos(math.MaxUint32).SetOtherBits(math.MaxUint8).SetKey(string(input))
	input[0] = 9
	got := []byte(m.Key())
	got[1] = 0

	if !m.IsLeaf() || m.BytePos() != math.MaxUint32 || m.OtherBits() != math.MaxUint8 || m.Key() != string([]byte{0, 255, 128}) {
		t.Fatal("local values or byte ownership are incorrect")
	}

	m.SetIsLeaf(false).SetBytePos(0).SetOtherBits(0).SetKey("")

	if m.IsLeaf() || m.BytePos() != 0 || m.OtherBits() != 0 || len(m.Key()) != 0 {
		t.Fatal("zero-valued assignments did not replace existing fields")
	}

	loaded, err := CritMetaFromSnap(nil)
	if err != nil || loaded.IsLeaf() || loaded.BytePos() != 0 || loaded.OtherBits() != 0 || loaded.Key() != "" {
		t.Fatalf("nil snapshot: %v", err)
	}
}

func TestCritMetaNodeRoundTrip(t *testing.T) {
	for _, tx := range []bool{false, true} {
		t.Run(fmt.Sprintf("transaction=%t", tx), func(t *testing.T) {
			e := effects.NewTestEngine()

			t.Cleanup(func() {
				if err := e.Close(); err != nil {
					t.Error(err)
				}
			})

			newContext := func() *effects.Context {
				c := e.NewContext()
				t.Cleanup(c.Abort)

				if tx {
					c.BeginTx()
				}

				return c
			}

			creation := newContext()
			if err := graph.CreateGraph("crit", "critbit/v1").Flush(creation); err != nil {
				t.Fatal(err)
			}

			if err := creation.Flush(); err != nil {
				t.Fatal(err)
			}

			loadGraph := func(c *effects.Context) *graph.Graph {
				snap, tips, err := c.GetSnapshot("crit")
				if err != nil {
					t.Fatal(err)
				}

				g, err := graph.GraphFromSnapshot("crit", snap, tips)
				if err != nil {
					t.Fatal(err)
				}

				return g
			}
			commit := func(g *graph.Graph, c *effects.Context) {
				if err := g.Flush(c); err != nil {
					t.Fatal(err)
				}

				if err := c.Flush(); err != nil {
					t.Fatal(err)
				}
			}
			c := newContext()
			g := loadGraph(c)
			value := NewCritMeta().SetIsLeaf(true).SetBytePos(math.MaxUint32).SetOtherBits(128).SetKey(string([]byte{0, 255}))
			// Unknown fields use the ordinary raw keyed representation and survive updates.
			value.ApplyField("extension", &pb.DataEffect{Op: pb.EffectOp_INSERT_OP, Value: &pb.DataEffect_Raw{Raw: []byte("kept")}})
			node := g.CreateNode("root", value)
			address := string(node.Address())

			var (
				mu      sync.Mutex
				emitted []*pb.Effect
			)

			e.SetOnLocalEffect(func(_ effects.Tip, effect *pb.Effect) {
				if string(effect.GetIKey()) == address && effect.GetData() != nil {
					mu.Lock()

					emitted = append(emitted, proto.Clone(effect).(*pb.Effect))
					mu.Unlock()
				}
			})
			defer e.SetOnLocalEffect(nil)

			commit(g, c)

			read := newContext()

			snapshot, _, err := read.GetSnapshotInternal(address)
			if err != nil {
				t.Fatal(err)
			}

			base := snapshot.GetPartitions()["value"]

			before := proto.Clone(base)
			if base.GetCollection() != pb.CollectionKind_KEYED || base.GetNetAdds()["isLeaf"].GetData().GetIntVal() != 1 || base.GetNetAdds()["bytePos"].GetData().GetIntVal() != math.MaxUint32 {
				t.Fatal("metadata did not use the ordinary keyed scalar representation")
			}

			g = loadGraph(read)

			loaded, err := g.GetNode(address, read, CritMetaType)
			if err != nil {
				t.Fatal(err)
			}

			if !loaded.Value().IsLeaf() || loaded.Value().BytePos() != math.MaxUint32 || loaded.Value().OtherBits() != 128 || loaded.Value().Key() != string([]byte{0, 255}) {
				t.Fatal("metadata changed after commit and reload")
			}

			commit(g, read) // Reading alone must emit nothing.
			mu.Lock()
			count := len(emitted)
			mu.Unlock()

			if count != 5 {
				t.Fatalf("read-only load emitted data: total=%d", count)
			}

			update := newContext()
			g = loadGraph(update)

			loaded, err = g.GetNode(address, update, CritMetaType)
			if err != nil {
				t.Fatal(err)
			}

			loaded.Value().SetIsLeaf(false).SetBytePos(7).SetIsLeaf(true)
			commit(g, update)

			if !proto.Equal(base, before) {
				t.Fatal("updates mutated the borrowed snapshot")
			}

			final := newContext()
			g = loadGraph(final)

			loaded, err = g.GetNode(address, final, CritMetaType)
			if err != nil {
				t.Fatal(err)
			}

			if !loaded.Value().IsLeaf() || loaded.Value().BytePos() != 7 || loaded.Value().OtherBits() != 128 || string(loaded.Value().Field("extension").Decompress()) != "kept" {
				t.Fatal("updated or untouched fields were lost")
			}

			mu.Lock()
			records := slices.Clone(emitted)
			mu.Unlock()

			fields := make([]string, 0, len(records))
			for _, effect := range records {
				fields = append(fields, string(effect.GetData().GetId()))
				if string(effect.GetVirtual()) != "value" || effect.GetData().GetCollection() != pb.CollectionKind_KEYED {
					t.Fatal("generated effect was not addressed to the node's value collection")
				}
			}

			if !slices.Equal(fields, []string{"isLeaf", "bytePos", "otherBits", "key", "extension", "isLeaf", "bytePos", "isLeaf"}) {
				t.Fatalf("emitted fields = %v", fields)
			}
		})
	}
}
