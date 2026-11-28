package bplus

import (
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/engine/keytrie"
)

// This reproduction uses only public engine APIs, without any tree operations.
// A new descriptor must be published before emitting nodes against it.
func TestEmitGraphRequiresEstablishedDescriptor(t *testing.T) {
	for _, published := range []bool{false, true} {
		name := "pending"
		if published {
			name = "published_control"
		}

		t.Run(name, func(t *testing.T) {
			e := testEngine(t)

			c := e.NewContext()
			defer func() { c.Abort() }()

			c.BeginTx()

			if _, _, err := c.GetSnapshot("graph"); err != nil {
				t.Fatal(err)
			}

			for _, eff := range pb.NewGraphEffects([]byte("graph"), []byte("example")) {
				if err := c.Emit(eff); err != nil {
					t.Fatal(err)
				}
			}

			if published {
				flush(t, c)
				c = e.NewContext()
				c.BeginTx()
			}

			state, tips, err := c.GetSnapshot("graph")
			if err != nil || state.GetTypeTag() != pb.ValueType_TYPE_GRAPH {
				t.Fatalf("descriptor read: %v %v", state, err)
			}

			address := []byte(keytrie.InternalKey("graph", "node"))

			err = c.EmitGraph(effects.GraphTip{Key: "graph", GraphTips: tips}, &pb.Effect{
				Key: address, IKey: address, Virtual: []byte(pb.GraphRegionsKey),
				Kind: &pb.Effect_Data{Data: &pb.DataEffect{
					Op: pb.EffectOp_INSERT_OP, Collection: pb.CollectionKind_KEYED,
					Id: []byte("lineage"), Value: &pb.DataEffect_Raw{Raw: []byte("r")},
				}},
			})
			if !published {
				if err == nil {
					t.Fatal("emission accepted an unestablished descriptor")
				}

				return
			}

			if err != nil {
				t.Fatalf("GetSnapshot returned GRAPH, but EmitGraph rejected its frontier: %v", err)
			}

			flush(t, c)
		})
	}
}
