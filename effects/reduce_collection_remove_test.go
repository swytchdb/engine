package effects

import (
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

func TestReduceBranch_ElementRemovalDoesNotDeleteCollection(t *testing.T) {
	for _, collection := range []pb.CollectionKind{pb.CollectionKind_KEYED, pb.CollectionKind_ORDERED} {
		t.Run(collection.String(), func(t *testing.T) {
			remove := makeDataEffect(1, &pb.DataEffect{
				Op: pb.EffectOp_REMOVE_OP, Collection: collection,
				Merge: pb.MergeRule_LAST_WRITE_WINS, Id: []byte("missing"),
			})
			insert := makeDataEffect(2, &pb.DataEffect{
				Op: pb.EffectOp_INSERT_OP, Collection: collection,
				Merge: pb.MergeRule_LAST_WRITE_WINS, Placement: pb.Placement_PLACE_TAIL,
				Id: []byte("present"), Value: &pb.DataEffect_Raw{Raw: []byte("value")},
			})

			seed := ReduceBranch([]*pb.Effect{remove})
			if seed.GetOp() == pb.EffectOp_REMOVE_OP {
				t.Error("element removal was marked as a whole-key tombstone")
			}

			for name, result := range map[string]*pb.ReducedEffect{
				"batch":  ReduceBranch([]*pb.Effect{remove, insert}),
				"seeded": ReduceChain(seed, []*pb.Effect{insert}),
			} {
				t.Run(name, func(t *testing.T) {
					if result.GetOp() == pb.EffectOp_REMOVE_OP {
						t.Error("insertion after an element removal still appears deleted")
					}

					if !result.GetNetRemoves()["missing"] {
						t.Error("folding across a seed discarded the element removal")
					}

					if collection == pb.CollectionKind_KEYED {
						if got := string(result.GetNetAdds()["present"].GetData().GetRaw()); got != "value" {
							t.Fatalf("keyed value = %q, want value", got)
						}
					} else if elems := result.GetOrderedElements(); len(elems) != 1 || string(elems[0].GetData().GetRaw()) != "value" {
						t.Fatalf("ordered value = %v", elems)
					}
				})
			}

			if !seed.GetNetRemoves()["missing"] || len(seed.GetNetAdds()) != 0 || len(seed.GetOrderedElements()) != 0 {
				t.Fatal("folding mutated the seed")
			}
		})
	}
}
