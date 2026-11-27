package effects

import (
	"encoding/binary"
	"slices"
	"testing"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCollectionElementsUseScalarReduction(t *testing.T) {
	integer := func(rule pb.MergeRule, v int64) *pb.DataEffect {
		return &pb.DataEffect{Op: pb.EffectOp_INSERT_OP, Merge: rule, Value: &pb.DataEffect_IntVal{IntVal: v}}
	}
	floating := func(rule pb.MergeRule, v float64) *pb.DataEffect {
		return &pb.DataEffect{Op: pb.EffectOp_INSERT_OP, Merge: rule, Value: &pb.DataEffect_FloatVal{FloatVal: v}}
	}

	raw := func(rule pb.MergeRule, v []byte) *pb.DataEffect {
		return &pb.DataEffect{Op: pb.EffectOp_INSERT_OP, Merge: rule, Value: &pb.DataEffect_Raw{Raw: v}}
	}
	for _, kind := range []pb.CollectionKind{pb.CollectionKind_KEYED, pb.CollectionKind_ORDERED} {
		tests := []struct {
			name  string
			chain []*pb.DataEffect
			want  *pb.DataEffect
		}{
			{"integer replacement and deltas", []*pb.DataEffect{integer(pb.MergeRule_LAST_WRITE_WINS, 10), integer(pb.MergeRule_ADDITIVE_INT, 3), integer(pb.MergeRule_ADDITIVE_INT, -1)}, integer(pb.MergeRule_LAST_WRITE_WINS, 12)},
			{"integer deltas", []*pb.DataEffect{integer(pb.MergeRule_ADDITIVE_INT, 3), integer(pb.MergeRule_ADDITIVE_INT, 4)}, integer(pb.MergeRule_ADDITIVE_INT, 7)},
			{"float replacement and deltas", []*pb.DataEffect{floating(pb.MergeRule_LAST_WRITE_WINS, 1.5), floating(pb.MergeRule_ADDITIVE_FLOAT, .5), floating(pb.MergeRule_ADDITIVE_FLOAT, -.25)}, floating(pb.MergeRule_LAST_WRITE_WINS, 1.75)},
			{"float deltas", []*pb.DataEffect{floating(pb.MergeRule_ADDITIVE_FLOAT, .5), floating(pb.MergeRule_ADDITIVE_FLOAT, .25)}, floating(pb.MergeRule_ADDITIVE_FLOAT, .75)},
			{"numeric bytes", []*pb.DataEffect{raw(pb.MergeRule_LAST_WRITE_WINS, []byte("10")), integer(pb.MergeRule_ADDITIVE_INT, 2)}, integer(pb.MergeRule_LAST_WRITE_WINS, 12)},
			{"maximum", []*pb.DataEffect{integer(pb.MergeRule_MAX_INT, 3), integer(pb.MergeRule_MAX_INT, 8), integer(pb.MergeRule_MAX_INT, 2)}, integer(pb.MergeRule_MAX_INT, 8)},
			{"byte maximum", []*pb.DataEffect{raw(pb.MergeRule_MAX_BYTES, []byte{1, 8}), raw(pb.MergeRule_MAX_BYTES, []byte{3, 2, 9})}, raw(pb.MergeRule_MAX_BYTES, []byte{3, 8, 9})},
			{"first write wins", []*pb.DataEffect{raw(pb.MergeRule_FIRST_WRITE_WINS, []byte("first")), raw(pb.MergeRule_FIRST_WRITE_WINS, []byte("second"))}, raw(pb.MergeRule_FIRST_WRITE_WINS, []byte("first"))},
			{"replacement", []*pb.DataEffect{integer(pb.MergeRule_ADDITIVE_INT, 10), integer(pb.MergeRule_LAST_WRITE_WINS, 4)}, integer(pb.MergeRule_LAST_WRITE_WINS, 4)},
		}
		for _, tt := range tests {
			t.Run(kind.String()+"/"+tt.name, func(t *testing.T) {
				var chain []*pb.Effect
				for i, d := range tt.chain {
					d = proto.Clone(d).(*pb.DataEffect)
					d.Collection = kind

					d.Id = []byte("target")
					if kind == pb.CollectionKind_ORDERED {
						d.Placement = pb.Placement_PLACE_TAIL
						if i > 0 {
							d.Placement = pb.Placement_PLACE_SELF
						}
					}

					chain = append(chain, &pb.Effect{Kind: &pb.Effect_Data{Data: d}})
				}

				originals := make([]*pb.Effect, len(chain))
				for i, e := range chain {
					originals[i] = proto.Clone(e).(*pb.Effect)
				}

				check := func(result *pb.ReducedEffect) {
					t.Helper()

					var d *pb.DataEffect
					if kind == pb.CollectionKind_KEYED {
						d = result.GetNetAdds()["target"].GetData()
					} else {
						if len(result.GetOrderedElements()) != 1 {
							t.Fatalf("element count = %d", len(result.GetOrderedElements()))
						}

						d = result.GetOrderedElements()[0].GetData()
					}

					if d.GetIntVal() != tt.want.GetIntVal() || d.GetFloatVal() != tt.want.GetFloatVal() || !slices.Equal(d.Decompress(), tt.want.Decompress()) || d.GetMerge() != tt.want.GetMerge() {
						t.Fatalf("data = %v, want value and merge from %v", d, tt.want)
					}

					if d.GetCollection() != kind || string(d.GetId()) != "target" {
						t.Fatal("scalar reduction lost collection or element identity")
					}
				}
				check(ReduceChain(nil, chain))
				// The same result must hold when any earlier prefix has become a snapshot.
				for split := 1; split < len(chain); split++ {
					seed := ReduceChain(nil, chain[:split])
					before := proto.Clone(seed)
					check(ReduceChain(seed, chain[split:]))

					if !proto.Equal(seed, before) {
						t.Fatal("reduction mutated borrowed snapshot")
					}
				}

				for i, e := range chain {
					if !proto.Equal(e, originals[i]) {
						t.Fatal("reduction mutated input effect")
					}
				}
			})
		}
	}
}

func TestKeyedStringOperationsUseScalarReduction(t *testing.T) {
	effect := func(value string, placement pb.Placement, ref []byte) *pb.Effect {
		return &pb.Effect{Kind: &pb.Effect_Data{Data: &pb.DataEffect{Op: pb.EffectOp_INSERT_OP, Collection: pb.CollectionKind_KEYED, Id: []byte("field"), Placement: placement, Reference: ref, Value: &pb.DataEffect_Raw{Raw: []byte(value)}}}}
	}
	chain := []*pb.Effect{
		effect("b", pb.Placement_PLACE_SELF, nil),
		effect("d", pb.Placement_PLACE_TAIL, nil),
		effect("a", pb.Placement_PLACE_HEAD, nil),
		effect("c", pb.Placement_PLACE_BEFORE, binary.BigEndian.AppendUint64(nil, 2)),
	}

	got := ReduceChain(nil, chain).GetNetAdds()["field"].GetData().Decompress()
	if string(got) != "abcd" {
		t.Fatalf("keyed string = %q, want abcd", got)
	}
}

func TestCollectionElementReductionPreservesMetadata(t *testing.T) {
	for _, kind := range []pb.CollectionKind{pb.CollectionKind_KEYED, pb.CollectionKind_ORDERED} {
		t.Run(kind.String(), func(t *testing.T) {
			ttl := timestamppb.New(time.Unix(100, 0))
			elem := &pb.ReducedElement{Data: &pb.DataEffect{Op: pb.EffectOp_INSERT_OP, Collection: kind, Id: []byte("target"), Value: &pb.DataEffect_IntVal{IntVal: 10}}, Hlc: timestamppb.New(time.Unix(1, 0)), NodeId: 2, ForkChoiceHash: []byte("first"), ExpiresAt: ttl}

			seed := &pb.ReducedEffect{Collection: kind}
			if kind == pb.CollectionKind_KEYED {
				seed.NetAdds = map[string]*pb.ReducedElement{"target": elem}
			} else {
				seed.OrderedElements = []*pb.ReducedElement{elem}
			}

			before := proto.Clone(seed)
			update := &pb.Effect{Hlc: timestamppb.New(time.Unix(3, 0)), NodeId: 4, ForkChoiceHash: []byte("next"), Kind: &pb.Effect_Data{Data: &pb.DataEffect{Op: pb.EffectOp_INSERT_OP, Collection: kind, Placement: pb.Placement_PLACE_SELF, Merge: pb.MergeRule_ADDITIVE_INT, Id: []byte("target"), Value: &pb.DataEffect_IntVal{IntVal: 2}}}}
			read := func(r *pb.ReducedEffect) *pb.ReducedElement {
				if kind == pb.CollectionKind_KEYED {
					return r.GetNetAdds()["target"]
				}

				return r.GetOrderedElements()[0]
			}

			got := read(ReduceChain(seed, []*pb.Effect{update}))
			if got.GetData().GetIntVal() != 12 || !proto.Equal(got.GetExpiresAt(), ttl) || got.GetHlc().GetSeconds() != 1 || got.GetNodeId() != 2 || string(got.GetForkChoiceHash()) != "first" {
				t.Fatalf("additive metadata = %v", got)
			}

			update.GetData().Merge = pb.MergeRule_LAST_WRITE_WINS

			got = read(ReduceChain(seed, []*pb.Effect{update}))
			if got.GetData().GetIntVal() != 2 || got.GetExpiresAt() != nil || got.GetHlc().GetSeconds() != 3 || got.GetNodeId() != 4 || string(got.GetForkChoiceHash()) != "next" {
				t.Fatalf("replacement metadata = %v", got)
			}

			if !proto.Equal(seed, before) {
				t.Fatal("seed metadata mutated")
			}
		})
	}
}
