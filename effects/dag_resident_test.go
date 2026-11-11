package effects

import (
	"errors"
	"fmt"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

// A short branch reaches S before a longer branch's nested snapshots:
// left -> S; right -> S2 -> B -> S1 -> A -> S. The entire data frontier
// crosses S, irrespective of tip order or fork choice. Its ancestry may be gone.
func TestResidentReconstructionNestedSnapshots(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		for _, absent := range []string{"none", "folded", "required"} {
			t.Run(fmt.Sprintf("reverse=%t/absent=%s", reverse, absent), func(t *testing.T) {
				e := newSnapshotEngine(nil)
				defer func() { _ = e.Close() }()

				old, s, a, s1, b, s2, left, right := Tip{7, 1}, Tip{7, 2}, Tip{7, 3}, Tip{7, 4}, Tip{7, 5}, Tip{7, 6}, Tip{7, 7}, Tip{7, 8}
				snapshot := func(tip, dep Tip, value int64) *pb.Effect {
					eff := cutEffect(tip, dep)
					eff.Kind = &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{State: &pb.ReducedEffect{
						Op: pb.EffectOp_INSERT_OP, Merge: pb.MergeRule_ADDITIVE_INT,
						Collection: pb.CollectionKind_SCALAR, Commutative: true,
						Scalar: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, value),
					}}}

					return eff
				}
				nodes := map[Tip]*pb.Effect{
					old: cutEffect(old), s: snapshot(s, old, 10),
					a: cutEffect(a, s), s1: snapshot(s1, a, 11),
					b: cutEffect(b, s1), s2: snapshot(s2, b, 12),
					left: cutEffect(left, s), right: cutEffect(right, s2),
				}
				nodes[old].GetData().Value = &pb.DataEffect_IntVal{IntVal: 10}

				tips := []Tip{left, right}
				if reverse {
					tips[0], tips[1] = tips[1], tips[0]
					nodes[left].ForkChoiceHash, nodes[right].ForkChoiceHash = nodes[right].GetForkChoiceHash(), nodes[left].GetForkChoiceHash()
				}

				switch absent {
				case "folded":
					delete(nodes, old)
				case "required":
					delete(nodes, b)
				}

				putCutEffects(t, e, nodes)
				e.broadcaster = &cutBroadcaster{load: func(tip Tip) ([]byte, error) {
					t.Fatalf("resident preparation fetched %v", tip)

					return nil, ErrEffectNotFound
				}}
				d := newDag(e, "k", "")

				err := d.prepare(tips)
				if absent == "required" {
					if !errors.Is(err, errEffectNotResident) {
						t.Fatalf("missing required vertex accepted: %v", err)
					}

					return
				}

				if err != nil || d.lcaTip != s {
					t.Fatalf("LCA=%v error=%v, want %v", d.lcaTip, err, s)
				}

				if _, retained := d.nodes[old]; retained {
					t.Fatal("reconstruction retained folded ancestry")
				}

				state, _, err := e.reconstruct("k", tips, "", false)
				if err != nil || state.GetScalar().GetIntVal() != 14 {
					t.Fatalf("reconstruction=%v error=%v, want 14", state, err)
				}
			})
		}
	}
}
