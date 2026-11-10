package cluster

import (
	"fmt"
	"slices"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
)

func TestDiscoverySubscriptionObservationSnapshotBoundary(t *testing.T) {
	for _, initialSubscription := range []bool{false, true} {
		for _, reverseDeps := range []bool{false, true} {
			for _, unsubscribe := range []bool{false, true} {
				t.Run(fmt.Sprintf("initial-%t/reverse-%t/unsubscribe-%t", initialSubscription, reverseDeps, unsubscribe), func(t *testing.T) {
					u, s, d, pruned := effects.Tip{7, 1}, effects.Tip{7, 2}, effects.Tip{7, 3}, effects.Tip{7, 99}
					ref := func(tip effects.Tip) *pb.EffectRef { return &pb.EffectRef{NodeId: tip[0], Offset: tip[1]} }
					data := func(n int64) *pb.DataEffect {
						return &pb.DataEffect{
							Op: pb.EffectOp_INSERT_OP, Merge: pb.MergeRule_ADDITIVE_INT,
							Collection: pb.CollectionKind_SCALAR, Value: &pb.DataEffect_IntVal{IntVal: n},
						}
					}

					sub := &pb.Effect{Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{SubscriberNodeId: 7, Unsubscribe: unsubscribe}}}
					if unsubscribe {
						sub.Deps = []*pb.EffectRef{ref(s)}
					}

					child := &pb.Effect{Deps: []*pb.EffectRef{ref(s), ref(u)}, Kind: &pb.Effect_Data{Data: data(1)}}
					if unsubscribe {
						child.Deps = []*pb.EffectRef{ref(u)}
					}

					if reverseDeps {
						slices.Reverse(child.GetDeps())
					}

					graph := map[effects.Tip]*pb.Effect{
						u: sub,
						s: {Deps: []*pb.EffectRef{ref(pruned)}, Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{State: &pb.ReducedEffect{
							Op: pb.EffectOp_INSERT_OP, Merge: pb.MergeRule_ADDITIVE_INT,
							Collection: pb.CollectionKind_SCALAR, Commutative: true, Scalar: data(10),
						}}}},
						d: child,
					}

					tips := []effects.Tip{d}
					if initialSubscription {
						tips = append(tips, u)
					}

					got, err := reduceDiscoveredTips(tips, func(tip effects.Tip) (*pb.Effect, error) {
						if eff, ok := graph[tip]; ok {
							return eff, nil
						}

						t.Errorf("requested discarded history %v", tip)

						return nil, fmt.Errorf("history %v no longer exists", tip)
					})
					if err != nil || got.GetScalar().GetIntVal() != 11 {
						t.Fatalf("state = %v, error = %v; want 11, nil", got, err)
					}
				})
			}
		}
	}
}
