package effects

import (
	"testing"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestFilterSnapshotPreservesPartitionOnlyRoots(t *testing.T) {
	now := time.Unix(100, 0)

	for _, kind := range []pb.CollectionKind{pb.CollectionKind_SCALAR, pb.CollectionKind_KEYED, pb.CollectionKind_ORDERED} {
		for _, op := range []pb.EffectOp{pb.EffectOp_UNKNOWN_OP, pb.EffectOp_REMOVE_OP} {
			t.Run(kind.String()+"/"+op.String(), func(t *testing.T) {
				snap := &pb.ReducedEffect{Collection: kind, Op: op, Partitions: map[string]*pb.ReducedEffect{
					"value": {Scalar: &pb.DataEffect{Value: &pb.DataEffect_Raw{Raw: []byte("payload")}}},
				}}

				before := proto.Clone(snap)
				if got := filterSnapshotAt(snap, now); got != snap {
					t.Fatal("partition-only snapshot was discarded or unnecessarily cloned")
				}

				if !proto.Equal(snap, before) {
					t.Fatal("filter mutated snapshot")
				}

				expired := proto.Clone(snap).(*pb.ReducedEffect)

				expired.ExpiresAt = timestamppb.New(now.Add(-time.Second))
				if filterSnapshotAt(expired, now) != nil {
					t.Fatal("partitions must not bypass key expiry")
				}

				empty := &pb.ReducedEffect{Collection: kind, Op: op}
				if filterSnapshotAt(empty, now) != nil {
					t.Fatal("truly empty root should remain absent")
				}
			})
		}
	}
}

func TestFilterSnapshotExpiredRootElementRetainsPartitions(t *testing.T) {
	now := time.Unix(100, 0)
	snap := &pb.ReducedEffect{Collection: pb.CollectionKind_KEYED, NetAdds: map[string]*pb.ReducedElement{
		"expired": {Data: &pb.DataEffect{Id: []byte("expired")}, ExpiresAt: timestamppb.New(now.Add(-time.Second))},
	}, Partitions: map[string]*pb.ReducedEffect{"child": {Scalar: &pb.DataEffect{Value: &pb.DataEffect_IntVal{IntVal: 42}}}}}
	before := proto.Clone(snap)

	got := filterSnapshotAt(snap, now)
	if got == nil || len(got.GetNetAdds()) != 0 || got.GetPartitions()["child"].GetScalar().GetIntVal() != 42 {
		t.Fatal("element expiry removed the partition's parent")
	}

	if !proto.Equal(snap, before) {
		t.Fatal("element expiry mutated borrowed snapshot")
	}
}

func TestRootDeletionStillRemovesPartitions(t *testing.T) {
	seed := &pb.ReducedEffect{Partitions: map[string]*pb.ReducedEffect{"value": {Scalar: &pb.DataEffect{Value: &pb.DataEffect_IntVal{IntVal: 42}}}}}
	removal := &pb.Effect{Kind: &pb.Effect_Data{Data: &pb.DataEffect{Op: pb.EffectOp_REMOVE_OP, Collection: pb.CollectionKind_SCALAR}}}

	deleted := ReduceChain(seed, []*pb.Effect{removal})
	if len(deleted.GetPartitions()) != 0 || filterSnapshotAt(deleted, time.Unix(100, 0)) != nil {
		t.Fatal("whole-key deletion retained partitions")
	}
}
