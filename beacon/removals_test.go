package beacon

import (
	"maps"
	"slices"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
)

func TestQueuedRemovalsSkipAbsentAndInvalidMembers(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(map[bool]string{false: "raw", true: "compressed"}[compressed], func(t *testing.T) {
			e := effects.NewEngine(effects.EngineConfig{CompressValues: compressed})

			t.Cleanup(func() {
				if err := e.Close(); err != nil {
					t.Error(err)
				}
			})

			ctx := e.NewContext()
			for id, addr := range map[uint64]string{0: "invalid", 1: "remove", 2: "keep", 3: ""} {
				if err := ctx.Emit(buildMemberInsert(id, addr)); err != nil {
					t.Fatal(err)
				}
			}

			if err := ctx.Flush(); err != nil {
				t.Fatal(err)
			}

			var removed []uint64

			e.SetOnLocalEffect(func(_ effects.Tip, eff *pb.Effect) {
				if eff.GetData().GetOp() == pb.EffectOp_REMOVE_OP {
					removed = append(removed, nodeIDFromBytes(eff.GetData().GetId()))
				}
			})

			b := New(Config{}, e, nil)

			for range 2 {
				// An absent entry before a present one exercises in-place filtering.
				for _, id := range []uint64{99, 1, 0, 3} {
					b.removeMember(id)
				}

				b.applyQueuedRemovals()
			}

			if !slices.Equal(removed, []uint64{1}) {
				t.Fatalf("removed %v, want only member 1 once", removed)
			}

			snapshot, _, err := e.NewReadOnlyContext().GetSnapshot(MembershipKey)
			if err != nil {
				t.Fatal(err)
			}

			if got := parseMembership(snapshot); !maps.Equal(got, map[uint64]string{2: "keep"}) {
				t.Fatalf("remaining membership = %v", got)
			}
		})
	}
}

func TestQueuedRemovalsEmptyRoster(t *testing.T) {
	e := effects.NewEngine(effects.EngineConfig{})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})

	e.SetOnLocalEffect(func(_ effects.Tip, _ *pb.Effect) {
		t.Error("removal from an empty roster emitted an effect")
	})

	b := New(Config{}, e, nil)
	b.removeMember(42)
	b.applyQueuedRemovals()
	b.applyQueuedRemovals()
}
