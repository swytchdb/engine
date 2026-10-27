package beacon

import (
	"context"
	"fmt"
	"net"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// The only arrival carrying the new member is a snapshot. Checking the
// projection must not read the membership key and accidentally repair it.
func TestMembershipSnapshotUpdatesTopology(t *testing.T) {
	for _, path := range []string{"notify", "backfill"} {
		t.Run(path, func(t *testing.T) {
			testMembershipSnapshotUpdatesTopology(t, path)
		})
	}
}

func testMembershipSnapshotUpdatesTopology(t *testing.T, path string) {
	t.Helper()

	listener, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	port := listener.LocalAddr().(*net.UDPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	address := fmt.Sprintf("127.0.0.1:%d", port)

	rt, err := NewRuntime(t.Context(), RuntimeConfig{
		ClusterPassphrase: t.Name(), ClusterPort: port, AdvertiseAddr: address, JoinAddr: address,
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := rt.Stop(); err != nil {
			t.Error(err)
		}
	})

	state, tips, err := rt.Engine.NewReadOnlyContext().GetSnapshot(MembershipKey)
	if err != nil {
		t.Fatal(err)
	}

	state = proto.Clone(state).(*pb.ReducedEffect)

	const peer = pb.NodeID(42)

	state.NetAdds[string(nodeIDBytes(uint64(peer)))] = &pb.ReducedElement{
		Data: buildMemberInsert(uint64(peer), "127.0.0.1:1").GetData(),
	}
	stamp := timestamppb.Now()

	eff := &pb.Effect{
		Key: []byte(MembershipKey), NodeId: uint64(peer), Hlc: stamp,
		ForkChoiceHash: effects.ComputeForkChoiceHash(peer, stamp),
		Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
			Collection: pb.CollectionKind_KEYED, State: state,
		}},
	}
	for _, tip := range tips {
		eff.Deps = append(eff.Deps, &pb.EffectRef{NodeId: tip[0], Offset: tip[1]})
	}

	wire, err := effects.MarshalEffect(eff)
	if err != nil {
		t.Fatal(err)
	}

	offset := effects.Tip{uint64(peer), 1}
	if path == "backfill" {
		// Model a fetched snapshot already cached before NACK ingestion.
		// Cache presence alone must not update the topology projection.
		if err := rt.Engine.EffectCache().Put(offset, eff); err != nil {
			t.Fatal(err)
		}

		if err := rt.Engine.HandleNack(&pb.NackNotify{
			Key: eff.GetKey(), TipDetails: []*pb.NackTipDetail{{
				Ref: &pb.EffectRef{NodeId: offset[0], Offset: offset[1]},
			}},
		}); err != nil {
			t.Fatal(err)
		}
	} else {
		if _, err := rt.Engine.HandleRemote(effects.BuildOffsetNotify(peer, offset, eff, wire, context.Background())); err != nil {
			t.Fatal(err)
		}
	}

	deadline := time.Now().Add(3 * time.Second)
	for !slices.Contains(rt.PeerManager.PeerIDs(), peer) {
		if time.Now().After(deadline) {
			t.Fatalf("snapshot arrived but topology stayed stale: members=%v, peers=%v", rt.Beacon.Members(), rt.PeerManager.PeerIDs())
		}

		time.Sleep(time.Millisecond)
	}

	// Membership and policy observers share the index-change hook.
	emitSafetyTest(t, rt.Engine, effects.ClusterSafetyKey, "", "unsafe", false)
	waitSafetyTest(t, rt.Engine, map[string]effects.SafetyMode{"ordinary": effects.UnsafeMode})
}

func TestMembershipObservationDoesNotCompact(t *testing.T) {
	e := effects.NewEngine(effects.EngineConfig{})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})

	const address = "127.0.0.1:7380"

	for range 20 {
		ctx := e.NewContext()
		if err := ctx.Emit(buildMemberInsert(uint64(e.NodeID()), address)); err != nil {
			t.Fatal(err)
		}

		if err := ctx.Flush(); err != nil {
			t.Fatal(err)
		}
	}

	b := New(Config{NodeID: e.NodeID(), AdvertiseAddr: address}, e, nil)
	// The unchanged projection needs no PeerManager. A long uncompacted
	// history still must not make observation emit more membership effects.
	b.members = []Member{{NodeID: uint64(e.NodeID()), Addr: address}}

	var minted atomic.Int32

	e.SetOnLocalEffect(func(_ effects.Tip, _ *pb.Effect) { minted.Add(1) })

	for range 20 {
		b.refreshTopology()
	}

	if minted.Load() != 0 {
		t.Fatalf("membership observation emitted %d effects", minted.Load())
	}
}
