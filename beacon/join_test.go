package beacon

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/swytchdb/engine/cluster"
	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
)

func TestJoinExchangesCachedMembershipAndAttemptsDiscoveredPeers(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	start := func(id pb.NodeID) (*effects.Engine, *cluster.PeerManager) {
		t.Helper()

		e := effects.NewEngine(effects.EngineConfig{NodeID: id})

		t.Cleanup(func() {
			if err := e.Close(); err != nil {
				t.Error(err)
			}
		})

		pm, err := cluster.NewPeerManager(&cluster.ClusterConfig{
			NodeID: e.NodeID(), TLSPassphrase: t.Name(),
			Nodes: []cluster.NodeConfig{{ID: e.NodeID(), Address: "127.0.0.1:0"}},
		}, cluster.NewEngineEffectHandler(e), cluster.NewEngineLogReader(e.EffectCache()))
		if err != nil {
			t.Fatal(err)
		}

		t.Cleanup(pm.Stop)

		if err := pm.Start(ctx); err != nil {
			t.Fatal(err)
		}

		e.SetBroadcaster(pm)

		if err := e.Subscribe(MembershipKey); err != nil {
			t.Fatal(err)
		}

		return e, pm
	}

	seed, seedPeers := start(100)
	reader, readerPeers := start(200)

	// Give the reader an old roster containing the seed. This is already
	// enough to satisfy the old count-based gate, but says nothing about
	// whether a fresh exchange has been attempted.
	seed.SetOnLocalEffect(func(tip effects.Tip, eff *pb.Effect) {
		wire, err := effects.MarshalEffect(eff)
		if err != nil {
			t.Fatal(err)
		}

		if _, err := reader.HandleRemote(effects.BuildOffsetNotify(seed.NodeID(), tip, eff, wire, ctx)); err != nil {
			t.Fatal(err)
		}
	})

	seedBeacon := New(Config{NodeID: seed.NodeID(), AdvertiseAddr: seedPeers.ListenAddr()}, seed, seedPeers)
	if err := seedBeacon.registerSelf(); err != nil {
		t.Fatal(err)
	}

	seed.SetOnLocalEffect(nil)

	// The seed learns a member after that cached view. Its address fails
	// dialing immediately; join must attempt it, not require it to be online.
	write := seed.NewContext()
	if err := write.Emit(buildMemberInsert(300, "invalid-address")); err != nil {
		t.Fatal(err)
	}

	if err := write.Flush(); err != nil {
		t.Fatal(err)
	}

	b := New(Config{NodeID: reader.NodeID(), AdvertiseAddr: readerPeers.ListenAddr(), Passphrase: t.Name()}, reader, readerPeers)

	b.expectedPeers = 1
	if err := b.registerSelf(); err != nil {
		t.Fatal(err)
	}

	readerPeers.UpdateTopology(&cluster.ClusterConfig{
		NodeID: reader.NodeID(), TLSPassphrase: t.Name(),
		Nodes: []cluster.NodeConfig{
			{ID: reader.NodeID(), Address: readerPeers.ListenAddr()},
			{ID: seed.NodeID(), Address: seedPeers.ListenAddr()},
		},
	})

	if err := b.joinMembership(ctx); err != nil {
		t.Fatal(err)
	}

	if len(b.Members()) != 3 || !slices.Contains(readerPeers.PeerIDs(), pb.NodeID(300)) {
		t.Fatalf("join used stale cached membership: members=%v peers=%v", b.Members(), readerPeers.PeerIDs())
	}

	// The exchange is bidirectional: the seed must receive our registration
	// before join returns, even though registration's normal broadcast is async.
	state, _, err := seed.NewReadOnlyContext().GetSnapshot(MembershipKey)
	if err != nil {
		t.Fatal(err)
	}

	if len(parseMembership(state)) != 3 {
		t.Fatalf("seed did not learn joining member: %v", parseMembership(state))
	}
}
