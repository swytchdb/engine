package beacon

import (
	"context"
	"testing"
	"time"

	"github.com/swytchdb/engine/cluster"
	"github.com/swytchdb/engine/effects"
)

// Reproduce the missed-write recovery path seen during the safety late-join
// test, without depending on a scheduling race: emit while the actual engines
// are disconnected, connect their QUIC transports, and trigger anti-entropy.
func TestDiscoveryRecoversMissedPolicyWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	start := func() (*effects.Engine, *cluster.PeerManager) {
		t.Helper()

		e := effects.NewEngine(effects.EngineConfig{})

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

		pm.SetKeyFilterProvider(e)

		if err := pm.Start(ctx); err != nil {
			t.Fatal(err)
		}

		e.SetBroadcaster(pm)

		if err := e.Subscribe(effects.ClusterSafetyKey); err != nil {
			t.Fatal(err)
		}

		return e, pm
	}

	source, sourcePeers := start()
	reader, readerPeers := start()

	emitSafetyTest(t, source, effects.ClusterSafetyKey, "", "unsafe", false)

	value, _, _, err := reader.GetSnapshot(effects.ClusterSafetyKey)
	if err != nil || value != nil {
		t.Fatalf("disconnected reader: value=%v, err=%v", value, err)
	}

	nodes := []cluster.NodeConfig{
		{ID: source.NodeID(), Address: sourcePeers.ListenAddr()},
		{ID: reader.NodeID(), Address: readerPeers.ListenAddr()},
	}
	sourcePeers.UpdateTopology(&cluster.ClusterConfig{NodeID: source.NodeID(), Nodes: nodes, TLSPassphrase: t.Name()})
	readerPeers.UpdateTopology(&cluster.ClusterConfig{NodeID: reader.NodeID(), Nodes: nodes, TLSPassphrase: t.Name()})

	if !sourcePeers.WaitForPeer(ctx, reader.NodeID()) || !readerPeers.WaitForPeer(ctx, source.NodeID()) {
		t.Fatal("peer streams did not connect")
	}

	// The reader already subscribed before connecting, so reads cannot trigger
	// subscription bootstrap. No writes are emitted after connection: only the
	// discovery reply and history fetch can make the missing value visible.
	reader.StartAntiEntropy(3 * time.Second)
	reader.ReconvergeAllKeys()

	deadline := time.Now().Add(5 * time.Second)

	for {
		value, _, _, err = reader.GetSnapshot(effects.ClusterSafetyKey)
		if err != nil {
			t.Fatal(err)
		}

		if string(value.GetScalar().GetRaw()) == "unsafe" {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("discovery did not recover missed policy: got %v", value)
		}

		time.Sleep(time.Millisecond)
	}
}
