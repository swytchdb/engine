package cluster

import (
	"testing"
	"time"
)

func TestMinimumVersionIncludesUnreachableAndUnknownMembers(t *testing.T) {
	pm, err := NewPeerManager(&ClusterConfig{Version: "dev", NodeID: 1, Nodes: []NodeConfig{{ID: 1}, {ID: 2}, {ID: 3}}}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	if got := pm.MinimumVersion(); got != "" {
		t.Fatalf("unknown peers yielded %q", got)
	}

	pm.HandleNodeVersion(2, "v1.8.0")
	pm.HandleNodeVersion(3, "1.7.9")
	// No peer is connected. A connection/liveness filter would incorrectly
	// ignore the older member and enable evidence-free snapshots.
	if got := pm.MinimumVersion(); got != "v1.7.9" {
		t.Fatalf("minimum=%q", got)
	}

	pm.UpdateTopology(&ClusterConfig{NodeID: 1, Nodes: []NodeConfig{{ID: 1}, {ID: 2}}})

	if got := pm.MinimumVersion(); got != "v1.8.0" {
		t.Fatalf("after removal=%q", got)
	}

	if _, exists := pm.versions[3]; exists {
		t.Fatal("removed peer retained its version")
	}

	pm.HandleNodeVersion(2, "dev")

	if got := pm.MinimumVersion(); got != "dev" {
		t.Fatalf("all-dev minimum=%q", got)
	}

	pm.inboundConns[4] = nil // identified inbound peer awaiting roster projection
	if got := pm.MinimumVersion(); got != "" {
		t.Fatalf("unannounced joining peer yielded %q", got)
	}

	pm.HandleNodeVersion(4, "1.8.0-rc.1")

	if got := pm.MinimumVersion(); got != "v1.8.0-rc.1" {
		t.Fatalf("inbound minimum=%q", got)
	}
}

func TestNodeVersionPacketDispatch(t *testing.T) {
	pm, err := NewPeerManager(&ClusterConfig{Version: "dev", NodeID: 1, Nodes: []NodeConfig{{ID: 1}, {ID: 2}}}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	transport := NewQUICNotifyTransport(1, pm, nil, nil)
	for _, tc := range []struct{ payload, want string }{
		{"1.8.0", "v1.8.0"}, {"dev", "dev"}, {"invalid", ""}, {"", ""},
	} {
		transport.dispatchPlaintext(2, append([]byte{PacketTypeNodeVersion}, tc.payload...))

		if got := pm.MinimumVersion(); got != tc.want {
			t.Fatalf("packet %q: minimum=%q", tc.payload, got)
		}
	}

	transport.dispatchPlaintext(2, append([]byte{PacketTypeNodeVersion}, make([]byte, 257)...))

	if got := pm.MinimumVersion(); got != "" {
		t.Fatalf("oversized announcement yielded %q", got)
	}
}

func TestPeerVersionExchange(t *testing.T) {
	for _, tc := range []struct{ name, first, second, minimum string }{
		{"dev", "dev", "dev", "dev"},
		{"modern", "dev", "1.8.0", "v1.8.0"},
		{"legacy", "1.8.0", "1.7.9", "v1.7.9"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tls := generateTestTLSConfig(t)
			peers := make([]*PeerManager, 0, 2)

			for i, version := range []string{tc.first, tc.second} {
				id := NodeId(i + 1)

				pm, err := NewPeerManager(&ClusterConfig{Version: version, NodeID: id, Nodes: []NodeConfig{{ID: id, Address: "127.0.0.1:0"}}}, &recordingHandler{}, &mockLogReader{})
				if err != nil {
					t.Fatal(err)
				}

				pm.serverTLS, pm.clientTLS = tls, tls
				if err := pm.Start(t.Context()); err != nil {
					t.Fatal(err)
				}

				t.Cleanup(pm.Stop)
				peers = append(peers, pm)
			}
			// Only the first node dials; the second must announce over its
			// inbound connection, before it has an outbound topology entry.
			peers[0].UpdateTopology(&ClusterConfig{NodeID: 1, Nodes: []NodeConfig{
				{ID: 1, Address: peers[0].ListenAddr()}, {ID: 2, Address: peers[1].ListenAddr()},
			}})

			deadline := time.Now().Add(5 * time.Second)

			for {
				announced := true

				for i, pm := range peers {
					token := pm.mu.RLock()
					_, known := pm.versions[NodeId(2-i)]
					pm.mu.RUnlock(token)

					announced = announced && known
				}

				first, second := peers[0].MinimumVersion(), peers[1].MinimumVersion()
				if announced && first == tc.minimum && second == tc.minimum {
					break
				}

				if time.Now().After(deadline) {
					t.Fatalf("version exchange: %q/%q, want %q", first, second, tc.minimum)
				}

				time.Sleep(10 * time.Millisecond)
			}
		})
	}
}
