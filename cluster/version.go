package cluster

import (
	"log/slog"

	"github.com/swytchdb/engine/internal/compatversion"
)

// Version is the application build, assigned from main.Version at startup.
// Peer connections announce it for compatibility; Cloud also sends it in the
// swytch-version metadata. Configure it before starting any runtimes.
var Version = "dev"

// MinimumVersion returns the lowest announced application version across self
// and every known member. Disconnected configured members still participate.
// Empty means at least one version is unknown; dev means all members are dev.
func (pm *PeerManager) MinimumVersion() string {
	token := pm.mu.RLock()
	defer pm.mu.RUnlock(token)

	minimum := compatversion.Normalize(pm.localVersion)
	for _, node := range pm.config.Nodes {
		if node.ID != pm.selfID {
			minimum = compatversion.Minimum(minimum, pm.versions[node.ID])
		}
	}
	// A joining peer can speak before its authoritative membership is projected.
	pm.inboundMu.RLock()
	defer pm.inboundMu.RUnlock()

	for id := range pm.inboundConns {
		minimum = compatversion.Minimum(minimum, pm.versions[id])
	}

	return minimum
}

// HandleNodeVersion is an optional PacketHandler extension. Old transports
// ignore the new packet type; their absent announcements retain compatibility.
func (pm *PeerManager) HandleNodeVersion(peerID NodeId, version string) {
	if peerID == pm.selfID {
		return
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.versions[peerID] = compatversion.Normalize(version)
}

func (pm *PeerManager) sendVersionTo(peerID NodeId) {
	if pm.quicTransport == nil {
		return
	}

	packet := append([]byte{PacketTypeNodeVersion}, pm.localVersion...)
	if err := pm.quicTransport.SendDirect(peerID, packet); err != nil {
		slog.Debug("failed to announce node version", "peer", peerID, "error", err)
	}
}
