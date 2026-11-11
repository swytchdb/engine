package effects

import (
	"slices"

	pb "github.com/swytchdb/engine/cluster/proto"
)

// HistoryProgress describes a peer's own publications and one local effect
// whose ancestry it processed. Processed is an exact witness, not a contiguous
// acknowledgement prefix: delivery on independent keys can finish out of order.
type HistoryProgress struct {
	Node, Minted, Processed uint64
	Received                uint64 // our processed witness for this origin
}

type historyProgressProvider interface {
	HistoryProgress() []HistoryProgress
	RecordHistorySent(pb.NodeID, uint64)
	RecordHistoryProcessed(pb.NodeID, uint64)
}

// An overlay freezes reclamation evidence, not the live index or snapshot
// production. The intrusive list has one entry per active context, no effect
// references, and is protected by Engine.historyMu.
type historyOverlay struct {
	progress   []HistoryProgress
	prev, next *historyOverlay
}

func (e *Engine) historyProgress() []HistoryProgress {
	var progress []HistoryProgress
	if provider, ok := e.broadcaster.(historyProgressProvider); ok {
		progress = provider.HistoryProgress()
	}

	return append(progress, HistoryProgress{Node: uint64(e.nodeID), Minted: e.nextOff.Load()})
}

func (e *Engine) pinHistory(overlay *historyOverlay) {
	if overlay.progress != nil { // only the owning context changes this field
		return
	}

	e.historyMu.Lock()
	defer e.historyMu.Unlock()

	overlay.progress = e.historyProgress()

	overlay.next = e.historyOverlays
	if overlay.next != nil {
		overlay.next.prev = overlay
	}

	e.historyOverlays = overlay
}

func (e *Engine) unpinHistory(overlay *historyOverlay) {
	if overlay.progress == nil {
		return
	}

	e.historyMu.Lock()
	defer e.historyMu.Unlock()

	if overlay.prev != nil {
		overlay.prev.next = overlay.next
	} else {
		e.historyOverlays = overlay.next
	}

	if overlay.next != nil {
		overlay.next.prev = overlay.prev
	}

	*overlay = historyOverlay{}
}

// ConstrainHistoryProgress applies active transaction overlays to outgoing
// evidence too: peers must not reclaim history our own transactions still need.
func (e *Engine) ConstrainHistoryProgress(peer pb.NodeID, my, your uint64) (uint64, uint64) {
	e.historyMu.Lock()
	defer e.historyMu.Unlock()

	for overlay := e.historyOverlays; overlay != nil; overlay = overlay.next {
		var received uint64

		for _, p := range overlay.progress {
			if p.Node == uint64(e.nodeID) {
				my = min(my, p.Minted)
			}

			if p.Node == uint64(peer) {
				received = p.Received
			}
		}

		your = min(your, received)
	}

	return my, your
}

func (e *Engine) recordHistorySent(tip Tip, eff *pb.Effect) {
	provider, ok := e.broadcaster.(historyProgressProvider)
	if !ok {
		return
	}

	keys := [][]byte{eff.GetKey()}
	for _, kb := range eff.GetTxnBind().GetKeys() {
		keys = append(keys, kb.GetKey())
	}

	for _, key := range keys {
		// Subscribers remain obligations during partitions. The provider drops
		// departed membership entries, not temporarily unreachable peers.
		for _, peer := range e.PeerSubscribers(string(key)) {
			provider.RecordHistorySent(peer, tip[1])
		}
	}
}

func (d *dag) recordHistoryProcessed() {
	if d.lcaTip != (Tip{}) {
		// The common snapshot covers every branch of this read. An arbitrary
		// visited effect could be on just one branch of an unresolved fork.
		d.engine.recordHistoryProcessed(d.lcaTip, d.nodes[d.lcaTip])
	}
}

func (e *Engine) recordHistoryProcessed(tip Tip, eff *pb.Effect) {
	provider, ok := e.broadcaster.(historyProgressProvider)
	if !ok {
		return
	}
	// A key-scoped walk does not certify every key in a bind. Neither
	// uncommitted data nor subscription metadata certify a data closure.
	if eff != nil && eff.GetTxnId() == "" && eff.GetTxnBind() == nil && eff.GetSubscription() == nil {
		provider.RecordHistoryProcessed(pb.NodeID(tip[0]), tip[1])
	}
}

// historyBoundary is scratch state for the existing LCA trimming traversal.
// historyMu stays held while sampling overlays and releasing creation refs;
// this traversal reads resident effects only and never waits for peer/CDN I/O.
type historyBoundary struct {
	engine *Engine
	peers  []HistoryProgress
}

func (e *Engine) historyBoundary(key string) *historyBoundary {
	h := &historyBoundary{engine: e}

	var members []pb.NodeID
	if e.broadcaster != nil {
		members = e.broadcaster.PeerIDs()
	}

	progress := e.historyProgress()
	for _, peer := range e.PeerSubscribers(key) {
		if !slices.Contains(members, peer) {
			continue
		}

		p := HistoryProgress{Node: uint64(peer)}
		for _, known := range progress {
			if known.Node == p.Node {
				p = known

				break
			}
		}

		for overlay := e.historyOverlays; overlay != nil; overlay = overlay.next {
			pinned := HistoryProgress{}

			for _, known := range overlay.progress {
				if known.Node == p.Node {
					pinned = known

					break
				}
			}

			p.Minted = min(p.Minted, pinned.Minted)
			p.Processed = min(p.Processed, pinned.Processed)
		}

		h.peers = append(h.peers, p)
	}

	return h
}

func (h *historyBoundary) beforeOverlays(tip Tip) bool {
	for overlay := h.engine.historyOverlays; overlay != nil; overlay = overlay.next {
		known := false

		for _, p := range overlay.progress {
			if p.Node == tip[0] {
				known = tip[1] <= p.Minted

				break
			}
		}

		if !known {
			return false
		}
	}

	return true
}
