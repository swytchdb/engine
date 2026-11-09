package effects

import (
	"fmt"
	"math/rand/v2"
	"slices"

	pb "github.com/swytchdb/engine/cluster/proto"
)

// AncestryFetcher streams a selected holder's available closure. have skips
// duplicate payload installation, not traversal. receive installs bytes only;
// isRoot records a culled history boundary. Otherwise nil bytes report a
// missing boundary from a completed stream. Replication
// reconciles further divergence; this is not a cluster-wide absence search.
type AncestryFetcher interface {
	FetchAncestry(key string, root Tip, peers []pb.NodeID, have func(Tip) bool, receive func(Tip, []byte, bool) error) error
}

// VisitAncestry visits resident dependencies, including missing boundaries.
// The consumer bounds the transfer by returning false. It never fetches, adds
// reverse links, publishes a frontier, or interprets snapshot state. Root
// markers travel with the records; they do not hide resident ancestry.
func (p *VertexPool) VisitAncestry(key string, root Tip, visit func(Tip, *pb.Effect, bool) bool) {
	seen := make(map[Tip]struct{})

	stack := []Tip{root}
	for len(stack) > 0 {
		tip := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if _, ok := seen[tip]; ok {
			continue
		}

		seen[tip] = struct{}{}
		v := p.getVertex(tip)

		var eff *pb.Effect

		isRoot := false

		if v != nil {
			eff = v.eff
			isRoot = v.root()
		}

		if !visit(tip, eff, isRoot) {
			return
		}

		if eff != nil {
			for ref := range keyDeps(key, eff) {
				if dep := r(ref); dep != (Tip{}) {
					stack = append(stack, dep)
				}
			}
		}
	}
}

// ancestryPeers uses existing subscription/filter evidence. Unknown filters
// remain candidates. Subscription knowledge and the effect's origin override
// negative filters; filters describe keys, not a complete history inventory.
func (e *Engine) ancestryPeers(key string, root Tip) []pb.NodeID {
	peers := slices.Clone(e.broadcaster.PeerIDs())

	var subscribers []pb.NodeID

	if leaf := e.index.LoadData(key); leaf != nil {
		leaf.subMu.Lock()
		subscribers = slices.Clone(leaf.subscribers)
		leaf.subMu.Unlock()
	}

	e.keyFilterMu.RLock()

	for _, pf := range e.peerKeyFilters {
		if pf.counted {
			peers = append(peers, pf.nodeID)
		}
	}

	slices.Sort(peers)
	peers = slices.Compact(peers)
	peers = slices.DeleteFunc(peers, func(peer pb.NodeID) bool {
		if peer == e.nodeID {
			return true
		}

		if uint64(peer) == root[0] || slices.Contains(subscribers, peer) {
			return false
		}

		i, found := e.peerFilterIndex(peer)

		return found && e.peerKeyFilters[i].hasBulk && !e.peerKeyFilters[i].maybeContains(key)
	})

	e.keyFilterMu.RUnlock()
	rand.Shuffle(len(peers), func(i, j int) { peers[i], peers[j] = peers[j], peers[i] })
	peers = peers[:min(2, len(peers))]

	return peers
}

func (e *Engine) fetchFrontierAncestry(key string, root Tip) ([]Tip, error) {
	fetcher, ok := e.broadcaster.(AncestryFetcher)
	if !ok || key == "" || e.effectCache == nil {
		_, err := e.getEffect(key, root)

		return nil, err
	}
	// Only active requests are retained. The vertex pool owns completed data.
	// Different roots on this key also join the active transfer, then recheck
	// residency: the stream may already have delivered their dependency.
	type result struct {
		root    Tip
		missing []Tip
	}
	for {
		if e.effectCache.getVertex(root) != nil {
			return nil, nil
		}

		value, err, _ := e.ancestryLoads.Do(key, func() (any, error) {
			if e.effectCache.getVertex(root) != nil {
				return result{root: root}, nil
			}

			var missing []Tip

			err := fetcher.FetchAncestry(key, root, e.ancestryPeers(key, root),
				func(tip Tip) bool { return e.effectCache.getVertex(tip) != nil }, func(tip Tip, data []byte, isRoot bool) error {
					if isRoot {
						if len(data) != 0 {
							if err := e.storeWireData(tip, data); err != nil {
								return err
							}
						}

						if v := e.effectCache.getVertex(tip); v != nil {
							v.markRoot()
						}

						return nil
					}

					if len(data) == 0 {
						missing = append(missing, tip)

						return nil
					}

					return e.storeWireData(tip, data)
				})

			return result{root: root, missing: missing}, err
		})

		loaded := value.(result)
		if loaded.root == root {
			return loaded.missing, err
		}

		if slices.Contains(loaded.missing, root) {
			return loaded.missing, ErrEffectNotFound
		}

		if e.closed.Load() {
			return nil, fmt.Errorf("ancestry download: engine closed")
		}
	}
}
