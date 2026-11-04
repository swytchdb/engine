package effects

import (
	"slices"
	"sync"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/memsize"
)

func subscriberSliceBytes(ids []pb.NodeID) int64 { return memsize.Alloc(int64(cap(ids)) * 8) }

// collectPeerTips announces a subscription concurrently, then folds the replies
// on the caller. Each worker owns one slot; Wait publishes those slots without
// a result mutex or atomic counter. Both bootstrap rounds use this same path.
func (e *Engine) collectPeerTips(key string, notify *pb.OffsetNotify, peers []pb.NodeID, tips []Tip) ([]Tip, int) {
	replies := make([]struct {
		nacks []*pb.NackNotify
		err   error
	}, len(peers))

	var wg sync.WaitGroup
	for i, peer := range peers {
		wg.Go(func() {
			replies[i].nacks, replies[i].err = e.broadcaster.ReplicateTo(notify, notify.GetEffectData(), peer)
		})
	}

	wg.Wait()

	acknowledged := 0

	for i, reply := range replies {
		if reply.err != nil {
			continue
		}

		acknowledged++
		// A NACK means this peer holds authoritative state for the key.
		if len(reply.nacks) > 0 {
			e.addPeerSubscriber(key, peers[i])
		}

		for _, nack := range reply.nacks {
			for _, ref := range nack.GetTips() {
				tips = append(tips, r(ref))
			}
		}
	}

	return tips, acknowledged
}

// addPeerSubscriber records that peer is subscribed to key. Idempotent.
// Called from ensureSubscribed (peers that NACK back during bootstrap)
// and HandleRemote (foreign SubscriptionEffect arrival). The subscriber set
// lives on the key's leaf; if the key has no leaf (never read/written or
// already evicted), there is nothing to subscribe to and the call is a no-op.
func (e *Engine) addPeerSubscriber(key string, peer pb.NodeID) {
	if peer == e.nodeID {
		return
	}

	ls := e.index.LoadData(key)
	if ls == nil {
		ls, _ = e.index.LoadOrStoreData(key, &leafState{})
	}

	if ls == nil {
		return
	}

	ls.subMu.Lock()
	if ls.subdag.Load() == evictedSubdag {
		ls.subMu.Unlock()

		return
	}

	if !slices.Contains(ls.subscribers, peer) {
		ls.subscribers = append(ls.subscribers, peer)
		cost := subscriberSliceBytes(ls.subscribers)
		e.subscriberBytes.Add(cost - ls.subscriberBytes)
		ls.subscriberBytes = cost
	}
	ls.subMu.Unlock()
}

// removePeerSubscriber removes peer from the subscriber set for key.
// Called from HandleRemote on a foreign unsubscribe SubscriptionEffect.
func (e *Engine) removePeerSubscriber(key string, peer pb.NodeID) {
	ls := e.index.LoadData(key)
	if ls == nil {
		return
	}

	ls.subMu.Lock()
	if i := slices.Index(ls.subscribers, peer); i >= 0 {
		ls.subscribers = shrinkSlice(slices.Delete(ls.subscribers, i, i+1))
		cost := subscriberSliceBytes(ls.subscribers)
		e.subscriberBytes.Add(cost - ls.subscriberBytes)
		ls.subscriberBytes = cost
	}
	ls.subMu.Unlock()
}

// snapshotSubscribers returns the key's current subscriber set in the proto
// map form, for stamping into a compaction SnapshotEffect's reduced state so
// the set survives chain compaction and is available to a bootstrapping peer.
// Returns nil when there are no subscribers. Sourced from the authoritative
// leafState set — no DAG walk.
func (e *Engine) snapshotSubscribers(key string) map[uint64]bool {
	ls := e.index.LoadData(key)
	if ls == nil {
		return nil
	}

	ls.subMu.Lock()
	defer ls.subMu.Unlock()

	if len(ls.subscribers) == 0 {
		return nil
	}

	m := make(map[uint64]bool, len(ls.subscribers))
	for _, id := range ls.subscribers {
		m[uint64(id)] = true
	}

	return m
}

// PeerSubscribers returns the current subscriber set for key. The returned
// slice is a snapshot; callers may iterate without holding any lock. It is
// unfiltered by membership — flushTx's collectSubscribers applies the
// current-membership filter at broadcast time.
func (e *Engine) PeerSubscribers(key string) []pb.NodeID {
	ls := e.index.LoadData(key)
	if ls == nil {
		return nil
	}

	ls.subMu.Lock()
	defer ls.subMu.Unlock()

	return slices.Clone(ls.subscribers)
}

// collectSubscribers returns distinct subscribers that are still cluster members.
// Unreachable members remain required; only departed peers are excluded. The
// leaf's subscriber set is authoritative, including after DAG compaction.
func (c *Context) collectSubscribers() []pb.NodeID {
	var members []pb.NodeID
	if c.engine.broadcaster != nil {
		members = c.engine.broadcaster.PeerIDs()
	}

	var result []pb.NodeID

	for key := range c.keys {
		ls := c.engine.index.LoadData(key)
		if ls == nil {
			continue
		}

		ls.subMu.Lock()
		for _, id := range ls.subscribers {
			if c.engine.broadcaster != nil && !slices.Contains(members, id) {
				continue
			}

			if !slices.Contains(result, id) {
				result = append(result, id)
			}
		}
		ls.subMu.Unlock()
	}

	return result
}
