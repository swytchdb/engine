/*
 * Copyright 2026 Swytch Labs BV
 *
 * This file is part of Swytch.
 *
 * Swytch is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Swytch is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Swytch. If not, see <https://www.gnu.org/licenses/>.
 *
 * Additional permissions under GNU AGPL version 3 section 7 apply to
 * this file. See the NOTICE.md file distributed with this source code
 * for the current set of additional permissions.
 */

package effects

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
	"google.golang.org/protobuf/proto"
)

// GraphTip identifies the observed node and graph-descriptor frontiers. An empty
// Tips slice means a new node; GraphTips must identify an observed descriptor.
// Key is the main logical key. The effect uses its complete internal address
// in both Key and IKey.
type GraphTip struct {
	Key       string
	Tips      []Tip
	GraphTips []Tip
}

// graphRoute is emission-local selection, never a cache of graph state.
type graphRoute struct {
	peers      map[pb.NodeID]bool
	descriptor string
}

func appendUniqueRefs(refs []*pb.EffectRef, tips []Tip) []*pb.EffectRef {
	for _, tip := range tips {
		if !refsContain(refs, tip) {
			refs = append(refs, toPbRef(tip))
		}
	}

	return refs
}

// EmitGraph emits a graph node using the descriptor frontier the caller observed.
// Regions live in the node's ordinary "regions" partition, and must remain stable
// for its lifetime. No region is bootstrapped by emission or predicate registration.
// Emit's ownership contract applies. Key and IKey both carry the node's full address.
func (c *Context) EmitGraph(deps GraphTip, v *pb.Effect) error {
	if v == nil || len(v.GetIKey()) == 0 || len(deps.GraphTips) == 0 {
		return errors.New("EmitGraph requires an internal address and observed graph frontier")
	}

	if err := prepareEffectAddress(v); err != nil {
		return err
	}

	key := string(v.GetIKey())
	if !isInternalAddress(key) || keytrie.LogicalKeyString(key) != deps.Key {
		return errors.New("EmitGraph address does not belong to the descriptor")
	}

	if v.GetData() == nil && v.GetMeta() == nil {
		return errors.New("EmitGraph accepts data or metadata observations")
	}

	descriptor := logicalAddress(deps.Key)

	c.engine.pinHistory(&c.history)

	if c.readErr != nil {
		return c.readErr
	}

	var subscribeErr error
	if c.inTx {
		subscribeErr = c.engine.ensureSubscribedBlocking(descriptor)
	} else {
		subscribeErr = c.engine.ensureSubscribed(descriptor)
	}

	if subscribeErr != nil {
		return subscribeErr
	}

	for _, group := range []struct {
		key  string
		tips []Tip
	}{{descriptor, deps.GraphTips}, {key, deps.Tips}} {
		for _, tip := range group.tips {
			eff, err := c.engine.getEffect(group.key, tip)
			if err != nil {
				return err
			}

			if !effectHasAddress(eff, group.key) {
				return fmt.Errorf("frontier %v does not belong to the observed graph address", tip)
			}
		}
	}

	view, err := c.engine.historicalReadView(c.TraceCtx(), []string{descriptor}, func(key string) ([]Tip, error) {
		if key == descriptor {
			return deps.GraphTips, nil
		}

		return nil, nil
	}, nil)
	if err != nil {
		return err
	}

	state, _, err := c.engine.reconstructWithView(descriptor, deps.GraphTips, c.txnID, false, view)
	if err != nil {
		return err
	}

	if state.GetTypeTag() != pb.ValueType_TYPE_GRAPH {
		return errors.New("observed descriptor is not a graph")
	}

	nodeTips := deps.Tips
	if pending := c.keys[key]; pending != nil {
		nodeTips = []Tip{pending.lastOffset}
	}

	node, err := c.engine.graphNodeState(key, nodeTips, c.txnID)
	if err != nil {
		return err
	}

	// A node deletion still belongs to its observed regions. Root deletion
	// removes the collection, but must reach the peers that hold that node.
	if !isRootKeyDel(v) {
		node = ReduceChain(node, []*pb.Effect{v})
	}

	peers, err := graphPredicatePeers(state, graphRegions(node))
	if err != nil {
		return err
	}

	// A non-nil empty frontier prevents Emit from substituting the live index.
	tips := append(make([]Tip, 0, len(deps.Tips)), deps.Tips...)

	return c.emitEffect(v, deps.GraphTips, &graphRoute{peers: peers, descriptor: descriptor}, tips)
}

// Predicate fields are ordinary keyed elements in the child partition named
// by each directory entry. The peer is part of the entry's key, not a second
// serialized copy inside its value.
func graphPredicatePeers(state *pb.ReducedEffect, regions map[string]string) (map[pb.NodeID]bool, error) {
	peers := make(map[pb.NodeID]bool)

	directory := state.GetPartitions()[pb.GraphRegionSubscriptionsKey]
	for id, entry := range directory.GetNetAdds() {
		peer, err := graphSubscriptionPeer(id)
		if err != nil {
			return nil, err
		}

		child := entry.GetData().GetChild()
		if len(child) == 0 {
			return nil, fmt.Errorf("graph subscription %q has no predicate collection", id)
		}

		predicate := make(map[string]string)
		for field, pattern := range state.GetPartitions()[string(child)].GetNetAdds() {
			predicate[field] = string(pattern.GetData().Decompress())
		}

		match, err := matchRegions(predicate, regions)
		if err != nil {
			return nil, fmt.Errorf("graph subscription %q: %w", id, err)
		}

		if match {
			peers[peer] = true
		}
	}

	return peers, nil
}

func graphSubscriptionPeer(id string) (pb.NodeID, error) {
	peer, _, found := strings.Cut(id, "/")
	if !found {
		return 0, fmt.Errorf("graph subscription %q has no peer key", id)
	}

	value, err := strconv.ParseUint(peer, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("graph subscription %q: %w", id, err)
	}

	return pb.NodeID(value), nil
}

func graphRegions(state *pb.ReducedEffect) map[string]string {
	labels := make(map[string]string)
	for field, entry := range state.GetPartitions()[pb.GraphRegionsKey].GetNetAdds() {
		labels[field] = string(entry.GetData().Decompress())
	}

	return labels
}

// Reconstruct only the supplied causal observation, including this context's
// pending node effects. Never substitute the current node or descriptor tip.
func (e *Engine) graphNodeState(key string, tips []Tip, txn string) (*pb.ReducedEffect, error) {
	if len(tips) == 0 {
		return nil, nil
	}

	view, err := e.historicalReadView(context.Background(), []string{key}, func(string) ([]Tip, error) { return tips, nil }, nil)
	if err != nil {
		return nil, err
	}
	state, _, err := e.reconstructWithView(key, tips, txn, false, view)

	return state, err
}

func matchRegions(predicate, regions map[string]string) (bool, error) {
	match := true

	for field, pattern := range predicate {
		re, err := regexp.Compile("\\A(?:" + pattern + ")\\z")
		if err != nil {
			return false, fmt.Errorf("region %q: %w", field, err)
		}

		value, exists := regions[field]
		match = match && exists && re.MatchString(value)
	}

	return match, nil
}

// SubscribeGraph records this peer's named predicate in the graph descriptor.
// It does not bootstrap existing nodes; that is the graph reader's discovery step.
func (c *Context) SubscribeGraph(key, id string, predicate map[string]string) error {
	if _, err := matchRegions(predicate, nil); err != nil {
		return err
	}

	state, _, err := c.GetSnapshot(key)
	if err != nil {
		return err
	}

	if state.GetTypeTag() != pb.ValueType_TYPE_GRAPH {
		return errors.New("key is not a graph")
	}

	return c.writeGraphPredicate(key, c.engine.nodeID, id, predicate, state)
}

// writeGraphPredicate uses the existing child-partition representation. Each
// predicate field is independently mergeable; removed observed fields receive
// ordinary keyed removals. Nothing is serialized as an opaque record.
func (c *Context) writeGraphPredicate(key string, peer pb.NodeID, id string, predicate map[string]string, state *pb.ReducedEffect) error {
	entryID := graphSubscriptionID(peer, id)
	partition := pb.GraphRegionSubscriptionsKey + "/" + string(entryID)

	old := state.GetPartitions()[partition]
	for field := range old.GetNetAdds() {
		if _, exists := predicate[field]; exists {
			continue
		}

		if err := c.Emit(&pb.Effect{Key: []byte(key), Virtual: []byte(partition), Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op: pb.EffectOp_REMOVE_OP, Collection: pb.CollectionKind_KEYED, Id: []byte(field),
		}}}); err != nil {
			return err
		}
	}

	for field, pattern := range predicate {
		if err := c.Emit(&pb.Effect{Key: []byte(key), Virtual: []byte(partition), Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op: pb.EffectOp_INSERT_OP, Collection: pb.CollectionKind_KEYED, Merge: pb.MergeRule_LAST_WRITE_WINS,
			Id: []byte(field), Value: &pb.DataEffect_Raw{Raw: []byte(pattern)},
		}}}); err != nil {
			return err
		}
	}

	return c.Emit(&pb.Effect{Key: []byte(key), Virtual: []byte(pb.GraphRegionSubscriptionsKey), Kind: &pb.Effect_Data{Data: &pb.DataEffect{
		Op: pb.EffectOp_INSERT_OP, Collection: pb.CollectionKind_KEYED, Merge: pb.MergeRule_LAST_WRITE_WINS,
		Id: entryID, Value: &pb.DataEffect_Child{Child: []byte(partition)},
	}}})
}

// UnsubscribeGraph removes this peer's named predicate without changing others.
func (c *Context) UnsubscribeGraph(key, id string) error {
	return c.Emit(&pb.Effect{Key: []byte(key), Virtual: []byte(pb.GraphRegionSubscriptionsKey), Kind: &pb.Effect_Data{Data: &pb.DataEffect{
		Op: pb.EffectOp_REMOVE_OP, Collection: pb.CollectionKind_KEYED, Id: graphSubscriptionID(c.engine.nodeID, id),
	}}})
}

func graphSubscriptionID(peer pb.NodeID, id string) []byte {
	return []byte(strconv.FormatUint(uint64(peer), 10) + "/" + hex.EncodeToString([]byte(id)))
}

// stampSnapshotSubscriptions owns state, but its partitions may still be shared.
// Predicate entries undergo the same subscriber/membership reduction as the
// ordinary subscriber snapshot. Never rewrite the historical source directory.
func (e *Engine) stampSnapshotSubscriptions(key string, state *pb.ReducedEffect) error {
	state.Subscribers = e.snapshotSubscribers(key)
	if state.GetTypeTag() != pb.ValueType_TYPE_GRAPH {
		return nil
	}

	directory := state.GetPartitions()[pb.GraphRegionSubscriptionsKey]

	var pruned *pb.ReducedEffect

	for id := range directory.GetNetAdds() {
		peer, err := graphSubscriptionPeer(id)
		if err != nil {
			return err
		}

		if peer == e.nodeID || state.GetSubscribers()[uint64(peer)] {
			continue
		}

		if pruned == nil {
			pruned = cloneReducedFlat(directory)
		}

		delete(pruned.GetNetAdds(), id)

		if pruned.NetRemoves == nil {
			pruned.NetRemoves = make(map[string]bool)
		}

		pruned.NetRemoves[id] = true
	}

	if pruned != nil {
		state.Partitions = maps.Clone(state.GetPartitions())
		state.Partitions[pb.GraphRegionSubscriptionsKey] = pruned
	}

	return nil
}

// contextSubscribers uses the same live subscriber/membership reduction as
// ordinary snapshots, then applies the graph observation's historical filter.
func (e *Engine) contextSubscribers(key string, ck *contextKey) []pb.NodeID {
	peers := e.snapshotSubscribers(key)
	for _, route := range ck.routes {
		if route == nil {
			continue
		}
		for peer := range e.snapshotSubscribers(route.descriptor) {
			if route.peers[pb.NodeID(peer)] {
				if peers == nil {
					peers = make(map[uint64]bool)
				}
				peers[peer] = true
			}
		}
	}
	result := make([]pb.NodeID, 0, len(peers))
	for peer := range peers {
		result = append(result, pb.NodeID(peer))
	}
	slices.Sort(result)
	return result
}

func (e *Engine) publishGraphNotice(key string, notice *pb.OffsetNotify, route *graphRoute, mode SafetyMode) error {
	if route == nil {
		return errors.New("missing graph predicate selection")
	}

	peers := e.contextSubscribers(key, &contextKey{routes: []*graphRoute{route}})
	if len(peers) == 0 {
		return nil
	}

	// Predicate selection establishes ordinary subscriptions to this exact address.
	for _, peer := range peers {
		e.addPeerSubscriber(key, peer)
	}

	e.recordHistoryTargets(r(notice.GetOrigin()), peers)

	body, err := proto.Marshal(notice)
	if err != nil {
		return err
	}

	send := func(peer pb.NodeID) error {
		nacks, err := e.broadcaster.ReplicateMarshalled(notice, body, peer)
		if err != nil {
			return err
		}

		for _, nack := range nacks {
			if nack.GetNotSubscribed() {
				return ErrAuthorityDropped
			}

			if err := e.HandleNack(nack); err != nil {
				return err
			}
		}

		return nil
	}
	if mode != SafeMode {
		for _, peer := range peers {
			go func() {
				if err := send(peer); err != nil {
					slog.Warn("graph replication failed", "key", key, "peer", peer, "error", err)
				}
			}()
		}

		return nil
	}

	results := make(chan error, len(peers))
	for _, peer := range peers {
		go func() { results <- send(peer) }()
	}
	// As with ordinary SafeMode, one actual ACK suffices. Every selected peer
	// is sent the observation; an unreachable subscriber is never filtered out.
	var failures []error

	for range peers {
		if err := <-results; err == nil {
			return nil
		} else {
			failures = append(failures, err)
		}
	}

	return errors.Join(failures...)
}

// isInternalAddress validates the graph API address namespace; descriptor data
// determines whether it is a graph. Generic engine paths do not use this test.
func isInternalAddress(key string) bool { return len(key) > 0 && key[0] != keytrie.LogicalKeyPrefix[0] }

func (e *Engine) recordHistoryTargets(tip Tip, peers []pb.NodeID) {
	if provider, ok := e.broadcaster.(historyProgressProvider); ok {
		for _, peer := range peers {
			provider.RecordHistorySent(peer, tip[1])
		}
	}
}

// admitGraphEffect is the ingress boundary for first delivery through a region
// subscription. It uses the descriptor observation carried by causal deps, then
// establishes ownership of only the selected address. Evidence fetched for other
// keys remains unowned. Ordinary engine paths never project an address to its owner.
func (e *Engine) admitGraphEffect(eff *pb.Effect) error {
	if bind := eff.GetTxnBind(); bind != nil {
		for _, kb := range bind.GetKeys() {
			if !isInternalAddress(string(kb.GetKey())) {
				continue
			}
			if _, ok := e.subscription(logicalAddress(keytrie.LogicalKeyString(string(kb.GetKey())))); !ok {
				continue
			}
			if _, ok := e.subscription(string(kb.GetKey())); ok {
				continue
			}
			value, err := e.getEffect(string(kb.GetKey()), r(kb.GetNewTip()))
			if err != nil {
				return err
			}
			if err := e.admitGraphNode(value); err != nil {
				return err
			}
		}
		return nil
	}
	return e.admitGraphNode(eff)
}

func (e *Engine) admitGraphNode(eff *pb.Effect) error {
	key := string(eff.GetIKey())
	if !isInternalAddress(key) || eff.GetData() == nil && eff.GetMeta() == nil {
		return nil
	}
	if _, ok := e.subscription(key); ok {
		return nil
	}
	descriptor := logicalAddress(keytrie.LogicalKeyString(key))
	if _, ok := e.subscription(descriptor); !ok {
		return nil
	}
	var observed []Tip
	var nodeTips []Tip
	for _, ref := range eff.GetDeps() {
		parent, err := e.getEffect(key, r(ref))
		if err != nil {
			return err
		}
		if effectHasAddress(parent, descriptor) {
			observed = append(observed, r(ref))
		}
		if effectHasAddress(parent, key) {
			nodeTips = append(nodeTips, r(ref))
		}
	}
	if len(observed) == 0 {
		return nil
	}
	state, err := e.graphNodeState(descriptor, observed, eff.GetTxnId())
	if err != nil {
		return err
	}
	if state.GetTypeTag() != pb.ValueType_TYPE_GRAPH {
		return nil
	}
	node, err := e.graphNodeState(key, nodeTips, eff.GetTxnId())
	if err != nil {
		return err
	}
	if !isRootKeyDel(eff) {
		node = ReduceChain(node, []*pb.Effect{eff})
	}
	peers, err := graphPredicatePeers(state, graphRegions(node))
	if err != nil {
		return err
	}
	if !peers[e.nodeID] {
		return nil
	}
	// Use ordinary discovery/readiness for the selected address. Region admission
	// must not publish a separate ready flag before its causal history is installed.
	return e.ensureSubscribedBlocking(key)
}
