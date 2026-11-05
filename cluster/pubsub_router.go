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

package cluster

import (
	"bytes"
	"cmp"
	"log/slog"
	"slices"
	"sync"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/engine/keytrie"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Reserved routing prefixes for pub/sub channel and pattern keys.
// The trailing 0x00 separates the prefix tag from the user-supplied
// channel/pattern name so a channel literally named "pat:x" cannot
// collide with the pattern namespace. KEYS may surface these — that's
// acceptable per the design.
var (
	chKeyPrefix  = []byte("__swytch:ch\x00")
	patKeyPrefix = []byte("__swytch:pat\x00")
)

// encodeRoutingKey returns the routing key carried in the Effect
// envelope. Pass chKeyPrefix for a literal channel, patKeyPrefix for
// a pattern.
func encodeRoutingKey(prefix []byte, name string) []byte {
	out := make([]byte, 0, len(prefix)+len(name))
	out = append(out, prefix...)
	out = append(out, name...)

	return out
}

// decodeRoutingKey identifies a routing key as a channel or pattern
// subscription and extracts the user-supplied name. Returns ok=false
// for keys that aren't pub/sub routing keys.
func decodeRoutingKey(routingKey []byte) (name string, isPattern bool, ok bool) {
	if bytes.HasPrefix(routingKey, chKeyPrefix) {
		return string(routingKey[len(chKeyPrefix):]), false, true
	}

	if bytes.HasPrefix(routingKey, patKeyPrefix) {
		return string(routingKey[len(patKeyPrefix):]), true, true
	}

	return "", false, false
}

// LocalPubSubBroker is the slice of the local pub/sub broker the
// cluster router needs: inbound delivery and a snapshot of local
// subscriptions for re-announce on peer join. Implemented by the
// in-process pub/sub Manager.
type LocalPubSubBroker interface {
	// DeliverLocal hands a received cross-node PUBLISH off to local
	// subscribers. The broker is responsible for matching against
	// both literal channels and locally-subscribed patterns.
	DeliverLocal(channel string, payload []byte)

	// LocalSubsSnapshot returns every channel and pattern that has at
	// least one local subscriber. Used to re-announce when a peer
	// joins.
	LocalSubsSnapshot() (channels []string, patterns []string)
}

// RouterTransport is the slice of PeerManager the router relies on.
// Exported so tests can supply a mock without spinning up QUIC.
type RouterTransport interface {
	PeerIDs() []NodeId
	SendOneWayTo(notify *pb.OffsetNotify, wireData []byte, peerID NodeId) error
}

// PubSubRouter owns the per-peer subscription table and bridges the
// local pub/sub broker with cluster gossip.
//
// It is registered into redis/shared via SetPubSubClusterRouter so
// the local broker can call into it, and into the effects engine via
// callbacks so inbound ephemeral effects land here.
type PubSubRouter struct {
	self      NodeId
	transport RouterTransport
	broker    LocalPubSubBroker

	mu    sync.RWMutex
	peers []peerSubscriptions // sorted by peer ID; guarded by mu
}

type peerSubscriptions struct {
	id       NodeId
	channels []string // sorted and unique
	patterns []string // sorted and unique
}

func (r *PubSubRouter) peerIndex(id NodeId) (int, bool) {
	return slices.BinarySearchFunc(r.peers, id, func(peer peerSubscriptions, id NodeId) int {
		return cmp.Compare(peer.id, id)
	})
}

// NewPubSubRouter constructs a router. The broker is the in-process
// pub/sub manager; the transport supplies broadcast/addressed-send.
// Caller is responsible for wiring the router into the engine's
// OnPubSubMessage / OnEphemeralSubscribe callbacks and into
// PeerManager.SetPeerLifecycleHooks.
func NewPubSubRouter(self NodeId, transport RouterTransport, broker LocalPubSubBroker) *PubSubRouter {
	return &PubSubRouter{
		self:      self,
		transport: transport,
		broker:    broker,
	}
}

// --- Engine callbacks (inbound) -----------------------------------------

// HandleEphemeralSubscribe is wired to effects.Engine.OnEphemeralSubscribe.
// It mutates the per-peer table based on the routing key encoding.
func (r *PubSubRouter) HandleEphemeralSubscribe(subscriberNodeID uint64, routingKey []byte, unsubscribe bool) {
	name, isPattern, ok := decodeRoutingKey(routingKey)
	if !ok {
		slog.Debug("pubsub router: ignoring unknown routing key prefix",
			"key", string(routingKey))

		return
	}

	peerID := NodeId(subscriberNodeID)
	if peerID == r.self {
		// Local-origin echo: we never broadcast to ourselves, but
		// guard regardless.
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	i, found := r.peerIndex(peerID)
	if !found {
		if unsubscribe {
			return
		}

		r.peers = slices.Insert(r.peers, i, peerSubscriptions{id: peerID})
	}

	peer := &r.peers[i]

	names := &peer.channels
	if isPattern {
		names = &peer.patterns
	}

	j, subscribed := slices.BinarySearch(*names, name)
	if unsubscribe && subscribed {
		*names = shrink(slices.Delete(*names, j, j+1))
	} else if !unsubscribe && !subscribed {
		*names = slices.Insert(*names, j, name)
	}

	if len(peer.channels)+len(peer.patterns) == 0 {
		r.peers = shrink(slices.Delete(r.peers, i, i+1))
	}
}

// HandleInboundMessage is wired to effects.Engine.OnPubSubMessage. It
// hands the message off to the local broker for delivery.
func (r *PubSubRouter) HandleInboundMessage(channel, payload []byte) {
	r.broker.DeliverLocal(string(channel), payload)
}

// --- Peer lifecycle (from PeerManager) ----------------------------------

// OnPeerAdded re-announces every local subscription to the new peer
// so it learns about them. Called from PeerManager's registerPeer.
func (r *PubSubRouter) OnPeerAdded(peerID NodeId) {
	if peerID == r.self {
		return
	}

	channels, patterns := r.broker.LocalSubsSnapshot()
	for _, ch := range channels {
		r.sendAnnounce(encodeRoutingKey(chKeyPrefix, ch), false, peerID)
	}

	for _, pat := range patterns {
		r.sendAnnounce(encodeRoutingKey(patKeyPrefix, pat), false, peerID)
	}
}

// OnPeerRemoved drops every entry the departing peer owned in the
// per-peer table. Called from PeerManager's unregisterPeer.
func (r *PubSubRouter) OnPeerRemoved(peerID NodeId) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if i, found := r.peerIndex(peerID); found {
		r.peers = shrink(slices.Delete(r.peers, i, i+1))
	}
}

// --- shared.PubSubClusterRouter implementation --------------------------

// AnnounceSub broadcasts an ephemeral SubscriptionEffect to every
// connected peer. Fire-and-forget; receivers register us in their
// per-peer tables and return immediately without log storage.
func (r *PubSubRouter) AnnounceSub(channel string, isPattern bool) {
	r.sendAnnounce(routingKeyFor(channel, isPattern), false, r.transport.PeerIDs()...)
}

// AnnounceUnsub broadcasts an unsubscribe ephemeral SubscriptionEffect.
func (r *PubSubRouter) AnnounceUnsub(channel string, isPattern bool) {
	r.sendAnnounce(routingKeyFor(channel, isPattern), true, r.transport.PeerIDs()...)
}

func routingKeyFor(name string, isPattern bool) []byte {
	if isPattern {
		return encodeRoutingKey(patKeyPrefix, name)
	}

	return encodeRoutingKey(chKeyPrefix, name)
}

// RouteMessage delivers a PUBLISH to every remote peer with a matching
// literal channel or matching pattern, and returns the count of peers
// the message was dispatched to.
func (r *PubSubRouter) RouteMessage(channel string, payload []byte) int {
	targets := r.targetsFor(channel)
	if len(targets) == 0 {
		return 0
	}

	hlc := timestamppb.Now()
	eff := &pb.Effect{
		Key:            encodeRoutingKey(chKeyPrefix, channel),
		Hlc:            hlc,
		NodeId:         uint64(r.self),
		ForkChoiceHash: effects.ComputeForkChoiceHash(r.self, hlc),
		Kind: &pb.Effect_PubsubMessage{PubsubMessage: &pb.PubSubMessage{
			Channel: []byte(channel),
			Payload: payload,
		}},
	}

	notify, err := buildEphemeralNotify(r.self, eff)
	if err != nil {
		slog.Debug("pubsub router: marshal message failed", "error", err)

		return 0
	}

	sent := 0

	for _, peerID := range targets {
		if err := r.transport.SendOneWayTo(notify, notify.GetEffectData(), peerID); err != nil {
			slog.Debug("pubsub router: one-way send failed",
				"peer", peerID, "channel", channel, "error", err)

			continue
		}

		sent++
	}

	return sent
}

// ClusterChannels returns every channel a remote peer is interested
// in (deduped), filtered by pattern if non-empty.
func (r *PubSubRouter) ClusterChannels(pattern string) []string {
	r.mu.RLock()

	var out []string

	for _, peer := range r.peers {
		for _, ch := range peer.channels {
			if pattern == "" || keytrie.MatchGlob(ch, pattern) {
				out = append(out, ch)
			}
		}
	}

	r.mu.RUnlock()
	slices.Sort(out)

	return shrink(slices.Compact(out))
}

// ClusterNumSub counts, per channel, the number of peers announcing
// that exact channel. Patterns are not counted (Redis NUMSUB ignores
// pattern subscriptions).
func (r *PubSubRouter) ClusterNumSub(channels []string) map[string]int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make(map[string]int, len(channels))
	for _, ch := range channels {
		count := 0

		for _, peer := range r.peers {
			if _, ok := slices.BinarySearch(peer.channels, ch); ok {
				count++
			}
		}

		out[ch] = count
	}

	return out
}

// ClusterPatterns returns every distinct pattern announced by any
// remote peer. The local broker unions this with its own pattern set
// to compute PUBSUB NUMPAT correctly across the cluster.
func (r *PubSubRouter) ClusterPatterns() []string {
	r.mu.RLock()

	var out []string
	for _, peer := range r.peers {
		out = append(out, peer.patterns...)
	}

	r.mu.RUnlock()
	slices.Sort(out)

	return shrink(slices.Compact(out))
}

// --- internals ----------------------------------------------------------

// targetsFor returns the peer IDs that should receive a PUBLISH for
// the given channel, based on the per-peer table.
func (r *PubSubRouter) targetsFor(channel string) []NodeId {
	r.mu.RLock()
	defer r.mu.RUnlock()

	targets := make([]NodeId, 0, len(r.peers))
	for _, peer := range r.peers {
		if _, ok := slices.BinarySearch(peer.channels, channel); ok {
			targets = append(targets, peer.id)

			continue
		}

		for _, pat := range peer.patterns {
			if keytrie.MatchGlob(channel, pat) {
				targets = append(targets, peer.id)

				break
			}
		}
	}

	return targets
}

// sendAnnounce builds one subscription announcement for the entire fan-out.
func (r *PubSubRouter) sendAnnounce(routingKey []byte, unsubscribe bool, peers ...NodeId) {
	if len(peers) == 0 {
		return
	}

	hlc := timestamppb.Now()
	eff := &pb.Effect{
		Key:            routingKey,
		Hlc:            hlc,
		NodeId:         uint64(r.self),
		ForkChoiceHash: effects.ComputeForkChoiceHash(r.self, hlc),
		Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{
			SubscriberNodeId: uint64(r.self),
			Unsubscribe:      unsubscribe,
			Ephemeral:        true,
		}},
	}

	notify, err := buildEphemeralNotify(r.self, eff)
	if err != nil {
		slog.Debug("pubsub router: marshal announce failed", "error", err)

		return
	}

	for _, peerID := range peers {
		if err := r.transport.SendOneWayTo(notify, notify.GetEffectData(), peerID); err != nil {
			slog.Debug("pubsub router: announce send failed",
				"peer", peerID, "unsubscribe", unsubscribe, "error", err)
		}
	}
}

// buildEphemeralNotify delegates to effects.BuildOffsetNotify so wire
// framing lives in one place. The Origin is a non-zero sentinel — the
// receiver short-circuits on the SubscriptionEffect.Ephemeral /
// PubSubMessage branches before any real offset/log lookup, but
// HandleRemote rejects nil origins so we set a plausible one.
func buildEphemeralNotify(self NodeId, eff *pb.Effect) (*pb.OffsetNotify, error) {
	data, err := effects.MarshalEffect(eff)
	if err != nil {
		return nil, err
	}

	origin := keytrie.EffectRef{uint64(self), uint64(time.Now().UnixNano())}

	return effects.BuildOffsetNotify(self, origin, eff, data, nil), nil
}
