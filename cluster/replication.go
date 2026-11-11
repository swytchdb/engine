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
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
)

// Sentinel errors for replication failures.
var (
	ErrNoPeers               = errors.New("no alive symmetric peers available for replication")
	ErrReplicationTimeout    = errors.New("replication timed out waiting for ACK")
	ErrAllPeersNotSubscribed = errors.New("every tracked peer reported they are not subscribed to the key")
)

// Retransmission constants.
const (
	sweepInterval      = 250 * time.Millisecond // deadline and peer-liveness checks
	retransmitInterval = 500 * time.Millisecond
)

// Replicator manages replication with first-ACK semantics for same-region
// and fire-and-forget for cross-region peers. Uses QUIC stream-per-message transport.
//
// Delivery policy lives here; connection management and wire delivery belong
// to the transport.
type Replicator struct {
	nodeID             NodeId
	localRegion        string
	replicationTimeout time.Duration

	healthTable *PeerHealthTable
	transport   NotificationTransport
	tracker     requestTracker

	mu sync.Mutex // serializes membership writers only
	// Published maps are immutable. Membership changes replace the map so
	// replication can keep a snapshot without copying it or holding a lock.
	peerRegions atomic.Pointer[map[NodeId]string]

	handler EffectHandler // for handling inbound notifications

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NotificationTransport delivers framed replication and control packets.
// QUICNotifyTransport is the production implementation.
type NotificationTransport interface {
	Send(peerID NodeId, plaintext []byte) (int, error)
	SendDirect(peerID NodeId, plaintext []byte) error
}

// NewReplicator creates a new Replicator.
func NewReplicator(
	nodeID NodeId,
	localRegion string,
	healthTable *PeerHealthTable,
	transport NotificationTransport,
	handler EffectHandler,
	replicationTimeout time.Duration,
) *Replicator {
	if replicationTimeout <= 0 {
		replicationTimeout = 30 * time.Second
	}

	return &Replicator{
		nodeID:             nodeID,
		localRegion:        localRegion,
		replicationTimeout: replicationTimeout,
		healthTable:        healthTable,
		transport:          transport,
		handler:            handler,
		tracker:            requestTracker{pending: xsync.NewMap[uint64, *trackedRequest]()},
		stopCh:             make(chan struct{}),
	}
}

// Start begins the timeout sweep loop.
func (r *Replicator) Start() {
	r.wg.Add(1)
	go r.sweepLoop()
}

// Stop shuts down the replicator.
func (r *Replicator) Stop() {
	close(r.stopCh)
	r.wg.Wait()
}

// AddPeer registers a peer for replication.
func (r *Replicator) AddPeer(nodeID NodeId, region string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	current := r.regionsSnapshot()
	if existing, ok := current[nodeID]; ok && existing == region {
		return
	}

	regions := make(map[NodeId]string, len(current)+1)
	maps.Copy(regions, current)
	regions[nodeID] = region
	r.peerRegions.Store(&regions)
}

// RemovePeer unregisters a peer.
func (r *Replicator) RemovePeer(nodeID NodeId) {
	r.mu.Lock()
	defer r.mu.Unlock()

	current := r.regionsSnapshot()
	if _, ok := current[nodeID]; !ok {
		return
	}

	// Rebuild at the remaining size; deleting from a long-lived map retains
	// its peak allocation after membership shrinks.
	regions := make(map[NodeId]string, len(current)-1)
	for id, region := range current {
		if id != nodeID {
			regions[id] = region
		}
	}

	r.peerRegions.Store(&regions)
}

func (r *Replicator) regionsSnapshot() map[NodeId]string {
	if regions := r.peerRegions.Load(); regions != nil {
		return *regions
	}

	return nil
}

// AllRegionPeersReachable returns true if every same-region peer is alive
// and has a verified symmetric path.
func (r *Replicator) AllRegionPeersReachable() bool {
	return r.healthTable.AllRegionPeersReachable(r.localRegion, r.regionsSnapshot())
}

// InMajorityPartition returns true if this node can reach a strict majority
// of same-region nodes (including itself).
func (r *Replicator) InMajorityPartition() bool {
	return r.healthTable.InMajorityPartition(r.localRegion, r.regionsSnapshot())
}

// Replicate sends a notification to all peers with first-ACK semantics for same-region.
// Returns a future that resolves when the first same-region ACK arrives, or rejects
// with ErrNoPeers if there are no alive+symmetric same-region peers.
// Cross-region peers receive the notification fire-and-forget.
func (r *Replicator) Replicate(notify *pb.OffsetNotify, wireData []byte) *ReplicationFuture {
	var sendSpan trace.Span

	if tracing.Enabled() {
		remoteCtx := tracing.ExtractFromBytes(notify.GetTraceContext())

		_, sendSpan = tracing.Tracer().Start(remoteCtx, "replication.send",
			trace.WithAttributes(attribute.String("effect.key", string(notify.GetKey()))))
		defer sendSpan.End()
	}

	regions := r.regionsSnapshot()

	// Attach effect data inline. Safe to mutate: callers pass a
	// per-effect notify that isn't read after this call.
	notify.EffectData = wireData

	// Find alive+symmetric same-region peers for tracked replication
	targets := r.healthTable.AliveSymmetricPeers(r.localRegion, regions)

	crossRegion := false

	for _, region := range regions {
		if region != r.localRegion {
			crossRegion = true

			break
		}
	}

	if len(targets) == 0 && !crossRegion {
		return rejectedFuture(ErrNoPeers)
	}

	// Marshal the notify body once: it is byte-identical for every peer
	// (only the per-request header varies), so AssembleNotifyPacket frames
	// a packet per peer without re-serializing the effect. Re-marshalling
	// per peer was O(peers × effectSize) of redundant work on every PUT.
	body, err := proto.Marshal(notify)
	if err != nil {
		return rejectedFuture(fmt.Errorf("marshal notify packet: %w", err))
	}

	// Share the serialized body across both regions. Avoid starting a
	// goroutine at all when there are no cross-region recipients.
	if crossRegion {
		go r.sendCrossRegion(body, regions)
	}

	if len(targets) == 0 {
		return rejectedFuture(ErrNoPeers)
	}

	// One tracked request per peer; future resolves on FIRST ACK (first-ACK semantics).
	future := &ReplicationFuture{done: make(chan struct{})}

	// Fan the per-peer sends out concurrently: each transport.Send opens a
	// QUIC uni-stream that can block on flow control, so a serial loop made
	// PUT latency grow linearly with same-region peer count. wg.Wait below
	// keeps the expected/setupDone finalization correct (every expected.Add
	// completes before we set setupDone).
	var wg sync.WaitGroup
	wg.Add(len(targets))

	for _, targetID := range targets {
		go func(targetID NodeId) {
			defer wg.Done()

			// Store tracked request BEFORE sending so the NACK handler can
			// find it. On fast networks (localhost) the peer's response can
			// arrive before Send() returns.
			tr := &trackedRequest{
				future:       future,
				peerID:       targetID,
				retransmit:   true,
				traceContext: notify.GetTraceContext(),
			}
			r.tracker.register(tr, body)

			// A send failure here is almost always "the peer's connection
			// isn't up yet" (startup race, mid-reconnect). The request stays
			// tracked; Replicator.sweep retransmits it once a connection is
			// available and only gives up if the peer leaves the
			// alive+symmetric set. We must NOT drop it: an un-replicated tip
			// that a later effect depends on becomes an unrecoverable orphan.
			_, err := r.transport.Send(targetID, tr.packetData)
			if err != nil {
				slog.Debug("same-region send failed, holding for retransmit",
					"peer", targetID, "error", err)

				return
			}

			RecordNotificationSent()
		}(targetID)
	}

	wg.Wait()

	// Finalize expected so NotSubscribed responses and sweep give-ups can
	// resolve the future. Re-check here covers the race where every tracked
	// peer responded before we set setupDone.
	future.setupDone.Store(true)

	if future.dropped.Load() == future.expected.Load() {
		future.reject(ErrAllPeersNotSubscribed)
	}

	return future
}

// sendCrossRegion sends the notification to all cross-region peers.
// Fire-and-forget — callers don't block on cross-region ACKs.
func (r *Replicator) sendCrossRegion(body []byte, regions map[NodeId]string) {
	deadlineNano := time.Now().Add(r.replicationTimeout).UnixNano()
	nullFuture := &ReplicationFuture{done: make(chan struct{})}
	nullFuture.resolve()

	for peerID, region := range regions {
		if region == r.localRegion {
			continue
		}

		tr := &trackedRequest{
			future:   nullFuture,
			deadline: deadlineNano,
			peerID:   peerID,
		}
		requestID := r.tracker.register(tr, body)

		_, err := r.transport.Send(peerID, tr.packetData)
		if err != nil {
			r.tracker.pending.Delete(requestID)
			slog.Error("cross-region send failed, notification dropped",
				"peer", peerID, "error", err)

			continue
		}

		RecordNotificationSent()
	}
}

// HandleNotify processes an inbound notification from a peer.
// It applies the effect and sends an ACK or NACK response back via QUIC.
func (r *Replicator) HandleNotify(peerID NodeId, requestID uint64, notify *pb.OffsetNotify) {
	var recvSpan trace.Span

	if tracing.Enabled() {
		remoteCtx := tracing.ExtractFromBytes(notify.GetTraceContext())

		_, recvSpan = tracing.Tracer().Start(remoteCtx, "replication.receive",
			trace.WithAttributes(
				attribute.Int("peer.id", int(peerID)),
				attribute.Int64("request.id", int64(requestID)),
			))
		defer recvSpan.End()
	}

	slog.Debug("HandleNotify: received",
		"peer", peerID, "requestID", requestID,
		"key", string(notify.GetKey()),
		"offset", notify.GetOrigin().GetOffset())

	RecordNotificationReceived()

	var nacks []*pb.NackNotify

	if r.handler != nil && notify != nil {
		var err error

		nacks, err = r.handler.HandleOffsetNotify(notify)
		if err != nil {
			slog.Debug("HandleNotify: handler error",
				"peer", peerID, "requestID", requestID,
				"error", err,
			)
		}
	}

	// Credits field kept for wire compatibility but QUIC handles flow control
	credits := uint32(0)
	status := "ACK"

	var packet []byte

	if len(nacks) > 0 {
		status = "NACK"
		packet = MarshalNotifyNACKPacket(r.nodeID, requestID, nacks, credits)
	} else {
		packet = MarshalNotifyACKPacket(r.nodeID, requestID, 0x00, credits)
	}

	if err := r.transport.SendDirect(peerID, packet); err != nil {
		slog.Debug("HandleNotify: failed to send response",
			"peer", peerID, "requestID", requestID, "status", status, "error", err)
	} else {
		slog.Debug("HandleNotify: sent response",
			"peer", peerID, "requestID", requestID, "status", status, "nacks", len(nacks))
	}
}

// HandleNotifyACK processes an inbound ACK from a peer.
func (r *Replicator) HandleNotifyACK(peerID NodeId, requestID uint64, status byte, credits uint32) {
	tr, ok := r.tracker.pending.LoadAndDelete(requestID)
	if !ok {
		slog.Debug("HandleNotifyACK: late ACK (already timed out or resolved)",
			"peer", peerID, "requestID", requestID)

		return
	}

	tr.future.resolve()

	now := time.Now()
	latencyMs := float64(now.UnixNano()-tr.createdAt) / 1e6

	if tracing.Enabled() {
		traceCtx := tracing.ExtractFromBytes(tr.traceContext)
		_, ackSpan := tracing.Tracer().Start(traceCtx, "replication.ack_received",
			trace.WithAttributes(
				attribute.Int("peer.id", int(peerID)),
				attribute.Int64("request.id", int64(requestID)),
				attribute.Float64("rtt_ms", latencyMs),
			))
		ackSpan.End()
	}

	RecordUDPNotifyACKLatency(latencyMs)

	slog.Debug("HandleNotifyACK: received ACK",
		"peer", peerID, "requestID", requestID,
		"rtt_ms", latencyMs)
}

// HandleNotifyACKWithData processes the full ACK/NACK packet including payload.
// Called from dispatch when status=0x01 to extract embedded NACKs.
func (r *Replicator) HandleNotifyACKWithData(peerID NodeId, requestID uint64, status byte, credits uint32, fullPacket []byte) {
	tr, ok := r.tracker.pending.LoadAndDelete(requestID)
	if !ok {
		slog.Debug("HandleNotifyACKWithData: late NACK (already timed out or resolved)",
			"peer", peerID, "requestID", requestID)

		return
	}

	now := time.Now()
	latencyMs := float64(now.UnixNano()-tr.createdAt) / 1e6
	RecordUDPNotifyACKLatency(latencyMs)

	slog.Debug("HandleNotifyACKWithData: received NACK",
		"peer", peerID, "requestID", requestID,
		"rtt_ms", latencyMs)

	// Parse and store NACKs BEFORE resolving the future so callers
	// of Wait()/Nacks() see them immediately.
	notSubscribed := false

	if status == 0x01 && len(fullPacket) > notifyACKHeaderSize {
		nacks, err := parseNotifyNACKPayload(fullPacket[notifyACKHeaderSize:])
		if err != nil {
			slog.Debug("failed to parse NACK payload", "peer", peerID, "error", err)
		}

		if len(nacks) > 0 {
			tr.future.nacks.Store(&nacks)

			notSubscribed = true

			for _, nack := range nacks {
				if !nack.GetNotSubscribed() {
					notSubscribed = false

					break
				}
			}
		}
	}

	// A NotSubscribed NACK means "this peer discarded the write."
	// It must not satisfy first-ACK semantics — wait for a real
	// accepting peer. When every tracked peer reports NotSubscribed
	// the future rejects fast instead of waiting for the deadline.
	if notSubscribed {
		tr.future.drop(ErrAllPeersNotSubscribed)

		return
	}

	tr.future.resolve()
}

// ReplicateTo sends a notification to a specific peer and waits for ACK or NACK.
// Used by transactional bind to get deliberate per-subscriber responses.
func (r *Replicator) ReplicateTo(notify *pb.OffsetNotify, wireData []byte, targetPeerID NodeId) ([]*pb.NackNotify, error) {
	// Subscription fan-out shares notify across peer goroutines. Keep the
	// caller's message immutable while attaching this call's wire payload.
	message := proto.Clone(notify).(*pb.OffsetNotify)
	message.EffectData = wireData

	body, err := proto.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("marshal notify packet: %w", err)
	}

	return r.replicateBody(body, targetPeerID)
}

// ReplicateMarshalled sends an already-marshalled OffsetNotify body to a
// specific peer and waits for ACK/NACK. The body is the proto-marshalled
// notify, shared across a fan-out so it is marshalled once; only the per-peer
// request header is assembled here. notify is unused by the wire path (the
// body carries everything) and is present only so test doubles can inspect it.
func (r *Replicator) ReplicateMarshalled(_ *pb.OffsetNotify, notifyBody []byte, targetPeerID NodeId) ([]*pb.NackNotify, error) {
	return r.replicateBody(notifyBody, targetPeerID)
}

// replicateBody registers a tracked request, frames the notify body into a
// packet, sends it, and blocks for the ACK/NACK. Shared by ReplicateTo (which
// marshals per call) and ReplicateMarshalled (which reuses a shared body).
func (r *Replicator) replicateBody(notifyBody []byte, targetPeerID NodeId) ([]*pb.NackNotify, error) {
	// Populate every field — deadline and retransmit included — BEFORE storing,
	// so the sweep can never observe a deadline=0 entry and expire it in the
	// window before initialization. Stored before Send because a fast peer's
	// ACK/NACK can land before Send() returns. retransmit stays false: this is
	// the deadline-mode bind ReplicateTo path.
	now := time.Now().UnixNano()
	future := &ReplicationFuture{done: make(chan struct{})}
	future.setupDone.Store(true)
	tr := &trackedRequest{
		future:   future,
		deadline: now + int64(r.replicationTimeout),
		peerID:   targetPeerID,
	}
	requestID := r.tracker.register(tr, notifyBody)

	_, err := r.transport.Send(targetPeerID, tr.packetData)
	if err != nil {
		r.tracker.pending.Delete(requestID)

		return nil, fmt.Errorf("send to peer %d: %w", targetPeerID, err)
	}

	RecordNotificationSent()

	// Shutdown may finish its final sweep before this request is registered
	// (e.g. an engine anti-entropy probe racing PeerManager.Stop). Such a
	// waiter must not depend on the stopped sweep to expire its deadline.
	select {
	case <-r.stopCh:
		// A response that already claimed the request owns its completion.
		if _, removed := r.tracker.pending.LoadAndDelete(requestID); removed {
			future.reject(ErrReplicationTimeout)
		}
	case <-future.Done():
	}

	if err := future.Wait(); err != nil {
		return nil, err
	}

	return future.Nacks(), nil
}

// SendOneWay sends a notify packet to a single peer without registering
// a tracked request. Used for ephemeral pub/sub effects that are fire-
// and-forget: no retransmits (which would violate at-most-once), no
// future to wait on, the ACK that comes back is silently discarded by
// HandleNotifyACK's missing-entry path.
func (r *Replicator) SendOneWay(notify *pb.OffsetNotify, wireData []byte, targetPeerID NodeId) error {
	notify.EffectData = wireData

	notifyPkt, err := MarshalNotifyPacket(0, notify)
	if err != nil {
		return fmt.Errorf("marshal notify packet: %w", err)
	}

	if _, err := r.transport.Send(targetPeerID, notifyPkt); err != nil {
		return fmt.Errorf("send to peer %d: %w", targetPeerID, err)
	}

	RecordNotificationSent()

	return nil
}

// SendNack sends an enriched NACK to a specific peer.
func (r *Replicator) SendNack(nack *pb.NackNotify, targetPeerID NodeId) {
	pkt, err := MarshalNackPacket(nack)
	if err != nil {
		slog.Error("failed to marshal NACK packet", "error", err)

		return
	}

	if err := r.transport.SendDirect(targetPeerID, pkt); err != nil {
		slog.Debug("failed to send NACK", "peer", targetPeerID, "error", err)
	}
}

// sweepLoop periodically retransmits held same-region requests and expires
// deadline-mode ones.
func (r *Replicator) sweepLoop() {
	defer r.wg.Done()

	ticker := time.NewTicker(sweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			r.tracker.pending.Range(func(key uint64, tr *trackedRequest) bool {
				if _, removed := r.tracker.pending.LoadAndDelete(key); removed {
					tr.future.reject(ErrReplicationTimeout)
				}

				return true
			})

			return
		case <-ticker.C:
			r.sweep()
		}
	}
}

// sweep advances every pending request once per tick. Deadline-mode requests
// (cross-region, bind ReplicateTo) expire on their clock. Same-region
// first-ACK requests (retransmit=true) are driven to durability: a request
// whose target has left the alive+symmetric set is given up — the peer is
// declared dead, so we stop holding the write open and let the originator
// fall back to local commit (it is now the last node standing on this key);
// otherwise an unacked request is retransmitted at retransmitInterval. Pending
// requests may already be processing on the receiver; lack of an ACK does not
// imply that the original notification never arrived.
func (r *Replicator) sweep() {
	now := time.Now().UnixNano()

	r.tracker.pending.Range(func(key uint64, tr *trackedRequest) bool {
		if !tr.retransmit {
			if now > tr.deadline {
				if _, removed := r.tracker.pending.LoadAndDelete(key); removed {
					tr.future.reject(ErrReplicationTimeout)
					RecordRetransmissionGiveUp(tr.peerID)
				}
			}

			return true
		}

		// Give up only when the peer is no longer a replication target: no
		// heartbeat within the liveness timeout. While it IS alive, connFunc
		// always resolves a live connection (outbound, or inbound fallback) we
		// can send over, so retransmission will land — no wall-clock needed.
		if ph := r.healthTable.Get(tr.peerID); ph == nil || !ph.IsReplicationTarget() {
			if _, removed := r.tracker.pending.LoadAndDelete(key); !removed {
				return true
			}

			RecordRetransmissionGiveUp(tr.peerID)
			// Mirror the NotSubscribed accounting: this peer can no longer
			// positively resolve the future. When the last live target drops,
			// reject so the caller (a blocking SafeMode flush) unblocks and
			// commits locally.
			tr.future.drop(ErrNoPeers)

			return true
		}

		if now-tr.lastSentAt.Load() < int64(retransmitInterval) {
			return true
		}

		tr.lastSentAt.Store(now)

		if _, err := r.transport.Send(tr.peerID, tr.packetData); err != nil {
			slog.Debug("retransmit failed (peer connection not up yet)",
				"peer", tr.peerID, "error", err)
		} else {
			RecordNotificationSent()
		}

		return true
	})
}
