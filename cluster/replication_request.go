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
	"sync"
	"sync/atomic"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	pb "github.com/swytchdb/engine/cluster/proto"
)

// ReplicationFuture represents a pending replication operation.
// It resolves when the first same-region peer ACKs or when the deadline expires.
// Callers check Wait()'s return value to distinguish success from failure.
//
// expected/dropped/setupDone implement fast-fail on "every peer says
// they're not subscribed": a NotSubscribed NACK does not resolve the
// future (it would falsely satisfy first-ACK), but when every tracked
// peer's response is NotSubscribed the future rejects immediately
// instead of waiting for the timeout.
type ReplicationFuture struct {
	done      chan struct{}
	once      sync.Once
	err       atomic.Pointer[error]
	nacks     atomic.Pointer[[]*pb.NackNotify] // NACKs from NACK response (status=0x01)
	expected  atomic.Int32                     // total tracker entries attached to this future
	dropped   atomic.Int32                     // tracked peers that can no longer ACK: NotSubscribed, or swept after leaving the alive+symmetric set
	setupDone atomic.Bool                      // true once expected has been finalized
}

// Wait blocks until the replication future is resolved and returns any error.
func (f *ReplicationFuture) Wait() error {
	<-f.done

	return f.Err()
}

// Done returns a channel that is closed when the future resolves.
func (f *ReplicationFuture) Done() <-chan struct{} {
	return f.done
}

// Err returns the error stored in the future, or nil on success.
func (f *ReplicationFuture) Err() error {
	if p := f.err.Load(); p != nil {
		return *p
	}

	return nil
}

// Nacks returns the NACKs attached to this future, or nil.
func (f *ReplicationFuture) Nacks() []*pb.NackNotify {
	if p := f.nacks.Load(); p != nil {
		return *p
	}

	return nil
}

// resolve closes the future successfully. First call wins; subsequent calls are no-ops.
func (f *ReplicationFuture) resolve() {
	f.once.Do(func() {
		close(f.done)
	})
}

// reject stores an error then closes the future. First call wins.
func (f *ReplicationFuture) reject(err error) {
	f.once.Do(func() {
		f.err.Store(&err)
		close(f.done)
	})
}

// drop records a peer that cannot acknowledge this request. Registration must
// finish before the last dropped peer can reject a multi-peer future.
func (f *ReplicationFuture) drop(err error) {
	dropped := f.dropped.Add(1)
	if f.setupDone.Load() && dropped == f.expected.Load() {
		f.reject(err)
	}
}

// rejectedFuture returns a pre-rejected future with the given error.
func rejectedFuture(err error) *ReplicationFuture {
	f := &ReplicationFuture{done: make(chan struct{})}
	f.reject(err)

	return f
}

// requestTracker maps pending request IDs to their futures.
type requestTracker struct {
	pending *xsync.Map[uint64, *trackedRequest]
	nextID  atomic.Uint64
}

type trackedRequest struct {
	future     *ReplicationFuture
	createdAt  int64        // UnixNano
	lastSentAt atomic.Int64 // UnixNano of most recent send attempt (original or retransmit)
	deadline   int64        // UnixNano — absolute deadline (deadline-mode requests only)
	peerID     NodeId
	packetData []byte // serialized plaintext for retransmission

	// retransmit selects the sweep's give-up policy. When true (same-region
	// first-ACK replication) a send that fails only because the peer's
	// connection isn't up yet must be HELD and retried, not dropped: dropping
	// it orphaned compaction snapshots whose dependents later became
	// unreconstructable cluster-wide. The sweep retransmits the packet every
	// tick until the peer ACKs or leaves the alive+symmetric set — no
	// wall-clock deadline. When false (cross-region fire-and-forget, bind
	// ReplicateTo) the sweep uses the deadline-based give-up below.
	retransmit bool

	traceContext []byte // OTel trace context for correlating ACK spans
}

// register fully initializes a request before publishing it to ACK handlers and
// the sweep. Callers set its delivery policy (deadline or retransmit) first.
func (rt *requestTracker) register(tr *trackedRequest, body []byte) uint64 {
	requestID := rt.nextID.Add(1)
	tr.createdAt = time.Now().UnixNano()
	tr.lastSentAt.Store(tr.createdAt)
	tr.packetData = AssembleNotifyPacket(requestID, body)
	tr.future.expected.Add(1)
	rt.pending.Store(requestID, tr)

	return requestID
}
