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
	"maps"
	"sync"
	"testing"
	"time"
)

func TestReplicationFutureFirstCompletionWins(t *testing.T) {
	t.Run("ACK before timeout", func(t *testing.T) {
		f := &ReplicationFuture{done: make(chan struct{})}
		f.resolve()
		f.reject(ErrReplicationTimeout)

		if err := f.Wait(); err != nil {
			t.Fatalf("late timeout changed successful ACK: %v", err)
		}
	})
	t.Run("timeout before ACK", func(t *testing.T) {
		f := rejectedFuture(ErrReplicationTimeout)
		f.resolve()
		f.reject(ErrNoPeers)

		if err := f.Wait(); !errors.Is(err, ErrReplicationTimeout) {
			t.Fatalf("first failure changed: %v", err)
		}
	})
}

func TestReplicationMembershipSnapshot(t *testing.T) {
	r := NewReplicator(1, "local", NewPeerHealthTable(ClockConfig{}), nil, nil, time.Second)
	r.AddPeer(2, "local")
	r.AddPeer(3, "remote")
	snapshot := r.regionsSnapshot()

	var wg sync.WaitGroup
	wg.Go(func() {
		for range 100 {
			r.AddPeer(2, "changed")
			r.RemovePeer(3)
			r.AddPeer(3, "remote")
		}
	})

	for range 100 {
		if !maps.Equal(snapshot, map[NodeId]string{2: "local", 3: "remote"}) {
			t.Error("membership update mutated an in-flight snapshot")
		}
	}

	wg.Wait()
	r.RemovePeer(3)

	if got := r.regionsSnapshot(); !maps.Equal(got, map[NodeId]string{2: "changed"}) {
		t.Fatalf("new readers did not see membership changes: %v", got)
	}
}

func TestReplicationSweepExpiresOnlyDeadlineRequests(t *testing.T) {
	health := NewPeerHealthTable(ClockConfig{})
	peer := health.GetOrCreate(2)
	peer.alive.Store(true)
	peer.symmetric.Store(true)

	r := NewReplicator(1, "local", health, nil, nil, time.Second)
	deadline := &ReplicationFuture{done: make(chan struct{})}
	held := &ReplicationFuture{done: make(chan struct{})}

	r.tracker.pending.Store(1, &trackedRequest{future: deadline, deadline: 1})
	request := &trackedRequest{future: held, peerID: 2, retransmit: true}
	request.lastSentAt.Store(time.Now().Add(time.Hour).UnixNano())
	r.tracker.pending.Store(2, request)
	r.sweep()

	if err := deadline.Wait(); !errors.Is(err, ErrReplicationTimeout) {
		t.Fatalf("expired request: %v", err)
	}

	select {
	case <-held.Done():
		t.Fatal("same-region request expired on a deadline")
	default:
	}

	if _, ok := r.tracker.pending.Load(1); ok {
		t.Fatal("expired request retained")
	}
}
