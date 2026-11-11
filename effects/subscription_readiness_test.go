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
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"

	pb "github.com/swytchdb/engine/cluster/proto"
)

type gatedBootstrapBroadcaster struct {
	mockBroadcaster

	rounds atomic.Int32
	binds  atomic.Int32
	gates  [2]chan struct{}
}

func (b *gatedBootstrapBroadcaster) ReplicateTo(n *pb.OffsetNotify, _ []byte, _ pb.NodeID) ([]*pb.NackNotify, error) {
	var effect pb.Effect
	if err := UnmarshalEffect(n.GetEffectData()[4+len(n.GetKey()):], &effect); err != nil {
		return nil, err
	}

	if effect.GetSubscription() != nil {
		round := b.rounds.Add(1)
		if round <= 2 {
			<-b.gates[round-1]
		}
		// An empty NACK still identifies a peer subscriber. No ancestry is needed.
		return []*pb.NackNotify{{Key: n.GetKey()}}, nil
	}

	if effect.GetTxnBind() != nil {
		b.binds.Add(1)
	}

	return nil, nil
}

func (b *gatedBootstrapBroadcaster) ReplicateMarshalled(n *pb.OffsetNotify, _ []byte, target pb.NodeID) ([]*pb.NackNotify, error) {
	return b.ReplicateTo(n, n.GetEffectData(), target)
}

func TestAsyncSubscriptionRequiresDiscoveryBeforeCommit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := &gatedBootstrapBroadcaster{peerIDs: []pb.NodeID{2}, allRegionPeersReachable: true, gates: [2]chan struct{}{make(chan struct{}), make(chan struct{})}}
		e := newTestEngine(b)
		e.safety.Store(&safetyMap{defaultMode: SafeMode})

		c := e.NewContext()
		if err := c.Watch("fresh"); err != nil {
			t.Fatal(err)
		}

		synctest.Wait()

		state, _ := e.subscriptions.Load("fresh")
		select {
		case <-state.ready:
			t.Error("async announcement claimed completed discovery")
		default:
		}
		// Another ordinary caller may still use the async subscription.
		if err := e.ensureSubscribed("fresh"); err != nil {
			t.Fatal(err)
		}

		c.BeginTx()

		done := make(chan error, 1)

		go func() {
			if err := c.Emit(dataEffect("fresh")); err != nil {
				done <- err

				return
			}

			done <- c.Flush()
		}()
		// Concurrent blocking callers must share the same discovery, not redo it.
		waiter := make(chan error, 1)
		go func() { waiter <- e.ensureSubscribedBlocking("fresh") }()

		for round := range 2 {
			synctest.Wait()

			if got := b.rounds.Load(); got != int32(round+1) {
				t.Errorf("rounds=%d, want %d", got, round+1)
			}

			if len(done) != 0 || len(waiter) != 0 || b.binds.Load() != 0 {
				t.Error("transaction or blocking caller escaped unfinished bootstrap")
			}

			close(b.gates[round])
		}

		synctest.Wait()

		if err := <-done; err != nil {
			t.Fatal(err)
		}

		if err := <-waiter; err != nil {
			t.Fatal(err)
		}

		if b.rounds.Load() != 2 || b.binds.Load() != 1 {
			t.Errorf("rounds=%d binds=%d, want 2 and 1", b.rounds.Load(), b.binds.Load())
		}

		select {
		case <-state.ready:
		default:
			t.Error("successful discovery did not mark subscription ready")
		}
	})
}

func TestAsyncSubscriptionUpgradePropagatesPartition(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := &gatedBootstrapBroadcaster{peerIDs: []pb.NodeID{2}, allRegionPeersReachable: true, gates: [2]chan struct{}{make(chan struct{}), make(chan struct{})}}
		e := newTestEngine(b)
		e.safety.Store(&safetyMap{defaultMode: SafeMode})

		if err := e.ensureSubscribed("fresh"); err != nil {
			t.Fatal(err)
		}

		synctest.Wait()

		state, _ := e.subscriptions.Load("fresh")

		done := make(chan error, 1)
		go func() { done <- e.ensureSubscribedBlocking("fresh") }()

		synctest.Wait()

		b.allRegionPeersReachable = false
		close(b.gates[0])
		close(b.gates[1])
		synctest.Wait()

		if err := <-done; !errors.Is(err, ErrRegionPartitioned) {
			t.Errorf("upgrade error=%v, want partition", err)
		}

		if _, ok := e.subscriptions.Load("fresh"); ok {
			t.Error("failed upgrade retained subscription")
		}

		select {
		case <-state.ready:
		default:
			t.Error("failed upgrade left waiters blocked")
		}

		if !state.incomplete.Load() {
			t.Error("failed upgrade marked ready without recording failure")
		}
	})
}
