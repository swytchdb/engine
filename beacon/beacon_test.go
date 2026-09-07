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

package beacon

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/swytchdb/engine/cluster"
	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
)

func TestStartCancellationAfterRegistrationRollsBackMembership(t *testing.T) {
	const nodeID = pb.NodeID(42)

	engine := effects.NewEngine(effects.EngineConfig{NodeID: nodeID})
	t.Cleanup(func() {
		if err := engine.Close(); err != nil {
			t.Errorf("close engine: %v", err)
		}
	})

	registered := make(chan struct{}, 1)
	engine.OnKeyDataAdded = func(key string) {
		if key == MembershipKey {
			select {
			case registered <- struct{}{}:
			default:
			}
		}
	}

	selfOps := make(chan pb.EffectOp, 2)
	engine.SetOnLocalEffect(func(_ effects.Tip, eff *pb.Effect) {
		data := eff.GetData()
		if string(eff.Key) != MembershipKey || data == nil ||
			nodeIDFromBytes(data.Id) != uint64(nodeID) {
			return
		}
		selfOps <- data.Op
	})

	b := New(Config{
		NodeID:        nodeID,
		AdvertiseAddr: "127.0.0.1:7380",
	}, engine, &cluster.PeerManager{})
	// Force the post-registration convergence wait without bootstrapping a
	// real transport. A zero PeerManager has no health table, so the symmetric
	// peer gate is a no-op while convergence still waits for one non-self entry.
	b.expectedPeers = 1

	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)
	go func() {
		errCh <- b.Start(ctx)
	}()

	select {
	case <-registered:
		cancel()
	case <-time.After(5 * time.Second):
		t.Fatal("self-registration did not complete")
	}

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Start error = %v, want context cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after cancellation")
	}

	for _, want := range []pb.EffectOp{pb.EffectOp_INSERT_OP, pb.EffectOp_REMOVE_OP} {
		select {
		case got := <-selfOps:
			if got != want {
				t.Fatalf("self membership operation = %v, want %v", got, want)
			}
		default:
			t.Fatalf("missing self membership operation %v", want)
		}
	}

	snapshot, _, err := engine.NewReadOnlyContext().GetSnapshot(MembershipKey)
	if err != nil {
		t.Fatalf("read membership after failed Start: %v", err)
	}
	for _, member := range parseMembership(snapshot) {
		if member.NodeID == uint64(nodeID) {
			t.Fatalf("failed Start left self registered: %+v", member)
		}
	}

	// Runtime assigns the beacon before an asynchronous Start completes. A
	// later Stop must not emit a second REMOVE after Start already rolled back.
	b.Stop()
	select {
	case op := <-selfOps:
		t.Fatalf("Stop emitted duplicate self membership operation %v", op)
	default:
	}
}
