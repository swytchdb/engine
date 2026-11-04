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
	"context"
	"errors"
	"testing"
	"time"
)

func TestWaitForInitialConnectionsAcceptsFailedAttempt(t *testing.T) {
	// Missing TLS makes a real connection attempt fail immediately. Waiting
	// for successful connectivity instead would never finish this join.
	pc := newPeerConn(2, "127.0.0.1:1", "", "", nil, nil)
	pm := &PeerManager{peers: map[NodeId]*PeerConn{2: pc}}
	pc.Start(t.Context())
	t.Cleanup(pc.Stop)

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	if err := pm.WaitForInitialConnections(ctx); err != nil {
		t.Fatal(err)
	}

	if pc.IsStreamReady() {
		t.Fatal("failed dial unexpectedly connected")
	}
}

func TestWaitForInitialConnectionsWaitsForEveryAttempt(t *testing.T) {
	first, second := make(chan struct{}), make(chan struct{})
	close(first)
	pm := &PeerManager{peers: map[NodeId]*PeerConn{
		1: {firstAttempt: first},
		2: {firstAttempt: second},
	}}

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	if err := pm.WaitForInitialConnections(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unfinished peer attempt did not block: %v", err)
	}

	close(second)

	if err := pm.WaitForInitialConnections(t.Context()); err != nil {
		t.Fatal(err)
	}
}
