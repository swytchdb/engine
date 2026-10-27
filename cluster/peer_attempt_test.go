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
	pc := newPeerConn(2, "127.0.0.1:1", "", "", nil, nil, nil)
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
