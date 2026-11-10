package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
)

func TestFetchExhaustionPreservesUnreachableHolder(t *testing.T) {
	pms := startNNodeCluster(t,
		[]EffectHandler{&recordingHandler{}, &recordingHandler{}, &recordingHandler{}},
		[]LogReader{newStaticLogReader(), newStaticLogReader(), newStaticLogReader()},
	)

	conns := pms[0].fetchConns()
	if len(conns) < 2 {
		t.Fatal("test requires multiple peer connections")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ref := &pb.EffectRef{NodeId: 42, Offset: 99}
	if _, err := pms[0].fetchFromConns(ctx, ref, conns); !errors.Is(err, effects.ErrEffectNotFound) {
		t.Fatalf("reachable holders with no effect: %v", err)
	}

	if err := conns[0].CloseWithError(0, "test unavailable holder"); err != nil {
		t.Fatal(err)
	}
	// The closed connection fails immediately; the surviving peers reply
	// missing afterward. Arrival order must not turn that into exhaustion.
	if _, err := pms[0].fetchFromConns(ctx, ref, conns); err == nil || errors.Is(err, effects.ErrEffectNotFound) {
		t.Fatalf("unreachable holder classified as missing: %v", err)
	}
}
