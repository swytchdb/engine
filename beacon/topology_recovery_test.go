package beacon

import (
	"context"
	"encoding/binary"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/swytchdb/engine/cluster"
	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var errMembershipFetch = errors.New("temporary membership fetch failure")

type membershipFetchFault struct {
	effects.Broadcaster

	available atomic.Bool
	failed    chan struct{}
	wire      []byte
}

func (p *membershipFetchFault) FetchFromAny(_ *pb.EffectRef, _ effects.FetchHint) ([]byte, error) {
	if !p.available.Load() {
		signal(p.failed)

		return nil, errMembershipFetch
	}

	return p.wire, nil
}

// A membership tip arrives before its dependency can be fetched, as happens
// when a joining node replaces its temporary connection. No sockets or timing
// race are needed to exercise the failed read and the topology observer.
func membershipWithMissingDependency(t *testing.T) (*Beacon, *membershipFetchFault) {
	t.Helper()

	e := effects.NewEngine(effects.EngineConfig{})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})

	const address = "127.0.0.1:7380"

	pm, err := cluster.NewPeerManager(&cluster.ClusterConfig{
		NodeID: e.NodeID(), Nodes: []cluster.NodeConfig{{ID: e.NodeID(), Address: address}},
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	b := New(Config{NodeID: e.NodeID(), AdvertiseAddr: address}, e, pm)
	if err := b.registerSelf(); err != nil {
		t.Fatal(err)
	}

	if err := b.refreshTopology(); err != nil {
		t.Fatal(err)
	}

	if len(b.Members()) != 1 {
		t.Fatal("initial self membership missing")
	}

	_, tips, err := e.NewReadOnlyContext().GetSnapshot(MembershipKey)
	if err != nil || len(tips) != 1 {
		t.Fatalf("initial tips=%v error=%v", tips, err)
	}

	const peer = pb.NodeID(42)

	stamp := timestamppb.Now()
	missing := buildMemberInsert(uint64(e.NodeID()), "127.0.0.1:7381")
	missing.NodeId, missing.Hlc = uint64(peer), stamp
	missing.ForkChoiceHash = effects.ComputeForkChoiceHash(peer, stamp)
	missing.Deps = []*pb.EffectRef{{NodeId: tips[0][0], Offset: tips[0][1]}}
	remote := &pb.Effect{
		Key: []byte(MembershipKey), NodeId: uint64(peer), Hlc: stamp,
		ForkChoiceHash: effects.ComputeForkChoiceHash(peer, stamp),
		Deps:           []*pb.EffectRef{{NodeId: uint64(peer), Offset: 1}},
		Kind:           &pb.Effect_Meta{Meta: &pb.MetaEffect{TypeTag: pb.ValueType_TYPE_HASH}},
	}

	wire, err := effects.MarshalEffect(remote)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := e.HandleRemote(effects.BuildOffsetNotify(peer, effects.Tip{uint64(peer), 2}, remote, wire, t.Context())); err != nil {
		t.Fatal(err)
	}

	missingWire, err := effects.MarshalEffect(missing)
	if err != nil {
		t.Fatal(err)
	}

	fetchWire := binary.LittleEndian.AppendUint32(nil, uint32(len(MembershipKey)))
	fetchWire = append(fetchWire, MembershipKey...)
	fetchWire = append(fetchWire, missingWire...)
	probe := &membershipFetchFault{Broadcaster: pm, failed: make(chan struct{}, 1), wire: fetchWire}
	e.SetBroadcaster(probe)

	// Only topologyLoop's initial read should drive recovery, not a leftover
	// notification from setup or a later test-triggered membership change.
	for len(b.topoRefresh) > 0 {
		<-b.topoRefresh
	}

	return b, probe
}

func TestMembershipIncompleteReadPreservesTopology(t *testing.T) {
	b, probe := membershipWithMissingDependency(t)

	_, _, err := b.engine.NewReadOnlyContext().GetSnapshot(MembershipKey)
	if !errors.Is(err, errMembershipFetch) {
		t.Errorf("incomplete read must preserve the fetch error, got %v", err)
	}

	if err := b.refreshTopology(); !errors.Is(err, errMembershipFetch) {
		t.Errorf("projection must report the fetch error, got %v", err)
	}

	if members := b.Members(); len(members) != 1 || members[0].Addr != b.cfg.AdvertiseAddr {
		t.Fatalf("incomplete read replaced the last valid topology: %v", members)
	}

	probe.available.Store(true)

	if err := b.refreshTopology(); err != nil {
		t.Fatal(err)
	}

	if members := b.Members(); len(members) != 1 || members[0].Addr != "127.0.0.1:7381" {
		t.Fatalf("restored fetch did not recover membership: %v", members)
	}

	// An actual empty roster must still clear topology. Skipping every nil
	// snapshot would hide failures but also ignore legitimate departures.
	ctx := b.engine.NewContext()
	if err := ctx.Emit(buildMemberRemove(uint64(b.cfg.NodeID))); err != nil {
		t.Fatal(err)
	}

	if err := ctx.Flush(); err != nil {
		t.Fatal(err)
	}

	if err := b.refreshTopology(); err != nil {
		t.Fatal(err)
	}

	if members := b.Members(); len(members) != 0 {
		t.Fatalf("authoritative empty membership was ignored: %v", members)
	}
}

func TestMembershipProjectionRetriesWithoutAnotherNotification(t *testing.T) {
	b, probe := membershipWithMissingDependency(t)
	b.ctx, b.cancel = context.WithCancel(t.Context())
	done := make(chan struct{})

	go func() {
		defer close(done)

		b.topologyLoop()
	}()

	t.Cleanup(func() {
		b.cancel()
		<-done
	})

	select {
	case <-probe.failed:
	case <-time.After(time.Second):
		t.Fatal("initial membership read did not attempt the missing dependency")
	}

	probe.available.Store(true)

	deadline := time.Now().Add(2 * time.Second)

	for {
		members := b.Members()
		if len(members) == 1 && members[0].Addr == "127.0.0.1:7381" {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("membership never retried after fetch recovery: %v", members)
		}

		time.Sleep(time.Millisecond)
	}
}

func TestMembershipProjectionCancelsDuringFailure(t *testing.T) {
	b, probe := membershipWithMissingDependency(t)

	b.ctx, b.cancel = context.WithCancel(t.Context())
	defer b.cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)

		b.topologyLoop()
	}()

	select {
	case <-probe.failed:
	case <-time.After(time.Second):
		t.Fatal("initial membership read did not fail")
	}

	b.cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("membership retry did not stop on cancellation")
	}
}
