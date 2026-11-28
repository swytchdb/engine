package effects

import (
	"errors"
	"slices"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"google.golang.org/protobuf/proto"
)

func TestVerdictSubscriberSetWirePresence(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  *pb.SubscriberSet
	}{
		{name: "legacy"},
		{name: "empty", set: &pb.SubscriberSet{}},
		{name: "outstanding", set: &pb.SubscriberSet{NodeIds: []uint64{9, 11}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, original := range []proto.Message{
				&pb.TransactionalBindEffect{Subscribers: tc.set},
				&pb.SnapshotEffect{OutstandingSubscribers: tc.set},
			} {
				data, err := proto.Marshal(original)
				if err != nil {
					t.Fatal(err)
				}

				decoded := original.ProtoReflect().New().Interface()
				if err := proto.Unmarshal(data, decoded); err != nil {
					t.Fatal(err)
				}

				if !proto.Equal(original, decoded) {
					t.Fatalf("subscriber presence changed: %v -> %v", original, decoded)
				}
			}
		})
	}
}

func TestCompletedVerdictSkipsProofValidation(t *testing.T) {
	e := newTestEngine(nil)

	for _, tc := range []struct {
		name    string
		set     *pb.SubscriberSet
		settled bool
	}{
		{name: "legacy"},
		{name: "outstanding", set: &pb.SubscriberSet{NodeIds: []uint64{9}}},
		{name: "complete", set: &pb.SubscriberSet{}, settled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// No owner or proof vertex exists: any validation attempt must fail.
			eff := bindEvidenceSnapshot("k", &pb.BindEvidence{Binds: toPbRefs([]Tip{{9, 1}})})
			eff.GetSnapshot().TxnVerdicts = map[string]pb.Verdict{"won": pb.Verdict_WON, "lost": pb.Verdict_LOST}
			eff.GetSnapshot().OutstandingSubscribers = tc.set

			verdicts, err := e.proofVerdicts(Tip{9, 2}, eff)
			if !tc.settled {
				if !errors.Is(err, errEffectNotResident) {
					t.Fatalf("missing evidence was not validated: %v", err)
				}

				return
			}

			if err != nil || !verdicts["won"].settled || !verdicts["lost"].settled ||
				verdicts["won"].verdict != pb.Verdict_WON || verdicts["lost"].verdict != pb.Verdict_LOST {
				t.Fatalf("completed outcomes = %v, error = %v", verdicts, err)
			}
		})
	}
}

func TestCompletedVerdictsSkipCompetitors(t *testing.T) {
	f := newCrossKeyHistory(t, false)

	for _, settled := range []bool{false, true} {
		verdicts := verdictMap{
			"shared":     {verdict: pb.Verdict_WON, proven: true, settled: settled},
			"competitor": {verdict: pb.Verdict_WON, proven: true, settled: settled},
		}

		losers, _ := f.engine.losersOnKey(logicalAddress("el-0"), nil, verdicts)
		if settled && len(losers) != 0 {
			t.Fatalf("completed outcomes reopened competition: %v", losers)
		}

		if !settled {
			if _, lost := losers["competitor"]; !lost {
				t.Fatal("legacy outcomes skipped competition")
			}
		}
	}
}

func TestFlushTxFreezesSubscribersBeforePublication(t *testing.T) {
	bc := &mockBroadcaster{peerIDs: []pb.NodeID{99, 100}, allRegionPeersReachable: true}
	e := newTxnTestEngine(bc)
	preSubscribe(e, "k")

	seed := e.NewContext()
	if err := seed.Emit(scalarSet("k", "initial")); err != nil {
		t.Fatal(err)
	}

	if err := seed.Flush(); err != nil {
		t.Fatal(err)
	}

	e.addPeerSubscriber(logicalAddress("k"), 99)

	var bind *pb.TransactionalBindEffect

	e.OnLocalEffect = func(_ Tip, eff *pb.Effect) {
		if eff.GetTxnBind() != nil {
			bind = eff.GetTxnBind()
			// A later subscriber must not change the already published set.
			e.addPeerSubscriber(logicalAddress("k"), 100)
		}
	}
	c := e.NewContext()
	c.BeginTx()

	if err := c.Emit(scalarSet("k", "value")); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	if !slices.Equal(bind.GetSubscribers().GetNodeIds(), []uint64{99}) || !slices.Equal(bc.replicateToPeers, []pb.NodeID{99}) {
		t.Fatalf("bind subscribers=%v, replicated=%v", bind.GetSubscribers(), bc.replicateToPeers)
	}

	snapshot, err := e.getEffect(logicalAddress("k"), e.index.Contains("k").Tips()[0])
	if err != nil {
		t.Fatal(err)
	}

	if snapshot.GetSnapshot().GetOutstandingSubscribers() == nil || len(snapshot.GetSnapshot().GetOutstandingSubscribers().GetNodeIds()) != 0 {
		t.Fatalf("processed response did not clear outstanding subscribers: %v", snapshot)
	}
}

func TestCompetitionOutstandingIncludesOtherBindParticipants(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)

	refs := make([]Tip, 0, 3)
	for _, bind := range []*pb.TransactionalBindEffect{
		{OriginatorNodeId: 1, Subscribers: &pb.SubscriberSet{NodeIds: []uint64{9}}},
		{OriginatorNodeId: 9, Subscribers: &pb.SubscriberSet{NodeIds: []uint64{1, 11}}},
		{OriginatorNodeId: 12, Subscribers: &pb.SubscriberSet{NodeIds: []uint64{9}}},
	} {
		refs = append(refs, log.putEffect(&pb.Effect{Key: []byte("k"), Kind: &pb.Effect_TxnBind{TxnBind: bind}}))
	}

	proof := &pb.BindEvidence{Binds: toPbRefs(refs)}

	got := e.competitionOutstanding(proof, map[uint64]bool{1: true, 9: true})
	if !slices.Equal(got.GetNodeIds(), []uint64{11, 12}) {
		t.Fatalf("uncontacted competitor subscriber and origin = %v, want [11 12]", got)
	}

	got = e.competitionOutstanding(proof, map[uint64]bool{1: true, 9: true, 11: true, 12: true})
	if got == nil || len(got.GetNodeIds()) != 0 {
		t.Fatalf("completed competition = %v, want explicit empty set", got)
	}

	legacy := log.putEffect(&pb.Effect{Key: []byte("k"), Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{}}})

	proof.Binds = append(proof.Binds, toPbRef(legacy))
	if e.competitionOutstanding(proof, map[uint64]bool{1: true, 9: true, 11: true, 12: true}) != nil {
		t.Fatal("legacy membership became an explicitly empty set")
	}
}

func TestCompletedVerdictRetainsLegacyWireEvidence(t *testing.T) {
	source := newTxnTestEngine(nil)
	c := source.NewContext()
	c.BeginTx()

	txn := c.txnID
	if err := c.Emit(scalarSet("k", "value")); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	owner := source.index.Contains("k").Tips()[0]

	eff, err := source.getEffect(logicalAddress("k"), owner)
	if err != nil {
		t.Fatal(err)
	}

	data, err := source.effectCache.MarshalEffect(eff)
	if err != nil {
		t.Fatal(err)
	}

	wire := &pb.Effect{}
	if err := UnmarshalEffect(data, wire); err != nil {
		t.Fatal(err)
	}

	if wire.GetSnapshot().GetOutstandingSubscribers() == nil || len(wire.GetEvidencePackage()) == 0 {
		t.Fatal("completed snapshot omitted completion state or legacy evidence")
	}
	// Simulate an older reader ignoring the newly added field.
	wire.GetSnapshot().OutstandingSubscribers = nil

	legacy := newTestEngine(nil)
	if err := legacy.effectCache.Put(owner, wire); err != nil {
		t.Fatal(err)
	}

	stored, _ := legacy.effectCache.Get(owner)

	verdicts, err := legacy.proofVerdicts(owner, stored)
	if err != nil || !verdicts[txn].proven || verdicts[txn].settled || verdicts[txn].verdict != pb.Verdict_WON {
		t.Fatalf("legacy proof evaluation=%v, error=%v", verdicts, err)
	}
}
