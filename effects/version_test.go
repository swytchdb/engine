package effects

import (
	"slices"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

type versionBroadcaster struct {
	mockBroadcaster

	minimum string
}

func (b *versionBroadcaster) MinimumVersion() string { return b.minimum }

func TestVerdictEvidenceVersionGate(t *testing.T) {
	for _, tc := range []struct {
		version  string
		evidence bool
	}{
		{"dev", false},
		{"1.8.0", false},
		{"v1.10.0", false},
		{"1.7.9", true},
		{"1.8.0-rc.1", true},
		{"", true},
		{"invalid", true},
	} {
		for _, clustered := range []bool{false, true} {
			t.Run(tc.version+map[bool]string{false: "/standalone", true: "/cluster"}[clustered], func(t *testing.T) {
				e := newTxnTestEngine(nil)
				defer func() { _ = e.Close() }()

				e.buildVersion = tc.version
				if clustered {
					e.broadcaster = &versionBroadcaster{minimum: tc.version}
				}

				for range 2 {
					c := e.NewContext()
					c.BeginTx()

					txn := c.txnID
					if err := c.Emit(scalarSet("k", "value")); err != nil {
						t.Fatal(err)
					}

					if err := c.Flush(); err != nil {
						t.Fatal(err)
					}

					tip := e.index.Contains("k").Tips()[0]

					eff, err := e.getEffect(logicalAddress("k"), tip)
					if err != nil {
						t.Fatal(err)
					}

					snap := eff.GetSnapshot()
					if (snap.GetEvidence() != nil) != tc.evidence {
						t.Fatalf("evidence present=%t, want %t", snap.GetEvidence() != nil, tc.evidence)
					}

					if snap.GetOutstandingSubscribers() == nil || len(snap.GetOutstandingSubscribers().GetNodeIds()) != 0 {
						t.Fatal("omitting evidence lost competition closure")
					}

					wire, err := e.effectCache.MarshalEffect(eff)
					if err != nil {
						t.Fatal(err)
					}

					decoded := &pb.Effect{}
					if err := UnmarshalEffect(wire, decoded); err != nil {
						t.Fatal(err)
					}

					if (len(decoded.GetEvidencePackage()) != 0) != tc.evidence {
						t.Fatal("unexpected wire evidence package")
					}

					verdicts := make(verdictMap)
					if err := e.harvestProofVerdicts(tip, decoded, verdicts); err != nil || !verdicts[txn].settled {
						t.Fatalf("verdict cannot be read: %v, %v", verdicts, err)
					}

					value, _, _, err := e.GetSnapshot("k")
					if err != nil || string(value.GetScalar().GetRaw()) != "value" {
						t.Fatalf("value=%v, err=%v", value, err)
					}
				}
			})
		}
	}
}

func TestVerdictOutstandingWithoutPredecessorEvidence(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)

	bind := log.putEffect(&pb.Effect{Key: []byte("k"), Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
		OriginatorNodeId: 1, Subscribers: &pb.SubscriberSet{NodeIds: []uint64{2}},
	}}})
	for _, tc := range []struct {
		name  string
		prior *pb.SubscriberSet
		want  *pb.SubscriberSet
	}{
		{"closed", &pb.SubscriberSet{}, &pb.SubscriberSet{}},
		{"outstanding", &pb.SubscriberSet{NodeIds: []uint64{2, 3}}, &pb.SubscriberSet{NodeIds: []uint64{3}}},
		{"legacy", nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := log.putEffect(&pb.Effect{Key: []byte("k"), Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
				TxnVerdicts: map[string]pb.Verdict{"old": pb.Verdict_LOST}, OutstandingSubscribers: tc.prior,
			}}})

			got := e.verdictOutstanding(bind, nil, []Tip{source, source}, map[uint64]bool{1: true, 2: true})
			if (got == nil) != (tc.want == nil) || !slices.Equal(got.GetNodeIds(), tc.want.GetNodeIds()) {
				t.Fatalf("outstanding=%v, want %v", got, tc.want)
			}
		})
	}
}
