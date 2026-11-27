package effects

import (
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"google.golang.org/protobuf/proto"
)

func TestSubscriptionReceiptRecoversOnlyItsKey(t *testing.T) {
	e, binds, _ := backfillHistory(1, []string{"a", "b"})
	defer func() { _ = e.Close() }()

	bind, missing, observation := binds[0], Tip{98, 1}, Tip{99, 1}
	e.updateIndex(logicalAddress("a"), nil, bind)
	e.updateIndex(logicalAddress("b"), nil, missing)

	local := &pb.Effect{Key: []byte("a"), Deps: toPbRefs([]Tip{bind}), Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}}}
	transport := errors.New("other key unavailable")
	localLoads, otherLoads := 0, 0
	e.broadcaster = &cutBroadcaster{load: func(tip Tip) ([]byte, error) {
		switch tip {
		case observation:
			localLoads++

			return wireEffect(local), nil
		case missing:
			otherLoads++

			return nil, transport
		default:
			t.Fatalf("unexpected dependency %v", tip)

			return nil, ErrEffectNotFound
		}
	}}

	arrival := Tip{77, 1}
	eff := &pb.Effect{
		Key: []byte("a"), NodeId: arrival[0], Hlc: sTs(77),
		ForkChoiceHash: ComputeForkChoiceHash(pb.NodeID(arrival[0]), sTs(77)),
		Deps:           toPbRefs([]Tip{observation}),
		Kind:           &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{SubscriberNodeId: arrival[0]}},
	}

	data, err := MarshalEffect(eff)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := e.HandleRemote(BuildOffsetNotify(pb.NodeID(arrival[0]), arrival, eff, data, nil)); err != nil {
		t.Fatal(err)
	}

	if localLoads != 1 || otherLoads != 0 {
		t.Fatalf("subscription fetches: own key=%d other key=%d", localLoads, otherLoads)
	}

	requireCutTips(t, e.index.Contains("a").Tips(), []Tip{arrival})
	requireCutTips(t, e.index.Contains("b").Tips(), []Tip{missing})

	if len(e.PeerSubscribers("a")) != 1 || e.PeerSubscribers("a")[0] != pb.NodeID(arrival[0]) {
		t.Fatal("subscription receipt lost membership")
	}

	// Bootstrap/read recovery still needs cross-key evidence for this open
	// competition; receipt of membership must not certify that closure.
	_, complete, err := e.closeIngestedFrontier(logicalAddress("a"), []Tip{arrival}, make(map[Tip]error))
	if complete || !errors.Is(err, transport) || otherLoads == 0 {
		t.Fatalf("open bind bypassed recovery: complete=%t err=%v other loads=%d", complete, err, otherLoads)
	}
}

func TestSubscriptionNackDoesNotExpandBind(t *testing.T) {
	for _, width := range []int{16, 1024} {
		for _, discovery := range []bool{false, true} {
			t.Run(fmt.Sprintf("keys=%d/discovery=%t", width, discovery), func(t *testing.T) {
				e, key, owner := completedClosureFixture(t, width)
				snapshot, _ := e.effectCache.Get(owner)
				bind := r(snapshot.GetDeps()[0])
				e.updateIndex(key, nil, bind)

				bc := &mockBroadcaster{}
				e.broadcaster = bc

				arrival := Tip{77, 1}
				eff := &pb.Effect{
					Key: []byte(wireKey(key)), NodeId: arrival[0], Hlc: sTs(77),
					ForkChoiceHash: ComputeForkChoiceHash(pb.NodeID(arrival[0]), sTs(77)),
					// This dependency differs from the receiver's initial tip.
					// Membership must still produce only one bootstrap reply.
					Deps: toPbRefs([]Tip{owner}),
					Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{
						SubscriberNodeId: arrival[0], Discovery: discovery,
					}},
				}

				data, err := MarshalEffect(eff)
				if err != nil {
					t.Fatal(err)
				}

				nacks, err := e.HandleRemote(BuildOffsetNotify(pb.NodeID(arrival[0]), arrival, eff, data, nil))
				if err != nil {
					t.Fatal(err)
				}

				nacks = append(nacks, bc.sentNacks...)
				if len(nacks) != 1 {
					t.Fatalf("expected one reply, got %d", len(nacks))
				}

				nack := nacks[0]
				if len(nack.GetTips()) != 1 || r(nack.GetTips()[0]) != bind {
					t.Fatalf("lost advertised bind: %v", nack.GetTips())
				}

				if len(nack.GetTipDetails()) != 1 || r(nack.GetTipDetails()[0].GetRef()) != bind {
					t.Fatalf("lost asynchronous ingestion reference: %v", nack.GetTipDetails())
				}

				// The wire reply needs one key and its frontier references,
				// regardless of the number of keys in the advertised bind.
				if proto.Size(nack) > len(nack.GetKey())+128 {
					t.Fatalf("subscription reply expanded history: %d bytes", proto.Size(nack))
				}
			})
		}
	}
}

func TestSubscriptionNackReferencesRecoverHistory(t *testing.T) {
	source, binds, _ := backfillHistory(2, []string{"wanted", "other"})
	defer func() { _ = source.Close() }()

	latest := binds[len(binds)-1]
	nack := source.buildSubscriptionNack(logicalAddress("wanted"), toPbRef(Tip{77, 1}), []Tip{latest, {99, 99}})

	data, err := proto.Marshal(nack)
	if err != nil {
		t.Fatal(err)
	}

	reply := &pb.NackNotify{}
	if err := proto.Unmarshal(data, reply); err != nil {
		t.Fatal(err)
	}

	if len(reply.GetTips()) != 1 || len(reply.GetTipDetails()) != 1 {
		t.Fatal("reply advertised a nonresident tip")
	}

	loads := 0

	receiver := newTxnTestEngine(&cutBroadcaster{load: func(tip Tip) ([]byte, error) {
		loads++

		eff, ok := source.effectCache.Get(tip)
		if !ok {
			return nil, ErrEffectNotFound
		}

		data, err := source.effectCache.MarshalEffect(eff)
		if err != nil {
			return nil, err
		}

		wire := binary.LittleEndian.AppendUint32(nil, uint32(len(eff.GetKey())))
		wire = append(wire, eff.GetKey()...)

		return append(wire, data...), nil
	}})
	defer func() { _ = receiver.Close() }()

	preSubscribe(receiver, "wanted")

	if err := receiver.HandleNack(reply); err != nil {
		t.Fatal(err)
	}

	requireCutTips(t, receiver.index.Contains("wanted").Tips(), []Tip{latest})

	value, _, _, err := receiver.GetSnapshot("wanted")
	if err != nil || string(value.GetScalar().GetRaw()) != "txn-1" || loads == 0 {
		t.Fatalf("reference-only history: value=%v error=%v loads=%d", value, err, loads)
	}
}
