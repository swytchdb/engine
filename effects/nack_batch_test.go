package effects

import (
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
	"google.golang.org/protobuf/proto"
)

func TestNackBatchKeepsKeysAndRicherDetails(t *testing.T) {
	e := newTestEngine(nil)
	ref := toPbRef(Tip{7, 1})
	first := &pb.NackNotify{Key: []byte("a"), TipDetails: []*pb.NackTipDetail{{Ref: ref}}}
	rich := &pb.NackNotify{Key: []byte("a"), TipDetails: []*pb.NackTipDetail{{Ref: ref, IsBind: true, Deps: toPbRefs([]Tip{{7, 2}})}}}
	other := &pb.NackNotify{Key: []byte("b"), TipDetails: rich.GetTipDetails()}
	before := proto.Clone(first)

	groups := e.groupNacks([]*pb.NackNotify{first, rich, rich, first, other})
	if len(groups) != 2 || groups[0].key != logicalAddress("a") || groups[1].key != logicalAddress("b") {
		t.Fatalf("key projections were merged: %v", groups)
	}

	if len(groups[0].details) != 1 || !groups[0].details[0].GetIsBind() || len(groups[0].details[0].GetDeps()) == 0 {
		t.Fatalf("richer duplicate details were lost: %v", groups[0].details)
	}

	if !proto.Equal(first, before) {
		t.Fatal("batching mutated a caller's response")
	}
}

type batchReplyBroadcaster struct {
	mockBroadcaster

	reply func(*pb.OffsetNotify, pb.NodeID) ([]*pb.NackNotify, error)
	fetch func(*pb.EffectRef) ([]byte, error)
}

func (b *batchReplyBroadcaster) ReplicateMarshalled(n *pb.OffsetNotify, _ []byte, target pb.NodeID) ([]*pb.NackNotify, error) {
	return b.reply(n, target)
}

func (b *batchReplyBroadcaster) FetchFromAny(ref *pb.EffectRef, _ FetchHint) ([]byte, error) {
	return b.fetch(ref)
}

func TestFlushTxDefersNackIngestionUntilFanoutCompletes(t *testing.T) {
	e := newTxnTestEngine(nil)
	preSubscribe(e, "k")

	seed := e.NewContext()
	if err := seed.Emit(scalarSet("k", "base")); err != nil {
		t.Fatal(err)
	}

	if err := seed.Flush(); err != nil {
		t.Fatal(err)
	}

	for _, peer := range []pb.NodeID{99, 100} {
		e.addPeerSubscriber(logicalAddress("k"), peer)
	}

	var fetches atomic.Int32

	received, release := make(chan struct{}), make(chan struct{})
	b := &batchReplyBroadcaster{peerIDs: []pb.NodeID{99, 100}, allRegionPeersReachable: true}
	b.fetch = func(ref *pb.EffectRef) ([]byte, error) {
		fetches.Add(1)

		return wireEffect(frontierEffect(ref.GetOffset())), nil
	}
	b.reply = func(n *pb.OffsetNotify, target pb.NodeID) ([]*pb.NackNotify, error) {
		if target == 100 {
			<-release

			return nil, nil
		}

		nack := &pb.NackNotify{Key: []byte("k"), Conflicting: n.GetOrigin(), TipDetails: []*pb.NackTipDetail{{Ref: toPbRef(Tip{7, 1}), IsData: true}}}
		if err := e.HandleNack(nack); err != nil {
			return nil, err
		}

		close(received)

		return []*pb.NackNotify{nack, nack}, nil
	}
	e.broadcaster = b
	c := e.NewContext()
	c.BeginTx()

	if err := c.Emit(scalarSet("k", "local")); err != nil {
		t.Fatal(err)
	}

	result := make(chan error, 1)
	go func() { result <- c.Flush() }()

	select {
	case <-received:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("first peer did not reply")
	}

	if got := fetches.Load(); got != 0 {
		t.Errorf("ingested history before the final peer replied: %d fetches", got)
	}

	close(release)

	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("commit did not finish")
	}

	if got := fetches.Load(); got != 1 {
		t.Fatalf("duplicate async/tracked replies caused %d fetches, want 1", got)
	}

	if err := e.HandleNack(&pb.NackNotify{Key: []byte("k"), TipDetails: []*pb.NackTipDetail{{Ref: toPbRef(Tip{7, 2})}}}); err != nil {
		t.Fatal(err)
	}

	if got := fetches.Load(); got != 2 {
		t.Fatalf("late reply was not ingested: %d fetches", got)
	}
}

func TestNackBatchDoesNotRepeatMissingAncestry(t *testing.T) {
	counts := make([]int, 0, 2)

	for _, copies := range []int{1, 4} {
		e := newSnapshotEngine(nil)
		preSubscribe(e, "k")

		calls := 0
		b := &batchReplyBroadcaster{}
		b.fetch = func(ref *pb.EffectRef) ([]byte, error) {
			calls++

			if ref.GetOffset() == 2 {
				return wireEffect(frontierEffect(2, 1)), nil
			}

			return nil, ErrEffectNotFound
		}
		e.broadcaster = b
		nack := &pb.NackNotify{Key: []byte("k"), TipDetails: []*pb.NackTipDetail{{Ref: toPbRef(Tip{7, 2})}}}

		nacks := make([]*pb.NackNotify, copies)
		for i := range nacks {
			nacks[i] = nack
		}

		_, _ = e.ingestNackBatch(nacks)

		counts = append(counts, calls)

		if !e.index.Contains("k").Contains(Tip{7, 2}) {
			t.Fatal("missing ancestry erased the advertised frontier")
		}
	}

	if counts[0] == 0 || counts[0] != counts[1] {
		t.Fatalf("duplicate NACKs repeated missing-ancestry work: %v", counts)
	}
}

func TestNackCandidatesDeduplicateAcrossPeerWrappers(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)
	bind := closureBind(log, "competitor", 2, []string{"k"}, nil)
	eff, _ := e.effectCache.Get(bind)
	ptxn := &pendingTxn{txnID: "local", forkChoiceHash: []byte{0}, keys: []pendingTxnKey{{key: logicalAddress("k")}}}

	nacks := make([]*pb.NackNotify, 0, 5)

	for range 4 {
		wrapper := log.putEffect(&pb.Effect{Key: []byte("k"), Deps: toPbRefs([]Tip{bind}), Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}}})
		wrapperEff, _ := e.effectCache.Get(wrapper)
		nacks = append(nacks, &pb.NackNotify{Key: []byte("k"), TipDetails: []*pb.NackTipDetail{nackTipDetail(wrapper, wrapperEff)}})
	}

	nacks = append(nacks, &pb.NackNotify{Key: []byte("k"), TipDetails: []*pb.NackTipDetail{nackTipDetail(bind, eff)}})
	groups := e.groupNacks(nacks)

	candidates, _, err := e.nackCandidates(ptxn, groups[0].key, groups[0].details)
	if err != nil || len(candidates) != 1 || r(candidates[0].GetRef()) != bind {
		t.Fatalf("duplicate candidates: %v, error=%v", candidates, err)
	}

	for _, responses := range [][]*pb.NackNotify{nacks[:4], nacks, slices.Concat(nacks, nacks)} {
		defeated, err := e.evaluateNacks(ptxn, responses)
		if err != nil || len(defeated) != 1 || defeated["competitor"] != bind {
			t.Fatalf("duplicate responses changed evaluation: %v, %v", defeated, err)
		}
	}
}

func TestDeferredNackHandoffDoesNotLoseLateReplies(t *testing.T) {
	p := &pendingTxn{}

	first, late := &pb.NackNotify{Key: []byte("first")}, &pb.NackNotify{Key: []byte("late")}
	if !p.deferNack(first) {
		t.Fatal("early reply was not queued")
	}

	if got := p.takeDeferredNacks(); len(got) != 1 || got[0] != first {
		t.Fatalf("queued replies = %v", got)
	}

	if p.deferNack(late) || len(p.takeDeferredNacks()) != 0 {
		t.Fatal("late reply disappeared into a drained batch")
	}
}

func TestNackBatchDoesNotTurnMissingPriorityEvidenceIntoAbort(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)
	bind := closureBind(log, "competitor", 1, []string{"k"}, nil)
	eff, _ := e.effectCache.Get(bind)
	nack := &pb.NackNotify{Key: []byte("k"), TipDetails: []*pb.NackTipDetail{nackTipDetail(bind, eff)}}
	// Extending the actual advertised hash gives us strictly lower priority.
	ptxn := &pendingTxn{txnID: "local", forkChoiceHash: append(slices.Clone(nack.GetTipDetails()[0].GetBindForkChoiceHash()), 0), keys: []pendingTxnKey{{key: logicalAddress("k")}}}
	if _, err := e.evaluateNacks(ptxn, []*pb.NackNotify{nack, nack, nack, nack}); !errors.Is(err, ErrTxnAmbiguous) {
		t.Fatalf("unresolved higher-priority bind = %v, want ambiguous", err)
	}
}
