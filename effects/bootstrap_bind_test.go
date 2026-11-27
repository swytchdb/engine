package effects

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"google.golang.org/protobuf/proto"
)

func TestBootstrapCompletedBindDoesNotRecoverOtherKeys(t *testing.T) {
	for _, version := range []string{"dev", "1.7.9"} {
		for _, completion := range []string{"closed", "outstanding", "unknown"} {
			t.Run(version+"/"+completion, func(t *testing.T) {
				source := newTxnTestEngine(nil)
				defer func() { _ = source.Close() }()

				source.buildVersion = version

				c := source.NewContext()
				c.BeginTx()

				for _, key := range []string{"wanted", "other"} {
					if err := c.Emit(scalarSet(key, "value")); err != nil {
						t.Fatal(err)
					}
				}

				if err := c.Flush(); err != nil {
					t.Fatal(err)
				}

				owner := source.index.Contains("wanted").Tips()[0]
				verdict, _ := source.effectCache.Get(owner)
				verdict = proto.Clone(verdict).(*pb.Effect)

				switch completion {
				case "outstanding":
					verdict.GetSnapshot().OutstandingSubscribers = &pb.SubscriberSet{NodeIds: []uint64{3}}
				case "unknown":
					verdict.GetSnapshot().OutstandingSubscribers = nil
				}

				unavailable := Tip{123, 456}
				otherLoads := 0
				transport := errors.New("other key history unavailable")
				bc := &cutBroadcaster{
					peerIDs: []pb.NodeID{source.nodeID}, allRegionPeersReachable: true,
					nackResponses: map[pb.NodeID]*pb.NackNotify{source.nodeID: {Key: []byte("wanted"), Tips: toPbRefs([]Tip{owner})}}}
				bc.load = func(tip Tip) ([]byte, error) {
					if tip == unavailable {
						otherLoads++

						return nil, transport
					}

					eff, ok := source.effectCache.Get(tip)
					if !ok {
						return nil, ErrEffectNotFound
					}

					if tip == owner {
						eff = verdict
					}

					data, err := source.effectCache.MarshalEffect(eff)
					if err != nil {
						return nil, err
					}

					wire := binary.LittleEndian.AppendUint32(nil, uint32(len(eff.GetKey())))
					wire = append(wire, eff.GetKey()...)

					return append(wire, data...), nil
				}

				receiver := newTxnTestEngine(bc)
				defer func() { _ = receiver.Close() }()

				receiver.nodeID = source.nodeID + 1
				preSubscribe(receiver, "other")
				receiver.updateIndex(logicalAddress("other"), nil, unavailable)

				if completion != "closed" {
					// Use the bootstrap ingestion step directly so an expected
					// failure does not start the background bootstrap retry timer.
					preSubscribe(receiver, "wanted")

					installed, _ := receiver.walkAndInstall(logicalAddress("wanted"), []Tip{owner})
					if installed != 0 || otherLoads == 0 {
						t.Fatalf("open competition bypassed other history: installed=%d loads=%d", installed, otherLoads)
					}

					return
				}

				if err := receiver.ensureSubscribedBlocking(logicalAddress("wanted")); err != nil {
					t.Fatal(err)
				}

				value, _, _, err := receiver.GetSnapshot("wanted")
				if err != nil || string(value.GetScalar().GetRaw()) != "value" {
					t.Fatalf("bootstrap value=%v error=%v", value, err)
				}

				if otherLoads != 0 {
					t.Fatalf("completed bind reopened other key %d times", otherLoads)
				}

				requireCutTips(t, receiver.index.Contains("other").Tips(), []Tip{unavailable})
			})
		}
	}
}

func completedClosureFixture(tb testing.TB, width int) (*Engine, string, Tip) {
	tb.Helper()

	keys := make([]string, width)
	for i := range keys {
		keys[i] = fmt.Sprintf("%s/key-%04d", strings.Repeat("k", 1000), i)
	}

	e, binds, _ := backfillHistory(1, keys)

	tb.Cleanup(func() { _ = e.Close() })

	owner := Tip{99, 1}
	key := keys[len(keys)-1] // exercise lookup at the end of a wide bind

	eff := &pb.Effect{
		Key: []byte(key), Deps: toPbRefs(binds),
		Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
			TxnVerdicts: map[string]pb.Verdict{"txn-0": pb.Verdict_WON}, OutstandingSubscribers: &pb.SubscriberSet{},
		}},
	}
	if err := e.effectCache.Put(owner, eff); err != nil {
		tb.Fatal(err)
	}

	return e, logicalAddress(key), owner
}

func TestCompletedClosureWorkIsIndependentOfBindWidth(t *testing.T) {
	for _, width := range []int{16, 256, 1024} {
		t.Run(fmt.Sprint(width), func(t *testing.T) {
			e, key, owner := completedClosureFixture(t, width)

			op := newClosureOperation(e, make(map[Tip]error))
			defer op.release()

			vertices, edges := countClosureWork(op)

			_, complete, err := op.run(key, []Tip{owner}, false, false)
			if err != nil || !complete {
				t.Fatalf("closure complete=%t error=%v", complete, err)
			}

			if len(op.work) != 1 || len(op.binds) != 0 || len(vertices) != 3 || len(edges) != 2 {
				t.Fatalf("width=%d keys=%d projection tables=%d vertices=%d edges=%d", width, len(op.work), len(op.binds), len(vertices), len(edges))
			}
		})
	}
}

func TestBootstrapFindsCompletionAfterBind(t *testing.T) {
	e, key, owner := completedClosureFixture(t, 16)
	snapshot, _ := e.effectCache.Get(owner)
	bind := r(snapshot.GetDeps()[0])
	// Make the verdict arrive through a different, initially missing path.
	// Seeing the resident bind first must not open its other key histories.
	e.effectCache.decrefChainNode(key, owner)
	e.effectCache.drainReclaim()
	e.broadcaster = &cutBroadcaster{load: func(tip Tip) ([]byte, error) {
		if tip != owner {
			t.Fatalf("unexpected fetch %v", tip)
		}

		return wireEffect(snapshot), nil
	}}

	gap := Tip{98, 1}
	if err := e.effectCache.Put(gap, &pb.Effect{
		Key: []byte(wireKey(key)), Deps: toPbRefs([]Tip{owner}),
		Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
	}); err != nil {
		t.Fatal(err)
	}

	op := newClosureOperation(e, make(map[Tip]error))
	defer op.release()

	_, complete, err := op.run(key, []Tip{bind, gap}, false, false)
	if err != nil || !complete || len(op.work) != 1 || len(op.binds) != 0 {
		t.Fatalf("late verdict: complete=%t error=%v keys=%d projection tables=%d", complete, err, len(op.work), len(op.binds))
	}
}

func BenchmarkCompletedBindBootstrapClosure(b *testing.B) {
	for _, width := range []int{16, 256, 1024} {
		b.Run(fmt.Sprint(width), func(b *testing.B) {
			e, key, owner := completedClosureFixture(b, width)
			b.ReportAllocs()

			for b.Loop() {
				if _, _, err := e.closeIngestedFrontier(key, []Tip{owner}, make(map[Tip]error)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
