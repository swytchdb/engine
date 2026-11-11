package effects

import (
	"errors"
	"testing"
	"testing/synctest"

	pb "github.com/swytchdb/engine/cluster/proto"
)

type predicatePriorityBroadcaster struct {
	bindPublicationBroadcaster

	sent      chan *pb.OffsetNotify
	allow     chan bool
	responses []*pb.NackNotify
}

func (b *predicatePriorityBroadcaster) ReplicateMarshalled(n *pb.OffsetNotify, _ []byte, _ pb.NodeID) ([]*pb.NackNotify, error) {
	b.sent <- n

	if !<-b.allow {
		return nil, errors.New("injected bind delivery failure")
	}

	ns, err := b.peer.HandleRemote(n)
	b.responses = ns

	return ns, err
}

// Two real Flush calls publish binds before either request is delivered.
// The stronger transaction's request fails in transport; the weaker one's
// request is delivered afterwards and receives the unresolved stronger bind.
func TestPredicateDisjointBindDoesNotRequirePriorityVerdict(t *testing.T) {
	for _, mode := range []string{"disjoint", "overlap", "missing-evidence", "other-key-overlap"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var (
					es    [2]*Engine
					links [2]*predicatePriorityBroadcaster
					tx    [2]*Context
					done  [2]chan error
				)

				for i := range es {
					links[i] = &predicatePriorityBroadcaster{sent: make(chan *pb.OffsetNotify, 1), allow: make(chan bool, 1)}
					links[i].peerIDs = []pb.NodeID{pb.NodeID(2 - i)}
					links[i].allRegionPeersReachable = true
					es[i] = newTxnTestEngine(links[i])
					es[i].nodeID = pb.NodeID(i + 1)
					es[i].UpdateSafetyRules(SafeMode, nil)

					for j, key := range []string{"k", "other"} {
						base := Tip{99, uint64(j + 1)}
						if err := es[i].effectCache.Put(base, scalarSet(key, "base")); err != nil {
							t.Fatal(err)
						}

						es[i].updateIndex(key, nil, base)
						preSubscribe(es[i], key)
						es[i].addPeerSubscriber(key, pb.NodeID(2-i))
					}

					tx[i] = es[i].NewContext()
					tx[i].BeginTx()
					done[i] = make(chan error, 1)
				}

				links[0].peer = es[1]
				links[1].peer = es[0]

				keys := []string{"k"}
				if mode == "other-key-overlap" {
					keys = append(keys, "other")
				}

				for i, c := range tx {
					for _, key := range keys {
						if mode == "missing-evidence" && i == 0 {
							if err := c.Emit(scalarSet(key, "plain")); err != nil {
								t.Fatal(err)
							}

							continue
						}

						row := int64(i + 1)
						if mode == "overlap" || key == "other" {
							row = 1
						}

						if err := c.Emit(&pb.Effect{Key: []byte(key), Kind: &pb.Effect_Observation{Observation: &pb.ObservationEffect{
							Predicate: &pb.Predicate{Kind: pb.Predicate_CMP, Col: 0, Op: pb.PredicateCmpOp_CMP_EQ, Literal: &pb.TypedValue{Kind: pb.TypedValue_INT, IntVal: row}},
						}}}); err != nil {
							t.Fatal(err)
						}

						if err := c.Emit(&pb.Effect{Key: []byte(key), Kind: &pb.Effect_RowWrite{RowWrite: &pb.RowWriteEffect{
							Kind: pb.RowWriteEffect_INSERT, Columns: []*pb.TypedValue{{Kind: pb.TypedValue_INT, IntVal: row}},
						}}}); err != nil {
							t.Fatal(err)
						}
					}
				}

				for i := range tx {
					go func() { done[i] <- tx[i].Flush() }()
				}

				ns := [2]*pb.OffsetNotify{<-links[0].sent, <-links[1].sent}

				var binds [2]*pb.Effect
				for i, n := range ns {
					binds[i], _ = es[i].effectCache.Get(r(n.GetOrigin()))
				}

				strong := 0
				if ForkChoiceLess(binds[1].GetForkChoiceHash(), binds[0].GetForkChoiceHash()) {
					strong = 1
				}

				weak := 1 - strong
				links[strong].allow <- false

				if err := <-done[strong]; !errors.Is(err, ErrTxnAmbiguous) {
					t.Fatalf("transport failure should be ambiguous: %v", err)
				}

				links[weak].allow <- true

				result := <-done[weak]
				if mode == "disjoint" {
					if result != nil {
						t.Fatalf("disjoint transaction required unrelated priority verdict: %v", result)
					}
				} else if !errors.Is(result, ErrTxnAmbiguous) {
					t.Fatalf("unresolved overlapping or unknown competitor: got %v, want ambiguous", result)
				}

				if len(links[weak].responses) == 0 {
					t.Fatal("no real NACK received")
				}

				ours := &pendingTxn{txnID: binds[weak].GetTxnId(), bindOffset: r(ns[weak].GetOrigin()), forkChoiceHash: binds[weak].GetForkChoiceHash()}
				for _, kb := range binds[weak].GetTxnBind().GetKeys() {
					ours.keys = append(ours.keys, pendingTxnKey{key: string(kb.GetKey()), newTip: r(kb.GetNewTip()), consumedTips: fromPbRefs(kb.GetConsumedTips())})
				}

				conflict, known := es[weak].hasPredicateConflict(ours.txnID, binds[strong].GetTxnId(), "k", collectOurBindTips(ours, "k"), []Tip{r(ns[strong].GetOrigin())})
				if (mode == "disjoint" || mode == "other-key-overlap") && (conflict || !known) {
					t.Fatalf("disjoint fixture failed: conflict=%t evidence=%t", conflict, known)
				}

				if mode == "overlap" && (!conflict || !known) {
					t.Fatal("overlap fixture failed")
				}

				if mode == "missing-evidence" && known {
					t.Fatal("missing evidence fixture failed")
				}
			})
		})
	}
}
