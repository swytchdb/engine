package effects

import (
	"slices"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

// closureBind builds a transaction with independent per-key ancestry. Keeping
// it out of the index lets tests reproduce asymmetric history discovery.
func closureBind(log *snapshotLog, txn string, hash byte, keys []string, parents map[string]Tip) Tip {
	binds := make([]*pb.TransactionalBindEffect_KeyBind, 0, len(keys))
	deps := make([]*pb.EffectRef, 0, len(keys))

	for _, key := range keys {
		var consumed []*pb.EffectRef
		if parent, ok := parents[key]; ok {
			consumed = []*pb.EffectRef{toPbRef(parent)}
		}

		value := txn
		if key != "winner" {
			value = "100"
		}

		data := log.putEffect(&pb.Effect{
			Key: []byte(key), NodeId: log.nodeID, Hlc: sTs(int64(log.nextOff)), TxnId: txn,
			Deps: consumed, Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte(value))},
		})
		binds = append(binds, &pb.TransactionalBindEffect_KeyBind{
			Key: []byte(key), NewTip: toPbRef(data), ConsumedTips: consumed,
		})
		deps = append(deps, toPbRef(data))
	}

	return log.putEffect(&pb.Effect{
		Key: []byte(keys[0]), NodeId: log.nodeID, Hlc: sTs(int64(log.nextOff)), TxnId: txn,
		ForkChoiceHash: []byte{hash}, Deps: deps,
		Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
			Keys: binds, OriginatorNodeId: log.nodeID, TxnHlc: sTs(int64(log.nextOff)),
		}},
	})
}

func TestBindKeyClosure_CrossKeyCandidate(t *testing.T) {
	for _, winner := range []string{"alice", "bob"} {
		t.Run(winner, func(t *testing.T) {
			log := newSnapshotLog()
			e := newSnapshotEngine(log)

			aliceHash, bobHash := byte(1), byte(2)
			if winner == "bob" {
				aliceHash, bobHash = bobHash, aliceHash
			}

			alice := closureBind(log, "alice", aliceHash, []string{"winner", "payout:alice"}, nil)
			bob := closureBind(log, "bob", bobHash, []string{"winner", "payout:bob"}, nil)

			e.updateIndex("winner", nil, alice)
			e.updateIndex("payout:alice", nil, alice)
			e.updateIndex("payout:bob", nil, bob)

			_, anchors, verdicts, err := e.bindKeyClosure("payout:bob")
			if err != nil {
				t.Fatal(err)
			}

			losers, _ := e.losersOnKey("winner", anchors["winner"], verdicts)

			loser := "bob"
			if winner == "bob" {
				loser = "alice"
			}

			if _, ok := losers[loser]; !ok || len(losers) != 1 {
				t.Fatalf("losers = %v; want only %s", losers, loser)
			}
		})
	}
}

func TestBindKeyClosure_RevisitsKeyForNewAnchor(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)
	old := closureBind(log, "old", 1, []string{"a", "c"}, nil)
	late := closureBind(log, "late", 2, []string{"b", "a"}, map[string]Tip{"a": old})
	root := closureBind(log, "root", 3, []string{"a", "b"}, nil)
	e.updateIndex("a", nil, root)
	e.updateIndex("b", nil, late)

	// a is visited first, then b reveals another a branch carrying (a,c).
	// That branch is absent from a's index and b's same-key ancestry.
	keys, anchors, _, err := e.bindKeyClosure("a")
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := keys["c"]; !ok || !slices.Contains(anchors["c"], old) {
		t.Fatalf("late branch not explored: keys=%v anchors=%v", keys, anchors)
	}
}
