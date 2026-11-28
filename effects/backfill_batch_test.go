package effects

import (
	"errors"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
)

func TestBackfillBatchDefersAllKeysAndSharesClosure(t *testing.T) {
	keys := []string{"a", "b", "c"}

	e, binds, records := backfillHistory(16, keys)
	defer func() { _ = e.Close() }()

	latest := binds[len(binds)-1]

	frontiers := make(ingestedFrontiers)
	for _, key := range keys {
		_ = e.ingestNackKey(keyNacks{key: logicalAddress(key), wireKey: []byte(key), details: []*pb.NackTipDetail{{Ref: toPbRef(latest)}}}, frontiers, nil)
	}

	for _, key := range keys {
		if e.index.Contains(key).Len() != 0 {
			t.Fatalf("replay published %s before the batch completed", key)
		}
	}

	op := newClosureOperation(e, make(map[Tip]error))
	defer op.release()

	vertices, edges := countClosureWork(op)
	publications := make(map[string]int)
	op.cas = func(key string, old, next *keytrie.TipSet) (*keytrie.TipSet, bool) {
		publications[key]++

		return e.index.InsertInternal(key, old, next)
	}
	op.publishBatch(frontiers)

	for _, key := range keys {
		requireCutTips(t, e.index.Contains(key).Tips(), []Tip{latest})

		if publications[logicalAddress(key)] != 1 {
			t.Fatalf("%s published %d times", key, publications[logicalAddress(key)])
		}
	}

	for tip, eff := range records {
		want := 1
		if eff.GetTxnBind() != nil {
			want = len(keys) // each key has its own dependency projection
		}

		if vertices[tip] != want {
			t.Errorf("%v expanded %d times, want %d", tip, vertices[tip], want)
		}

		for _, dep := range eff.GetDeps() {
			if edges[[2]Tip{tip, r(dep)}] != 1 {
				t.Errorf("edge %v -> %v traversed %d times", tip, r(dep), edges[[2]Tip{tip, r(dep)}])
			}
		}
	}
}

func TestNackBatchPublishesSiblingKeysAfterAllGroups(t *testing.T) {
	e, binds, _ := backfillHistory(4, []string{"a", "b", "unserved"})
	defer func() { _ = e.Close() }()

	e.subscriptions.Delete(logicalAddress("unserved"))
	preSubscribe(e, "last")

	latest := binds[len(binds)-1]
	missing := Tip{99, 1}
	fetched := false
	e.broadcaster = &cutBroadcaster{load: func(tip Tip) ([]byte, error) {
		if tip != missing {
			t.Fatalf("unexpected fetch %v", tip)
		}

		fetched = true

		for _, key := range []string{"a", "b"} {
			if e.index.Contains(key).Len() != 0 {
				t.Fatalf("%s published before the last NACK group was ingested", key)
			}
		}

		return nil, ErrEffectNotFound
	}}
	_, _ = e.ingestNackBatch([]*pb.NackNotify{
		{Key: []byte("a"), TipDetails: []*pb.NackTipDetail{{Ref: toPbRef(latest)}}},
		{Key: []byte("last"), TipDetails: []*pb.NackTipDetail{{Ref: toPbRef(missing)}}},
	})

	if !fetched {
		t.Fatal("last group was not ingested")
	}

	for _, key := range []string{"a", "b"} {
		requireCutTips(t, e.index.Contains(key).Tips(), []Tip{latest})
	}

	if e.index.Contains("unserved").Len() != 0 {
		t.Fatal("replayed bind published an unsubscribed key")
	}
}

func TestBackfillBatchIncludesSiblingPublicationAndCASRetry(t *testing.T) {
	e, binds, _ := backfillHistory(4, []string{"a", "b"})
	defer func() { _ = e.Close() }()

	latest, concurrent := binds[len(binds)-1], Tip{9, 1}
	if err := e.effectCache.Put(concurrent, scalarSet("b", "concurrent")); err != nil {
		t.Fatal(err)
	}

	frontiers := make(ingestedFrontiers)
	_ = e.ingestNackKey(keyNacks{key: logicalAddress("a"), wireKey: []byte("a"), details: []*pb.NackTipDetail{{Ref: toPbRef(latest)}}}, frontiers, nil)

	op := newClosureOperation(e, make(map[Tip]error))
	defer op.release()

	vertices, _ := countClosureWork(op)
	attempts := make(map[string]int)
	op.cas = func(key string, old, next *keytrie.TipSet) (*keytrie.TipSet, bool) {
		attempts[key]++
		if key == logicalAddress("a") {
			// The sibling was already explored during a's closure. Its new
			// frontier must still be incorporated when b is published.
			e.updateIndex(logicalAddress("b"), nil, concurrent)
		} else if attempts[key] == 1 {
			if _, ok := e.index.InsertInternal(key, old, keytrie.NewTipSet(old.Tips()...)); !ok {
				t.Fatal("could not force CAS retry")
			}
		}

		return e.index.InsertInternal(key, old, next)
	}
	op.publishBatch(frontiers)
	requireCutTips(t, e.index.Contains("a").Tips(), []Tip{latest})
	requireCutTips(t, e.index.Contains("b").Tips(), []Tip{latest, concurrent})

	if attempts[logicalAddress("b")] != 2 || vertices[latest] != 2 || vertices[concurrent] != 1 {
		t.Fatalf("retry repeated closure or missed new history: attempts=%v vertices=%v", attempts, vertices)
	}
}

func TestBackfillBatchRetriesMissingSiblingHistoryNextBatch(t *testing.T) {
	e := newSnapshotEngine(nil)
	defer func() { _ = e.Close() }()

	preSubscribe(e, "a", "b")

	bind, data, missing, old := Tip{7, 1}, Tip{7, 2}, Tip{8, 1}, Tip{9, 1}
	putCutEffects(t, e, map[Tip]*pb.Effect{
		data: scalarSet("a", "value"), old: scalarSet("b", "old"),
		bind: {Key: []byte("a"), Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
			Keys: []*pb.TransactionalBindEffect_KeyBind{
				{Key: []byte("a"), NewTip: toPbRef(data)},
				{Key: []byte("b"), NewTip: toPbRef(missing)},
			},
		}}},
	})
	e.updateIndex(logicalAddress("b"), nil, old)

	failure, loads := errors.New("peer unavailable"), 0
	e.broadcaster = &cutBroadcaster{load: func(tip Tip) ([]byte, error) {
		loads++

		if tip != missing {
			t.Fatalf("unexpected fetch %v", tip)
		}

		return nil, failure
	}}
	frontiers := make(ingestedFrontiers)
	frontiers.add(logicalAddress("a"), bind)
	frontiers.add(logicalAddress("b"), bind)

	for batch := 1; batch <= 2; batch++ {
		e.installIngestedFrontiers(frontiers, nil)

		if loads != batch {
			t.Fatalf("missing history fetched %d times after %d batches", loads, batch)
		}

		requireCutTips(t, e.index.Contains("b").Tips(), []Tip{bind, old})
	}

	bridge := scalarSet("b", "bridge")

	bridge.Deps = toPbRefs([]Tip{old})
	if err := e.effectCache.Put(missing, bridge); err != nil {
		t.Fatal(err)
	}

	e.installIngestedFrontiers(frontiers, nil)
	requireCutTips(t, e.index.Contains("b").Tips(), []Tip{bind})
}
