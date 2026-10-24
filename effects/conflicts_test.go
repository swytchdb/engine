package effects

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func conflictLottery(t *testing.T, names ...string) (*Engine, *snapshotLog, map[string]Tip) {
	t.Helper()

	log := newSnapshotLog()
	e := newSnapshotEngine(log)
	binds := make(map[string]Tip)

	for i, name := range names {
		bind := addConflictLottery(e, log, name, byte(i+1), nil)
		binds[name] = bind
	}

	return e, log, binds
}

func addConflictLottery(e *Engine, log *snapshotLog, name string, hash byte, parents map[string]Tip) Tip {
	bind := closureBind(log, name, hash, []string{"winner", "payout:" + name}, parents)
	for _, key := range []string{"winner", "payout:" + name} {
		snapshot := log.putEffect(&pb.Effect{
			Key: []byte(key), NodeId: 1, Hlc: sTs(int64(log.nextOff)),
			Deps: []*pb.EffectRef{toPbRef(bind)},
			Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
				TxnVerdicts: map[string]pb.Verdict{name: pb.Verdict_WON},
			}},
		})
		e.updateIndex(key, nil, snapshot)
	}

	return bind
}

func conflictDocument(t *testing.T, e *Engine, txn string) []byte {
	t.Helper()

	value, _, _, err := e.GetSnapshot(conflictDocumentPrefix + txn)
	if err != nil {
		t.Fatal(err)
	}

	if value.GetTypeTag() != pb.ValueType_TYPE_JSON || value.GetScalar() == nil {
		t.Fatalf("missing JSON transaction document for %s: %v", txn, value)
	}

	return value.GetScalar().GetRaw()
}

func conflictList(t *testing.T, e *Engine, key string) []string {
	t.Helper()

	value, _, _, err := e.GetSnapshot(key)
	if err != nil {
		t.Fatal(err)
	}

	result := make([]string, 0, len(value.GetOrderedElements()))
	for _, element := range value.GetOrderedElements() {
		result = append(result, string(element.GetData().GetRaw()))
	}

	return result
}

func TestConflictEffects_EmittedAndDocumentGate(t *testing.T) {
	e, _, _ := conflictLottery(t, "alice", "bob")

	var emitted []*pb.Effect

	e.SetOnLocalEffect(func(_ Tip, eff *pb.Effect) {
		if strings.HasPrefix(string(eff.GetKey()), "__swytch::conflict") && eff.GetData() != nil {
			emitted = append(emitted, eff)
		}
	})

	if _, _, _, err := e.GetSnapshot("winner"); err != nil {
		t.Fatal(err)
	}

	if len(emitted) == 0 {
		t.Fatal("competing WON tips emitted no conflict effects")
	}

	for key, count := range map[string]int{
		"__swytch::conflict::winner":       2,
		"__swytch::conflict::payout:alice": 1,
		"__swytch::conflict::payout:bob":   1,
		"__swytch::conflict::__swytch":     3,
	} {
		if got := conflictList(t, e, key); len(got) != count {
			t.Fatalf("%s = %v; want %d entries", key, got, count)
		}
	}

	for _, txn := range []string{"alice", "bob"} {
		doc := conflictDocument(t, e, txn)

		var report map[string]json.RawMessage
		if err := json.Unmarshal(doc, &report); err != nil {
			t.Fatal(err)
		}

		if string(report["txid"]) != `"`+txn+`"` || report["keys"] == nil || report["competing_txids"] != nil || report["transactions"] != nil {
			t.Fatalf("expected evidence for only %s: %s", txn, doc)
		}
	}

	before := len(emitted)

	if _, _, _, err := e.GetSnapshot("winner"); err != nil {
		t.Fatal(err)
	}

	if len(emitted) != before {
		t.Fatal("existing documents did not suppress rediscovery")
	}

	for _, eff := range emitted {
		if eff.GetTxnId() != "" || eff.GetTxnBind() != nil {
			t.Fatal("reporting must emit ordinary nontransactional effects")
		}
	}
}

func TestConflictEffects_NewCompetitorPreservesDocuments(t *testing.T) {
	e, log, binds := conflictLottery(t, "alice", "bob")
	if err := e.emitConflict("winner", binds["alice"], binds["bob"]); err != nil {
		t.Fatal(err)
	}

	original := string(conflictDocument(t, e, "alice"))

	var rewritten bool

	e.SetOnLocalEffect(func(_ Tip, eff *pb.Effect) {
		if string(eff.GetKey()) == conflictDocumentPrefix+"alice" && eff.GetData() != nil {
			rewritten = true
		}
	})

	charlie := addConflictLottery(e, log, "charlie", 3, nil)
	if err := e.emitConflict("winner", binds["alice"], charlie); err != nil {
		t.Fatal(err)
	}

	if rewritten || string(conflictDocument(t, e, "alice")) != original {
		t.Fatal("new competitor rewrote Alice's immutable document")
	}

	conflictDocument(t, e, "charlie")

	if got := conflictList(t, e, conflictIndexPrefix+"winner"); !slices.Equal(got, []string{"alice", "bob", "alice", "charlie"}) {
		t.Fatalf("notifications = %v", got)
	}
	// Bob and Charlie were never reported together. Existence alone gates
	// reporting; documents need no knowledge of their competitors.
	if err := e.emitConflict("winner", binds["bob"], charlie); err != nil {
		t.Fatal(err)
	}

	if got := conflictList(t, e, conflictIndexPrefix+"winner"); len(got) != 4 {
		t.Fatalf("existing documents did not gate new pair: %v", got)
	}
}

func TestConflictEffects_ConcurrentReportsPreserveDuplicates(t *testing.T) {
	e, _, binds := conflictLottery(t, "alice", "bob")
	ready := make(chan struct{}, 2)
	release := make(chan struct{})
	done := make(chan error, 2)

	e.SetOnLocalEffect(func(_ Tip, eff *pb.Effect) {
		if string(eff.GetKey()) == conflictDocumentPrefix+"alice" && eff.GetData() != nil {
			ready <- struct{}{}

			<-release
		}
	})

	for range 2 {
		go func() { done <- e.emitConflict("winner", binds["alice"], binds["bob"]) }()
	}

	for range 2 {
		select {
		case <-ready:
		case <-time.After(5 * time.Second):
			close(release)
			t.Fatal("both reporters must pass the document gate before either flushes")
		}
	}

	close(release)

	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}

	got := conflictList(t, e, conflictIndexPrefix+"winner")
	slices.Sort(got)

	if !slices.Equal(got, []string{"alice", "alice", "bob", "bob"}) {
		t.Fatalf("concurrent notification duplicates were lost: %v", got)
	}

	conflictDocument(t, e, "alice")
	conflictDocument(t, e, "bob")
}

func TestConflictEffects_HistoricalEvidence(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)
	base := log.putEffect(&pb.Effect{
		Key: []byte("winner"), NodeId: 1, Hlc: sTs(1),
		Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("undecided"))},
	})
	parents := map[string]Tip{"winner": base}
	alice := addConflictLottery(e, log, "alice", 1, parents)
	bob := addConflictLottery(e, log, "bob", 2, parents)

	tips := append([]Tip(nil), e.index.Contains("winner").Tips()...)
	if err := e.emitConflict("winner", alice, bob); err != nil {
		t.Fatal(err)
	}

	if !slices.Equal(tips, e.index.Contains("winner").Tips()) {
		t.Fatal("historical extraction changed application tips")
	}

	for _, txn := range []string{"alice", "bob"} {
		var report conflictTransaction
		if err := json.Unmarshal(conflictDocument(t, e, txn), &report); err != nil {
			t.Fatal(err)
		}

		if len(report.Keys) != 2 {
			t.Fatalf("missing bound keys: %+v", report)
		}

		for _, key := range report.Keys {
			var before, after pb.ReducedEffect
			if string(key.Before) != "null" {
				if err := protojson.Unmarshal(key.Before, &before); err != nil {
					t.Fatal(err)
				}
			}

			if err := protojson.Unmarshal(key.After, &after); err != nil {
				t.Fatal(err)
			}

			wantBefore, wantAfter := "", "100"
			if string(key.Key) == "winner" {
				wantBefore, wantAfter = "undecided", txn
			}

			if string(before.GetScalar().GetRaw()) != wantBefore || string(after.GetScalar().GetRaw()) != wantAfter {
				t.Fatalf("%s %s: before=%v after=%v", txn, key.Key, &before, &after)
			}

			if len(key.Effects) != 1 {
				t.Fatalf("missing operations: %s", key.Effects)
			}

			var operation pb.Effect
			if err := protojson.Unmarshal(key.Effects[0], &operation); err != nil {
				t.Fatal(err)
			}

			if operation.GetTxnId() != txn || string(operation.GetData().GetRaw()) != wantAfter {
				t.Fatalf("wrong operation: %v", &operation)
			}
		}
	}
}

func TestConflictEffects_RequiresCompetingWonTips(t *testing.T) {
	for _, winners := range []int{0, 1} {
		t.Run(string(rune('0'+winners)), func(t *testing.T) {
			log := newSnapshotLog()
			e := newSnapshotEngine(log)

			for i, txn := range []string{"alice", "bob"} {
				if i < winners {
					addConflictLottery(e, log, txn, byte(i+1), nil)
				} else {
					bind := closureBind(log, txn, byte(i+1), []string{"winner"}, nil)
					e.updateIndex("winner", nil, bind)
				}
			}

			if _, _, _, err := e.GetSnapshot("winner"); err != nil {
				t.Fatal(err)
			}

			if e.index.Contains(conflictDocumentPrefix+"alice") != nil || e.index.Contains(conflictDocumentPrefix+"bob") != nil {
				t.Fatal("reported without two competing WON tips")
			}
		})
	}
}

func TestConflictEffects_DependentTipsDoNotReport(t *testing.T) {
	for _, key := range []string{"winner", "payout:alice"} {
		t.Run(key, func(t *testing.T) {
			e, log, binds := conflictLottery(t, "alice", "bob")
			merged := log.putEffect(&pb.Effect{
				Key: []byte(key), NodeId: 1, Hlc: sTs(int64(log.nextOff)),
				Deps: []*pb.EffectRef{toPbRef(binds["alice"]), toPbRef(binds["bob"])},
				Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("charlie"))},
			})
			// Keep redundant ancestor tips. A dependency also resolves the
			// transaction when it is only present on an associated key.
			e.updateIndex(key, nil, merged)

			if _, _, _, err := e.GetSnapshot("winner"); err != nil {
				t.Fatal(err)
			}

			if e.index.Contains(conflictDocumentPrefix+"alice") != nil {
				t.Fatal("ancestral WON transactions were reported as unresolved tips")
			}
		})
	}
}

func TestConflictEffects_ThreeWonTips(t *testing.T) {
	e, _, _ := conflictLottery(t, "alice", "bob", "charlie")

	if _, _, _, err := e.GetSnapshot("winner"); err != nil {
		t.Fatal(err)
	}

	for _, txn := range []string{"alice", "bob", "charlie"} {
		conflictDocument(t, e, txn)
	}
}

func TestConflictEffects_MissingHistoryRetries(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)
	missing := Tip{1, 42}
	alice := addConflictLottery(e, log, "alice", 1, map[string]Tip{"winner": missing})
	bob := addConflictLottery(e, log, "bob", 2, nil)

	if err := e.emitConflict("winner", alice, bob); err == nil {
		t.Fatal("missing history must not produce a complete document")
	}

	if e.index.Contains(conflictDocumentPrefix+"alice") != nil || e.index.Contains(conflictDocumentPrefix+"bob") != nil {
		t.Fatal("incomplete evidence would suppress a later retry")
	}

	log.effectCache.Put(missing, &pb.Effect{
		Key: []byte("winner"), NodeId: 1, Hlc: sTs(1),
		Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("undecided"))},
	})

	if err := e.emitConflict("winner", alice, bob); err != nil {
		t.Fatal(err)
	}

	conflictDocument(t, e, "alice")
	conflictDocument(t, e, "bob")
}

func TestConflictEffects_BinaryStateAndExpiry(t *testing.T) {
	binaryKey := string([]byte{0xff, 0x00})
	state := &pb.ReducedEffect{
		ExpiresAt: sTs(1),
		Partitions: map[string]*pb.ReducedEffect{binaryKey: {
			NetAdds: map[string]*pb.ReducedElement{binaryKey: {
				Data: scalarInsertRaw([]byte{0xfe, 0x00}), ExpiresAt: sTs(2),
			}},
		}},
	}
	original := proto.Clone(state)

	data, err := marshalConflictState(state)
	if err != nil {
		t.Fatal(err)
	}

	if !proto.Equal(original, state) {
		t.Fatal("encoding mutated the cached historical state")
	}

	var decoded pb.ReducedEffect
	if err := protojson.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}

	element := decoded.GetPartitions()["ff00"].GetNetAdds()["ff00"]
	if !slices.Equal(element.GetData().GetRaw(), []byte{0xfe, 0x00}) || !proto.Equal(decoded.GetExpiresAt(), sTs(1)) || !proto.Equal(element.GetExpiresAt(), sTs(2)) {
		t.Fatalf("lost binary data or recorded expiry: %s", data)
	}
}
