package effects

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

func asymmetricLottery(winner string) *Engine {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)

	ah, bh := byte(1), byte(2)
	if winner == "bob" {
		ah, bh = bh, ah
	}

	a := closureBind(log, "alice", ah, []string{"winner", "payout:alice"}, nil)

	b := closureBind(log, "bob", bh, []string{"winner", "payout:bob"}, nil)
	for _, item := range []struct {
		key  string
		txn  string
		bind Tip
	}{
		{"winner", "alice", a}, {"payout:alice", "alice", a}, {"payout:bob", "bob", b},
	} {
		snap := log.putEffect(&pb.Effect{
			Key: []byte(item.key), NodeId: 1, Hlc: sTs(int64(log.nextOff)),
			Deps: []*pb.EffectRef{toPbRef(item.bind)},
			Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
				TxnVerdicts: map[string]pb.Verdict{item.txn: pb.Verdict_WON},
			}},
		})
		e.updateIndex(item.key, nil, snap)
	}

	return e
}

func TestGetSnapshots_AsymmetricLottery(t *testing.T) {
	keys := []string{"winner", "payout:alice", "payout:bob"}

	for _, winner := range []string{"alice", "bob"} {
		for _, order := range [][]int{{0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}} {
			t.Run(fmt.Sprint(winner, order), func(t *testing.T) {
				e := asymmetricLottery(winner)
				ordered := []string{keys[order[0]], keys[order[1]], keys[order[2]]}

				got, err := e.NewReadOnlyContext().GetSnapshots(ordered)
				if err != nil {
					t.Fatal(err)
				}

				for i, key := range ordered {
					want := ""

					switch key {
					case "winner":
						want = winner
					case "payout:" + winner:
						want = "100"
					}

					if string(got[i].GetScalar().GetRaw()) != want || (want == "" && got[i] != nil) {
						t.Fatalf("first batch %v: %s = %v; want %q", ordered, key, got[i], want)
					}
				}
			})
		}
	}
}

// The closure fix alone cannot repair values already buffered by a sequential
// MGET. Keep that counterexample explicit alongside the batch regression.
func TestGetSnapshots_SequentialReadCounterexample(t *testing.T) {
	e := asymmetricLottery("bob")

	c := e.NewReadOnlyContext()
	for _, key := range []string{"winner", "payout:alice", "payout:bob"} {
		got, _, err := c.GetSnapshot(key)

		want := "100"
		if key == "winner" {
			want = "alice"
		}

		if err != nil || string(got.GetScalar().GetRaw()) != want {
			t.Fatalf("counterexample changed at %s: %v, %v", key, got, err)
		}
	}
}

func TestSnapshotReadView_ValidatesAssociatedFrontiers(t *testing.T) {
	e := asymmetricLottery("alice")

	v := newSnapshotReadView(e, context.Background())
	if err := v.discover([]string{"payout:alice"}); err != nil {
		t.Fatal(err)
	}

	if !v.valid() {
		t.Fatal("unchanged view rejected")
	}
	// The requested key stays unchanged; a newly learned competitor on its
	// associated winner key must nevertheless invalidate the entire view.
	bob, _ := e.effectCache.Get(e.index.Contains("payout:bob").Tips()[0])
	e.updateIndex("winner", nil, r(bob.GetDeps()[0]))

	if v.valid() {
		t.Fatal("new cross-key competitor did not invalidate view")
	}
}

func TestSnapshotReadView_ValidatesVisibility(t *testing.T) {
	e := newSnapshotEngine(newSnapshotLog())
	e.horizon = newHorizonSet(e)
	v := newSnapshotReadView(e, context.Background())
	bind := &pb.TransactionalBindEffect{Keys: []*pb.TransactionalBindEffect_KeyBind{
		{Key: []byte("a"), NewTip: toPbRef(Tip{1, 100})},
	}}
	e.horizon.Add("pending", Tip{1, 200}, bind)

	if !v.isInvisible("pending") {
		t.Fatal("pending bind visible")
	}

	e.horizon.MakeVisible("pending")

	if !v.isInvisible("pending") {
		t.Fatal("decision changed within captured view")
	}

	if v.valid() {
		t.Fatal("horizon completion did not invalidate view")
	}
}

func TestGetSnapshots_MissesDuplicatesAndCancellation(t *testing.T) {
	e := newSnapshotEngine(newSnapshotLog())
	ctx := e.NewReadOnlyContext()

	got, err := ctx.GetSnapshots([]string{"missing", "missing"})
	if err != nil || len(got) != 2 || got[0] != nil || got[1] != nil {
		t.Fatalf("misses: %v, %v", got, err)
	}

	if e.index.Contains("missing") != nil {
		t.Fatal("free miss created an index leaf")
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := e.GetSnapshots(cancelled, []string{"missing"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled read: %v", err)
	}

	if got, err := ctx.GetSnapshots(nil); err != nil || len(got) != 0 {
		t.Fatalf("empty batch: %v, %v", got, err)
	}
}

func TestGetSnapshots_CloudFailureReturnsNoPartialBatch(t *testing.T) {
	e := asymmetricLottery("bob")
	e.SetCloudReader(&fakeCloudReader{err: errors.New("cloud unavailable")})

	got, err := e.NewReadOnlyContext().GetSnapshots([]string{"winner", "absent"})
	if err == nil || got != nil {
		t.Fatalf("partial batch escaped: %v, %v", got, err)
	}
}

func TestGetSnapshots_MissingBranchReturnsError(t *testing.T) {
	e := newSnapshotEngine(newSnapshotLog())
	e.updateIndex("missing-branch", nil, Tip{99, 99})

	got, err := e.NewReadOnlyContext().GetSnapshots([]string{"missing-branch"})
	if err == nil || got != nil {
		t.Fatalf("unreadable history returned as data: %v, %v", got, err)
	}
}

func TestGetSnapshots_PreservesTransactionReads(t *testing.T) {
	e := newSnapshotEngine(newSnapshotLog())
	writer := e.NewContext()
	emit := func(c *Context, key, value string) {
		t.Helper()

		if err := c.Emit(&pb.Effect{Key: []byte(key), Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte(value))}}); err != nil {
			t.Fatal(err)
		}
	}
	emit(writer, "a", "before")

	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}

	reader := e.NewContext()
	reader.BeginTx()
	emit(writer, "a", "after")

	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}

	emit(reader, "b", "own-write")

	got, err := reader.GetSnapshots([]string{"a", "b", "a"})
	if err != nil {
		t.Fatal(err)
	}

	for i, want := range []string{"before", "own-write", "before"} {
		if string(got[i].GetScalar().GetRaw()) != want {
			t.Fatalf("read %d = %v; want %s", i, got[i], want)
		}
	}

	if reader.keys["a"] == nil {
		t.Fatal("transactional read was not recorded")
	}
}

type batchInterleaveHandler struct {
	slog.Handler

	afterFirstValue func()
	reads           int
}

func (*batchInterleaveHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *batchInterleaveHandler) Handle(_ context.Context, record slog.Record) error {
	if record.Message != "reconstruct: done" {
		return nil
	}

	var key string

	record.Attrs(func(a slog.Attr) bool {
		if a.Key == "key" {
			key = a.Value.String()
		}

		return true
	})

	if key == "a" {
		h.reads++
		// First a read is hydration/preflight. Second is materialization
		// from the captured view, before b has been materialized.
		if h.reads == 2 {
			h.afterFirstValue()
		}
	}

	return nil
}

func TestGetSnapshots_RetriesEntireBatchOnConcurrentWrite(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)
	newTips := make(map[string]Tip)

	for _, key := range []string{"a", "b"} {
		old := log.putEffect(&pb.Effect{
			Key: []byte(key), NodeId: 1, Hlc: sTs(int64(log.nextOff)),
			Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("old"))},
		})
		e.updateIndex(key, nil, old)
		newTips[key] = log.putEffect(&pb.Effect{
			Key: []byte(key), NodeId: 1, Hlc: sTs(int64(log.nextOff)),
			Deps: []*pb.EffectRef{toPbRef(old)},
			Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("new"))},
		})
	}

	handler := &batchInterleaveHandler{
		Handler: slog.DiscardHandler,
		afterFirstValue: func() {
			for key, tip := range newTips {
				e.updateIndex(key, e.index.Contains(key), tip)
			}
		},
	}
	previous := slog.Default()

	slog.SetDefault(slog.New(handler))
	defer slog.SetDefault(previous)

	got, err := e.NewReadOnlyContext().GetSnapshots([]string{"a", "b"})
	if err != nil {
		t.Fatal(err)
	}

	if handler.reads < 3 {
		t.Fatal("test did not force a batch retry")
	}

	for i, result := range got {
		if string(result.GetScalar().GetRaw()) != "new" {
			t.Fatalf("stale batch value %d escaped: %v", i, result)
		}
	}
}
