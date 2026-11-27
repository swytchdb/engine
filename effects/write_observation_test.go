package effects

import (
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
)

func TestWriteObservationReusesHistoryButChecksNewFrontier(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)
	base := log.putEffect(scalarSet("k", "base"))

	current := base
	for range 64 {
		current = log.putEffect(&pb.Effect{Key: []byte("k"), Deps: toPbRefs([]Tip{current}), Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}}})
	}

	e.updateIndex(logicalAddress("k"), nil, current)
	w := e.newWriteObservation(logicalAddress("k"), []Tip{base}, "ours")
	loads := 0

	w.load = func(tip Tip) (*pb.Effect, error) {
		loads++

		return e.getEffect(logicalAddress("k"), tip)
	}
	if w.changed() || loads != 64 {
		t.Fatalf("observation-only history changed or was walked repeatedly: %d loads", loads)
	}

	first := loads
	if w.changed() || loads != first {
		t.Fatalf("unchanged frontier rewalked old history: %d -> %d loads", first, loads)
	}
	// Local tentative publication wraps the old frontier between preflight and
	// the locked recheck. It adds one vertex, not another full history walk.
	ours := scalarSet("k", "ours")
	ours.TxnId, ours.Deps = "ours", toPbRefs([]Tip{current})
	local := log.putEffect(ours)
	e.updateIndex(logicalAddress("k"), e.index.Contains("k"), local)

	if w.changed() || loads != first+1 {
		t.Fatalf("local publication rescanned old history: %d loads", loads)
	}

	foreign := scalarSet("k", "foreign")
	foreign.Deps = toPbRefs([]Tip{current})
	write := log.putEffect(foreign)
	e.updateIndex(logicalAddress("k"), nil, write)

	if !w.changed() {
		t.Fatal("cached preflight hid a write that arrived before publication")
	}
}

func TestWriteObservationSharesObservedAncestry(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)

	tips := make([]Tip, 0, 128)

	for i := range 128 {
		eff := scalarSet("k", "old")
		if i != 0 {
			eff.Deps = toPbRefs([]Tip{tips[i-1]})
		}

		tips = append(tips, log.putEffect(eff))
	}
	// Several peers can advertise distinct stale ancestors of our observation.
	e.index.Insert("k", nil, keytrie.NewTipSet(tips[:32]...))
	w := e.newWriteObservation(logicalAddress("k"), tips[127:], "ours")
	loads := make(map[Tip]int)

	w.load = func(tip Tip) (*pb.Effect, error) {
		loads[tip]++

		return e.getEffect(logicalAddress("k"), tip)
	}
	if w.changed() {
		t.Fatal("stale ancestors were treated as new writes")
	}

	for tip, count := range loads {
		// One frontier visit and at most one observed-ancestry visit.
		if count > 2 {
			t.Fatalf("restarted observed history at %v: %d visits", tip, count)
		}
	}
}

func TestWriteObservationRetriesIncompleteHistory(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)
	missing := Tip{9, 1}
	observed := log.putEffect(&pb.Effect{Key: []byte("k"), Deps: toPbRefs([]Tip{missing}), Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}}})
	old := log.putEffect(scalarSet("k", "old"))
	e.index.Insert("k", nil, keytrie.NewTipSet(old))

	w := e.newWriteObservation(logicalAddress("k"), []Tip{observed}, "ours")
	if !w.changed() {
		t.Fatal("missing ancestry incorrectly proved the write was observed")
	}

	if err := e.effectCache.Put(missing, &pb.Effect{Key: []byte("k"), Deps: toPbRefs([]Tip{old}), Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}}}); err != nil {
		t.Fatal(err)
	}

	if w.changed() {
		t.Fatal("later check reused an earlier failed ancestry search")
	}
}
