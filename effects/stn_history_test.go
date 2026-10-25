package effects

import (
	"context"
	"sort"
	"testing"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/stn"
	"google.golang.org/protobuf/proto"
)

// This test generates effects through Context.Emit/Flush and transfers their
// actual wire representations through HandleRemote. It never fabricates DAG
// dependencies or inserts synthetic vertices into an engine index.
//
// The injectable clocks have independent origins and share one measured
// monotonic rate. This establishes the elapsed-time assumption for this
// experiment. The production clock separately preserves serialized elapsed
// intervals by projecting a monotonic source onto a captured origin.
func TestSTNRealHistoryNumericalInference(t *testing.T) {
	type record struct {
		tip    Tip
		effect *pb.Effect
	}

	var history []record

	start := time.Now()
	epoch := time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC)
	skews := map[uint64]time.Duration{}
	newNode := func(skew time.Duration) *Engine {
		e := NewEngine(EngineConfig{DefaultMode: UnsafeMode})
		skews[uint64(e.NodeID())] = skew
		e.clock.SetClock(func() time.Time { return epoch.Add(time.Since(start)).Add(skew) })
		e.SetOnLocalEffect(func(tip Tip, eff *pb.Effect) {
			history = append(history, record{tip, proto.Clone(eff).(*pb.Effect)})
		})
		t.Cleanup(func() {
			if err := e.Close(); err != nil {
				t.Error(err)
			}
		})

		return e
	}
	a, b := newNode(-2*time.Hour), newNode(3*time.Hour)
	write := func(e *Engine, key, value string) Tip {
		c := e.NewContext()
		if err := c.Emit(&pb.Effect{Key: []byte(key), Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte(value))}}); err != nil {
			t.Fatal(err)
		}

		tip := c.keys[key].lastOffset
		if err := c.Flush(); err != nil {
			t.Fatal(err)
		}

		return tip
	}
	// Establish one actual cross-node dependency on an unrelated witness key.
	if err := a.ensureSubscribed("witness"); err != nil {
		t.Fatal(err)
	}

	witnessB := write(b, "witness", "b")
	for _, rec := range append([]record(nil), history...) {
		if rec.tip[0] != uint64(b.NodeID()) || string(rec.effect.GetKey()) != "witness" {
			continue
		}

		data, err := MarshalEffect(rec.effect)
		if err != nil {
			t.Fatal(err)
		}

		notify := BuildOffsetNotify(b.NodeID(), rec.tip, rec.effect, data, context.Background())
		if _, err := a.HandleRemote(notify); err != nil {
			t.Fatal(err)
		}
	}

	if got, _, _, err := a.GetSnapshot("witness"); err != nil || string(got.GetScalar().GetRaw()) != "b" {
		t.Fatalf("replicated witness: %v, %v", got, err)
	}

	witnessA := write(a, "witness", "a")
	// No traffic or DAG connection is created between this new remote key
	// and A's subsequent transaction boundary.
	remote := write(b, "previously-unsubscribed", "historical")

	time.Sleep(20 * time.Millisecond)

	tx := a.NewContext()

	tx.BeginTx()
	defer tx.Abort()

	boundary := a.clock.Now()
	// Capture the actual local boundary as an STN variable, not a fake effect.
	n := len(history) + 1
	boundaryID := n - 1
	ids := make(map[Tip]int, len(history))
	byNode := make(map[uint64][]int)

	for i, rec := range history {
		ids[rec.tip] = i
		byNode[rec.tip[0]] = append(byNode[rec.tip[0]], i)
	}

	byNode[uint64(a.NodeID())] = append(byNode[uint64(a.NodeID())], boundaryID)
	stamp := func(i int) int64 {
		if i == boundaryID {
			return boundary.UnixNano()
		}

		return history[i].effect.GetHlc().AsTime().UnixNano()
	}

	type constraint struct {
		from, to int
		upper    int64
	}

	var (
		causal, numerical []constraint
		topology          []stn.Pair
	)

	for i, rec := range history {
		for _, dep := range allDeps(rec.effect) {
			j, ok := ids[r(dep)]
			if !ok {
				t.Fatalf("unrecorded dependency %v", dep)
			}

			causal = append(causal, constraint{i, j, 0})
			topology = append(topology, stn.Pair{A: i, B: j})
		}
	}

	for _, events := range byNode {
		sort.Slice(events, func(i, j int) bool {
			if events[j] == boundaryID {
				return true
			}

			if events[i] == boundaryID {
				return false
			}

			return history[events[i]].tip[1] < history[events[j]].tip[1]
		})

		for i := 1; i < len(events); i++ {
			prev, next := events[i-1], events[i]

			delta := stamp(next) - stamp(prev)
			if delta < 0 {
				t.Fatal("controlled monotonic clock regressed")
			}

			topology = append(topology, stn.Pair{A: prev, B: next})
			causal = append(causal, constraint{next, prev, 0})
			numerical = append(numerical, constraint{prev, next, delta}, constraint{next, prev, -delta})
		}
	}

	baseline, err := stn.New(n, topology)
	if err != nil {
		t.Fatal(err)
	}

	numeric, err := stn.New(n, topology)
	if err != nil {
		t.Fatal(err)
	}

	for _, c := range causal {
		if err := baseline.Constrain(c.from, c.to, c.upper); err != nil {
			t.Fatal(err)
		}

		if err := numeric.Constrain(c.from, c.to, c.upper); err != nil {
			t.Fatal(err)
		}
	}

	for _, c := range numerical {
		if err := numeric.Constrain(c.from, c.to, c.upper); err != nil {
			t.Fatal(err)
		}
	}

	causalBound, _ := baseline.Upper(boundaryID, ids[remote])
	numericBound, _ := numeric.Upper(boundaryID, ids[remote])

	if causalBound != stn.Infinity {
		t.Fatalf("causal baseline unexpectedly ordered the private key: %d", causalBound)
	}

	if numericBound >= 0 {
		t.Fatalf("numerical model failed to prove before: %d", numericBound)
	}
	// Withhold just the real cross-domain DAG evidence. Clock differences
	// alone must not manufacture a relation between independently based clocks.
	isolated, _ := stn.New(n, topology)

	domain := func(i int) uint64 {
		if i == boundaryID {
			return uint64(a.NodeID())
		}

		return history[i].tip[0]
	}
	for _, c := range causal {
		if domain(c.from) != domain(c.to) {
			continue
		}

		if err := isolated.Constrain(c.from, c.to, c.upper); err != nil {
			t.Fatal(err)
		}
	}

	for _, c := range numerical {
		if err := isolated.Constrain(c.from, c.to, c.upper); err != nil {
			t.Fatal(err)
		}
	}

	if got, _ := isolated.Upper(boundaryID, ids[remote]); got != stn.Infinity {
		t.Fatalf("inference without cross-domain evidence: %d", got)
	}
	// Widen the measured durations rather than assuming a precise rate.
	// The controlled clocks satisfy all these envelopes. This is sensitivity
	// analysis, not a claim that production has any particular ppm guarantee.
	for _, ppm := range []int64{1000, 10000} {
		bounded, _ := stn.New(n, topology)
		for _, c := range causal {
			if err := bounded.Constrain(c.from, c.to, c.upper); err != nil {
				t.Fatal(err)
			}
		}

		for _, c := range numerical {
			magnitude := c.upper
			if magnitude < 0 {
				magnitude = -magnitude
			}

			margin := (magnitude*ppm + 999999) / 1000000
			if err := bounded.Constrain(c.from, c.to, c.upper+margin); err != nil {
				t.Fatal(err)
			}
		}

		got, _ := bounded.Upper(boundaryID, ids[remote])
		if got >= 0 {
			t.Fatalf("elapsed interval +/- %d ppm lost expected inference: %d", ppm, got)
		}

		t.Logf("elapsed interval +/- %d ppm: unrelated write before boundary by at least %dns", ppm, -got)
	}
	// Check every ordered pair against the known common monotonic source.
	truth := func(i int) int64 {
		if i == boundaryID {
			return stamp(i) - int64(skews[uint64(a.NodeID())])
		}

		return stamp(i) - int64(skews[history[i].tip[0]])
	}
	// Exercise the exact compressed representation used in production, using
	// only these real DAG edges. No receipt/RTT constraints are fed here.
	anchors, err := stn.NewClocks(elapsedClockRatePPM)
	if err != nil {
		t.Fatal(err)
	}

	sample := func(i int) stn.Sample { return stn.Sample{Domain: domain(i), Tick: stamp(i)} }
	for i, rec := range history {
		for _, dep := range allDeps(rec.effect) {
			anchors, err = anchors.Before(sample(ids[r(dep)]), sample(i))
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	_, anchoredUpper, err := anchors.Difference(sample(boundaryID), sample(ids[remote]))
	if err != nil || anchoredUpper >= 0 {
		t.Fatalf("production clock anchors lost DAG-only inference: upper=%d err=%v", anchoredUpper, err)
	}

	anchorBefore, anchorAfter, anchorUnknown := 0, 0, 0
	queries, baselineOrdered, numericOrdered, numericAfter, numericUnknown, violations := 0, 0, 0, 0, 0, 0

	for i := range n {
		for j := range n {
			if i == j {
				continue
			}

			queries++

			lo, hi, err := anchors.Difference(sample(j), sample(i))
			if err != nil {
				t.Fatal(err)
			}

			actual := truth(i) - truth(j)
			if actual < lo || actual > hi {
				t.Fatalf("unsound production anchor interval: [%d,%d], actual=%d", lo, hi, actual)
			}

			if hi <= 0 {
				anchorBefore++
			} else if lo >= 0 {
				anchorAfter++
			} else {
				anchorUnknown++
			}

			bc, _ := baseline.Upper(j, i)
			nc, _ := numeric.Upper(j, i)

			if bc <= 0 {
				baselineOrdered++
			}

			reverse, _ := numeric.Upper(i, j)
			if nc <= 0 {
				numericOrdered++
			} else if reverse <= 0 {
				numericAfter++
			} else {
				numericUnknown++
			}

			if nc != stn.Infinity && truth(i)-truth(j) > nc {
				violations++
			}
		}
	}

	if violations != 0 {
		t.Fatalf("%d unsound numerical bounds", violations)
	}

	t.Logf("effects=%d causal_constraints=%d duration_constraints=%d pair_queries=%d baseline_before=%d numerical_before=%d numerical_after=%d numerical_unknown=%d unsound_bounds=%d", len(history), len(causal), len(numerical), queries, baselineOrdered, numericOrdered, numericAfter, numericUnknown, violations)
	t.Logf("production_anchor_before=%d production_anchor_after=%d production_anchor_unknown=%d unrelated_upper_ns=%d", anchorBefore, anchorAfter, anchorUnknown, anchoredUpper)
	t.Logf("witness B=%v @ %d -> A=%v @ %d; unrelated=%v @ %d; boundary A @ %d; numerical upper=%dns, causal=unknown", witnessB, stamp(ids[witnessB]), witnessA, stamp(ids[witnessA]), remote, stamp(ids[remote]), stamp(boundaryID), numericBound)
	// This transport-free ordering experiment does not claim subscription or
	// historical read acceptance; the real runtime workloads exercise those.
}

func TestSTNRejectsInjectedClockDiscontinuity(t *testing.T) {
	e := NewEngine(EngineConfig{DefaultMode: UnsafeMode})
	defer func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	}()

	now := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)

	e.clock.SetClock(func() time.Time { return now })

	var emitted []*pb.Effect

	e.SetOnLocalEffect(func(_ Tip, eff *pb.Effect) {
		if eff.GetData() != nil {
			emitted = append(emitted, proto.Clone(eff).(*pb.Effect))
		}
	})

	for range 2 {
		c := e.NewContext()
		if err := c.Emit(&pb.Effect{Key: []byte("clock-step"), Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("value"))}}); err != nil {
			t.Fatal(err)
		}

		if err := c.Flush(); err != nil {
			t.Fatal(err)
		}

		now = now.Add(-time.Hour)
	}

	if len(emitted) != 2 || len(emitted[1].GetDeps()) == 0 {
		t.Fatal("missing real sequential effects")
	}

	delta := emitted[1].GetHlc().AsTime().Sub(emitted[0].GetHlc().AsTime()).Nanoseconds()
	if delta >= 0 {
		t.Fatalf("expected serialized wall regression, got %d", delta)
	}

	s, _ := stn.New(2, []stn.Pair{{A: 0, B: 1}})
	if err := s.Constrain(1, 0, 0); err != nil {
		t.Fatal(err)
	} // actual DAG order

	if err := s.Constrain(0, 1, delta); err != stn.ErrInconsistent {
		t.Fatalf("wall difference must contradict causal order, got %v", err)
	}
}
