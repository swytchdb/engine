package effects

import (
	"context"
	"log/slog"
	"sync/atomic"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/stn"
)

// The elapsed-clock contract permits up to 0.1% rate error. Clock origins
// need not agree. NewHLC projects monotonic elapsed time into wire timestamps;
// node incarnation IDs delimit restarts. This bound is not an RTT estimate.
const elapsedClockRatePPM = 1000

type temporalEvidence struct {
	current atomic.Pointer[stn.Clocks]
}

type temporalObservation struct {
	before, after stn.Sample
}

const (
	temporalDepsObserved uint32 = 1 << iota
	temporalReceiptObserved
)

// merge publishes immutable evidence without serializing readers or writers.
// A failed CAS means another writer strengthened the graph: replay against that
// version so neither writer's constraints are overwritten. Samples are fixed by
// the caller; retrying must not change the time of an observation.
func (t *temporalEvidence) merge(base *stn.Clocks, observations []temporalObservation) {
	for {
		next := base
		for _, observation := range observations {
			updated, err := next.Before(observation.before, observation.after)
			if err != nil {
				// A violated clock assumption supplies no numerical evidence.
				// DAG reconstruction and fork choice are unaffected.
				slog.Debug("mvcc: rejected numerical clock observation", "before", observation.before, "after", observation.after, "error", err)

				continue
			}

			next = updated
		}

		if next == base || t.current.CompareAndSwap(base, next) {
			return
		}

		base = t.current.Load()
	}
}

func effectSample(eff *pb.Effect) stn.Sample {
	return stn.Sample{Domain: eff.GetNodeId(), Tick: eff.GetHlc().AsTime().UnixNano()}
}

func (e *Engine) clockEvidence() *stn.Clocks {
	if c := e.temporal.current.Load(); c != nil {
		return c
	}

	c, err := stn.NewClocks(elapsedClockRatePPM)
	if err != nil {
		panic(err) // fixed, validated implementation constant
	}

	if e.temporal.current.CompareAndSwap(nil, c) {
		return c
	}

	return e.temporal.current.Load()
}

// observeTemporal accumulates inequalities from resident DAG dependencies.
// It never fetches history, gates a read, or chooses a transaction winner.
// Only ingestion samples a receipt. A prepared DAG retries dependency evidence
// until all parents have been observed; completed resident vertices are skipped.
// Immutable versions let BeginTx retain evidence in O(1).
func (e *Engine) observeTemporal(tip Tip, eff *pb.Effect, receipt bool) {
	if eff.GetHlc() == nil || e.effectCache == nil {
		return
	}

	var observed uint32

	v, _ := e.effectCache.m.Load(tip)
	if v != nil {
		observed = v.temporal.Load()
	}

	receipt = receipt && eff.GetNodeId() != uint64(e.nodeID) && observed&temporalReceiptObserved == 0
	if observed&temporalDepsObserved != 0 && !receipt {
		return
	}

	child := effectSample(eff)

	// Most effects have only a few dependencies. Keep that common case on
	// the stack, while allowing transaction envelopes with larger frontiers.
	var local [8]temporalObservation

	observations := local[:0]

	var completed uint32
	if observed&temporalDepsObserved == 0 {
		completed |= temporalDepsObserved

		for _, ref := range allDeps(eff) {
			parent, ok := e.effectCache.Get(r(ref))
			if !ok {
				completed &^= temporalDepsObserved

				continue
			}
			// Immutable effects with no timestamp cannot supply numerical
			// evidence; unlike an absent parent, retrying cannot fix them.
			if parent.GetHlc() != nil {
				observations = append(observations, temporalObservation{effectSample(parent), child})
			}
		}
	}

	if receipt {
		// Actual local receipt is a causal observation, not a claim of
		// symmetric delay. DAG timestamp differences remain the primary
		// information source; this also bounds old, sparsely written keys.
		received := stn.Sample{Domain: uint64(e.nodeID), Tick: e.clock.Now().UnixNano()}
		observations = append(observations, temporalObservation{child, received})
		completed |= temporalReceiptObserved
	}

	if len(observations) != 0 {
		e.temporal.merge(e.clockEvidence(), observations)
	}

	if v != nil && completed != 0 {
		// Publish completion after merging. Concurrent observers may repeat
		// work, but never skip evidence that is still waiting to publish.
		v.temporal.Or(completed)
	}
}

func (e *Engine) observeTemporalDag(d *dag) {
	if d == nil {
		return
	}

	for _, tip := range d.topoOrder {
		e.observeTemporal(tip, d.nodes[tip], false)
	}
}

func temporalDebug(ctx context.Context, at stn.Sample, tip Tip, decision string) {
	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		slog.DebugContext(ctx, "mvcc: historical membership", "boundary_node", at.Domain, "boundary_ns", at.Tick, "tip", tip, "decision", decision)
	}
}
