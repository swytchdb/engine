package effects

import (
	"context"
	"fmt"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
	"github.com/swytchdb/engine/stn"
)

// transactionSnapshot is a local temporal boundary plus an incrementally
// constructed causal cut. BeginTx does not visit the index. Only accessed key
// closures acquire frontiers. Numerical Unknown is an inclusion choice, not
// an inferred time inequality; later evidence cannot reverse that choice.
// Nothing in this type decides whether a transaction is admissible to commit.
type transactionSnapshot struct {
	engine    *Engine
	at        stn.Sample
	expiryAt  time.Time // Redis TTLs are wall dates, separate from elapsed-clock coordinates
	clocks    *stn.Clocks
	views     map[string]*snapshotReadView
	decided   map[Tip]bool
	ambiguous int
}

func (e *Engine) newTransactionSnapshot() *transactionSnapshot {
	return &transactionSnapshot{
		engine:   e,
		at:       stn.Sample{Domain: uint64(e.nodeID), Tick: e.clock.Now().UnixNano()},
		expiryAt: time.Now(),
		clocks:   e.clockEvidence(),
	}
}

func (s *transactionSnapshot) tips(ctx context.Context, key string) (*keytrie.TipSet, error) {
	if err := s.prepare(ctx, []string{key}); err != nil {
		return nil, err
	}

	v := s.views[key]

	tips := append([]Tip(nil), v.resolved[key]...)
	if len(tips) == 0 {
		return nil, nil
	}

	return keytrie.NewTipSet(tips...), nil
}

// contains uses the existing DAG walker. Only when a later-discovered effect
// needs proof of membership behind a retained state snapshot do we continue
// behind that snapshot. An unavailable required ancestor is an error, not nil.
func (s *transactionSnapshot) contains(ctx context.Context, key string, target Tip) (bool, error) {
	v := s.views[key]
	if v == nil {
		return false, nil
	}

	frontier := append([]Tip(nil), v.resolved[key]...)
	frontier = append(frontier, v.anchors[key]...)
	seen := make(map[Tip]bool)

	for len(frontier) != 0 {
		if err := ctx.Err(); err != nil {
			return false, err
		}

		d := newDag(s.engine, key, "")
		if err := d.prepare(frontier); err != nil {
			return false, err
		}

		if _, ok := d.nodes[target]; ok {
			return true, nil
		}

		frontier = nil

		for tip, eff := range d.nodes {
			if eff.GetSnapshot() == nil || seen[tip] {
				continue
			}

			seen[tip] = true

			frontier = append(frontier, fromPbRefs(eff.GetDeps())...)
		}
	}

	return false, nil
}

func (s *transactionSnapshot) member(ctx context.Context, tip Tip, eff *pb.Effect, visiting map[Tip]bool) (bool, error) {
	if chosen, ok := s.decided[tip]; ok {
		return chosen, nil
	}

	if err := ctx.Err(); err != nil {
		return false, err
	}

	if visiting[tip] {
		return false, fmt.Errorf("mvcc: cyclic effect history at %v", tip)
	}

	visiting[tip] = true
	defer delete(visiting, tip)

	choose := func(include bool, why string) (bool, error) {
		s.decided[tip] = include
		temporalDebug(ctx, s.at, tip, why)

		return include, nil
	}

	// A newly discovered branch cannot extend a key whose history was
	// already exposed. Membership is shared for the entire bind; this is
	// history selection, not a new rule for adjudicating competing binds.
	keys := []string{string(eff.GetKey())}
	if bind := eff.GetTxnBind(); bind != nil {
		keys = nil
		for _, kb := range bind.GetKeys() {
			keys = append(keys, string(kb.GetKey()))
		}
	}

	knownPast := false

	for _, key := range keys {
		if s.views[key] == nil {
			continue
		}

		included, err := s.contains(ctx, key, tip)
		if err != nil {
			return false, err
		}

		if !included {
			return choose(false, "outside frozen key frontier")
		}

		knownPast = true
	}

	if knownPast {
		return choose(true, "ancestor of frozen frontier")
	}

	lower, upper, err := s.clocks.Difference(s.at, effectSample(eff))
	if err != nil {
		return false, fmt.Errorf("mvcc: clock interval for %v: %w", tip, err)
	}

	if lower > 0 {
		return choose(false, "numerically after boundary")
	}
	// A proven-before state snapshot summarizes an entire causal prefix.
	// Before any keys have been frozen there are no prior cut choices that
	// could reject part of that prefix. Do not demand discarded ancestry
	// merely to re-prove what an existing state snapshot already records.
	if eff.GetSnapshot().GetState() != nil && upper <= 0 && len(s.views) == 0 {
		return choose(true, "before-boundary state snapshot")
	}

	for _, ref := range allDeps(eff) {
		parent, err := s.engine.getEffect(string(eff.GetKey()), r(ref))
		if err != nil {
			return false, err
		}

		include, err := s.member(ctx, r(ref), parent, visiting)
		if err != nil {
			return false, err
		}

		if !include {
			return choose(false, "descendant of excluded history")
		}
	}

	if upper <= 0 {
		return choose(true, "numerically before boundary")
	}

	s.ambiguous++

	return choose(true, "ambiguous; fixed causal inclusion")
}

func (s *transactionSnapshot) prepare(ctx context.Context, requested []string) error {
	var keys []string

	for _, key := range requested {
		if s.views[key] == nil {
			keys = append(keys, key)
		}
	}

	if len(keys) == 0 {
		return nil
	}

	if s.views == nil {
		s.views = make(map[string]*snapshotReadView)
		s.decided = make(map[Tip]bool)
	}
	// Bootstrap the history without materializing a discarded live value.
	// Cloud's read-miss backstop currently needs a reconstruction to distinguish
	// authoritative nil from an evicted key; retain that path when configured.
	// Ordinary reads reconstruct only the selected historical view below.
	for _, key := range keys {
		if s.engine.cloudReader != nil {
			if _, _, _, err := s.engine.GetSnapshot(key); err != nil {
				return err
			}
		} else if !s.engine.freeMissEligible(key) {
			if err := s.engine.ensureSubscribed(key); err != nil {
				return err
			}
		}
	}

	live := newSnapshotReadView(s.engine, ctx)
	if err := live.discover(keys); err != nil {
		return err
	}

	for key := range live.keys {
		tips := append([]Tip(nil), live.resolved[key]...)
		tips = append(tips, live.anchors[key]...)

		d := newDag(s.engine, key, "")
		if err := d.prepare(tips); err != nil {
			return err
		}

		for _, tip := range d.topoOrder {
			eff := d.nodes[tip]
			if eff.GetTxnBind() != nil && s.engine.horizon != nil && s.engine.horizon.IsInvisible(eff.GetTxnId()) {
				wait := s.engine.horizon.WaitForClear(eff.GetTxnId())
				wait.Release()
				// Completion can reveal verdicts and competitors. Recollect
				// the closure before publishing any value from this read.
				return s.prepare(ctx, keys)
			}
		}

		s.engine.observeTemporalDag(d)
	}
	// Collect all currently available DAG evidence before membership, not an
	// early read-admission gate. Existing inclusion/exclusion decisions win
	// over subsequent numerical tightening.
	s.clocks = s.engine.clockEvidence()
	selected := make(map[string][]Tip)
	frontier := func(key string) ([]Tip, error) {
		if old := s.views[key]; old != nil {
			return old.resolved[key], nil
		}

		if tips, ok := selected[key]; ok {
			return tips, nil
		}

		_, err := live.capture(key)
		if err != nil {
			return nil, err
		}
		// Keep the actual selected frontier for Context.Emit's consumed
		// tips. resolveTipDeps is a reconstruction operation: substituting
		// pending WATCH/transaction tips here would turn valid consumed
		// tips into stale ancestors before the existing commit protocol.
		tips := live.frontier[key].Tips()

		tips, err = s.engine.historicalFrontier(ctx, key, tips, func(t Tip, eff *pb.Effect) (bool, error) {
			return s.member(ctx, t, eff, make(map[Tip]bool))
		})
		if err == nil {
			selected[key] = tips
		}

		return tips, err
	}

	view, err := s.engine.historicalReadView(ctx, keys, frontier)
	if err != nil {
		return err
	}

	for key := range view.keys {
		if s.views[key] == nil {
			s.views[key] = view
		}
	}

	for _, key := range keys {
		if s.views[key] == nil {
			s.views[key] = view
		}
	}

	return nil
}
