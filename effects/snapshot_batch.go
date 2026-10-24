package effects

import (
	"context"
	"errors"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
)

// snapshotReadView captures only the transaction closure of a batch. TipSet
// pointers are immutable; validating them after reconstruction detects changes
// during discovery or materialization without copying the entire key index.
type snapshotReadView struct {
	engine    *Engine
	ctx       context.Context
	frontier  map[string]*keytrie.TipSet
	resolved  map[string][]Tip
	invisible map[string]bool
	keys      map[string]struct{}
	anchors   map[string][]Tip
	verdicts  verdictMap
	lost      map[string]struct{}
	walked    int
}

func newSnapshotReadView(e *Engine, ctx context.Context) *snapshotReadView {
	return &snapshotReadView{
		engine: e, ctx: ctx,
		frontier:  make(map[string]*keytrie.TipSet),
		resolved:  make(map[string][]Tip),
		invisible: make(map[string]bool),
		lost:      make(map[string]struct{}),
	}
}

func (v *snapshotReadView) capture(key string) ([]Tip, error) {
	if err := v.ctx.Err(); err != nil {
		return nil, err
	}

	if _, ok := v.frontier[key]; !ok {
		if !v.engine.freeMissEligible(key) {
			if err := v.engine.ensureSubscribed(key); err != nil {
				return nil, err
			}
		}

		tips := v.engine.index.Contains(key)
		v.frontier[key] = tips
		v.resolved[key] = v.engine.resolveTipDeps(tips.Tips())
	}

	return v.resolved[key], nil
}

// Freeze visibility decisions too: horizon completion need not change a key's
// tips. A changed decision invalidates the entire batch at validation time.
func (v *snapshotReadView) isInvisible(txn string) bool {
	invisible, ok := v.invisible[txn]
	if !ok {
		invisible = v.engine.horizon != nil && v.engine.horizon.IsInvisible(txn)
		v.invisible[txn] = invisible
	}

	return invisible
}

func (v *snapshotReadView) valid() bool {
	for key, tips := range v.frontier {
		if v.engine.index.Contains(key) != tips ||
			!tipsEqual(v.resolved[key], v.engine.resolveTipDeps(tips.Tips())) {
			return false
		}
	}

	for txn, invisible := range v.invisible {
		if invisible != (v.engine.horizon != nil && v.engine.horizon.IsInvisible(txn)) {
			return false
		}
	}

	return true
}

func (v *snapshotReadView) discover(keys []string) error {
	var err error

	v.keys, v.anchors, v.verdicts, err = v.engine.bindKeyClosureFrom(keys, v.capture, true)
	if err != nil {
		return err
	}

	for txn, entry := range v.verdicts {
		if entry.verdict == pb.Verdict_LOST {
			v.lost[txn] = struct{}{}
		}
	}

	for key := range v.keys {
		losers, walked := v.engine.losersOnKeyAt(key, v.resolved[key], v.anchors[key], v.verdicts, v.isInvisible)

		v.walked += walked
		for txn := range losers {
			v.lost[txn] = struct{}{}
		}
	}

	return nil
}

// GetSnapshots reconstructs a read-only batch using one closure and one set of
// fork-choice decisions. Results preserve input order and duplicates. Discovery
// or concurrent writes that change any captured input restart the whole batch;
// no values from a discarded attempt are returned.
func (e *Engine) GetSnapshots(ctx context.Context, keys []string) ([]*pb.ReducedEffect, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// Keep the existing subscription, free-miss, and Cloud hydration rules.
		// These preliminary values are discarded: later keys can reveal
		// competitors that change an earlier key's reconstruction.
		ready := make(map[string]bool)
		for _, key := range keys {
			if ready[key] {
				continue
			}

			if _, _, _, err := e.GetSnapshot(key); err != nil {
				return nil, err
			}

			ready[key] = true
		}

		view := newSnapshotReadView(e, ctx)
		if err := view.discover(keys); err != nil {
			if !view.valid() {
				continue
			}

			return nil, err
		}

		results := make([]*pb.ReducedEffect, len(keys))

		var readErr error

		for i, key := range keys {
			// Include histories learned through other keys even when their
			// bind has not yet been installed in this key's live frontier.
			tips := append([]Tip(nil), view.resolved[key]...)
			tips = append(tips, view.anchors[key]...)

			results[i], _, readErr = e.reconstructWithView(key, tips, "", true, view)
			if readErr != nil {
				break
			}
		}

		if errors.Is(readErr, errHorizonRetryNeeded) || !view.valid() {
			continue
		}

		if readErr != nil {
			return nil, readErr
		}

		for i, key := range keys {
			e.updateSerializationState(key, results[i])
		}

		return results, nil
	}
}
