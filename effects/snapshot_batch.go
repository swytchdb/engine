/*
 * Copyright 2026 Swytch Labs BV
 *
 * This file is part of Swytch.
 *
 * Swytch is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Swytch is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Swytch. If not, see <https://www.gnu.org/licenses/>.
 *
 * Additional permissions under GNU AGPL version 3 section 7 apply to
 * this file. See the NOTICE.md file distributed with this source code
 * for the current set of additional permissions.
 */

package effects

import (
	"context"
	"errors"
	"maps"
	"slices"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
)

// snapshotReadView captures only the transaction closure of a batch. TipSet
// pointers are immutable; validating them after reconstruction detects changes
// during discovery or materialization without copying the entire key index.
type snapshotReadView struct {
	engine     *Engine
	ctx        context.Context
	frontiers  map[string]snapshotFrontier
	invisible  map[string]bool
	anchors    map[string][]Tip // every key in the closure, including keys without anchors
	verdicts   verdictMap
	lost       map[string]struct{}
	walked     int
	historical bool // report reconstruction uses recorded ancestry only
}

// Keep the captured index identity and its resolved tips together. Historical
// views populate only resolved; they never validate against the live index.
type snapshotFrontier struct {
	index    *keytrie.TipSet
	resolved []Tip
}

func newSnapshotReadView(e *Engine, ctx context.Context) *snapshotReadView {
	return &snapshotReadView{
		engine: e, ctx: ctx,
		frontiers: make(map[string]snapshotFrontier),
	}
}

func (v *snapshotReadView) capture(key string) ([]Tip, error) {
	if err := v.ctx.Err(); err != nil {
		return nil, err
	}

	frontier, ok := v.frontiers[key]
	if !ok {
		if !v.engine.freeMissEligible(key) {
			if err := v.engine.ensureSubscribed(key); err != nil {
				return nil, err
			}
		}

		tips := v.engine.index.Contains(key)
		frontier = snapshotFrontier{index: tips, resolved: v.engine.resolveTipDeps(tips.Tips())}
		v.frontiers[key] = frontier
	}

	return frontier.resolved, nil
}

// Freeze visibility decisions too: horizon completion need not change a key's
// tips. A changed decision invalidates the entire batch at validation time.
func (v *snapshotReadView) isInvisible(txn string) bool {
	if v.historical {
		return false
	}

	invisible, ok := v.invisible[txn]
	if !ok {
		invisible = v.engine.horizon != nil && v.engine.horizon.IsInvisible(txn)
		if v.invisible == nil {
			v.invisible = make(map[string]bool)
		}

		v.invisible[txn] = invisible
	}

	return invisible
}

func (v *snapshotReadView) valid() bool {
	if v.historical {
		return true
	}

	for key, frontier := range v.frontiers {
		if v.engine.index.Contains(key) != frontier.index ||
			!tipsEqual(frontier.resolved, v.engine.resolveTipDeps(frontier.index.Tips())) {
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

// discover applies the same closure and arbitration rules to live and historical
// frontiers. Historical views never report conflicts or consult live visibility.
func (v *snapshotReadView) discover(keys []string, frontier func(string) ([]Tip, error)) error {
	var err error

	v.anchors, v.verdicts, err = v.engine.bindKeyClosureFrom(keys, frontier, true)
	if err != nil {
		return err
	}

	for txn, entry := range v.verdicts {
		if entry.verdict == pb.Verdict_LOST {
			if v.lost == nil {
				v.lost = make(map[string]struct{})
			}

			v.lost[txn] = struct{}{}
		}
	}

	for key := range v.anchors {
		losers, walked := v.engine.losersOnKeyAt(key, v.frontiers[key].resolved, v.anchors[key], v.verdicts, v.isInvisible, !v.historical)

		v.walked += walked
		if v.lost == nil {
			v.lost = losers
		} else {
			maps.Copy(v.lost, losers)
		}
	}

	return nil
}

// GetSnapshots reconstructs a read-only batch using one closure and one set of
// fork-choice decisions. Results preserve input order and duplicates. Discovery
// or concurrent writes that change any captured input restart the whole batch;
// no values from a discarded attempt are returned.
func (e *Engine) GetSnapshots(ctx context.Context, keys []string) ([]*pb.ReducedEffect, error) {
	unique := slices.Clone(keys)
	slices.Sort(unique)
	unique = slices.Compact(unique)

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// Keep the existing subscription, free-miss, and Cloud hydration rules.
		// These preliminary values are discarded: later keys can reveal
		// competitors that change an earlier key's reconstruction.
		for _, key := range unique {
			if _, _, _, err := e.GetSnapshot(key); err != nil {
				return nil, err
			}
		}

		view := newSnapshotReadView(e, ctx)
		if err := view.discover(keys, view.capture); err != nil {
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
			tips := slices.Concat(view.frontiers[key].resolved, view.anchors[key])

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
