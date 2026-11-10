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
	"fmt"

	pb "github.com/swytchdb/engine/cluster/proto"
)

// historicalFrontier walks past effects outside a fixed cut, including state
// snapshots that would otherwise stop reconstruction. It returns an older
// frontier, not an admission error for the current tip. Membership must be
// immutable for the lifetime of the view, causally closed, and shared across
// keys. Establishing that cut is a separate prerequisite, not done here.
//
// In particular, membership of transactional data is decided by its bind.
// Excluding an envelope restores each key's ConsumedTips, never its NewTip.
func (e *Engine) historicalFrontier(ctx context.Context, key string, tips []Tip, member func(Tip, *pb.Effect) (bool, error)) ([]Tip, error) {
	stack := append([]Tip(nil), tips...)
	seen := make(map[Tip]bool)

	var frontier []Tip

	for len(stack) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		t := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if seen[t] {
			continue
		}

		seen[t] = true

		eff, err := e.getEffect(key, t)
		if err != nil {
			return nil, fmt.Errorf("historical frontier %q: %w", key, err)
		}

		include, err := member(t, eff)
		if err != nil {
			return nil, err
		}

		if include {
			frontier = append(frontier, t)

			continue
		}

		if bind := eff.GetTxnBind(); bind != nil {
			for _, kb := range bind.GetKeys() {
				if string(kb.GetKey()) == key {
					stack = appendRefs(stack, kb.GetConsumedTips())

					break
				}
			}
		} else {
			// Snapshot.Deps describe the state it compacted. PrevSnapshot
			// alone can skip intervening writes, so it is not a substitute.
			stack = appendRefs(stack, eff.GetDeps())
		}
	}

	return frontier, nil
}

// historicalReadView applies the existing bind closure and fork choice to
// selected historical frontiers, retaining live competition as evidence only.
// The caller supplies the same fixed cut for every key. It must establish
// completeness before exposing interactive reads;
// this helper does not claim that currently available tips are complete.
func (e *Engine) historicalReadView(ctx context.Context, keys []string, frontier func(string) ([]Tip, error), live *snapshotReadView) (*snapshotReadView, error) {
	v := newSnapshotReadView(e, ctx)
	v.historical = true

	err := v.discoverWithEvidence(keys, func(key string) ([]Tip, error) {
		if captured, ok := v.frontiers[key]; ok {
			return captured.resolved, nil
		}

		tips, err := frontier(key)
		if err == nil {
			v.frontiers[key] = snapshotFrontier{resolved: tips}
		}

		return tips, err
	}, live)
	if err != nil {
		return nil, err
	}

	return v, nil
}
