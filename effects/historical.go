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
// selected historical frontiers. The caller supplies the same fixed cut for
// every key. It must establish completeness before exposing interactive reads;
// this helper does not claim that currently available tips are complete.
func (e *Engine) historicalReadView(ctx context.Context, keys []string, frontier func(string) ([]Tip, error)) (*snapshotReadView, error) {
	v := newSnapshotReadView(e, ctx)
	v.historical = true

	err := v.discover(keys, func(key string) ([]Tip, error) {
		if tips, ok := v.resolved[key]; ok {
			return tips, nil
		}

		tips, err := frontier(key)
		if err == nil {
			v.resolved[key] = tips
		}

		return tips, err
	})
	if err != nil {
		return nil, err
	}

	return v, nil
}
