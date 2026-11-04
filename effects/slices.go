package effects

import (
	"cmp"
	"slices"

	pb "github.com/swytchdb/engine/cluster/proto"
)

// refsContain avoids converting a protobuf frontier just to test membership.
func refsContain(refs []*pb.EffectRef, tip Tip) bool {
	for _, ref := range refs {
		if r(ref) == tip {
			return true
		}
	}

	return false
}

// appendRefs extends traversal scratch without allocating an intermediate slice.
func appendRefs(tips []Tip, refs []*pb.EffectRef) []Tip {
	tips = slices.Grow(tips, len(refs))
	for _, ref := range refs {
		tips = append(tips, r(ref))
	}

	return tips
}

// compactTips sorts and deduplicates owned scratch storage in place. Callers
// must not pass a published TipSet's slice.
func compactTips(tips []Tip) []Tip {
	slices.SortFunc(tips, func(a, b Tip) int {
		if order := cmp.Compare(a[0], b[0]); order != 0 {
			return order
		}

		return cmp.Compare(a[1], b[1])
	})

	return slices.Compact(tips)
}

// shrinkSlice bounds retained capacity after deletions. Deleted pointer slots
// must already be cleared (for example by slices.Delete).
func shrinkSlice[T any](values []T) []T {
	if len(values) == 0 {
		return nil
	}

	if len(values) < cap(values)/4 {
		return slices.Clone(values)
	}

	return values
}
