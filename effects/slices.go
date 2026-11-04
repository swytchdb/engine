package effects

import "slices"

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
