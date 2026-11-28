package bplus

import (
	"bytes"
	"fmt"

	"github.com/swytchdb/engine/effects"
)

// treeStats is diagnostic state used only by tests.
type treeStats struct{ Nodes, Leaves, Entries, Height int }

// checkTree walks the whole tree in one view, validating occupancy, separators,
// parent links, leaf depth, and both leaf-sibling directions. Normal operations
// never perform this whole-tree walk.
func checkTree(t *Tree, c *effects.Context) (treeStats, error) {
	o, err := t.operation(c)
	if err != nil {
		return treeStats{}, err
	}

	var stats treeStats

	seen := make(map[string]bool)

	var (
		leaves []*node
		walk   func(string, string, int) ([]byte, []byte, error)
	)

	walk = func(address, parent string, depth int) ([]byte, []byte, error) {
		if seen[address] {
			return nil, nil, fmt.Errorf("%w: multiply referenced/cyclic page", ErrCorrupt)
		}

		seen[address] = true

		n, err := o.load(address)
		if err != nil {
			return nil, nil, err
		}

		stats.Nodes++

		if n.Parent != parent {
			return nil, nil, fmt.Errorf("%w: parent link", ErrCorrupt)
		}

		if parent != "" && len(n.Keys) < o.minimum(n) {
			return nil, nil, fmt.Errorf("%w: page underflow", ErrCorrupt)
		}

		if n.Leaf {
			if stats.Height != 0 && stats.Height != depth {
				return nil, nil, fmt.Errorf("%w: unequal leaf depths", ErrCorrupt)
			}

			stats.Height = depth
			stats.Leaves++
			stats.Entries += len(n.Keys)

			leaves = append(leaves, n)
			if len(n.Keys) == 0 {
				if parent != "" {
					return nil, nil, fmt.Errorf("%w: empty nonroot leaf", ErrCorrupt)
				}

				return nil, nil, nil
			}

			return n.Keys[0], n.Keys[len(n.Keys)-1], nil
		}

		if n.Prev != "" || n.Next != "" || len(n.Keys) == 0 {
			return nil, nil, fmt.Errorf("%w: internal page shape", ErrCorrupt)
		}

		var minimum, maximum []byte

		for i, child := range n.Children {
			low, high, err := walk(child, n.address, depth+1)
			if err != nil {
				return nil, nil, err
			}

			if i == 0 {
				minimum = low
			} else if !bytes.Equal(n.Keys[i-1], low) || bytes.Compare(maximum, low) >= 0 {
				return nil, nil, fmt.Errorf("%w: separator or child range", ErrCorrupt)
			}

			maximum = high
		}

		return minimum, maximum, nil
	}
	if _, _, err := walk(t.RootAddress(), "", 1); err != nil {
		return treeStats{}, err
	}

	for i, n := range leaves {
		prev, next := "", ""
		if i > 0 {
			prev = leaves[i-1].address
		}

		if i+1 < len(leaves) {
			next = leaves[i+1].address
		}

		if n.Prev != prev || n.Next != next {
			return treeStats{}, fmt.Errorf("%w: sibling chain", ErrCorrupt)
		}
	}

	return stats, nil
}
