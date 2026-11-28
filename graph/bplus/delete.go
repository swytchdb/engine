package bplus

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/swytchdb/engine/effects"
)

// Delete removes a key, borrowing or merging pages when needed. Removed pages
// receive explicit deletions; unlinking alone is never their deletion signal.
func (t *Tree) Delete(c *effects.Context, key []byte) (found bool, err error) {
	c.BeginTx()
	sp := c.TakeSavepoint()

	defer func() {
		if err != nil {
			c.RestoreSavepoint(sp)
		}
	}()

	o, err := t.operation(c)
	if err != nil {
		return false, err
	}

	n, err := o.find(key)
	if err != nil {
		return false, err
	}

	i, found := slices.BinarySearchFunc(n.Keys, key, bytes.Compare)
	if !found {
		return false, nil
	}

	n.Keys = slices.Delete(n.Keys, i, i+1)
	n.Values = slices.Delete(n.Values, i, i+1)

	n.dirty = true
	if i == 0 && len(n.Keys) > 0 {
		if err := o.propagateMin(n, n.Keys[0]); err != nil {
			return false, err
		}
	}

	if err := o.rebalance(n); err != nil {
		return false, err
	}

	return true, o.emit()
}

func (o *operation) minimum(n *node) int {
	if n.Leaf {
		return (o.opts.MaxKeys + 1) / 2
	}

	return (o.opts.MaxKeys+2)/2 - 1
}

func (o *operation) rebalance(n *node) error {
	for {
		if n.Parent == "" {
			if !n.Leaf && len(n.Children) == 1 {
				return o.shrinkRoot(n)
			}

			return nil
		}

		if len(n.Keys) >= o.minimum(n) {
			return nil
		}

		parent, err := o.load(n.Parent)
		if err != nil {
			return err
		}

		i := slices.Index(parent.Children, n.address)
		if i < 0 {
			return fmt.Errorf("%w: rebalance parent", ErrCorrupt)
		}

		var left, right *node
		if i > 0 {
			left, err = o.load(parent.Children[i-1])
			if err != nil {
				return err
			}

			if left.Leaf != n.Leaf {
				return fmt.Errorf("%w: sibling height", ErrCorrupt)
			}

			if len(left.Keys) > o.minimum(left) {
				return o.borrowLeft(parent, i, left, n)
			}
		}

		if i+1 < len(parent.Children) {
			right, err = o.load(parent.Children[i+1])
			if err != nil {
				return err
			}

			if right.Leaf != n.Leaf {
				return fmt.Errorf("%w: sibling height", ErrCorrupt)
			}

			if len(right.Keys) > o.minimum(right) {
				return o.borrowRight(parent, i, n, right)
			}
		}

		if left != nil {
			if err := o.merge(parent, i-1, left, n); err != nil {
				return err
			}
		} else if right != nil {
			if err := o.merge(parent, i, n, right); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("%w: no sibling for underfull page", ErrCorrupt)
		}

		n = parent
	}
}

func (o *operation) borrowLeft(parent *node, i int, left, n *node) error {
	last := len(left.Keys) - 1
	if n.Leaf {
		n.Keys = slices.Insert(n.Keys, 0, left.Keys[last])
		n.Values = slices.Insert(n.Values, 0, left.Values[last])
		left.Keys = left.Keys[:last]
		left.Values = left.Values[:last]
		parent.Keys[i-1] = bytes.Clone(n.Keys[0])
	} else {
		child := left.Children[len(left.Children)-1]
		n.Children = slices.Insert(n.Children, 0, child)
		n.Keys = slices.Insert(n.Keys, 0, parent.Keys[i-1])
		parent.Keys[i-1] = left.Keys[last]
		left.Keys = left.Keys[:last]
		left.Children = left.Children[:len(left.Children)-1]

		moved, err := o.load(child)
		if err != nil {
			return err
		}

		moved.Parent = n.address
		moved.dirty = true
	}

	left.dirty = true
	n.dirty = true
	parent.dirty = true

	return nil
}

func (o *operation) borrowRight(parent *node, i int, n, right *node) error {
	if n.Leaf {
		n.Keys = append(n.Keys, right.Keys[0])
		n.Values = append(n.Values, right.Values[0])
		right.Keys = slices.Delete(right.Keys, 0, 1)
		right.Values = slices.Delete(right.Values, 0, 1)
		parent.Keys[i] = bytes.Clone(right.Keys[0])
	} else {
		child := right.Children[0]
		n.Children = append(n.Children, child)
		n.Keys = append(n.Keys, parent.Keys[i])
		parent.Keys[i] = right.Keys[0]
		right.Keys = slices.Delete(right.Keys, 0, 1)
		right.Children = slices.Delete(right.Children, 0, 1)

		moved, err := o.load(child)
		if err != nil {
			return err
		}

		moved.Parent = n.address
		moved.dirty = true
	}

	n.dirty = true
	right.dirty = true
	parent.dirty = true

	return nil
}

func (o *operation) merge(parent *node, separator int, left, right *node) error {
	if left.Leaf {
		left.Keys = append(left.Keys, right.Keys...)
		left.Values = append(left.Values, right.Values...)

		left.Next = right.Next
		if right.Next != "" {
			next, err := o.load(right.Next)
			if err != nil {
				return err
			}

			next.Prev = left.address
			next.dirty = true
		}
	} else {
		left.Keys = append(left.Keys, parent.Keys[separator])
		left.Keys = append(left.Keys, right.Keys...)

		left.Children = append(left.Children, right.Children...)
		for _, address := range right.Children {
			child, err := o.load(address)
			if err != nil {
				return err
			}

			child.Parent = left.address
			child.dirty = true
		}
	}

	parent.Keys = slices.Delete(parent.Keys, separator, separator+1)
	parent.Children = slices.Delete(parent.Children, separator+1, separator+2)
	parent.dirty = true
	left.dirty = true
	right.dirty = true
	right.deleted = true

	return nil
}

func (o *operation) shrinkRoot(root *node) error {
	child, err := o.load(root.Children[0])
	if err != nil {
		return err
	}
	// Keep the root's lifetime allocation counter, address, and region. Reusing
	// a deleted child's counter could recycle previously deleted page identities.
	splits := root.Splits
	root.page = child.page
	root.Parent = ""
	root.Prev = ""
	root.Next = ""
	root.Splits = splits
	root.dirty = true
	child.deleted = true
	child.dirty = true

	return o.reparent(root)
}
