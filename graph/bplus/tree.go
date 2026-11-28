// Package bplus implements a byte-ordered B+ tree over independently addressed
// engine graph nodes. A Tree holds only its logical name, never cached pages.
// Operations use the supplied context's MVCC view and compose with an outer
// transaction. The caller owns Flush/Abort and any retry of a failed transaction.
package bplus

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/engine/keytrie"
)

const (
	Algorithm        = "bplus/v1"
	RootRegion       = "r"
	RegionLabel      = "lineage"
	optionsPartition = "bplus/options"
)

var (
	ErrExists        = errors.New("bplus: graph already exists")
	ErrNotFound      = errors.New("bplus: graph does not exist")
	ErrCorrupt       = errors.New("bplus: invalid tree")
	ErrUninitialized = errors.New("bplus: descriptor has no tree yet")
)

// Options is persisted in the descriptor. MaxKeys limits both leaf entries and
// internal separators. Zero selects 32; small orders are useful for split tests.
type Options struct {
	MaxKeys int `json:"max_keys"`
}

func (o Options) Normalized() (Options, error) {
	if o.MaxKeys == 0 {
		o.MaxKeys = 32
	}

	if o.MaxKeys < 3 || o.MaxKeys > 256 {
		return o, errors.New("bplus: MaxKeys must be between 3 and 256")
	}

	return o, nil
}

// Tree is safe to share; contexts are not. Byte keys are compared lexically.
// SQL callers encode typed tuples (and row IDs for nonunique indexes) themselves.
type Tree struct{ key string }

// Open constructs a handle. Every operation checks the descriptor in its own view.
func Open(key string) *Tree                  { return &Tree{key: key} }
func (t *Tree) Key() string                  { return t.key }
func (t *Tree) Address(region string) string { return keytrie.InternalKey(t.key, region) }
func (t *Tree) RootAddress() string          { return t.Address(RootRegion) }

// Create queues only the descriptor. The caller must Flush successfully before
// calling Initialize in a subsequent transaction. Neither method flushes for it.
func Create(c *effects.Context, key string, opts Options) (tree *Tree, err error) {
	opts, err = opts.Normalized()
	if err != nil {
		return nil, err
	}

	c.BeginTx()
	sp := c.TakeSavepoint()

	defer func() {
		if err != nil {
			c.RestoreSavepoint(sp)
		}
	}()

	state, _, err := c.GetSnapshot(key)
	if err != nil {
		return nil, err
	}

	if state != nil {
		return nil, ErrExists
	}

	tree = Open(key)
	for _, eff := range pb.NewGraphEffects([]byte(key), []byte(Algorithm)) {
		if err = c.Emit(eff); err != nil {
			return nil, err
		}
	}

	raw, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}

	if err = c.Emit(&pb.Effect{Key: []byte(key), Virtual: []byte(optionsPartition), Kind: &pb.Effect_Data{Data: &pb.DataEffect{
		Op: pb.EffectOp_INSERT_OP, Collection: pb.CollectionKind_SCALAR, Value: &pb.DataEffect_Raw{Raw: raw},
	}}}); err != nil {
		return nil, err
	}

	return tree, nil
}

// Initialize queues the initial root and its entry-point reference atomically
// against an established descriptor. It can compose with the caller's other
// writes, but must run after descriptor creation has been flushed.
func (t *Tree) Initialize(c *effects.Context) (err error) {
	c.BeginTx()
	sp := c.TakeSavepoint()

	defer func() {
		if err != nil {
			c.RestoreSavepoint(sp)
		}
	}()

	op, state, err := t.descriptor(c)
	if err != nil {
		return err
	}

	if len(state.GetPartitions()[pb.GraphEntryPointKey].GetOrderedElements()) != 0 {
		return ErrExists
	}

	op.newPage(RootRegion, true, "")

	if err = op.emit(); err != nil {
		return err
	}

	return c.Emit(&pb.Effect{Key: []byte(t.key), Virtual: []byte(pb.GraphEntryPointKey), Kind: &pb.Effect_Data{Data: &pb.DataEffect{
		Op: pb.EffectOp_INSERT_OP, Collection: pb.CollectionKind_ORDERED, Placement: pb.Placement_PLACE_TAIL,
		Id: []byte("root"), Value: &pb.DataEffect_Raw{Raw: []byte(t.RootAddress())},
	}}})
}

// page is a single bounded node value, not a serialized tree. References are
// complete opaque addresses. Regions live separately in ordinary node data.
type page struct {
	Leaf     bool
	Parent   string
	Keys     [][]byte
	Values   [][]byte
	Children []string
	Prev     string
	Next     string
	Splits   uint64
}

type node struct {
	page

	address, region       string
	tips                  []effects.Tip
	fresh, dirty, deleted bool
}

// operation owns only the pages read during this operation. It never survives
// the call, and every page comes from the caller's one transaction view.
type operation struct {
	tree       *Tree
	ctx        *effects.Context
	opts       Options
	descriptor []effects.Tip
	nodes      map[string]*node
}

func (t *Tree) operation(c *effects.Context) (*operation, error) {
	op, state, err := t.descriptor(c)
	if err != nil {
		return nil, err
	}

	entries := state.GetPartitions()[pb.GraphEntryPointKey].GetOrderedElements()
	if len(entries) == 0 {
		return nil, ErrUninitialized
	}

	if len(entries) != 1 || string(entries[0].GetData().Decompress()) != t.RootAddress() {
		return nil, fmt.Errorf("%w: entry point", ErrCorrupt)
	}

	return op, nil
}

func (t *Tree) descriptor(c *effects.Context) (*operation, *pb.ReducedEffect, error) {
	c.BeginTx() // idempotent: an outer transaction supplies the same causal cut

	state, tips, err := c.GetSnapshot(t.key)
	if err != nil {
		return nil, nil, err
	}

	if state == nil {
		return nil, nil, ErrNotFound
	}

	if state.GetTypeTag() != pb.ValueType_TYPE_GRAPH || string(state.GetNetAdds()[pb.GraphAlgorithmKey].GetData().Decompress()) != Algorithm {
		return nil, nil, fmt.Errorf("%w: incompatible algorithm", ErrCorrupt)
	}

	var opts Options
	if err := json.Unmarshal(state.GetPartitions()[optionsPartition].GetScalar().Decompress(), &opts); err != nil {
		return nil, nil, fmt.Errorf("%w: options: %v", ErrCorrupt, err)
	}

	if _, err := opts.Normalized(); err != nil || opts.MaxKeys == 0 {
		return nil, nil, fmt.Errorf("%w: page order", ErrCorrupt)
	}

	return &operation{tree: t, ctx: c, opts: opts, descriptor: tips, nodes: make(map[string]*node)}, state, nil
}

func (o *operation) load(address string) (*node, error) {
	if n, ok := o.nodes[address]; ok {
		if n.deleted {
			return nil, fmt.Errorf("%w: reference to deleted page", ErrCorrupt)
		}

		return n, nil
	}

	state, tips, err := o.ctx.GetSnapshotInternal(address)
	if err != nil {
		return nil, err
	}

	if state == nil {
		return nil, fmt.Errorf("%w: missing referenced page %q", ErrCorrupt, address)
	}

	n := &node{address: address, tips: tips}
	if err := decodePage(state.GetScalar().Decompress(), &n.page); err != nil {
		return nil, fmt.Errorf("%w: page: %v", ErrCorrupt, err)
	}

	n.region = string(state.GetPartitions()[pb.GraphRegionsKey].GetNetAdds()[RegionLabel].GetData().Decompress())
	if n.region == "" || o.tree.Address(n.region) != address {
		return nil, fmt.Errorf("%w: page lineage", ErrCorrupt)
	}

	if !slices.IsSortedFunc(n.Keys, bytes.Compare) || len(n.Keys) > o.opts.MaxKeys {
		return nil, fmt.Errorf("%w: key order or capacity", ErrCorrupt)
	}

	for i := 1; i < len(n.Keys); i++ {
		if bytes.Equal(n.Keys[i-1], n.Keys[i]) {
			return nil, fmt.Errorf("%w: duplicate separator/entry", ErrCorrupt)
		}
	}

	if n.Leaf && (len(n.Values) != len(n.Keys) || len(n.Children) != 0) || !n.Leaf && (len(n.Children) != len(n.Keys)+1 || len(n.Values) != 0) {
		return nil, fmt.Errorf("%w: page arity", ErrCorrupt)
	}

	o.nodes[address] = n

	return n, nil
}

func (o *operation) newPage(region string, leaf bool, parent string) *node {
	n := &node{Leaf: leaf, Parent: parent, address: o.tree.Address(region), region: region, fresh: true, dirty: true}
	o.nodes[n.address] = n

	return n
}

func (o *operation) find(key []byte) (*node, error) {
	address := o.tree.RootAddress()

	seen := make(map[string]bool)
	for {
		if seen[address] {
			return nil, fmt.Errorf("%w: child cycle", ErrCorrupt)
		}

		seen[address] = true

		n, err := o.load(address)
		if err != nil {
			return nil, err
		}

		if n.Leaf {
			return n, nil
		}

		i, found := slices.BinarySearchFunc(n.Keys, key, bytes.Compare)
		if found {
			i++
		}

		address = n.Children[i]
	}
}

// Get reads through a single MVCC view. Pure readers call Abort when finished;
// no transaction bind needs to be published just to return the result.
func (t *Tree) Get(c *effects.Context, key []byte) ([]byte, bool, error) {
	o, err := t.operation(c)
	if err != nil {
		return nil, false, err
	}

	n, err := o.find(key)
	if err != nil {
		return nil, false, err
	}

	i, found := slices.BinarySearchFunc(n.Keys, key, bytes.Compare)
	if !found {
		return nil, false, nil
	}

	return bytes.Clone(n.Values[i]), true, nil
}

type Entry struct{ Key, Value []byte }

// Scan returns [lower, upper) in lexical order. A nil bound is unbounded, while
// a nonnil empty slice is the empty byte key. limit <= 0 means no result limit.
func (t *Tree) Scan(c *effects.Context, lower, upper []byte, limit int) ([]Entry, error) {
	o, err := t.operation(c)
	if err != nil {
		return nil, err
	}

	n, err := o.find(lower)
	if err != nil {
		return nil, err
	}

	var result []Entry

	seen := make(map[string]bool)
	for {
		if seen[n.address] {
			return nil, fmt.Errorf("%w: leaf cycle", ErrCorrupt)
		}

		seen[n.address] = true
		for i, key := range n.Keys {
			if lower != nil && bytes.Compare(key, lower) < 0 {
				continue
			}

			if upper != nil && bytes.Compare(key, upper) >= 0 {
				return result, nil
			}

			result = append(result, Entry{bytes.Clone(key), bytes.Clone(n.Values[i])})
			if limit > 0 && len(result) >= limit {
				return result, nil
			}
		}

		if n.Next == "" {
			return result, nil
		}

		prev := n.address

		n, err = o.load(n.Next)
		if err != nil {
			return nil, err
		}

		if !n.Leaf || n.Prev != prev {
			return nil, fmt.Errorf("%w: leaf linkage", ErrCorrupt)
		}
	}
}

// Put replaces an existing key or inserts it. Page read/modify/write and any
// split are one transaction; no extra Flush or transaction is nested inside it.
func (t *Tree) Put(c *effects.Context, key, value []byte) (err error) {
	c.BeginTx()
	sp := c.TakeSavepoint()

	defer func() {
		if err != nil {
			c.RestoreSavepoint(sp)
		}
	}()

	o, err := t.operation(c)
	if err != nil {
		return err
	}

	n, err := o.find(key)
	if err != nil {
		return err
	}

	i, exists := slices.BinarySearchFunc(n.Keys, key, bytes.Compare)
	if exists {
		n.Values[i] = bytes.Clone(value)
	} else {
		n.Keys = slices.Insert(n.Keys, i, bytes.Clone(key))
		n.Values = slices.Insert(n.Values, i, bytes.Clone(value))
	}

	n.dirty = true
	if !exists && i == 0 {
		if err := o.propagateMin(n, n.Keys[0]); err != nil {
			return err
		}
	}

	for len(n.Keys) > o.opts.MaxKeys {
		n, err = o.split(n)
		if err != nil {
			return err
		}
	}

	return o.emit()
}

func (o *operation) propagateMin(n *node, minimum []byte) error {
	seen := make(map[string]bool)
	for n.Parent != "" {
		if seen[n.address] {
			return fmt.Errorf("%w: parent cycle", ErrCorrupt)
		}

		seen[n.address] = true

		parent, err := o.load(n.Parent)
		if err != nil {
			return err
		}

		i := slices.Index(parent.Children, n.address)
		if i < 0 {
			return fmt.Errorf("%w: missing parent link", ErrCorrupt)
		}

		if i > 0 {
			parent.Keys[i-1] = bytes.Clone(minimum)
			parent.dirty = true

			return nil
		}

		n = parent
	}

	return nil
}

func (o *operation) reparent(n *node) error {
	for _, address := range n.Children {
		child, err := o.load(address)
		if err != nil {
			return err
		}

		child.Parent = n.address
		child.dirty = true
	}

	return nil
}

func (o *operation) split(n *node) (*node, error) {
	if n.Splits == ^uint64(0) {
		return nil, errors.New("bplus: split lineage exhausted")
	}

	n.Splits++
	lineage := n.region + "/" + strconv.FormatUint(n.Splits, 10)
	left := n

	root := n.Parent == ""
	if root {
		left = o.newPage(lineage+"/0", n.Leaf, n.address)
		left.Keys, left.Values, left.Children = n.Keys, n.Values, n.Children
	}

	rightRegion := lineage
	if root {
		rightRegion += "/1"
	}

	right := o.newPage(rightRegion, n.Leaf, left.Parent)

	var separator []byte

	if n.Leaf {
		mid := len(left.Keys) / 2
		right.Keys = slices.Clone(left.Keys[mid:])
		right.Values = slices.Clone(left.Values[mid:])
		left.Keys = left.Keys[:mid:mid]
		left.Values = left.Values[:mid:mid]
		separator = bytes.Clone(right.Keys[0])
		right.Prev = left.address
		right.Next = left.Next

		left.Next = right.address
		if right.Next != "" {
			next, err := o.load(right.Next)
			if err != nil {
				return nil, err
			}

			next.Prev = right.address
			next.dirty = true
		}
	} else {
		mid := len(left.Children) / 2
		separator = bytes.Clone(left.Keys[mid-1])
		right.Keys = slices.Clone(left.Keys[mid:])
		right.Children = slices.Clone(left.Children[mid:])
		left.Keys = left.Keys[: mid-1 : mid-1]
		left.Children = left.Children[:mid:mid]

		if err := o.reparent(right); err != nil {
			return nil, err
		}
	}

	left.dirty = true
	if root {
		if err := o.reparent(left); err != nil {
			return nil, err
		}

		n.page = page{Keys: [][]byte{separator}, Children: []string{left.address, right.address}, Splits: n.Splits}
		n.dirty = true

		return n, nil
	}

	parent, err := o.load(n.Parent)
	if err != nil {
		return nil, err
	}

	i := slices.Index(parent.Children, n.address)
	if i < 0 {
		return nil, fmt.Errorf("%w: split parent", ErrCorrupt)
	}

	parent.Children = slices.Insert(parent.Children, i+1, right.address)
	parent.Keys = slices.Insert(parent.Keys, i, separator)
	parent.dirty = true

	return parent, nil
}

func (o *operation) emit() error {
	addresses := make([]string, 0, len(o.nodes))
	for address, n := range o.nodes {
		if n.dirty {
			addresses = append(addresses, address)
		}
	}

	slices.Sort(addresses) // deterministic effect order; topology refs are not deps

	for _, address := range addresses {
		n := o.nodes[address]

		deps := effects.GraphTip{Key: o.tree.key, Tips: n.tips, GraphTips: o.descriptor}
		if n.fresh && n.deleted {
			continue
		}

		if n.fresh {
			if err := o.ctx.EmitGraph(deps, &pb.Effect{Key: []byte(address), IKey: []byte(address), Virtual: []byte(pb.GraphRegionsKey), Kind: &pb.Effect_Data{Data: &pb.DataEffect{
				Op: pb.EffectOp_INSERT_OP, Collection: pb.CollectionKind_KEYED, Id: []byte(RegionLabel), Value: &pb.DataEffect_Raw{Raw: []byte(n.region)},
			}}}); err != nil {
				return err
			}
		}

		data := &pb.DataEffect{Op: pb.EffectOp_REMOVE_OP, Collection: pb.CollectionKind_SCALAR}

		if !n.deleted {
			raw, err := encodePage(&n.page)
			if err != nil {
				return err
			}

			data.Op = pb.EffectOp_INSERT_OP
			data.Value = &pb.DataEffect_Raw{Raw: raw}
		}

		if err := o.ctx.EmitGraph(deps, &pb.Effect{Key: []byte(address), IKey: []byte(address), Kind: &pb.Effect_Data{Data: data}}); err != nil {
			return err
		}
	}

	return nil
}
