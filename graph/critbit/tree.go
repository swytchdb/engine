// Package critbit implements a binary-key crit-bit tree over graph nodes.
package critbit

import (
	"crypto/rand"
	"errors"
	"fmt"
	"slices"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/engine/graph"
	"github.com/swytchdb/engine/graph/critbit/meta"
	"github.com/swytchdb/engine/keytrie"
)

const Algorithm = "critbit/v1"

// ErrIncomplete reports a missing or malformed routing node. A reader can retry
// with a fresh context if another writer's multi-node publication is in progress.
var ErrIncomplete = errors.New("incomplete critbit tree")

// Crit holds one graph observation and its pending edits. Routing nodes use
// Left/Right lists; leaves hold payload nodes in their Down list. Readers follow
// the last live slot without reconciliation. Writers reconcile observed routing
// alternatives and replace observed payload slots. No transactions are started.
//
// The Type supplies the runtime implementation of payloads, including generated
// or handwritten composites. Flush queues effects; the caller commits the context.
// After emission, reload with Open. Crit and its aliases are not concurrency-safe.
type Crit[T graph.Value] struct {
	*graph.Tree

	ctx      *effects.Context
	typ      graph.Type[T]
	root     *CritNode
	key      string
	branches map[string]*CritNode
}

type Direction = int

const (
	Left  = 0
	Right = 1
)

type CritNode = graph.Node[*meta.CritMeta]

// Create queues the descriptor and its stable root entry. To persist a new tree,
// Flush and commit this descriptor first, then Open in a fresh operation before
// inserting nodes. Local Insert/Find also work before emission. typ.New must
// construct a fresh empty value, as required by graph.Type.
func Create[T graph.Value](key string, typ graph.Type[T]) *Crit[T] {
	g := graph.CreateGraph(key, Algorithm)
	g.GetDescriptor().AddEntryPoint("root", keytrie.InternalKey(key, "root"))

	return &Crit[T]{Tree: &graph.Tree{Graph: g}, typ: typ, key: key}
}

// Open reads an existing descriptor and retains ctx for lazy node loading. It
// queues no effects and does not create a missing tree or change transaction mode.
func Open[T graph.Value](key string, ctx *effects.Context, typ graph.Type[T]) (*Crit[T], error) {
	if typ.New == nil {
		return nil, errors.New("runtime Type.New factory is required")
	}

	snap, tips, err := ctx.GetSnapshot(key)
	if err != nil {
		return nil, err
	}

	if snap.GetTypeTag() != pb.ValueType_TYPE_GRAPH || string(snap.GetNetAdds()[pb.GraphAlgorithmKey].GetData().Decompress()) != Algorithm {
		return nil, fmt.Errorf("%q is not a %s graph", key, Algorithm)
	}

	g, err := graph.GraphFromSnapshot(key, snap, tips)
	if err != nil {
		return nil, err
	}

	if len(g.GetDescriptor().GetEntryPoint("root")) == 0 {
		return nil, fmt.Errorf("%w: missing root entry", ErrIncomplete)
	}

	return &Crit[T]{Tree: &graph.Tree{Graph: g}, ctx: ctx, typ: typ, key: key}, nil
}

// The root is a stable sentinel. Its Left list holds the actual tree roots, so
// replacing a root uses ordered slots instead of overwriting a descriptor scalar.
func (c *Crit[T]) getRoot(write bool) (*CritNode, error) {
	if c.root != nil {
		return c.root, nil
	}

	if c.ctx == nil {
		if !write {
			return nil, nil
		}

		c.root = c.CreateNode("root", meta.NewCritMeta())

		return c.root, nil
	}

	root, err := c.GetNode(string(c.GetDescriptor().GetEntryPoint("root")), c.ctx, meta.CritMetaType)
	if err != nil {
		return nil, err
	}

	c.root = root

	return root, nil
}

func graphDirection(direction Direction) graph.Direction {
	if direction == Left {
		return graph.Left
	}

	return graph.Right
}

// otherBits == 0 tests byte presence, before that byte's eight data bits. This
// distinguishes every prefix, including an empty key or a trailing zero byte.
// Ordinary critical-bit masks are 0x7f, 0xbf, ..., 0xfe, never zero.
func (c *Crit[T]) getDirection(key string, bytePos uint32, otherBits uint8) Direction {
	if uint64(bytePos) >= uint64(len(key)) {
		return Left
	}

	if otherBits == 0 {
		return Right
	}

	return int(1+uint16(otherBits|key[bytePos])) >> 8
}

func (c *Crit[T]) highestBit(b byte) uint8 {
	b |= b >> 1
	b |= b >> 2
	b |= b >> 4

	return b ^ (b >> 1)
}

func (c *Crit[T]) findCritBit(a, b string) (uint32, uint8) {
	for i := 0; i < min(len(a), len(b)); i++ {
		if a[i] != b[i] {
			return uint32(i), c.highestBit(a[i]^b[i]) ^ 0xff
		}
	}

	return uint32(min(len(a), len(b))), 0
}

func (c *Crit[T]) shouldInsertBefore(aPos uint32, aBits uint8, bPos uint32, bBits uint8) bool {
	return aPos < bPos || aPos == bPos && aBits < bBits
}

func (c *Crit[T]) addresses(node *CritNode, direction graph.Direction) []string {
	addresses, _ := c.NeighborAddresses(node, direction)

	return slices.Collect(addresses)
}

func (c *Crit[T]) loadRouting(address string) (*CritNode, error) {
	node, err := c.GetNode(address, c.ctx, meta.CritMetaType)
	if err != nil {
		return nil, err
	}

	if node.Value().Snapshot() == nil {
		return nil, fmt.Errorf("%w: missing routing node %q", ErrIncomplete, address)
	}

	return node, nil
}

func (c *Crit[T]) child(node *CritNode, direction graph.Direction, write bool) (*CritNode, error) {
	addresses := c.addresses(node, direction)
	if len(addresses) == 0 {
		return nil, nil
	}

	if write && len(addresses) > 1 {
		return c.reconcile(node, direction, addresses)
	}

	return c.loadRouting(addresses[len(addresses)-1])
}

func (c *Crit[T]) findBestMatch(root *CritNode, key string, write bool) (*CritNode, error) {
	node, err := c.child(root, graph.Left, write)
	if err != nil {
		return nil, err
	}

	seen := make(map[string]bool)

	for node != nil && !node.Value().IsLeaf() {
		address := string(node.Address())
		if seen[address] {
			return nil, fmt.Errorf("%w: routing cycle", ErrIncomplete)
		}

		seen[address] = true

		next, nextErr := c.child(node, graphDirection(c.getDirection(key, node.Value().BytePos(), node.Value().OtherBits())), write)
		if nextErr != nil {
			return nil, nextErr
		}

		if next == nil {
			return nil, fmt.Errorf("%w: branch without child", ErrIncomplete)
		}

		node = next
	}

	return node, nil
}

// Find reads the key's deterministic leaf address and its last live Down slot,
// without discovering routing nodes. A missing or deleted key returns T's zero
// value and nil. A stored leaf is readable even while its routing path competes
// with another path. Reading never reconciles; mutations through the returned
// runtime value participate in the next Flush.
func (c *Crit[T]) Find(key string) (T, error) {
	value, _, err := c.findDirect(key)

	return value, err
}

func (c *Crit[T]) findDirect(key string) (T, bool, error) {
	var zero T

	leaf, err := c.GetNode(keytrie.InternalKey(c.key, "leaf/"+key), c.ctx, meta.CritMetaType)
	if err != nil {
		return zero, false, err
	}

	value := leaf.Value()
	if value.Snapshot() == nil {
		return zero, false, nil
	}

	if !value.IsLeaf() || value.Key() != key {
		return zero, false, fmt.Errorf("%w: invalid leaf for %q", ErrIncomplete, key)
	}

	return c.leafPayload(leaf)
}

func (c *Crit[T]) leafPayload(leaf *CritNode) (T, bool, error) {
	var zero T

	addresses := c.addresses(leaf, graph.Down)
	if len(addresses) == 0 {
		return zero, false, nil
	}

	node, err := c.GetNode(addresses[len(addresses)-1], c.ctx, c.typ)
	if err != nil {
		return zero, false, err
	}

	return node.Value(), true, nil
}

// Entry is a key and its retained runtime payload. Value aliases the tree's
// builder: edits participate in Flush, and emission consumes that alias too.
type Entry[T graph.Value] struct {
	Key   string
	Value T
}

// Scan returns live entries in [lower, upper) in ascending byte order, including
// pending edits. An empty bound is unbounded; an empty lower includes the empty key.
// limit <= 0 means no result limit. Deleted leaves do not count toward the limit.
// It follows the last live routing and Down slots and skips leaves without
// payload slots. Earlier alternatives remain untouched; scanning
// never reconciles or queues graph mutations. Values retain their runtime identity.
//
// The lower-bound seek and subsequent traversal visit O(h + k) routing nodes,
// where h is tree height and k is the number of leaves examined (including
// tombstones). Skipped prefixes are checked against a representative leaf.
// A live exact lower key is included by direct lookup, as in Find. With limit == 1
// this bypasses routing entirely; longer scans seek to discover subsequent keys.
//
// An empty tree returns nil, nil. Read errors and incomplete routing return nil
// and an error, without partial results. The context supplies each node's read
// view; Scan does not establish an atomic snapshot of a multi-node publication.
func (c *Crit[T]) Scan(lower, upper string, limit int) ([]Entry[T], error) {
	if upper != "" && lower >= upper {
		return nil, nil
	}

	value, direct, err := c.findDirect(lower)
	if err != nil {
		return nil, err
	}

	var entries []Entry[T]
	if direct {
		entries = append(entries, Entry[T]{Key: lower, Value: value})
		if limit == 1 {
			return entries, nil
		}
	}

	root, err := c.getRoot(false)
	if err != nil {
		return nil, err
	}

	if root == nil {
		return entries, nil
	}

	roots := c.addresses(root, graph.Left)
	if len(roots) == 0 {
		return entries, nil
	}

	stack, err := c.seekLower(roots[len(roots)-1], lower)
	if err != nil {
		return nil, err
	}

	seen := make(map[string]bool)

	for len(stack) > 0 {
		address := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if seen[address] {
			return nil, fmt.Errorf("%w: repeated routing node %q", ErrIncomplete, address)
		}

		seen[address] = true

		node, err := c.loadRouting(address)
		if err != nil {
			return nil, err
		}

		if node.Value().IsLeaf() {
			key := node.Value().Key()
			if upper != "" && key >= upper {
				return entries, nil
			}

			if direct && key == lower {
				continue
			}

			value, found, err := c.leafPayload(node)
			if err != nil {
				return nil, err
			}

			if !found {
				continue
			}

			entries = append(entries, Entry[T]{Key: key, Value: value})
			if limit > 0 && len(entries) >= limit {
				return entries, nil
			}

			continue
		}
		// Push right first so the left subtree is visited first. Presence tests
		// put a prefix before its extensions; data-bit tests order unsigned bytes.
		for _, direction := range []graph.Direction{graph.Right, graph.Left} {
			children := c.addresses(node, direction)
			if len(children) == 0 {
				return nil, fmt.Errorf("%w: branch without child", ErrIncomplete)
			}

			stack = append(stack, children[len(children)-1])
		}
	}

	return entries, nil
}

// seekLower returns an ordered traversal frontier, with the next subtree at the
// end of the stack. Critical bits alone do not compare compressed prefixes:
// first follow lower to a representative leaf and retain that search path.
// Their first differing bit identifies the subtree in which an insertion of
// lower would split the path. Every key in that subtree lies on the same side
// of lower, so include it whole or skip it whole. Earlier left turns retain
// their unexplored right siblings, which all sort after lower.
func (c *Crit[T]) seekLower(address, lower string) ([]string, error) {
	if lower == "" {
		return []string{address}, nil
	}

	var path []*CritNode

	seen := make(map[string]bool)
	for {
		if seen[address] {
			return nil, fmt.Errorf("%w: routing cycle", ErrIncomplete)
		}

		seen[address] = true

		node, err := c.loadRouting(address)
		if err != nil {
			return nil, err
		}

		path = append(path, node)
		if node.Value().IsLeaf() {
			break
		}

		direction := graphDirection(c.getDirection(lower, node.Value().BytePos(), node.Value().OtherBits()))

		children := c.addresses(node, direction)
		if len(children) == 0 {
			return nil, fmt.Errorf("%w: branch without child", ErrIncomplete)
		}

		address = children[len(children)-1]
	}

	key := path[len(path)-1].Value().Key()
	pos, bits := c.findCritBit(lower, key)

	var stack []string

	for _, node := range path {
		value := node.Value()
		if value.IsLeaf() || key != lower && !c.shouldInsertBefore(value.BytePos(), value.OtherBits(), pos, bits) {
			if key >= lower {
				stack = append(stack, string(node.Address()))
			}

			break
		}

		if c.getDirection(lower, value.BytePos(), value.OtherBits()) == Left {
			right := c.addresses(node, graph.Right)
			if len(right) == 0 {
				return nil, fmt.Errorf("%w: branch without child", ErrIncomplete)
			}

			stack = append(stack, right[len(right)-1])
		}
	}

	return stack, nil
}

// Insert composes value into a fresh payload node. It appends that node to the
// leaf's Down list before removing the observed older slots. Concurrent unseen
// appends remain distinct. It reconciles competing routing paths encountered by
// this write, without starting or committing a transaction. Values follow graph
// composition ownership rules; supplying an already attached value is invalid.
func (c *Crit[T]) Insert(key string, value T) error {
	if uint64(len(key)) > uint64(^uint32(0)) {
		return errors.New("critbit key exceeds uint32 byte positions")
	}

	root, err := c.getRoot(true)
	if err != nil {
		return err
	}

	best, err := c.findBestMatch(root, key, true)
	if err != nil {
		return err
	}

	if best != nil && best.Value().Key() == key {
		c.replaceValue(best, value)

		return nil
	}

	if best == nil {
		leaf := c.CreateNode("leaf/"+key, meta.NewCritMeta().SetIsLeaf(true).SetKey(key))
		c.replaceValue(leaf, value)
		c.Link(root, leaf, graph.Left)

		return nil
	}

	pos, bits := c.findCritBit(key, best.Value().Key())
	parent, direction := root, graph.Left

	node, err := c.child(parent, direction, true)
	if err != nil {
		return err
	}

	for node != nil && !node.Value().IsLeaf() && c.shouldInsertBefore(node.Value().BytePos(), node.Value().OtherBits(), pos, bits) {
		parent = node
		direction = graphDirection(c.getDirection(key, node.Value().BytePos(), node.Value().OtherBits()))

		node, err = c.child(parent, direction, true)
		if err != nil {
			return err
		}
	}

	if node == nil {
		return fmt.Errorf("%w: insertion path lost its child", ErrIncomplete)
	}

	leaf := c.CreateNode("leaf/"+key, meta.NewCritMeta().SetIsLeaf(true).SetKey(key))
	c.replaceValue(leaf, value)

	branch, err := c.branch(key, pos, bits)
	if err != nil {
		return err
	}

	newDirection := graphDirection(c.getDirection(key, pos, bits))

	oldDirection := graph.Left
	if newDirection == graph.Left {
		oldDirection = graph.Right
	}

	c.Link(branch, leaf, newDirection)
	c.Link(branch, node, oldDirection)
	c.replaceEdge(parent, direction, branch)

	return nil
}

func (c *Crit[T]) replaceValue(leaf *CritNode, value T) {
	_, count := c.NeighborAddresses(leaf, graph.Down)
	node := c.CreateNode("value/"+rand.Text(), value)
	c.Link(leaf, node, graph.Down)

	for i := count - 1; i >= 0; i-- {
		c.UnlinkAt(leaf, graph.Down, i)
	}
}

// Delete removes the observed payload slots for key, returning whether any were
// present. Routing leaves remain as reusable tombstones, so competing old paths
// still observe the deletion. Unobserved concurrent payload appends survive.
// Detached payload and branch nodes are retained; this API does not reclaim them.
func (c *Crit[T]) Delete(key string) (bool, error) {
	root, err := c.getRoot(false)
	if err != nil || root == nil {
		return false, err
	}

	leaf, err := c.findBestMatch(root, key, true)
	if err != nil || leaf == nil {
		return false, err
	}

	if leaf.Value().Key() != key {
		return false, nil
	}

	_, count := c.NeighborAddresses(leaf, graph.Down)
	for i := count - 1; i >= 0; i-- {
		c.UnlinkAt(leaf, graph.Down, i)
	}

	return count > 0, nil
}

func (c *Crit[T]) replaceEdge(parent *CritNode, direction graph.Direction, replacement *CritNode) {
	c.replaceRoutingEdge(parent, direction, string(replacement.Address()))
}

// Relationships carry addresses, so installing a merged edge need not load its
// target. Append before removing observed slots; an unchanged edge emits nothing.
func (c *Crit[T]) replaceRoutingEdge(parent *CritNode, direction graph.Direction, replacement string) {
	addresses := c.addresses(parent, direction)
	if len(addresses) == 1 && addresses[0] == replacement {
		return
	}

	if direction == graph.Left {
		parent.AttachLeft(graph.Address(replacement))
	} else {
		parent.AttachRight(graph.Address(replacement))
	}

	for i := range slices.Backward(addresses) {
		c.UnlinkAt(parent, direction, i)
	}
}

// reconcile merges only differing routing paths. Equal addresses are convergence
// points: their contents and any internal conflicts remain undiscovered until a
// writer visits them. No leaves are collected, sorted, or copied.
func (c *Crit[T]) reconcile(parent *CritNode, direction graph.Direction, addresses []string) (*CritNode, error) {
	known := make(map[string]*CritNode)
	active := make(map[[2]string]bool)
	load := func(address string) (*CritNode, error) {
		if node := known[address]; node != nil {
			return node, nil
		}

		node, err := c.loadRouting(address)
		if err == nil {
			known[address] = node
		}

		return node, err
	}
	contains := func(node *CritNode, address string) bool {
		if node == nil || node.Value().IsLeaf() {
			return false
		}

		return slices.Contains(c.addresses(node, graph.Left), address) ||
			slices.Contains(c.addresses(node, graph.Right), address)
	}

	var merge func(string, string) (string, error)
	// An already referenced subtree is shared, even if other slots coexist in
	// this child list. Those conflicts belong to the child traversal, not here.
	mergeInto := func(node *CritNode, dir graph.Direction, incoming string) error {
		children := c.addresses(node, dir)
		if slices.Contains(children, incoming) {
			return nil
		}

		replacement := incoming

		for _, child := range children {
			var err error

			replacement, err = merge(replacement, child)
			if err != nil {
				return err
			}
		}

		c.replaceRoutingEdge(node, dir, replacement)

		return nil
	}
	merge = func(a, b string) (string, error) {
		if a == b {
			return a, nil
		}

		pair := [2]string{min(a, b), max(a, b)}
		if active[pair] {
			return "", fmt.Errorf("%w: routing cycle", ErrIncomplete)
		}

		active[pair] = true
		defer delete(active, pair)

		// A known wrapper may already reference the other subtree. Check this
		// before loading that subtree, including when it is not locally present.
		if contains(known[a], b) {
			return a, nil
		}

		if contains(known[b], a) {
			return b, nil
		}

		left, err := load(a)
		if err != nil {
			return "", err
		}

		if contains(left, b) {
			return a, nil
		}

		right, err := load(b)
		if err != nil {
			return "", err
		}

		if contains(right, a) {
			return b, nil
		}

		aKey, err := c.routingPrefix(left)
		if err != nil {
			return "", err
		}

		bKey, err := c.routingPrefix(right)
		if err != nil {
			return "", err
		}

		av, bv := left.Value(), right.Value()
		pos, bits := c.findCritBit(aKey, bKey)
		// A difference before both branch points means disjoint prefixes.
		// Join their existing roots under the canonical common branch.
		if aKey != bKey &&
			(av.IsLeaf() || c.shouldInsertBefore(pos, bits, av.BytePos(), av.OtherBits())) &&
			(bv.IsLeaf() || c.shouldInsertBefore(pos, bits, bv.BytePos(), bv.OtherBits())) {
			node, err := c.branch(aKey, pos, bits)
			if err != nil {
				return "", err
			}

			known[string(node.Address())] = node
			if err := mergeInto(node, graphDirection(c.getDirection(aKey, pos, bits)), a); err != nil {
				return "", err
			}

			if err := mergeInto(node, graphDirection(c.getDirection(bKey, pos, bits)), b); err != nil {
				return "", err
			}

			return string(node.Address()), nil
		}
		// One prefix contains the other. Only that side can differ; leave the
		// opposite child unopened and preserve the containing branch's identity.
		if !av.IsLeaf() && (bv.IsLeaf() || c.shouldInsertBefore(av.BytePos(), av.OtherBits(), bv.BytePos(), bv.OtherBits())) {
			if err := mergeInto(left, graphDirection(c.getDirection(bKey, av.BytePos(), av.OtherBits())), b); err != nil {
				return "", err
			}

			return a, nil
		}

		if !bv.IsLeaf() && (av.IsLeaf() || c.shouldInsertBefore(bv.BytePos(), bv.OtherBits(), av.BytePos(), av.OtherBits())) {
			if err := mergeInto(right, graphDirection(c.getDirection(aKey, bv.BytePos(), bv.OtherBits())), a); err != nil {
				return "", err
			}

			return b, nil
		}

		return "", fmt.Errorf("%w: duplicate routing identity", ErrIncomplete)
	}

	replacement := addresses[0]
	for _, address := range addresses[1:] {
		var err error

		replacement, err = merge(replacement, address)
		if err != nil {
			return nil, err
		}
	}

	c.replaceRoutingEdge(parent, direction, replacement)

	return c.loadRouting(replacement)
}

// routingPrefix uses only this node's metadata. Branches store the common prefix
// preceding their critical bit in Key; leaves store their complete key.
func (c *Crit[T]) routingPrefix(node *CritNode) (string, error) {
	value := node.Value()

	prefix := value.Key()
	if !value.IsLeaf() {
		length := uint64(value.BytePos())
		if value.OtherBits() != 0 {
			length++
		}

		if uint64(len(prefix)) != length || string(node.Address()) != keytrie.InternalKey(c.key, branchName(prefix, value.BytePos(), value.OtherBits())) {
			return "", fmt.Errorf("%w: invalid branch prefix", ErrIncomplete)
		}
	}

	return prefix, nil
}

// branchName identifies a critical prefix independently of insertion order.
// Data-bit prefixes include only bits above the critical bit; presence tests
// include only preceding bytes. Decimal position/mask delimit the raw suffix.
func branchName(key string, pos uint32, bits uint8) string {
	return fmt.Sprintf("branch/%d/%d/%s", pos, bits, branchPrefix(key, pos, bits))
}

func branchPrefix(key string, pos uint32, bits uint8) string {
	prefix := key[:pos]

	if bits != 0 {
		bit := uint16(^bits)
		prefix += string([]byte{key[pos] & byte(^(bit*2 - 1))})
	}

	return prefix
}

// Reusing critical-prefix identities keeps stale descendant writers reachable
// when another writer merges the surrounding routing. Removal targets only
// observed slots; unseen edits to these same branches survive consolidation.
func (c *Crit[T]) branch(key string, pos uint32, bits uint8) (*CritNode, error) {
	name := branchName(key, pos, bits)
	if c.ctx == nil {
		if node := c.branches[name]; node != nil {
			return node, nil
		}

		node := c.CreateNode(name, meta.NewCritMeta().SetIsLeaf(false).SetBytePos(pos).SetOtherBits(bits).SetKey(branchPrefix(key, pos, bits)))
		if c.branches == nil {
			c.branches = make(map[string]*CritNode)
		}

		c.branches[name] = node

		return node, nil
	}

	node, err := c.GetNode(keytrie.InternalKey(c.key, name), c.ctx, meta.CritMetaType)
	if err != nil {
		return nil, err
	}

	if node.Value().Snapshot() == nil {
		node.Value().SetIsLeaf(false).SetBytePos(pos).SetOtherBits(bits).SetKey(branchPrefix(key, pos, bits))
	}

	return node, nil
}
