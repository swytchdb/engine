package graph

import (
	"errors"
	"fmt"
	"iter"
	"slices"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/engine/keytrie"
)

// ErrAlreadyExists reports an existing value where graph creation requires absence.
// CreateGraph itself does not perform an existence check.
var ErrAlreadyExists = errors.New("already exists")

// Direction selects one of a node's independent ordered relationship lists.
type Direction int

const (
	Left Direction = iota
	Right
	Up
	Down
)

// Graph holds one observed descriptor and the node builders used by an operation.
// GetNode caches each address with its concrete value-builder type. CreateNode
// registers a new node from a local name. CreateNode encodes that name with the
// descriptor key; GetNode and relationship references use complete internal
// addresses, as returned by Node.Address.
//
// Creation has two phases: emit and commit the descriptor, then read its snapshot
// and tips and load a fresh Graph before adding nodes. CreateGraph's builder has
// no observed descriptor tips; it cannot emit nodes.
//
// Use the descriptor and node observations from the same context/read view.
// Relationship addresses are data, not causal dependencies. GraphTip.Tips tracks
// the observed node; GraphTip.GraphTips tracks the observed descriptor.
//
// Flush transfers pending effects into a context; the caller owns Context.Flush,
// Abort, savepoints, and transaction boundaries. Builders and their aliases are
// consumed on emission, including errors. Reload the graph after publication or
// abort. Graph and its builders are not safe for concurrent use.
type Graph struct {
	description Descriptor
	pending     map[string]EmitsGraph
}

// CreateNode registers a typed value at the graph-local name address without
// reading storage. It converts address with keytrie.InternalKey; do not pass an
// already encoded node address. The graph must have been loaded with the tips
// of a committed descriptor before its nodes can be emitted. The node's GraphTip
// has no node tips and retains that observed descriptor frontier.
//
// This method does not check whether the address already exists or merge an
// existing pending node. Reusing a name replaces its pending builder and edits.
func (g *Graph) CreateNode[T Value](address string, value T) *Node[T] {
	gt := effects.GraphTip{
		Key:       g.description.getKey(),
		Tips:      nil,
		GraphTips: g.description.getTips(),
	}

	address = keytrie.InternalKey(g.description.getKey(), address)

	n := (&Node[T]{tips: gt, address: Address(address), fields: &NodeFields[T]{}}).SetValue(value)

	if g.pending == nil {
		g.pending = make(map[string]EmitsGraph)
	}

	g.pending[address] = n

	return n
}

// Link appends a reference from from to to in dir. Each call creates a distinct
// slot, even for the same address. It changes only from, without a reciprocal
// reference. An invalid direction is a no-op. Both nodes
// should come from this Graph so the changed builder is included in Flush.
func (g *Graph) Link[T, U Value](from *Node[T], to *Node[U], dir Direction) {
	if links := from.links(dir); links != nil {
		links.Append(string(to.address))
	}
}

// Unlink removes every currently observed slot referring to to in from's dir
// list. Removals target slot IDs, leaving unobserved concurrent appends intact.
// It does not delete the target or change reciprocal links. An absent reference
// or invalid direction is a no-op. Use UnlinkAt to remove just one slot.
func (g *Graph) Unlink[T, U Value](from *Node[T], to *Node[U], dir Direction) {
	if links := from.links(dir); links != nil {
		addresses := links.Values()
		for i, addresse := range slices.Backward(addresses) {
			if addresse == string(to.address) {
				links.Remove(i)
			}
		}
	}
}

// UnlinkAt removes the slot at index in the current local relationship list.
// It captures the slot's ID, so later or concurrent appends are not removed.
// The target node and reciprocal links remain unchanged. An invalid direction
// is a no-op; an out-of-range index panics, matching ListField.Remove.
func (g *Graph) UnlinkAt[T Value](from *Node[T], dir Direction, index int) {
	if links := from.links(dir); links != nil {
		links.Remove(index)
	}
}

// AttachChild links from to to in Down. If reciprocal is true it also links to
// to from in Up. The caller chooses the transaction boundary for the two changes.
func (g *Graph) AttachChild[T, U Value](from *Node[T], to *Node[U], reciprocal bool) {
	g.Link(from, to, Down)

	if reciprocal {
		g.Link(to, from, Up)
	}
}

// DetachChild removes the observed Down references from from to to and Up
// references from to to from. Neither node is deleted. Missing references are
// no-ops; the caller chooses the publication boundary.
func (g *Graph) DetachChild[T, U Value](from *Node[T], to *Node[U]) {
	g.Unlink(from, to, Down)
	g.Unlink(to, from, Up)
}

// NeighborAddresses evaluates the selected list's snapshot and pending effects,
// returning addresses in live slot order, including duplicates, and their count.
// It never sorts, reconciles, or removes links. An invalid direction returns an
// empty sequence. No node is loaded or emitted, and the returned addresses are
// not added to causal dependencies.
func (g *Graph) NeighborAddresses[T Value](
	from *Node[T],
	direction Direction,
) (iter.Seq[string], int) {
	if links := from.links(direction); links != nil {
		addresses := links.Values()

		return slices.Values(addresses), len(addresses)
	}

	return func(yield func(string) bool) {}, 0
}

// GetNode returns the pending typed node at from, or reads its snapshot and tips
// from c and seeds its generated body using typ. A cached node of another concrete
// T returns an error without rereading storage. Read and type-factory errors propagate.
//
// The node's GraphTip combines the descriptor's stored frontier with the tips
// returned by this exact node read. Value lazily binds the runtime implementation;
// a missing snapshot seeds an empty value without queuing effects. The observed
// absence tips are retained.
func (g *Graph) GetNode[T Value](from string, c *effects.Context, typ Type[T]) (*Node[T], error) {
	if g.pending == nil {
		g.pending = make(map[string]EmitsGraph)
	}

	if n, ok := g.pending[from]; ok {
		typed, ok := n.(*Node[T])
		if !ok {
			return nil, fmt.Errorf(
				"node %q: incompatible type: have %T, requested %T",
				from, n, (*Node[T])(nil),
			)
		}

		return typed, nil
	}

	snap, tips, err := c.GetSnapshotInternal(from)
	if err != nil {
		return nil, err
	}

	tip := effects.GraphTip{
		Key:       g.description.getKey(),
		Tips:      tips,
		GraphTips: g.description.getTips(),
	}

	n, err := FromSnap(snap, tip, typ)
	if err != nil {
		return nil, err
	}

	n.address = Address(from)
	// track pending node
	g.pending[from] = n

	return n, nil
}

// GetNeighbors loads the selected neighbor addresses through GetNode using c and
// typ. Results follow live slot order, including duplicates. It stops at the first
// error; nodes loaded before that error remain in the pending cache.
func (g *Graph) GetNeighbors[T, U Value](from *Node[T], c *effects.Context, direction Direction, typ Type[U]) ([]*Node[U], error) {
	neighbors, s := g.NeighborAddresses(from, direction)

	nodes := make([]*Node[U], 0, s)

	for address := range neighbors {
		n, err := g.GetNode(address, c, typ)
		if err != nil {
			return nil, err
		}

		nodes = append(nodes, n)
	}

	return nodes, nil
}

// Flush emits the descriptor, then pending nodes, into c without flushing or
// committing c. Node iteration order is unspecified. Success clears the pending
// node map; failure retains it, but earlier effects may already be in c and their
// builders must be treated as consumed. Abort or restore a savepoint on failure.
//
// For a new graph, emit only the descriptor, commit with Context.Flush, then
// reload through GraphFromSnapshot before adding nodes. Emitting the descriptor
// here does not make its pending effects an observed descriptor frontier.
// Nodes retain the descriptor observation captured when created or loaded.
func (g *Graph) Flush(c *effects.Context) error {
	err := g.description.Emit(g.description.getKey(), c, g.description.getTips())
	if err != nil {
		return err
	}

	for _, node := range g.pending {
		err := node.EmitGraphNode(c)
		if err != nil {
			return err
		}
	}

	g.pending = nil

	return nil
}

// GetDescriptor returns the shared descriptor builder. Entry-point edits through
// it are included in the next Flush.
func (g *Graph) GetDescriptor() Descriptor {
	return g.description
}

// CreateGraph queues descriptor initialization at a logical application key.
// It does not read storage or check existence. Commit the descriptor before
// attaching nodes: call Graph.Flush to emit it, then Context.Flush to commit it.
// In the subsequent operation, read the descriptor with Context.GetSnapshot and
// pass its snapshot and tips to GraphFromSnapshot. Add nodes to that fresh graph.
//
// The creation builder has no observed descriptor tips and is consumed by
// emission. It must not be reused for node writes after the descriptor commits.
func CreateGraph(onKey string, algorithm string) *Graph {
	d := NewDescriptor(onKey, algorithm)

	return &Graph{
		description: d,
		pending:     nil,
	}
}

// GraphFromSnapshot borrows desc and tips from a committed descriptor read at
// logical key. Use it after creating and committing the descriptor, and for each
// subsequent graph operation. The snapshot seeds builders; it is not emitted.
// Keep the snapshot and tips immutable and load nodes in the same context/read
// view. Supplying an empty frontier does not authorize node emission.
func GraphFromSnapshot(key string, desc *pb.ReducedEffect, tips []effects.Tip) (*Graph, error) {
	d, err := DescriptorFromSnap(key, desc, tips)
	if err != nil {
		return nil, err
	}

	return &Graph{
		description: d,
		pending:     map[string]EmitsGraph{},
	}, nil
}

// GraphFromDescriptor wraps a shared descriptor builder with an empty pending
// node cache. It does not clone builders or establish a new read observation.
// Node writes require a descriptor loaded with committed tips; wrapping a newly
// constructed descriptor does not bypass the separate descriptor commit.
func GraphFromDescriptor(key Descriptor) *Graph {
	return &Graph{
		description: key,
		pending:     map[string]EmitsGraph{},
	}
}
