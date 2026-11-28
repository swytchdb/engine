package graph

import (
	"fmt"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
)

// Address is an opaque internal node key. Its bytes must remain immutable while
// referenced by a node or pending relationship effect.
type Address []byte

// Node wraps a generated NodeFields value with an internal address and observed
// graph/node tips. Value, relationships, and regions share the generated body's
// effect sequence. Relationships do not imply reciprocal links or causal edges.
//
// Node itself is not an ordinary Value or Emits. EmitGraphNode publishes through
// the graph-aware context API. Emission consumes the body and all sharing values,
// even on error. Reload before further work; do not use aliases concurrently.
// The schema in node.effects defines the stored representation.
type Node[T Value] struct {
	tips    effects.GraphTip
	address Address
	fields  *NodeFields[T]
	inner   []*pb.Effect // Node deletion remains a graph-level operation, emitted last.
}

const (
	valueKey  = "value"
	regionKey = pb.GraphRegionsKey
	upKey     = "up"
	downKey   = "down"
	leftKey   = "left"
	rightKey  = "right"
)

// FromSnap seeds the generated node body from an immutable snapshot. typ supplies
// the empty runtime value bound lazily by Value. Loading and binding queue no
// effects; nil seeds an empty value while retaining the observed tips. The Graph
// layer assigns the address. A missing type factory returns an error.
func FromSnap[T Value](snap *pb.ReducedEffect, tips effects.GraphTip, typ Type[T]) (*Node[T], error) {
	if typ.New == nil {
		return nil, fmt.Errorf("runtime Type.New factory is required")
	}

	fields, err := NodeFieldsType(typ).FromSnapshot(snap)
	if err != nil {
		return nil, err
	}

	return &Node[T]{tips: tips, fields: fields}, nil
}

// EmitGraphNode addresses and publishes the generated body's pending effects in
// mutation order, then node-level operations such as Delete. It does not flush
// the context. Stored GraphTips must observe a committed descriptor. Snapshots
// are never emitted as data. Emission consumes the body and its aliases even
// when it fails; abort or restore a savepoint if rollback is required.
func (n *Node[T]) EmitGraphNode(c *effects.Context) error {
	for _, batch := range [][]*pb.Effect{n.fields.Runtime().getEffects(), n.inner} {
		for _, effect := range batch {
			effect.Key = n.address

			effect.IKey = n.address
			if err := c.EmitGraph(n.tips, effect); err != nil {
				return err
			}
		}
	}

	return nil
}

func (n *Node[T]) getInnerEffects() []*pb.Effect { return n.inner }

type EmitsGraph interface {
	EmitGraphNode(c *effects.Context) error
	getInnerEffects() []*pb.Effect
}

// Address returns the borrowed internal address. Do not modify its bytes.
func (n *Node[T]) Address() []byte { return n.address }

// Value returns the retained runtime value, binding it on first access after a
// load. Missing data reads as an empty value without queuing effects. Mutations
// through the returned reference participate in this node.
func (n *Node[T]) Value() T {
	return n.fields.Value()
}

// SetValue composes value into the generated body and returns the node. The
// value's pending operations are retained; its snapshot is not emitted. This
// is allowed after Delete but does not cancel the queued deletion. As with
// runtime composition, a value bound to another owner cannot be moved here;
// such a programming error panics because this fluent API has no error return.
func (n *Node[T]) SetValue(value T) *Node[T] {
	if err := n.fields.SetValue(value); err != nil {
		panic(err)
	}

	return n
}

// AttachUp appends a parent reference in a new slot, including repeated addresses.
// Only this node changes; address bytes are copied.
func (n *Node[T]) AttachUp(address Address) *Node[T] {
	n.fields.Up().Append(string(address))

	return n
}

// AttachDown appends a child reference in a new slot, including repeated addresses.
// Only this node changes; address bytes are copied.
func (n *Node[T]) AttachDown(address Address) *Node[T] {
	n.fields.Down().Append(string(address))

	return n
}

// AttachLeft appends a left reference in a new slot, including repeated addresses.
// Only this node changes; address bytes are copied.
func (n *Node[T]) AttachLeft(address Address) *Node[T] {
	n.fields.Left().Append(string(address))

	return n
}

// AttachRight appends a right reference in a new slot, including repeated addresses.
// Only this node changes; address bytes are copied.
func (n *Node[T]) AttachRight(address Address) *Node[T] {
	n.fields.Right().Append(string(address))

	return n
}

// SetRegion assigns a label in the generated region map. Node relationships and
// value are unaffected. Labels are also used by graph subscription routing.
func (n *Node[T]) SetRegion(tag, region string) *Node[T] {
	n.fields.Regions().Set(tag, region)

	return n
}

func (n *Node[T]) links(direction Direction) *ListField[string] {
	switch direction {
	case Up:
		return n.fields.Up()
	case Down:
		return n.fields.Down()
	case Left:
		return n.fields.Left()
	case Right:
		return n.fields.Right()
	default:
		return nil
	}
}

// Delete queues a root removal after all body effects. It removes this node's
// value and partitions, but does not remove incoming references or invalidate
// the address. Recreate it with a fresh node builder in a subsequent emission.
func (n *Node[T]) Delete() {
	n.inner = append(n.inner, &pb.Effect{Kind: &pb.Effect_Data{Data: &pb.DataEffect{Op: pb.EffectOp_REMOVE_OP, Collection: pb.CollectionKind_SCALAR}}})
}
