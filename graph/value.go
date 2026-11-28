package graph

import (
	"fmt"
	"strings"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"google.golang.org/protobuf/proto"
)

// Value is a runtime Swytch value. Runtime returns the same persistent runtime
// for the value's lifetime. Handwritten types can implement this method directly
// or embed Composite; neither code generation nor package-private methods are
// required. Materialized reads remain specific to the concrete type.
type Value interface{ Runtime() *Composite }

// Emit publishes a value's pending operations to a context at a logical key.
// It does not commit the context or emit snapshot seeds. Emission consumes the
// value and all sharing values, even on error. Pass tips from the observed read.
func Emit(value Value, key string, ctx *effects.Context, tips []effects.Tip) error {
	return value.Runtime().Emit(key, ctx, tips)
}

// EmitPartition mounts a value under a partition, preserving relative nested
// paths. Its ownership and context lifetime rules are the same as Emit.
func EmitPartition(value Value, key, partition string, ctx *effects.Context, tips []effects.Tip) error {
	return value.Runtime().EmitPartition(key, partition, ctx, tips)
}

// Type constructs an empty runtime implementation of a logical type. New must
// return a fresh value with no pending effects. It is used for typed reads and
// generic fields; constructing a type does not require a schema.
type Type[T Value] struct{ New func() T }

// FromSnapshot constructs a value seeded by an immutable snapshot. Loading does
// not create effects. The returned value retains this observation for reads.
func (typ Type[T]) FromSnapshot(snapshot *pb.ReducedEffect) (T, error) {
	if typ.New == nil {
		var zero T

		return zero, fmt.Errorf("runtime Type.New factory is required")
	}

	value := typ.New()
	value.Runtime().state = &compositeState{base: snapshot}

	return value, nil
}

// Runtime exposes this composite's runtime to the public Value protocol.
func (c *Composite) Runtime() *Composite { return c }

// Bind returns a typed value at name, retaining its identity across calls.
// Scalar types bind to keyed entries; other types bind to child partitions.
// Merely binding a value queues no effects. The supplied Type must match the
// value already bound at that location.
func Bind[T Value](owner *Composite, name string, typ Type[T]) T {
	if existing, ok := owner.boundValue(name); ok {
		typed, ok := existing.(T)
		if !ok {
			panic(fmt.Sprintf("field %q is already bound as %T", name, existing))
		}

		return typed
	}

	candidate := typ.New()
	scalar := candidate.Runtime().scalar
	target := owner.binding(name, scalar)
	root := target.owner()

	key := target.key()
	if existing, ok := root.values[key]; ok {
		typed, ok := existing.(T)
		if !ok {
			panic(fmt.Sprintf("field %q is already bound as %T", name, existing))
		}

		return typed
	}

	*candidate.Runtime() = target
	candidate.Runtime().scalar = scalar

	if root.values == nil {
		root.values = make(map[runtimeKey]Value)
	}

	root.values[key] = candidate

	return candidate
}

// boundValue looks up an existing instance without allocating a factory value.
// This also permits explicitly populated generic fields to be read without a
// factory, as their runtime implementation is already known.
func (c *Composite) boundValue(name string) (Value, bool) {
	for _, scalar := range []bool{true, false} {
		target := c.binding(name, scalar)
		if value := target.owner().values[target.key()]; value != nil {
			return value, true
		}
	}

	return nil, false
}

// FieldSnapshot projects a named runtime value from a composite snapshot. Scalar
// entries become scalar snapshots; structured values retain relative partitions.
// The result borrows immutable data and never creates effects.
func FieldSnapshot(snapshot *pb.ReducedEffect, name string) *pb.ReducedEffect {
	if entry, ok := snapshot.GetNetAdds()[name]; ok {
		return &pb.ReducedEffect{
			Op: pb.EffectOp_INSERT_OP, Collection: pb.CollectionKind_SCALAR,
			Scalar: entry.GetData(), Merge: entry.GetData().GetMerge(), Hlc: entry.GetHlc(), NodeId: entry.GetNodeId(),
		}
	}

	segment := strings.ReplaceAll(strings.ReplaceAll(name, "~", "~0"), "/", "~1")

	return PartitionSnapshot(snapshot, segment)
}

type runtimeKey struct {
	path, field string
	scalarField bool
}

func (c *Composite) key() runtimeKey {
	r := c.resolve()

	key := runtimeKey{path: r.path}
	if r.field != nil {
		key.field = *r.field
		key.scalarField = true
	}

	return key
}

func (c *Composite) binding(name string, scalar bool) Composite {
	if !scalar {
		return c.Child(name)
	}

	r := c.resolve()

	return Composite{state: r.state, path: r.path, field: &name, scalar: true}
}

// Set composes a standalone value at name, preserving its identity and pending
// operations. Further mutations through either reference belong to this owner.
// Scalar values use ordinary keyed entries; composites use relative partitions.
// A value already attached elsewhere cannot be attached a second time. Loading
// a snapshot does not turn that snapshot into insertion effects.
func (c *Composite) Set(name string, value Value) error {
	source := value.Runtime()
	target := c.binding(name, source.scalar)

	from, to := source.resolve(), target.resolve()
	if from.state == to.state {
		if from.key() == to.key() {
			return nil
		}

		return fmt.Errorf("value is already composed into this owner")
	}

	if from.path != "" || from.field != nil || source.state.redirect != nil {
		return fmt.Errorf("value is already composed into another owner")
	}
	// Do not permit a parent to be inserted into its own descendant.
	for state := to.state; state != nil; {
		if state == from.state {
			return fmt.Errorf("composition cycle")
		}

		if state.redirect == nil {
			break
		}

		state = state.redirect.state
	}

	root := to.state
	if root.values == nil {
		root.values = make(map[runtimeKey]Value)
	}
	// Seed local reads from a loaded value without publishing its snapshot.
	if from.state.base != nil {
		to.seed(from.state.base)
	}

	batch := from.state.inner
	if to.field != nil {
		for _, effect := range batch {
			to.apply([]*pb.Effect{effect})
		}
	} else {
		for _, effect := range batch {
			effect.Virtual = []byte(partitionJoin(to.path, string(effect.GetVirtual())))
			root.inner = append(root.inner, effect)
		}
	}

	root.values[to.key()] = value
	for key, child := range from.state.values {
		key.path = partitionJoin(to.path, key.path)
		root.values[key] = child
	}

	from.state.inner = nil
	from.state.redirect = &target

	return nil
}

func (c *Composite) seed(snapshot *pb.ReducedEffect) {
	r := c.resolve()

	root := r.state
	if root.base == nil {
		root.base = &pb.ReducedEffect{}
	} else {
		root.base = proto.Clone(root.base).(*pb.ReducedEffect)
	}

	if r.field != nil {
		var base *pb.ReducedEffect
		if r.path == "" {
			base = root.base
		} else {
			if root.base.Partitions == nil {
				root.base.Partitions = make(map[string]*pb.ReducedEffect)
			}

			base = root.base.GetPartitions()[r.path]
			if base == nil {
				base = &pb.ReducedEffect{}
				root.base.Partitions[r.path] = base
			}
		}

		base.Collection = pb.CollectionKind_KEYED
		if base.NetAdds == nil {
			base.NetAdds = make(map[string]*pb.ReducedElement)
		}

		data := snapshot.GetScalar()
		if data != nil {
			data = proto.Clone(data).(*pb.DataEffect)
			data.Collection = pb.CollectionKind_KEYED
			data.Id = []byte(*r.field)
			base.NetAdds[*r.field] = &pb.ReducedElement{Data: data}
		}

		return
	}

	if root.base.Partitions == nil {
		root.base.Partitions = make(map[string]*pb.ReducedEffect)
	}

	base := proto.Clone(snapshot).(*pb.ReducedEffect)
	base.Partitions = nil

	root.base.Partitions[r.path] = base
	for path, child := range snapshot.GetPartitions() {
		root.base.Partitions[partitionJoin(r.path, path)] = child
	}
}

// Lookup returns the retained runtime value, or decodes a stored scalar or
// composite on demand. Typed collections can be retrieved with Bind and a Type.
// Missing names return nil, false. No schema or effects are created by lookup.
func (c *Composite) Lookup(name string) (Value, bool) {
	for _, scalar := range []bool{true, false} {
		target := c.binding(name, scalar)
		if value := target.owner().values[target.key()]; value != nil {
			return value, true
		}
	}

	if data := c.Field(name); data != nil {
		switch data.GetValue().(type) {
		case *pb.DataEffect_IntVal:
			return Bind(c, name, Int64Type), true
		case *pb.DataEffect_FloatVal:
			return Bind(c, name, Float64Type), true
		default:
			return Bind(c, name, StringType), true
		}
	}

	child := c.Child(name)
	if child.Snapshot() != nil {
		return Bind(c, name, CompositeType), true
	}

	return nil, false
}

// CompositeType describes an empty, dynamically shaped composite.
var CompositeType = Type[*Composite]{New: func() *Composite { return &Composite{} }}

// NewComposite creates a standalone dynamically shaped value.
func NewComposite() *Composite { return &Composite{} }
