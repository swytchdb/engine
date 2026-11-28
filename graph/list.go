package graph

import (
	"crypto/rand"
	"slices"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
)

// ListFromSnap borrows snap as the base for resolving list positions. The snapshot
// must be an ordered collection and remain immutable. No validation or decoding
// is performed; the error is currently always nil. Nil seeds an empty list.
//
// Only subsequently composed effects are emitted, never the snapshot. Pass the
// tips from the same read when emitting. T is an effect builder, not an element's
// Go value type.
func ListFromSnap[T Emits](snap *pb.ReducedEffect) (*ListEffect[T], error) {
	return &ListEffect[T]{base: snap}, nil
}

// ListEffect composes the pending data effects of T into an ordered collection.
// For example:
//
//	list := (&ListEffect[*StringEffect]{}).Append(ScalarString("hello").AsEffect())
//
// Composition takes ownership of the input builders' messages and rewrites their
// collection, placement, and element IDs. It does not evaluate input snapshots or
// copy messages. Do not reuse an input builder or its aliases after composition.
// The operation and merge rule of each input effect are retained.
//
// The zero value has no base or pending effects. Methods mutate the receiver
// and return the same pointer for chaining. Pointer aliases observe those edits.
// Copying the struct does not clone messages, slices, or the borrowed snapshot;
// do not branch shallow copies or use aliases concurrently.
//
// Emit and EmitPartition consume the builder and all aliases, even on error.
// Start a fresh builder or read a new snapshot before further work.
type ListEffect[T Emits] struct {
	inner        []*pb.Effect
	base         *pb.ReducedEffect
	runtime      *Composite
	runtimeIndex int
}

// Append consumes values and appends their pending effects in argument order.
// Each input effect receives a fresh element ID. The receiver is updated in place. See ListEffect for ownership rules.
func (s *ListEffect[T]) Append(values ...T) *ListEffect[T] {
	ret := s
	for _, v := range values {
		ret = ret.Insert(v, -1)
	}

	s.syncRuntime()

	return ret
}

// Prepend consumes values and inserts their effects at the front. For inputs
// with one effect each, argument order is preserved; later calls precede earlier
// calls. The supplied variadic slice is reversed in place, so passing a slice
// with values... changes that slice's order. The receiver is updated in place.
func (s *ListEffect[T]) Prepend(values ...T) *ListEffect[T] {
	ret := s

	slices.Reverse(values)

	for _, v := range values {
		ret = ret.Insert(v, 0)
	}

	s.syncRuntime()

	return ret
}

// Insert consumes value's pending effects and assigns each a fresh element ID.
// An index of zero inserts at the front; -1 or the current length appends.
// Other indices must identify an existing element. That element's ID is captured
// as the insertion reference, so subsequent edits do not retarget the insertion.
//
// Collection and placement are overwritten; the input operations and merge rules
// are retained. Multiple input effects become distinct elements, not one decoded
// value. No input snapshot is emitted. The receiver is updated in place.
func (s *ListEffect[T]) Insert(value T, at int) *ListEffect[T] {
	ret := s
	elems := s.elements()

	op := pb.Placement_PLACE_BEFORE
	if at == 0 {
		op = pb.Placement_PLACE_HEAD
	} else if at == len(elems) || at == -1 {
		op = pb.Placement_PLACE_TAIL
	}

	for _, e := range value.getEffects() {
		e.GetData().Collection = pb.CollectionKind_ORDERED

		e.GetData().Placement = op
		if op == pb.Placement_PLACE_BEFORE {
			idx := elems[at].GetData().GetId()
			e.GetData().Reference = idx
		}

		e.GetData().Id = []byte(rand.Text())
		ret.inner = append(ret.inner, e)
	}

	s.syncRuntime()

	return ret
}

// Set consumes value's pending effects, targeting all of them at the existing
// element at index at. The element's ID is preserved and placement becomes SELF;
// input operations and merge rules are retained. Additive effects accumulate
// against the existing element using scalar reduction semantics. A nonempty
// input panics for an invalid index. An input with no pending effects is a no-op.
// The receiver is updated in place.
func (s *ListEffect[T]) Set(value T, at int) *ListEffect[T] {
	ret := s
	elems := s.elements()

	for _, e := range value.getEffects() {
		e.GetData().Collection = pb.CollectionKind_ORDERED
		e.GetData().Placement = pb.Placement_PLACE_SELF
		e.GetData().Id = elems[at].GetData().GetId()
		ret.inner = append(ret.inner, e)
	}

	s.syncRuntime()

	return ret
}

// Remove removes the element at index at, targeting its ID rather than its
// value. Equal values at other indices remain. It panics unless 0 <= at < the
// current local length. The receiver is updated in place.
func (s *ListEffect[T]) Remove(at int) *ListEffect[T] {
	id := s.elements()[at].GetData().GetId()
	d := &pb.DataEffect{
		Op: pb.EffectOp_REMOVE_OP, Collection: pb.CollectionKind_ORDERED,
		Merge: pb.MergeRule_LAST_WRITE_WINS, Placement: pb.Placement_PLACE_SELF,
		Id: slices.Clone(id),
	}
	s.inner = append(s.inner, &pb.Effect{Kind: &pb.Effect_Data{Data: d}})
	s.syncRuntime()

	return s
}

// Replace appends a scalar removal followed by the pending effects of values.
// It retains the borrowed base and earlier pending effects; the removal resets
// state during reduction. An empty replacement records only the removal.
// At the root, this removes the entire key including metadata and partitions;
// EmitPartition scopes it to that partition. Input builders are consumed.
func (s *ListEffect[T]) Replace(values []T) *ListEffect[T] {
	d := &pb.DataEffect{
		Op: pb.EffectOp_REMOVE_OP, Collection: pb.CollectionKind_SCALAR,
		Merge: pb.MergeRule_LAST_WRITE_WINS, Placement: pb.Placement_PLACE_NONE,
	}
	s.inner = append(s.inner, &pb.Effect{Kind: &pb.Effect_Data{Data: d}})

	return s.Append(values...)
}

// Emit transfers pending effects to c in order without flushing it. forKey is
// a logical application key; the context derives its internal address. Pass the
// snapshot's tips, or nil to let the context resolve dependencies. The same
// tips may be reused within one pending context: later effects chain from the
// previous emission for the key. After Flush or Abort, read fresh tips.
//
// The builder and all aliases are consumed even on error. Earlier effects may
// already be pending in c; abort or restore a savepoint if rollback is required.
func (s *ListEffect[T]) Emit(forKey string, c *effects.Context, tips []effects.Tip) error {
	if s.runtime != nil {
		return s.Runtime().Emit(forKey, c, tips)
	}

	key := []byte(forKey)
	for _, eff := range s.inner {
		eff.Key = key
		if err := c.Emit(eff, tips); err != nil {
			return err
		}
	}

	return nil
}

// EmitPartition emits pending effects at partition within forKey, overwriting
// each message's Virtual field. An empty partition selects the root. It does not
// flush c or emit the snapshot. Emit's address, tips, consumption, and error
// handling rules apply. A replacement removes only the selected partition when
// partition is nonempty.
func (s *ListEffect[T]) EmitPartition(forKey string, partition string, c *effects.Context, tips []effects.Tip) error {
	if s.runtime != nil {
		return s.Runtime().EmitPartition(forKey, partition, c, tips)
	}

	key := []byte(forKey)
	part := []byte(partition)

	for _, eff := range s.inner {
		eff.Key = key

		eff.Virtual = part
		if err := c.Emit(eff, tips); err != nil {
			return err
		}
	}

	return nil
}

func (s *ListEffect[T]) getEffects() []*pb.Effect {
	return s.inner
}

func (s *ListEffect[T]) elements() []*pb.ReducedElement {
	if s.runtime != nil {
		return s.Runtime().current().GetOrderedElements()
	}

	return effects.ReduceChain(s.base, s.inner).GetOrderedElements()
}

// Runtime adapts this low-level builder to the public Value protocol. Existing
// builder methods retain their original composition and ownership rules.
func (s *ListEffect[T]) Runtime() *Composite {
	if s.runtime == nil {
		s.runtime = &Composite{state: &compositeState{base: s.base, inner: append([]*pb.Effect(nil), s.inner...)}, scalar: false, legacyAbsolute: true}
		s.runtimeIndex = len(s.inner)
	}

	s.syncRuntime()

	return s.runtime
}

func (s *ListEffect[T]) syncRuntime() {
	if s.runtime != nil && s.runtimeIndex < len(s.inner) {
		s.runtime.apply(s.inner[s.runtimeIndex:])
		s.runtimeIndex = len(s.inner)
	}
}
