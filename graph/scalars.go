package graph

import (
	"encoding/binary"
	"slices"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
)

// StringFromSnap starts a string builder whose local Value is seeded by snap.
// A nil snapshot seeds an empty string. The snapshot is borrowed, not copied;
// keep it immutable while using the builder. It is never emitted as an effect.
// Pass the tips from the same snapshot read to Emit to establish causality.
// The caller must supply a byte-valued scalar snapshot (raw or compressed);
// no type validation is performed and the returned error is currently always nil.
func StringFromSnap(snap *pb.ReducedEffect) (*StringEffect, error) {
	return &StringEffect{base: snap}, nil
}

// IntFromSnap starts an integer builder whose local Value is seeded by snap.
// A nil snapshot seeds zero. The snapshot is borrowed, not copied; keep it
// immutable while using the builder. It is never emitted as an effect.
// Pass the tips from the same snapshot read to Emit to establish causality.
// The caller must supply an IntVal scalar snapshot; numeric strings are not
// parsed. No type validation is performed and the error is currently always nil.
func IntFromSnap(snap *pb.ReducedEffect) (*IntEffect, error) {
	return &IntEffect{base: snap}, nil
}

// FloatFromSnap starts a float builder whose local Value is seeded by snap.
// A nil snapshot seeds zero. The snapshot is borrowed, not copied; keep it
// immutable while using the builder. It is never emitted as an effect.
// Pass the tips from the same snapshot read to Emit to establish causality.
// The caller must supply a FloatVal scalar snapshot; integers and numeric
// strings are not converted. No type validation is performed and the error
// is currently always nil.
func FloatFromSnap(snap *pb.ReducedEffect) (*FloatEffect, error) {
	return &FloatEffect{base: snap}, nil
}

// StringFromElement decodes a byte-valued collection entry for MapEffect.Get.
// Raw and compressed values are supported; nil seeds an empty string. It borrows
// the element's data, which must remain immutable, and queues no effects.
// Compose subsequent edits back into the map with Set. As with StringFromSnap,
// no type validation is performed and the error is currently always nil.
func StringFromElement(element *pb.ReducedElement) (*StringEffect, error) {
	return StringFromSnap(&pb.ReducedEffect{Scalar: element.GetData()})
}

// IntFromElement decodes an IntVal collection entry for MapEffect.Get. Nil seeds
// zero. It borrows the element's data, which must remain immutable, and queues
// no effects. Compose subsequent edits back into the map with Set. As with
// IntFromSnap, no type conversion or validation is performed and the error is
// currently always nil.
func IntFromElement(element *pb.ReducedElement) (*IntEffect, error) {
	return IntFromSnap(&pb.ReducedEffect{Scalar: element.GetData()})
}

// FloatFromElement decodes a FloatVal collection entry for MapEffect.Get. Nil
// seeds zero. It borrows the element's data, which must remain immutable, and
// queues no effects. Compose subsequent edits back into the map with Set. As
// with FloatFromSnap, no type conversion or validation is performed and the
// error is currently always nil.
func FloatFromElement(element *pb.ReducedElement) (*FloatEffect, error) {
	return FloatFromSnap(&pb.ReducedEffect{Scalar: element.GetData()})
}

func makeStringEffect(
	placement pb.Placement,
	raw *pb.DataEffect_Raw,
	ref []byte,
) *pb.Effect {
	return &pb.Effect{
		Kind: &pb.Effect_Data{
			Data: &pb.DataEffect{
				Op:         pb.EffectOp_INSERT_OP,
				Collection: pb.CollectionKind_SCALAR,
				Merge:      pb.MergeRule_LAST_WRITE_WINS,
				Placement:  placement,
				Value:      raw,
				Reference:  ref,
			},
		},
	}
}

func makeIntEffect(
	op pb.EffectOp,
	collection pb.CollectionKind,
	mergeRule pb.MergeRule,
	placement pb.Placement,
	raw *pb.DataEffect_IntVal,
	id []byte,
	ref []byte,
) *pb.Effect {
	return &pb.Effect{
		Kind: &pb.Effect_Data{
			Data: &pb.DataEffect{
				Op:         op,
				Collection: collection,
				Merge:      mergeRule,
				Placement:  placement,
				Value:      raw,
				Id:         id,
				Reference:  ref,
			},
		},
	}
}

func makeFloatEffect(
	op pb.EffectOp,
	collection pb.CollectionKind,
	mergeRule pb.MergeRule,
	placement pb.Placement,
	raw *pb.DataEffect_FloatVal,
	id []byte,
	ref []byte,
) *pb.Effect {
	return &pb.Effect{
		Kind: &pb.Effect_Data{
			Data: &pb.DataEffect{
				Op:         op,
				Collection: collection,
				Merge:      mergeRule,
				Placement:  placement,
				Value:      raw,
				Id:         id,
				Reference:  ref,
			},
		},
	}
}

// Stringy exposes the local string value of a scalar or an un-emitted builder.
type Stringy interface {
	Value() string
}

// ScalarString is a reusable string value that can create a scalar replacement.
type ScalarString string

// AsEffect creates a fresh builder that replaces the entire string with s.
// Each call allocates independent effect messages.
func (s ScalarString) AsEffect() *StringEffect {
	se := &StringEffect{}
	b := []byte(s)
	next := makeStringEffect(
		pb.Placement_PLACE_SELF,
		&pb.DataEffect_Raw{
			Raw: b,
		},
		nil,
	)
	se.inner = make([]*pb.Effect, 1)
	se.inner[0] = next

	return se
}

// Value returns s as a string.
func (s ScalarString) Value() string {
	return string(s)
}

// StringEffect composes string mutations in call order. Its zero value starts
// from an empty string for Value and emits only the mutations added to it.
// Append, Prepend, and Insert mutate the receiver and return it for chaining.
// Replace returns an independent builder; retain its return value.
//
// Pointer aliases observe the same mutations. Shallow struct copies share
// effect pointers, may share slice storage, and borrow the same
// base snapshot. Extending two copies can overwrite one another's pending
// effects when the slice has spare capacity. Use one evolving builder rather
// than branching copies; start independent builders with AsEffect or StringFromSnap.
// Builders are not safe for concurrent use.
//
// Emit transfers the pending messages to the context, which mutates and retains
// them. Treat the builder and all its aliases as consumed after an Emit call,
// even on error: do not extend, evaluate, or emit them again. Start a new builder
// for subsequent work, reading a fresh snapshot when a base value is needed.
//
// Passing this builder into a list or map consumes its pending messages too:
// the receiving builder rewrites them in place. Do not use this builder or its
// aliases after composition. Use AsEffect again for independent messages.
type StringEffect struct {
	inner        []*pb.Effect
	base         *pb.ReducedEffect
	runtime      *Composite
	runtimeIndex int
}

// Append returns the builder with an effect that appends v to the current
// string. It shares storage with s; see StringEffect's ownership rules.
func (s *StringEffect) Append(v string) *StringEffect {
	b := []byte(v)
	next := makeStringEffect(
		pb.Placement_PLACE_TAIL,
		&pb.DataEffect_Raw{
			Raw: b,
		}, nil,
	)
	s.inner = append(s.inner, next)
	s.syncRuntime()

	return s
}

// Prepend returns the builder with an effect that prepends v to the current
// string. Repeated prepends appear in reverse call order in the value.
// It shares storage with s; see StringEffect's ownership rules.
func (s *StringEffect) Prepend(v string) *StringEffect {
	b := []byte(v)
	next := makeStringEffect(
		pb.Placement_PLACE_HEAD,
		&pb.DataEffect_Raw{
			Raw: b,
		}, nil,
	)
	s.inner = append(s.inner, next)
	s.syncRuntime()

	return s
}

// Insert returns the builder with an effect that inserts v before byte offset
// at in the value produced by preceding effects. Offsets count bytes, not runes;
// 0 inserts at the beginning and len(value) inserts at the end. The offset must
// be in that range when applied. It is not checked here; an invalid offset
// panics during Value or engine reduction. It shares storage with s.
func (s *StringEffect) Insert(v string, at int) *StringEffect {
	b := []byte(v)
	idx := binary.BigEndian.AppendUint64(nil, uint64(at))
	next := makeStringEffect(
		pb.Placement_PLACE_BEFORE,
		&pb.DataEffect_Raw{
			Raw: b,
		}, idx,
	)
	s.inner = append(s.inner, next)
	s.syncRuntime()

	return s
}

// Replace returns a fresh replacement builder for n, discarding the borrowed
// base and all pending mutations. The result shares no effects with s.
func (s *StringEffect) Replace(n string) *StringEffect {
	e := ScalarString(n)

	return e.AsEffect()
}

// Value evaluates the borrowed base followed by pending mutations in call order.
// It neither reads the engine nor mutates the base or pending effect payloads.
// Call it before Emit; this is a local preview, not a reconciled engine read.
func (s *StringEffect) Value() string {
	if s.runtime != nil {
		return string(s.Runtime().current().GetScalar().Decompress())
	}

	val := make([]byte, 0)
	if s.base != nil {
		val = slices.Clone(s.base.GetScalar().Decompress())
	}

	for _, e := range s.inner {
		switch e.GetData().GetPlacement() {
		case pb.Placement_PLACE_SELF, pb.Placement_PLACE_NONE:
			val = slices.Clone(e.GetData().Decompress())
		case pb.Placement_PLACE_HEAD:
			val = slices.Insert(val, 0, e.GetData().Decompress()...)
		case pb.Placement_PLACE_TAIL:
			val = append(val, e.GetData().Decompress()...)
		case pb.Placement_PLACE_BEFORE, pb.Placement_PLACE_AFTER:
			v := e.GetData().Decompress()

			idx := 0
			if e.GetData().GetReference() != nil {
				idx = int(binary.BigEndian.Uint64(e.GetData().GetReference()))
			}

			if e.GetData().GetPlacement() == pb.Placement_PLACE_AFTER {
				idx++
			}

			val = slices.Insert(val, idx, v...)
		}
	}

	return string(val)
}

// Emit transfers pending effects to c for forKey in call order. forKey must be
// a logical application key; Context.Emit derives its internal trie address.
// Graph nodes use Node.EmitGraphNode with their complete internal addresses.
// It does not emit the base snapshot or flush the context. Pass the tips from
// the snapshot used to build s, or nil to let the context resolve dependencies.
// Reusing the same tips across emissions in one pending context is supported:
// later effects chain from that context's preceding emission for the key.
// After Flush or Abort, read fresh tips when continuing from the stored value.
//
// Emit mutates the shared messages, including their keys, and consumes s and
// its aliases even if it fails. On error, earlier effects may already be pending
// in c; the caller must abort or restore a savepoint if rollback is required.
func (s *StringEffect) Emit(forKey string, c *effects.Context, tips []effects.Tip) error {
	if s.runtime != nil {
		return s.Runtime().Emit(forKey, c, tips)
	}

	key := []byte(forKey)
	for _, e := range s.inner {
		e.Key = key

		err := c.Emit(e, tips)
		if err != nil {
			return err
		}
	}

	return nil
}

// EmitPartition emits pending effects at partition within forKey, overwriting
// each message's Virtual field. An empty partition selects the root. The base
// snapshot is not emitted. Emit's address, tips, consumption, and error handling
// rules apply; this method does not flush c.
func (s *StringEffect) EmitPartition(forKey, partition string, c *effects.Context, tips []effects.Tip) error {
	if s.runtime != nil {
		return s.Runtime().EmitPartition(forKey, partition, c, tips)
	}

	key := []byte(forKey)
	part := []byte(partition)

	for _, e := range s.inner {
		e.Key = key
		e.Virtual = part

		err := c.Emit(e, tips)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *StringEffect) getEffects() []*pb.Effect {
	return s.inner
}

// ScalarInt is a reusable, machine-sized integer value. Its effects and Value
// use int64, matching the engine's scalar integer representation.
type ScalarInt int

// Emits is a composable builder of pending effects. Scalar values become
// builders through AsEffect; StringEffect, IntEffect, FloatEffect, ListEffect,
// and MapEffect implement this interface.
//
// Composition takes ownership of getEffects' shared messages and may rewrite
// collection fields. Only pending effects are exposed; a borrowed base snapshot
// is never an effect. Treat input builders and aliases as consumed when composed
// into another builder. Emit and EmitPartition also consume their messages, even
// on error, and do not flush the context.
//
// The unexported getEffects method makes this a package-owned interface.
type Emits interface {
	Value
	Emit(forKey string, c *effects.Context, tips []effects.Tip) error
	EmitPartition(forKey string, partition string, c *effects.Context, tips []effects.Tip) error
	getEffects() []*pb.Effect
}

// CountableInt exposes the local integer value of a scalar or un-emitted builder.
type CountableInt interface {
	Value() int64
}

// AsEffect creates a fresh builder that replaces the entire scalar with s.
// Each call allocates independent effect messages.
func (s ScalarInt) AsEffect() *IntEffect {
	se := &IntEffect{}
	next := makeIntEffect(
		pb.EffectOp_INSERT_OP,
		pb.CollectionKind_SCALAR,
		pb.MergeRule_LAST_WRITE_WINS,
		pb.Placement_PLACE_NONE,
		&pb.DataEffect_IntVal{
			IntVal: int64(s),
		},
		nil, nil,
	)
	se.inner = make([]*pb.Effect, 1)
	se.inner[0] = next

	return se
}

// Value returns s as an int64.
func (s ScalarInt) Value() int64 {
	return int64(s)
}

// IntEffect composes integer mutations in call order. Its zero value starts
// from zero for Value and emits only the mutations added to it. Add mutates the
// receiver and returns the same pointer for chaining.
//
// Pointer aliases observe the same mutations. Shallow struct copies share
// effect pointers, may share slice storage, and borrow the same
// base snapshot. Extending two copies can overwrite pending effects when the
// slice has spare capacity. Use one evolving builder, keep the base immutable,
// and do not use aliases concurrently.
//
// Emit transfers the pending messages to the context. Treat the builder and all
// aliases as consumed after the call, even on error: do not extend, evaluate,
// or emit them again. Start a fresh builder for subsequent work.
//
// Passing this builder into a list or map consumes its pending messages too:
// the receiving builder rewrites them in place. Do not use this builder or its
// aliases after composition. Use AsEffect again for independent messages.
type IntEffect struct {
	inner        []*pb.Effect
	base         *pb.ReducedEffect
	runtime      *Composite
	runtimeIndex int
}

// Emit transfers pending effects to c for forKey in call order. forKey must be
// a logical application key; Context.Emit derives its internal trie address.
// Graph nodes use Node.EmitGraphNode with their complete internal addresses.
// It does not emit the base snapshot or flush c. Pass the tips from the snapshot
// used to build s, or nil to let the context resolve dependencies. Reusing those
// tips in one pending context is supported: later effects chain from its
// preceding emission for the key. After Flush or Abort, read fresh tips when
// continuing from the stored value.
//
// The shared messages are mutated and s and its aliases are consumed, even on
// error. Earlier effects may already be pending in c when an error occurs;
// abort or restore a savepoint if rollback is required.
func (s *IntEffect) Emit(forKey string, c *effects.Context, tips []effects.Tip) error {
	if s.runtime != nil {
		return s.Runtime().Emit(forKey, c, tips)
	}

	key := []byte(forKey)
	for _, e := range s.inner {
		e.Key = key

		err := c.Emit(e, tips)
		if err != nil {
			return err
		}
	}

	return nil
}

// EmitPartition emits pending effects at partition within forKey, overwriting
// each message's Virtual field. An empty partition selects the root. The base
// snapshot is not emitted. Emit's address, tips, consumption, and error handling
// rules apply; this method does not flush c.
func (s *IntEffect) EmitPartition(forKey, partition string, c *effects.Context, tips []effects.Tip) error {
	if s.runtime != nil {
		return s.Runtime().EmitPartition(forKey, partition, c, tips)
	}

	key := []byte(forKey)
	part := []byte(partition)

	for _, e := range s.inner {
		e.Key = key
		e.Virtual = part

		err := c.Emit(e, tips)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *IntEffect) getEffects() []*pb.Effect {
	return s.inner
}

// Add returns the builder with an additive delta of i; negative values subtract.
// It shares storage with s. Addition uses int64 arithmetic, including overflow.
func (s *IntEffect) Add(i int) *IntEffect {
	next := makeIntEffect(
		pb.EffectOp_INSERT_OP,
		pb.CollectionKind_SCALAR,
		pb.MergeRule_ADDITIVE_INT,
		pb.Placement_PLACE_NONE,
		&pb.DataEffect_IntVal{
			IntVal: int64(i),
		}, nil, nil,
	)
	s.inner = append(s.inner, next)
	s.syncRuntime()

	return s
}

// Value evaluates the borrowed IntVal base followed by pending mutations.
// It neither reads the engine nor mutates the base or pending effects.
// Call it before Emit; this is a local preview, not a reconciled engine read.
func (s *IntEffect) Value() int64 {
	if s.runtime != nil {
		return s.Runtime().current().GetScalar().GetIntVal()
	}

	val := int64(0)
	if s.base != nil {
		val = s.base.GetScalar().GetIntVal()
	}

	for _, e := range s.inner {
		if e.GetData().GetMerge() == pb.MergeRule_ADDITIVE_INT {
			val += e.GetData().GetIntVal()
		} else {
			val = e.GetData().GetIntVal()
		}
	}

	return val
}

// ScalarFloat is a reusable float64 value that can create a scalar replacement.
type ScalarFloat float64

// CountableFloat exposes the local float value of a scalar or un-emitted builder.
type CountableFloat interface {
	Value() float64
}

// AsEffect creates a fresh builder that replaces the entire scalar with s.
// Each call allocates independent effect messages.
func (s ScalarFloat) AsEffect() *FloatEffect {
	se := &FloatEffect{}
	next := makeFloatEffect(
		pb.EffectOp_INSERT_OP,
		pb.CollectionKind_SCALAR,
		pb.MergeRule_LAST_WRITE_WINS,
		pb.Placement_PLACE_NONE,
		&pb.DataEffect_FloatVal{
			FloatVal: float64(s),
		},
		nil, nil,
	)
	se.inner = make([]*pb.Effect, 1)
	se.inner[0] = next

	return se
}

// Value returns s as a float64.
func (s ScalarFloat) Value() float64 {
	return float64(s)
}

// FloatEffect composes float mutations in call order. Its zero value starts
// from zero for Value and emits only the mutations added to it. Add mutates the
// receiver and returns the same pointer for chaining.
//
// Pointer aliases observe the same mutations. Shallow struct copies share
// effect pointers, may share slice storage, and borrow the same
// base snapshot. Extending two copies can overwrite pending effects when the
// slice has spare capacity. Use one evolving builder, keep the base immutable,
// and do not use aliases concurrently.
//
// Emit transfers the pending messages to the context. Treat the builder and all
// aliases as consumed after the call, even on error: do not extend, evaluate,
// or emit them again. Start a fresh builder for subsequent work.
//
// Passing this builder into a list or map consumes its pending messages too:
// the receiving builder rewrites them in place. Do not use this builder or its
// aliases after composition. Use AsEffect again for independent messages.
type FloatEffect struct {
	inner        []*pb.Effect
	base         *pb.ReducedEffect
	runtime      *Composite
	runtimeIndex int
}

// Emit transfers pending effects to c for forKey in call order. forKey must be
// a logical application key; Context.Emit derives its internal trie address.
// Graph nodes use Node.EmitGraphNode with their complete internal addresses.
// It does not emit the base snapshot or flush c. Pass the tips from the snapshot
// used to build s, or nil to let the context resolve dependencies. Reusing those
// tips in one pending context is supported: later effects chain from its
// preceding emission for the key. After Flush or Abort, read fresh tips when
// continuing from the stored value.
//
// The shared messages are mutated and s and its aliases are consumed, even on
// error. Earlier effects may already be pending in c when an error occurs;
// abort or restore a savepoint if rollback is required.
func (s *FloatEffect) Emit(forKey string, c *effects.Context, tips []effects.Tip) error {
	if s.runtime != nil {
		return s.Runtime().Emit(forKey, c, tips)
	}

	key := []byte(forKey)
	for _, e := range s.inner {
		e.Key = key
		if err := c.Emit(e, tips); err != nil {
			return err
		}
	}

	return nil
}

// EmitPartition emits pending effects at partition within forKey, overwriting
// each message's Virtual field. An empty partition selects the root. The base
// snapshot is not emitted. Emit's address, tips, consumption, and error handling
// rules apply; this method does not flush c.
func (s *FloatEffect) EmitPartition(forKey, partition string, c *effects.Context, tips []effects.Tip) error {
	if s.runtime != nil {
		return s.Runtime().EmitPartition(forKey, partition, c, tips)
	}

	key := []byte(forKey)
	part := []byte(partition)

	for _, e := range s.inner {
		e.Key = key
		e.Virtual = part

		err := c.Emit(e, tips)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *FloatEffect) getEffects() []*pb.Effect {
	return s.inner
}

// Add returns the builder with an additive delta of f; negative values subtract.
// It shares storage with s. Addition uses float64 arithmetic in call order,
// including rounding, infinities, and NaN; non-finite values are not rejected.
func (s *FloatEffect) Add(f float64) *FloatEffect {
	next := makeFloatEffect(
		pb.EffectOp_INSERT_OP,
		pb.CollectionKind_SCALAR,
		pb.MergeRule_ADDITIVE_FLOAT,
		pb.Placement_PLACE_NONE,
		&pb.DataEffect_FloatVal{
			FloatVal: f,
		}, nil, nil,
	)
	s.inner = append(s.inner, next)
	s.syncRuntime()

	return s
}

// Value evaluates the borrowed FloatVal base followed by pending mutations.
// It neither reads the engine nor mutates the base or pending effects.
// Call it before Emit; this is a local preview, not a reconciled engine read.
func (s *FloatEffect) Value() float64 {
	if s.runtime != nil {
		return s.Runtime().current().GetScalar().GetFloatVal()
	}

	val := float64(0)
	if s.base != nil {
		val = s.base.GetScalar().GetFloatVal()
	}

	for _, e := range s.inner {
		if e.GetData().GetMerge() == pb.MergeRule_ADDITIVE_FLOAT {
			val += e.GetData().GetFloatVal()
		} else {
			val = e.GetData().GetFloatVal()
		}
	}

	return val
}

// Runtime adapts this low-level builder to the public Value protocol. Existing
// builder methods retain their original composition and ownership rules.
func (s *StringEffect) Runtime() *Composite {
	if s.runtime == nil {
		s.runtime = &Composite{state: &compositeState{base: s.base, inner: append([]*pb.Effect(nil), s.inner...)}, scalar: true, legacyAbsolute: true}
		s.runtimeIndex = len(s.inner)
	}

	s.syncRuntime()

	return s.runtime
}

func (s *StringEffect) syncRuntime() {
	if s.runtime != nil && s.runtimeIndex < len(s.inner) {
		s.runtime.apply(s.inner[s.runtimeIndex:])
		s.runtimeIndex = len(s.inner)
	}
}

// Runtime adapts this low-level builder to the public Value protocol. Existing
// builder methods retain their original composition and ownership rules.
func (s *IntEffect) Runtime() *Composite {
	if s.runtime == nil {
		s.runtime = &Composite{state: &compositeState{base: s.base, inner: append([]*pb.Effect(nil), s.inner...)}, scalar: true, legacyAbsolute: true}
		s.runtimeIndex = len(s.inner)
	}

	s.syncRuntime()

	return s.runtime
}

func (s *IntEffect) syncRuntime() {
	if s.runtime != nil && s.runtimeIndex < len(s.inner) {
		s.runtime.apply(s.inner[s.runtimeIndex:])
		s.runtimeIndex = len(s.inner)
	}
}

// Runtime adapts this low-level builder to the public Value protocol. Existing
// builder methods retain their original composition and ownership rules.
func (s *FloatEffect) Runtime() *Composite {
	if s.runtime == nil {
		s.runtime = &Composite{state: &compositeState{base: s.base, inner: append([]*pb.Effect(nil), s.inner...)}, scalar: true, legacyAbsolute: true}
		s.runtimeIndex = len(s.inner)
	}

	s.syncRuntime()

	return s.runtime
}

func (s *FloatEffect) syncRuntime() {
	if s.runtime != nil && s.runtimeIndex < len(s.inner) {
		s.runtime.apply(s.inner[s.runtimeIndex:])
		s.runtimeIndex = len(s.inner)
	}
}
