package graph

import (
	"slices"
	"sort"

	pb "github.com/swytchdb/engine/cluster/proto"
)

// MapField is a typed runtime map, usable standalone through NewMap or composed
// into another value. Keys are raw strings. Composition retains its identity;
// mutations through either reference enter the same effect sequence. Use Emit
// to publish it independently. No schema or generated code is required.
type MapField[T any] struct {
	scope  Composite
	encode func(T) *pb.DataEffect
	decode func(*pb.DataEffect) T
}

// NewMapField binds a generated map accessor to an attached child view. Encode
// must return a fresh scalar replacement; decode must not mutate borrowed data.
func NewMapField[T any](scope Composite, encode func(T) *pb.DataEffect, decode func(*pb.DataEffect) T) *MapField[T] {
	return &MapField[T]{scope: scope, encode: encode, decode: decode}
}

// Get reads a locally reduced entry. Missing keys return the zero value and false.
func (m *MapField[T]) Get(key string) (T, bool) {
	entry, ok := m.scope.current().GetNetAdds()[key]
	if !ok {
		var zero T

		return zero, false
	}

	return m.decode(entry.GetData()), true
}

// Set queues a replacement and returns this attached view.
func (m *MapField[T]) Set(key string, value T) *MapField[T] {
	m.scope.ApplyField(key, m.encode(value))

	return m
}

// Remove queues removal of key; removing an absent key is allowed.
func (m *MapField[T]) Remove(key string) *MapField[T] {
	m.scope.ApplyField(key, &pb.DataEffect{Op: pb.EffectOp_REMOVE_OP})

	return m
}

// Values returns a new map of locally reduced entries.
func (m *MapField[T]) Values() map[string]T {
	values := make(map[string]T)
	for key, entry := range m.scope.current().GetNetAdds() {
		values[key] = m.decode(entry.GetData())
	}

	return values
}

// Clear removes this collection without touching sibling fields or partitions.
func (m *MapField[T]) Clear() { m.scope.Clear() }

// Replace clears the map and inserts values in sorted key order.
func (m *MapField[T]) Replace(values map[string]T) *MapField[T] {
	m.Clear()

	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	for _, key := range keys {
		m.Set(key, values[key])
	}

	return m
}

// StringSetField is a standalone or composed set<string>. Members use their raw bytes as
// keyed IDs and payloads, with the engine's ordinary keyed insert/remove rules.
type StringSetField struct{ scope Composite }

// NewStringSetField binds a generated set accessor to an attached child view.
func NewStringSetField(scope Composite) *StringSetField { return &StringSetField{scope: scope} }

// Add queues membership for values. Duplicate members remain a single entry.
func (s *StringSetField) Add(values ...string) *StringSetField {
	for _, value := range values {
		s.scope.ApplyField(value, &pb.DataEffect{Op: pb.EffectOp_INSERT_OP, Value: &pb.DataEffect_Raw{Raw: []byte(value)}})
	}

	return s
}

// Remove queues removal of each member; absent members are allowed.
func (s *StringSetField) Remove(values ...string) *StringSetField {
	for _, value := range values {
		s.scope.ApplyField(value, &pb.DataEffect{Op: pb.EffectOp_REMOVE_OP})
	}

	return s
}

// Contains reports membership after local pending edits.
func (s *StringSetField) Contains(value string) bool {
	_, ok := s.scope.current().GetNetAdds()[value]

	return ok
}

// Values returns the current members sorted by raw string bytes.
func (s *StringSetField) Values() []string {
	adds := s.scope.current().GetNetAdds()
	if len(adds) == 0 {
		return nil
	}

	values := make([]string, 0, len(adds))
	for value := range adds {
		values = append(values, value)
	}

	sort.Strings(values)

	return values
}

// Clear removes this set without touching sibling fields or partitions.
func (s *StringSetField) Clear() { s.scope.Clear() }

// Replace clears the set and adds values in argument order.
func (s *StringSetField) Replace(values ...string) *StringSetField {
	s.Clear()

	return s.Add(values...)
}

// ListField is a standalone or composed typed ordered collection. It delegates placement,
// element identity, updates, and removal to ListEffect, while transferring every
// operation immediately into the containing composite's effect sequence.
type ListField[T any] struct {
	scope  Composite
	encode func(T) *pb.DataEffect
	decode func(*pb.DataEffect) T
}

// NewListField binds a generated list accessor to an attached child view. Encode
// must return a fresh scalar replacement; decode must not mutate borrowed data.
func NewListField[T any](scope Composite, encode func(T) *pb.DataEffect, decode func(*pb.DataEffect) T) *ListField[T] {
	return &ListField[T]{scope: scope, encode: encode, decode: decode}
}

func (l *ListField[T]) builder() *ListEffect[*MapEffect[Emits]] {
	return &ListEffect[*MapEffect[Emits]]{base: l.scope.current()}
}

// element wraps one freshly encoded scalar effect for ListEffect's existing
// composition API; the temporary carrier is never evaluated as a map.
func (l *ListField[T]) element(value T) *MapEffect[Emits] {
	return &MapEffect[Emits]{inner: []*pb.Effect{{Kind: &pb.Effect_Data{Data: l.encode(value)}}}}
}

// Append appends values in argument order and returns this attached view.
func (l *ListField[T]) Append(values ...T) *ListField[T] {
	b := l.builder()
	for _, value := range values {
		b.Append(l.element(value))
	}

	l.scope.apply(b.inner)

	return l
}

// Prepend prepends values in argument order without changing the input slice.
func (l *ListField[T]) Prepend(values ...T) *ListField[T] {
	b := l.builder()
	for _, value := range slices.Backward(values) {
		b.Prepend(l.element(value))
	}

	l.scope.apply(b.inner)

	return l
}

// Insert inserts value before index; -1 or the current length appends.
// Other invalid indices panic, matching ListEffect.
func (l *ListField[T]) Insert(index int, value T) *ListField[T] {
	b := l.builder().Insert(l.element(value), index)
	l.scope.apply(b.inner)

	return l
}

// Set replaces the existing element at index, preserving its ID. Invalid
// indices panic, matching ListEffect.
func (l *ListField[T]) Set(index int, value T) *ListField[T] {
	b := l.builder().Set(l.element(value), index)
	l.scope.apply(b.inner)

	return l
}

// Remove removes the element at index. Invalid indices panic.
func (l *ListField[T]) Remove(index int) *ListField[T] {
	b := l.builder().Remove(index)
	l.scope.apply(b.inner)

	return l
}

// Get returns the locally reduced element at index. Invalid indices panic.
func (l *ListField[T]) Get(index int) T {
	return l.decode(l.scope.current().GetOrderedElements()[index].GetData())
}

// Values returns a new slice of locally reduced values in traversal order.
func (l *ListField[T]) Values() []T {
	entries := l.scope.current().GetOrderedElements()
	if len(entries) == 0 {
		return nil
	}

	values := make([]T, 0, len(entries))
	for _, entry := range entries {
		values = append(values, l.decode(entry.GetData()))
	}

	return values
}

// Clear removes this list without touching sibling fields or partitions.
func (l *ListField[T]) Clear() { l.scope.Clear() }

// Replace clears the list and appends values in argument order.
func (l *ListField[T]) Replace(values ...T) *ListField[T] {
	l.Clear()

	return l.Append(values...)
}
