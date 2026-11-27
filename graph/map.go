package graph

import (
	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
)

// MapFromSnap borrows snap as the base of a keyed collection. Keys are the
// existing raw element IDs; no private encoding, validation, or value decoding is
// performed. The error is currently always nil. Nil seeds an empty map.
//
// Keep snap immutable. Only subsequently composed effects are emitted, never the
// snapshot. Pass tips from the same read when emitting. U is an effect builder,
// not the Go type of the stored value.
func MapFromSnap[U Emits](snap *pb.ReducedEffect) (*MapEffect[U], error) {
	return &MapEffect[U]{base: snap}, nil
}

// MapEffect composes U's pending data effects at raw byte keys. For example:
//
//	m := (&MapEffect[*IntEffect]{}).Set("count", (&IntEffect{}).Add(1))
//
// Set overwrites each input effect's collection and ID, preserving its operation,
// merge rule, placement, payload, and virtual partition. It takes ownership of
// those messages without evaluating the input's snapshot. Do not reuse the input
// builder or its aliases after composition. String keys are converted directly
// to byte IDs without encoding.
//
// The zero value has no base or pending effects. Methods mutate the receiver
// and return the same pointer for chaining. Pointer aliases observe those edits.
// Copying the struct does not clone messages, slices, or the borrowed snapshot;
// do not branch shallow copies or use aliases concurrently.
//
// Emit and EmitPartition consume the builder and all aliases, even on error.
// Start a fresh builder or read a new snapshot before further work.
type MapEffect[U Emits] struct {
	inner        []*pb.Effect
	base         *pb.ReducedEffect
	runtime      *Composite
	runtimeIndex int
}

// Set consumes value's pending effects and targets them at key, using key's
// bytes directly as DataEffect.Id, without encoding.
// A replacement effect replaces the entry; additive effects update it according
// to their merge rules. No pending effects means no change. The receiver is updated in place.
func (s *MapEffect[U]) Set(key string, value U) *MapEffect[U] {
	for _, e := range value.getEffects() {
		e.GetData().Id = []byte(key)
		e.GetData().Collection = pb.CollectionKind_KEYED
		s.inner = append(s.inner, e)
	}

	s.syncRuntime()

	return s
}

// Get reduces the borrowed snapshot and pending effects, then passes key's
// entry to decoder. The decoder receives a ReducedElement, not a whole-key
// ReducedEffect; it must treat the entry and its data as immutable and return
// a fresh builder seeded from that value, with no pending effects.
// Use StringFromElement, IntFromElement, or FloatFromElement for scalar entries:
//
//	value, err := m.Get("count", IntFromElement)
//
// Get does not read storage or queue effects. Changes to the returned builder
// must be composed back with Set. A missing or removed key returns U's zero
// value and a nil error without calling decoder. Decoder errors propagate
// unchanged. Call Get before emitting the map.
func (s *MapEffect[U]) Get(key string, decoder func(*pb.ReducedElement) (U, error)) (U, error) {
	entry, ok := s.entries()[key]
	if !ok {
		var zero U

		return zero, nil
	}

	return decoder(entry)
}

// Remove appends a keyed removal using key's bytes directly as the element ID.
// Removing an absent key is allowed. The receiver is updated in place.
func (s *MapEffect[U]) Remove(key string) *MapEffect[U] {
	d := &pb.DataEffect{
		Op: pb.EffectOp_REMOVE_OP, Collection: pb.CollectionKind_KEYED,
		Merge: pb.MergeRule_LAST_WRITE_WINS, Id: []byte(key),
	}
	s.inner = append(s.inner, &pb.Effect{Kind: &pb.Effect_Data{Data: d}})
	s.syncRuntime()

	return s
}

// Replace appends a scalar removal followed by Set for each entry in values.
// String keys are converted directly to bytes. Entry emission order follows Go
// map iteration and is unspecified. The input builders are consumed.
//
// The borrowed base and earlier effects remain in the builder; the removal resets
// state during reduction. An empty replacement records only the removal. At the
// root it removes the whole key, including metadata and partitions; EmitPartition
// scopes it to the selected nonempty partition. The receiver is updated in place.
func (s *MapEffect[U]) Replace(values map[string]U) *MapEffect[U] {
	s.inner = append(s.inner, &pb.Effect{
		Kind: &pb.Effect_Data{
			Data: &pb.DataEffect{
				Op:    pb.EffectOp_REMOVE_OP,
				Merge: pb.MergeRule_LAST_WRITE_WINS,
			},
		},
	})

	ret := s
	for k, v := range values {
		ret = ret.Set(k, v)
	}

	s.syncRuntime()

	return ret
}

// Emit transfers pending effects to c in order without flushing it. forKey is
// a logical application key; the context derives its internal address. Pass the
// snapshot's tips, or nil to let the context resolve dependencies. The same
// tips may be reused within one pending context: later effects chain from the
// previous emission for the key. After Flush or Abort, read fresh tips.
//
// The builder and all aliases are consumed even on error. Earlier effects may
// already be pending in c; abort or restore a savepoint if rollback is required.
func (s *MapEffect[U]) Emit(forKey string, c *effects.Context, tips []effects.Tip) error {
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
// handling rules apply.
func (s *MapEffect[U]) EmitPartition(forKey, partition string, c *effects.Context, tips []effects.Tip) error {
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

func (s *MapEffect[U]) getEffects() []*pb.Effect {
	return s.inner
}

func (s *MapEffect[U]) entries() map[string]*pb.ReducedElement {
	if s.runtime != nil {
		return s.Runtime().current().GetNetAdds()
	}

	return effects.ReduceChain(s.base, s.inner).GetNetAdds()
}

// Runtime adapts this low-level builder to the public Value protocol. Existing
// builder methods retain their original composition and ownership rules.
func (s *MapEffect[U]) Runtime() *Composite {
	if s.runtime == nil {
		s.runtime = &Composite{state: &compositeState{base: s.base, inner: append([]*pb.Effect(nil), s.inner...)}, scalar: false, legacyAbsolute: true}
		s.runtimeIndex = len(s.inner)
	}

	s.syncRuntime()

	return s.runtime
}

func (s *MapEffect[U]) syncRuntime() {
	if s.runtime != nil && s.runtimeIndex < len(s.inner) {
		s.runtime.apply(s.inner[s.runtimeIndex:])
		s.runtimeIndex = len(s.inner)
	}
}
