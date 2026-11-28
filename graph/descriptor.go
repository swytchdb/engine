package graph

import (
	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
)

type descriptor struct {
	key    string
	tips   []effects.Tip
	fields *DescriptorFields
	inner  []*pb.Effect // Graph initialization precedes the generated body's effects.
}

// Emit transfers graph initialization followed by pending algorithm and entry-point
// effects in mutation order, using tips. It neither flushes c nor refreshes the
// stored frontier. Snapshots are never emitted. Emission consumes the descriptor
// and its aliases, even on error; abort or restore a savepoint to roll back.
func (d *descriptor) Emit(forKey string, c *effects.Context, tips []effects.Tip) error {
	for _, e := range d.inner {
		e.Key = []byte(forKey)
		if err := c.Emit(e, tips); err != nil {
			return err
		}
	}

	return d.fields.Emit(forKey, c, tips)
}

// getKey returns the logical application key of this descriptor.
func (d *descriptor) getKey() string { return d.key }

// getTips returns the borrowed descriptor frontier captured at load time.
// Emission does not advance it. It is distinct from every node frontier.
func (d *descriptor) getTips() []effects.Tip { return d.tips }

// GetEntryPoint returns the locally observed address for tag, including pending
// edits, or nil when absent. The returned bytes can be modified independently.
func (d *descriptor) GetEntryPoint(tag string) Address {
	if address, ok := d.fields.EntryPoint().Get(tag); ok {
		return Address(address)
	}

	return nil
}

// AddEntryPoint assigns address to the raw tag in the entry_point map. Repeating
// a tag replaces its address. It neither creates the node nor adds a causal edge.
func (d *descriptor) AddEntryPoint(tag, address string) {
	d.fields.EntryPoint().Set(tag, address)
}

// RemoveEntryPoint queues removal of tag, even when absent. The referenced node
// is not deleted.
func (d *descriptor) RemoveEntryPoint(tag string) { d.fields.EntryPoint().Remove(tag) }

// DescriptorFromSnap seeds the generated body from snap and retains tips as the
// descriptor observation at logical key. Snapshots and tips must remain immutable.
// No effects are queued and no storage is read.
//
// No type-tag or shape validation is performed. Nil seeds an empty builder.
// The algorithm is a root keyed entry; entry points use the entry_point partition.
// Older descriptor layouts are not migrated.
func DescriptorFromSnap(key string, snap *pb.ReducedEffect, tips []effects.Tip) (*descriptor, error) {
	fields, err := DescriptorFieldsFromSnap(snap)
	if err != nil {
		return nil, err
	}

	return &descriptor{key: key, tips: tips, fields: fields}, nil
}

// NewDescriptor queues TYPE_GRAPH and algorithm initialization at logical key.
// It creates an empty entry-point map without reading storage or obtaining tips.
// Emit and commit this descriptor first, then reload its snapshot and tips before
// adding nodes. Emission consumes this builder; it does not advance its tips.
func NewDescriptor(key string, algorithm string) *descriptor {
	return &descriptor{
		key:    key,
		fields: NewDescriptorFields().SetAlgorithm(algorithm),
		inner:  []*pb.Effect{{Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{TypeTag: pb.ValueType_TYPE_GRAPH}}}},
	}
}

// Descriptor exposes entry-point edits and emission for a logical graph key.
// Its observed tips belong to the descriptor, not a node. It is a graph operation
// with its own emission API; it does not implement Value or Emits. The interface
// is package-owned, and instances and their aliases are not safe for concurrent use.
type Descriptor interface {
	Emit(forKey string, c *effects.Context, tips []effects.Tip) error
	AddEntryPoint(tag string, address string)
	RemoveEntryPoint(tag string)
	GetEntryPoint(tag string) Address
	getKey() string
	getTips() []effects.Tip
}
