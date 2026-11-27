package graph

import (
	"sort"
	"strings"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"google.golang.org/protobuf/proto"
)

// Composite is a dynamically shaped runtime value, usable directly, as a
// handwritten domain type's storage, or as the basis of generated types. Scalar
// fields are keyed entries; nested values use relative partitions. Its zero
// value is ready for use. Set retains supplied values and their pending edits.
//
// Mutations append immediately in call order across all attached views. Snapshots
// only seed reads. Runtime composition preserves value identity; emission
// consumes all sharing values, including on error. Do not copy an active root
// or use sharing values concurrently. Legacy MapEffect/ListEffect composition
// still transfers messages under those builders' consumption rules.
type Composite struct {
	state          *compositeState
	path           string
	field          *string
	scalar         bool
	legacyAbsolute bool
}

type compositeState struct {
	base     *pb.ReducedEffect
	inner    []*pb.Effect
	redirect *Composite
	values   map[runtimeKey]Value
}

// CompositeFromSnap borrows an immutable snapshot with relative partitions,
// without queuing effects. Nil seeds an empty composite.
func CompositeFromSnap(snap *pb.ReducedEffect) Composite {
	return Composite{state: &compositeState{base: snap}}
}

func (c *Composite) resolve() Composite {
	if c.state == nil {
		c.state = &compositeState{}
	}

	r := *c
	for r.state.redirect != nil {
		parent := r.state.redirect.resolve()
		if r.path == "" && r.field == nil {
			r.field = parent.field
		}

		r.path = partitionJoin(parent.path, r.path)
		r.state = parent.state
	}

	return r
}

func (c *Composite) owner() *compositeState { return c.resolve().state }

func partitionJoin(parent, child string) string {
	if parent == "" {
		return child
	}

	if child == "" {
		return parent
	}

	return parent + "/" + child
}

func partitionWithin(path, parent string) bool {
	return parent == "" || path == parent || strings.HasPrefix(path, parent+"/")
}

// Child returns an attached view sharing this owner's snapshots and effect
// sequence. Merely obtaining a child queues nothing. Repeated calls observe the
// same state. Names are individual path segments; '/' and '~' use JSON Pointer
// escaping to distinguish literal names from nesting.
func (c *Composite) Child(name string) Composite {
	segment := strings.ReplaceAll(strings.ReplaceAll(name, "~", "~0"), "/", "~1")
	if segment == "" {
		panic("composite child name must not be empty")
	}

	r := c.resolve()

	return Composite{state: r.state, path: partitionJoin(r.path, segment)}
}

func (c *Composite) current() *pb.ReducedEffect {
	r := c.resolve()

	snapshot := effects.ReduceChain(r.state.base, r.state.inner)
	if r.path != "" {
		snapshot = snapshot.GetPartitions()[r.path]
	}

	if r.field != nil {
		data := snapshot.GetNetAdds()[*r.field].GetData()
		if data == nil {
			return nil
		}

		return &pb.ReducedEffect{Scalar: data, Merge: data.GetMerge(), Collection: pb.CollectionKind_SCALAR}
	}

	return snapshot
}

// Snapshot evaluates this subtree and returns a read-only snapshot with child
// partition paths relative to this view. It does not read storage or emit data.
func (c *Composite) Snapshot() *pb.ReducedEffect {
	r := c.resolve()
	if r.field != nil {
		return c.current()
	}

	return PartitionSnapshot(effects.ReduceChain(r.state.base, r.state.inner), r.path)
}

// PartitionSnapshot projects a flat partition subtree into a composite snapshot.
// For example, partition "value" becomes the root and "value/profile" becomes
// "profile". Siblings are excluded. A missing root with existing descendants is
// valid. The result borrows immutable descendants; keep the input immutable.
func PartitionSnapshot(snapshot *pb.ReducedEffect, partition string) *pb.ReducedEffect {
	if partition == "" {
		return snapshot
	}

	root := snapshot.GetPartitions()[partition]

	var children map[string]*pb.ReducedEffect

	prefix := partition + "/"
	for path, child := range snapshot.GetPartitions() {
		if strings.HasPrefix(path, prefix) {
			if children == nil {
				children = make(map[string]*pb.ReducedEffect)
			}

			children[strings.TrimPrefix(path, prefix)] = child
		}
	}

	if len(children) == 0 {
		return root
	}

	if root == nil {
		root = &pb.ReducedEffect{}
	} else {
		root = proto.Clone(root).(*pb.ReducedEffect)
	}

	root.Partitions = children

	return root
}

// Field returns a locally reduced scalar field, or nil if absent. Its data is
// borrowed and immutable. Generated accessors decode it into ordinary Go values.
func (c *Composite) Field(name string) *pb.DataEffect {
	return c.current().GetNetAdds()[name].GetData()
}

// ApplyField transfers a fresh data message into this view's keyed collection,
// preserving its operation and merge rule. The message and buffers are consumed.
func (c *Composite) ApplyField(name string, data *pb.DataEffect) {
	data.Id = []byte(name)
	data.Collection = pb.CollectionKind_KEYED
	c.apply([]*pb.Effect{{Kind: &pb.Effect_Data{Data: data}}})
}

func (c *Composite) apply(batch []*pb.Effect) {
	r := c.resolve()
	for _, effect := range batch {
		effect.Virtual = []byte(r.path)
		if r.field != nil {
			effect.GetData().Collection = pb.CollectionKind_KEYED
			effect.GetData().Id = []byte(*r.field)
		}

		r.state.inner = append(r.state.inner, effect)
	}
}

// Clear queues removals for this view and its currently observed descendants.
// Siblings remain intact. Previously obtained child views remain attached and
// can be used to add new effects after the clear. Unknown descendants are also
// removed; Clear does not enumerate or remove unobserved concurrent partitions.
func (c *Composite) Clear() {
	r := c.resolve()
	if r.field != nil {
		c.apply([]*pb.Effect{{Kind: &pb.Effect_Data{Data: &pb.DataEffect{Op: pb.EffectOp_REMOVE_OP}}}})

		return
	}

	snapshot := c.Snapshot()

	paths := make([]string, 0, len(snapshot.GetPartitions()))
	for path := range snapshot.GetPartitions() {
		paths = append(paths, path)
	}

	sort.Strings(paths)

	paths = append([]string{""}, paths...)
	for _, path := range paths {
		c.owner().inner = append(c.owner().inner, &pb.Effect{
			Virtual: []byte(partitionJoin(r.path, path)),
			Kind:    &pb.Effect_Data{Data: &pb.DataEffect{Op: pb.EffectOp_REMOVE_OP, Collection: pb.CollectionKind_SCALAR}},
		})
	}
}

func (c *Composite) getEffects() []*pb.Effect {
	r := c.resolve()
	if r.path == "" && r.field == nil {
		return r.state.inner
	}

	var batch []*pb.Effect

	for _, effect := range r.state.inner {
		if !partitionWithin(string(effect.GetVirtual()), r.path) {
			continue
		}

		if r.field != nil && (string(effect.GetVirtual()) != r.path || string(effect.GetData().GetId()) != *r.field) {
			continue
		}

		batch = append(batch, effect)
	}

	return batch
}

// effectsAtPartition mounts runtime subtrees for ordinary emission, preserving
// explicit partitions on standalone low-level builders.
func (c *Composite) effectsAtPartition(partition string) []*pb.Effect {
	r := c.resolve()

	batch := c.getEffects()
	for _, effect := range batch {
		path := string(effect.GetVirtual())
		if r.path != "" {
			path = strings.TrimPrefix(strings.TrimPrefix(path, r.path), "/")
		}

		if r.field != nil {
			effect.GetData().Collection = pb.CollectionKind_SCALAR
			effect.GetData().Id = nil
		}

		if c.legacyAbsolute && r.path == "" && r.field == nil && len(effect.GetVirtual()) > 0 {
			continue
		}

		effect.Virtual = []byte(partitionJoin(partition, path))
	}

	return batch
}

// Emit transfers this subtree's pending effects to a logical key, preserving
// relative partitions and call order. It does not flush the context. Use tips
// from the snapshot read. The owner and all views are consumed, even on error.
func (c *Composite) Emit(key string, ctx *effects.Context, tips []effects.Tip) error {
	return c.EmitPartition(key, "", ctx, tips)
}

// EmitPartition mounts this subtree at partition, prefixing every relative child
// path. It preserves sibling partitions and does not emit snapshot data or flush.
// After emission, including failure, do not reuse the owner or any attached view.
func (c *Composite) EmitPartition(key, partition string, ctx *effects.Context, tips []effects.Tip) error {
	for _, effect := range c.effectsAtPartition(partition) {
		effect.Key = []byte(key)
		if err := ctx.Emit(effect, tips); err != nil {
			return err
		}
	}

	return nil
}
