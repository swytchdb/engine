# Runtime values and composition

The runtime library is independent of the schema compiler. Dynamic objects,
handwritten domain types, and generated types use the same public protocol:

```go
type Value interface {
    Runtime() *Composite
}
```

`Runtime` must return the value's persistent runtime, not a new object on each
call. It owns the observed snapshot, pending operations, and any composed values.
It does not prescribe the Go type returned by materialized reads. Nodes accept
`Node[T Value]`; a value does not have to implement `Emit` or a package-private
method to become a node's value.

## Independent values

```go
s := graph.NewString("Hello")
s.Append(" world")

node := tree.CreateNode(address, s)
node.Value().Append("!")
// node.Value() == s, and s.Value() == "Hello world!"
```

`String`, `Bytes`, `Int64`, `Float64`, `Uint32`, `Uint8`, and `Bool` are standalone
values. Constructors queue replacements. `Set` changes the existing value;
`String` also supports `Append`, `Prepend`, and byte-offset `Insert`; `Int64` and
`Float64` support `Add`, and `Int64` supports `Max`. They use the existing effect
representations and reducers. Byte inputs and materialized outputs are copied.

Use `graph.Emit(value, key, ctx, tips)` for any Value, including handwritten
implementations that do not embed library types. `graph.EmitPartition` prefixes
relative partition paths. These functions queue effects; the caller commits
with `ctx.Flush()`. Values embedding Composite also have emission methods.

## Dynamic composition

No schema is needed:

```go
object := graph.NewComposite()
greeting := graph.NewString("Hello")
greeting.Append(" world")
if err := object.Set("greeting", greeting); err != nil {
    return err
}
greeting.Append("!")

value, found := object.Lookup("greeting")
// found is true and value == greeting.

profile := graph.NewComposite()
if err := profile.Set("name", graph.NewString("Rob")); err != nil {
    return err
}
if err := object.Set("profile", profile); err != nil {
    return err
}
```

Set retains the runtime value and transfers its pending operations to the shared
sequence at the composition point. Later mutations through either reference
participate in that sequence. Existing nested handles retain their identity when
their containing object is composed. Replacing the runtime at a location does not
invalidate older references to that location; they continue to observe it.

A standalone value can be composed once. Attaching the same instance to another
location is rejected, as are cycles. Make an independent value from its
materialized value when a copy is intended. Composing a value applies its pending
operations; it does not implicitly clear an existing destination subtree.

Scalars occupy raw keyed entries. Structured values and collections occupy
relative partitions. `Lookup` retains live types already present in memory.
When loading an untyped snapshot, wire integers become Int64, floats become
Float64, bytes become String, and partitioned children become Composite. The wire
representation does not distinguish bool from int64, or bytes from string; use a
Type and Bind when those logical distinctions matter.

## Handwritten domain types

A domain object does not have to embed a library type or use generated code:

```go
type Person struct {
    state graph.Composite
}

func (p *Person) Runtime() *graph.Composite { return &p.state }

func (p *Person) Name() *graph.String {
    return graph.Bind(&p.state, "name", graph.StringType)
}

var PersonType = graph.Type[*Person]{
    New: func() *Person { return &Person{} },
}
```

`Bind` returns the retained typed value at a location. Repeated calls return the
same instance. Missing fields bind an empty value without queuing effects; reads
then return its zero value. A conflicting runtime type at that location is a
programming error. Bind selects keyed entries for scalar types and partitions
for structured types.

A Type supplies a fresh empty implementation through its New factory. It must
not queue initial effects. `PersonType.FromSnapshot(snapshot)` constructs an
object and seeds its runtime without emitting the snapshot. Type factories are
also how generic generated objects create their type parameters: ordinary Go
runtime type parameters alone cannot allocate an arbitrary implementation of
Value or identify its logical storage shape.

## Collections

The collection implementations are also available independently of generation:

```go
messages := graph.NewList(graph.StringCodec, "Hello")
attributes := graph.NewMap(graph.StringCodec)
attributes.Set("locale", "en")
tags := graph.NewStringSet("active")

if err := object.Set("messages", messages); err != nil {
    return err
}
messages.Append("World")
```

They implement Value. `ListType(codec)`, `MapType(codec)`, and `StringSetType`
provide factories for loading and generic composition. The generator targets
these same types, factories, and codecs, with no generated-code dependency in
the runtime library. Ordered operations still delegate to ListEffect.

This iteration retains materialized scalar collection APIs: e.g. NewList with
StringCodec returns `*ListField[string]`, whose Values returns `[]string`.
It does not yet implement lists of retained runtime element instances such as
`List[*String]`. Collection elements do not acquire independently mutable runtime
handles through Get. The new Value contract applies to the collection itself;
element-level runtime composition remains a separate step.

## Generated values and generics

```text
type Box<T> {
    value: T
    label: string
}

type Catalog {
    title: Box<string>
}
```

The generated Go type is `Box[T graph.Value]`. The DSL's `Box<string>` maps to
`*Box[*graph.String]`. Constructors supply runtime type descriptors:

```go
box := NewBox(graph.StringType)
s := graph.NewString("Hello")
if err := box.SetValue(s); err != nil {
    return err
}
box.Value().Append(" world")

people := NewBox(PersonType)
people.Value().Name().Set("Rob")
```

Generated fields are ordinary runtime bindings. Scalar convenience accessors
still materialize Go values (`account.Name()`); `account.NameValue()` exposes the
standard String so callers can append or perform other scalar operations.
Generated mutation conveniences call that standard value. Nested and collection
accessors retain their instances, and Set<Field> accepts standalone values.

Generated `BoxType(PersonType)` can be passed directly to
`Graph.GetNode`. A handwritten Person can therefore sit inside a generated Box,
which can sit inside a handwritten or generated object, or become a graph node.

The generator supports type parameters, multiple parameters, and named generic
instantiations with scalar, named non-generic, or enclosing parameter arguments.
Generic constructors are required to supply descriptors; a zero-valued generic
struct has no factory for its parameters. Deep generic argument expressions and
`list<T>`/`map<string,T>` are not supported in this iteration. A generic field can
still hold a collection through `NewBox(graph.ListType(graph.StringCodec))`.

## Ownership and compatibility

Runtime composition retains usable aliases. Emission consumes the sharing
values, including on error; do not mutate, inspect, or emit them again. Reload
from the next observation for subsequent work. These values are not safe for
concurrent mutation. Abort or restore the context's savepoint after a failed
emission when rollback is required.

Snapshots only seed reads. Composing a loaded value does not synthesize insertion
effects from its historical value. To insert a materialized copy elsewhere,
construct a fresh value explicitly. Reads of generic or dynamic values do not
queue effects.

The original StringEffect/IntEffect/FloatEffect, MapEffect, and ListEffect APIs
remain available and bridge to Value. Their original low-level composition
still consumes pending messages and retains its existing ownership rules. New
runtime values, dynamic composition, and generated types use the public runtime
protocol instead. The graph descriptor remains a special graph operation; a
Node is still a graph-aware wrapper, not an ordinary Value.

## Generated node body

`Node[T Value]` keeps its graph API: `Value`, `SetValue`, `AttachUp`, `AttachDown`,
`AttachLeft`, `AttachRight`, `SetRegion`, `Address`, and `Delete`.
Its value, regions, and relationships are composed by `NodeFields<T>`, generated
from [node.effects](node.effects). The wrapper supplies the internal node address
and observed graph/node tips when emitting. It does not implement Value or Emits.

```text
type NodeFields<T> {
    value: T
    regions: map<string, string>
    up: list<string>
    down: list<string>
    left: list<string>
    right: list<string>
}
```

The generated body uses the same storage rules as any composite. A scalar node
value occupies the root keyed entry `value`; a composite or collection value
occupies the `value` partition and its descendants. Regions and relationships
occupy `regions`, `up`, `down`, `left`, and `right` partitions. This changes the
older scalar-in-a-partition node layout; existing data is not migrated.
`Graph.GetNode(address, ctx, typ)` loads the generated body using a runtime Type:

```go
node, err := g.GetNode(address, ctx, graph.StringType)
// Handle err.
node.Value().Append("!")
```

`Value()` binds the runtime value on first access and retains its identity.
Scalar, handwritten, generated, generic, and collection Types use the same path;
no per-node decoder is needed. Loading and binding emit nothing. An absent node
binds an empty value that can be edited directly, retaining its absence tips.
`GetNeighbors` accepts a runtime Type for the neighboring values as well.

Relationships are ordered lists of address slots. `Link` and `Attach*` append a
fresh slot, including repeated addresses. `NeighborAddresses` and `GetNeighbors`
preserve live slot order and never reconcile it. Readers can select the last
live slot while a writer appends a replacement and removes earlier slots.
`UnlinkAt(node, direction, index)` removes one observed slot by its ID;
`Unlink(from, to, direction)` removes all observed slots referring to that target.
Neither removes an unobserved concurrent append or deletes the target node.

Body mutations share one sequence in call order, including edits made through
the original standalone value.
`SetValue` composes the supplied pending operations, without emitting its
snapshot or implicitly clearing an existing subtree. Node deletion is emitted
after all body mutations. A fresh builder can reuse the deleted address.

Commit a new graph's descriptor first, then load a fresh Graph with its observed
snapshot and tips before attaching nodes. Generation does not change that
lifecycle. Regenerate the body from the repository root with `go generate ./graph`.

## Generated descriptor body

[descriptor.effects](descriptor.effects) defines the descriptor's stored fields:

```text
type DescriptorFields {
    algorithm: string
    entry_point: map<string, string>
}
```

`NewDescriptor` wraps that generated body with a logical graph key, observed tips,
and `TYPE_GRAPH` initialization. `AddEntryPoint`, `RemoveEntryPoint`, and
`GetEntryPoint` operate on its shared entry-point map. The generated accessor is
`EntryPoint()`; the storage partition remains `entry_point`.

Initialization emits first, followed by body mutations in call order. The
algorithm occupies the root keyed entry `algorithm`. No migration from the earlier
partitioned algorithm layout is included. Load with `DescriptorFromSnap`; loading
and reading queue no effects.

The public Descriptor interface declares its own `Emit` method and entry-point
operations. It does not embed Emits or expose Runtime, EmitPartition, or ordinary
value effects. Descriptor metadata wrappers and decoding are not currently part
of this API.

Commit descriptor initialization before creating nodes, then read fresh descriptor
tips. Emission consumes the descriptor and its aliases, including on error.
`go generate ./graph` regenerates both graph bodies.
