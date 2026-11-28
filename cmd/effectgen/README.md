# Composite effects

Declare fields and their reduction semantics:

```text
type CritMeta {
    isLeaf: bool
    bytePos: uint32
    otherBits: uint8
    key: string
}
```

Generate a Go type in the package where it will be used:

```sh
go run ./cmd/effectgen -schema path/to/critmeta.effects -package meta -out path/to/critmeta_gen.go
```

The checked-in example lives in `graph/critbit/meta`. Regenerate it with:

```sh
go generate ./graph/critbit/meta
```

The generated type implements the public `graph.Value` protocol, so it can be a node's value:

```go
value := meta.NewCritMeta().
SetIsLeaf(true).
SetBytePos(12).
SetOtherBits(128).
SetKey("hello")
node := g.CreateNode("root", value)
node.Value().SetBytePos(13)
position := node.Value().BytePos()
```

For an existing node, pass `meta.CritMetaType` to `g.GetNode`. For a new
graph, commit the descriptor and reload its snapshot and tips before creating
nodes. `g.Flush(c)` queues the graph's effects; `c.Flush()` commits them.

Setters immediately append effects to one shared buffer. Getters reduce the
borrowed snapshot and that buffer, so local reads include pending edits without
requiring another `Set`. Loading a snapshot and reading fields queue nothing.
There is no separate dirty tracking or cache. Each getter currently reduces the
whole local owner, including its partitions; this prioritizes correct composition
over read performance.

The zero value works, and `NewCritMeta` is optional. Missing fields read as Go
zero values. Explicitly setting false, zero, or empty bytes emits a replacement.
Byte setters copy their input, and byte getters return a copy. Snapshots remain
borrowed and must stay immutable. Emission consumes sharing values and
all aliases, even on error; reload before further use. Use one evolving builder,
without shallow copies or concurrent access.

## Types and policies

| Declaration                | Go value  | Generated mutation    | Wire representation               |
|----------------------------|-----------|-----------------------|-----------------------------------|
| `isLeaf: bool`             | `bool`    | `SetIsLeaf(bool)`     | `IntVal`, 0 or 1, last write wins |
| `bytePos: uint32`          | `uint32`  | `SetBytePos(uint32)`  | `IntVal`, last write wins         |
| `otherBits: uint8`         | `uint8`   | `SetOtherBits(uint8)` | `IntVal`, last write wins         |
| `count: int64`             | `int64`   | `SetCount(int64)`     | `IntVal`, last write wins         |
| `score: float64`           | `float64` | `SetScore(float64)`   | `FloatVal`, last write wins       |
| `name: string`             | `string`  | `SetName(string)`     | raw bytes, last write wins        |
| `key: bytes`               | `[]byte`  | `SetKey([]byte)`      | raw bytes, last write wins        |
| `balance: int64 additive`  | `int64`   | `AddBalance(int64)`   | `ADDITIVE_INT` delta              |
| `weight: float64 additive` | `float64` | `AddWeight(float64)`  | `ADDITIVE_FLOAT` delta            |
| `lastSeen: int64 max`      | `int64`   | `MaxLastSeen(int64)`  | `MAX_INT` candidate               |

`replace` may be written explicitly. Additive and max fields intentionally do
not get a generated replacement setter. Accessor names capitalize the first letter
and each underscore-separated word (`isLeaf` becomes `IsLeaf`, `entry_point` becomes `EntryPoint`). Storage names stay
exactly as written in the schema. Names that produce the same Go method, including
setters and runtime accessors, are rejected. Schemas may contain multiple types,
comments, and optional commas or semicolons between fields. Duplicate generated
names, reserved methods, unsupported types, and invalid policies are errors.

Scalar fields are ordinary keyed entries whose IDs are the schema's literal field
names. A node stores that keyed collection in its existing `value` partition;
standalone `Emit` and `EmitPartition` also work. There is no private field-name
encoding. Unknown entries in a snapshot survive updates to known fields.
Loading does not validate or migrate existing field payloads: the earlier
handwritten critbit string/binary representation is not this schema's numeric
representation.

## Nested composites and collections

Named types become nested composites. They may refer to types declared later in
the same schema. Collection fields support `set<string>`, `map<string, scalar>`,
and `list<scalar>` using any of the scalar types above as map/list values.

```text
type Account {
    name: string
    balance: int64 additive
    lastSeen: int64 max
    tags: set<string>
    attributes: map<string, string>
    messages: list<string>
    profile: Profile
}

type Profile {
    displayName: string
    contact: Contact
}

type Contact {
    email: string
}
```

Accessors return retained runtime values sharing one effect buffer:

```go
account := NewAccount()
profile := account.Profile()
profile.SetDisplayName("Rob")
profile.Contact().SetEmail("rob@example.test")
account.AddBalance(500)
account.Tags().Add("active")
account.Attributes().Set("locale", "en")
account.Messages().Append("Hello")

name := account.Profile().DisplayName() // "Rob"
locale, found := account.Attributes().Get("locale")
messages := account.Messages().Values()
```

There is no second `Set` to attach these changes. Views obtained before other
edits remain attached, and interleaved writes to different fields retain their
original order. Merely accessing a child creates no effects or storage entries.
Zero-value generated roots work without constructor calls.

| Accessor         | Operations                                                                                                                                                         |
|------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Nested composite | Generated fields, `Clear()`                                                                                                                                        |
| Set              | `Add(values...)`, `Remove(values...)`, `Contains(value)`, `Values()`, `Replace(values...)`, `Clear()`                                                              |
| Map              | `Set(key, value)`, `Get(key) (value, found)`, `Remove(key)`, `Values()`, `Replace(map)`, `Clear()`                                                                 |
| List             | `Append(values...)`, `Prepend(values...)`, `Insert(index, value)`, `Set(index, value)`, `Remove(index)`, `Get(index)`, `Values()`, `Replace(values...)`, `Clear()` |

Map keys and set members use raw string bytes as element IDs. Set membership
uses the engine's keyed insert/remove semantics. Sets return sorted members;
maps return Go maps. Lists delegate positioning and element identity to
`ListEffect`; invalid indices panic. Byte values in maps/lists are copied on
input and output. Collection values are ordinary Go values, without scalar
builder wrappers or decoder arguments. These collections can also be created
independently with `NewList`, `NewMap`, and `NewStringSet`, then composed later.

## Partition mapping

Scalar fields remain ordinary keyed entries. Nested objects and collections each
occupy their own flat engine partition, named by their relative field path:

| Relative partition | Stored collection                       |
|--------------------|-----------------------------------------|
| root               | Account's `name`, `balance`, `lastSeen` |
| `profile`          | Profile's `displayName`                 |
| `profile/contact`  | Contact's `email`                       |
| `tags`             | keyed set                               |
| `attributes`       | keyed map                               |
| `messages`         | ordered list                            |

Schema identifiers are literal path segments. The low-level `Composite.Child`
method also supports names containing `/` or `~`, escaping them as `~1` or `~0`
respectively. This escaping applies to partition segments, not map keys or scalar
field IDs. Empty child names are rejected.

The reducer treats these names as flat partition addresses. Generated composites
handle the path mapping; this is not a change to reducer behavior or a dependency
on the JSON module's opaque container IDs.

`EmitPartition(key, "account", ctx, tips)` prefixes all paths with `account`.
Generated composite node values are automatically mounted under `value`, giving
paths such as `value/profile/contact` and `value/messages`. Scalar node values
instead occupy the root keyed entry `value`. Standalone low-level builders retain
their explicit-partition behavior.

`Graph.GetNode` loads the generated node body and binds its value using the
supplied runtime Type. The bound value sees paths relative to its own subtree. When loading an explicitly mounted
standalone value, do the
projection yourself:

```go
snapshot, tips, err := ctx.GetSnapshot("key")
// Handle err.
account, err := AccountFromSnap(graph.PartitionSnapshot(snapshot, "account"))
// Handle err, then edit account.
err = account.EmitPartition("key", "account", ctx, tips)
```

A root with no scalar fields but existing child partitions is valid. The
projection includes those descendants and excludes similarly named siblings.
Snapshots seed the owner's read view and are never emitted as data.

## Clearing and ownership

`profile.Clear()` queues removals for that partition and all currently observed
descendant partitions, including unknown ones. Sibling fields remain intact.
Previously obtained views stay attached, so this works in call order:

```go
contact := account.Profile().Contact()
account.Profile().Clear()
contact.SetEmail("new@example.test")
```

`account.Clear()` clears the observed whole composite. When mounted under a
partition or in a node, it preserves the containing key's other partitions,
including node relationships. Clears enumerate the local observed subtree;
they do not discover or delete unobserved concurrent partitions.

Emission consumes the entire owner and its attached views, including on error.
Emitting a child on its own selects just that child's pending effects and rebases
its partition paths, but still consumes the shared owner. Collection accessors
are emitted through their owner. After commit or abort, load a fresh value and
read fresh tips before continuing.

`graph.Composite` provides dynamic composition, the shared buffer, relative views,
snapshot projection, and the `Field`/`ApplyField` escape hatch. Collection facades feed the same buffer;
ordered collection operations reuse `ListEffect`. No new context API is needed.

The generator currently requires scalar map/list elements and string set
members. Collections of composites, collections of collections, schema imports,
and automatic field-type migrations are not implemented. Ordinary
`MapEffect.Set` still retargets all supplied effects to one entry; it does not
construct a nested composite. Use the generated attached accessors for nesting.

The complete Account example and deeper nesting are exercised in
`internal/effectschema/testmodel`. The [critbit tree](../../graph/critbit/README.md)
uses generated metadata for routing and separate typed graph nodes for payloads.

## Runtime protocol and generic types

The generator targets the same runtime library used directly by dynamic and
handwritten objects. See [Runtime values and composition](../../graph/values.md)
for the public Value/Type protocol, standalone values, and ownership rules.

```text
type Box<T> {
    value: T
    label: string
}

type Catalog {
    title: Box<string>
}
```

`Box<string>` becomes `Box[*graph.String]`. In Go, construct it with
`NewBox(graph.StringType)`. A handwritten domain type can be supplied through
its own `graph.Type[T]`, without generating that type.
`BoxType(domainType)` supplies the runtime type for `Graph.GetNode`.

Scalar accessors retain their convenient Go reads and setters; an additional
`NameValue()` accessor exposes the standard runtime value for a `name` field.
Nested, generic, and collection fields gain Set<Field> methods accepting
standalone runtime values and retaining their identity.

Multiple type parameters and named instantiations are supported. Generic types
require constructors with runtime Type descriptors. Nested generic argument
expressions and runtime-valued collection elements (`list<T>`) remain outside
this iteration; use a generic field with a concrete collection Type instead.

## Generating the runtime's graph bodies

The runtime uses this generator too: [node.effects](../../graph/node.effects)
defines `NodeFields<T>`, which composes the node value, regions, and four
ordered relationship lists. `Node[T]` wraps that generated body and retains its graph API,
addressing, observed tips, and graph-aware emission. Node itself does not implement
Value. See [Generated node body](../../graph/values.md#generated-node-body) for
storage and emission behavior.

[descriptor.effects](../../graph/descriptor.effects) defines `DescriptorFields`
for the algorithm and entry points. Its wrapper retains graph initialization and
observed tips. The algorithm uses a keyed entry; `EntryPoint()` binds the literal
`entry_point` partition. The descriptor exposes its own emission interface, with
no ordinary-value or metadata API. See
[Generated descriptor body](../../graph/values.md#generated-descriptor-body).

Regenerate both bodies with:

```sh
go generate ./graph
```

This invokes effectgen with `-runtime`, which emits references to the runtime
types in the same package without importing that package into itself. Application
schemas should omit this flag and import the runtime normally. Checked-in output
is verified against its schema by the generator tests.
