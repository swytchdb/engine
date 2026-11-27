# Critbit

`Crit[T graph.Value]` indexes arbitrary byte strings, including empty keys,
prefixes, and embedded zero bytes. Payloads can be runtime scalars, collections,
or generated/handwritten composites. Supply their runtime Type once:

```go
creation := engine.NewContext()
defer creation.Abort()
tree := critbit.Create("lookup", graph.StringType)
if err := tree.Flush(creation); err != nil {
    return err
}
if err := creation.Flush(); err != nil {
    return err
}

// Node writes observe the descriptor committed above.
write := engine.NewContext()
defer write.Abort()
tree, err := critbit.Open("lookup", write, graph.StringType)
if err != nil {
    return err
}
if err := tree.Insert("hello", graph.NewString("world")); err != nil {
    return err
}
if err := tree.Flush(write); err != nil {
    return err
}
if err := write.Flush(); err != nil {
    return err
}

read := engine.NewContext()
defer read.Abort()
tree, err = critbit.Open("lookup", read, graph.StringType)
if err != nil {
    return err
}
value, err := tree.Find("hello")
// Handle err; value.Value() is "world".
```

`Insert` replaces the observed value slots for an existing key. `Find` returns
the retained runtime value by loading `leaf/` + key and its last live payload
slot directly. It does not discover the root or routing branches. Missing and
deleted keys return T's zero value and a nil error. A stored key can be found
even while its routing path is a competing alternative. An explicit mutation through the returned value participates in the next Flush.
`Delete(key)` removes the observed value slots and returns whether any existed.
An empty stored string is distinct from a missing key.

`Scan(lower, upper string, limit int)` returns `([]Entry[T], error)` for
`[lower, upper)` in ascending byte order. Empty bounds are unbounded (an empty
lower includes the empty key), and `limit <= 0` is unlimited.
Deleted leaves do not count toward the limit. Each entry has
`Key string` and `Value T`; values are the same retained builders returned by
`Find`. It includes pending edits and skips deleted leaves. A live exact lower
key is included by direct lookup; with `limit == 1` that completes the scan
without any routing discovery. Subsequent keys are discovered through the last
live routing slots. Missing or deleted lower keys fall back to the normal seek.
Neither path reconciles earlier alternatives.
An empty scan returns nil, nil; a failed scan returns an error without partial
results. Scanning uses the context's node observations and does not make
multi-node publication atomic.

The lower bound seeks through the critical-bit path and compares a representative
leaf to account for compressed prefixes and skipped bits. It retains unexplored
right subtrees, then resumes ordered traversal from the first possible match.
Routing work is O(h + k), where h is tree height and k is the number of leaves
examined, including deleted leaves. A small limit near the end of the tree does
not reconstruct the preceding keys. Upper and limit bounds stop traversal early.

```go
entries, err := tree.Scan("a", "z", 100)
if err != nil {
    return err
}
for _, entry := range entries {
    fmt.Printf("%q: %s\n", entry.Key, entry.Value.Value())
}
```

Flush transfers effects to the supplied context; it does not commit it. Neither
Insert nor Delete starts a transaction. After emission, commit or abort and open
a fresh tree for subsequent work. Input values retain their identity when
attached, and follow the runtime's composition and aliasing rules. Their pending
operations are inserted; their snapshot seeds are never converted to effects.

## Links and concurrent writes

The descriptor's `root` entry always points to a stable sentinel node. Its Left
list contains the routing roots. Internal routing nodes use Left and Right
lists. Leaves contain the original key in generated `CritMeta` and reference
separate payload nodes through their Down list.

Leaves have graph-local names `leaf/` followed by the literal key bytes. This
gives concurrent insertions of the same key the same leaf identity. Branch names
are `branch/<bytePos>/<otherBits>/<prefix>`: decimal position and mask, followed
by the literal preceding bytes and, for data-bit tests, the current byte masked
to bits above the critical bit. The same common prefix is stored in the branch metadata
(`CritMeta.key`), so merging can compare prefixes without discovering a leaf.
This identity survives routing changes, keeping
concurrent edits to old branches reachable. Payloads receive fresh random names
under `value/`.
Addresses are produced by the graph API; no payload or key needs a private codec.

Readers use the last live payload slot. Reading never reconciles, removes slots,
or queues graph mutations. Point lookups and an exact scan lower key use direct
leaf addresses. Range continuation follows the last routing path, so keys only
reachable through earlier alternatives may be absent from that continuation
until a writer consolidates those paths.

Writers append replacements before removing earlier observed slots by ID.
Unobserved concurrent appends survive, even for the same key. When a write
encounters competing routing paths, it merges their structural differences:
disjoint prefixes gain a common branch, and a containing prefix merges only the
child on the overlapping side. Identical addresses are convergence points;
merging stops there without discovering that subtree's contents. Existing
references also let a wrapper absorb a competing reference to its own subtree.
Only changed edges are appended before their observed old slots are removed.

Conflicts inside a shared subtree remain for a writer that enters that path.
Reconciling a parent does not repair the entire index beneath it. This preserves
lazy discovery and avoids collecting, sorting, or rebuilding the shared contents.
Branch identities, leaf identities, and Down histories are retained. A payload
replacement uses the writer's supplied value; it does not merge payload contents
automatically.

Deletion keeps the routing leaf with no observed Down slots. This prevents an
older alternative path from resurrecting the deleted payload. Detached branch
and payload nodes remain stored; physical reclamation is not implemented here.

Context publication remains per key. This library does not make a multi-node
rewrite atomic. A missing routing node or child produces `ErrIncomplete`, which
can be retried with a fresh context while publication completes. Competing writes
need a later writer to consolidate the paths it observes; there is no background
reconciliation.

## Critical bits

Routing metadata records `bytePos` and `otherBits`. Ordinary masks select one bit
of the byte (`0x7f`, `0xbf`, ..., `0xfe`). The mask `0` tests whether the byte
exists, ordered before its data bits. This distinguishes `"a"` from `"a\x00"`
without modifying or encoding the stored key. Keys are limited to uint32 byte
positions.

## Comparable benchmarks

The shared harness in `cluster/graph_index_bench_test.go` compares critbit with
B+ on the same engine, request traces, and real loopback QUIC peers. Both use
caller-owned transactions. Reads include a fresh context and `Open`; writes
include `tree.Flush(context)` in preparation and measure `context.Flush()`
separately. Every peer's full contents are verified outside request timing.

```sh
go test ./cluster -run '^TestGraphIndexFivePeers$' -count=3 -timeout=3m
go test ./cluster -run '^$' -bench '^BenchmarkGraphIndexRead$' -benchmem -benchtime=300ms -count=3
go test ./cluster -run '^$' -bench '^BenchmarkGraphIndexEndToEnd$' -benchmem -benchtime=1x -count=3 -timeout=10m
```

For a single incremental size case:

```sh
GRAPH_INDEX_ALGORITHM=Critbit BPLUS_SCALE_ENTRIES=1000 BPLUS_SCALE_PEERS=5 \
  BPLUS_SCALE_DIR=/tmp/critbit-scale go test ./cluster -run '^$' \
  -bench '^BenchmarkGraphIndexScale$' -benchtime=1x -count=1 -timeout=15m
```

Use `GRAPH_INDEX_ALGORITHM=BPlus` for the matched control. The suite defaults to
modern `1.8.0` peers. The epsilon runner additionally enforces a five-minute
construction limit and records peak RSS, allocation rate, GC activity, and
retained heap. The shell command above has only the whole-process Go timeout.

Results and exact commands: [epsilon comparison](benchmarks/2026-11-27-epsilon-comparison/README.md).
The workload is a single writer; it does not measure concurrent conflict rates.
Critbit's tombstones and detached payloads are retained, whereas B+ merges
remove retired pages, so identical insert/delete requests do not imply identical
physical reclamation.


For non-transactional critbit, set both selectors:

```sh
GRAPH_INDEX_ALGORITHM=Critbit GRAPH_INDEX_TRANSACTION_MODE=non-transactional \
  go test ./cluster -run '^$' -bench '^BenchmarkGraphIndexEndToEnd$' \
  -benchmem -benchtime=1x -count=3 -timeout=10m
```

The same selectors apply to `BenchmarkGraphIndexRead` and
`BenchmarkGraphIndexScale`. Omit `GRAPH_INDEX_TRANSACTION_MODE` or use
`caller-owned` for the transactional control. Non-transactional mode uses fresh
contexts and normal graph/context Flush calls, without BeginTx during
construction, requests, or replica checks. It verifies that construction and
write phases emit no transaction binds. It does not provide atomic multi-node
updates or a transaction-wide read snapshot. These serial workloads check every
replica after publication; they do not test concurrent readers or writers.

[Paired transaction-mode results](benchmarks/2026-11-27-epsilon-nontransactional/README.md)
use the same current source for both modes, including direct-address Find and
the exact-lower-bound scan shortcut.

[Transaction-mode rerun after the closure changes](benchmarks/2026-11-27-epsilon-transaction-rerun/README.md)
includes the same paired matrix through 20,000 entries, memory timelines, and
a comparison with the preceding run. Both five-peer transactional builds now
complete within the five-minute construction limit.

[Fresh-build critbit/B+ results](benchmarks/2026-11-27-epsilon-reclaim-retry/README.md)
include all small workloads and the 128- and 1,000-entry matrix. The large B+
build was stopped after its construction cost grew severely; this report is
explicitly partial and preserves the measurements behind that judgment.
