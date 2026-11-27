# B+ tree prototype

`graph/bplus` is a byte-ordered index over independently addressed graph pages.
It provides descriptor creation, tree initialization, lookup, insertion,
replacement, deletion, and bounded scans. Structural invariant checking lives
only in the package's test helpers.

## Creation and context ownership

Creation has two explicit stages. `Create` queues the descriptor and options.
After that context flushes successfully, `Initialize` atomically creates the
initial root page and attaches its entry-point reference. This respects the
engine requirement that graph emissions observe an established descriptor.

```go
create := engine.NewContext()
defer create.Abort()
tree, err := bplus.Create(create, "animals", bplus.Options{MaxKeys: 32})
if err != nil {
    return err
}
if err := create.Flush(); err != nil {
    return err
}

initialize := engine.NewContext()
defer initialize.Abort()
if err := tree.Initialize(initialize); err != nil {
    return err
}
if err := tree.Put(initialize, []byte("cat"), []byte("row/42")); err != nil {
    return err
}
return initialize.Flush()
```

A published descriptor without an entry point returns `ErrUninitialized` on
reads. Initialization can be retried after an abort; it rejects an already
initialized tree. `Open` creates a handle and checks descriptor state during
operations. No handle retains pages between calls.

Operations use the supplied context and reuse an outer transaction. The caller
owns flushing, aborting, retries, and the engine handler locking contract for
concurrent local flushes (`PendingKeysInternal`). Library calls never flush.
Pure readers abort their context after reading; they need not publish a bind.

Page read/modify/write requires transaction validation even without a split,
because another writer may have changed or moved entries. Splits and merges
also need atomic visibility across all changed pages. Application row writes
can share the same context and transaction outcome as index updates.
Begin the caller transaction before adding associated application-row writes.

## Representation

Each page has a parent reference; leaves also have previous/next references.
The root address stays fixed through growth and shrinkage. Merges explicitly
delete retired pages, and allocation counters prevent their addresses from
being recycled during subsequent growth.

Pages are scalar JSON records with byte-encoded references, keys, and values.
Addresses remain opaque binary strings in the library. `MaxKeys` bounds entry
counts (default 32, range 3–256); entry byte sizes are not currently bounded.
Whole-page updates mean writes to separate entries on one page may conflict.

Ordering is bytewise. SQL callers supply order-preserving tuple encodings and
include row IDs where necessary for nonunique indexes. `Scan` returns
`[lower, upper)`; nil bounds are unbounded, and a nonnil empty slice is the empty
key. A nonpositive result limit means unlimited.

## Regions and discovery

Regions describe immutable allocation lineage, not key ranges or current tree
position. Reparenting a node preserves its address and region. Labels are
ordinary node data and do not select replication recipients.

Tree reads discover referenced keys using their explicit internal addresses and
establish ordinary exact-key subscriptions. Subscribing to the descriptor does
not subscribe to other keys in the graph or to future nodes. Parent and sibling
links are available in node state; lookup and scan start at the root.

Reading shared ancestors and the descriptor adds them to transaction read sets.
Binds coordinate with the subscribers of their participating keys.

## Validation

Local tests cover small page orders, shuffled insertion/deletion, multilevel
splits, borrowing, merges, root shrink/regrowth, binary keys and addresses,
range limits, pending reads, initialization rollback, and outer transactions.
They check parent links, sibling links, occupancy, separators, and leaf depths.
A snapshot reader spans a split, and a stale transaction aborts its index and
application-row writes together.

`cluster.TestBPlusFivePeers` uses five real QUIC peers. It exercises cold page
discovery, registration without bootstrap, delivery of future split pages,
remote snapshot reads, stale-write rejection, mutations from different peers,
and complete deletion back to one empty root. It compares scans on every peer
after mutations and checks that the final root is an empty leaf. The local
tests additionally walk parent links, separators, and occupancy using the
unexported `checkTree` test helper.

Validated commands (cluster tests require localhost UDP sockets):

```sh
go test -race ./graph/bplus -count=1 -timeout=6m
go test -race ./cluster -run '^(TestBPlusFivePeers|TestGraphNodeDiscoveryFivePeers)$' -count=3 -timeout=2m
go vet ./graph/bplus ./cluster
```

The exhaustive local race test takes several minutes with its accumulated
transaction history; its initial three-minute timeout was too short.

## Benchmarks

The [epsilon baseline](benchmarks/2026-11-23-epsilon/README.md) contains measured
results, raw samples, and the exact build/runtime metadata.
The [construction-history comparison and profiles](benchmarks/2026-11-23-epsilon-profiles/README.md)
compare SingleTx and Incremental builds and identify the dominant read,
preparation, and publication stacks.
The [optimized rerun](benchmarks/2026-11-24-epsilon-optimized/README.md) repeats
those cases after the verdict-evidence changes and includes retained-binary
controls, remaining bottlenecks, and the intermittent cold-subscription test failure.
The [ingestion rerun](benchmarks/2026-11-24-epsilon-ingestion/README.md) measures
NACK batching and shared ancestry traversal against that optimized build,
including fresh controls and the remaining cross-key frontier work.
The [cross-key backfill rerun](benchmarks/2026-11-24-epsilon-backfill/README.md)
measures batching those frontier updates and sharing closure discovery, with
new profiles showing ordinary receive-side closure as the largest remaining cost.
The [size-scaling exploration](benchmarks/2026-11-24-epsilon-scale/README.md)
extends to 1,000, 10,000, and 20,000 entries, recording construction and operation
costs, allocation growth, and runs that exceed time or memory limits.
The [modern-cluster rerun](benchmarks/2026-11-24-epsilon-modern/README.md)
repeats that matrix with version 1.8.0 peers, checking that workload verdict
snapshots omit compatibility evidence.
The [full sweep after the MVCC refactor](benchmarks/2026-11-24-epsilon-mvcc/README.md)
compares codecs, reads, 1/3/5-peer workloads, component benchmarks, size scaling,
and profiles across the optimization series.
The [incremental reconstruction profiles](benchmarks/2026-11-24-epsilon-incremental-profile/README.md)
measure transaction-history growth, query allocation, and the effect of existing
state compaction on an isolated fixture.
The [savepoint sweep](benchmarks/2026-11-24-epsilon-savepoints/README.md) measures
constant-cost savepoint capture, lower bulk construction allocation, and the
remaining incremental-history and distributed size limits.
The [compaction and traversal sweep](benchmarks/2026-11-24-epsilon-compaction/README.md)
measures faster reads, the added Flush cost, and a completed 10k incremental build.
The [publication-memory investigation](benchmarks/2026-11-24-epsilon-publication-memory/README.md)
raises the five-peer 10k bulk RSS guard to 100 GiB and records allocation rate,
GC activity, retained heap, and outstanding work after fixture cleanup.
The [closure-walk rerun](benchmarks/2026-11-24-epsilon-walk/README.md) records
lower allocation churn, continued publication failure, and retained closure work.
The [batching rerun](benchmarks/2026-11-24-epsilon-batching/README.md) records
successful 10k publication, a sharp 1k-to-10k scaling cost, and CPU/lock profiles
of reverse-parent maintenance and subscription bootstrapping.
The [parent-link rerun](benchmarks/2026-11-25-epsilon-parent-links/README.md)
completes all 10k workloads, reduces initial Flush from 332 s to 16.3 s, and
profiles the remaining repeated closure work during first-read discovery.
The [completed-transaction closure rerun](benchmarks/2026-11-25-epsilon-allocations/README.md)
reduces the 10k first scan to 0.95 s and total allocation to 24.3 GiB;
initial bulk publication remains the main scaling concern.
The [full sweep after ingestion changes](benchmarks/2026-11-25-epsilon-ingestion-sweep/README.md)
reduces five-peer 10k initial Flush to 0.90 s and completes bulk workloads through
20k entries. Incremental construction remains expensive; CPU profiles identify
repeated competing-bind history walks as the growing Flush cost.
The [five-minute incremental rerun](benchmarks/2026-11-25-epsilon-incremental-five-minute/README.md)
completes all sizes: 20k construction takes 7.42 s with one peer and 128.42 s
with five. Large-write Flush costs fall sharply; live-engine memory and
remaining closure traversal are the next scaling concerns.
The [matched critbit comparison](../critbit/benchmarks/2026-11-27-epsilon-comparison/README.md)
runs both indexes on the same engine with caller-owned transactions, bounded
scans, identical byte keys and values, and five-minute incremental-build limits.

`BenchmarkBPlusScale` is an opt-in size exploration using the default page
capacity of 32. It builds one fixture, then runs Get (64 requests), Scan16
(16), ReadHeavy (40), Mixed (40), Replace (40), and Structural (128), in that
order. Reads and replacements sample the whole key range with a fixed seed;
the structural phase inserts 64 keys at the right edge and deletes them again.
Each phase checks contents on every peer outside its operation timer. Phases
share the fixture and its growing history, so these are not independent
fresh-fixture workload comparisons. One process performs one sample:

```sh
BPLUS_SCALE_ENTRIES=10000 BPLUS_SCALE_PEERS=5 BPLUS_SCALE_BUILD=SingleTx \
  BPLUS_SCALE_VERSION=1.8.0 \
  BPLUS_SCALE_DIR=/tmp/bplus-scale ./cluster.test -test.run '^$' \
  -test.bench '^BenchmarkBPlusScale$' -test.benchtime=1x -test.count=1 -test.timeout=5m
```

`BPLUS_SCALE_VERSION` sets the fixture version and waits for every peer to
report that compatibility floor before construction. The same variable also
applies to `BenchmarkBPlusRead`, `BenchmarkBPlusEndToEnd`, and both opt-in
profile fixtures. Without it, the standalone
fixture has an unknown version and retains legacy evidence behavior. Modern
scale runs count verdict snapshots and reject any carrying compatibility evidence.

The scale harness saves construction progress, phase latency/allocation metrics,
and post-GC heap snapshots in the supplied directory. The epsilon runner records
kernel peak RSS and CPU usage, plus any time or memory limits reached. Full
replica verification and background work contribute to process resource totals;
peak RSS is not a measure of index data size. This suite does not replace
concurrent-writer, contention, or long-duration production testing.

`BenchmarkBPlusPageCodec` measures leaf-page encoding and decoding in isolation.
`BenchmarkBPlusRead` measures a point lookup or 16-entry range scan, including
a new context, MVCC traversal, and `Abort`, against a fixed published history.
The invariant checker is used only to verify the read fixture before timing.

`cluster.BenchmarkBPlusEndToEnd` measures complete 40-request traces against
fresh engines and freshly seeded trees. Cases vary 1/3/5 peers, 32/128 starting
entries, both SingleTx and Incremental construction, and these request mixes:

| Workload | Reads | Replacements | Inserts/deletes |
| --- | ---: | ---: | ---: |
| ReadHeavy | 90% | 5% | 5% |
| Mixed | 50% | 25% | 25% |
| Replace | 0% | 100% | 0% |
| Structural | 0% | 0% | 100% |

The structural requests grow the right edge and then delete those new entries.
Not every insertion/deletion changes topology: `new-pages/trace` and
`deleted-pages/trace` report actual page creation and retirement. All cases use
eight-byte ordered keys, 32-byte values, and `MaxKeys: 8` to exercise splits in
small fixtures. This is intentionally smaller than the production API default
of 32; these measurements do not select a production page size.

One Go benchmark `op` is **one entire 40-request trace**. `ns/op`, `B/op`, and
`allocs/op` therefore describe that trace, while custom `ns/request`,
`requests/s`, and p50/p95 metrics describe individual API requests. The request
latency samples include context creation, tree work, and `Flush` for writes or
`Abort` for reads. Custom phase metrics separate reads, write preparation, and
flush time. Preparation includes MVCC reconstruction and graph effect emission;
flush includes engine transaction validation, publication, and coordination.
Neither is a measurement of transaction validation alone.

The completion boundary is the engine's `Flush` return. Timing does not include
waiting for every peer to expose the final state. Every peer's final scan is
checked outside the timer. Cluster startup, seeding, subscription warmup,
correctness checks, and teardown are also untimed. Both modes publish the descriptor first. SingleTx initializes the root and seeds
all entries in one transaction. Incremental flushes after each insertion, with
root initialization sharing the first insertion transaction. Insertion order
and final topology are identical. Each timed write uses its own context.
Fresh fixtures keep history length independent of Go's calibration count.

These are sequential requests from one writer, with remote peers discovering keys
through reads. Three/five-peer cases use real loopback QUIC in one process;
the single-peer case has no network. They measure serial latency and throughput,
not concurrent capacity, WAN performance, or selective fanout. Allocation
counts include work performed by all peers in this process. A small local-effect
hook counts page-value/deletion effects; its overhead is included. Region-label
effects, NOOPs, binds, and snapshots are not included in that count. It is not a
wire-byte counter.

Standalone and coordinated transactions now both emit verdict snapshots. The
original baseline did not emit them in the standalone path, so comparisons with
that baseline include this change in proof history. Peer-count comparisons also
include replica work and different histories; they do not isolate transport overhead.

Run performance commands sequentially, without the race detector:

```sh
go test ./graph/bplus -run '^$' -bench '^BenchmarkBPlus' -benchmem -benchtime=200ms -count=3
go test ./cluster -run '^$' -bench '^BenchmarkBPlusEndToEnd$' -benchmem -benchtime=1x -count=3 -timeout=10m
```

For a smaller comparison, select cases with Go's slash-separated benchmark
filter, for example:

```sh
go test ./cluster -run '^$' -bench '^BenchmarkBPlusEndToEnd$/Peers(1|5)$/Entries32$/Order8$/Build(SingleTx|Incremental)$/Mixed$' -benchmem -benchtime=1x -count=3
```

Use fixed-count end-to-end runs and repetitions for initial comparisons: fixture
setup is intentionally excluded from timing but still takes wall-clock time.
The percentile samples are small and deterministic, useful for exploration
rather than production latency estimates. No engine or cloud behavior was
changed for these benchmarks.

The tests can also be built locally and run on a separate benchmark host. This
keeps the compiler fixed and requires transferring only executables:

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go test -c -o /tmp/bplus.test ./graph/bplus
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go test -c -o /tmp/cluster.test ./cluster
```

After copying them to the benchmark host, run the binaries sequentially with
the equivalent test-runner flags:

```sh
./bplus.test -test.run '^$' -test.bench '^BenchmarkBPlus' -test.benchmem -test.benchtime=300ms -test.count=3
./cluster.test -test.run '^$' -test.bench '^BenchmarkBPlusEndToEnd$' -test.benchmem -test.benchtime=1x -test.count=3 -test.timeout=10m
```

Opt-in diagnostic tests collect CPU profiles and allocation-profile differences
after fixture construction. They do not run during ordinary tests:

```sh
BPLUS_PROFILE_DIR=/tmp/bplus-lookup BPLUS_BUILD=SingleTx BPLUS_ENTRIES=128 ./bplus.test -test.run '^TestBPlusLookupProfile$' -test.memprofilerate=65536
BPLUS_PROFILE_DIR=/tmp/bplus-trace BPLUS_BUILD=Incremental BPLUS_ENTRIES=128 BPLUS_PEERS=5 BPLUS_WORKLOAD=Structural ./cluster.test -test.run '^TestBPlusTraceProfile$' -test.memprofilerate=65536
go tool pprof -top -sample_index=alloc_space -base /tmp/bplus-lookup/alloc-before.pprof ./bplus.test /tmp/bplus-lookup/alloc-after.pprof
```

The lookup diagnostic performs 1,000 reads and records observed address counts.
The trace diagnostic runs three independent 40-request fixtures. CPU labels
distinguish preparation, Flush, receive handling, and serving history. Per-request
JSON records page writes/creation/deletion, observed addresses, bind keys and
actual receiving peers, local snapshot proof references, notification payloads,
served history, and returned NACK ancestry. History is attributed to the active
request window; late publication notifications retain their originating request.
This does not claim exact causality for asynchronous history requests.
CPU labels propagate to spawned goroutines, so Flush-labelled publication work
is not a serial breakdown of time spent by the caller inside `Flush`.

These probes wrap the concrete test transport adapters and preserve their optional
interfaces. Timings include probe overhead. Payload counters omit QUIC/TLS framing,
and allocation profiles include all peers in the process. Local proof-reference
counts are not counts of all evidence carried on the wire. Subtract the before
profile using `alloc_space` or `alloc_objects`; retained-heap profiles answer a
different question. Fixture creation and final correctness scans are excluded
from the CPU window; small profiler bookkeeping costs remain in allocation deltas.
