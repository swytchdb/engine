# Engine

The causal-effect engine underneath [Swytch](https://getswytch.com). No wire protocol, no CLI, no binary — just the
guts.

> **Join us on [Discord](https://discord.gg/ZdKsCkBbne)** to discuss the hardest parts of computer science, physics,
> and — when those get too easy — the weather.

## The short version

Swytch is the Redis-compatible server. This is the library it's built on.

Everything Swytch does — replication without leaders, merges without coordination, transactions without a vote — happens
in here. The server is a thin front-end that speaks RESP and hands the real work to these seven packages. Pull them into
your own Go program, and you get the same machinery, minus the Redis compatibility.

The whole model rests on one idea. Every mutation produces an *effect* that carries pointers to the effects it observed.
The happened-before relation is encoded directly in the data, not inferred from clocks. Order the dependency graph, and
you've ordered history — deterministically, everywhere, with nobody in charge.

```bash
go get github.com/swytchdb/engine
```

Go 1.27+. That's the only requirement.

## The packages

Seven of them. Each does one thing.

| Package   | Import                               | What it is                                                 |
|-----------|--------------------------------------|------------------------------------------------------------|
| `effects` | `github.com/swytchdb/engine/effects` | The causal effect log — the DAG, fork-choice, horizon wait |
| `keytrie` | `github.com/swytchdb/engine/keytrie` | Lock-free crit-bit key index with adaptive eviction        |
| `cache`   | `github.com/swytchdb/engine/cache`   | CloxCache — the sharded, adaptive L0 cache                 |
| `crdt`    | `github.com/swytchdb/engine/crdt`    | HLC clock and glob matching                                |
| `cluster` | `github.com/swytchdb/engine/cluster` | QUIC+mTLS peer transport, replication, peer health         |
| `beacon`  | `github.com/swytchdb/engine/beacon`  | Discovery, join handshake, dynamic membership, the Runtime |
| `tracing` | `github.com/swytchdb/engine/tracing` | OpenTelemetry setup and trace-context-in-logs              |

### effects — the causal effect log

This is the heart. Everything else exists to serve it.

Each mutation produces an effect: a key, dependency pointers to the prior effects it observed, a semantic tag, a node
identifier. Effects are immutable and append-only. Each node mints addresses in its own namespace as
`(NodeID, Sequence)`, so nothing collides without coordination. The dependency DAG *is* the ordering — none of it is
metadata wrapping the real data; it is the data.

You drive it through an `Engine` and the `Context` objects it hands out:

```go
package myapp

import (
	"github.com/swytchdb/engine/effects"
	pb "github.com/swytchdb/engine/cluster/proto"
)

func example() error {
	config := effects.EngineConfig{
		NodeID:      pb.NewNodeID(),
		Broadcaster: nil, // nil = standalone, no cluster
		MemoryLimit: 64 << 20,
	}
	eng := effects.NewEngine(config)

	ctx := eng.NewContext()
	// ... build an *pb.Effect, then:
	if err := ctx.Emit(effect); err != nil {
		// ErrBootstrapIncomplete: this key's causal chain isn't fully local yet
		// because a peer is unreachable. Transient — the engine keeps retrying
		// the fetch in the background, so a later Emit on the same key succeeds.
		return err
	}
	if err := ctx.Flush(); err != nil {
		// ErrTxnAborted:        lost fork-choice or hit a real conflict. Safe to
		//                       rebuild the context and retry the whole thing.
		// ErrRegionPartitioned: SafeMode refused the write to stay consistent —
		//                       a same-region peer is unreachable. Retry once the
		//                       partition heals, or route the key to UnsafeMode.
		return err
	}
	return nil
}
```

#### Composing an effect

So what *is* the `*pb.Effect` you hand to `Emit`? Most of it fills itself in. You set the `Key` and the payload; `Emit`
stamps the HLC, the node id, the fork-choice hash, and — the important one — the `Deps`, the causal pointers to whatever
this key's context has already observed. That last step is the whole model: you never write causality by hand, you just
read-then-write and the dependency edges fall out.

The payload is a `oneof` — exactly one kind per effect. The one you'll reach for is `DataEffect`, and it describes a
mutation along four independent axes:

- **`Op`** — `INSERT_OP` or `REMOVE_OP`. Add/overwrite, or tombstone.
- **`Collection`** — `SCALAR` (one value per key), `KEYED` (named elements addressed by an `Id`), or `ORDERED`
  (positionally placed elements).
- **`Merge`** — how two writes to the same element combine. `ADDITIVE_INT` / `ADDITIVE_FLOAT` fold a delta into a
  running sum; `MAX_INT` / `MAX_BYTES` keep the larger — a run of any one of these commutes, so concurrent writes need no
  ordering among themselves. `LAST_WRITE_WINS` and `FIRST_WRITE_WINS` are order-sensitive, and "last"/"first" mean
  last/first in *causal* order: if A happened-before B, B wins, full stop. Only when two writes are genuinely concurrent —
  no causal edge either way — does fork-choice break the tie, and it breaks it the same deterministic way everywhere
  (lowest `hash(NodeID ‖ HLC)`), never by whose HLC is larger.
- **`Placement`** — for ordered collections: `PLACE_HEAD`, `PLACE_TAIL`, `PLACE_AFTER`/`PLACE_BEFORE` a `Reference`
  element, `PLACE_SELF`, or `PLACE_NONE` for everything scalar.

Plus an `Id` (which element, for keyed/ordered ops) and a `Value` (`Raw` bytes, `IntVal`, `FloatVal`, `Compressed`, or a
nested `Child`). Set the axes, and you've described a mutation. A scalar overwrite of key `foo` with the bytes `bar`:

```go
&pb.Effect{
    Key: []byte("foo"),
    Kind: &pb.Effect_Data{Data: &pb.DataEffect{
        Op:         pb.EffectOp_INSERT_OP,
        Collection: pb.CollectionKind_SCALAR,
        Merge:      pb.MergeRule_LAST_WRITE_WINS,
        Value:      &pb.DataEffect_Raw{Raw: []byte("bar")},
    }},
}
```

A scalar increment of `counter` by `5` looks almost identical, but the value is a *delta* and the merge rule folds it in:

```go
&pb.Effect{
    Key: []byte("counter"),
    Kind: &pb.Effect_Data{Data: &pb.DataEffect{
        Op:         pb.EffectOp_INSERT_OP,
        Collection: pb.CollectionKind_SCALAR,
        Merge:      pb.MergeRule_ADDITIVE_INT,
        Value:      &pb.DataEffect_IntVal{IntVal: 5},
    }},
}
```

That one line is why counters need no coordination — and it is *not* the CRDT counter it resembles. Two nodes each emit a
`+5` delta on their own side of a partition; on reconnect every node lands on `+10`. Not by adding the two results (both
read `+5`) — by folding the *deltas* back over the DAG from their last common ancestor. The causal graph is what supplies
the base the deltas apply to; take it away and you double-count on every re-converge. And it stays arithmetic only while
the element stays additive: a `LAST_WRITE_WINS` write to the same key resets the base and flips the fold non-commutative,
so whether it lands before or after the increments is settled by causal order (fork-choice on a tie), not by addition. A
`KEYED` collection with an element `Id` gives you named-field and set-membership writes; an `ORDERED`
collection with `PLACE_TAIL` gives you an append. Same struct, different axes — which is exactly how membership rides on
it (`beacon` writes `__swytch:members` as a plain `KEYED` collection).

Reads run the mutation backwards: `GetSnapshot` walks a key's DAG down to the last common ancestor and folds the
effects back together by their merge rules into one materialised value. Build the effect right, and reconciliation is
automatic; the merge rule already said what "concurrent" means for this data.

#### Where causality replaces the CRDT

It's tempting to file these merge rules under "CRDTs." Resist it. The additive rules do commute — a pile of `+5`s is a
pile of `+5`s in any order — but even the counter isn't a CRDT: it leans on the DAG for the base its deltas fold into
(above), and one `LAST_WRITE_WINS` write to the key takes the commutativity away. The keyed and ordered collections don't
even pretend — they're where people *expect* a CRDT and don't get one because they don't need one.

A CRDT set has to pick a bias. Two concurrent effects on the same element — one `INSERT_OP`, one `REMOVE_OP` — who wins?
OR-Sets say add-wins, 2P-sets say remove-wins, and either way you're carrying per-element tags to enforce it. Swytch
carries none of that. The insert and the remove are just two effects in the DAG: if one happened-before the other,
that's the answer; if they're genuinely concurrent, fork-choice picks the winner the same deterministic way it always
does.

Ordered collections are the sharper example. Push `a,b,c` onto one on one node and `x,y,z` onto it on another, at the
same time. A sequence CRDT (RGA, Logoot) hands every element a position id so concurrent inserts **interleave** — you can land
on `a,x,b,y,c,z`. Swytch doesn't, and can't. Each insert chains causally (`c` deps on `b` deps on `a`), and
reconciliation is a depth-first walk that drains one chain completely before it touches the next. So the blocks stay
whole: you get `a,b,c,x,y,z` or `x,y,z,a,b,c` — fork-choice decides *which* block leads, every node agrees, and nothing
gets shuffled in between. Contiguous, never interleaved — which is almost always what you meant by an appending in the first
place.

A `Context` is where a command's reads and writes accumulate. `Watch`, `BeginTx`, `CheckWatches`, `Abort`,
`TakeSavepoint` — the transaction primitives live here. When transactions race against the same causal base,
fork-choice picks a winner deterministically: each commit computes `H = hash(NodeID || HLC)`, lowest H wins.

The reason that's *valid* is straight relativity: events that are spacelike-separated in the causal graph have no true
order, so any deterministic function over the DAG is as correct as any other. Hash is just the function we picked
because it's symmetric and computable from the same inputs everywhere.

Writes during a partition are governed per-key:

```go
const (
    SafeMode   effects.SafetyMode = iota // block when region peers unreachable
    UnsafeMode                           // keep writing, form branches, merge on reconnect
)
```

`SafeMode` refuses to write when it can't make it safe. `UnsafeMode` never blocks — it lets both sides of a partition
write, forms branches in the DAG, and reconciles them through the same fork-choice on reconnection. You pin the mode per
key pattern with `KeyRangeRules`, so the counters keep counting while the money stays consistent.

### keytrie — the key index

A concurrent, lock-free crit-bit trie mapping keys to their current tip set. Every operation on it is a CAS:

```go
idx := keytrie.New()
idx.Insert(key, oldTips, newTips) // returns (current, false) if someone beat you
idx.Contains(key) // nil if absent
idx.RemoveTips(key, refs) // atomic, safe under concurrency
```

`Insert`, `Delete`, `RemoveTips` — all compare-and-swap and lock-free. The
package also owns eviction (`evict.go`), glob matching (`MatchGlob`), and the `TipSet` type the whole engine passes
around. Parts of it are dual-licensed under MIT.

### cache — CloxCache

Lock-free, sharded, adaptive in-memory cache. Items above a learned access-frequency threshold are protected from
eviction; the threshold adapts online, per-shard, with no manual tuning.

```go
c := cache.NewCloxCache[string, []byte](cache.ConfigFromMemorySize(256 << 20))
c.Put("k", v)
got, ok := c.Get("k", 0)
```

The policy is protected-freq eviction with LRU as the tiebreaker — and it's honest about where it wins. Measured against
offline Belady replay, the edge over plain LRU lives at cache-size/working-set ratios of roughly 0.3–0.8. Below ~0.25
every policy produces near-identical contents; above ~1 plain recency already retains the working set. Any hot tier
above this one skims exactly the frequency skew the policy feeds on. So benchmark claims should state those conditions —
which is why the package doc does.

Build a config from an entry count (`ConfigFromCapacity`) or a byte budget (`ConfigFromMemorySize`), whichever you
actually know.

### crdt — clock and matching

Small package, two jobs. `HLC` is a thin, injectable wrapper around the OS monotonic clock — thin on purpose, because
the causal graph does the ordering and the clock is only there for the bounded horizon wait. `MatchGlobPattern` does
Redis-style glob matching. That's it.

```go
clk := crdt.NewHLC()
now := clk.Now()
clk.SetClock(fakeClock) // for tests
```

### cluster — peers, health, replication

The transport layer. Nodes find each other, authenticate, and stream effects — all over QUIC.

A `PeerManager` is the entry point. It takes a config, an `EffectHandler`, and a `LogReader`:

```go
pm, err := cluster.NewPeerManager(clusterCfg, handler, logReader)
```

The trust model is one shared passphrase and nothing else. `ClusterConfig.TLSPassphrase` derives a shared CA via HKDF;
every node generates an ephemeral leaf certificate on startup and authenticates peers with mTLS over TLS 1.3. Nothing to
distribute, nothing to rotate. A `HeartbeatManager` and `PeerHealthTable` track which peers are alive and reachable by a
symmetric path; the beacon reads that health to decide who's really there. The package also carries pub/sub routing
(`PubSubRouter`), tiered cloud storage sync (`CloudSync`), and per-peer key filters so a fetch knows whether to ask a
peer or the CDN before it asks.

### beacon — discovery, membership, and the Runtime

Cluster gives you a wire between nodes. Beacon is what gets a node *onto* that wire and keeps it there.

Membership isn't a separate subsystem. The roster lives at the reserved key `__swytch:members`, held as an ordinary
`KEYED` collection — one element per node, keyed by node id, valued by its `host:port`, merged `LAST_WRITE_WINS`. A node
joins by emitting an `INSERT_OP` for itself; it leaves by emitting a `REMOVE_OP`. The live connection table is just a
reactive projection of that key, rebuilt whenever it changes — no polling, no write-back to "fix" a stale read. Because
it's a normal key, once a Swytch front-end is in front of the engine you can watch the whole cluster form with a Redis
client you already have:

```bash
redis-cli HGETALL __swytch:members
```

Joining is a phased handshake, and the ordering matters. A node discovers peers (DNS via `--join`, or the cloud roster),
waits for at least one peer to come alive and symmetric, primes its subscription so peers' membership tips arrive first,
*then* registers itself — and only starts serving once the membership key has converged across every candidate. That
last step is why a fresh node doesn't answer with half a cluster's view. Graceful shutdown emits the `REMOVE_OP` so
peers stop dialling a node that's gone; a crash leaves the entry for an operator or the cloud to reap.

Most of the time you don't touch `Beacon` directly — you build a `Runtime`, which is the one call that assembles the
whole engine:

```go
rt, err := beacon.NewRuntime(beacon.RuntimeConfig{
    ClusterPassphrase: passphrase, // empty → single-node, no peers, no beacon
    ClusterPort:       7000,
    JoinAddr:          "my-cache.local", // DNS name every peer resolves to
    MemoryLimit:       64 << 20,
})
defer rt.Stop()

// rt.Engine, rt.PeerManager, rt.Beacon, rt.CloudSync are all wired and live.
```

`NewRuntime` builds the engine, stands up the `PeerManager`, wires the broadcaster, starts anti-entropy, optionally
connects Swytch Cloud, and runs the beacon's join — in that order, with the teardown ordered in reverse so nothing races
a close. Set `ClusterPassphrase` empty, and you get a single-node engine and nothing else. Set `AsyncJoin` and it returns
the moment the local engine is up, converging with peers in the background — for callers that must serve immediately and
can accept a solo writer or two before the cluster is whole.

### tracing — observability plumbing

OpenTelemetry over OTLP HTTP, plus a `slog.Handler` that injects the active trace context straight into your structured
logs:

```go
shutdown := tracing.Init(ctx, tracing.Config{ /* ... */ })
defer shutdown(ctx)

logger := slog.New(tracing.NewTracingHandler(baseHandler))
```

`InjectIntoBytes` / `ExtractFromBytes` carry trace context across the wire, so a request keeps its span as it hops
between nodes.

## Wiring it together

If you just want a running node, `beacon.NewRuntime` above is all you need. This section is what it does under the
hood — worth reading if you're embedding the pieces yourself or want to understand the shape.

The `effects` package and the `cluster` package don't know about each other on purpose. Two adapters in
`cluster/engine_glue.go` marry them:

```go
eng := effects.NewEngine(cfg)

handler := cluster.NewEngineEffectHandler(eng) // inbound effects → engine
logReader := cluster.NewEngineLogReader(eng.EffectCache()) // peer fetches → engine's cache

pm, _ := cluster.NewPeerManager(clusterCfg, handler, logReader)
eng.SetBroadcaster(pm) // outbound effects → peers
```

Now the loop is closed. The engine hands outbound effects to the `Broadcaster`; the `PeerManager` streams them to peers;
inbound effects come back through the `EffectHandler` into the engine's DAG. Hang a `beacon.Beacon` off the same engine
and PeerManager and the node can find peers and register itself; that's exactly the assembly `NewRuntime` performs. Same
shape every front-end uses — Redis, SQL, whatever comes next — which is why the glue lives here and not in any one of
them.

## Where the front-ends live

This repository is the engine only. The Redis server, the CLI, the installers, the container images — those live
in [swytch](https://github.com/swytchdb/swytch), which imports this module. If you want a cache to run, you want that.
If you want the machinery to build on, you're in the right place.

## License

AGPL-3.0. See the `LICENSE` file and source file headers. Parts of `keytrie` (CloxCache) are additionally available
under the MIT License. Commercial licensing available; email `hello@getswytch.com`.
