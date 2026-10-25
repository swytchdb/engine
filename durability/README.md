# Local effect mirror

This package mirrors immutable resident effects for restart recovery. It does not
serve cache misses from disk. The effects engine uses it through
`effects.OpenEngine(ctx, effects.EngineConfig{DataDir: path})`; Swytch exposes
`--data-dir`. An empty directory setting preserves memory-only operation.

Files are `<data-dir>/<originating-node-id>/<offset % 64>.swytch`. Each has a
versioned header, linked TOC pages, and append-only protobuf data. All payload
allocations own complete filesystem blocks. TOC entries contain the original
offset, position, actual/allocated lengths, checksum, and publication marker.
Checksums cover both record metadata and payload. Records are immutable.

Writers reserve under the TOC mutex and copy independent payloads concurrently.
A per-file RW lock protects mappings from growth, unmapping, deletion, and hole
punching. Directory locking prevents concurrent processes from owning a mirror.
TOC pages grow inside each bucket file; there is no fixed effect-count limit.
New headers are synced before the final filename is published.

The oldest unsynced publication sets a 10 ms deadline; later publications do not
postpone it. Different files sync independently, and writes during a sync remain
dirty for a subsequent batch. This is asynchronous persistence: scheduling and
storage latency can extend the loss window. Clean shutdown flushes pending data.
Sync errors are logged and retried, and explicit Sync/Close return errors.

Eviction removes TOC entries. Tombstones reach storage before hole punching,
preventing recovery from following an old entry into reclaimed bytes. Linux
hole punching is used when supported; other platforms leave unreachable bytes.
An empty bucket is rechecked under the exclusive file lock and deleted. Space
within a nonempty file is not reused for payloads; TOC slots are reused.

Recovery opens files and then processes TOC pages across GOMAXPROCS workers, validates checksums,
and unmarshals directly from mapped slices. Callbacks cannot retain these slices
or mutate the store. The engine reconstructs key frontiers from causal edges,
including transaction-bound keys, independent of record arrival order. It does
not replay commands or decide transaction outcomes. New engine incarnations use
fresh node IDs; recovered effects retain their original IDs.

If recovery exceeds the memory budget, the engine cancels and drains workers,
clears partial RAM state, and logs:

> not enough free memory to recover state, please restart with more memory. Deleting storage and continuing in 30s

Cancellation during that interval preserves storage. Otherwise the engine deletes
its mirror files and starts empty. This can discard data unavailable on peers.
Unrelated files in the configured directory are not removed. Corrupt published
records and unsupported formats fail startup; they do not trigger deletion.
