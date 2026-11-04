// Package keytrie provides a concurrent crit-bit index with immutable tip sets,
// optional leaf payloads, and bounded eviction.
//
// Ownership and synchronization:
//   - Critbit owns the tree and two arenas. Arenas allocate slots; reclamation
//     detaches nodes, clears their references, and retires their slots.
//   - reapMu protects node lifetimes. Lookups and CAS mutations share its read
//     lock; unlinking, relocation, and Close take its write lock. Atomics are
//     still necessary because mutations run concurrently under the read lock.
//   - Each arena's growMu serializes directory publication and recycling.
//     Directories are immutable; counters belong to a single chunk generation.
//   - evictMu serializes the eviction policy, with reapMu nested inside it for
//     node access. Notifications and range callbacks run outside both locks.
//
// Tip sets and payload pointers can escape a lookup; arena nodes cannot. TipSet
// slices are immutable, and payload contents remain the consumer's responsibility.
// Configure ownership and eviction hooks before publishing a trie to callers.
//
// Maintenance storage is demand-driven: the reap queue is allocated on the first
// pending deletion, and arena directories grow without copying their published
// prefix on every new chunk. Close releases both arena and queue storage.
package keytrie
