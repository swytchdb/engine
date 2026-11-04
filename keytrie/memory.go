package keytrie

import (
	"unsafe"

	"github.com/swytchdb/engine/memsize"
)

func tipSetBytes(tips *TipSet) int64 {
	if tips == nil {
		return 0
	}

	return memsize.Alloc(int64(unsafe.Sizeof(*tips))) + memsize.Alloc(int64(cap(tips.tips))*int64(unsafe.Sizeof(EffectRef{})))
}

func payloadBytes[T any](data *T) int64 {
	if data == nil {
		return 0
	}

	return memsize.Alloc(int64(unsafe.Sizeof(*data)))
}

// Callers hold reapMu until the pointer mutation and its accounting complete.
// This prevents a chunk from being dropped/recycled between those operations.
func (a *critbitArena[T]) addExtra(chunk uint32, delta int64) {
	a.list.Load().chunks[chunk].extra.Add(delta)
}

func (c *Critbit[T]) releaseLeafMemory(leaf *critNode[T], tips *TipSet, data *T) {
	c.leafArena.addExtra(leaf.chunkIdx, -tipSetBytes(tips)-payloadBytes(data))
}

func (c *Critbit[T]) keyBytes(key string) int64 {
	if c.retainKey != nil {
		return 0 // shared owner charges the allocation once
	}

	return memsize.Alloc(int64(len(key)))
}

// Retire only after detaching under reapMu's write lock, or for an unpublished
// CAS loser while holding its read lock. All public readers copy their result
// under that lock, so fields can be cleared without invalidating escaped values.
// Clear before recording the slot: the final record may drop/recycle its array.
func (c *Critbit[T]) retireLeaf(leaf *critNode[T]) {
	key, tips, data := leaf.key, leaf.tips.Load(), leaf.data.Load()
	leaf.key = ""
	leaf.tips.Store(nil)
	leaf.data.Store(nil)
	leaf.deleted.Store(true)
	c.leafArena.addExtra(leaf.chunkIdx, -c.keyBytes(key)-tipSetBytes(tips)-payloadBytes(data))

	if c.releaseKey != nil && key != "" {
		c.releaseKey(key)
	}

	c.leafArena.recordReaped(leaf.chunkIdx)
}

func (c *Critbit[T]) retireInternal(node *critNode[T]) {
	node.child[0].Store(nil)
	node.child[1].Store(nil)
	c.internalArena.recordReaped(node.chunkIdx)
}

func (a *critbitArena[T]) residentBytes() int64 {
	list := a.list.Load()
	if list == nil {
		return 0
	}

	var node T

	n := memsize.Alloc(int64(unsafe.Sizeof(*list))) + memsize.Alloc(int64(cap(list.chunks))*int64(unsafe.Sizeof((*arenaChunk[T])(nil))))
	for _, chunk := range list.chunks {
		if chunk != nil {
			n += memsize.Alloc(int64(unsafe.Sizeof(*chunk)))
			n += memsize.Alloc(int64(cap(chunk.nodes))*int64(unsafe.Sizeof(node))) + max(chunk.extra.Load(), 0)
		}
	}

	return n
}

// ResidentBytes budgets current arena capacity, keys (including ghosts/dead
// slots), current frontier arrays, and payload headers. Maintenance queue
// allocations are excluded from primary-cache ownership.
// Externally interned keys and payload contents belong to their shared owner.
// Retired fields are cleared under the structural write lock before their
// capacity is credited. Arrays are never reused in place; results retained only
// by callers remain outside cache ownership.
func (c *Critbit[T]) ResidentBytes() int64 {
	if c.closed.Load() {
		return 0
	}

	return int64(unsafe.Sizeof(*c)) + c.leafArena.residentBytes() + c.internalArena.residentBytes()
}
