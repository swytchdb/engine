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
	a.list.Load().extra[chunk].Add(delta)
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

func (a *critbitArena[T]) residentBytes() int64 {
	list := a.list.Load()
	if list == nil {
		return 0
	}

	var node T

	n := int64(unsafe.Sizeof(*list)) + memsize.Alloc(int64(cap(list.chunks))*24) + 3*memsize.Alloc(int64(len(list.chunks))*8)
	for i, chunk := range list.chunks {
		n += 3 * 8 // reaped, outstanding, extra counters survive a nil chunk
		if chunk != nil {
			n += memsize.Alloc(int64(cap(chunk))*int64(unsafe.Sizeof(node))) + max(list.extra[i].Load(), 0)
		}
	}

	return n
}

// ResidentBytes budgets current arena capacity, keys (including ghosts/dead
// slots), current frontier arrays, and payload headers. Maintenance queue
// allocations are excluded from primary-cache ownership.
// Externally interned keys and payload contents belong to their shared owner.
// Keys stay charged until their arena chunk is dropped. Relocation transfers
// the tips/payload charge without modifying husks visible to concurrent readers.
func (c *Critbit[T]) ResidentBytes() int64 {
	if c.closed.Load() {
		return 0
	}

	return int64(unsafe.Sizeof(*c)) + c.leafArena.residentBytes() + c.internalArena.residentBytes()
}

// SetKeyOwnership shares key storage with an external cache owner. Configure
// only on an empty trie before use. Ghosts and detached slots retain their key
// reference until their containing chunk is dropped; no reader fields change.
func (c *Critbit[T]) SetKeyOwnership(retain func(string) string, release func(string)) {
	if retain == nil || release == nil {
		panic("keytrie: key ownership requires retain and release")
	}

	c.retainKey = retain
	c.leafArena.dropNode = func(n *critNode[T]) {
		if n.key != "" {
			release(n.key)
		}
	}
}
