package keytrie

import "testing"

func TestArenaRecycleKeepsGenerationAccountingSeparate(t *testing.T) {
	var arena critbitArena[int]
	arena.init(2, nil)
	arena.alloc()
	arena.alloc()

	old := arena.list.Load().chunks[0]
	arena.recordReaped(0)
	arena.recordReaped(0)

	fresh := arena.list.Load().chunks[0]
	if old == fresh || old.reaped.Load() != 2 || fresh.reaped.Load() != 0 {
		t.Fatal("recycling changed the old generation's accounting")
	}

	if fresh.outstanding.Load() != 2 || arena.freeCount.Load() != 2 {
		t.Fatal("recycled slots were not accounted before publication")
	}

	arena.alloc()

	if fresh.outstanding.Load() != 1 || old.outstanding.Load() != 0 {
		t.Fatal("allocation modified the wrong generation")
	}
}

func TestArenaDropsChunkWhenRecycleQueueIsFull(t *testing.T) {
	var arena critbitArena[int]
	arena.init(2, nil)
	arena.freeCap = 0
	arena.alloc()
	arena.alloc()
	arena.recordReaped(0)
	arena.recordReaped(0)

	if arena.list.Load().chunks[0] != nil || arena.bytes() != 0 {
		t.Fatal("full recycle queue retained the dead chunk")
	}

	if _, chunk := arena.alloc(); chunk != 1 {
		t.Fatalf("allocation reused a dropped chunk: %d", chunk)
	}
}
