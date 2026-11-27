package effects

import (
	"fmt"
	"strings"
	"testing"

	"github.com/swytchdb/engine/memsize"
)

func TestReverseHintMemoryIsBoundedAndReclaimed(t *testing.T) {
	p := newVertexPool()

	tip, parent := Tip{7, 1}, Tip{7, 2}
	if err := p.Put(tip, cutEffect(tip)); err != nil {
		t.Fatal(err)
	}

	v := p.getVertex(tip)
	key := logicalAddress(strings.Repeat("k", 1024))
	p.addParent(v, parent, key)

	charged := p.Bytes()
	for i := range 12557 {
		p.addParent(v, Tip{8, uint64(i + 1)}, fmt.Sprintf("%s/%d", key, i))
	}

	if p.Bytes() != charged {
		t.Fatalf("reverse hints grew memory from %d to %d", charged, p.Bytes())
	}

	if got, ok := v.firstParent(key); !ok || got != parent {
		t.Fatalf("established hint changed: %v, %t", got, ok)
	}

	if _, ok := v.firstParent(key + "/0"); ok {
		t.Fatal("reverse navigation crossed keys")
	}

	// Releasing ownership recalculates accounting; the retained key must
	// remain charged until the vertex itself is reclaimed.
	p.decrefChainNode(logicalAddress("k"), tip)

	if want := memsize.Alloc(int64(len(key))); v.creationBytes != want {
		t.Fatalf("reverse hint charge = %d, want %d", v.creationBytes, want)
	}

	p.drainReclaim()

	if p.vertexBytes.Load() != 0 || p.EntryCount() != 0 {
		t.Fatal("reclamation retained reverse hint memory")
	}
}
