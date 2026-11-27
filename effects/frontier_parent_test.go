package effects

import (
	"fmt"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

func TestClosureParentLinksBoundIncrementalDiscovery(t *testing.T) {
	for _, length := range []int{64, 1024} {
		t.Run(fmt.Sprint(length), func(t *testing.T) {
			e := newSnapshotEngine(nil)
			defer func() { _ = e.Close() }()

			if e.closedFrontiers != nil {
				e.closedFrontiers.Close()
				e.closedFrontiers = nil
			}

			total := 0

			for n := 1; n <= length; n++ {
				tip := Tip{7, uint64(n)}

				var deps []Tip
				if n > 1 {
					deps = []Tip{{7, uint64(n - 1)}}
				}

				putCutEffects(t, e, map[Tip]*pb.Effect{tip: cutEffect(tip, deps...)})

				walked, err := publishCountedClosure(t, e, tip)
				if err != nil {
					t.Fatal(err)
				}

				total += walked
				if walked > 2 {
					t.Fatalf("arrival %d expanded %d vertices", n, walked)
				}

				requireFrontier(t, e, tip)
			}

			t.Logf("%d arrivals expanded %d vertices without the closure cache", length, total)
		})
	}
}

func TestClosureParentLinksPreserveDelayedAncestorAndSibling(t *testing.T) {
	e := newSnapshotEngine(nil)
	defer func() { _ = e.Close() }()

	base, middle, current, sibling := Tip{7, 1}, Tip{7, 2}, Tip{7, 3}, Tip{8, 1}
	putCutEffects(t, e, map[Tip]*pb.Effect{
		base: cutEffect(base), middle: cutEffect(middle, base),
		current: cutEffect(current, middle), sibling: cutEffect(sibling, base),
	})
	e.installIngestedTips(logicalAddress("k"), current)

	op := newClosureOperation(e, make(map[Tip]error))
	defer op.release()

	op.cacheDisabled = true
	vertices, _ := countClosureWork(op)

	live, complete, err := op.publish(logicalAddress("k"), []Tip{base, sibling}, true)
	if !complete || err != nil {
		t.Fatalf("complete=%t err=%v", complete, err)
	}

	requireCutTips(t, live, []Tip{current, sibling})

	if vertices[base] != 0 || vertices[middle] != 0 {
		t.Fatalf("reopened established ancestry: %v", vertices)
	}
}

func TestClosureParentLinksAfterLocalPublication(t *testing.T) {
	e := newSnapshotEngine(nil)
	defer func() { _ = e.Close() }()

	key := logicalAddress("k")

	const length = 1024
	for n := 1; n <= length; n++ {
		tip := Tip{7, uint64(n)}

		var deps []Tip
		if n > 1 {
			deps = []Tip{{7, uint64(n - 1)}}
		}

		putCutEffects(t, e, map[Tip]*pb.Effect{tip: cutEffect(tip, deps...)})
	}

	for n := length; n <= length+64; n++ {
		tip := Tip{7, uint64(n)}
		if n > length {
			putCutEffects(t, e, map[Tip]*pb.Effect{tip: cutEffect(tip, Tip{7, uint64(n - 1)})})
		}

		e.updateIndex(key, e.index.ContainsInternal(key), tip)
		op := newClosureOperation(e, make(map[Tip]error))
		op.cacheDisabled = true
		vertices, _ := countClosureWork(op)
		// Associated-key discovery retains its work without publishing a key
		// or recording a closed-frontier cache entry.
		live, complete, err := op.run(key, []Tip{tip}, false, true)
		op.release()

		if !complete || err != nil {
			t.Fatalf("arrival %d: complete=%t err=%v", n, complete, err)
		}

		requireCutTips(t, live, []Tip{tip})

		want := 2
		if n == length {
			want = length // New key: establish all previously unknown history.
		}

		if len(vertices) != want {
			t.Fatalf("arrival %d expanded %d vertices, want %d", n, len(vertices), want)
		}
	}
}
