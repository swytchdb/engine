package effects

import (
	"fmt"
	"slices"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

func convergenceFixture(t testing.TB, history int) (*Engine, Tip, Tip, Tip, Tip) {
	t.Helper()

	e := newTestEngine(nil)

	var prev Tip

	for i := range history {
		tip := Tip{42, uint64(i + 1)}

		eff := dataEffect("key")
		if prev != (Tip{}) {
			eff.Deps = toPbRefs([]Tip{prev})
		}

		if err := e.effectCache.Put(tip, eff); err != nil {
			t.Fatal(err)
		}

		prev = tip
	}

	snapshot := Tip{42, uint64(history + 1)}
	if err := e.effectCache.Put(snapshot, &pb.Effect{
		Key: []byte("key"), Deps: toPbRefs([]Tip{prev}),
		Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{State: &pb.ReducedEffect{}}},
	}); err != nil {
		t.Fatal(err)
	}

	left, right, merged := Tip{42, uint64(history + 2)}, Tip{42, uint64(history + 3)}, Tip{42, uint64(history + 4)}
	for _, tip := range []Tip{left, right, merged} {
		eff := dataEffect("key")

		eff.Deps = toPbRefs([]Tip{snapshot, snapshot})
		if tip == merged {
			eff.Deps = toPbRefs([]Tip{left, right, left})
		}

		if err := e.effectCache.Put(tip, eff); err != nil {
			t.Fatal(err)
		}
	}

	return e, snapshot, left, right, merged
}

func TestTopoCollectStopsAtConvergedSnapshot(t *testing.T) {
	for _, history := range []int{16, 4096} {
		t.Run(fmt.Sprint(history), func(t *testing.T) {
			e, snapshot, left, right, merged := convergenceFixture(t, history)
			for _, tc := range []struct {
				name string
				tips []Tip
				want int
			}{
				{"snapshot", []Tip{snapshot}, 1},
				{"duplicate-snapshot", []Tip{snapshot, snapshot}, 1},
				{"two-tips", []Tip{left, right}, 3},
				{"duplicate-tips", []Tip{right, left, right, left}, 3},
				{"merged", []Tip{merged}, 4},
			} {
				t.Run(tc.name, func(t *testing.T) {
					original := slices.Clone(tc.tips)
					d := newDag(e, logicalAddress("key"), "")
					d.nodes = make(map[Tip]*pb.Effect)
					before, _, _ := e.effectCache.Stats()
					paths, err := d.topoCollect(tc.tips, true)
					after, _, _ := e.effectCache.Stats()

					if err != nil {
						t.Fatal(err)
					}

					if len(paths) != tc.want || after-before != uint64(tc.want) {
						t.Fatalf("visited %d vertices / %d pool reads, want %d independently of history", len(paths), after-before, tc.want)
					}

					if d.lcaTip != snapshot {
						t.Fatalf("common snapshot=%v, want %v", d.lcaTip, snapshot)
					}

					if !slices.Equal(tc.tips, original) {
						t.Fatal("modified caller's frontier")
					}
				})
			}
		})
	}
}

func BenchmarkTopoCollectConvergedSnapshot(b *testing.B) {
	for _, history := range []int{16, 4096} {
		b.Run(fmt.Sprint(history), func(b *testing.B) {
			e, _, left, right, _ := convergenceFixture(b, history)
			b.ReportAllocs()

			for b.Loop() {
				d := newDag(e, logicalAddress("key"), "")

				d.nodes = make(map[Tip]*pb.Effect)
				if _, err := d.topoCollect([]Tip{left, right, left}, true); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
