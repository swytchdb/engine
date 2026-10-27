package effects

import (
	"fmt"
	"slices"
	"sync"
	"testing"
)

func TestSpokenBindHistorySnapshots(t *testing.T) {
	history := &spokenBindHistory{}
	if got := history.snapshot(); got != nil {
		t.Fatalf("empty history: %v", got)
	}

	var snapshots [][]Tip

	for i := range 4096 {
		tip := Tip{1, uint64(i + 1)}
		history.add(tip)
		history.add(tip)

		if i == 0 || i == 15 || i == 256 || i == 4095 {
			snapshots = append(snapshots, history.snapshot())
		}
	}

	for i, snapshot := range snapshots {
		if len(snapshot) != []int{1, 16, 257, 4096}[i] || cap(snapshot) != len(snapshot) {
			t.Fatalf("snapshot %d changed size or exposes writable capacity: len=%d cap=%d", i, len(snapshot), cap(snapshot))
		}

		for j, tip := range snapshot {
			if tip != (Tip{1, uint64(j + 1)}) {
				t.Fatalf("snapshot %d lost consumer %d: %v", i, j, tip)
			}
		}

		appended := append(snapshot, Tip{99, 99})
		if appended[len(snapshot)] != (Tip{99, 99}) {
			t.Fatal("reader append failed")
		}
	}

	for i, tip := range history.snapshot() {
		if tip != (Tip{1, uint64(i + 1)}) {
			t.Fatalf("reader append modified published consumer %d: %v", i, tip)
		}
	}
}

func TestSpokenBindHistoryConcurrentSnapshots(t *testing.T) {
	history := &spokenBindHistory{}
	started, done := make(chan struct{}, 4), make(chan struct{})

	var readers sync.WaitGroup
	for range 4 {
		readers.Go(func() {
			started <- struct{}{}

			for {
				snapshot := history.snapshot()
				for i, tip := range snapshot {
					if tip != (Tip{1, uint64(i + 1)}) {
						t.Errorf("concurrent snapshot changed at %d: %v", i, tip)

						return
					}
				}

				select {
				case <-done:
					return
				default:
				}
			}
		})
	}

	for range 4 {
		<-started
	}

	for i := range 4096 {
		history.add(Tip{1, uint64(i + 1)})
	}

	close(done)
	readers.Wait()

	if got := len(history.snapshot()); got != 4096 {
		t.Fatalf("lost consumers: got %d", got)
	}
}

func BenchmarkSpokenBindHistory(b *testing.B) {
	for _, size := range []int{64, 1024, 4096} {
		b.Run(fmt.Sprintf("append-only/%d", size), func(b *testing.B) {
			b.ReportAllocs()

			for b.Loop() {
				history := &spokenBindHistory{}

				for i := range size {
					history.add(Tip{1, uint64(i)})
				}
			}
		})
		b.Run(fmt.Sprintf("scan-and-clone/%d", size), func(b *testing.B) {
			b.ReportAllocs()

			for b.Loop() {
				var tips []Tip

				for i := range size {
					tip := Tip{1, uint64(i)}
					if !slices.Contains(tips, tip) {
						tips = append(slices.Clone(tips), tip)
					}
				}
			}
		})
	}
}
