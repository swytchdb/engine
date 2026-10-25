package stn

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
)

func TestNumericalInferenceBeyondReachability(t *testing.T) {
	// b0 <= a0 is the sole cross-clock observation. b1 is 10 units
	// after b0, while a1 is 30 units after a0. There is no causal path
	// b1 -> a1, but time(b1)-time(a1) <= -20 follows numerically.
	topology := []Pair{{0, 1}, {0, 2}, {1, 3}}
	causal, _ := New(4, topology)
	numeric, _ := New(4, topology)

	for _, c := range []struct {
		a, b int
		w    int64
	}{{1, 0, 0}, {2, 0, 0}, {3, 1, 0}} {
		if err := causal.Constrain(c.a, c.b, c.w); err != nil {
			t.Fatal(err)
		}

		if err := numeric.Constrain(c.a, c.b, c.w); err != nil {
			t.Fatal(err)
		}
	}

	for _, c := range []struct {
		a, b int
		w    int64
	}{{0, 2, 10}, {2, 0, -10}, {1, 3, 30}, {3, 1, -30}} {
		if err := numeric.Constrain(c.a, c.b, c.w); err != nil {
			t.Fatal(err)
		}
	}

	if got, _ := causal.Upper(3, 2); got != Infinity {
		t.Fatalf("reachability unexpectedly ordered b1/a1: %d", got)
	}

	if got, _ := numeric.Upper(3, 2); got != -20 {
		t.Fatalf("numerical bound = %d, want -20", got)
	}

	t.Log("causal: unknown; IPPC: b1 precedes a1 by at least 20 time units")
}

// floyd is an independent, deliberately dense test oracle over the original
// input constraints. Production IPPC does not call it or store this matrix.
func floyd(raw [][]int64) ([][]int64, bool) {
	d := make([][]int64, len(raw))
	for i := range raw {
		d[i] = append([]int64(nil), raw[i]...)
	}

	for k := range d {
		for i := range d {
			for j := range d {
				if d[i][k] != Infinity && d[k][j] != Infinity {
					d[i][j] = min(d[i][j], d[i][k]+d[k][j])
				}
			}
		}
	}

	for i := range d {
		if d[i][i] < 0 {
			return d, false
		}
	}

	return d, true
}

func TestIPPCMatchesFloydWarshallAfterEveryInsertion(t *testing.T) {
	for seed := uint64(1); seed <= 40; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, seed+1))
			n := 2 + rng.IntN(20)
			topology := make([]Pair, 0)

			for i := range n {
				for j := i + 1; j < n; j++ {
					if rng.IntN(4) == 0 {
						topology = append(topology, Pair{i, j})
					}
				}
			}

			if len(topology) == 0 {
				topology = append(topology, Pair{0, 1})
			}

			s, err := New(n, topology)
			if err != nil {
				t.Fatal(err)
			}

			raw := make([][]int64, n)
			for i := range raw {
				raw[i] = make([]int64, n)
				for j := range raw[i] {
					if i != j {
						raw[i][j] = Infinity
					}
				}
			}

			for step := range 100 {
				p := topology[rng.IntN(len(topology))]
				if rng.IntN(2) == 0 {
					p.A, p.B = p.B, p.A
				}

				w := int64(rng.IntN(81) - 40)
				old := raw[p.A][p.B]
				raw[p.A][p.B] = min(old, w)
				want, consistent := floyd(raw)

				err := s.Constrain(p.A, p.B, w)
				if !consistent {
					if !errors.Is(err, ErrInconsistent) {
						t.Fatalf("step %d: want contradiction, got %v", step, err)
					}

					raw[p.A][p.B] = old
					want, _ = floyd(raw)
				} else if err != nil {
					t.Fatalf("step %d: %v", step, err)
				}

				for i := range raw {
					for j := range raw {
						got, err := s.Upper(i, j)
						if err != nil || got != want[i][j] {
							t.Fatalf("step %d, %d->%d: IPPC %d (%v), Floyd %d", step, i, j, got, err, want[i][j])
						}
					}
				}
			}
		})
	}
}

func TestValidationAndAtomicContradiction(t *testing.T) {
	s, err := New(3, []Pair{{0, 1}})
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		a, b int
		w    int64
		err  error
	}{
		{-1, 0, 0, ErrVertex},
		{0, 3, 0, ErrVertex},
		{0, 2, 0, ErrEdge},
		{0, 0, -1, ErrInconsistent},
		{0, 1, Infinity, ErrWeight},
		{0, 1, -Infinity, ErrWeight},
	} {
		if got := s.Constrain(tc.a, tc.b, tc.w); !errors.Is(got, tc.err) {
			t.Fatalf("%+v: %v", tc, got)
		}
	}

	if err := s.Constrain(0, 1, -10); err != nil {
		t.Fatal(err)
	}

	if err := s.Constrain(1, 0, 9); !errors.Is(err, ErrInconsistent) {
		t.Fatal(err)
	}

	if got, _ := s.Upper(1, 0); got != Infinity {
		t.Fatalf("rejected constraint mutated network: %d", got)
	}

	if err := s.Constrain(1, 0, 10); err != nil {
		t.Fatal(err)
	}

	if err := s.Constrain(0, 1, 0); err != nil {
		t.Fatal(err)
	}

	if got, _ := s.Upper(0, 1); got != -10 {
		t.Fatalf("looser constraint changed bound: %d", got)
	}

	if _, err := New(2, []Pair{{0, 2}}); !errors.Is(err, ErrVertex) {
		t.Fatal(err)
	}

	if _, err := New(-1, nil); !errors.Is(err, ErrVertex) {
		t.Fatal(err)
	}
}

func TestLargeWeightsDoNotWrap(t *testing.T) {
	s, _ := New(4, []Pair{{0, 1}, {1, 2}, {2, 3}, {3, 0}})

	w := s.WeightLimit()
	for i := range 3 {
		if err := s.Constrain(i, i+1, -w); err != nil {
			t.Fatal(err)
		}
	}

	if got, _ := s.Upper(0, 3); got != -3*w {
		t.Fatalf("bound %d, want %d", got, -3*w)
	}

	if err := s.Constrain(3, 0, w); !errors.Is(err, ErrInconsistent) {
		t.Fatalf("%v", err)
	}
}

func TestGrowingTopologyPreservesBounds(t *testing.T) {
	s, _ := New(0, nil)
	for i := range 8 {
		if got, err := s.AddPoint(); err != nil || got != i {
			t.Fatalf("add point %d: %d, %v", i, got, err)
		}
	}
	// Grow a cycle one edge at a time, then insert diagonals whose chordal
	// fill changes the graph. Every topology-only change must preserve bounds.
	for _, p := range []Pair{{0, 1}, {1, 2}, {2, 3}, {3, 4}, {4, 5}, {5, 6}, {6, 7}, {7, 0}, {0, 4}, {2, 6}} {
		before := make([][]int64, 8)
		for i := range before {
			before[i] = make([]int64, 8)
			for j := range before[i] {
				before[i][j], _ = s.Upper(i, j)
			}
		}

		if err := s.Connect(p.A, p.B); err != nil {
			t.Fatal(err)
		}

		for i := range before {
			for j := range before[i] {
				if got, _ := s.Upper(i, j); got != before[i][j] {
					t.Fatalf("Connect(%v) changed %d->%d: %d != %d", p, i, j, got, before[i][j])
				}
			}
		}

		if err := s.Constrain(p.A, p.B, int64(p.B-p.A)); err != nil {
			t.Fatal(err)
		}

		if err := s.Constrain(p.B, p.A, int64(p.A-p.B)); err != nil {
			t.Fatal(err)
		}
	}

	for i := range 8 {
		for j := range 8 {
			if got, _ := s.Upper(i, j); got != int64(j-i) {
				t.Fatalf("%d->%d = %d", i, j, got)
			}
		}
	}
}

func BenchmarkIPPCTightenChain(b *testing.B) {
	for _, n := range []int{16, 128, 1024} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			topology := make([]Pair, 0, n-1)
			for i := 1; i < n; i++ {
				topology = append(topology, Pair{i - 1, i})
			}

			s, _ := New(n, topology)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := s.Constrain(0, 1, -int64(i)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
