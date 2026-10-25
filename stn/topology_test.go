package stn

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
)

func checkPEO(t *testing.T, s *Network) {
	t.Helper()

	if len(s.order) != len(s.adj) || len(s.rank) != len(s.adj) {
		t.Fatal("incomplete elimination order")
	}

	seen := make([]bool, len(s.adj))
	for rank, v := range s.order {
		if !s.vertex(v) || seen[v] || s.rank[v] != rank {
			t.Fatalf("invalid order %v / rank %v", s.order, s.rank)
		}

		seen[v] = true
		for a := range s.adj[v] {
			for b := range s.adj[v] {
				if a != b && s.rank[a] > rank && s.rank[b] > rank {
					if _, ok := s.adj[a][b]; !ok {
						t.Fatalf("order is not perfect: %d has unjoined later neighbors %d,%d", v, a, b)
					}
				}
			}
		}
	}
}

func rawNetwork(n int) [][]int64 {
	raw := make([][]int64, n)
	for i := range raw {
		raw[i] = make([]int64, n)
		for j := range raw[i] {
			if i != j {
				raw[i][j] = Infinity
			}
		}
	}

	return raw
}

func checkAllBounds(t *testing.T, s *Network, raw [][]int64) {
	t.Helper()

	want, ok := floyd(raw)
	if !ok {
		t.Fatal("oracle input unexpectedly inconsistent")
	}

	checkPEO(t, s)

	for i := range raw {
		for j := range raw {
			got, err := s.Upper(i, j)
			if err != nil || got != want[i][j] {
				t.Fatalf("%d->%d: got %d (%v), Floyd %d", i, j, got, err, want[i][j])
			}
		}
	}
}

func TestChordalGraphsDoNotAcquireFill(t *testing.T) {
	for _, center := range []int{0, 31, 63} {
		t.Run(fmt.Sprintf("star-%d", center), func(t *testing.T) {
			var pairs []Pair

			for i := range 64 {
				if i != center {
					pairs = append(pairs, Pair{center, i})
				}
			}

			s, err := New(64, pairs)
			if err != nil {
				t.Fatal(err)
			}

			checkPEO(t, s)

			count := 0
			for _, edges := range s.adj {
				count += len(edges)
			}

			if count != 2*len(pairs) {
				t.Fatalf("star acquired fill: %d directed edges", count)
			}
		})
	}
	// Disconnected, randomly relabeled trees exercise MCS's zero-cardinality
	// buckets; the oracle previously caught an omitted queue removal here.
	for seed := uint64(1); seed <= 20; seed++ {
		rng := rand.New(rand.NewPCG(seed, seed+7))
		labels := rng.Perm(40)

		var pairs []Pair

		for i := 1; i < 40; i++ {
			if i%7 != 0 {
				pairs = append(pairs, Pair{labels[i], labels[rng.IntN(i)]})
			}
		}

		s, err := New(40, pairs)
		if err != nil {
			t.Fatal(err)
		}

		checkPEO(t, s)

		count := 0
		for _, edges := range s.adj {
			count += len(edges)
		}

		if count != 2*len(pairs) {
			t.Fatalf("forest acquired fill, seed %d", seed)
		}
	}
}

func TestGrowingStarRemainsSparse(t *testing.T) {
	s, _ := New(1, nil)
	for i := 1; i <= 256; i++ {
		if _, err := s.AddPoint(); err != nil {
			t.Fatal(err)
		}

		if err := s.Connect(0, i); err != nil {
			t.Fatal(err)
		}

		if err := s.Constrain(i, 0, -int64(i)); err != nil {
			t.Fatal(err)
		}

		if err := s.Constrain(0, i, int64(i+10)); err != nil {
			t.Fatal(err)
		}

		count := 0
		for _, edges := range s.adj {
			count += len(edges)
		}

		if count != 2*i {
			t.Fatalf("domain %d acquired fill: %d", i, count)
		}
	}

	checkPEO(t, s)

	if got, err := s.Upper(256, 1); err != nil || got != -245 {
		t.Fatalf("leaf-to-leaf bound %d: %v", got, err)
	}
}

func TestGrowingTopologyMatchesFloyd(t *testing.T) {
	for seed := uint64(1); seed <= 30; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, seed+11))
			s, _ := New(0, nil)

			var raw [][]int64

			for n := 1; n <= 18; n++ {
				if _, err := s.AddPoint(); err != nil {
					t.Fatal(err)
				}

				for i := range raw {
					raw[i] = append(raw[i], Infinity)
				}

				row := make([]int64, n)
				for i := 0; i < n-1; i++ {
					row[i] = Infinity
				}

				raw = append(raw, row)
				checkAllBounds(t, s, raw)

				if n < 2 {
					continue
				}

				for range 6 {
					a, b := rng.IntN(n), rng.IntN(n)
					if a == b {
						continue
					}

					if err := s.Connect(a, b); err != nil {
						t.Fatal(err)
					}

					checkAllBounds(t, s, raw) // topology/fill must not change any bound

					w := int64(rng.IntN(81) - 40)
					old := raw[a][b]
					raw[a][b] = min(old, w)
					_, consistent := floyd(raw)

					err := s.Constrain(a, b, w)
					if !consistent {
						if !errors.Is(err, ErrInconsistent) {
							t.Fatalf("expected inconsistency, got %v", err)
						}

						raw[a][b] = old
					} else if err != nil {
						t.Fatal(err)
					}

					checkAllBounds(t, s, raw)
				}
			}
		})
	}
}

func TestSparseQueriesLargeWeightsMatchFloyd(t *testing.T) {
	for seed := uint64(1); seed <= 20; seed++ {
		rng := rand.New(rand.NewPCG(seed, seed+23))

		const n = 25

		var pairs []Pair
		for i := 1; i < n; i++ {
			pairs = append(pairs, Pair{i - 1, i})
		}

		for range 20 {
			a, b := rng.IntN(n), rng.IntN(n)
			if a != b {
				pairs = append(pairs, Pair{a, b})
			}
		}

		s, err := New(n, pairs)
		if err != nil {
			t.Fatal(err)
		}

		raw := rawNetwork(n)

		pot := make([]int64, n)
		for i := range pot {
			pot[i] = rng.Int64N(s.WeightLimit()/2) - s.WeightLimit()/4
		}

		for _, p := range pairs {
			for _, p := range []Pair{p, {p.B, p.A}} {
				w := pot[p.B] - pot[p.A] + rng.Int64N(s.WeightLimit()/4)
				if err := s.Constrain(p.A, p.B, w); err != nil {
					t.Fatal(err)
				}

				raw[p.A][p.B] = min(raw[p.A][p.B], w)
			}
		}

		checkAllBounds(t, s, raw)
	}
}

func TestUpwardPathOverflowDoesNotManufactureNegativeBound(t *testing.T) {
	// Input constraints are bounded, but a nonoptimal path can combine many
	// derived bounds. Such a candidate must not become a negative ordering.
	if got := pathSum(Infinity-2, 10); got != Infinity {
		t.Fatalf("overflow became a finite bound: %d", got)
	}

	if got := pathSum(Infinity, -10); got != Infinity {
		t.Fatalf("unreachable became finite: %d", got)
	}

	if got := pathSum(-100, 70); got != -30 {
		t.Fatalf("negative weights lost: %d", got)
	}
}
