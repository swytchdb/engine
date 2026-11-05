package stn

import (
	"fmt"
	"testing"
)

func BenchmarkUpperChain(b *testing.B) {
	for _, n := range []int{16, 128, 1024} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			pairs := make([]Pair, n-1)
			for i := range pairs {
				pairs[i] = Pair{i, i + 1}
			}

			s, err := New(n, pairs)
			if err != nil {
				b.Fatal(err)
			}

			for _, p := range pairs {
				if err := s.Constrain(p.A, p.B, 1); err != nil {
					b.Fatal(err)
				}
			}

			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				if got, err := s.Upper(0, n-1); err != nil || got != int64(n-1) {
					b.Fatalf("bound = %d, error = %v", got, err)
				}
			}
		})
	}
}

func BenchmarkClocks(b *testing.B) {
	for _, n := range []int{16, 128} {
		c, err := NewClocks(0)
		if err != nil {
			b.Fatal(err)
		}

		for i := 1; i < n; i++ {
			c, err = c.Before(Sample{Domain: uint64(i - 1)}, Sample{Domain: uint64(i)})
			if err != nil {
				b.Fatal(err)
			}
		}

		b.Run(fmt.Sprintf("Difference/%d", n), func(b *testing.B) {
			b.ReportAllocs()

			for range b.N {
				lo, hi, err := c.Difference(Sample{Domain: uint64(n - 1)}, Sample{Domain: 0})
				if err != nil || lo != -Infinity || hi != 0 {
					b.Fatalf("bounds = [%d,%d], error = %v", lo, hi, err)
				}
			}
		})
		b.Run(fmt.Sprintf("Before/%d", n), func(b *testing.B) {
			b.ReportAllocs()

			for range b.N {
				// Branch from the same snapshot so every iteration tightens
				// a bound without changing the benchmark's starting state.
				_, err := c.Before(Sample{Domain: 0, Tick: 1}, Sample{Domain: 1})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
