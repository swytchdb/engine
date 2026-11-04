/*
 * Copyright 2026 Swytch Labs BV
 *
 * This file is part of Swytch.
 *
 * Swytch is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Swytch is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Swytch. If not, see <https://www.gnu.org/licenses/>.
 *
 * Additional permissions under GNU AGPL version 3 section 7 apply to
 * this file. See the NOTICE.md file distributed with this source code
 * for the current set of additional permissions.
 */

package stn

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/rand/v2"
	"testing"
)

func TestClockConstraintsNumericalInferenceAndPersistence(t *testing.T) {
	c, err := NewClocks(1000)
	if err != nil {
		t.Fatal(err)
	}
	// A causal observation supplies only an upper bound, never an exact
	// offset. The origins differ by five hours. All intervals are ns.
	p := Sample{Domain: 1, Tick: 5 * 60 * 60 * 1e9}
	e := Sample{Domain: 2, Tick: 0}

	next, err := c.Before(p, e)
	if err != nil {
		t.Fatal(err)
	}

	query := Sample{Domain: 1, Tick: p.Tick + 10e6}
	boundary := Sample{Domain: 2, Tick: 40e6}

	lo, hi, err := next.Difference(boundary, query)
	if err != nil || hi >= 0 || lo != -Infinity {
		t.Fatalf("expected one-sided numerical before: [%d,%d] %v", lo, hi, err)
	}

	_, oldHi, err := c.Difference(boundary, query)
	if err != nil || oldHi != Infinity {
		t.Fatalf("mutated retained evidence: %d %v", oldHi, err)
	}

	redundant, err := next.Before(p, e)
	if err != nil || redundant != next {
		t.Fatalf("redundant observation allocated a version: %v", err)
	}

	_, err = next.Before(Sample{Domain: 2, Tick: 1e9}, p)
	if !errors.Is(err, ErrInconsistent) {
		t.Fatalf("expected a contradictory cycle, got %v", err)
	}

	_, afterHi, _ := next.Difference(boundary, query)
	if afterHi != hi {
		t.Fatal("rejected observation mutated a retained version")
	}
}

func TestClockElapsedOutwardRounding(t *testing.T) {
	rng := rand.New(rand.NewPCG(17, 31))

	for _, ppm := range []int64{0, 1, 1000, 10_000, 999_999} {
		c, err := NewClocks(ppm)
		if err != nil {
			t.Fatal(err)
		}

		for i := range 1000 {
			d := rng.Int64N(1 << 50)
			if i%2 == 0 {
				d = -d
			}

			lo, hi, err := c.elapsed(d, 0)
			if err != nil {
				// Very large intervals with nearly unbounded clock rate
				// error legitimately exceed int64. No wrapped result.
				if !errors.Is(err, ErrWeight) {
					t.Fatal(err)
				}

				continue
			}

			for _, den := range []int64{1_000_000 - ppm, 1_000_000 + ppm} {
				exact := new(big.Rat).SetFrac(new(big.Int).Mul(big.NewInt(d), big.NewInt(1_000_000)), big.NewInt(den))
				if exact.Cmp(new(big.Rat).SetInt64(lo)) < 0 || exact.Cmp(new(big.Rat).SetInt64(hi)) > 0 {
					t.Fatalf("ppm=%d d=%d bounds=[%d,%d] exclude %s", ppm, d, lo, hi, exact)
				}
			}
		}
	}

	c, _ := NewClocks(1000)
	if _, _, err := c.elapsed(math.MaxInt64, math.MinInt64); !errors.Is(err, ErrWeight) {
		t.Fatalf("unchecked subtraction overflow: %v", err)
	}
}

func TestClockConstraintsSoundAgainstElapsedSources(t *testing.T) {
	rng := rand.New(rand.NewPCG(41, 63))
	c, _ := NewClocks(10_000)
	// Mathematical clock sources with different origins and rates, all
	// inside the declared 1% envelope. These are not engine histories.
	rates := []int64{995_000, 1_000_000, 1_006_000, 1_009_000}
	origins := []int64{-9e12, 7e12, 30e12, -2e12}
	sample := func(domain int, truth int64) Sample {
		return Sample{Domain: uint64(domain), Tick: origins[domain] + (truth/1_000_000)*rates[domain]}
	}

	for range 200 {
		a, b := rng.IntN(len(rates)), rng.IntN(len(rates))
		before := rng.Int64N(1e9) * 1_000_000
		after := before + rng.Int64N(1e6)*1_000_000

		var err error

		c, err = c.Before(sample(a, before), sample(b, after))
		if err != nil {
			t.Fatal(err)
		}

		for range 20 {
			u, v := rng.IntN(len(rates)), rng.IntN(len(rates))
			t1, t2 := rng.Int64N(1e9)*1_000_000, rng.Int64N(1e9)*1_000_000

			lo, hi, err := c.Difference(sample(u, t1), sample(v, t2))
			if err != nil || t2-t1 < lo || t2-t1 > hi {
				t.Fatalf("true difference %d outside [%d,%d]: %v", t2-t1, lo, hi, err)
			}
		}
	}
}

func TestClockSnapshotsBranchConcurrently(t *testing.T) {
	c, err := NewClocks(0)
	if err != nil {
		t.Fatal(err)
	}

	for i := range 2 {
		c, err = c.Before(Sample{Domain: uint64(i)}, Sample{Domain: uint64(i + 1)})
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := range 8 {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()

			// Tightening keeps the anchors; growth adds an anchor and changes
			// topology. Neither may modify the base or another branch.
			branch, err := c.Before(Sample{Domain: 0, Tick: 1}, Sample{Domain: 1})
			if err != nil {
				t.Fatal(err)
			}

			added := Sample{Domain: uint64(i + 10)}

			branch, err = branch.Before(Sample{Domain: 2}, added)
			if err != nil {
				t.Fatal(err)
			}

			lo, hi, err := branch.Difference(added, Sample{Domain: 0})
			if err != nil || lo != -Infinity || hi != -1 {
				t.Fatalf("branch bounds = [%d,%d], error = %v", lo, hi, err)
			}

			lo, hi, err = c.Difference(Sample{Domain: 2}, Sample{Domain: 0})
			if err != nil || lo != -Infinity || hi != 0 {
				t.Fatalf("base bounds changed: [%d,%d], error = %v", lo, hi, err)
			}

			lo, hi, err = c.Difference(added, Sample{Domain: 0})
			if err != nil || lo != -Infinity || hi != Infinity {
				t.Fatalf("new domain leaked into base: [%d,%d], error = %v", lo, hi, err)
			}
		})
	}
}
