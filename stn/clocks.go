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
	"fmt"
	"maps"
	"math"
)

// Sample is a serialized reading in one node incarnation's elapsed-clock
// domain. Tick is in nanoseconds. Domain must change when the clock restarts.
type Sample struct {
	Domain uint64
	Tick   int64
}

type clockAnchor struct {
	vertex int
	tick   int64
}

// Clocks is an immutable system of difference constraints between clock
// anchors. A causal observation p -> e implies
//
//	T(anchor(p))-T(anchor(e)) <= U(e-anchor(e))-L(p-anchor(p)).
//
// L and U bound elapsed time using the supplied rate error. Thus local
// timestamp differences contribute numerical information, without comparing
// wall-clock origins or treating a causal edge as a latency measurement.
//
// One variable per observed incarnation keeps storage independent of the
// number of keys/effects. Anchoring weakens bounds as elapsed uncertainty
// grows; it never strengthens them. Retaining a *Clocks is an O(1) immutable
// evidence snapshot. Before returns a new version only for stronger evidence.
type Clocks struct {
	network *Network
	anchors map[uint64]clockAnchor
	ppm     int64
}

// NewClocks requires a bound on fractional elapsed-clock rate error in parts
// per million. This is an assumption about elapsed time, not clock offset or
// wall-clock synchronization. Zero is appropriate only for an exact-rate
// source (for example a controlled mathematical experiment).
func NewClocks(rateErrorPPM int64) (*Clocks, error) {
	if rateErrorPPM < 0 || rateErrorPPM >= 1_000_000 {
		return nil, fmt.Errorf("stn: invalid elapsed-clock rate bound: %d ppm", rateErrorPPM)
	}

	n, err := New(0, nil)
	if err != nil {
		return nil, err
	}

	return &Clocks{network: n, anchors: make(map[uint64]clockAnchor), ppm: rateErrorPPM}, nil
}

func (c *Clocks) anchor(s Sample) (clockAnchor, error) {
	if a, ok := c.anchors[s.Domain]; ok {
		return a, nil
	}

	v, err := c.network.AddPoint()
	if err != nil {
		return clockAnchor{}, err
	}

	a := clockAnchor{vertex: v, tick: s.Tick}
	c.anchors[s.Domain] = a

	return a, nil
}

// Before incorporates an actual causal observation. The receiver and all its
// previous versions remain unchanged, including on inconsistent input.
func (c *Clocks) Before(p, e Sample) (*Clocks, error) {
	if p.Domain == e.Domain {
		if p.Tick > e.Tick {
			return c, ErrInconsistent
		}

		return c, nil
	}

	pa, pok := c.anchors[p.Domain]
	ea, eok := c.anchors[e.Domain]

	if !pok {
		pa.tick = p.Tick
	}

	if !eok {
		ea.tick = e.Tick
	}

	pl, _, err := c.elapsed(p.Tick, pa.tick)
	if err != nil {
		return c, err
	}

	_, eu, err := c.elapsed(e.Tick, ea.tick)
	if err != nil {
		return c, err
	}

	upper, err := checkedSubtract(eu, pl)
	if err != nil {
		return c, err
	}

	if pok && eok {
		old, err := c.network.Upper(ea.vertex, pa.vertex)
		if err != nil {
			return c, err
		}

		if upper >= old {
			return c, nil
		}
	}

	next := &Clocks{network: c.network.clone(), anchors: c.anchors, ppm: c.ppm}
	// Existing anchors never change. Copy the map only when adding a domain.
	if !pok || !eok {
		next.anchors = maps.Clone(c.anchors)
	}

	pa, err = next.anchor(p)
	if err != nil {
		return c, err
	}

	ea, err = next.anchor(e)
	if err != nil {
		return c, err
	}

	if err := next.network.Connect(ea.vertex, pa.vertex); err != nil {
		return c, err
	}

	if err := next.network.Constrain(ea.vertex, pa.vertex, upper); err != nil {
		return c, err
	}

	return next, nil
}

// Difference bounds T(b)-T(a). An unbounded side is returned as +/-Infinity.
// No snapshot membership policy is implied by an interval crossing zero.
func (c *Clocks) Difference(a, b Sample) (lower, upper int64, err error) {
	if a.Domain == b.Domain {
		return c.elapsed(b.Tick, a.Tick)
	}

	aa, aok := c.anchors[a.Domain]

	ba, bok := c.anchors[b.Domain]
	if !aok || !bok {
		return -Infinity, Infinity, nil
	}

	al, au, err := c.elapsed(a.Tick, aa.tick)
	if err != nil {
		return 0, 0, err
	}

	bl, bu, err := c.elapsed(b.Tick, ba.tick)
	if err != nil {
		return 0, 0, err
	}

	ab, baBound := c.network.bounds(aa.vertex, ba.vertex)

	upper = Infinity
	if ab != Infinity {
		upper = outwardSum(ab, bu, -al)
	}

	lower = -Infinity
	if baBound != Infinity {
		lower = -outwardSum(baBound, au, -bl)
	}

	return lower, upper, nil
}

func checkedSubtract(a, b int64) (int64, error) {
	if b > 0 && a < math.MinInt64+b || b < 0 && a > math.MaxInt64+b {
		return 0, ErrWeight
	}

	return a - b, nil
}

// elapsed rounds bounds outwards, including for observations older than the
// anchor. Splitting the quotient avoids multiplying a nanosecond duration by
// a million (which would overflow for durations of a few hours).
func (c *Clocks) elapsed(tick, anchor int64) (int64, int64, error) {
	d, err := checkedSubtract(tick, anchor)
	if err != nil || d == math.MinInt64 {
		return 0, 0, ErrWeight
	}

	negative := d < 0
	if negative {
		d = -d
	}

	scale := func(denominator int64, ceil bool) (int64, error) {
		q, r := d/denominator, d%denominator

		fraction := r * 1_000_000 / denominator
		if ceil && r*1_000_000%denominator != 0 {
			fraction++
		}

		if q > (math.MaxInt64-fraction)/1_000_000 {
			return 0, ErrWeight
		}

		return q*1_000_000 + fraction, nil
	}

	lower, err := scale(1_000_000+c.ppm, false)
	if err != nil {
		return 0, 0, err
	}

	upper, err := scale(1_000_000-c.ppm, true)
	if err != nil {
		return 0, 0, err
	}

	if negative {
		return -upper, -lower, nil
	}

	return lower, upper, nil
}

// A bound beyond representable duration is unknown rather than a wrapped
// number that could falsely prove an order. Inputs here have three terms.
func outwardSum(a, b, c int64) int64 {
	for _, v := range []int64{b, c} {
		if v > 0 && a > math.MaxInt64-v || v < 0 && a < math.MinInt64-v {
			return Infinity
		}

		a += v
	}

	return a
}
