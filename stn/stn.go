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

// Package stn implements incremental partial path consistency (IPPC),
// Algorithm 1 and Tag in Planken, de Weerdt and Yorke-Smith, ICAPS 2010:
// https://doi.org/10.1609/icaps.v20i1.13417.
// An edge u -> v of weight w means time(v)-time(u) <= w.
package stn

import (
	"errors"
	"maps"
	"math"
	"slices"
)

const Infinity int64 = math.MaxInt64

var (
	ErrInconsistent = errors.New("stn: inconsistent constraint")
	ErrVertex       = errors.New("stn: invalid vertex")
	ErrEdge         = errors.New("stn: edge absent from chordal topology")
	ErrWeight       = errors.New("stn: weight outside supported range")
)

// Pair declares an undirected topology edge. Both directional weights start
// at infinity. New adds chordal fill edges, also initially unconstrained.
type Pair struct{ A, B int }

// Network stores tight bounds on chordal edges, not an all-pairs matrix.
// The caller owns synchronization. No clock or conflict-arbitration policy is
// embedded in this package.
type Network struct {
	adj      []map[int]int64
	limit    int64
	maxInput int64
	order    []int // perfect elimination order, first eliminated to last
	rank     []int // inverse of order; vertex IDs remain stable
}

// New creates an unconstrained STN. Chordal input keeps its topology; otherwise
// minimum-degree elimination supplies fill before incremental tightening.
func New(n int, topology []Pair) (*Network, error) {
	if n < 0 || n > 1<<20 {
		return nil, ErrVertex
	}

	s := &Network{adj: make([]map[int]int64, n), limit: (1 << 60) / int64(n+1)}
	for i := range s.adj {
		s.adj[i] = make(map[int]int64)
	}

	for _, p := range topology {
		if !s.vertex(p.A) || !s.vertex(p.B) {
			return nil, ErrVertex
		}

		if p.A != p.B {
			s.link(p.A, p.B)
		}
	}

	s.triangulate(s.link)

	return s, nil
}

func (s *Network) vertex(v int) bool { return v >= 0 && v < len(s.adj) }

// clone copies mutable edge weights. Elimination orders are replaced, never
// edited; clipping their capacity also makes AddPoint's appends independent.
func (s *Network) clone() *Network {
	n := &Network{
		adj: make([]map[int]int64, len(s.adj)), limit: s.limit, maxInput: s.maxInput,
		order: slices.Clip(s.order), rank: slices.Clip(s.rank),
	}
	for v, edges := range s.adj {
		n.adj[v] = maps.Clone(edges)
	}

	return n
}

func (s *Network) link(a, b int) {
	if _, ok := s.adj[a][b]; !ok {
		s.adj[a][b], s.adj[b][a] = Infinity, Infinity
	}
}

// AddPoint appends an unconstrained variable. Existing vertex IDs remain
// stable. It checks the arithmetic budget before changing the network.
func (s *Network) AddPoint() (int, error) {
	n := len(s.adj)
	if n >= 1<<20 {
		return 0, ErrVertex
	}

	limit := int64(1<<60) / int64(n+2)
	if s.maxInput > limit {
		return 0, ErrWeight
	}

	s.adj = append(s.adj, make(map[int]int64))
	// An isolated vertex can be placed anywhere in a PEO.
	s.order = append(s.order, n)
	s.rank = append(s.rank, n)
	s.limit = limit

	return n, nil
}

// Connect admits a new topology edge without imposing a constraint. Existing
// shortest-path bounds initialize it and any required chordal fill, preserving
// the feasible set and PPC. This structural operation may cost more than an
// IPPC tightening: new fill bounds require shortest-path queries. Callers that
// know the topology should pass it to New to avoid repeated triangulation.
func (s *Network) Connect(a, b int) error {
	if !s.vertex(a) || !s.vertex(b) {
		return ErrVertex
	}

	if a == b {
		return nil
	}

	if _, ok := s.adj[a][b]; ok {
		return nil
	}

	add := func(u, v int) {
		s.adj[u][v], s.adj[v][u] = s.bounds(u, v)
	}
	add(a, b)

	// Added edges carry already-implied bounds. Until the new PEO is installed,
	// the old upward paths still contain a shortest path for every old query;
	// the redundant edges cannot introduce a stronger one. Thus add can query
	// existing bounds while the new chordal completion is being constructed.
	s.triangulate(add)

	return nil
}

// WeightLimit bounds input magnitude so finite simple paths and IPPC's
// three-term additions fit int64. Invalid input never wraps or saturates.
func (s *Network) WeightLimit() int64 { return s.limit }

// Constrain tightens time(to)-time(from) <= upper using published IPPC with
// the original Tag procedure. Contradictions are detected before mutation.
// Redundant and looser constraints are no-ops.
func (s *Network) Constrain(from, to int, upper int64) error {
	if !s.vertex(from) || !s.vertex(to) {
		return ErrVertex
	}

	if upper < -s.limit || upper > s.limit {
		return ErrWeight
	}

	if from == to {
		if upper < 0 {
			return ErrInconsistent
		}

		return nil
	}

	old, ok := s.adj[from][to]
	if !ok {
		return ErrEdge
	}
	// In a PPC graph the reverse edge is already a tight shortest-path
	// bound. Algorithm 1 checks it before touching any weights.
	if pathSum(upper, s.adj[to][from]) < 0 {
		return ErrInconsistent
	}

	if upper >= old {
		return nil
	}

	s.maxInput = max(s.maxInput, upper, -upper)
	distance := make([]distances, len(s.adj))
	resetDistances(distance)
	distance[from].forward, distance[to].backward = 0, 0
	q := newCardinality(len(s.adj))

	// Tag the constrained endpoints first, then visit by maximum cardinality.
	// forward tracks u -> from; backward tracks to -> u (Algorithm 1).
	for v := from; v >= 0; {
		q.remove(v)

		for u, vu := range s.adj[v] {
			uv := s.adj[u][v]
			if q.contains(u) {
				distance[u].forward = min(distance[u].forward, pathSum(uv, distance[v].forward))
				distance[u].backward = min(distance[u].backward, pathSum(distance[v].backward, vu))
				q.increment(u)
			} else {
				s.adj[u][v] = min(uv, pathSum(pathSum(distance[u].forward, upper), distance[v].backward))
				s.adj[v][u] = min(vu, pathSum(pathSum(distance[v].forward, upper), distance[u].backward))
			}
		}

		if v == from {
			v = to
		} else {
			v = q.maximum()
		}
	}

	return nil
}

// Upper returns the tight upper bound on time(to)-time(from); Infinity means
// no finite bound. Chordal-edge queries are O(1). Other pairs use upward paths
// in the perfect elimination order, in O(V+E) time and O(V) temporary memory.
// In a PPC graph any valley in a path can be replaced by its tight chord, so
// some shortest path rises to a common peak and then descends. Negative edge
// weights are supported: both upward searches are DAG dynamic programs.
func (s *Network) Upper(from, to int) (int64, error) {
	if !s.vertex(from) || !s.vertex(to) {
		return 0, ErrVertex
	}

	if from == to {
		return 0, nil
	}

	if w, ok := s.adj[from][to]; ok {
		return w, nil
	}

	return s.upper(from, to, make([]distances, len(s.adj))), nil
}

// bounds queries both directions between distinct, valid vertices. The two
// searches reuse one buffer; adjacent vertices need no scratch allocation.
func (s *Network) bounds(from, to int) (forward, backward int64) {
	if w, ok := s.adj[from][to]; ok {
		return w, s.adj[to][from]
	}

	distance := make([]distances, len(s.adj))

	return s.upper(from, to, distance), s.upper(to, from, distance)
}

// upper queries validated, nonadjacent vertices using caller-owned scratch.
// Keeping scratch out of Network allows concurrent immutable clock reads.
func (s *Network) upper(from, to int, distance []distances) int64 {
	resetDistances(distance)
	distance[from].forward, distance[to].backward = 0, 0

	bound := Infinity
	for _, v := range s.order {
		bound = min(bound, pathSum(distance[v].forward, distance[v].backward))
		if distance[v].forward == Infinity && distance[v].backward == Infinity {
			continue
		}

		for u, vu := range s.adj[v] {
			if s.rank[u] <= s.rank[v] {
				continue
			}

			distance[u].forward = min(distance[u].forward, pathSum(distance[v].forward, vu))
			distance[u].backward = min(distance[u].backward, pathSum(distance[v].backward, s.adj[u][v]))
		}
	}

	return bound
}

type distances struct{ forward, backward int64 }

func resetDistances(distance []distances) {
	for i := range distance {
		distance[i] = distances{Infinity, Infinity}
	}
}

// A nonoptimal upward path may sum many already-derived edge bounds. Ignore
// positive overflow rather than wrapping it into a false negative bound. This
// cannot discard the optimum: some shortest path is upward/downward, every
// prefix/suffix of that path is shortest, and every finite shortest-path bound
// is within V*maxInput (< 1<<60). Consistency likewise prevents negative
// underflow on any path. IPPC's three-term sums fit within the input budget.
func pathSum(a, b int64) int64 {
	if a == Infinity || b == Infinity || b > 0 && a > math.MaxInt64-b {
		return Infinity
	}

	return a + b
}
