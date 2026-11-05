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

import "container/heap"

// chordalOrder recognizes an existing chordal graph with maximum cardinality
// search. Its reverse selection order is a PEO iff every vertex's higher-rank
// neighbors form a clique. This preserves stars, trees, and all other chordal
// inputs without adding fill, regardless of vertex numbering.
func (s *Network) chordalOrder() (order, rank []int) {
	n := len(s.adj)
	q := newCardinality(n)
	order, rank = make([]int, n), make([]int, n)

	for i := n - 1; i >= 0; i-- {
		v := q.maximum()
		q.remove(v)

		order[i], rank[v] = v, i
		for u := range s.adj[v] {
			if q.contains(u) {
				q.increment(u)
			}
		}
	}

	for v := range s.adj {
		parent := -1
		for u := range s.adj[v] {
			if rank[u] > rank[v] && (parent < 0 || rank[u] < rank[parent]) {
				parent = u
			}
		}

		if parent < 0 {
			continue
		}

		for u := range s.adj[v] {
			if u != parent && rank[u] > rank[v] {
				if _, ok := s.adj[parent][u]; !ok {
					return order, nil
				}
			}
		}
	}

	return order, rank
}

// triangulate only adds fill when recognition fails. Minimum degree is a
// heuristic, not an optimal-treewidth claim. add initializes the new edge's
// weights; Connect supplies existing shortest-path bounds, New uses infinity.
func (s *Network) triangulate(add func(int, int)) {
	order, rank := s.chordalOrder()
	if rank != nil {
		s.order, s.rank = order, rank

		return
	}

	n := len(s.adj)
	q := &degreeQueue{vertices: make([]int, n), position: make([]int, n), degree: make([]int, n)}

	for v := range s.adj {
		q.vertices[v], q.position[v], q.degree[v] = v, v, len(s.adj[v])
	}

	heap.Init(q)

	order = order[:0]

	var neighbors []int

	for q.Len() != 0 {
		v := heap.Pop(q).(int)
		order = append(order, v)

		neighbors = neighbors[:0]

		for u := range s.adj[v] {
			if q.position[u] < 0 {
				continue
			}

			neighbors = append(neighbors, u)
			q.degree[u]--
			heap.Fix(q, q.position[u])
		}

		for i, u := range neighbors {
			for _, w := range neighbors[i+1:] {
				if _, ok := s.adj[u][w]; ok {
					continue
				}

				add(u, w)
				q.degree[u]++
				q.degree[w]++
				heap.Fix(q, q.position[u])
				heap.Fix(q, q.position[w])
			}
		}
	}

	s.order = order

	s.rank = make([]int, len(order))
	for rank, v := range order {
		s.rank[v] = rank
	}
}

type degreeQueue struct{ vertices, position, degree []int }

func (q *degreeQueue) Len() int { return len(q.vertices) }
func (q *degreeQueue) Less(i, j int) bool {
	a, b := q.vertices[i], q.vertices[j]

	return q.degree[a] < q.degree[b] || q.degree[a] == q.degree[b] && a < b
}

func (q *degreeQueue) Swap(i, j int) {
	q.vertices[i], q.vertices[j] = q.vertices[j], q.vertices[i]
	q.position[q.vertices[i]], q.position[q.vertices[j]] = i, j
}

func (q *degreeQueue) Push(v any) {
	q.position[v.(int)] = len(q.vertices)
	q.vertices = append(q.vertices, v.(int))
}

func (q *degreeQueue) Pop() any {
	i := len(q.vertices) - 1
	v := q.vertices[i]
	q.vertices = q.vertices[:i]
	q.position[v] = -1

	return v
}
