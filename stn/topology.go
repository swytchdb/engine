package stn

import "container/heap"

// chordalOrder recognizes an existing chordal graph with maximum cardinality
// search. Its reverse selection order is a PEO iff every vertex's higher-rank
// neighbors form a clique. This preserves stars, trees, and all other chordal
// inputs without adding fill, regardless of vertex numbering.
func (s *Network) chordalOrder() []int {
	n := len(s.adj)
	q := newCardinality(n)
	order, rank := make([]int, n), make([]int, n)

	selected := make([]bool, n)
	for i := n - 1; i >= 0; i-- {
		v := q.maximum()
		q.remove(v)

		order[i], rank[v], selected[v] = v, i, true
		for u := range s.adj[v] {
			if !selected[u] {
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
					return nil
				}
			}
		}
	}

	return order
}

// triangulate only adds fill when recognition fails. Minimum degree is a
// heuristic, not an optimal-treewidth claim. add initializes the new edge's
// weights; Connect supplies existing shortest-path bounds, New uses infinity.
func (s *Network) triangulate(add func(int, int)) {
	order := s.chordalOrder()
	if order == nil {
		n := len(s.adj)

		q := &degreeQueue{vertices: make([]int, n), position: make([]int, n), degree: make([]int, n)}
		for v := range s.adj {
			q.vertices[v], q.position[v], q.degree[v] = v, v, len(s.adj[v])
		}

		heap.Init(q)

		order = make([]int, 0, n)

		for q.Len() != 0 {
			v := heap.Pop(q).(int)
			order = append(order, v)

			neighbors := make([]int, 0, q.degree[v])
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
