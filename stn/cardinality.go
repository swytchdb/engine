package stn

// cardinality visits vertices by the number of already visited neighbors.
// Each bucket is a doubly linked list, so moving a vertex costs O(1) and a
// complete search costs O(V+E). A negative count marks a removed vertex.
type cardinality struct {
	first    []int // bucket heads, indexed by neighbor count
	vertices []cardinalityVertex
	max      int
}

type cardinalityVertex struct{ next, prev, count int }

func newCardinality(n int) cardinality {
	q := cardinality{first: make([]int, n), vertices: make([]cardinalityVertex, n)}
	for v := range n {
		q.first[v] = -1
		q.vertices[v] = cardinalityVertex{next: v - 1, prev: v + 1}
	}

	if n > 0 {
		q.first[0] = n - 1
		q.vertices[n-1].prev = -1
	}

	return q
}

func (q *cardinality) contains(v int) bool { return q.vertices[v].count >= 0 }

func (q *cardinality) remove(v int) {
	entry := &q.vertices[v]
	if entry.prev < 0 {
		q.first[entry.count] = entry.next
	} else {
		q.vertices[entry.prev].next = entry.next
	}

	if entry.next >= 0 {
		q.vertices[entry.next].prev = entry.prev
	}

	entry.count = -1
}

func (q *cardinality) increment(v int) {
	count := q.vertices[v].count + 1
	q.remove(v)

	next := q.first[count]
	q.vertices[v] = cardinalityVertex{next: next, prev: -1, count: count}
	q.first[count] = v

	if next >= 0 {
		q.vertices[next].prev = v
	}

	q.max = max(q.max, count)
}

func (q *cardinality) maximum() int {
	for q.max >= 0 && q.first[q.max] < 0 {
		q.max--
	}

	if q.max < 0 {
		return -1
	}

	return q.first[q.max]
}
