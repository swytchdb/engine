// Package stn implements incremental partial path consistency (IPPC),
// Algorithm 1 and Tag in Planken, de Weerdt and Yorke-Smith, ICAPS 2010:
// https://doi.org/10.1609/icaps.v20i1.13417.
// An edge u -> v of weight w means time(v)-time(u) <= w.
package stn

import (
	"errors"
	"math"
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

	add := func(u, v int) {
		if _, ok := s.adj[u][v]; ok {
			return
		}

		uv, _ := s.Upper(u, v)
		vu, _ := s.Upper(v, u)
		s.adj[u][v], s.adj[v][u] = uv, vu
	}

	if _, ok := s.adj[a][b]; ok {
		return nil
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
	if plus(upper, s.adj[to][from]) < 0 {
		return ErrInconsistent
	}

	if upper >= old {
		return nil
	}

	s.maxInput = max(s.maxInput, upper, -upper)
	n := len(s.adj)
	toA, fromB := make([]int64, n), make([]int64, n)
	tagged := make([]bool, n)

	for i := range toA {
		toA[i], fromB[i] = Infinity, Infinity
	}

	toA[from], fromB[to] = 0, 0
	q := newCardinality(n)
	tag := func(v int) {
		tagged[v] = true
		q.remove(v)

		for u, vu := range s.adj[v] {
			uv := s.adj[u][v]
			if !tagged[u] {
				toA[u] = min(toA[u], plus(uv, toA[v]))
				fromB[u] = min(fromB[u], plus(fromB[v], vu))
				q.increment(u)
			} else {
				s.adj[u][v] = min(uv, plus(plus(toA[u], upper), fromB[v]))
				s.adj[v][u] = min(vu, plus(plus(toA[v], upper), fromB[u]))
			}
		}
	}
	tag(from)
	tag(to)

	for v := q.maximum(); v >= 0; v = q.maximum() {
		tag(v)
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

	forward, backward := make([]int64, len(s.adj)), make([]int64, len(s.adj))
	for i := range forward {
		forward[i], backward[i] = Infinity, Infinity
	}

	forward[from], backward[to] = 0, 0

	bound := Infinity
	for _, v := range s.order {
		bound = min(bound, pathSum(forward[v], backward[v]))
		if forward[v] == Infinity && backward[v] == Infinity {
			continue
		}

		for u, vu := range s.adj[v] {
			if s.rank[u] <= s.rank[v] {
				continue
			}

			forward[u] = min(forward[u], pathSum(forward[v], vu))
			backward[u] = min(backward[u], pathSum(backward[v], s.adj[u][v]))
		}
	}

	return bound, nil
}

// A nonoptimal upward path may sum many already-derived edge bounds. Ignore
// positive overflow rather than wrapping it into a false negative bound. This
// cannot discard the optimum: some shortest path is upward/downward, every
// prefix/suffix of that path is shortest, and every finite shortest-path bound
// is within V*maxInput (< 1<<60). Consistency likewise prevents negative
// underflow on any path. IPPC's separate three-term arithmetic is unchanged.
func pathSum(a, b int64) int64 {
	if a == Infinity || b == Infinity || b > 0 && a > math.MaxInt64-b {
		return Infinity
	}

	return a + b
}

func plus(a, b int64) int64 {
	if a == Infinity || b == Infinity {
		return Infinity
	}

	return a + b
}

// Intrusive buckets implement maximum-cardinality search in O(V+E), avoiding
// an O(V) scan to choose each next vertex in Algorithm 1.
type cardinality struct {
	first, next, prev, count []int
	max                      int
}

func newCardinality(n int) *cardinality {
	q := &cardinality{first: make([]int, n), next: make([]int, n), prev: make([]int, n), count: make([]int, n)}
	for i := range n {
		q.first[i], q.next[i], q.prev[i] = -1, -1, -1
	}

	for i := range n {
		q.insert(i)
	}

	return q
}

func (q *cardinality) insert(v int) {
	c := q.count[v]
	u := q.first[c]

	q.first[c], q.next[v], q.prev[v] = v, u, -1
	if u >= 0 {
		q.prev[u] = v
	}

	q.max = max(q.max, c)
}

func (q *cardinality) remove(v int) {
	u, w := q.prev[v], q.next[v]
	if u < 0 {
		q.first[q.count[v]] = w
	} else {
		q.next[u] = w
	}

	if w >= 0 {
		q.prev[w] = u
	}
}

func (q *cardinality) increment(v int) {
	q.remove(v)
	q.count[v]++
	q.insert(v)
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
