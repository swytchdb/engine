package effects

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"hash/maphash"
	"log/slog"
	"slices"
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/swytchdb/engine/keytrie"
)

// closureLock must be acquired before allocating traversal state. Keep at
// most one shard held, including for multi-key binds. When nodeMu is needed,
// it is acquired first; closure must never acquire it or reenter ingestion.
func (e *Engine) closureLock(key string) *sync.Mutex {
	return &e.closureLocks[xxhash.Sum64String(key)%uint64(len(e.closureLocks))]
}

// installIngestedTips reconciles the incoming and existing causal regions.
// Closure discovers graph relationships, not values: snapshots have the same
// dependency semantics as any other vertex here.
func (e *Engine) installIngestedTips(key string, arrivals ...Tip) {
	mu := e.closureLock(key)
	mu.Lock()
	defer mu.Unlock()

	// Payloads remain in the vertex pool. Only fetch outcomes survive a failed
	// publication CAS, so retries do not repeat unavailable boundary requests.
	attempted := make(map[Tip]error)

	for {
		current := e.index.Contains(key)
		tips := keytrie.NewTipSet(slices.Concat(current.Tips(), arrivals)...).Tips()
		live, complete, err := e.closeIngestedFrontierLocked(key, tips, attempted)
		next := keytrie.NewTipSet(live...)

		if current.Len() == next.Len() && current.ContainsAll(live) {
			if e.index.Contains(key) != current {
				continue
			}
		} else if _, ok := e.index.Insert(key, current, next); !ok {
			if e.index.Closed() {
				return
			}

			continue
		} else {
			e.notifyKeyChanged(key)
		}

		if complete && next.Len() == 1 && e.effectCache != nil {
			tip := live[0]

			eff, _ := e.effectCache.Get(tip)
			e.recordHistoryProcessed(tip, eff)
		}

		if err != nil {
			slog.Warn("frontier closure incomplete; retaining unresolved tips", "key", key, "error", err)
		}

		return
	}
}

func (e *Engine) closeIngestedFrontier(key string, supplied []Tip, attempted map[Tip]error) ([]Tip, bool, error) {
	mu := e.closureLock(key)
	mu.Lock()
	defer mu.Unlock()

	return e.closeIngestedFrontierLocked(key, supplied, attempted)
}

// closureVisit keeps the per-invocation state of an identity in one place.
// Indices, rather than pointers into visits, survive table and slice growth.
// Cached vertices are only navigation hints: every reuse checks reclamation.
type closureVisit struct {
	tip    Tip
	vertex *vertex
	owner  *closureCut
	flags  uint8
}

const (
	closureScheduled uint8 = 1 << iota
	closureExpanded
	closureNoFetch
	closureLive
)

// Cuts express connected membership, never dominance or downward expansion.
type closureCut struct {
	parent *closureCut
	first  Tip
	ranges map[uint64][2]uint64
	size   int
}

// The table stores index+1, with zero denoting an empty slot. Keeping identity
// lookup separate from the dense state array makes queue processing hash-free.
// A randomized hash avoids depending on origin/offset distributions.
type closureWork struct {
	seed   maphash.Seed
	slots  []int
	visits []closureVisit
	queue  []int
}

var closureWorkPool = sync.Pool{New: func() any {
	return &closureWork{seed: maphash.MakeSeed(), slots: make([]int, 128)}
}}

func (w *closureWork) grow() {
	w.slots = make([]int, len(w.slots)*2)
	mask := uint64(len(w.slots) - 1)
	for i := range w.visits {
		slot := maphash.Comparable(w.seed, w.visits[i].tip) & mask
		for w.slots[slot] != 0 {
			slot = (slot + 1) & mask
		}
		w.slots[slot] = i + 1
	}
}

func (w *closureWork) intern(tip Tip) int {
	if (len(w.visits)+1)*4 >= len(w.slots)*3 {
		w.grow()
	}
	mask := uint64(len(w.slots) - 1)
	slot := maphash.Comparable(w.seed, tip) & mask
	for {
		entry := w.slots[slot]
		if entry == 0 {
			i := len(w.visits)
			w.visits = append(w.visits, closureVisit{tip: tip})
			w.slots[slot] = i + 1
			return i
		}
		if w.visits[entry-1].tip == tip {
			return entry - 1
		}
		slot = (slot + 1) & mask
	}
}

func (w *closureWork) release() {
	// No graph, key, cut, or error references survive the invocation. Large
	// exceptional walks are discarded rather than retaining their peak size.
	if cap(w.visits) > 32768 || cap(w.queue) > 65536 {
		return
	}
	clear(w.visits)
	clear(w.slots)
	w.visits = w.visits[:0]
	w.queue = w.queue[:0]
	closureWorkPool.Put(w)
}

// closeIngestedFrontierLocked requires the key's closure shard. Ingestion
// retains it across frontier publication and CAS retries; recovery uses the
// wrapper above and releases it before reconstruction.
func (e *Engine) closeIngestedFrontierLocked(key string, supplied []Tip, attempted map[Tip]error) ([]Tip, bool, error) {
	w := closureWorkPool.Get().(*closureWork)
	defer w.release()

	// Missing boundaries are uncommon and retain their downward parent IDs
	// only until the payload is available. They never own vertex lifetimes.
	pending := make(map[int][]Tip)
	resident := func(i int) *vertex {
		v := w.visits[i].vertex
		if v != nil && v.refs.Load() >= 0 {
			return v
		}
		if e.effectCache == nil {
			return nil
		}
		v = e.effectCache.getVertex(w.visits[i].tip)
		w.visits[i].vertex = v
		return v
	}
	// Comparison is rare (seeds, cut joins, and fetch scheduling), and uses
	// current pool residency just as publication-order selection always has.
	compareVertex := func(tip Tip) *vertex {
		if e.effectCache == nil {
			return nil
		}
		return e.effectCache.getVertex(tip)
	}
	compare := func(a, b Tip) int {
		var ah, bh []byte
		if v := compareVertex(a); v != nil {
			ah = v.eff.GetForkChoiceHash()
		}

		if v := compareVertex(b); v != nil {
			bh = v.eff.GetForkChoiceHash()
		}

		if order := bytes.Compare(ah, bh); order != 0 {
			return order
		}

		if order := cmp.Compare(a[0], b[0]); order != 0 {
			return order
		}

		return cmp.Compare(a[1], b[1])
	}
	result := func() []Tip {
		var tips []Tip
		for i := range w.visits {
			if w.visits[i].flags&closureLive != 0 {
				tips = append(tips, w.visits[i].tip)
			}
		}

		slices.SortFunc(tips, compare)

		return tips
	}
	root := func(c *closureCut) *closureCut {
		for c.parent != nil {
			if c.parent.parent != nil {
				c.parent = c.parent.parent
			}

			c = c.parent
		}

		return c
	}
	observe := func(c *closureCut, tip Tip) {
		r, ok := c.ranges[tip[0]]
		if !ok {
			r = [2]uint64{tip[1], tip[1]}
		} else {
			r[0], r[1] = min(r[0], tip[1]), max(r[1], tip[1])
		}

		c.ranges[tip[0]] = r
	}
	join := func(a, b *closureCut) {
		a, b = root(a), root(b)
		if a == b {
			return
		}

		if a.size < b.size {
			a, b = b, a
		}

		b.parent = a

		a.size += b.size
		if compare(b.first, a.first) < 0 {
			a.first = b.first
		}

		for node, r := range b.ranges {
			observe(a, Tip{node, r[0]})
			observe(a, Tip{node, r[1]})
		}
	}
	claim := func(i int, c *closureCut) {
		c = root(c)
		w.visits[i].owner = c
		c.size++
		observe(c, w.visits[i].tip)
	}
	enqueue := func(i int) {
		if w.visits[i].flags&closureScheduled == 0 {
			w.visits[i].flags |= closureScheduled
			w.queue = append(w.queue, i)
		}
	}

	// 1. Supplied tips seed downward discovery in fork-choice order. They do
	// not initiate upward searches, including when an input is already local.
	seeds := keytrie.NewTipSet(supplied...).Tips()
	slices.SortFunc(seeds, compare)

	for _, tip := range seeds {
		c := &closureCut{first: tip, ranges: make(map[uint64][2]uint64)}
		i := w.intern(tip)
		claim(i, c)
		w.visits[i].flags |= closureLive

		enqueue(i)
	}

	// 2. A preexisting reverse link identifies established history. Follow
	// one chain to its cut BEFORE installing the newly discovered incoming
	// edge. Record membership along the chain so another join stops there.
	// This never consumes tips and never marks a vertex expanded downward.
	ascend := func(v *vertex, c *closureCut) {
		for {
			parent, ok := v.firstParent(key)
			if !ok {
				return // the downward edge is already consuming this dependency
			}

			i := w.intern(parent)
			if known := w.visits[i].owner; known != nil {
				join(c, known)

				return
			}

			claim(i, c)

			v = resident(i)
			if v == nil {
				// The retained link proves a join to a reclaimed cut. The
				// missing parent is neither a new tip nor a fetch obligation.
				return
			}

			if _, hasParent := v.firstParent(key); !hasParent {
				w.visits[i].flags |= closureLive

				c = root(c)
				if compare(parent, c.first) < 0 {
					c.first = parent
				}

				enqueue(i)

				return
			}
		}
	}

	var loadErr error

	for {
		// 3. Drain resident downward work before requesting any boundary.
		for next := 0; next < len(w.queue); next++ {
			i := w.queue[next]
			tip := w.visits[i].tip
			if w.visits[i].flags&closureExpanded != 0 {
				continue
			}

			v := resident(i)
			if v == nil {
				if w.visits[i].flags&closureNoFetch != 0 {
					delete(pending, i)

					continue
				}

				if _, exists := pending[i]; !exists {
					pending[i] = nil
				}

				continue
			}

			if parents := pending[i]; len(parents) != 0 {
				// A fetched dependency may now connect to established local
				// history. Its incoming edges must not mask those older links.
				ascend(v, w.visits[i].owner)

				for _, parent := range parents {
					e.effectCache.addParent(v, parent, key)
				}
			}

			delete(pending, i)

			w.visits[i].flags |= closureExpanded

			if v.root() {
				w.visits[i].flags |= closureNoFetch
			}

			for ref := range keyDeps(key, v.eff) {
				dep := r(ref)
				if dep == (Tip{}) || dep == tip {
					continue
				}
				j := w.intern(dep)
				// Only this genuine downward edge establishes consumption.
				w.visits[j].flags &^= closureLive

				if w.visits[i].flags&closureNoFetch != 0 && w.visits[j].flags&closureNoFetch == 0 {
					w.visits[j].flags |= closureNoFetch
					// Newly discovered boundary evidence still propagates
					// through ancestry that another path expanded earlier.
					if w.visits[j].flags&closureExpanded != 0 {
						w.visits[j].flags &^= closureExpanded
						w.queue = append(w.queue, j)
					}
				}

				dv := resident(j)
				if known := w.visits[j].owner; known != nil {
					join(w.visits[i].owner, known)
				} else {
					claim(j, w.visits[i].owner)
					if dv != nil {
						ascend(dv, w.visits[j].owner)
					}
				}

				if dv != nil {
					e.effectCache.addParent(dv, tip, key)
				} else if w.visits[j].flags&closureExpanded == 0 {
					pending[j] = append(pending[j], tip)
				}

				enqueue(j)
			}
		}

		w.queue = w.queue[:0]

		if len(pending) == 0 {
			return result(), loadErr == nil, loadErr
		}

		// 4. Resolve each boundary once. Exhaustion and failed availability
		// are different outcomes even though neither produces runnable work.
		var roots []int

		for i := range pending {
			tip := w.visits[i].tip
			if resident(i) != nil {
				w.queue = append(w.queue, i)

				continue
			}

			if w.visits[i].flags&closureNoFetch != 0 {
				delete(pending, i)

				continue
			}

			if err, tried := attempted[tip]; tried {
				if err == nil {
					// A successful load lost during a publication retry is
					// not a definitive response from the history holders.
					err = fmt.Errorf("effect %v reclaimed during frontier closure", tip)
				}

				if !errors.Is(err, ErrEffectNotFound) && !errors.Is(err, ErrCDNBlobMissing) {
					loadErr = errors.Join(loadErr, err)
				}

				delete(pending, i)
			} else {
				roots = append(roots, i)
			}
		}

		if len(w.queue) != 0 {
			continue
		}

		if len(roots) == 0 {
			return result(), loadErr == nil, loadErr
		}

		// 5. Counter ranges prioritize cuts; they never establish dominance.
		// Pairwise range preferences need not be transitive, so use a stable
		// scan rather than passing them to a sorting comparator.
		slices.SortFunc(roots, func(a, b int) int { return compare(w.visits[a].tip, w.visits[b].tip) })

		best := roots[0]
		for _, candidate := range roots[1:] {
			a, b := root(w.visits[candidate].owner), root(w.visits[best].owner)
			newer, older := false, false

			for node, ar := range a.ranges {
				if br, ok := b.ranges[node]; ok {
					newer = newer || ar[0] > br[1]
					older = older || ar[1] < br[0]
				}
			}

			if newer && !older || newer == older && compare(a.first, b.first) < 0 {
				best = candidate
			}
		}

		missing, err := e.fetchFrontierAncestry(key, w.visits[best].tip)
		for _, tip := range missing {
			attempted[tip] = ErrEffectNotFound
		}

		if err == nil && resident(best) == nil {
			err = fmt.Errorf("effect %v reclaimed during frontier closure", w.visits[best].tip)
		}

		attempted[w.visits[best].tip] = err
		if err == nil {
			w.queue = append(w.queue, best)
		}
	}
}
