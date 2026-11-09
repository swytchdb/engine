package effects

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
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

// closeIngestedFrontierLocked requires the key's closure shard. Ingestion
// retains it across frontier publication and CAS retries; recovery uses the
// wrapper above and releases it before reconstruction.
func (e *Engine) closeIngestedFrontierLocked(key string, supplied []Tip, attempted map[Tip]error) ([]Tip, bool, error) {
	// These are connected cuts of the discovered graph, not a second graph.
	// Membership never establishes dominance or implies downward expansion.
	type cut struct {
		parent *cut
		first  Tip
		ranges map[uint64][2]uint64
		size   int
	}

	owners := make(map[Tip]*cut)
	live := make(map[Tip]struct{})
	expanded := make(map[Tip]bool) // absent: unscheduled; false: queued; true: expanded
	pending := make(map[Tip][]Tip) // missing boundary -> downward parents awaiting its payload
	noFetch := make(map[Tip]bool)  // ancestry below a reclamation boundary

	var queue []Tip

	resident := func(tip Tip) *vertex {
		if e.effectCache == nil {
			return nil
		}

		return e.effectCache.getVertex(tip)
	}
	compare := func(a, b Tip) int {
		var ah, bh []byte
		if v := resident(a); v != nil {
			ah = v.eff.GetForkChoiceHash()
		}

		if v := resident(b); v != nil {
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
		tips := make([]Tip, 0, len(live))
		for tip := range live {
			tips = append(tips, tip)
		}

		slices.SortFunc(tips, compare)

		return tips
	}
	root := func(c *cut) *cut {
		for c.parent != nil {
			if c.parent.parent != nil {
				c.parent = c.parent.parent
			}

			c = c.parent
		}

		return c
	}
	observe := func(c *cut, tip Tip) {
		r, ok := c.ranges[tip[0]]
		if !ok {
			r = [2]uint64{tip[1], tip[1]}
		} else {
			r[0], r[1] = min(r[0], tip[1]), max(r[1], tip[1])
		}

		c.ranges[tip[0]] = r
	}
	join := func(a, b *cut) {
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
	claim := func(tip Tip, c *cut) {
		c = root(c)
		owners[tip] = c
		c.size++
		observe(c, tip)
	}
	enqueue := func(tip Tip) {
		if _, scheduled := expanded[tip]; !scheduled {
			expanded[tip] = false
			queue = append(queue, tip)
		}
	}

	// 1. Supplied tips seed downward discovery in fork-choice order. They do
	// not initiate upward searches, including when an input is already local.
	seeds := keytrie.NewTipSet(supplied...).Tips()
	slices.SortFunc(seeds, compare)

	for _, tip := range seeds {
		c := &cut{first: tip, ranges: make(map[uint64][2]uint64)}
		claim(tip, c)
		live[tip] = struct{}{}

		enqueue(tip)
	}

	// 2. A preexisting reverse link identifies established history. Follow
	// one chain to its cut BEFORE installing the newly discovered incoming
	// edge. Record membership along the chain so another join stops there.
	// This never consumes tips and never marks a vertex expanded downward.
	ascend := func(v *vertex, c *cut) {
		for {
			parent, ok := v.firstParent(key)
			if !ok {
				return // the downward edge is already consuming this dependency
			}

			if known := owners[parent]; known != nil {
				join(c, known)

				return
			}

			claim(parent, c)

			v = resident(parent)
			if v == nil {
				// The retained link proves a join to a reclaimed cut. The
				// missing parent is neither a new tip nor a fetch obligation.
				return
			}

			if _, hasParent := v.firstParent(key); !hasParent {
				live[parent] = struct{}{}

				c = root(c)
				if compare(parent, c.first) < 0 {
					c.first = parent
				}

				enqueue(parent)

				return
			}
		}
	}

	var loadErr error

	for {
		// 3. Drain resident downward work before requesting any boundary.
		for i := 0; i < len(queue); i++ {
			tip := queue[i]
			if expanded[tip] {
				continue
			}

			v := resident(tip)
			if v == nil {
				if noFetch[tip] {
					delete(pending, tip)

					continue
				}

				if _, exists := pending[tip]; !exists {
					pending[tip] = nil
				}

				continue
			}

			if parents := pending[tip]; len(parents) != 0 {
				// A fetched dependency may now connect to established local
				// history. Its incoming edges must not mask those older links.
				ascend(v, owners[tip])

				for _, parent := range parents {
					e.effectCache.addParent(v, parent, key)
				}
			}

			delete(pending, tip)

			expanded[tip] = true

			if v.root() {
				noFetch[tip] = true
			}

			for ref := range keyDeps(key, v.eff) {
				dep := r(ref)
				if dep == (Tip{}) || dep == tip {
					continue
				}
				// Only this genuine downward edge establishes consumption.
				delete(live, dep)

				if noFetch[tip] && !noFetch[dep] {
					noFetch[dep] = true
					// Another path may have expanded this vertex first. Pass
					// the newly discovered fetch boundary through its resident
					// ancestry too; each vertex can gain this evidence once.
					if expanded[dep] {
						expanded[dep] = false
						queue = append(queue, dep)
					}
				}

				dv := resident(dep)
				if known := owners[dep]; known != nil {
					join(owners[tip], known)
				} else {
					claim(dep, owners[tip])

					if dv != nil {
						ascend(dv, owners[dep])
					}
				}

				if dv != nil {
					e.effectCache.addParent(dv, tip, key)
				} else if !expanded[dep] {
					pending[dep] = append(pending[dep], tip)
				}

				enqueue(dep)
			}
		}

		queue = queue[:0]

		if len(pending) == 0 {
			return result(), loadErr == nil, loadErr
		}

		// 4. Resolve each boundary once. Exhaustion and failed availability
		// are different outcomes even though neither produces runnable work.
		var roots []Tip

		for tip := range pending {
			if resident(tip) != nil {
				queue = append(queue, tip)

				continue
			}

			if noFetch[tip] {
				delete(pending, tip)

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

				delete(pending, tip)
			} else {
				roots = append(roots, tip)
			}
		}

		if len(queue) != 0 {
			continue
		}

		if len(roots) == 0 {
			return result(), loadErr == nil, loadErr
		}

		// 5. Counter ranges prioritize cuts; they never establish dominance.
		// Pairwise range preferences need not be transitive, so use a stable
		// scan rather than passing them to a sorting comparator.
		slices.SortFunc(roots, compare)

		best := roots[0]
		for _, candidate := range roots[1:] {
			a, b := root(owners[candidate]), root(owners[best])
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

		missing, err := e.fetchFrontierAncestry(key, best)
		for _, tip := range missing {
			attempted[tip] = ErrEffectNotFound
		}

		if err == nil && resident(best) == nil {
			err = fmt.Errorf("effect %v reclaimed during frontier closure", best)
		}

		attempted[best] = err
		if err == nil {
			queue = append(queue, best)
		}
	}
}
