package effects

import (
	"slices"
	"strings"

	"github.com/cespare/xxhash/v2"
	pb "github.com/swytchdb/engine/cluster/proto"
)

// A proof owns no history. Reclamation and FLUSH invalidate it; cache eviction
// merely restores the ordinary closure walk. One fixed-size proof per key keeps
// a hot key from retaining an ever-growing traversal table.
// CloxCache hashes key representations, so use a numeric content hash and check
// the retained address on lookup; a hash collision must behave as a cache miss.
type closedFrontier struct {
	key        string
	tip        Tip
	generation uint64
	reclaimed  uint64
}

func (op *closureOperation) cacheCurrent() bool {
	return op.engine.effectCache != nil &&
		op.generation == op.engine.flushGeneration.Load() &&
		op.reclaimed == op.engine.effectCache.reclaimed.Load()
}

func (op *closureOperation) rememberClosedFrontier(key string, live []Tip) {
	if op.engine.closedFrontiers == nil || op.subscriptionOnly || len(live) != 1 || !op.cacheCurrent() {
		return
	}

	// An unavailable boundary may be acceptable to the existing walk, but it
	// is not a reusable claim that all of the history has been established.
	for _, err := range op.attempted {
		if err != nil {
			return
		}
	}

	v := op.engine.effectCache.getVertex(live[0])
	if v == nil {
		return
	}

	if _, hasParent := v.firstParent(key); hasParent {
		return
	}

	op.engine.closedFrontiers.Put(xxhash.Sum64String(key), closedFrontier{
		key: strings.Clone(key),
		tip: live[0], generation: op.generation, reclaimed: op.reclaimed,
	})
}

// extendClosedFrontier accepts only a resident suffix whose EVERY dependency
// path reaches the established frontier. Sharing an ancestor with that frontier
// is insufficient: a sibling or delayed ancestor falls back to normal discovery.
func (op *closureOperation) extendClosedFrontier(key string, supplied []Tip, visitBind func(Tip, *pb.Effect)) ([]Tip, bool) {
	e := op.engine
	if e.closedFrontiers == nil || op.cacheDisabled || !op.cacheCurrent() {
		return nil, false
	}

	w := op.work[key]
	if w != nil {
		if len(w.visits) != 0 {
			return nil, false
		}

		if tipsEqual(w.cachedInput, supplied) && w.cachedOutput != nil {
			return w.cachedOutput, true
		}
	}

	proof, ok := e.closedFrontiers.Get(xxhash.Sum64String(key), 0)
	if !ok || proof.key != key || proof.generation != op.generation || proof.reclaimed != op.reclaimed {
		return nil, false
	}

	boundary := e.effectCache.getVertex(proof.tip)
	if boundary == nil {
		return nil, false
	}

	// A new reverse link can expose a successor outside the supplied roots.
	// Let ordinary navigation discover it before establishing another proof.
	if _, hasParent := boundary.firstParent(key); hasParent {
		return nil, false
	}

	// Bound speculative work for arrivals that ultimately need full discovery.
	const maxSuffix = 256

	states := make(map[Tip]uint8)
	consumed := make(map[Tip]bool)

	var order []Tip

	records := make(map[Tip]*pb.Effect)

	var closed func(Tip) bool

	closed = func(tip Tip) bool {
		if tip == proof.tip || states[tip] == 2 {
			return true
		}

		if states[tip] == 1 || len(states) >= maxSuffix {
			return false
		}

		v := e.effectCache.getVertex(tip)
		if v == nil {
			return false
		}

		states[tip] = 1
		records[tip] = v.eff
		dependencies := 0

		for ref := range keyDeps(key, v.eff) {
			dep := r(ref)
			if dep == (Tip{}) || dep == tip {
				continue
			}

			dependencies++

			consumed[dep] = true
			if !closed(dep) {
				return false
			}
		}

		if dependencies == 0 {
			return false
		}

		states[tip] = 2
		order = append(order, tip)

		return true
	}

	for _, tip := range supplied {
		if !closed(tip) {
			return nil, false
		}
	}

	// Ordinary discovery also follows reverse hints on dependencies. A suffix
	// must not hide an established successor outside these supplied paths.
	for _, tip := range order {
		if !consumed[tip] {
			continue
		}

		v := e.effectCache.getVertex(tip)
		if v == nil {
			return nil, false
		}

		if parent, ok := v.firstParent(key); ok && states[parent] != 2 {
			return nil, false
		}
	}

	if !op.cacheCurrent() {
		return nil, false
	}

	// Only now publish discovery to the operation. A failed speculative walk
	// must not hide work or alter the full walk's cross-key obligations.
	for _, tip := range order {
		eff := records[tip]
		op.harvestVerdicts(tip, eff)

		if eff.GetTxnBind() != nil {
			visitBind(tip, eff)
		}

		if op.observe != nil {
			op.observe(tip, nil)
		}

		for ref := range keyDeps(key, eff) {
			if op.observe != nil {
				op.observe(tip, ref)
			}

			if v := e.effectCache.getVertex(r(ref)); v != nil && !op.subscriptionOnly {
				e.effectCache.addParent(v, tip, key)
			}
		}
	}

	var live []Tip

	for _, tip := range supplied {
		if !consumed[tip] {
			live = append(live, tip)
		}
	}

	live = compactTips(live)

	if w == nil {
		w = closureWorkPool.Get().(*closureWork)
		op.work[key] = w
	}

	w.cachedInput, w.cachedOutput = slices.Clone(supplied), live
	op.cacheUsed = true

	return live, true
}
