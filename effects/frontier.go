package effects

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"hash/maphash"
	"log/slog"
	"maps"
	"slices"
	"sync"

	"github.com/cespare/xxhash/v2"
	pb "github.com/swytchdb/engine/cluster/proto"
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
	op := newClosureOperation(e, make(map[Tip]error))
	defer op.release()

	// A membership announcement is not a new transaction admission. Recover
	// its key's causal dependencies, but do not reopen every other key of an
	// observed bind. Bind receipt and read/bootstrap recovery own that work.
	op.subscriptionOnly = len(arrivals) != 0
	for _, tip := range arrivals {
		if e.effectCache == nil {
			op.subscriptionOnly = false

			break
		}

		eff, ok := e.effectCache.Get(tip)
		if !ok || eff.GetSubscription() == nil {
			op.subscriptionOnly = false

			break
		}
	}

	_, _, err := op.publish(key, arrivals, false)
	if err != nil {
		slog.Warn("frontier closure incomplete; retaining unresolved tips", "key", key, "error", err)
	}
}

// ingestedFrontiers collects publication obligations across a recovery batch.
// A bind can be replayed through several keys; publish each projection once,
// after all available history has been ingested.
type ingestedFrontiers map[string]map[Tip]struct{}

func (frontiers ingestedFrontiers) add(key string, arrivals ...Tip) {
	if len(arrivals) == 0 {
		return
	}

	if frontiers[key] == nil {
		frontiers[key] = make(map[Tip]struct{}, len(arrivals))
	}

	for _, tip := range arrivals {
		frontiers[key][tip] = struct{}{}
	}
}

func (e *Engine) installIngestedFrontiers(frontiers ingestedFrontiers, verdicts verdictMap) {
	op := newClosureOperation(e, make(map[Tip]error))
	defer op.release()

	op.verdicts = make(verdictMap, len(verdicts))
	maps.Copy(op.verdicts, verdicts)

	op.publishBatch(frontiers)
}

// Bind responses must not report acceptance after a closure fetch failed.
// Missing older history retains the existing reconstruction boundary rules.
func (e *Engine) installBindFrontiers(frontiers ingestedFrontiers) error {
	op := newClosureOperation(e, make(map[Tip]error))
	defer op.release()

	op.recordProgress = true

	return op.publishBatchChecked(frontiers, true)
}

func (op *closureOperation) publishBatch(frontiers ingestedFrontiers) {
	_ = op.publishBatchChecked(frontiers, false)
}

func (op *closureOperation) publishBatchChecked(frontiers ingestedFrontiers, failOnError bool) error {
	keys := make([]string, 0, len(frontiers))
	for key := range frontiers {
		keys = append(keys, key)
	}

	slices.Sort(keys)

	for _, key := range keys {
		arrivals := make([]Tip, 0, len(frontiers[key]))
		for tip := range frontiers[key] {
			arrivals = append(arrivals, tip)
		}

		// Associated-key discovery from earlier publications remains in op.
		// publish still reloads the current frontier and holds only one shard.
		if _, _, err := op.publish(key, arrivals, failOnError); err != nil {
			if failOnError {
				return err
			}

			slog.Warn("frontier closure incomplete; retaining unresolved tips", "key", key, "error", err)
		}
	}

	return nil
}

// closureOperation owns discovery through publication. Cloud and notification
// ingestion share the publisher; recovery uses the same discovery without
// publishing. Optional observation and an explicit CAS let regressions count
// actual graph work and force contention without timing-dependent goroutines.
type closureOperation struct {
	generation       uint64
	reclaimed        uint64
	cacheUsed        bool
	cacheDisabled    bool
	engine           *Engine
	recordProgress   bool // strict bind publication still records successful receipt; cloud does not
	subscriptionOnly bool // membership publication does not adjudicate cross-key transactions
	attempted        map[Tip]error
	work             map[string]*closureWork
	binds            map[Tip]map[string]*pb.EffectRef // immutable per-key projections, enumerated once per operation
	verdicts         verdictMap                       // adjudicated competitions discovered on this operation's paths
	observe          func(Tip, *pb.EffectRef)         // nil ref: vertex expansion; otherwise: dependency edge
	observeBind      func(Tip)                        // bind-wide enumeration, independent of per-key expansion
	cas              func(string, *keytrie.TipSet, *keytrie.TipSet) (*keytrie.TipSet, bool)
}

func newClosureOperation(e *Engine, attempted map[Tip]error) *closureOperation {
	op := &closureOperation{
		generation: e.flushGeneration.Load(),
		engine:     e, attempted: attempted, work: make(map[string]*closureWork),
		cas: e.index.InsertInternal,
	}
	if e.effectCache != nil {
		op.reclaimed = e.effectCache.reclaimed.Load()
	}

	return op
}

// A loss cannot enter another competition even if the winning transaction is
// still waiting on subscribers. Invalid or unavailable evidence grants no shortcut.
func (op *closureOperation) harvestVerdicts(tip Tip, eff *pb.Effect) {
	snap := eff.GetSnapshot()
	if len(snap.GetTxnVerdicts()) == 0 {
		return
	}

	if snap.GetOutstandingSubscribers() == nil || len(snap.GetOutstandingSubscribers().GetNodeIds()) != 0 {
		lost := false
		for _, outcome := range snap.GetTxnVerdicts() {
			lost = lost || outcome == pb.Verdict_LOST
		}

		if !lost {
			return
		}
	}

	incoming := make(verdictMap)
	if err := op.engine.harvestProofVerdicts(tip, eff, incoming); err != nil {
		return
	}

	for txn, entry := range incoming {
		if incoming.adjudicated(txn) {
			if op.verdicts == nil {
				op.verdicts = make(verdictMap)
			}

			op.verdicts.retain(txn, entry)
		}
	}
}

func (op *closureOperation) release() {
	for _, w := range op.work {
		w.release()
	}

	clear(op.work)
	clear(op.binds)
	clear(op.verdicts)
}

func (op *closureOperation) publish(key string, arrivals []Tip, failOnError bool) ([]Tip, bool, error) {
	return op.run(key, arrivals, true, failOnError)
}

func (e *Engine) closeIngestedFrontier(key string, supplied []Tip, attempted map[Tip]error) ([]Tip, bool, error) {
	op := newClosureOperation(e, attempted)
	defer op.release()

	return op.run(key, supplied, false, false)
}

// run keeps discovery alive through the CAS. Same-key work and publication use
// one shard hold; only associated-key discovery releases it, before taking any
// other shard. Each key keeps its own cuts and consumption relationships.
func (op *closureOperation) run(key string, supplied []Tip, publish, failOnError bool) ([]Tip, bool, error) {
	e := op.engine

	pending := make(map[string][]Tip)

	var queue []string

	walk := func(key string, tips []Tip) []Tip {
		type discoveredBind struct {
			tip Tip
			eff *pb.Effect
		}

		var discovered []discoveredBind

		visitBind := func(tip Tip, eff *pb.Effect) {
			if op.subscriptionOnly || op.verdicts.adjudicated(eff.GetTxnId()) {
				return
			}

			if _, seen := op.binds[tip]; seen {
				return
			}

			discovered = append(discovered, discoveredBind{tip, eff})
		}

		live, cached := op.extendClosedFrontier(key, tips, visitBind)
		if !cached {
			live = op.walk(key, tips, visitBind)
		}
		// A verdict may be reached after its bind, through another supplied
		// tip or fetched ancestry. Finish this key before opening other keys.
		for _, candidate := range discovered {
			if op.verdicts.adjudicated(candidate.eff.GetTxnId()) {
				continue
			}

			tip, bind := candidate.tip, candidate.eff.GetTxnBind()

			if op.binds == nil {
				op.binds = make(map[Tip]map[string]*pb.EffectRef)
			}

			projections := make(map[string]*pb.EffectRef, len(bind.GetKeys()))

			op.binds[tip] = projections
			if op.observeBind != nil {
				op.observeBind(tip)
			}

			// A shared bind is expanded separately on every key, but its key
			// list and associated-key discovery belong to the whole operation.
			for _, kb := range bind.GetKeys() {
				other := string(kb.GetKey())
				if _, exists := projections[other]; exists {
					continue
				}

				projections[other] = kb.GetNewTip()
				if _, served := e.subscription(other); !served {
					continue
				}

				if other != key {
					if _, queued := pending[other]; !queued {
						queue = append(queue, other)
					}

					pending[other] = append(pending[other], tip)
				}
			}
		}

		return live
	}

	mu := e.closureLock(key)
	mu.Lock()

	locked := true
	defer func() {
		if locked {
			mu.Unlock()
		}
	}()

	retry := false

	for {
		current := e.index.ContainsInternal(key)

		tips := supplied
		if publish {
			tips = slices.Concat(current.Tips(), supplied)
		}

		if retry {
			// Associated frontiers and unresolved boundaries may also have
			// advanced. Extend their existing cuts without repeating history.
			for other := range op.work {
				if other != key {
					pending[other] = nil
					queue = append(queue, other)
				}
			}

			slices.Sort(queue)

			retry = false
		}

		live := walk(key, tips)

		if len(queue) != 0 {
			locked = false
			mu.Unlock()

			for next := 0; next < len(queue); next++ {
				other := queue[next]

				func() {
					otherMu := e.closureLock(other)
					otherMu.Lock()
					defer otherMu.Unlock()
					// Associated branches supply closure, not an index update on
					// another key. Ingestion still owns bind publication ordering.
					tips := slices.Concat(e.index.ContainsInternal(other).Tips(), pending[other])
					delete(pending, other)
					walk(other, tips)
				}()
			}

			queue = queue[:0]

			mu.Lock()
			locked = true
			// Incorporate local publications made while another key was
			// being explored before trying to publish this key's frontier.
			continue
		}

		// Reclamation can invalidate a proof while associated-key discovery
		// releases our shard. Retry with ordinary discovery in that case.
		if op.cacheUsed && !op.cacheCurrent() {
			op.release()
			op.cacheUsed = false
			op.cacheDisabled = true

			continue
		}

		var err error

		for _, w := range op.work {
			for _, failure := range w.failures {
				err = errors.Join(err, failure)
			}
		}

		complete := err == nil
		if !publish || failOnError && err != nil {
			return live, complete, err
		}

		next := keytrie.NewTipSet(live...)

		if e.index.ContainsInternal(key) != current {
			retry = true

			continue
		}

		if current.Len() == next.Len() && current.ContainsAll(live) {
		} else if _, ok := op.cas(key, current, next); !ok {
			if e.index.Closed() {
				return nil, false, errors.Join(err, errors.New("engine closed"))
			}

			retry = true

			continue
		} else {
			e.notifyKeyChanged(key)
		}
		// Notification ingestion records processing only after publication,
		// under the same shard as before. Cloud retains its existing policy.
		if (!failOnError || op.recordProgress) && complete && len(live) == 1 && e.effectCache != nil {
			tip := live[0]
			eff, _ := e.effectCache.Get(tip)
			e.recordHistoryProcessed(tip, eff)
		}

		if complete {
			op.rememberClosedFrontier(key, live)
		}

		return live, complete, err
	}
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
	closureEstablished
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
	seed         maphash.Seed
	slots        []int
	visits       []closureVisit
	queue        []int
	pending      map[int][]Tip
	failures     map[int]error
	cachedInput  []Tip
	cachedOutput []Tip
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

	w.cachedInput = nil
	w.cachedOutput = nil
	clear(w.visits)
	clear(w.pending)
	clear(w.failures)
	clear(w.slots)
	w.visits = w.visits[:0]
	w.queue = w.queue[:0]
	closureWorkPool.Put(w)
}

// walk extends this operation's per-key cuts under the key's closure shard.
// New seeds add work; established consumption and expanded ancestry survive CAS
// retries. Cross-key branches are queued rather than taking a second shard.
func (op *closureOperation) walk(key string, supplied []Tip, visitBind func(Tip, *pb.Effect)) []Tip {
	e, attempted := op.engine, op.attempted

	w := op.work[key]
	if w == nil {
		w = closureWorkPool.Get().(*closureWork)
		op.work[key] = w
	}

	if w.pending == nil {
		w.pending = make(map[int][]Tip)
	}

	pending := w.pending
	stop := func(i int) {
		if w.visits[i].flags&closureExpanded == 0 {
			w.visits[i].flags |= closureExpanded | closureEstablished
		}
	}
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

	// 1. Register every supplied tip before following existing parent links,
	// so an established chain can join another supplied frontier directly.
	seeds := keytrie.NewTipSet(supplied...).Tips()
	slices.SortFunc(seeds, compare)

	for _, tip := range seeds {
		i := w.intern(tip)
		if w.visits[i].owner != nil {
			// In particular, do not make a previously consumed seed live.
			// An unresolved identity may have become resident in the meantime.
			if w.visits[i].flags&closureExpanded == 0 {
				w.queue = append(w.queue, i)
			}

			continue
		}

		c := &closureCut{first: tip, ranges: make(map[uint64][2]uint64)}
		claim(i, c)
		w.visits[i].flags |= closureLive
		enqueue(i)
	}

	// 2. A preexisting reverse link identifies established history. Follow
	// one chain to its cut BEFORE installing the newly discovered incoming
	// edge. Record membership along the chain so another join stops there.
	// A retained link accounts for the dependency ancestry already discovered.
	// Follow successors to find the frontier without reopening that ancestry.
	ascend := func(v *vertex, c *closureCut) bool {
		established := false

		for {
			parent, ok := v.firstParent(key)
			if !ok {
				return established
			}

			established = true

			i := w.intern(parent)
			if known := w.visits[i].owner; known != nil {
				join(c, known)

				return true
			}

			claim(i, c)

			v = resident(i)
			if v == nil {
				// The retained link proves a join to a reclaimed cut. The
				// missing parent is neither a new tip nor a fetch obligation.
				return true
			}

			stop(i)

			if _, hasParent := v.firstParent(key); !hasParent {
				w.visits[i].flags |= closureLive | closureExpanded

				c = root(c)
				if compare(parent, c.first) < 0 {
					c.first = parent
				}

				return true
			}
		}
	}

	// A delayed advertised ancestor is already consumed by its retained
	// parent. Discover that successor without walking the ancestor again.
	for _, tip := range seeds {
		i := w.intern(tip)
		if v := resident(i); v != nil {
			if _, known := v.firstParent(key); known {
				w.visits[i].flags &^= closureLive
				stop(i)
				ascend(v, w.visits[i].owner)
			}
		}
	}

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
					delete(w.failures, i)

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
				established := ascend(v, w.visits[i].owner)

				for _, parent := range parents {
					if !op.subscriptionOnly {
						e.effectCache.addParent(v, parent, key)
					}
				}

				if established {
					stop(i)
					delete(pending, i)
					delete(w.failures, i)

					continue
				}
			}

			delete(pending, i)
			delete(w.failures, i)

			w.visits[i].flags |= closureExpanded
			w.visits[i].flags &^= closureEstablished

			op.harvestVerdicts(tip, v.eff)

			deps := v.eff.GetDeps()
			if bind := v.eff.GetTxnBind(); bind != nil {
				visitBind(tip, v.eff)
				// Settled binds need only this key's projection. Full projection
				// tables are built only when cross-key discovery is necessary.
				deps = nil

				if projections, expanded := op.binds[tip]; expanded {
					if ref := projections[key]; ref != nil {
						deps = []*pb.EffectRef{ref}
					}
				} else {
					for ref := range keyDeps(key, v.eff) {
						deps = append(deps, ref)
					}
				}
			}

			if v.root() {
				w.visits[i].flags |= closureNoFetch
			}

			if op.observe != nil {
				op.observe(tip, nil)
			}

			for _, ref := range deps {
				dep := r(ref)
				if dep == (Tip{}) || dep == tip {
					continue
				}

				if op.observe != nil {
					op.observe(tip, ref)
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

					if dv != nil && ascend(dv, w.visits[j].owner) {
						stop(j)
					}
				}

				if dv != nil {
					// Membership-only discovery deliberately omits bind closure;
					// it must not mark that omitted work as already processed.
					if !op.subscriptionOnly {
						e.effectCache.addParent(dv, tip, key)
					}
				} else if w.visits[j].flags&closureExpanded == 0 {
					pending[j] = append(pending[j], tip)
				}

				if w.visits[j].flags&closureExpanded == 0 {
					enqueue(j)
				}
			}
		}

		w.queue = w.queue[:0]

		// Parent links establish closure, but a key-scoped link may be absent
		// on a shared vertex. If candidate regions have not joined, follow the
		// retained boundaries only as far as needed to relate those frontiers.
		var first *closureCut

		separate := false

		for i := range w.visits {
			if w.visits[i].flags&closureLive != 0 {
				cut := root(w.visits[i].owner)
				if first == nil {
					first = cut
				} else if first != cut {
					separate = true

					break
				}
			}
		}

		if separate {
			for i := range w.visits {
				if w.visits[i].flags&closureEstablished != 0 {
					w.visits[i].flags &^= closureExpanded
					w.queue = append(w.queue, i)
				}
			}

			if len(w.queue) != 0 {
				continue
			}
		}

		if len(pending) == 0 {
			return result()
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
				delete(w.failures, i)

				continue
			}

			if err, tried := attempted[tip]; tried {
				if err == nil {
					// A successful load lost during a publication retry is
					// not a definitive response from the history holders.
					err = fmt.Errorf("effect %v reclaimed during frontier closure", tip)
				}

				if !errors.Is(err, ErrEffectNotFound) && !errors.Is(err, ErrCDNBlobMissing) {
					if w.failures == nil {
						w.failures = make(map[int]error)
					}

					w.failures[i] = err
				} else {
					delete(pending, i)
					delete(w.failures, i)
				}
			} else {
				roots = append(roots, i)
			}
		}

		if len(w.queue) != 0 {
			continue
		}

		if len(roots) == 0 {
			return result()
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
			if absent := attempted[w.visits[best].tip]; errors.Is(absent, ErrEffectNotFound) {
				err = absent
			} else {
				err = fmt.Errorf("effect %v reclaimed during frontier closure", w.visits[best].tip)
			}
		}

		attempted[w.visits[best].tip] = err
		if err == nil {
			w.queue = append(w.queue, best)
		}
	}
}
