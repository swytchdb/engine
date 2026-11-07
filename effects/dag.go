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

package effects

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"slices"
	"sort"
	"strings"

	pb "github.com/swytchdb/engine/cluster/proto"
)

// allDeps visits ordinary dependencies followed by a bind's new tips without
// copying or modifying the immutable effect's slices.
func allDeps(eff *pb.Effect) iter.Seq[*pb.EffectRef] {
	return func(yield func(*pb.EffectRef) bool) {
		for _, ref := range eff.GetDeps() {
			if !yield(ref) {
				return
			}
		}

		for _, kb := range eff.GetTxnBind().GetKeys() {
			if !yield(kb.GetNewTip()) {
				return
			}
		}
	}
}

// keyDeps follows a bind through this key's new tip, excluding other keys.
// Ordinary effects retain all dependencies, including cross-key read evidence.
func keyDeps(key string, eff *pb.Effect) iter.Seq[*pb.EffectRef] {
	return func(yield func(*pb.EffectRef) bool) {
		if bind := eff.GetTxnBind(); bind != nil {
			for _, kb := range bind.GetKeys() {
				if string(kb.GetKey()) == key {
					yield(kb.GetNewTip())

					return
				}
			}

			return
		}

		for _, ref := range eff.GetDeps() {
			if !yield(ref) {
				return
			}
		}
	}
}

// normalizeTipCandidates removes candidates named by the key-scoped ancestry
// of other candidates or additional roots. Only candidates can be removed.
// A nil effect or failed load is a gap: retain unresolved candidates rather than
// infer edges. Continue other branches and return load errors alongside the
// partial proof. The caller decides whether incomplete history prevents its
// operation; it does not change the walk or discard successful ancestry proof.
func normalizeTipCandidates(key string, candidates, roots []Tip, load func(Tip) (*pb.Effect, error)) (realTips, superseded []Tip, err error) {
	candidateSet := make(map[Tip]struct{}, len(candidates))

	unique := make([]Tip, 0, len(candidates))
	for _, candidate := range candidates {
		if _, exists := candidateSet[candidate]; exists {
			continue
		}

		candidateSet[candidate] = struct{}{}
		unique = append(unique, candidate)
	}

	supersededSet := make(map[Tip]struct{}, len(unique))

	nodes, _, err := walkKeyHistory(key, slices.Concat(unique, roots), load)
	for cur, eff := range nodes {
		for ref := range keyDeps(key, eff) {
			dep := r(ref)
			if _, candidate := candidateSet[dep]; candidate && dep != cur {
				supersededSet[dep] = struct{}{}
			}
		}
	}

	for _, candidate := range unique {
		if _, stale := supersededSet[candidate]; stale {
			superseded = append(superseded, candidate)
		} else {
			realTips = append(realTips, candidate)
		}
	}

	return realTips, superseded, err
}

type dag struct {
	engine      *Engine
	key         string
	activeTxnID string
	nodes       map[Tip]*pb.Effect
	lcaTip      Tip
	// topoOrder is the post-order DFS sequence of every tip reached
	// during phase 2, including tx-data effects that iterate filters
	// out of the processor stream. Callers that need to propagate
	// per-tip state along dependency edges (e.g. HorizonSet
	// invisibility propagation in reconstruct) read this between
	// prepare and iterate.
	topoOrder []Tip
}

// dagTip carries the effect while sorting, avoiding map lookups per comparison.
type dagTip struct {
	tip Tip
	eff *pb.Effect
}

func compareDagTips(a, b dagTip) int {
	return bytes.Compare(a.eff.GetForkChoiceHash(), b.eff.GetForkChoiceHash())
}

func newDag(engine *Engine, key string, activeTxnID string) *dag {
	return &dag{engine: engine, key: key, activeTxnID: activeTxnID}
}

// walk performs a two-phase traversal of the effect graph:
//
//  1. BFS from tips following deps, tracking path convergence. Stops at the
//     LCA snapshot (first snapshot where all paths have converged). Collects
//     all nodes between tips and the LCA.
//  2. Topological replay (DFS post-order) from tips within the collected set,
//     ordered by fork choice hash at each fork. Calls processor in causal
//     order (LCA snapshot → tips).
//
// Transactional effects (non-empty TxnId, not a bind) are skipped from the
// processor output but their deps are still followed. Bind effects appear
// in the sequence as envelopes.
//
// walk is a convenience wrapper over prepare + iterate; callers that need
// to inspect d.topoOrder between BFS and the processor loop should call
// prepare and iterate explicitly.
func (d *dag) walk(tips []Tip, processor func(*pb.Effect) error) error {
	if err := d.prepare(tips); err != nil {
		return err
	}

	return d.iterate(func(_ Tip, eff *pb.Effect) error {
		return processor(eff)
	})
}

// prepare runs BFS + LCA trim and builds d.topoOrder. Safe to read
// d.nodes and d.topoOrder after this returns successfully.
func (d *dag) prepare(tips []Tip) error {
	d.lcaTip = Tip{}
	d.topoOrder = nil

	if err := d.bfs(tips, &d.lcaTip); err != nil {
		return err
	}

	if len(d.nodes) == 0 {
		return nil
	}

	// Phase 1.5: trim nodes already folded into the LCA snapshot. BFS may
	// have walked into the snapshot's dep chain (because the snapshot was
	// dequeued with a non-empty queue, so it wasn't terminal at that point).
	// Those nodes are already represented in the snapshot's reduced state;
	// leaving them in d.nodes makes ReduceChain process the same data
	// effects twice — duplicates in list reads, double-counts in scalars.
	d.trimAncestorsOfLCA()

	if slog.Default().Enabled(context.Background(), slog.LevelDebug) {
		slog.Debug("dag.walk", "key", d.key, "nodes", len(d.nodes),
			"lca", d.lcaTip, "encoded", d.encode(tips))
	}

	// Phase 2: topo-order via DFS post-order within collected nodes.
	d.topoOrder = make([]Tip, 0, len(d.nodes))
	topoVisited := make(map[Tip]bool, len(d.nodes))

	sortedTips := make([]dagTip, 0, len(tips))
	for _, tip := range tips {
		if eff, ok := d.nodes[tip]; ok {
			sortedTips = append(sortedTips, dagTip{tip, eff})
		}
	}

	slices.SortFunc(sortedTips, compareDagTips)

	for _, entry := range sortedTips {
		d.topoCollect(entry.tip, topoVisited)
	}

	return nil
}

// iterate calls processor for each emitted tip in d.topoOrder, filtering
// out tx-data effects (TxnId != "", not a bind, not the active txn) and
// non-LCA snapshots. Requires prepare to have run.
func (d *dag) iterate(processor func(Tip, *pb.Effect) error) error {
	for _, t := range d.topoOrder {
		eff := d.nodes[t]
		if d.shouldEmit(t, eff) {
			if err := processor(t, eff); err != nil {
				return err
			}
		}
	}

	return nil
}

// shouldEmit returns true iff the effect at tip t should be passed to
// the processor. Non-LCA snapshots and tx-data from non-active txns
// participate in dependency walking but are not emitted.
func (d *dag) shouldEmit(t Tip, eff *pb.Effect) bool {
	if eff.GetSnapshot() != nil {
		return t == d.lcaTip
	}

	if eff.GetTxnId() != "" && eff.GetTxnBind() == nil && eff.GetTxnId() != d.activeTxnID {
		return false
	}

	return true
}

// bfs explores the graph from tips following deps. The dependency frontier
// is separate from the queue: visiting a root finishes work, but does not
// make that path converge with another path. A snapshot can terminate the
// walk only when it is the sole remaining frontier entry.
//
// Only state-carrying snapshots terminate the walk. Verdict-only snapshots
// (State == nil) are opaque markers for fork-choice adjudication; their
// chain content is reachable only via their Deps, so we follow them like
// any other effect. Treating them as LCAs would leave reconstruct with a
// single dead-end tip and no way to recover the underlying chain.
func (d *dag) bfs(tips []Tip, lcaTip *Tip) error {
	nodes, lca, err := walkKeyHistory(d.key, tips, func(tip Tip) (*pb.Effect, error) {
		return d.engine.getEffect(d.key, tip)
	})
	d.nodes = nodes
	*lcaTip = lca

	return err
}

// walkKeyHistory is the shared dependency walk for replay and tip normalization.
// Missing nodes remain on the dependency frontier, so an exhausted queue cannot
// disguise an unresolved branch. Non-missing load errors take precedence over
// confirmed missing blobs: joining both would let errors.Is classify a transport
// failure as a recoverable hole. State snapshots stop traversal only at
// convergence; their folded history need not remain available. Callers decide
// how to use partial nodes when errors occur.
func walkKeyHistory(key string, tips []Tip, load func(Tip) (*pb.Effect, error)) (map[Tip]*pb.Effect, Tip, error) {
	nodes := make(map[Tip]*pb.Effect, len(tips)*8)

	var (
		lcaTip        Tip
		loadErrors    []error
		missingErrors []error
	)

	recordError := func(err error) {
		if errors.Is(err, ErrCDNBlobMissing) {
			missingErrors = append(missingErrors, err)
		} else {
			loadErrors = append(loadErrors, err)
		}
	}
	walkError := func() error {
		if len(loadErrors) > 0 {
			return errors.Join(loadErrors...)
		}

		return errors.Join(missingErrors...)
	}

	queue := make([]Tip, 0, len(tips)*2)
	queuePos := 0
	snapshots := make([]Tip, 0, len(tips))
	frontier := make(map[Tip]struct{}, len(tips))
	enqueue := func(t Tip, eff *pb.Effect) {
		if snap := eff.GetSnapshot(); snap != nil && snap.GetState() != nil {
			snapshots = append(snapshots, t)
		} else {
			queue = append(queue, t)
		}
	}

	for _, t := range tips {
		if _, seen := nodes[t]; seen {
			continue
		}

		frontier[t] = struct{}{}

		eff, err := load(t)
		if err != nil {
			nodes[t] = nil

			recordError(err)

			continue
		}

		nodes[t] = eff
		if eff == nil {
			continue
		}
		// A standalone subscription tip carries no data ancestry. If another
		// path later references it, it becomes part of that path's frontier.
		if eff.GetSubscription() != nil && len(eff.GetDeps()) == 0 {
			delete(frontier, t)
		}

		enqueue(t, eff)
	}

	for len(queue) > 0 || len(snapshots) > 0 {
		var cur Tip
		if len(queue) > 0 {
			cur = queue[queuePos]
			queuePos++

			if queuePos == len(queue) {
				queue = queue[:0]
				queuePos = 0
			}
		} else {
			// Let ordinary paths reach a pending snapshot before expanding
			// its ancestry. Each effect is still fetched and expanded once.
			cur = snapshots[0]
			snapshots = snapshots[1:]
		}

		eff := nodes[cur]

		if snap := eff.GetSnapshot(); snap != nil && snap.GetState() != nil && len(frontier) == 1 {
			lcaTip = cur

			return nodes, lcaTip, walkError()
		}

		// Roots finish queued work without resolving their dependency path.
		first := true
		for ref := range keyDeps(key, eff) {
			if first {
				delete(frontier, cur)

				first = false
			}

			dt := r(ref)
			if dep, seen := nodes[dt]; seen {
				if dep.GetSubscription() != nil && len(dep.GetDeps()) == 0 {
					frontier[dt] = struct{}{}
				}

				continue
			}

			frontier[dt] = struct{}{}

			depEff, err := load(dt)
			if err != nil {
				nodes[dt] = nil

				recordError(err)

				continue
			}

			nodes[dt] = depEff
			if depEff == nil {
				continue
			}

			enqueue(dt, depEff)
		}
	}

	// A root snapshot may have been processed before a longer path reached
	// it. Its frontier entry survives even though its queue entry is gone.
	if len(frontier) == 1 {
		for t := range frontier {
			if snap := nodes[t].GetSnapshot(); snap != nil && snap.GetState() != nil {
				lcaTip = t
			}
		}
	}

	return nodes, lcaTip, walkError()
}

// trimAncestorsOfLCA removes from d.nodes every node in the transitive
// dep-ancestry of the LCA snapshot. Those nodes' state is already folded
// into the snapshot's reduced effect; including them in ReduceChain would
// double-count their data effects (the symptom is duplicate elements in
// a list-append read).
//
// Only nodes already in d.nodes are removed — we don't fetch additional
// effects from the engine. The bfs that populated d.nodes is the only
// source of folded-ancestor entries we need to clear.
func (d *dag) trimAncestorsOfLCA() {
	var zero Tip
	if d.lcaTip == zero {
		return
	}

	lcaEff, ok := d.nodes[d.lcaTip]
	if !ok || lcaEff.GetSnapshot() == nil {
		return
	}

	stack := make([]Tip, 0, len(lcaEff.GetDeps()))
	for _, dep := range lcaEff.GetDeps() {
		stack = append(stack, r(dep))
	}

	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		eff, ok := d.nodes[cur]
		if !ok {
			continue
		}

		// Removing a node also marks it visited, so shared ancestors need no
		// separate set and are expanded only once.
		delete(d.nodes, cur)

		for dep := range keyDeps(d.key, eff) {
			stack = append(stack, r(dep))
		}
	}
}

// topoCollect does a post-order DFS within the collected nodes, recording
// every visited tip into d.topoOrder. At forks, deps are visited in fork
// choice hash order. Filtering (tx-data effects, non-LCA snapshots) is
// applied by iterate, not here — d.topoOrder is the complete causal order
// of visited tips so callers can propagate per-tip state along dep edges.
func (d *dag) topoCollect(t Tip, visited map[Tip]bool) {
	if visited[t] {
		return
	}

	visited[t] = true

	eff, ok := d.nodes[t]
	if !ok {
		return
	}

	// LCA snapshot: record and don't descend past it
	if eff.GetSnapshot() != nil && t == d.lcaTip {
		d.topoOrder = append(d.topoOrder, t)

		return
	}

	// Ordinary chains need neither an iterator nor a sorting buffer.
	if eff.GetTxnBind() == nil && len(eff.GetDeps()) == 1 {
		d.topoCollect(r(eff.GetDeps()[0]), visited)
		d.topoOrder = append(d.topoOrder, t)

		return
	}

	// Typical forks fit on the stack; unusually wide forks can grow the slice.
	var local [8]dagTip

	deps := local[:0]

	for ref := range keyDeps(d.key, eff) {
		tip := r(ref)
		if dep, ok := d.nodes[tip]; ok {
			deps = append(deps, dagTip{tip, dep})
		}
	}

	slices.SortFunc(deps, compareDagTips)

	for _, dep := range deps {
		d.topoCollect(dep.tip, visited)
	}

	d.topoOrder = append(d.topoOrder, t)
}

// encode produces a compact string encoding of the collected dag nodes.
// Format: "N<count>;L<lca_node>:<lca_off>;T<tip_node>:<tip_off>,...;[<node>:<off>:kind:val:dep_node:dep_off,...;...]"
// Effects are identified by their full (nodeID, offset) so log readers can
// directly check whether a specific emitted offset shows up — and with what
// kind — without reverse-mapping position indexes.
func (d *dag) encode(tips []Tip) string {
	offsets := make([]Tip, 0, len(d.nodes))
	for off := range d.nodes {
		offsets = append(offsets, off)
	}

	sort.Slice(offsets, func(i, j int) bool {
		if offsets[i][0] != offsets[j][0] {
			return offsets[i][0] < offsets[j][0]
		}

		return offsets[i][1] < offsets[j][1]
	})

	var b strings.Builder
	fmt.Fprintf(&b, "N%d;", len(d.nodes))

	if d.lcaTip != (Tip{}) {
		fmt.Fprintf(&b, "L%d:%d;", d.lcaTip[0], d.lcaTip[1])
	}

	b.WriteString("T")

	first := true

	for _, t := range tips {
		if _, ok := d.nodes[t]; ok {
			if !first {
				b.WriteByte(',')
			}

			fmt.Fprintf(&b, "%d:%d", t[0], t[1])

			first = false
		}
	}

	b.WriteByte(';')

	for _, off := range offsets {
		eff := d.nodes[off]

		kind := byte('?')
		val := int64(0)

		switch {
		case eff.GetData() != nil:
			kind = 'D'
			val = eff.GetData().GetIntVal()
		case eff.GetSnapshot() != nil:
			kind = 'S'

			if s := eff.GetSnapshot().GetState(); s != nil && s.GetScalar() != nil {
				val = s.GetScalar().GetIntVal()
			}
		case eff.GetNoop() != nil:
			kind = 'N'
		case eff.GetMeta() != nil:
			kind = 'M'
		case eff.GetSubscription() != nil:
			kind = 'U'
		case eff.GetTxnBind() != nil:
			kind = 'B'
		case eff.GetSerialization() != nil:
			kind = 'X'
		}

		fmt.Fprintf(&b, "%d:%d:%c:%d:", off[0], off[1], kind, val)

		depFirst := true

		for _, dep := range eff.GetDeps() {
			dt := r(dep)
			if _, ok := d.nodes[dt]; ok {
				if !depFirst {
					b.WriteByte(',')
				}

				fmt.Fprintf(&b, "%d:%d", dt[0], dt[1])

				depFirst = false
			}
		}

		b.WriteByte(';')
	}

	return b.String()
}
