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

	nodes, err := walkKeyHistory(key, slices.Concat(unique, roots), load)
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
	// during preparation, including tx-data effects that iterate filters
	// out of the processor stream. Callers that need to propagate
	// per-tip state along dependency edges (e.g. HorizonSet
	// invisibility propagation in reconstruct) read this between
	// prepare and iterate.
	topoOrder []Tip
	compact   bool // only a live frontier may release folded history
}

var errEffectNotResident = errors.New("required effect is not resident")

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

// prepare builds the causal order directly from resident vertices. Safe to read
// d.nodes and d.topoOrder after this returns successfully. Ingestion owns fetches;
// iterate applies transaction and snapshot filtering to the resulting order.
func (d *dag) prepare(tips []Tip) error {
	d.lcaTip = Tip{}
	d.topoOrder = nil
	d.nodes = make(map[Tip]*pb.Effect)

	if err := d.topoCollect(tips); err != nil {
		return err
	}

	if len(d.nodes) == 0 {
		return nil
	}

	// Snapshot relationships were established from resident edges. Remove the
	// folded ancestors from the output, retaining compaction's ownership checks.
	d.trimAncestorsOfLCA(tips)

	order := d.topoOrder[:0]
	for _, tip := range d.topoOrder {
		eff, selected := d.nodes[tip]
		if !selected {
			continue
		}

		if eff == nil {
			return fmt.Errorf("reconstruct %q: effect %v: %w", d.key, tip, errEffectNotResident)
		}

		order = append(order, tip)
	}

	d.topoOrder = order

	if slog.Default().Enabled(context.Background(), slog.LevelDebug) {
		slog.Debug("dag.walk", "key", d.key, "nodes", len(d.nodes),
			"lca", d.lcaTip, "encoded", d.encode(tips))
	}

	d.recordHistoryProcessed()

	return nil
}

// ingest is used by recovery entry points, not by the resident graph traversal.
// Preserve the fetch outcome when a definitively absent dependency is still
// required for reconstruction; absence below the selected LCA is harmless.
func (d *dag) ingest(tips []Tip) error {
	attempted := make(map[Tip]error)
	if _, _, err := d.engine.closeIngestedFrontier(d.key, tips, attempted); err != nil {
		return err
	}

	err := d.prepare(tips)
	if !errors.Is(err, errEffectNotResident) {
		return err
	}

	var missing []error

	for tip, eff := range d.nodes {
		if eff != nil {
			continue
		}

		failure := attempted[tip]
		if !errors.Is(failure, ErrCDNBlobMissing) {
			return errors.Join(err, failure)
		}

		missing = append(missing, failure)
	}

	return errors.Join(append(missing, err)...)
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

// An empty subscription records no observed data. Referencing it records an
// observation of metadata, not another data branch. Subscription membership is
// maintained separately; keep the effect for causal bookkeeping only.
func emptySubscription(eff *pb.Effect) bool {
	return eff.GetSubscription() != nil && len(eff.GetDeps()) == 0
}

// visitDataDeps loads causal dependencies while treating subscription metadata
// as transparent. An unsubscribe can carry observed data deps: follow those,
// without making the subscription itself a data root. A chain containing only
// subscription observations has the same data ancestry as no dependencies.
// Missing effects remain unresolved data dependencies, never assumed metadata.
func visitDataDeps(key string, eff *pb.Effect, visit func(Tip) *pb.Effect) bool {
	pending := []*pb.Effect{eff}

	var subscriptions map[Tip]bool

	hasDataDeps := false

	for len(pending) > 0 {
		cur := pending[len(pending)-1]
		pending = pending[:len(pending)-1]

		for ref := range keyDeps(key, cur) {
			tip := r(ref)

			dep := visit(tip)
			if dep.GetSubscription() == nil {
				hasDataDeps = true

				continue
			}

			if emptySubscription(dep) || subscriptions[tip] {
				continue
			}

			if subscriptions == nil {
				subscriptions = make(map[Tip]bool)
			}

			subscriptions[tip] = true

			pending = append(pending, dep)
		}
	}

	return hasDataDeps
}

// walkKeyHistory loads history for cloud tip normalization.
// Missing nodes remain on the dependency frontier, so an exhausted queue cannot
// disguise an unresolved branch. Non-missing load errors take precedence over
// confirmed missing blobs: joining both would let errors.Is classify a transport
// failure as a recoverable hole. State snapshots stop traversal only at
// convergence; their folded history need not remain available. Callers decide
// how to use partial nodes when errors occur.
func walkKeyHistory(key string, tips []Tip, load func(Tip) (*pb.Effect, error)) (map[Tip]*pb.Effect, error) {
	nodes := make(map[Tip]*pb.Effect, len(tips)*8)

	var (
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
		if eff.GetSubscription() != nil {
			delete(frontier, t)

			return
		}

		if snap := eff.GetSnapshot(); snap != nil && snap.GetState() != nil {
			snapshots = append(snapshots, t)
		} else {
			queue = append(queue, t)
		}
	}

	visit := func(t Tip) *pb.Effect {
		if eff, seen := nodes[t]; seen {
			return eff
		}

		frontier[t] = struct{}{}

		eff, err := load(t)
		if err != nil {
			nodes[t] = nil

			recordError(err)

			return nil
		}

		nodes[t] = eff
		if eff == nil {
			return nil
		}

		enqueue(t, eff)

		return eff
	}

	for _, t := range tips {
		if eff := visit(t); eff.GetSubscription() != nil {
			visitDataDeps(key, eff, visit)
		}
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
			return nodes, walkError()
		}

		// A data effect with only subscription observations is a data root,
		// exactly as if those metadata references had not been attached.
		hasDataDeps := visitDataDeps(key, eff, visit)
		if hasDataDeps {
			delete(frontier, cur)
		}
	}

	return nodes, walkError()
}

// trimAncestorsOfLCA removes from d.nodes every node in the transitive
// dep-ancestry of the LCA snapshot. Those nodes' state is already folded
// into the snapshot's reduced effect; including them in ReduceChain would
// double-count their data effects (the symptom is duplicate elements in
// a list-append read).
//
// For a live frontier, the same traversal also visits resident folded history.
// Subscriber evidence and transaction overlays select a retained snapshot cut;
// only creation ownership below that snapshot is released. Reads and frontier
// normalization never fetch below the LCA to perform reclamation.
func (d *dag) trimAncestorsOfLCA(tips []Tip) {
	var zero Tip
	if d.lcaTip == zero {
		return
	}

	lcaEff, ok := d.nodes[d.lcaTip]
	if !ok || lcaEff.GetSnapshot() == nil {
		return
	}

	var boundary *historyBoundary

	if d.compact && !isSystemKey([]byte(d.key)) {
		d.engine.historyMu.Lock()
		defer d.engine.historyMu.Unlock()

		if tipsEqual(d.engine.index.Contains(d.key).Tips(), tips) {
			boundary = d.engine.historyBoundary(d.key)
		}
	}

	type visit struct {
		tip     Tip
		known   []bool
		release bool
	}

	first := visit{tip: d.lcaTip}
	if boundary != nil {
		first.known = make([]bool, len(boundary.peers))
	}

	stack := []visit{first}
	visited := make(map[Tip]bool)

	for len(stack) > 0 {
		entry := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		cur := entry.tip
		if visited[cur] {
			continue
		}

		visited[cur] = true

		eff := d.nodes[cur]
		if cur != d.lcaTip {
			delete(d.nodes, cur)
		}

		if eff == nil && boundary != nil {
			eff, _ = d.engine.effectCache.Get(cur)
		}

		if eff == nil {
			continue // Unknown ancestry supplies no proof; never fetch it here.
		}

		if boundary != nil {
			copied := false

			for i, peer := range boundary.peers {
				if !entry.known[i] && ((cur[0] == peer.Node && cur[1] <= peer.Minted) ||
					(cur[0] == uint64(d.engine.nodeID) && cur[1] == peer.Processed)) {
					if !copied {
						entry.known = slices.Clone(entry.known)
						copied = true
					}

					entry.known[i] = true
				}
			}

			if entry.release {
				d.engine.effectCache.decrefChainNode(d.key, cur)
			} else if cur != d.lcaTip && eff.GetSnapshot().GetState() != nil &&
				!slices.Contains(entry.known, false) && boundary.beforeOverlays(cur) {
				// Keep the live LCA and at least one older state snapshot on
				// each path, including the history between them. Overlays and
				// missing subscriber evidence can move this cut further back.
				entry.release = true
			}
		}

		for dep := range keyDeps(d.key, eff) {
			stack = append(stack, visit{tip: r(dep), known: entry.known, release: entry.release})
		}
	}
}

// topoCollect visits resident vertices once in fork-choice DFS post-order.
// For each vertex, root is the nearest state snapshot crossed by EVERY data
// path from it. A snapshot's parent is the next such snapshot in its ancestry.
// Intersecting those chains at forks establishes the LCA without depending on
// discovery order. The order numbers are local DFS positions, not clocks.
func (d *dag) topoCollect(tips []Tip) error {
	type path struct {
		root, parent Tip
		order        int
		data         bool
	}

	paths := make(map[Tip]path)
	intersect := func(a, b Tip) Tip {
		for a != b && a != (Tip{}) && b != (Tip{}) {
			if paths[a].order > paths[b].order {
				a = paths[a].parent
			} else {
				b = paths[b].parent
			}
		}

		if a == b {
			return a
		}

		return Tip{}
	}
	resident := func(tip Tip) *pb.Effect {
		if eff, seen := d.nodes[tip]; seen {
			return eff
		}

		var eff *pb.Effect
		if d.engine.effectCache != nil {
			eff, _ = d.engine.effectCache.Get(tip)
		}

		d.nodes[tip] = eff

		return eff
	}

	var visit func(Tip, bool) (path, error)

	visit = func(tip Tip, dominates bool) (path, error) {
		if p, seen := paths[tip]; seen {
			if p.order < 0 {
				return p, fmt.Errorf("reconstruct %q: dependency cycle at %v", d.key, tip)
			}

			return p, nil
		}

		paths[tip] = path{order: -1}
		eff := resident(tip)
		// Along a sole data path a state snapshot already dominates the
		// requested frontier. There are no sibling paths to reconcile.
		if dominates && eff.GetSnapshot().GetState() != nil {
			p := path{root: tip, order: len(d.topoOrder), data: true}
			paths[tip] = p
			d.topoOrder = append(d.topoOrder, tip)

			return p, nil
		}

		var local [8]dagTip

		deps := local[:0]
		dataDeps := 0

		for ref := range keyDeps(d.key, eff) {
			dep := r(ref)
			value := resident(dep)

			deps = append(deps, dagTip{dep, value})
			if !emptySubscription(value) {
				dataDeps++
			}
		}

		slices.SortFunc(deps, compareDagTips)

		p := path{}

		for _, dep := range deps {
			ancestor, err := visit(dep.tip, dominates && dataDeps == 1 && !emptySubscription(dep.eff))
			if err != nil {
				return p, err
			}

			if !ancestor.data {
				continue
			}

			if p.data {
				p.root = intersect(p.root, ancestor.root)
			} else {
				p.root = ancestor.root
			}

			p.data = true
		}
		// Metadata-only observations do not introduce a data root. Missing
		// payloads do: they cannot be assumed to contain only metadata.
		p.data = p.data || eff.GetSubscription() == nil
		if eff.GetSnapshot().GetState() != nil {
			p.parent, p.root = p.root, tip
		}

		p.order = len(d.topoOrder)
		paths[tip] = p
		d.topoOrder = append(d.topoOrder, tip)

		return p, nil
	}
	sorted := make([]dagTip, 0, len(tips))
	dataTips := 0

	for _, tip := range tips {
		eff := resident(tip)

		sorted = append(sorted, dagTip{tip, eff})
		if !emptySubscription(eff) {
			dataTips++
		}
	}

	slices.SortFunc(sorted, compareDagTips)

	hasData := false

	for _, entry := range sorted {
		p, err := visit(entry.tip, dataTips == 1 && !emptySubscription(entry.eff))
		if err != nil {
			return err
		}

		if !p.data {
			continue
		}

		if hasData {
			d.lcaTip = intersect(d.lcaTip, p.root)
		} else {
			d.lcaTip = p.root
		}

		hasData = true
	}

	return nil
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
