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

package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
	dp "github.com/swytchdb/engine/cluster/proto/dataplane"
	"github.com/swytchdb/engine/effects"
)

// discoverySkipsTip reports whether an initial cloud frontier tip is metadata
// with no ancestry to walk. Dependency-less subscriptions accumulate as
// permanent tips and must not keep the LCA queue open. An unsubscribe carries
// the tips it consumed as deps, however, so it must enter the walk even though
// ReduceChain later ignores the subscription effect itself.
func discoverySkipsTip(eff *pb.Effect) bool {
	return eff.GetSubscription() != nil && len(eff.GetDeps()) == 0
}

// DiscoverMembers reads the membership roster from the cloud for peer
// discovery: GetTips names candidate marker tips and delivers the closure
// effects inline. The candidates are first reduced to the maximal tips of the
// actual DAG, then ReduceBranch — the real reducer — produces the roster.
// Deliberately zero engine interaction: pre-loading membership into the
// engine makes this node non-divergent at subscription bootstrap, so peers
// ACK instead of NACK and their key filters never arrive — free-read-misses
// then return false negatives for keys written before we joined. Discovery
// only yields candidate addresses; the authoritative state arrives through
// the normal join path.
//
// Only the GetTips round-trip is backstopped. The walk below is bounded per
// fetch (cs.httpClient's timeout) and not in aggregate: its size is the
// membership DAG's depth, so any fixed overall budget is a bound on unbounded
// work, and blowing it would report "cloud unreachable" for a cloud that is
// merely far away — an answer the caller cannot distinguish from "no members"
// and would act on by starting solo.
func (cs *CloudSync) DiscoverMembers(ctx context.Context, membershipKey string) (*pb.ReducedEffect, error) {
	tipsCtx, cancel := context.WithTimeout(ctx, cloudBackstopTimeout)
	resp, err := cs.client.GetTips(tipsCtx, &dp.GetTipsRequest{
		AuthKey: cs.authKey,
		Keys:    [][]byte{CloudKeyName(cs.keyNameKey, []byte(membershipKey))},
	})

	cancel() // unary: resp is fully materialized, nothing streams past here

	if err != nil {
		return nil, fmt.Errorf("cloud get tips: %w", err)
	}
	// Pull the sub-DAG tips-down, seeded from the inline closure sidecar; the
	// per-tip CDN fetch below only covers stragglers the sidecar didn't carry.
	//
	// The walk mirrors dag.bfs's LCA rule: a state-carrying snapshot dequeued
	// with an empty queue is where every tip path has converged, so everything
	// beneath it is folded into its state and the walk stops there. Stopping
	// at the FIRST snapshot seen would be wrong — another tip can reach
	// beneath it, making it a non-LCA sibling whose adoption would drop that
	// tip's branch. Below the LCA the walk is deliberately unbounded:
	// reducing a truncated sub-DAG resurrects any member whose REMOVE was cut
	// while its INSERT survived, and the cluster then redials the dead.
	sidecar := map[effects.Tip]*pb.Effect{}

	var initialTips []effects.Tip

	for _, kt := range resp.GetKeys() {
		for _, ce := range cs.appendSidecar(nil, kt) {
			sidecar[ce.Tip] = ce.Eff
		}

		for _, ref := range kt.GetTips() {
			initialTips = append(initialTips, effects.Tip{ref.GetNodeId(), ref.GetOffset()})
		}
	}
	// Cloud's tips directory is a marker index, not an authoritative DAG
	// frontier. A consumed marker can survive beside the newer candidate that
	// references it, and its blob may later disappear. Walk all readable
	// candidate ancestry first so such a marker is removed as non-maximal before
	// a missing fetch is allowed to become a repair verdict.
	load := func(t effects.Tip) (*pb.Effect, error) {
		if eff, ok := sidecar[t]; ok {
			return eff, nil
		}

		eff, err := cs.fetchEffect(ctx, t)
		if err != nil {
			return nil, err
		}

		sidecar[t] = eff

		return eff, nil
	}
	candidateCount := len(initialTips)

	var superseded []effects.Tip
	// DiscoverMembers is deliberately zero-engine-interaction (see the doc
	// comment above), so there is no local frontier to seed normalization
	// with.
	initialTips, superseded, err = effects.NormalizeCloudTipCandidates(membershipKey, initialTips, nil, load)
	if err != nil {
		return nil, fmt.Errorf("cloud discover: normalize candidate frontier: %w", err)
	}

	if len(superseded) > 0 {
		slog.Info("cloud discover: normalized candidate markers to real tips",
			"candidates", candidateCount, "real_tips", len(initialTips), "superseded", len(superseded))
	}

	return reduceDiscoveredTips(initialTips, load)
}

type cloudEffectLoader func(effects.Tip) (*pb.Effect, error)

// reduceDiscoveredTips reconstructs one normalized candidate frontier without
// mutating the engine. A missing blob remains an error; the caller decides
// whether independently readable sibling candidates make fork choice possible.
func reduceDiscoveredTips(initialTips []effects.Tip, load cloudEffectLoader) (*pb.ReducedEffect, error) {
	visited := map[effects.Tip]bool{}

	var queue []effects.Tip

	enqueue := func(t effects.Tip) {
		if !visited[t] {
			visited[t] = true
			queue = append(queue, t)
		}
	}

	// Pre-resolve the real tips and filter subscriptions before the BFS.
	// Dependency-less subscription effects are metadata that accumulate as
	// permanent frontier entries. Enqueueing them alongside data tips defeats
	// the LCA rule: a state-carrying snapshot dequeued while subscription tips
	// are still queued fails the len(queue)==0 gate. Dependency-bearing
	// unsubscribes are different: their deps are the dropped tips they consumed,
	// so the walk must traverse them; ReduceChain skips the subscription itself.
	for _, t := range initialTips {
		eff, err := load(t)
		if err != nil {
			return nil, fmt.Errorf("cloud discover: fetch real tip %v: %w", t, err)
		}

		if discoverySkipsTip(eff) {
			visited[t] = true

			continue
		}

		enqueue(t)
	}

	fetched := map[effects.Tip]*pb.Effect{}

	var lcaTip effects.Tip

	lcaFound := false
	walkStarted := time.Now()
	lastProgress := walkStarted
	progressed := false

	for len(queue) > 0 {
		tip := queue[0]
		queue = queue[1:]

		eff, err := load(tip)
		if err != nil {
			// A skipped fetch drops the tip's whole dep subtree, which is
			// the same partial-ancestry reduce as a truncated walk. Fail
			// instead; the caller retries with backoff.
			return nil, fmt.Errorf("cloud discover: fetch %v: %w", tip, err)
		}

		fetched[tip] = eff
		if snap := eff.GetSnapshot(); snap != nil && snap.GetState() != nil && len(queue) == 0 {
			lcaTip, lcaFound = tip, true

			break
		}

		for _, dep := range eff.GetDeps() {
			enqueue(effects.Tip{dep.GetNodeId(), dep.GetOffset()})
		}
		// A prune of thousands of dead members leaves a chain that takes
		// minutes to walk. Without this the node is opaque for the whole of
		// it — blocked in bootstrap, so the metrics listener is not up yet and
		// nothing else logs. `queued` is the useful half: falling means the
		// walk is converging on the LCA, climbing means the DAG is still
		// fanning out.
		if time.Since(lastProgress) >= discoverWalkProgressInterval {
			slog.Info("cloud discover: membership walk in progress",
				"fetched", len(fetched), "queued", len(queue), "elapsed", time.Since(walkStarted))

			lastProgress = time.Now()
			progressed = true
		}
	}

	if progressed {
		slog.Info("cloud discover: membership walk complete",
			"fetched", len(fetched), "elapsed", time.Since(walkStarted))
	}

	if len(fetched) == 0 {
		return nil, nil // fresh cluster: nothing in the cloud yet
	}
	// ReduceChain skips snapshot effects (they carry no data op), so the LCA
	// seeds the reduce as state, not as a chain entry. Its dep-ancestry is
	// trimmed first: a tip path that dove beneath the snapshot before the
	// walk converged fetched effects already folded into it, and reducing
	// them again double-counts (mirrors dag.trimAncestorsOfLCA).
	var seed *pb.ReducedEffect

	if lcaFound {
		trimAncestry(fetched, fetched[lcaTip])
		seed = fetched[lcaTip].GetSnapshot().GetState()
		delete(fetched, lcaTip)
	}

	return effects.ReduceChain(seed, topoOrder(fetched)), nil
}

// trimAncestry deletes from fetched every effect in the transitive
// dep-ancestry of eff. Only already-fetched entries are removed; nothing new
// is fetched.
func trimAncestry(fetched map[effects.Tip]*pb.Effect, eff *pb.Effect) {
	stack := make([]effects.Tip, 0, len(eff.GetDeps()))
	for _, dep := range eff.GetDeps() {
		stack = append(stack, effects.Tip{dep.GetNodeId(), dep.GetOffset()})
	}

	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		e, ok := fetched[cur]
		if !ok {
			continue
		}

		delete(fetched, cur)

		for _, dep := range e.GetDeps() {
			stack = append(stack, effects.Tip{dep.GetNodeId(), dep.GetOffset()})
		}
	}
}

// topoOrder sorts a fetched sub-DAG deps-before-dependents (Kahn). The walk's
// discovery order is NOT that: the cloud's tip set can contain orphans — an
// effect whose superseding tip-delete raced ahead of it in the cloud's
// concurrent write pipeline — and an orphan tip that is also an ancestor would
// otherwise reduce after its own descendants (a REMOVE'd member resurrecting
// off its stale INSERT was the observed failure). Concurrent siblings order by
// ForkChoiceHash ascending, mirroring dag.walk.
func topoOrder(fetched map[effects.Tip]*pb.Effect) []*pb.Effect {
	indeg := make(map[effects.Tip]int, len(fetched))

	dependents := make(map[effects.Tip][]effects.Tip, len(fetched))
	for tip, eff := range fetched {
		for _, d := range eff.GetDeps() {
			dt := effects.Tip{d.GetNodeId(), d.GetOffset()}
			if _, ok := fetched[dt]; ok {
				indeg[tip]++
				dependents[dt] = append(dependents[dt], tip)
			}
		}
	}

	var ready []effects.Tip

	for tip := range fetched {
		if indeg[tip] == 0 {
			ready = append(ready, tip)
		}
	}

	chain := make([]*pb.Effect, 0, len(fetched))
	for len(ready) > 0 {
		next := 0
		for i := 1; i < len(ready); i++ {
			if effects.ForkChoiceLess(fetched[ready[i]].GetForkChoiceHash(), fetched[ready[next]].GetForkChoiceHash()) {
				next = i
			}
		}

		tip := ready[next]
		ready[next] = ready[len(ready)-1]
		ready = ready[:len(ready)-1]

		chain = append(chain, fetched[tip])
		for _, dep := range dependents[tip] {
			indeg[dep]--
			if indeg[dep] == 0 {
				ready = append(ready, dep)
			}
		}
	}

	return chain
}
