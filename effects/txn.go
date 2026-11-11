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
	"errors"
	"sync/atomic"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
	"google.golang.org/protobuf/proto"
)

// ErrTxnAborted is returned by Flush when a transaction loses FWW or
// encounters a real conflict that cannot be resolved.
var ErrTxnAborted = errors.New("transaction aborted")

// ErrTxnAmbiguous is returned when a published bind could not be confirmed by
// every subscriber. Its effects may become visible; callers must not treat
// this as a definite abort or automatically retry the transaction.
var ErrTxnAmbiguous = errors.New("transaction outcome unknown; commit may have occurred")

// ErrRegionPartitioned is returned by Flush in SafeMode when not all
// same-region peers are reachable.
var ErrRegionPartitioned = errors.New("region partitioned: not all same-region peers are reachable")

// ErrBootstrapIncomplete is returned by ensureSubscribed when the bootstrap
// could not fetch the full causal chain because some peers are unreachable.
// A background retry continues until the chain is complete.
var ErrBootstrapIncomplete = errors.New("bootstrap incomplete: some peers unreachable")

// ErrAuthorityDropped signals that HandleRemote rejected the inbound
// effect because this node has no authority over the key. The
// transport layer interprets this as "do not respond" — neither ACK
// nor NACK is sent — so the sender's tracked replication times out
// rather than counting us as a successful first-ACK replica. The
// sender's other peers (those with authority) still get the chance
// to accept the write.
var ErrAuthorityDropped = errors.New("authority dropped: not subscribed to this key")

// DefaultSerializationThreshold is the number of consecutive aborts on a
// key before escalating to serialized coordination.
const DefaultSerializationThreshold = 3

// txnState constants for pendingTxn.state.
const (
	txnStatePending   uint32 = 0
	txnStateCommitted uint32 = 1
	txnStateAborted   uint32 = 2
	txnStateAmbiguous uint32 = 3
)

// pendingTxn tracks a transaction awaiting NACK resolution.
type pendingTxn struct {
	txnID          string // transaction ID (matches TxnId on emitted effects)
	txnHLC         time.Time
	originNode     pb.NodeID
	forkChoiceHash []byte           // precomputed ForkChoiceHash(originNode, txnHLC)
	bindOffset     Tip              // offset of the TransactionalBindEffect
	keys           []pendingTxnKey  // read-only after creation
	state          atomic.Uint32    // txnState*
	done           chan struct{}    // closed on decision
	resolved       chan struct{}    // closed after evaluation and evidence publication
	responses      []*pb.NackNotify // immutable once resolved closes
	proofSources   []Tip            // predecessor snapshots actually used in resolution
}

type pendingTxnKey struct {
	key          string
	consumedTips []Tip
	newTip       Tip
	readOnly     bool              // only NoopEffect on this key (phantom write detection)
	collection   pb.CollectionKind // collection kind
	elementIDs   [][]byte          // element IDs our tx touches on this key
}

// hasWritesSince validates an observation against the current key history.
// NOOPs and snapshots can replace the observed tips without changing the value.
// Tentative effects do not write a value until their bind arrives. Follow these
// wrappers to find actual writes, stopping at the observed causal boundary.
func (e *Engine) hasWritesSince(key string, observed []Tip, txnID string) bool {
	seen := make(map[Tip]bool, len(observed))
	base := &pendingTxn{}

	for _, tip := range observed {
		seen[tip] = true
		base.keys = append(base.keys, pendingTxnKey{key: key, newTip: tip})
	}

	current := e.index.Contains(key).Tips()
	if len(current) == 0 && len(observed) != 0 {
		return true
	}

	stack := append([]Tip(nil), current...)
	for len(stack) > 0 {
		tip := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if seen[tip] {
			continue
		}

		seen[tip] = true

		eff, err := e.getEffect(key, tip)
		if err != nil {
			if e.reachesFromTxOnKey(base, &key, tip) {
				continue
			}

			return true
		}

		if eff.GetTxnId() != txnID || txnID == "" {
			writes := eff.GetTxnId() == "" && (eff.GetData() != nil || eff.GetMeta() != nil)
			if eff.GetTxnBind() != nil {
				// The bind, not its pre-bind effects, must be in our past.
				if e.reachesFromTxOnKey(base, &key, tip) {
					continue
				}

				effects, err := e.collectBindEffects(eff, key)
				if err != nil {
					return true
				}

				for _, bound := range effects {
					writes = writes || bound.GetData() != nil || bound.GetMeta() != nil || bound.GetRowWrite() != nil
				}
			}

			if writes {
				if !e.reachesFromTxOnKey(base, &key, tip) {
					return true
				}

				continue
			}
		}

		for dep := range keyDeps(key, eff) {
			stack = append(stack, r(dep))
		}
	}

	return false
}

// isRealConflict determines whether a NACK Tip represents a real conflict
// with our pending transaction on the given key.
//
// NACK itself only says "peer's tips diverged from your bind's
// ConsumedTips". Whether that divergence is a real conflict is a
// deterministic function of (our effects, their effects, predicate
// rules) — the same predicate and ancestry rules used on every
// node. All call sites must reach the same answer so the client's
// abort/commit signal matches the cluster's view of the bind.
func (e *Engine) isRealConflict(ptxn *pendingTxn, key string, detail *pb.NackTipDetail) bool {
	conflict, _ := e.checkNackConflict(ptxn, key, detail)

	return conflict
}

// checkNackConflict unwraps snapshots and other non-data markers before applying
// fork choice. A snapshot at the peer's frontier is not a cutoff: only ancestry
// already shared with our transaction can be skipped. Missing competing history
// cannot authorize a commit. Keep the defeated bind anchors from the same pass
// so flushTx publishes the same decisions for wrapped and unwrapped competitors.
func (e *Engine) checkNackConflict(ptxn *pendingTxn, key string, detail *pb.NackTipDetail) (bool, map[string]Tip) {
	candidates, _, err := e.nackCandidates(ptxn, key, []*pb.NackTipDetail{detail})
	if err != nil {
		return true, nil
	}

	var defeated map[string]Tip

	for _, cur := range candidates {
		conflict, txn := e.classifyNackTip(ptxn, key, cur)
		if conflict {
			return true, nil
		}

		if txn != "" {
			if defeated == nil {
				defeated = make(map[string]Tip)
			}

			defeated[txn] = r(cur.GetRef())
		}
	}

	return false, defeated
}

// nackCandidates discovers competing envelopes and cached fork-choice evidence.
// Discovery does not decide winners. Unrelated keys can supply evidence without
// subscribing to them or treating their effects as conflicts with our transaction.
func (e *Engine) nackCandidates(ptxn *pendingTxn, key string, details []*pb.NackTipDetail) ([]*pb.NackTipDetail, verdictMap, error) {
	queue := append([]*pb.NackTipDetail(nil), details...)
	visited := make(map[Tip]bool)
	verdicts := make(verdictMap)

	var (
		candidates []*pb.NackTipDetail
		snapshots  []Tip
	)

	for len(queue) > 0 {
		cur := queue[len(queue)-1]
		queue = queue[:len(queue)-1]

		tip := r(cur.GetRef())
		if visited[tip] {
			continue
		}

		visited[tip] = true

		if cur.GetIsData() || cur.GetIsBind() {
			candidates = append(candidates, cur)

			continue
		}

		deps := cur.GetDeps()

		var predecessor *pb.EffectRef

		if eff, err := e.getEffect(key, tip); err == nil {
			if snap := eff.GetSnapshot(); snap != nil {
				if err := e.harvestProofVerdicts(tip, eff, verdicts); err != nil {
					return nil, nil, err
				}

				predecessor = snap.GetPrevSnapshot()
			}
		}

		if tip != (Tip{}) && e.reachesFromTxOnKey(ptxn, &key, tip) {
			continue
		}

		if predecessor != nil {
			snapshots = append(snapshots, r(predecessor))
		}

		for _, dep := range deps {
			dt := r(dep)
			if visited[dt] || e.reachesFromTxOnKey(ptxn, &key, dt) {
				continue
			}

			eff, err := e.getEffect(key, dt)
			if err != nil {
				if findPendingKey(ptxn, key) != nil {
					return nil, nil, ErrTxnAmbiguous
				}

				continue
			}

			queue = append(queue, nackTipDetail(dt, eff))
		}
	}

	// PrevSnapshot is a non-owning verdict shortcut. Visit required ancestry
	// first so a shortcut cannot hide a missing ordinary dependency. Available
	// predecessors supply verdicts, not another requirement for folded value history.
	for len(snapshots) > 0 {
		tip := snapshots[len(snapshots)-1]
		snapshots = snapshots[:len(snapshots)-1]

		if visited[tip] || e.reachesFromTxOnKey(ptxn, &key, tip) {
			continue
		}

		visited[tip] = true

		eff, err := e.getEffect(key, tip)
		if err != nil || eff.GetSnapshot() == nil {
			continue
		}

		if err := e.harvestProofVerdicts(tip, eff, verdicts); err != nil {
			return nil, nil, err
		}

		if prev := eff.GetSnapshot().GetPrevSnapshot(); prev != nil {
			snapshots = append(snapshots, r(prev))
		}
	}

	return candidates, verdicts, nil
}

// evaluateNacks runs only after every subscriber has responded. First collect
// ALL evidence: a later response can resolve a bind named by an earlier NACK.
// A better hash grants priority, not victory. Without its resolution the
// published transaction remains ambiguous, rather than reporting a false abort.
func (e *Engine) evaluateNacks(ptxn *pendingTxn, nacks []*pb.NackNotify) (map[string]Tip, error) {
	ptxn.proofSources = nil

	type candidate struct {
		key    string
		detail *pb.NackTipDetail
		txn    string
	}

	var candidates []candidate

	verdicts := make(verdictMap)

	for _, nack := range nacks {
		key := string(nack.GetKey())

		tips, evidence, err := e.nackCandidates(ptxn, key, nack.GetTipDetails())
		if err != nil {
			return nil, err
		}

		for txn, entry := range evidence {
			if prior, ok := verdicts[txn]; ok && prior.verdict != entry.verdict {
				return nil, ErrTxnAmbiguous
			}

			verdicts[txn] = entry
		}

		if findPendingKey(ptxn, key) == nil {
			continue
		}

		for _, tip := range tips {
			c := candidate{key: key, detail: tip}
			if tip.GetIsBind() {
				eff, err := e.getEffect(key, r(tip.GetRef()))
				if err != nil || eff.GetTxnBind() == nil {
					return nil, ErrTxnAmbiguous
				}

				c.txn = eff.GetTxnId()
			}

			candidates = append(candidates, c)
		}
	}
	// Establish priority before evaluating competing candidates. Predicate-
	// disjoint binds can coexist regardless of the other transaction's outcome.
	for _, c := range candidates {
		if c.detail.GetIsBind() && c.txn != ptxn.txnID &&
			ForkChoiceLess(c.detail.GetBindForkChoiceHash(), ptxn.forkChoiceHash) &&
			!e.nackTipOrdered(ptxn, c.key, c.detail) && verdicts[c.txn].verdict == pb.Verdict_VERDICT_UNSPECIFIED {
			if e.bindPredicatesDisjoint(ptxn, c.key, c.txn, r(c.detail.GetRef())) {
				continue
			}

			return nil, ErrTxnAmbiguous
		}
	}

	defeated := make(map[string]Tip)

	for _, c := range candidates {
		if verdicts[c.txn].verdict == pb.Verdict_LOST {
			ptxn.proofSources = append(ptxn.proofSources, verdicts[c.txn].snapshotTip)

			continue
		}

		conflict, txn := e.classifyNackTip(ptxn, c.key, c.detail)
		if conflict {
			return nil, ErrTxnAborted
		}

		if txn != "" {
			defeated[txn] = r(c.detail.GetRef())
		}
	}

	return defeated, nil
}

// priorityPredecessors is called at the bind ingestion/publication boundary.
// Only local origins defer responses: every origin is a subscriber of its own
// keys, so the sender must receive their response before evaluating. Other
// subscribers can answer immediately. All waits point toward a smaller hash.
func (e *Engine) priorityPredecessors(ref Tip, eff *pb.Effect) []*pendingTxn {
	var prior []*pendingTxn

	detail := nackTipDetail(ref, eff)

	e.pendingTxns.Range(func(_ Tip, txn *pendingTxn) bool {
		if txn.resolved == nil || !ForkChoiceLess(txn.forkChoiceHash, detail.GetBindForkChoiceHash()) {
			return true
		}

		for _, kb := range eff.GetTxnBind().GetKeys() {
			key := string(kb.GetKey())
			if findPendingKey(txn, key) != nil && !e.nackTipOrdered(txn, key, detail) {
				prior = append(prior, txn)

				break
			}
		}

		return true
	})

	return prior
}

// finishPriorityResponse waits without nodeMu, then forwards the evidence the
// predecessor used, including keys the requester need not subscribe to. A
// bounded wait without evidence leaves the original unresolved NACK intact;
// evaluateNacks will return an ambiguous result, never infer an abort.
func (e *Engine) finishPriorityResponse(ref *pb.EffectRef, nacks []*pb.NackNotify, prior []*pendingTxn) []*pb.NackNotify {
	if len(prior) == 0 {
		return nacks
	}

	timer := time.NewTimer(horizonFallbackWait)
	defer timer.Stop()

	for _, txn := range prior {
		select {
		case <-txn.resolved:
		case <-timer.C:
			return nacks
		}

		for _, response := range txn.responses {
			forwarded := proto.Clone(response).(*pb.NackNotify)
			forwarded.Conflicting = ref
			nacks = append(nacks, forwarded)
		}

		for _, pk := range txn.keys {
			nacks = append(nacks, e.buildEnrichedNack(pk.key, ref, e.index.Contains(pk.key).Tips()))
		}
	}

	return nacks
}

// nackTipOrdered proves that a data effect or bind is causally ordered with our
// transaction. Wrappers must be expanded first: a snapshot may merge our own
// bind with an independent competitor, and its edge to us cannot excuse that
// other branch.
func (e *Engine) nackTipOrdered(ptxn *pendingTxn, key string, detail *pb.NackTipDetail) bool {
	for _, dep := range detail.GetDeps() {
		if r(dep) == ptxn.bindOffset {
			return true
		}
	}

	if detail.GetRef() != nil {
		target := r(detail.GetRef())

		var zero Tip
		if target != zero && e.reachesFromTxOnKey(ptxn, &key, target) {
			return true
		}
	}

	// Descendancy can also run through data, noops, or snapshots; the direct
	// dependency shortcut above alone misses those paths.
	var theirs pendingTxn
	for _, dep := range detail.GetDeps() {
		theirs.keys = append(theirs.keys, pendingTxnKey{key: key, newTip: r(dep)})
	}

	return e.reachesFromTxOnKey(&theirs, &key, ptxn.bindOffset)
}

// classifyNackTip decides both sides of fork choice in one pass. Ordered or
// predicate-disjoint effects coexist; otherwise a bind's hash selects the loser.
// The returned transaction ID identifies a defeated competitor, when known.
func (e *Engine) classifyNackTip(ptxn *pendingTxn, key string, detail *pb.NackTipDetail) (conflict bool, defeated string) {
	// Commutative effects: never conflict
	if !detail.GetIsData() && !detail.GetIsBind() {
		// MetaEffect, SubscriptionEffect, SerializationEffect, NoopEffect
		return false, ""
	}

	if e.nackTipOrdered(ptxn, key, detail) {
		return false, ""
	}

	// Competing bind: NACK tells us there's a competitor; fork-choice
	// hash decides who's first. ForkChoiceHash is the cluster-wide
	// arbiter — every observer's reconstruct uses the same rule, so
	// origin's abort decision MUST match it. Local "they NACK'd me, so
	// I lose" is wrong: space-like concurrency means there's no observer-
	// independent "first"; the hash provides the deterministic order
	// every observer agrees on. Predicate refinement is the only out:
	// disjoint writes coexist regardless of hash.
	if detail.GetIsBind() {
		theirBindOffset := r(detail.GetRef())

		var theirTxnID string
		if theirEff, err := e.getEffect(key, theirBindOffset); err == nil {
			theirTxnID = theirEff.GetTxnId()
		}

		if theirTxnID != "" && theirTxnID == ptxn.txnID {
			return false, "" // our own bind
		}

		if e.bindPredicatesDisjoint(ptxn, key, theirTxnID, theirBindOffset) {
			return false, "" // disjoint predicates — can coexist
		}

		if ForkChoiceLess(detail.GetBindForkChoiceHash(), ptxn.forkChoiceHash) {
			// Their hash is lower → they win, we lose, abort.
			return true, ""
		}

		if detail.GetRef() != nil && ForkChoiceLess(ptxn.forkChoiceHash, detail.GetBindForkChoiceHash()) {
			return false, theirTxnID
		}

		return false, ""
	}

	// Transactional effect without a bind: in-progress, not yet competing
	if detail.GetIsTransactional() {
		return false, ""
	}

	// Mixing transactional and non-transactional operations has no isolation
	// guarantee. An unordered ordinary write does not defeat a published bind:
	// reconstruction cannot derive that transaction loss from competing binds.
	// Writes observed before publication are still checked by hasWritesSince.
	return false, ""
}

// bindPredicatesDisjoint shares the per-key exemption between priority gating
// and classification. Missing predicate evidence keeps the conservative fallback.
func (e *Engine) bindPredicatesDisjoint(ptxn *pendingTxn, key, theirTxnID string, theirBindOffset Tip) bool {
	if ptxn.txnID == "" || theirTxnID == "" {
		return false
	}

	conflict, bothHadEvidence := e.hasPredicateConflict(
		ptxn.txnID, theirTxnID, key,
		collectOurBindTips(ptxn, key), []Tip{theirBindOffset})

	return bothHadEvidence && !conflict
}

// reachesFromTx returns true if target is in the DAG ancestry of our
// pending tx (its bind offset or any per-key NewTip), following dependencies
// and bind NewTips via getEffect. Unavailable effects stop that branch, so false
// means "we cannot prove ancestry", not "definitely concurrent".
// reachesFromTx requires every target to be in the transaction's ancestry.
// Missing intermediate effects cannot prove a path. Targets share one walk.
func (e *Engine) reachesFromTx(ptxn *pendingTxn, targets ...Tip) bool {
	return e.reachesFromTxOnKey(ptxn, nil, targets...)
}

// A key-scoped proof follows only that key's bind NewTip. Global reachability
// can order transactions through another key while their branches on this key
// remain concurrent. A nil key retains the cross-key proof used by admission;
// an empty string is still a real key.
func (e *Engine) reachesFromTxOnKey(ptxn *pendingTxn, key *string, targets ...Tip) bool {
	var zero Tip

	remaining := make(map[Tip]struct{}, len(targets))
	for _, target := range targets {
		if target == zero {
			return false
		}

		remaining[target] = struct{}{}
	}

	if len(remaining) == 0 {
		return true
	}

	visited := make(map[Tip]struct{})
	stack := make([]Tip, 0, 1+len(ptxn.keys))
	push := func(t Tip) {
		if t == zero {
			return
		}

		if _, seen := visited[t]; seen {
			return
		}

		visited[t] = struct{}{}
		stack = append(stack, t)
	}
	push(ptxn.bindOffset)

	for _, pk := range ptxn.keys {
		if key == nil || pk.key == *key {
			push(pk.newTip)
		}
	}

	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		delete(remaining, cur)

		if len(remaining) == 0 {
			return true
		}

		var fetchKey string
		if key != nil {
			fetchKey = *key
		}

		eff, err := e.getEffect(fetchKey, cur)
		if err != nil {
			continue
		}

		refs := allDeps(eff)
		if key != nil {
			if eff.GetTxnBind() == nil && string(eff.GetKey()) != *key {
				continue
			}

			refs = keyDeps(*key, eff)
		}

		for dep := range refs {
			push(r(dep))
		}
	}

	return false
}

// collectOurBindTips returns the local tx's start tips for the walk
// on a specific key — our bind offset plus our NewTip on that key.
func collectOurBindTips(ptxn *pendingTxn, key string) []Tip {
	tips := []Tip{ptxn.bindOffset}
	for i := range ptxn.keys {
		if ptxn.keys[i].key == key {
			tips = append(tips, ptxn.keys[i].newTip)

			break
		}
	}

	return tips
}

// findPendingKey finds the pendingTxnKey for a given key name.
func findPendingKey(ptxn *pendingTxn, key string) *pendingTxnKey {
	for i := range ptxn.keys {
		if ptxn.keys[i].key == key {
			return &ptxn.keys[i]
		}
	}

	return nil
}

// finish publishes one terminal decision and wakes waiters exactly once.
// Ambiguous ends the local wait without claiming commit or abort; an already
// recorded decision always takes priority over a later attempt.
func (ptxn *pendingTxn) finish(state uint32) bool {
	if ptxn.state.CompareAndSwap(txnStatePending, state) {
		close(ptxn.done)

		return true
	}

	return false
}
