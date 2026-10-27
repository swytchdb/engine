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
	"errors"
	"sync/atomic"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
)

// ErrTxnAborted is returned by Flush when a transaction loses FWW or
// encounters a real conflict that cannot be resolved.
var ErrTxnAborted = errors.New("transaction aborted")

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
)

// pendingTxn tracks a transaction awaiting NACK resolution.
type pendingTxn struct {
	txnID          string // transaction ID (matches TxnId on emitted effects)
	txnHLC         time.Time
	originNode     pb.NodeID
	forkChoiceHash []byte          // precomputed ForkChoiceHash(originNode, txnHLC)
	bindOffset     Tip             // offset of the TransactionalBindEffect
	keys           []pendingTxnKey // read-only after creation
	state          atomic.Uint32   // txnState*
	done           chan struct{}   // closed on decision
}

type pendingTxnKey struct {
	key          string
	consumedTips []Tip
	newTip       Tip
	readOnly     bool              // only NoopEffect on this key (phantom write detection)
	collection   pb.CollectionKind // collection kind
	elementIDs   [][]byte          // element IDs our tx touches on this key
}

// isRealConflict determines whether a NACK Tip represents a real conflict
// with our pending transaction on the given key.
//
// NACK itself only says "peer's tips diverged from your bind's
// ConsumedTips". Whether that divergence is a real conflict is a
// deterministic function of (our effects, their effects, predicate
// rules) — the same function evaluateBindForkChoice runs on every
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
	queue := []*pb.NackTipDetail{detail}
	visited := make(map[Tip]bool)

	var defeated map[string]Tip

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		tip := r(cur.GetRef())

		if visited[tip] {
			continue
		}

		visited[tip] = true

		if cur.GetIsData() || cur.GetIsBind() {
			if e.isRealConflictTip(ptxn, key, cur) {
				return true, nil
			}

			if txn := e.defeatedCompetitor(ptxn, key, cur); txn != "" {
				if defeated == nil {
					defeated = make(map[string]Tip)
				}

				defeated[txn] = tip
			}

			continue
		}

		if tip != (Tip{}) && e.reachesFromTxOnKey(ptxn, &key, tip) {
			continue
		}

		for _, dep := range cur.GetDeps() {
			dt := r(dep)
			if visited[dt] || e.reachesFromTxOnKey(ptxn, &key, dt) {
				continue
			}

			eff, err := e.getEffect(key, dt)
			if err != nil {
				return true, nil
			}

			queue = append(queue, nackTipDetail(dt, eff))
		}
	}

	return false, defeated
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

		for _, pk := range ptxn.keys {
			if pk.key == key && r(dep) == pk.newTip {
				return true
			}
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

	if e.reachesFromTxOnKey(&theirs, &key, ptxn.bindOffset) {
		return true
	}

	for _, pk := range ptxn.keys {
		if pk.key == key && e.reachesFromTxOnKey(&theirs, &key, pk.newTip) {
			return true
		}
	}

	return false
}

func (e *Engine) isRealConflictTip(ptxn *pendingTxn, key string, detail *pb.NackTipDetail) bool {
	if e.nackTipOrdered(ptxn, key, detail) {
		return false
	}

	// Commutative effects: never conflict
	if !detail.GetIsData() && !detail.GetIsBind() {
		// MetaEffect, SubscriptionEffect, SerializationEffect, NoopEffect
		return false
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
			return false // our own bind
		}

		if ptxn.txnID != "" && theirTxnID != "" {
			ourStart := collectOurBindTips(ptxn, key)

			conflict, bothHadEvidence := e.hasPredicateConflict(
				ptxn.txnID, theirTxnID, key,
				ourStart,
				[]Tip{theirBindOffset})
			if bothHadEvidence && !conflict {
				return false // disjoint predicates — can coexist
			}
		}

		if ForkChoiceLess(detail.GetBindForkChoiceHash(), ptxn.forkChoiceHash) {
			// Their hash is lower → they win, we lose, abort.
			return true
		}
		// Our hash is lower → we win, continue.
		return false
	}

	// Transactional effect without a bind: in-progress, not yet competing
	if detail.GetIsTransactional() && !detail.GetIsBind() {
		return false
	}

	// Non-transactional DataEffect on same key
	if detail.GetIsData() {
		pk := findPendingKey(ptxn, key)
		if pk == nil {
			return false
		}

		return affectsSameData(pk, detail)
	}

	return false
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
			if bind := eff.GetTxnBind(); bind != nil {
				refs = nil

				for _, kb := range bind.GetKeys() {
					if string(kb.GetKey()) == *key {
						refs = []*pb.EffectRef{kb.GetNewTip()}

						break
					}
				}
			} else if string(eff.GetKey()) != *key {
				continue
			}
		}

		for _, dep := range refs {
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

// affectsSameData checks if a competing Tip affects the same data as our
// transaction's key entry.
func affectsSameData(pk *pendingTxnKey, detail *pb.NackTipDetail) bool {
	// If our key is read-only (NoopEffect), any SCALAR write is a phantom write conflict
	if pk.readOnly {
		if detail.GetCollection() == pb.CollectionKind_SCALAR {
			return true
		}
		// For KEYED/ORDERED, a noop read doesn't conflict with writes to
		// different elements, but we conservatively treat it as conflict
		// because we can't distinguish which elements were "read".
		return true
	}

	// SCALAR: always conflicts (single value per key)
	if pk.collection == pb.CollectionKind_SCALAR || detail.GetCollection() == pb.CollectionKind_SCALAR {
		return true
	}

	// ORDERED: any concurrent insert conflicts — position depends on seeing
	// all prior inserts, so concurrent inserts create incompatible orderings.
	if pk.collection == pb.CollectionKind_ORDERED || detail.GetCollection() == pb.CollectionKind_ORDERED {
		return true
	}

	// KEYED: compare element IDs — concurrent writes to different fields are fine
	if len(detail.GetElementId()) == 0 {
		return true // no element ID → assume conflict
	}

	for _, eid := range pk.elementIDs {
		if bytes.Equal(eid, detail.GetElementId()) {
			return true
		}
	}

	return false
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

// defeatedCompetitor returns the txnID of a competing bind that the local
// pendingTxn beat in fork-choice on the given key, or "" if `detail` is not
// a competing bind, names our own tx, is predicate-disjoint (coexists), or
// has a strictly-lower hash than ours. flushTx's NACK loop calls this after
// isRealConflict returns false to gather verdicts for the winner-commit
// snapshot. Mirrors isRealConflict's filtering structure exactly so the two
// classifications stay aligned.
func (e *Engine) defeatedCompetitor(ptxn *pendingTxn, key string, detail *pb.NackTipDetail) string {
	if !detail.GetIsBind() || detail.GetRef() == nil {
		return ""
	}

	theirBindOffset := r(detail.GetRef())

	var theirTxnID string
	if theirEff, err := e.getEffect(key, theirBindOffset); err == nil {
		theirTxnID = theirEff.GetTxnId()
	}

	if theirTxnID == "" || theirTxnID == ptxn.txnID {
		return ""
	}
	// Reachability filters mirror isRealConflict: a foreign bind that is
	// causally ordered relative to our pending tx is not a competitor and
	// must not enter defeatedSet. Without these, our post-commit snapshot
	// records LOST for a transaction that committed cleanly elsewhere
	// and propagates the wrong verdict.
	if e.nackTipOrdered(ptxn, key, detail) {
		return ""
	}

	if ptxn.txnID != "" {
		ourStart := collectOurBindTips(ptxn, key)

		conflict, bothHadEvidence := e.hasPredicateConflict(
			ptxn.txnID, theirTxnID, key,
			ourStart,
			[]Tip{theirBindOffset})
		if bothHadEvidence && !conflict {
			return ""
		}
	}

	if !ForkChoiceLess(ptxn.forkChoiceHash, detail.GetBindForkChoiceHash()) {
		return ""
	}

	return theirTxnID
}

// commitPendingTxn CAS-sets the state to committed and closes the done channel.
func commitPendingTxn(ptxn *pendingTxn) bool {
	if ptxn.state.CompareAndSwap(txnStatePending, txnStateCommitted) {
		close(ptxn.done)

		return true
	}

	return false
}

// abortPendingTxn CAS-sets the state to aborted and closes the done channel.
func abortPendingTxn(ptxn *pendingTxn) bool {
	if ptxn.state.CompareAndSwap(txnStatePending, txnStateAborted) {
		close(ptxn.done)

		return true
	}

	return false
}
