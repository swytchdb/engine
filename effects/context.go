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
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	rand "math/rand/v2"
	"slices"
	"sort"
	"sync"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
	"github.com/swytchdb/engine/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// watchedKeyState records the state at WATCH time so that BeginTx and
// flushTx can detect modifications via the causal structure.
type watchedKeyState struct {
	noopOffset Tip             // offset of the WATCH-time NOOP in the log
	tips       *keytrie.TipSet // TipSet pointer AFTER the WATCH NOOP was flushed
	hadData    bool            // key had actual data (non-nil filterSnapshot) at WATCH time
	flushGen   uint64          // engine.flushGeneration at WATCH time
}

// Context tracks per-key state for dep chaining (Emit) and deferred
// index update + broadcast (Flush). One Context per command invocation.
// A context may be reused after Flush or Abort, but is not safe for concurrent use.
type Context struct {
	engine      *Engine
	traceCtx    context.Context // carries the active OTel span for this command
	inTx        bool
	txnID       string
	keys        map[string]*contextKey
	watchedKeys map[string]*watchedKeyState // keys registered via Watch with WATCH-time state
	txSnapshot  *transactionSnapshot        // compact boundary and lazily captured causal history
	history     historyOverlay
	readErr     error // failed transactional reads must not flush queued writes

	// readOnly marks a context used only for read-only, non-transactional
	// commands. Such a context may answer a read-miss from the cluster key
	// filters without subscribing (free misses), and skips chain compaction.
	// Recording an observation or emitting an effect makes it writable.
	readOnly bool
}

type contextKey struct {
	initialTips *keytrie.TipSet    // tips read from index on first Emit for this key
	lastOffset  Tip                // most recently written effect (nodeID + offset)
	notifies    []*pb.OffsetNotify // all notifies in emit order — every effect must be broadcast for durability

	// Transaction metadata (only populated when inTx)
	readOnly   bool              // only NoopEffects on this key
	watched    bool              // true if this key was added via WATCH (SSI fork detection in flushTx)
	collection pb.CollectionKind // collection kind
	elementIDs [][]byte          // element IDs touched
	hasData    bool              // at least one DataEffect emitted

	// Notification intent — tracks whether this key should wake blocked clients
	shouldNotifyData   bool // an INSERT_OP was the last data-modifying op on this key
	shouldNotifyDelete bool // a full-key REMOVE was the last data-modifying op

	// Pending peer-subscriber mutations from SubscriptionEffects emitted on this
	// key. Applied in Flush AFTER updateIndex creates the leaf — the subscriber
	// set lives on the leaf (leafState.subscribers), so addPeerSubscriber is a
	// no-op until the leaf exists. The first effect on a key is often the
	// SubscriptionEffect itself, so applying at Emit time would drop it.
	subOps []subOp
}

// subOp is a deferred peer-subscriber mutation; see contextKey.subOps.
type subOp struct {
	peer        pb.NodeID
	unsubscribe bool
}

// ContextSavepoint is an opaque snapshot of a Context's pending
// per-key state. Restore it (via Context.RestoreSavepoint) to
// discard every Emit made after the snapshot was taken; already-
// written log offsets become orphans (never indexed), same as
// Abort. Used by higher-level SQL SAVEPOINT semantics.
//
// Savepoints do NOT snapshot the engine's committed index — the
// engine's state is unchanged until Flush, so reverting the
// Context's pending keys is sufficient to roll back.
type ContextSavepoint struct {
	keys    map[string]*contextKey
	readErr error
}

// TakeSavepoint captures a deep-enough copy of the Context's pending
// key state that RestoreSavepoint can revert every Emit issued
// between Take and Restore. Cheap: each contextKey is a small struct
// with slice storage that we copy; the underlying values (notifies and
// elementIDs) are immutable once added and can be shared.
func (c *Context) TakeSavepoint() *ContextSavepoint {
	return &ContextSavepoint{
		keys:    cloneContextKeys(c.keys),
		readErr: c.readErr,
	}
}

// RestoreSavepoint replaces the Context's pending-key state with the
// snapshot. Any Emit issued after TakeSavepoint is discarded from
// the Context; its log offsets remain in the engine's log but never
// reach the index (identical to what happens to all effects on
// Abort).
func (c *Context) RestoreSavepoint(sp *ContextSavepoint) {
	if sp == nil {
		return
	}
	// Clone the snapshot's keys map so subsequent Emits don't mutate
	// it (in case the caller retains the snapshot for nested
	// rollbacks).
	c.keys = cloneContextKeys(sp.keys)
	// Explicit command rollback discards its partial writes and handles its
	// read failure. Unhandled failures still prevent Flush from committing.
	// Snapshot membership stays fixed: rollback cannot undo exposed values.
	c.readErr = sp.readErr
}

// Savepoints own the mutable slice storage. Elements and tip sets are immutable
// once emitted, so their contents can be shared across repeated restores.
func cloneContextKeys(keys map[string]*contextKey) map[string]*contextKey {
	if len(keys) == 0 {
		return nil
	}

	cloned := make(map[string]*contextKey, len(keys))
	for key, state := range keys {
		clonedState := *state
		clonedState.notifies = slices.Clone(state.notifies)
		clonedState.elementIDs = slices.Clone(state.elementIDs)
		clonedState.subOps = slices.Clone(state.subOps)
		cloned[key] = &clonedState
	}

	return cloned
}

// NewContext creates a new write context bound to the engine.
func (e *Engine) NewContext() *Context {
	return &Context{
		engine: e,
	}
}

// NewReadOnlyContext creates a context for read-only, non-transactional
// commands. It uses the per-node cluster key filters to answer a read-miss
// without issuing a subscription — a key no peer holds returns nil for free.
// Explicit observations or effect emission promote it to a write context.
// It must not be used inside MULTI/EXEC (SSI reads must subscribe).
func (e *Engine) NewReadOnlyContext() *Context {
	return &Context{
		engine:   e,
		readOnly: true,
	}
}

// PendingKeys returns the names of all keys with pending effects in
// this Context. Callers use this to acquire per-key locks before
// calling Flush — Flush's fork-choice critical section races with
// any other Flush on the same key, and the handler layer (redis
// handler, sql handler) is where that serialisation belongs.
func (c *Context) PendingKeys() []string {
	if c == nil || len(c.keys) == 0 {
		return nil
	}

	out := make([]string, 0, len(c.keys))
	for k := range c.keys {
		out = append(out, k)
	}

	return out
}

// SetTraceCtx sets the OTel trace context for this command.
func (c *Context) SetTraceCtx(ctx context.Context) {
	c.traceCtx = ctx
}

// TraceCtx returns the OTel trace context, or context.Background() if none set.
func (c *Context) TraceCtx() context.Context {
	if c.traceCtx != nil {
		return c.traceCtx
	}

	return context.Background()
}

// GetSnapshot returns the current materialized state of a key, including
// any unflushed effects from this context. Within MULTI/EXEC, earlier
// commands' effects are in the log but not yet in the index; this method
// reconstructs from the log so commands within the same transaction can
// see each other's writes.
// The returned ReducedEffect is immutable and may be shared by later reads.
//
// When a txSnapshot is active (MULTI/EXEC with SSI), reads for keys not
// yet in the context use the snapshot instead of the live index, and a
// NOOP is emitted to record the read in the causal structure.
func (c *Context) GetSnapshot(key string) (*pb.ReducedEffect, []Tip, error) {
	if c != nil && c.txSnapshot != nil && slog.Default().Enabled(c.TraceCtx(), slog.LevelDebug) {
		s := c.txSnapshot

		decisions, ambiguous, evidence := len(s.decided), s.ambiguous, s.clocks
		defer func() {
			slog.DebugContext(c.TraceCtx(), "mvcc: read work", "key", key,
				"additional_history", len(s.decided) > decisions,
				"additional_evidence", s.clocks != evidence,
				"ambiguous_inclusion", s.ambiguous > ambiguous,
				"failed", c.readErr != nil)
		}()
	}

	result, tips, err := c.getSnapshot(key)
	if err != nil && c != nil && c.txSnapshot != nil {
		c.readErr = err
	}

	return result, tips, err
}

func (c *Context) getSnapshot(key string) (*pb.ReducedEffect, []Tip, error) {
	if c == nil {
		return nil, nil, nil
	}

	if c.readErr != nil {
		return nil, nil, c.readErr
	}

	if tracing.Enabled() {
		_, snapSpan := tracing.Tracer().Start(c.TraceCtx(), "effects.get_snapshot",
			trace.WithAttributes(attribute.String("effect.key", key)))
		defer snapSpan.End()
	}

	ck, hasPending := c.keys[key]
	if !hasPending {
		// SSI: if we have a snapshot, read from it instead of the live index
		if c.txSnapshot != nil {
			return c.getSnapshotFromTx(key)
		}
		// No unflushed effects for this key — delegate to committed state.
		// A definite miss (nothing local, no peer filter admits the key) is
		// answered by engine.GetSnapshot's free-miss gate without announcing
		// a subscription; with Cloud configured its backstop still checks
		// durable storage before deciding the read is a miss.
		result, tips, chainLen, err := c.engine.GetSnapshot(key)
		if err != nil {
			return nil, nil, err
		}
		// Compact long chains by emitting a state snapshot into this context.
		// Guard on result != nil: engine.GetSnapshot is meant to zero chainLen
		// when result is nil, but the check is defensive against future
		// regressions in that contract.
		compactThreshold := 5 + rand.IntN(10)
		if c.engine.broadcaster == nil {
			compactThreshold = 5
		}

		if !c.readOnly && result != nil && chainLen >= compactThreshold {
			ls := c.engine.index.LoadData(key)

			stamp := time.Since(compactionEpoch).Nanoseconds() + 1
			if ls == nil || !ls.claimCompaction(stamp) {
				return filterSnapshot(result), tips, nil
			}

			slog.Debug("compaction: emitting snapshot",
				"key", key,
				"chainLen", chainLen,
				"tips", tips)
			// Stamp the authoritative subscriber set onto the snapshot's state.
			// Reads no longer carry Subscribers (it left the reduce pipeline),
			// so the compaction snapshot is where the set is persisted for a
			// future bootstrapping peer to recover after the raw
			// SubscriptionEffects have been compacted away.
			// reconstructLocal may have returned the immutable reduced memo.
			// Compaction owns the state it emits, so detach before stamping the
			// subscriber set instead of forcing every read to clone up front.
			result = cloneReduced(result)
			result.Subscribers = c.engine.snapshotSubscribers(key)
			snapEff := &pb.SnapshotEffect{
				Collection: result.GetCollection(),
				State:      result,
			}

			for _, t := range tips {
				cached, err := c.engine.getEffect(key, t)
				if err != nil {
					continue
				}

				if cached.GetSnapshot() != nil {
					snapEff.PrevSnapshot = toPbRef(t)

					break
				}
			}

			if err := c.Emit(&pb.Effect{
				Key:  []byte(key),
				Kind: &pb.Effect_Snapshot{Snapshot: snapEff},
			}, tips); err != nil {
				ls.lastCompaction.CompareAndSwap(stamp, 0)
				slog.Error("compaction: snapshot emit failed",
					"key", key,
					"chainLen", chainLen,
					"error", err)
			}
		}

		return filterSnapshot(result), tips, nil
	}

	// Reconstruct from lastOffset merged with index tips. When an SSI
	// snapshot is active (MULTI/EXEC), use the snapshot to preserve
	// isolation — concurrent writes must not be visible. Outside SSI,
	// use the live index so concurrent writes (e.g. XGROUP DESTROY
	// while a blocking XREADGROUP holds a compaction snapshot) are visible.
	myTip := ck.lastOffset
	reconTips := []Tip{myTip}

	var baseTips []Tip

	if c.txSnapshot != nil {
		ts, err := c.txSnapshot.tips(c.TraceCtx(), key)
		if err != nil {
			return nil, nil, err
		}

		if ts != nil {
			// Match getSnapshotFromTx: resolve pending-tx tips to their
			// pre-tx deps so a later read in the same txn walks the same
			// committed-as-of-snapshot DAG as the first read did.
			baseTips = c.engine.resolveTipDeps(ts.Tips())
		}
	} else if ts := c.engine.index.Contains(key); ts != nil {
		baseTips = ts.Tips()
	}

	for _, t := range baseTips {
		if t != myTip {
			reconTips = append(reconTips, t)
		}
	}

	slog.Debug("Context.GetSnapshot: hasPending reconstruct", "key", key,
		"txn_id", c.txnID, "recon_tips", reconTips)
	// waitForHorizon=true: client-facing in-txn read. If the walk reaches
	// an in-horizon bind on the ancestry, block on its resolution before
	// returning. Without this the read can return a list that excludes a
	// bind which the very next read (post-promotion) will include —
	// :incompatible-order on a single key.
	var view *snapshotReadView
	if c.txSnapshot != nil {
		view = c.txSnapshot.views[key]
		reconTips = append(reconTips, view.anchors[key]...)
	}

	result, _, err := c.engine.reconstructWithView(key, reconTips, c.txnID, true, view)
	if err != nil {
		return nil, nil, err
	}

	if c.txSnapshot != nil {
		result = filterSnapshotAt(result, c.txSnapshot.expiryAt)
	} else {
		result = filterSnapshot(result)
	}

	// Return initialTips — these are the committed tips from before our writes.
	var tips []Tip
	if ck.initialTips != nil {
		tips = ck.initialTips.Tips()
	}

	return result, tips, nil
}

// GetSnapshots reads an ordinary batch from a shared reconstruction view.
// Transactional and pending-effect contexts retain their SSI/read-your-writes
// path, including read-set NOOPs; they must not switch to a live read view.
func (c *Context) GetSnapshots(keys []string) ([]*pb.ReducedEffect, error) {
	results := make([]*pb.ReducedEffect, len(keys))
	if c == nil {
		return results, nil
	}

	if c.readErr != nil {
		return nil, c.readErr
	}

	if c.inTx || c.txSnapshot != nil || len(c.keys) != 0 {
		if c.txSnapshot != nil {
			s := c.txSnapshot
			decisions, ambiguous, evidence := len(s.decided), s.ambiguous, s.clocks

			if err := c.txSnapshot.prepare(c.TraceCtx(), keys); err != nil {
				c.readErr = err

				return nil, err
			}

			slog.DebugContext(c.TraceCtx(), "mvcc: batch preparation work", "keys", keys,
				"additional_history", len(s.decided) > decisions,
				"additional_evidence", s.clocks != evidence,
				"ambiguous_inclusion", s.ambiguous > ambiguous)
		}

		for i, key := range keys {
			var err error

			results[i], _, err = c.GetSnapshot(key)
			if err != nil {
				return nil, err
			}
		}

		return results, nil
	}

	results, err := c.engine.GetSnapshots(c.TraceCtx(), keys)
	if err != nil {
		return nil, err
	}

	for i := range results {
		results[i] = filterSnapshot(results[i])
	}

	return results, nil
}

// getSnapshotFromTx reads a key from the SSI snapshot and emits a NOOP
// to record the read in the causal log for the Bind's read set.
func (c *Context) getSnapshotFromTx(key string) (*pb.ReducedEffect, []Tip, error) {
	slog.Debug("getSnapshotFromTx: entry", "key", key, "txn_id", c.txnID)

	tips, err := c.txSnapshot.tips(c.TraceCtx(), key)
	if err != nil {
		return nil, nil, err
	}

	snapshotTips := tips.Tips()

	tipOffsets := c.engine.resolveTipDeps(snapshotTips)

	tipOffsets = slices.Concat(tipOffsets, c.txSnapshot.views[key].anchors[key])

	var result *pb.ReducedEffect
	if len(tipOffsets) != 0 {
		// waitForHorizon=true: SSI-snapshot read served back to the client.
		result, _, err = c.engine.reconstructWithView(key, tipOffsets, c.txnID, true, c.txSnapshot.views[key])
		if err != nil {
			return nil, nil, err
		}
	}

	result = filterSnapshotAt(result, c.txSnapshot.expiryAt)

	// Record every successful read, including absence. An empty historical
	// frontier is still a dependency: omitting it from c.keys would omit it
	// from SSI validation and the Bind, allowing absent-key write skew.
	// Emit retains the frozen data frontier, adding only subscription metadata.
	if err := c.Emit(&pb.Effect{
		Key:  []byte(key),
		Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
	}, snapshotTips); err != nil {
		slog.Error("getSnapshotFromTx: NOOP emit failed", "key", key, "error", err)

		return nil, nil, err
	}

	return result, snapshotTips, nil
}

// Watch records a key for optimistic locking by emitting a NoopEffect
// immediately and flushing it. This records the observation in the
// causal log so all nodes can verify whether the key was modified
// between WATCH and EXEC. The NOOP offset and post-flush TipSet pointer
// are stored for comparison at BeginTx time.
//
// The NOOP carries the upcoming transaction's id so its observation can be
// validated with that transaction. Other observations and snapshots may consume
// it: only observing the bind establishes that the transaction has committed.
//
// Errors fail the WATCH: a read that can't be answered (e.g. the Cloud
// consult failed for an evicted key) must not silently record hadData=false —
// the EXEC verdict would then rest on a fabricated observation.
func (c *Context) Watch(key string) error {
	// An observation writes a NOOP into causal history. Reuse the ordinary
	// compaction path before recording it, including for read-only callers.
	c.readOnly = false

	if c.watchedKeys == nil {
		c.watchedKeys = make(map[string]*watchedKeyState)
	}

	// Lazily materialize the txn id so the WATCH noop is tx-marked.
	// The upcoming BeginTx (at MULTI/EXEC) inherits this id.
	if c.txnID == "" {
		c.txnID = c.engine.generateTxnID()
	}

	// Check if key has actual data BEFORE emitting the NOOP
	snap, _, err := c.GetSnapshot(key)
	if err != nil {
		return err
	}

	hadData := snap != nil

	// Emit NOOP to record the observation in the causal log. We set
	// TxnId explicitly because Emit only stamps it inside an active
	// MULTI (c.inTx); WATCH is pre-MULTI but the noop still belongs
	// to the upcoming tx.
	if err := c.Emit(&pb.Effect{
		Key:   []byte(key),
		TxnId: c.txnID,
		Kind:  &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
	}); err != nil {
		slog.Error("Watch: NOOP emit failed", "key", key, "error", err)

		return err
	}

	// Capture the NOOP offset before Flush clears c.keys
	noopOffset := c.keys[key].lastOffset

	// Flush immediately so the NOOP is durable and in the index.
	// ExecuteInto's post-handler Flush will be a no-op (c.keys empty).
	if err := c.Flush(); err != nil {
		return err
	}

	// Capture the post-flush TipSet pointer (immutable — pointer identity = equality)
	tips := c.engine.index.Contains(key)
	c.watchedKeys[key] = &watchedKeyState{
		noopOffset: noopOffset,
		tips:       tips,
		hadData:    hadData,
		flushGen:   c.engine.flushGeneration.Load(),
	}

	return nil
}

// ClearWatches removes all watched keys. Called from UNWATCH, DISCARD,
// and EXEC's pre-BeginTx abort paths — all cases where the upcoming
// transaction is being discarded. Also drops the lazily-generated
// txnID so the next WATCH/MULTI starts fresh; if we're already inside
// MULTI/EXEC (c.inTx) the id is load-bearing for in-flight effects
// and must be preserved.
func (c *Context) ClearWatches() {
	c.watchedKeys = nil

	if !c.inTx {
		c.txnID = ""
	}
}

// BeginTx marks subsequent effects as transactional.
// This is called by read-modify-write commands (INCR, LPUSH, etc.) for
// atomicity AND by handleExec for MULTI/EXEC. Watch processing is NOT
// done here — use CheckWatches after BeginTx for EXEC.
func (c *Context) BeginTx() {
	if c.inTx {
		return // nested transaction inherits outer
	}

	c.engine.pinHistory(&c.history)

	c.inTx = true
	// A prior WATCH may have already materialized our txn id; reuse it
	// so the WATCH noop and the MULTI/EXEC effects share one tx.
	if c.txnID == "" {
		c.txnID = c.engine.generateTxnID()
	}
	// O(1) with respect to indexed keys: capture only the temporal boundary.
	// Histories and bind closures are selected when a key is accessed.
	if c.txSnapshot == nil {
		c.txSnapshot = c.engine.newTransactionSnapshot()
	}
}

// CheckWatches validates WATCH observations and emits transactional NOOPs
// for the Bind's read set. Must be called AFTER BeginTx (so NOOPs get
// IsTransactional=true). Only called from handleExec.
//
// Returns false if any watched key was modified — the caller should abort
// the transaction and return a null array to the client.
func (c *Context) CheckWatches() bool {
	c.engine.pinHistory(&c.history)
	// BeginTx already captured the SSI snapshot at txn start. Cover the
	// case where CheckWatches is called without a prior BeginTx (no
	// transactional emits before EXEC).
	if c.txSnapshot == nil {
		c.txSnapshot = c.engine.newTransactionSnapshot()
	}

	if len(c.watchedKeys) == 0 {
		return true
	}

	// Validate watched keys — same check a subscriber would perform
	currentFlushGen := c.engine.flushGeneration.Load()
	for key, ws := range c.watchedKeys {
		currentTips := c.engine.index.Contains(key)
		if ws.tips == currentTips {
			continue // TipSet pointer identical — no change
		}
		// Pointer changed. Handle FLUSHALL on non-existent key:
		// Redis says FLUSHALL does NOT dirty watches on keys that didn't exist.
		if ws.flushGen != currentFlushGen && !ws.hadData && currentTips == nil {
			continue // key didn't exist at WATCH time, still doesn't after flush
		}

		if !c.engine.hasWritesSince(key, ws.tips.Tips(), c.txnID) {
			continue // Only observations or compaction changed the frontier.
		}

		// Key was modified — abort
		c.watchedKeys = nil
		c.inTx = false
		c.txnID = ""
		c.txSnapshot = nil
		c.engine.unpinHistory(&c.history)

		return false
	}

	// Emit transactional NOOPs using WATCH NOOP offsets as snapshotTips.
	// This forces deps on the WATCH-time state, creating a fork if the
	// key was modified (the fork is structurally detectable by any node).
	for key, ws := range c.watchedKeys {
		if err := c.Emit(&pb.Effect{
			Key:  []byte(key),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, []Tip{ws.noopOffset}); err != nil {
			slog.Error("CheckWatches: transactional NOOP emit failed", "key", key, "error", err)

			c.watchedKeys = nil
			c.inTx = false
			c.txnID = ""
			c.txSnapshot = nil
			c.engine.unpinHistory(&c.history)

			return false
		}
		// Mark as watched for SSI fork detection in flushTx
		if ck, ok := c.keys[key]; ok {
			ck.watched = true
		}
	}

	c.watchedKeys = nil

	return true
}

// Abort discards pending index updates and broadcasts. Effects already
// written to the log remain durable but invisible (index not updated).
func (c *Context) Abort() {
	c.reset()
	c.watchedKeys = nil
}

// Emit writes a single effect to the log. The context tracks per-key
// state so that consecutive effects on the same key form a dep chain.
// The index is NOT updated until Flush.
//
// Emit takes ownership of eff: it fills causality fields in place and
// caches the message itself, so the caller must not retain, reuse, or
// mutate eff (or its submessages) after the call. Byte-slice fields may
// alias pooled buffers (e.g. RESP parser args) — Emit copies those
// before caching.
//
// For read-modify-write commands, pass the tip offsets returned by
// GetSnapshot as snapshotTips so that the first effect depends on the
// tips the handler actually read, not whatever the index contains now.
// Pure writes (SET, LPUSH) omit snapshotTips and Emit reads the index.
func (c *Context) Emit(eff *pb.Effect, snapshotTips ...[]Tip) error {
	c.engine.pinHistory(&c.history)

	if c.readErr != nil {
		return c.readErr
	}

	key := string(eff.GetKey())

	// The effect must dep-reference our own SubscriptionEffect, or the
	// subscription's offset is orphaned in this node's DAG and the subscriber
	// graph splits brain (see ensureSubscribed). Reads no longer subscribe on
	// a definite miss (GetSnapshot's free-miss gate), so ANY context can reach
	// Emit unsubscribed — authority is claimed here, where state is created.
	// Allocation-free no-op when already subscribed.
	var subscribeErr error
	if c.inTx {
		// A fresh transactional key must discover its peer subscribers before
		// commit; an async announcement can leave both origins believing solo.
		subscribeErr = c.engine.ensureSubscribedBlocking(key)
	} else {
		subscribeErr = c.engine.ensureSubscribed(key)
	}

	if subscribeErr != nil {
		return subscribeErr
	}

	if tracing.Enabled() {
		_, emitSpan := tracing.Tracer().Start(c.TraceCtx(), "effects.emit",
			trace.WithAttributes(attribute.String("effect.key", key)))
		defer emitSpan.End()
	}

	// Fill causality
	eff.Hlc = timestamppb.New(c.engine.clock.Now())
	eff.NodeId = uint64(c.engine.nodeID)

	eff.ForkChoiceHash = ComputeForkChoiceHash(c.engine.nodeID, eff.GetHlc())
	if c.inTx {
		eff.TxnId = c.txnID
	}

	// Fill deps
	ck, exists := c.keys[key]
	if !exists {
		var tips *keytrie.TipSet
		if len(snapshotTips) > 0 && snapshotTips[0] != nil {
			tips = keytrie.NewTipSet(snapshotTips[0]...)
		} else if c.txSnapshot != nil {
			var err error

			tips, err = c.txSnapshot.tips(c.TraceCtx(), key)
			if err != nil {
				c.readErr = err

				return err
			}
		} else {
			tips = c.engine.index.Contains(key)
		}

		if c.inTx {
			// Subscribing after BeginTx creates metadata outside the fixed
			// data snapshot. Consume those subscription tips as well: leaving
			// them orphaned loses the shared causal base and can make future
			// retries abort forever on an already-spoken subscription tip.
			// Never adopt newer data, binds, or snapshots from the live index.
			deps := append([]Tip(nil), tips.Tips()...)
			for _, tip := range c.engine.index.Contains(key).Tips() {
				if parent, ok := c.engine.effectCache.Get(tip); ok && parent.GetSubscription() != nil {
					deps = append(deps, tip)
				}
			}

			if len(deps) != 0 {
				tips = keytrie.NewTipSet(deps...)
			}
		}

		ck = &contextKey{initialTips: tips, readOnly: c.inTx}
		if c.keys == nil {
			c.keys = make(map[string]*contextKey)
		}

		c.keys[key] = ck

		if tips != nil {
			tipOffsets := tips.Tips()
			// Exclude tentative values, retaining observed NOOPs as dependencies.
			eff.Deps = toPbRefs(c.engine.resolveTipDeps(tipOffsets))
		}
	} else {
		eff.Deps = []*pb.EffectRef{toPbRef(ck.lastOffset)}
	}

	// Track notification intent from DataEffect ops
	if data := eff.GetData(); data != nil {
		switch data.GetOp() {
		case pb.EffectOp_UNKNOWN_OP:
			// Unspecified operations leave notification intent unchanged.
		case pb.EffectOp_INSERT_OP:
			ck.shouldNotifyData = true
			ck.shouldNotifyDelete = false
		case pb.EffectOp_REMOVE_OP:
			if len(data.GetId()) == 0 {
				// Full key delete
				ck.shouldNotifyData = false
				ck.shouldNotifyDelete = true
			} else if data.GetCollection() == pb.CollectionKind_ORDERED || data.GetCollection() == pb.CollectionKind_KEYED {
				// Element remove on ordered/keyed collection — chain-wake
				// so the next blocked client gets a chance to pop
				ck.shouldNotifyData = true
			}
		}
	}

	// Track tx metadata
	if c.inTx {
		if data := eff.GetData(); data != nil {
			ck.readOnly = false
			ck.hasData = true

			ck.collection = data.GetCollection()
			if len(data.GetId()) > 0 {
				ck.elementIDs = append(ck.elementIDs, bytes.Clone(data.GetId()))
			}
		}
		// A RowWriteEffect is semantic evidence of a row-level write
		// on this key — flip readOnly so SSI's "any concurrent write
		// aborts" doesn't fire on write-carrying txs. The refined
		// predicate check in checkCompetingBinds / classifyNackTip
		// handles conflict detection for these txs.
		if eff.GetRowWrite() != nil {
			ck.readOnly = false
		}
	}

	// Value compression (--compress): swap raw values for their compressed
	// form before the effect is owned, marshaled, cached, and broadcast —
	// the compressed form IS the effect everywhere downstream, and readers
	// inflate off the per-effect flag at the reduce boundary.
	if c.engine.compressValues {
		CompressEffectValues(eff)
	}

	// Own the byte slices before caching: handler-built effects carry
	// []byte fields that alias pooled parser buffers, which are recycled
	// after the command returns while the cached message lives on in the
	// vertex pool. Only those fields need copying — the message structs
	// are built fresh per call (Emit already mutated them in place above,
	// so sharing one across calls was never legal), so a full reflective
	// proto.Clone here just duplicated every submessage and payload.
	// A just-compressed raw value is already owned (fresh codec output).
	ownEffectBytes(eff)

	offset, notify, err := c.rawEmit(eff)
	if err != nil {
		// Clean up the contextKey if we just created it and it has no prior effects
		if len(ck.notifies) == 0 {
			delete(c.keys, key)
		}

		return err
	}

	// Update contextKey
	ck.lastOffset = offset
	ck.notifies = append(ck.notifies, notify)
	c.readOnly = false

	// Defer peer-subscriber tracking to Flush: the subscriber set lives on the
	// key's leaf, which updateIndex only creates at Flush time. Self-subscribes
	// are filtered inside addPeerSubscriber when applied.
	if sub := eff.GetSubscription(); sub != nil && !sub.GetEphemeral() {
		ck.subOps = append(ck.subOps, subOp{
			peer:        pb.NodeID(sub.GetSubscriberNodeId()),
			unsubscribe: sub.GetUnsubscribe(),
		})
	}

	return nil
}

// ownEffectBytes copies the []byte fields of eff that can alias
// caller-pooled memory. Engine-built kinds (TxnBind, Snapshot,
// Subscription, Serialization, Noop) own their memory already — a
// Snapshot's ReducedEffect may share slices with cached effects, but
// cache memory is immutable and never recycled, so aliasing it is safe.
func ownEffectBytes(eff *pb.Effect) {
	eff.Key = bytes.Clone(eff.GetKey())
	switch k := eff.GetKind().(type) {
	case *pb.Effect_Data:
		d := k.Data
		d.Reference = bytes.Clone(d.GetReference())

		d.Id = bytes.Clone(d.GetId())
		if v, ok := d.GetValue().(*pb.DataEffect_Raw); ok {
			v.Raw = bytes.Clone(v.Raw)
		}
		// A compressed arm needs no clone: it only exists as fresh codec
		// output from CompressEffectValues, never as pooled parser memory.
	case *pb.Effect_Meta:
		k.Meta.ElementId = bytes.Clone(k.Meta.GetElementId())
	case *pb.Effect_PubsubMessage:
		m := k.PubsubMessage
		m.Channel = bytes.Clone(m.GetChannel())
		m.Payload = bytes.Clone(m.GetPayload())
	case *pb.Effect_RowWrite:
		rw := k.RowWrite

		rw.Pk = bytes.Clone(rw.GetPk())
		for _, col := range rw.GetColumns() {
			col.BlobVal = bytes.Clone(col.GetBlobVal())
		}
	case *pb.Effect_Observation:
		ownPredicateBytes(k.Observation.GetPredicate())
	}
}

func ownPredicateBytes(p *pb.Predicate) {
	if p == nil {
		return
	}

	if p.GetLiteral() != nil {
		p.Literal.BlobVal = bytes.Clone(p.GetLiteral().GetBlobVal())
	}

	ownPredicateBytes(p.GetChild())

	for _, c := range p.GetChildren() {
		ownPredicateBytes(c)
	}
}

// applySubOps applies a key's deferred peer-subscriber mutations after its leaf
// has been created by updateIndex.
func (c *Context) applySubOps(key string, ck *contextKey) {
	for _, op := range ck.subOps {
		if op.unsubscribe {
			c.engine.removePeerSubscriber(key, op.peer)
		} else {
			c.engine.addPeerSubscriber(key, op.peer)
		}
	}
}

// rawEmit is the core write path: serialize, write to log, cache, and
// build notify. The caller is responsible for setting all fields on the
// effect (Key, Hlc, NodeId, ForkChoiceHash, TxnId, Deps, Kind).
// Returns the log offset and the constructed OffsetNotify.
func (c *Context) rawEmit(eff *pb.Effect) (Tip, *pb.OffsetNotify, error) {
	offset := c.engine.nextOffset()

	if slog.Default().Enabled(c.TraceCtx(), slog.LevelDebug) {
		slog.Debug("Emit: wrote effect",
			"key", string(eff.GetKey()), "offset", offset, "deps", eff.GetDeps(), "tx", eff.GetTxnId() != "")
	}

	// Marshal once and reuse the length for the pool's byte accounting (avoids
	// a redundant proto.Size walk) and the bytes for the notify. The standalone
	// (no-broadcaster) path has nothing to marshal, so it falls back to Put.
	if c.engine.broadcaster == nil {
		if c.engine.effectCache != nil {
			if err := c.engine.effectCache.Put(offset, eff); err != nil {
				return Tip{}, nil, err
			}
		}

		c.engine.fireLocalEffect(offset, eff)

		return offset, nil, nil
	}

	data, err := c.engine.effectCache.MarshalEffect(eff)
	if err != nil {
		return Tip{}, nil, err
	}

	if c.engine.effectCache != nil {
		if err := c.engine.effectCache.PutSized(offset, eff, len(data)); err != nil {
			return Tip{}, nil, err
		}
	}

	c.engine.fireLocalEffect(offset, eff)

	notify := BuildOffsetNotify(c.engine.nodeID, offset, eff, data, c.TraceCtx())

	return offset, notify, nil
}

// Flush updates the index for all touched keys and broadcasts all
// notifications per key for durability, then resets the context for reuse.
func (c *Context) Flush() error {
	defer c.engine.unpinHistory(&c.history)

	if c.readErr != nil {
		err := c.readErr
		c.Abort()

		return err
	}

	if tracing.Enabled() {
		_, flushSpan := tracing.Tracer().Start(c.TraceCtx(), "effects.flush",
			trace.WithAttributes(attribute.Int("flush.keys", len(c.keys))))
		defer flushSpan.End()
	}

	// Pace only contexts that published effects. An empty/read-only flush must
	// not wait behind write pressure. Publication finishes before the wait.
	wrote := len(c.keys) != 0

	var err error
	if !c.inTx {
		err = c.flushNonTx()
	} else {
		err = c.flushTx()
	}

	if wrote {
		c.engine.backpressure(c.TraceCtx())
	} else {
		// Reads can fetch/cache effects too; keep background reclamation
		// prompt without making readers join the write-pressure wait.
		c.engine.wakeMemoryGovernor()
	}

	return err
}

// flushNonTx is the original Flush body for non-transactional writes.
func (c *Context) flushNonTx() error {
	if slog.Default().Enabled(c.TraceCtx(), slog.LevelDebug) {
		slog.Debug("Flush: non-tx", "keys", len(c.keys))
	}

	for key, ck := range c.keys {
		// Handle flush-all: wipe index before broadcasting
		if key == FlushKey {
			slog.Info("Flush: flush-all effect, wiping index")

			if err := c.engine.FlushIndex(); err != nil {
				return err
			}
			// Broadcast to peers but skip index update (already wiped)
			if c.engine.broadcaster != nil {
				for _, n := range ck.notifies {
					c.engine.broadcaster.BroadcastWithData(n, n.GetEffectData())
				}
			}
			// Fire OnFlushAll callback
			if c.engine.OnFlushAll != nil {
				c.engine.OnFlushAll()
			}

			delete(c.keys, key)

			continue
		}

		mode := c.engine.modeForKey(key)

		if tracing.Enabled() {
			_, idxSpan := tracing.Tracer().Start(c.TraceCtx(), "flush.update_index",
				trace.WithAttributes(attribute.String("effect.key", key)))
			defer idxSpan.End()
		}

		slog.Debug("Flush: updating index", "key", key, "offset", ck.lastOffset)
		c.engine.updateIndex(key, ck.initialTips, ck.lastOffset)
		c.applySubOps(key, ck)

		if c.engine.broadcaster != nil {
			var bcastTrace []byte

			if tracing.Enabled() {
				bcastCtx, bcastSpan := tracing.Tracer().Start(c.TraceCtx(), "flush.broadcast",
					trace.WithAttributes(
						attribute.String("effect.key", key),
						attribute.Int("flush.mode", int(mode)),
						attribute.Int("flush.notifies", len(ck.notifies)),
					))
				defer bcastSpan.End()

				bcastTrace = tracing.InjectIntoBytes(bcastCtx)
			}

			for _, n := range ck.notifies {
				n.TraceContext = bcastTrace
			}
			// SafeMode downgrade: if no other node in the cluster holds this
			// key — and we're in the majority partition, so the filter view is
			// authoritative — the first-ACK wait has no target. Every peer
			// would NACK NotSubscribed and discard the write, so the wait is
			// pure latency ending in a swallowed error. Broadcast async
			// instead. This is the write-side mirror of the read-only
			// fast-miss: a key that exists nowhere needs no synchronous
			// handshake. Once any peer subscribes to the key,
			// clusterMaybeHasKey turns true and SafeMode resumes waiting.
			if mode == SafeMode &&
				c.engine.inMajorityPartition() &&
				!c.engine.clusterMaybeHasKey(key) {
				mode = UnsafeMode
			}

			if mode == SafeMode {
				// Pre-check already verified all peers are reachable.
				// Index is updated, so the write is committed — replicate
				// but don't fail the client if replication errors out.
				// Intermediate effects are fire-and-forget; send them
				// concurrently so they don't serialize QUIC stream opens
				// before the blocking Replicate on the final effect.
				for _, n := range ck.notifies[:len(ck.notifies)-1] {
					go c.engine.broadcaster.BroadcastWithData(n, n.GetEffectData())
				}

				last := ck.notifies[len(ck.notifies)-1]
				if err := c.engine.broadcaster.Replicate(last, last.GetEffectData()); err != nil {
					if len(c.engine.broadcaster.PeerIDs()) == 0 {
						slog.Debug("SafeMode replication skipped: no peers", "key", key, "error", err)
					} else {
						slog.Warn("SafeMode replication failed after commit", "key", key, "error", err)
					}
				}
			} else {
				for _, n := range ck.notifies {
					c.engine.broadcaster.BroadcastWithData(n, n.GetEffectData())
				}
			}
		}

		// Fire notification callbacks after effect is durable
		if ck.shouldNotifyData {
			c.engine.ownFilterAdd(key)

			if c.engine.OnKeyDataAdded != nil {
				c.engine.OnKeyDataAdded(key)
			}
		}

		if ck.shouldNotifyDelete && c.engine.OnKeyDeleted != nil {
			c.engine.OnKeyDeleted(key)
		}

		delete(c.keys, key)
	}

	return nil
}

// flushTx implements the transactional commit protocol.
func (c *Context) flushTx() error {
	slog.Debug("Flush: transactional commit", "keys", len(c.keys))
	c.inTx = false

	c.txSnapshot = nil
	if len(c.keys) == 0 {
		return nil
	}

	// SafeMode pre-check: transactions block when in a minority partition.
	// Commutative (non-tx) writes are allowed — they merge on reconnect.
	if c.engine.broadcaster != nil {
		for key := range c.keys {
			if c.engine.modeForKey(key) == SafeMode {
				if !c.engine.broadcaster.InMajorityPartition() {
					c.reset()

					return ErrRegionPartitioned
				}

				break // one check is enough — reachability is per-region, not per-key
			}
		}
	}

	// Step 0.5: Ensure subscription for every key in the transaction.
	// Per whitepaper §3.3, a node must subscribe before any read or write.
	// If we're in a minority partition, ensureSubscribed returns an error
	// and the transaction must abort. Force the blocking bootstrap: the
	// commit protocol's fork-choice base-sharing requires the round-2 tip
	// collection to finish before the bind is emitted, so the txn path must
	// not take the async-announce shortcut even for a key held nowhere.
	for key := range c.keys {
		if err := c.engine.ensureSubscribedBlocking(key); err != nil {
			c.reset()

			return ErrRegionPartitioned
		}
	}

	// A changed frontier can contain only observations or compaction. Validate
	// writes, not tip identity; retain the promises made for consumed binds.
	var spokenBinds []Tip

	for key, ck := range c.keys {
		if c.engine.hasWritesSince(key, ck.initialTips.Tips(), c.txnID) {
			slog.Debug("flushTx: write after observation, aborting", "key", key, "txn", c.txnID)

			for _, pending := range c.keys {
				c.engine.pendingTxTips.Delete(pending.lastOffset)
			}

			c.reset()

			return ErrTxnAborted
		}

		if c.engine.spokenBinds != nil {
			for _, t := range ck.initialTips.Tips() {
				if binds, spoken := c.engine.spokenBinds.Get(t, 0); spoken {
					spokenBinds = append(spokenBinds, binds.snapshot()...)
				}
			}
		}
	}

	if len(spokenBinds) > 0 {
		// Reuse the NACK conflict check's DAG walk over our actual emitted
		// dependencies. Including P alone is not enough: every known consumer
		// of P must be in our past, even if ingestion has not indexed it yet.
		candidate := &pendingTxn{keys: make([]pendingTxnKey, 0, len(c.keys))}
		for _, ck := range c.keys {
			candidate.keys = append(candidate.keys, pendingTxnKey{newTip: ck.lastOffset})
		}

		if !c.engine.reachesFromTx(candidate, spokenBinds...) {
			slog.Debug("flushTx: pre-flight aborting, spoken bind is not an ancestor",
				"binds", spokenBinds)

			for _, ck := range c.keys {
				c.engine.pendingTxTips.Delete(ck.lastOffset)
			}

			c.reset()

			return ErrTxnAborted
		}
	}

	// Step 1: Update index + broadcast individual effects per key
	for key, ck := range c.keys {
		c.engine.updateIndex(key, ck.initialTips, ck.lastOffset)
		c.applySubOps(key, ck)

		// Register as pending tx tip
		var preTxDeps []Tip
		if ck.initialTips != nil {
			preTxDeps = ck.initialTips.Tips()
		}

		if eff, _ := c.engine.effectCache.Get(ck.lastOffset); eff.GetNoop() == nil {
			c.engine.pendingTxTips.Store(ck.lastOffset, preTxDeps)
		}

		if c.engine.broadcaster != nil {
			for _, n := range ck.notifies {
				c.engine.broadcaster.BroadcastWithData(n, n.GetEffectData())
			}
		}
	}

	// Build the bind. Other tentative tips on the frontier are not abort
	// evidence: competing binds are adjudicated below and by peer responses.
	txnHLC := c.engine.clock.Now()
	hlcTs := timestamppb.New(txnHLC)
	bind := &pb.TransactionalBindEffect{
		TxnHlc:           timestamppb.New(txnHLC),
		OriginatorNodeId: uint64(c.engine.nodeID),
	}

	for key, ck := range c.keys {
		var consumedTips []*pb.EffectRef
		if ck.initialTips != nil {
			// Match the data effect's value base, retaining causal observations
			// while substituting tentative writes. A consumed NOOP does not
			// imply that its transaction's future bind has been observed.
			consumedTips = toPbRefs(c.engine.resolveTipDeps(ck.initialTips.Tips()))
		}

		kb := &pb.TransactionalBindEffect_KeyBind{
			Key:          []byte(key),
			ConsumedTips: consumedTips,
			NewTip:       toPbRef(ck.lastOffset),
		}
		bind.Keys = append(bind.Keys, kb)
	}
	// Sort bind.Keys by key bytes so the canonical key (bind.Keys[0])
	// and every observer's iteration order are deterministic — c.keys
	// is a Go map, raw iteration is random. Any predicate or behavior
	// that branches on eff.Key (or on bind.Keys ordering) needs this
	// to be stable across runs.
	sort.Slice(bind.GetKeys(), func(i, j int) bool {
		return bytes.Compare(bind.GetKeys()[i].GetKey(), bind.GetKeys()[j].GetKey()) < 0
	})

	// Bind deps: the last effect on each key in the transaction.
	// Walk bind.Keys (already sorted) so eff.Deps is deterministic too.
	bindDeps := make([]*pb.EffectRef, 0, len(bind.GetKeys()))
	for _, kb := range bind.GetKeys() {
		bindDeps = append(bindDeps, kb.GetNewTip())
	}

	// Serialize the final conflict check, publication on every key, and pending
	// registration with incoming bind responses. Earlier preflight checks alone
	// cannot protect this boundary: a peer may have received our response since.
	nodeLock := c.takeNodeLock()
	defer nodeLock.release()

	// Recheck writes at the publication boundary too.
	for key, ck := range c.keys {
		if c.engine.hasWritesSince(key, ck.initialTips.Tips(), c.txnID) {
			slog.Debug("flushTx: write after observation, aborting", "key", key, "txn", c.txnID)

			for _, pending := range c.keys {
				c.engine.pendingTxTips.Delete(pending.lastOffset)
			}

			c.reset()

			return ErrTxnAborted
		}
	}

	// Every previously ingested structural competitor counts, including binds
	// still in the horizon. We cannot publish after promising not to compete.
	if conflict := c.engine.checkCompetingBinds(bind, c.txnID); conflict != "" {
		slog.Debug("flushTx: competing bind found, aborting before emission",
			"competing_txn", conflict)

		for key, ck3 := range c.keys {
			if c.engine.horizon == nil {
				c.engine.pendingTxTips.Delete(ck3.lastOffset)
			}
			// Check serialization escalation (only if no leader already active)
			if c.engine.CheckSerializationLeader(key) == nil && c.engine.incrementAbortCount(key) {
				slog.Info("adaptive serialization: abort threshold exceeded",
					"key", key, "threshold", DefaultSerializationThreshold)
				c.emitSerializationEffect(key)
			}
		}

		c.reset()

		return ErrTxnAborted
	}

	// One hash, reused for the bind effect, the void check, and the pending
	// txn record — all keyed on (nodeID, hlcTs), so recomputing would be waste.
	bindHash := ComputeForkChoiceHash(c.engine.nodeID, hlcTs)
	bindEff := &pb.Effect{
		Key:            bind.GetKeys()[0].GetKey(), // use first key as canonical
		Hlc:            hlcTs,
		NodeId:         uint64(c.engine.nodeID),
		ForkChoiceHash: bindHash,
		TxnId:          c.txnID,
		Deps:           bindDeps,
		Kind:           &pb.Effect_TxnBind{TxnBind: bind},
	}

	// Use rawEmit: the bind bypasses Emit's dep/context tracking because
	// it must not update any contextKey's lastOffset (which would corrupt
	// pending txn tracking). rawEmit handles log write, effect cache, and
	// notify construction.
	bindOffset, bindNotify, err := c.rawEmit(bindEff)
	if err != nil {
		return err
	}

	// Index bind under ALL keys it touches, consuming the DATA tip that
	// Step 1 indexed (ck.lastOffset). This replaces the tentative DATA
	// tip with the BIND, preventing tip accumulation across transactions.
	// Concurrent tips from other writers are preserved (not consumed).
	for _, kb := range bind.GetKeys() {
		key := string(kb.GetKey())
		ck := c.keys[key]
		c.engine.updateIndex(key, keytrie.NewTipSet(ck.lastOffset), bindOffset)
	}

	// Step 4: Create pending txn for NACK tracking
	ptxn := &pendingTxn{
		txnID:          c.txnID,
		txnHLC:         txnHLC,
		originNode:     c.engine.nodeID,
		forkChoiceHash: bindHash,
		bindOffset:     bindOffset,
		done:           make(chan struct{}),
		resolved:       make(chan struct{}),
	}
	defer close(ptxn.resolved)

	for key, ck := range c.keys {
		var consumedTips []Tip
		if ck.initialTips != nil {
			// Same substitution as the cluster-visible bind above, so the
			// local NACK-conflict view matches what peers see.
			consumedTips = c.engine.resolveTipDeps(ck.initialTips.Tips())
		}

		ptxn.keys = append(ptxn.keys, pendingTxnKey{
			key:          key,
			consumedTips: consumedTips,
			newTip:       ck.lastOffset,
			readOnly:     ck.readOnly,
			collection:   ck.collection,
			elementIDs:   ck.elementIDs,
		})
	}

	c.engine.pendingTxns.Store(bindOffset, ptxn)
	nodeLock.release()

	// Step 5: Gather subscribers and replicate bind

	if c.engine.broadcaster != nil {
		subscribers := c.collectSubscribers()
		slog.Debug("flushTx: replicating bind",
			"bind_offset", bindOffset,
			"subscribers", len(subscribers))

		if len(subscribers) == 0 {
			// Single-node fast path: commit immediately. No
			// horizon.Add needed — the bind has no peers to wait
			// on. Adding it would have made our own in-flight
			// bind invisible to the collectSubscribers reconstruct
			// above, causing the stale-tip pruning to yank the
			// bind offset out of the index before we ever got a
			// chance to commit it.
			slog.Debug("flushTx: no subscribers, committing immediately",
				"bind_offset", bindOffset)
			ptxn.finish(txnStateCommitted)
		} else {
			// Peers exist — bind stays invisible until we finish processing
			// peer responses and explicitly call MakeVisible (commit) or
			// Abort (abort) below. The decision must happen AFTER the NACK
			// loop, otherwise the bind can briefly become visible while a
			// concurrent reader picks up its effects before isRealConflict
			// has had a chance to fire.
			if c.engine.horizon != nil {
				c.engine.horizon.Add(c.txnID, bindOffset, bind)
			}
			// Replicate to all subscribers concurrently, collect responses.
			// Per the TLA+ spec (ExactlyOnce.CommitPop): commit requires
			// every subscriber to respond (ACK or NACK). A peer that ACK'd
			// has committed via the "once you've spoken" invariant to NACK
			// any subsequent competing bind on this key. A peer we couldn't
			// reach hasn't made that commitment, so we cannot rely on it
			// to enforce AtMostOneCommit.
			type replicaResult struct {
				subID pb.NodeID
				nacks []*pb.NackNotify
				err   error
			}

			// Marshal the bind notify once: the proto body is identical for
			// every subscriber (only the per-peer request header differs), so
			// re-marshalling per goroutine is wasted work — and would race on
			// the shared bindNotify. The fan-out reuses this single body.
			bindBody, marshalErr := proto.Marshal(bindNotify)
			if marshalErr != nil {
				return fmt.Errorf("flushTx: marshal bind notify: %w", marshalErr)
			}

			results := make([]replicaResult, len(subscribers))

			var wg sync.WaitGroup
			wg.Add(len(subscribers))

			for i, subID := range subscribers {
				go func(idx int, target pb.NodeID) {
					defer wg.Done()

					slog.Debug("flushTx: ReplicateTo",
						"bind_offset", bindOffset,
						"target", target)
					nacks, err := c.engine.broadcaster.ReplicateMarshalled(bindNotify, bindBody, target)
					results[idx] = replicaResult{subID: target, nacks: nacks, err: err}
				}(i, subID)
			}

			wg.Wait()

			responseCount := 0

			for _, res := range results {
				if res.err != nil {
					slog.Warn("flushTx: ReplicateTo failed", "bind_offset", bindOffset, "target", res.subID, "error", res.err)

					continue
				}

				responseCount++

				ptxn.responses = append(ptxn.responses, res.nacks...)
			}
			// Ingest all responses before evaluating: the origin of a better
			// priority bind may supply evidence absent from earlier replies.
			for _, nack := range ptxn.responses {
				c.engine.ingestNackTips(nack)
			}

			var (
				defeatedSet map[string]Tip
				proof       *pb.BindEvidence
			)

			decision := txnStateAmbiguous

			if responseCount == len(subscribers) {
				var err error

				defeatedSet, err = c.engine.evaluateNacks(ptxn, ptxn.responses)
				if err == nil {
					proof, err = c.engine.bindProof(bindOffset, defeatedSet, ptxn.proofSources)
					defer func() { c.engine.effectCache.releaseEvidence(evidenceTips(proof)) }()
				}

				switch {
				case errors.Is(err, ErrTxnAborted):
					decision = txnStateAborted
				case err != nil:
					decision = txnStateAmbiguous
				default:
					decision = txnStateCommitted
				}

				slog.Debug("flushTx: priority evaluation", "bind_offset", bindOffset, "state", decision, "error", err)
			}

			if decision == txnStateCommitted {
				verdicts := make(map[string]pb.Verdict, 1+len(defeatedSet))
				verdicts[c.txnID] = pb.Verdict_WON

				// Cache the computed outcomes on our keys and the defeated
				// binds' keys. Readers and deferred responses can reuse this
				// evidence without rediscovering cross-key competitors.

				emitKeys := make(map[string]struct{}, len(bind.GetKeys()))
				for _, kb := range bind.GetKeys() {
					emitKeys[string(kb.GetKey())] = struct{}{}
				}

				for beatenTxn, beatenOffset := range defeatedSet {
					verdicts[beatenTxn] = pb.Verdict_LOST

					loserEff, err := c.engine.getEffect("", beatenOffset)
					if err != nil {
						slog.Warn("flushTx: cannot fetch beaten bind for verdict emit",
							"loser_txn", beatenTxn,
							"loser_offset", beatenOffset,
							"bind_offset", bindOffset,
							"error", err)

						continue
					}

					loserBind := loserEff.GetTxnBind()
					if loserBind == nil {
						slog.Warn("flushTx: beaten effect is not a TxnBind",
							"loser_txn", beatenTxn,
							"loser_offset", beatenOffset,
							"bind_offset", bindOffset)

						continue
					}

					for _, kb := range loserBind.GetKeys() {
						emitKeys[string(kb.GetKey())] = struct{}{}
					}
				}

				for key := range emitKeys {
					if err := c.engine.emitSnapshot(key, verdicts, proof); err != nil {
						decision = txnStateAmbiguous

						slog.Warn("flushTx: emitSnapshot for verdict failed",
							"key", key,
							"bind_offset", bindOffset,
							"error", err)
					}
				}
			}
			// Evidence ownership must be published before local completion and
			// before deferred priority responses can use this outcome.
			ptxn.finish(decision)

			if c.engine.horizon != nil {
				if decision == txnStateCommitted {
					c.engine.horizon.MakeVisible(c.txnID)
				} else {
					c.engine.horizon.Abort(c.txnID)
				}
			}
		}
	} else {
		// Standalone: commit immediately
		ptxn.finish(txnStateCommitted)
	}

	// Step 6: Process result
	state := ptxn.state.Load()

	c.engine.pendingTxns.Delete(bindOffset)

	if state != txnStateCommitted {
		// Release local pending state. Horizon.Abort already did this when
		// enabled; neither path retracts the published bind from peers.
		if c.engine.horizon == nil {
			for _, pk := range ptxn.keys {
				c.engine.pendingTxTips.Delete(pk.newTip)
			}
		}

		if state == txnStateAmbiguous {
			slog.Debug("Flush: transaction outcome unknown", "bind_offset", bindOffset)
			c.reset()

			return ErrTxnAmbiguous
		}

		slog.Debug("Flush: transaction aborted", "bind_offset", bindOffset)

		// Check serialization escalation (only if no leader already active)
		for _, pk := range ptxn.keys {
			if c.engine.CheckSerializationLeader(pk.key) == nil && c.engine.incrementAbortCount(pk.key) {
				slog.Info("adaptive serialization: abort threshold exceeded",
					"key", pk.key, "threshold", DefaultSerializationThreshold)
				c.emitSerializationEffect(pk.key)
			}
		}

		c.reset()

		return ErrTxnAborted
	}

	slog.Debug("Flush: transaction committed", "bind_offset", bindOffset)
	// Committed: clean up pending tx tips, reset abort counts.
	// When horizon is active, MakeVisible (fast-path or timer) handles
	// pendingTxTips cleanup.
	for _, pk := range ptxn.keys {
		if c.engine.horizon == nil {
			c.engine.pendingTxTips.Delete(pk.newTip)
		}

		c.engine.resetAbortCount(pk.key)
	}

	// Fire notification callbacks after successful commit
	for key, ck := range c.keys {
		if ck.shouldNotifyData {
			c.engine.ownFilterAdd(key)

			if c.engine.OnKeyDataAdded != nil {
				c.engine.OnKeyDataAdded(key)
			}
		}

		if ck.shouldNotifyDelete && c.engine.OnKeyDeleted != nil {
			c.engine.OnKeyDeleted(key)
		}
	}

	c.reset()

	return nil
}

// emitSerializationEffect emits a serialization request for a key without
// a transaction so it becomes visible immediately. Under heavy contention
// the transactional path would abort or remain invisible behind pending
// tx tips, causing concurrent GetSnapshot calls to clear the leader.
// The leader is selected using RTT measurements when available.
func (c *Context) emitSerializationEffect(key string) {
	if !adaptiveSerializationEnabled {
		return
	}

	leader := c.engine.selectSerializationLeader(key)

	slog.Info("adaptive serialization: requesting leader",
		"key", key, "leader", leader, "self", c.engine.nodeID)

	ctx := c.engine.NewContext()
	if err := ctx.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Serialization{Serialization: &pb.SerializationEffect{
			LeaderNodeId: uint64(leader),
		}},
	}); err != nil {
		slog.Error("failed to emit serialization effect", "key", key, "error", err)

		return
	}

	if err := ctx.Flush(); err != nil {
		slog.Debug("serialization effect flush failed",
			"key", key, "error", err)
	}
}

// reset clears the context for reuse.
func (c *Context) reset() {
	c.engine.unpinHistory(&c.history)
	// Reuse small command maps, but let an unusually large transaction's map
	// go: clear alone retains its buckets for the lifetime of the context.
	if len(c.keys) > 16 {
		c.keys = nil
	} else {
		clear(c.keys)
	}

	c.inTx = false
	c.txnID = ""
	c.txSnapshot = nil
	c.readErr = nil
}

// updateIndex performs a CAS loop to merge the new offset into the
// index, preserving any tips added by concurrent writers.
// After a successful CAS, records the transition in the Tip recovery log.
func (e *Engine) updateIndex(key string, initialTips *keytrie.TipSet, lastOffset Tip) {
	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			slog.Debug("updateIndex: CAS retry", "key", key, "attempt", attempt)
		}

		current := e.index.Contains(key)

		var offsets []Tip

		if current != nil {
			for _, tp := range current.Tips() {
				if initialTips == nil || !initialTips.Contains(tp) {
					offsets = append(offsets, tp) // keep tips we didn't consume
				}
			}
		}

		offsets = append(offsets, lastOffset)

		newTips := keytrie.NewTipSet(offsets...)
		if _, ok := e.index.Insert(key, current, newTips); ok {
			// Insert fires the trie's refDelta hook (applyRefDelta), which
			// increfs the added tips and decrefs the consumed ones — no manual
			// refcount call here.
			if current.Len() != newTips.Len() || !current.ContainsAll(newTips.Tips()) {
				e.notifyKeyChanged(key)
			}

			return
		}

		if e.index.Closed() {
			// Not a CAS race: Insert fails unconditionally on a closed trie,
			// so retrying would spin forever during shutdown.
			return
		}
	}
}

// BuildOffsetNotify constructs a single-effect notification.
// EffectData is wire format: [4-byte LE keyLen][key][protoData].
// traceCtx is optional; when non-nil, OTel trace context is injected into the notification.
func BuildOffsetNotify(nodeID pb.NodeID, offset Tip, eff *pb.Effect, data []byte, traceCtx context.Context) *pb.OffsetNotify {
	// Wire format: [4-byte LE keyLen][key][protoData]
	keyBytes := eff.GetKey()
	wireData := make([]byte, 4+len(keyBytes)+len(data))
	binary.LittleEndian.PutUint32(wireData[:4], uint32(len(keyBytes)))
	copy(wireData[4:4+len(keyBytes)], keyBytes)
	copy(wireData[4+len(keyBytes):], data)

	notify := &pb.OffsetNotify{
		Origin:     toPbRef(offset),
		Hlc:        eff.GetHlc(),
		Key:        bytes.Clone(eff.GetKey()),
		Deps:       eff.GetDeps(),
		EffectData: wireData,
	}

	if traceCtx != nil {
		notify.TraceContext = tracing.InjectIntoBytes(traceCtx)
	}

	return notify
}
