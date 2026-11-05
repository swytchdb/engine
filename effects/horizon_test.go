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
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	clox "github.com/swytchdb/engine/cache"
	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/crdt"
	"github.com/swytchdb/engine/keytrie"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func newHorizonTestEngine() *Engine {
	e := &Engine{
		effectCache:       newVertexPool(),
		index:             keytrie.NewCritbit[leafState](),
		nodeID:            1,
		clock:             crdt.NewHLC(),
		subscriptions:     xsync.NewMap[string, *subscriptionState](),
		pendingTxns:       xsync.NewMap[Tip, *pendingTxn](),
		pendingTxTips:     xsync.NewMap[Tip, []Tip](),
		txAbortCounts:     xsync.NewMap[string, *atomic.Int32](),
		pendingBootstraps: xsync.NewMap[string, *bootstrapCollector](),
		spokenBinds:       clox.NewCloxCache[Tip, *spokenBindHistory](clox.ConfigFromCapacity(256)),
	}
	e.safety.Store(&safetyMap{defaultMode: UnsafeMode})
	e.horizon = newHorizonSet(e)

	return e
}

// installFakeTimers replaces timer creation with a controllable fire function.
func installFakeTimers(h *HorizonSet) func() {
	var (
		mu  sync.Mutex
		fns []func()
	)

	h.afterFunc = func(d time.Duration, f func()) *time.Timer {
		mu.Lock()

		fns = append(fns, f)
		mu.Unlock()

		return time.NewTimer(time.Hour) // never fires
	}

	return func() {
		mu.Lock()
		defer mu.Unlock()

		for _, f := range fns {
			f()
		}

		fns = nil
	}
}

func TestHorizonSet_StandaloneEngineNilHorizon(t *testing.T) {
	e := &Engine{
		effectCache:       newVertexPool(),
		index:             keytrie.NewCritbit[leafState](),
		nodeID:            1,
		clock:             crdt.NewHLC(),
		subscriptions:     xsync.NewMap[string, *subscriptionState](),
		pendingTxns:       xsync.NewMap[Tip, *pendingTxn](),
		pendingTxTips:     xsync.NewMap[Tip, []Tip](),
		txAbortCounts:     xsync.NewMap[string, *atomic.Int32](),
		pendingBootstraps: xsync.NewMap[string, *bootstrapCollector](),
	}
	e.safety.Store(&safetyMap{defaultMode: UnsafeMode})

	if e.horizon != nil {
		t.Fatal("standalone engine should have nil horizon")
	}
}

func testBind(consumedTips []*pb.EffectRef, newTip *pb.EffectRef) *pb.TransactionalBindEffect {
	return &pb.TransactionalBindEffect{
		TxnHlc:           timestamppb.New(time.Unix(0, 100)),
		OriginatorNodeId: 1,
		Keys: []*pb.TransactionalBindEffect_KeyBind{
			{Key: []byte("k1"), ConsumedTips: consumedTips, NewTip: newTip},
		},
	}
}

// Add registers a bind as invisible. The originator's flushTx will later
// call MakeVisible or Abort.
func TestHorizonSet_AddMakesInvisible(t *testing.T) {
	e := newHorizonTestEngine()

	bind := testBind(
		[]*pb.EffectRef{{NodeId: 1, Offset: 10}},
		&pb.EffectRef{NodeId: 1, Offset: 20},
	)

	e.horizon.Add("tx1", Tip{1, 30}, bind)

	if !e.horizon.IsInvisible("tx1") {
		t.Fatal("tx1 should be invisible after Add")
	}
}

// MakeVisible promotes the entry and cleans pendingTxTips.
func TestHorizonSet_MakeVisibleRemovesEntry(t *testing.T) {
	e := newHorizonTestEngine()

	e.pendingTxTips.Store(Tip{1, 20}, []Tip{{1, 10}})

	bind := testBind(
		[]*pb.EffectRef{{NodeId: 1, Offset: 10}},
		&pb.EffectRef{NodeId: 1, Offset: 20},
	)

	e.horizon.Add("tx1", Tip{1, 30}, bind)
	e.horizon.MakeVisible("tx1")

	if e.horizon.IsInvisible("tx1") {
		t.Fatal("tx1 should be visible after MakeVisible")
	}

	if _, ok := e.pendingTxTips.Load(Tip{1, 20}); ok {
		t.Fatal("pendingTxTips should be cleaned after MakeVisible")
	}
}

// Abort removes the entry without making it visible.
func TestHorizonSet_AbortRemovesEntryWithoutPromotion(t *testing.T) {
	e := newHorizonTestEngine()

	e.pendingTxTips.Store(Tip{1, 20}, []Tip{{1, 10}})

	bind := testBind(
		[]*pb.EffectRef{{NodeId: 1, Offset: 10}},
		&pb.EffectRef{NodeId: 1, Offset: 20},
	)

	e.horizon.Add("tx1", Tip{1, 30}, bind)
	e.horizon.Abort("tx1")

	if e.horizon.IsInvisible("tx1") {
		t.Fatal("tx1 should no longer be in the invisible set after Abort")
	}

	if _, ok := e.pendingTxTips.Load(Tip{1, 20}); ok {
		t.Fatal("pendingTxTips should be cleaned after Abort")
	}
}

// applySnapshotVerdicts promotes every txn named in the verdict map out
// of horizon as soon as the originator's verdict snapshot arrives,
// without waiting for the crash-fallback timer.
func TestHorizonSet_VerdictSnapshotEndsWaitEarly(t *testing.T) {
	e := newHorizonTestEngine()
	installFakeTimers(e.horizon) // fake timer never fires

	e.pendingTxTips.Store(Tip{1, 20}, []Tip{{1, 10}})
	e.pendingTxTips.Store(Tip{1, 40}, []Tip{{1, 10}})

	winnerBind := testBind(
		[]*pb.EffectRef{{NodeId: 1, Offset: 10}},
		&pb.EffectRef{NodeId: 1, Offset: 20},
	)
	loserBind := testBind(
		[]*pb.EffectRef{{NodeId: 1, Offset: 10}},
		&pb.EffectRef{NodeId: 1, Offset: 40},
	)

	e.horizon.Add("winner", Tip{1, 30}, winnerBind)
	e.horizon.Add("loser", Tip{1, 50}, loserBind)
	e.horizon.ScheduleMakeVisible("winner", time.Hour)
	e.horizon.ScheduleMakeVisible("loser", time.Hour)

	if !e.horizon.IsInvisible("winner") || !e.horizon.IsInvisible("loser") {
		t.Fatal("both txns should be invisible before snapshot arrives")
	}

	verdictEff := &pb.Effect{
		Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
			TxnVerdicts: map[string]pb.Verdict{
				"winner": pb.Verdict_WON,
				"loser":  pb.Verdict_LOST,
			},
		}},
	}
	e.applySnapshotVerdicts(verdictEff)

	if e.horizon.IsInvisible("winner") {
		t.Fatal("winner should be visible after verdict snapshot")
	}

	if e.horizon.IsInvisible("loser") {
		t.Fatal("loser should be visible after verdict snapshot (DAG/snapshot will filter it on reconstruct)")
	}

	if _, ok := e.pendingTxTips.Load(Tip{1, 20}); ok {
		t.Fatal("winner pendingTxTips should be cleaned after MakeVisible")
	}

	if _, ok := e.pendingTxTips.Load(Tip{1, 40}); ok {
		t.Fatal("loser pendingTxTips should be cleaned after MakeVisible")
	}
}

// applySnapshotVerdicts is a no-op when the txn isn't in horizon (e.g.,
// snapshot arrives before its bind, or bind was already promoted).
func TestHorizonSet_VerdictSnapshotIsIdempotent(t *testing.T) {
	e := newHorizonTestEngine()

	verdictEff := &pb.Effect{
		Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
			TxnVerdicts: map[string]pb.Verdict{"unknown": pb.Verdict_WON},
		}},
	}
	e.applySnapshotVerdicts(verdictEff) // must not panic
}

// ScheduleMakeVisible's timer fires MakeVisible (remote-arrival path).
func TestHorizonSet_ScheduledTimerFiresMakesVisible(t *testing.T) {
	e := newHorizonTestEngine()
	fireTimer := installFakeTimers(e.horizon)

	e.pendingTxTips.Store(Tip{1, 20}, []Tip{{1, 10}})

	bind := testBind(
		[]*pb.EffectRef{{NodeId: 1, Offset: 10}},
		&pb.EffectRef{NodeId: 1, Offset: 20},
	)

	e.horizon.Add("tx1", Tip{1, 30}, bind)
	e.horizon.ScheduleMakeVisible("tx1", 100*time.Millisecond)

	if !e.horizon.IsInvisible("tx1") {
		t.Fatal("tx1 should be invisible before timer fires")
	}

	fireTimer()

	if e.horizon.IsInvisible("tx1") {
		t.Fatal("tx1 should be visible after timer fires")
	}

	if _, ok := e.pendingTxTips.Load(Tip{1, 20}); ok {
		t.Fatal("pendingTxTips should be cleaned after timer fires")
	}
}

func TestHorizonSet_FallbackSurvivesMergeAndAbort(t *testing.T) {
	for _, abort := range []bool{false, true} {
		name := "merge"
		if abort {
			name = "merge_and_abort"
		}

		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				e := newHorizonTestEngine()
				h := e.horizon
				baseA := &pb.EffectRef{NodeId: 1, Offset: 10}
				baseB := &pb.EffectRef{NodeId: 1, Offset: 20}
				h.Add("local", Tip{1, 31}, testBind([]*pb.EffectRef{baseA}, &pb.EffectRef{NodeId: 1, Offset: 30}))
				h.Add("remote", Tip{1, 41}, testBind([]*pb.EffectRef{baseB}, &pb.EffectRef{NodeId: 1, Offset: 40}))
				h.ScheduleMakeVisible("remote", time.Second)

				// The local bridge merges the remote group into the untimed local group.
				time.Sleep(500 * time.Millisecond)
				h.Add("bridge", Tip{1, 51}, testBind([]*pb.EffectRef{baseA, baseB}, &pb.EffectRef{NodeId: 1, Offset: 50}))

				if abort {
					h.Abort("remote")
				}

				done := make(chan struct{})

				go func() {
					h.WaitForClear("local")
					close(done)
				}()

				synctest.Wait()

				if !h.IsInvisible("local") || !h.IsInvisible("bridge") {
					t.Fatal("merged entries became visible before the fallback deadline")
				}

				time.Sleep(500 * time.Millisecond)
				synctest.Wait()

				if !h.Empty() {
					// Release the waiter even when testing a broken implementation.
					h.MakeVisible("local")
					t.Fatal("merged group remained invisible after the original fallback deadline")
				}

				select {
				case <-done:
				default:
					t.Fatal("fallback did not release the merged group's waiter")
				}
			})
		})
	}
}

func TestHorizonSet_FallbackSurvivesSchedulingEntryAbort(t *testing.T) {
	e := newHorizonTestEngine()
	fireTimer := installFakeTimers(e.horizon)
	bind := testBind([]*pb.EffectRef{{NodeId: 1, Offset: 10}}, &pb.EffectRef{NodeId: 1, Offset: 20})
	e.horizon.Add("remote", Tip{1, 30}, bind)
	e.horizon.Add("local", Tip{1, 40}, bind)
	e.horizon.ScheduleMakeVisible("remote", time.Second)
	e.horizon.Abort("remote")
	fireTimer()

	if !e.horizon.Empty() {
		t.Fatal("aborting the scheduling entry disabled the group's fallback")
	}
}

func TestHorizonSet_FallbackIgnoresStoppedCallbacks(t *testing.T) {
	e := newHorizonTestEngine()
	h := e.horizon

	var callbacks []func()

	h.afterFunc = func(_ time.Duration, f func()) *time.Timer {
		callbacks = append(callbacks, f)

		return time.NewTimer(time.Hour)
	}
	bind := testBind([]*pb.EffectRef{{NodeId: 1, Offset: 10}}, &pb.EffectRef{NodeId: 1, Offset: 20})
	h.Add("remote", Tip{1, 30}, bind)
	h.ScheduleMakeVisible("remote", time.Second)
	h.ScheduleMakeVisible("remote", 2*time.Second)

	// A stopped timer's callback may already be waiting for the lifecycle lock.
	callbacks[0]()

	if !h.IsInvisible("remote") {
		t.Fatal("replaced timer made the group visible before the extended deadline")
	}

	callbacks[1]()

	if !h.Empty() {
		t.Fatal("replacement timer did not make the group visible")
	}

	h.Add("remote", Tip{1, 30}, bind)
	callbacks[1]()

	if !h.IsInvisible("remote") {
		t.Fatal("completed group's timer made a redelivered transaction visible")
	}

	h.Abort("remote")
}
