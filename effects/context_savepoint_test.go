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
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/swytchdb/engine/durability"

	pb "github.com/swytchdb/engine/cluster/proto"
)

func TestNestedSavepointsPreserveSubscriptions(t *testing.T) {
	e := newHorizonTestEngine()
	c := e.NewContext()
	emit := func(peer uint64) {
		t.Helper()

		if err := c.Emit(&pb.Effect{
			Key: []byte("key"),
			Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{
				SubscriberNodeId: peer,
			}},
		}); err != nil {
			t.Fatal(err)
		}
	}

	for _, peer := range []uint64{2, 3, 4} {
		emit(peer)
	}

	first := c.TakeSavepoint()

	emit(5)

	second := c.TakeSavepoint()
	c.RestoreSavepoint(first)
	emit(6)
	c.RestoreSavepoint(second)

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	if got := e.PeerSubscribers("key"); !slices.Equal(got, []pb.NodeID{2, 3, 4, 5}) {
		t.Fatalf("restore changed saved subscription operations: %v", got)
	}

	// Restoring and flushing again must not mutate the retained savepoint.
	c.RestoreSavepoint(second)

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	if got := e.PeerSubscribers("key"); !slices.Equal(got, []pb.NodeID{2, 3, 4, 5}) {
		t.Fatalf("repeated restore changed subscription operations: %v", got)
	}
}

func TestContextReleasesLargeCommandState(t *testing.T) {
	e := newHorizonTestEngine()
	c := e.NewContext()
	empty := c.TakeSavepoint()

	// A large aborted command must not leave its map buckets on a reused
	// context. Subsequent emission must also work after an empty restore.
	for range 128 {
		if err := c.Emit(dataEffect(e.generateTxnID())); err != nil {
			t.Fatal(err)
		}
	}

	c.Abort()

	if c.keys != nil {
		t.Fatal("large aborted command retained its key map")
	}

	c.RestoreSavepoint(empty)

	if err := c.Emit(dataEffect("next")); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	if e.index.Contains("next") == nil {
		t.Fatal("reused context did not publish the next command")
	}
}

// These fixtures do not start the governor, so reclamation is deterministic.
func pendingTestEmit(t testing.TB, c *Context, key string) Tip {
	t.Helper()
	preSubscribe(c.engine, key)

	if err := c.Emit(scalarSet(key, "value")); err != nil {
		t.Fatal(err)
	}

	return c.keys[logicalAddress(key)].lastOffset
}

func TestSavepointRollbackReclaimsOnlyDiscardedEffects(t *testing.T) {
	e := newHorizonTestEngine()
	c := e.NewContext()
	first := pendingTestEmit(t, c, "keep")
	saved := c.TakeSavepoint()

	discarded := make([]Tip, 0, 101)
	for i := range 100 {
		discarded = append(discarded, pendingTestEmit(t, c, fmt.Sprintf("discard-%d", i%3)))
	}
	// Include a later effect on the retained key, not only newly introduced keys.
	discarded = append(discarded, pendingTestEmit(t, c, "keep"))
	c.RestoreSavepoint(saved)

	for _, tip := range discarded {
		v := e.effectCache.getVertex(tip)
		if v == nil || v.refs.Load() != 0 || len(v.creationKeys) != 0 {
			t.Fatalf("discarded effect still owned: %v, %v", tip, v)
		}
	}

	e.effectCache.drainReclaim()

	if e.effectCache.EntryCount() != 1 {
		t.Fatalf("retained %d vertices, want one", e.effectCache.EntryCount())
	}

	if c.keys[logicalAddress("keep")].lastOffset != first || len(c.keys) != 1 {
		t.Fatal("rollback did not preserve earlier pending state")
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	e.effectCache.drainReclaim()

	if got := e.index.Contains("keep").Tips(); !slices.Equal(got, []Tip{first}) {
		t.Fatalf("wrong committed tips: %v", got)
	}

	runtime.KeepAlive(saved)
}

func TestPendingHistoryReclaimsOnAbortAndFailedFlush(t *testing.T) {
	for _, failure := range []string{"abort", "read", "preflight"} {
		t.Run(failure, func(t *testing.T) {
			e := newTestEngine(nil)
			c := e.NewContext()
			c.BeginTx()

			for range 10 {
				pendingTestEmit(t, c, "key")
			}

			switch failure {
			case "abort":
				c.Abort()
			case "read":
				want := errors.New("read failed")

				c.readErr = want
				if err := c.Flush(); !errors.Is(err, want) {
					t.Fatalf("flush: %v", err)
				}
			case "preflight":
				e.safety.Store(&safetyMap{defaultMode: SafeMode})
				e.broadcaster = &mockBroadcaster{}

				if err := c.Flush(); !errors.Is(err, ErrRegionPartitioned) {
					t.Fatalf("flush: %v", err)
				}
			}

			e.effectCache.drainReclaim()

			if got := e.effectCache.EntryCount(); got != 0 {
				t.Fatalf("%s left %d unpublished vertices", failure, got)
			}

			if c.pending != nil || len(c.keys) != 0 {
				t.Fatal("context retained pending history")
			}
		})
	}
}

func TestSavepointRetainsEffectsAcrossAbortAndRepublication(t *testing.T) {
	e := newHorizonTestEngine()
	c := e.NewContext()

	tips := make([]Tip, 0, 5)
	for range 5 {
		tips = append(tips, pendingTestEmit(t, c, "key"))
	}

	saved := c.TakeSavepoint()
	c.Abort()
	e.effectCache.drainReclaim()

	if e.effectCache.EntryCount() != len(tips) {
		t.Fatal("abort reclaimed a retained savepoint")
	}

	c.RestoreSavepoint(saved)

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
	// Model compaction/eviction releasing the indexed chain's ownership while
	// the retained savepoint still holds its independent pins.
	for _, tip := range tips {
		e.effectCache.decrefChainNode(logicalAddress("key"), tip)
	}

	e.effectCache.drainReclaim()

	if e.effectCache.EntryCount() != len(tips) {
		t.Fatal("published savepoint lost its pins")
	}

	c.RestoreSavepoint(saved)

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	for _, tip := range tips {
		v := e.effectCache.getVertex(tip)
		if v == nil || !slices.Contains(v.creationKeys, logicalAddress("key")) {
			t.Fatalf("restoring published history did not reacquire key ownership: %v", tip)
		}
	}

	runtime.KeepAlive(saved)
}

func TestUnreachableSavepointReleasesHistory(t *testing.T) {
	e := newHorizonTestEngine()

	func() {
		c := e.NewContext()
		for range 10 {
			pendingTestEmit(t, c, "key")
		}

		saved := c.TakeSavepoint()
		c.Abort()
		e.effectCache.drainReclaim()

		if e.effectCache.EntryCount() != 10 {
			t.Fatal("live savepoint was reclaimed")
		}

		runtime.KeepAlive(saved)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for e.effectCache.EntryCount() != 0 && time.Now().Before(deadline) {
		runtime.GC()
		runtime.Gosched()
		e.effectCache.drainReclaim()
		time.Sleep(time.Millisecond)
	}

	if got := e.effectCache.EntryCount(); got != 0 {
		t.Fatalf("unreachable savepoint retained %d vertices", got)
	}
}

func TestSavepointClearsDiscardedSliceStorage(t *testing.T) {
	e := newTestEngine(&mockBroadcaster{})
	preSubscribe(e, "key")
	c := e.NewContext()
	c.BeginTx()

	emit := func(id string) {
		t.Helper()

		eff := dataEffect("key")
		eff.GetData().Collection = pb.CollectionKind_KEYED

		eff.GetData().Id = []byte(id)
		if err := c.Emit(eff); err != nil {
			t.Fatal(err)
		}
	}
	// Three entries leave spare capacity in the append-backed slices.
	for _, id := range []string{"a", "b", "c"} {
		emit(id)
	}

	saved := c.TakeSavepoint()
	state := c.keys[logicalAddress("key")]

	emit("discarded")

	notices := state.notifies[:cap(state.notifies)]

	ids := state.elementIDs[:cap(state.elementIDs)]
	if notices[3] == nil || ids[3] == nil {
		t.Fatal("fixture did not share append storage")
	}

	c.RestoreSavepoint(saved)

	if notices[3] != nil || ids[3] != nil {
		t.Fatal("retained prefix still holds discarded payloads beyond its length")
	}

	emit("replacement")

	later := c.TakeSavepoint()
	c.RestoreSavepoint(saved)
	emit("other")
	c.RestoreSavepoint(later)

	if got := c.keys[logicalAddress("key")].elementIDs; string(got[3]) != "replacement" {
		t.Fatalf("later savepoint was overwritten: %q", got)
	}

	c.Abort()
	runtime.KeepAlive(saved)
	runtime.KeepAlive(later)
}

func TestSavepointRestoresReadErrorWithoutChangingReadView(t *testing.T) {
	e := newHorizonTestEngine()
	c := e.NewContext()
	c.BeginTx()
	view := c.txSnapshot
	saved := c.TakeSavepoint()
	pendingTestEmit(t, c, "key")
	c.readErr = errors.New("failed command")
	c.RestoreSavepoint(saved)

	if c.readErr != nil || c.txSnapshot != view || !c.inTx {
		t.Fatal("rollback changed the enclosing transaction or retained its read error")
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
}

func TestPendingHistoryDurabilityAndFailedEmit(t *testing.T) {
	store, err := durability.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	e := newHorizonTestEngine()
	e.effectCache.mirror = store
	c := e.NewContext()
	tip := pendingTestEmit(t, c, "key")

	var persisted bool

	if err := store.Recover(context.Background(), 1, func(id durability.ID, _ []byte) error {
		if id == tip {
			persisted = true
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if !persisted {
		t.Fatal("pending-history pin skipped persistence")
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	saved := c.TakeSavepoint()
	if err := c.Emit(scalarSet("key", "failed")); err == nil {
		t.Fatal("write to closed store succeeded")
	}

	if c.pending != saved.state.position || c.keys[logicalAddress("key")].lastOffset != tip {
		t.Fatal("failed emission changed the saved pending state")
	}
	// Reclamation against a closed mirror is intentionally retried by the pool;
	// detach it here so this test can check in-memory cleanup independently.
	e.effectCache.mirror = nil

	c.Abort()
	runtime.KeepAlive(saved)
}

func TestFailedBindPersistenceKeepsPublishedHistory(t *testing.T) {
	store, err := durability.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	e := newTestEngine(nil)
	c := e.NewContext()
	c.BeginTx()

	tips := make(map[string]Tip)
	for _, key := range []string{"one", "two", "three"} {
		tips[key] = pendingTestEmit(t, c, key)
	}
	// Individual effects are already resident. The bind's persistence fails
	// after Flush has published them, so Abort must not release their ownership.
	e.effectCache.mirror = store

	if err := c.Flush(); err == nil {
		t.Fatal("bind persistence unexpectedly succeeded")
	}

	e.effectCache.mirror = nil

	c.Abort()
	e.effectCache.drainReclaim()

	for key, tip := range tips {
		if _, ok := e.effectCache.Get(tip); !ok {
			t.Fatalf("abort reclaimed published effect for %s", key)
		}

		if got := e.index.Contains(key).Tips(); !slices.Equal(got, []Tip{tip}) {
			t.Fatalf("wrong published frontier for %s: %v", key, got)
		}
	}
}

func TestCopiedAndZeroSavepoints(t *testing.T) {
	e := newHorizonTestEngine()
	c := e.NewContext()
	tip := pendingTestEmit(t, c, "key")
	copied := func() ContextSavepoint { return *c.TakeSavepoint() }()

	c.Abort()
	runtime.GC()
	runtime.Gosched()
	e.effectCache.drainReclaim()
	c.RestoreSavepoint(&copied)

	if _, ok := e.effectCache.Get(tip); !ok {
		t.Fatal("copy lost the retained effect")
	}

	if c.keys[logicalAddress("key")].lastOffset != tip {
		t.Fatal("copy did not restore pending state")
	}

	var empty ContextSavepoint
	c.RestoreSavepoint(&empty)

	if c.pending != nil || len(c.keys) != 0 {
		t.Fatal("zero savepoint did not restore empty state")
	}

	runtime.KeepAlive(copied)
}
