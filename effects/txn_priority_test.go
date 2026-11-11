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
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
)

// A and C have no common key. Only B can pass C's evidence to A.
func TestNackPriorityEvidenceAcrossKeys(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)
	base := log.putEffect(scalarSet("ab", "base"))
	a := newTestPendingTxn(pendingTxnKey{key: "ab", newTip: base})
	a.txnID = "A"

	node := pb.NodeID(2)
	for !ForkChoiceLess(ComputeForkChoiceHash(node, sTs(100)), a.forkChoiceHash) {
		node++
	}

	b := &pb.Effect{
		Key: []byte("ab"), TxnId: "B", NodeId: uint64(node), Hlc: sTs(100),
		Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
			OriginatorNodeId: uint64(node), TxnHlc: sTs(100),
			Keys: []*pb.TransactionalBindEffect_KeyBind{
				{Key: []byte("ab"), NewTip: toPbRef(base)},
				{Key: []byte("bc"), NewTip: toPbRef(base)},
			},
		}},
	}
	bt := log.putEffect(b)
	early := e.buildEnrichedNack("ab", toPbRef(a.bindOffset), []Tip{bt})
	// Knowing B's hash does not establish a loss for A.
	if _, err := e.evaluateNacks(a, []*pb.NackNotify{early}); !errors.Is(err, ErrTxnAmbiguous) {
		t.Fatalf("unresolved B: got %v, want ambiguous", err)
	}

	for _, outcome := range []pb.Verdict{pb.Verdict_LOST, pb.Verdict_WON} {
		evidence := log.putEffect(&pb.Effect{Key: []byte("bc"), Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
			TxnVerdicts: map[string]pb.Verdict{"B": outcome, "C": pb.Verdict_WON},
		}}})

		late := e.buildEnrichedNack("bc", toPbRef(a.bindOffset), []Tip{evidence})
		for _, responses := range [][]*pb.NackNotify{{early, late}, {late, early}} {
			_, err := e.evaluateNacks(a, responses)
			if outcome == pb.Verdict_LOST && err != nil {
				t.Fatalf("B lost: %v", err)
			}

			if outcome == pb.Verdict_WON && !errors.Is(err, ErrTxnAborted) {
				t.Fatalf("B won: %v", err)
			}
		}
	}

	if _, ok := e.subscriptions.Load("bc"); ok {
		t.Fatal("evaluating forwarded evidence subscribed A to C's key")
	}
}

func TestBindResponseWaitsForPriorityEvidence(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)
	base := log.putEffect(scalarSet("ab", "base"))
	b := newTestPendingTxn(pendingTxnKey{key: "ab", newTip: base})
	b.txnID = "B"
	b.resolved = make(chan struct{})

	node := pb.NodeID(2)
	for !ForkChoiceLess(b.forkChoiceHash, ComputeForkChoiceHash(node, sTs(100))) {
		node++
	}

	incoming := &pb.Effect{
		Key: []byte("ab"), TxnId: "A", NodeId: uint64(node), Hlc: sTs(100),
		ForkChoiceHash: ComputeForkChoiceHash(node, sTs(100)),
		Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
			OriginatorNodeId: uint64(node), TxnHlc: sTs(100),
			Keys: []*pb.TransactionalBindEffect_KeyBind{{Key: []byte("ab"), NewTip: toPbRef(base)}},
		}},
	}
	ref := Tip{uint64(node), 700}

	e.pendingTxns.Store(b.bindOffset, b)
	e.subscriptions.Store("ab", &subscriptionState{})
	e.updateIndex("ab", nil, base)

	wire, err := MarshalEffect(incoming)
	if err != nil {
		t.Fatal(err)
	}

	result := make(chan []*pb.NackNotify, 1)
	errs := make(chan error, 1)

	go func() {
		nacks, err := e.HandleRemote(&pb.OffsetNotify{Key: []byte("ab"), Origin: toPbRef(ref), EffectData: wire})
		result <- nacks

		errs <- err
	}()

	deadline := time.After(time.Second)

	for !e.index.Contains("ab").Contains(ref) {
		select {
		case <-deadline:
			t.Fatal("bind was not installed before waiting")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	select {
	case <-result:
		t.Fatal("response preceded B's evaluation")
	default:
	}
	// A new arrival/publication must remain possible while B waits for C.
	unlocked := make(chan struct{})

	go func() { lock := e.takeNodeLock(); lock.release(); close(unlocked) }()

	select {
	case <-unlocked:
	case <-time.After(time.Second):
		t.Fatal("priority wait held nodeMu")
	}

	b.finish(txnStateAborted)

	select {
	case <-result:
		t.Fatal("local finish is not evidence publication")
	default:
	}

	proof := log.putEffect(&pb.Effect{Key: []byte("bc"), Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
		TxnVerdicts: map[string]pb.Verdict{"B": pb.Verdict_LOST, "C": pb.Verdict_WON},
	}}})
	b.responses = []*pb.NackNotify{e.buildEnrichedNack("bc", toPbRef(b.bindOffset), []Tip{proof})}
	close(b.resolved)

	select {
	case nacks := <-result:
		if err := <-errs; err != nil {
			t.Fatal(err)
		}

		found := false

		for _, n := range nacks {
			if string(n.GetKey()) == "bc" {
				found = true
			}
		}

		if !found {
			t.Fatal("B did not forward C's evidence on the unrelated key")
		}
	case <-time.After(time.Second):
		t.Fatal("response did not resume after evidence publication")
	}
}

// The transport holds bind delivery until all three origins have published.
// B receives A before it learns C. A does not subscribe to C's key.
type priorityTestNetwork struct {
	engines   map[pb.NodeID]*Engine
	mu        sync.Mutex
	published map[pb.NodeID]Tip
	a, b, c   pb.NodeID
	ready     chan struct{}
}
type priorityTestBroadcaster struct {
	mockBroadcaster

	net  *priorityTestNetwork
	node pb.NodeID
}

func (b *priorityTestBroadcaster) BroadcastWithData(*pb.OffsetNotify, []byte) {}
func (b *priorityTestBroadcaster) FetchFromAny(ref *pb.EffectRef, _ FetchHint) ([]byte, error) {
	if e := b.net.engines[pb.NodeID(ref.GetNodeId())]; e != nil {
		if eff, ok := e.effectCache.Get(r(ref)); ok {
			data, err := e.effectCache.MarshalEffect(eff)
			if err != nil {
				return nil, err
			}

			wire := make([]byte, 4+len(eff.GetKey())+len(data))
			binary.LittleEndian.PutUint32(wire, uint32(len(eff.GetKey())))
			copy(wire[4:], eff.GetKey())
			copy(wire[4+len(eff.GetKey()):], data)

			return wire, nil
		}
	}

	return nil, fmt.Errorf("missing effect %v", ref)
}

func (b *priorityTestBroadcaster) ReplicateMarshalled(n *pb.OffsetNotify, _ []byte, target pb.NodeID) ([]*pb.NackNotify, error) {
	b.net.mu.Lock()

	b.net.published[b.node] = r(n.GetOrigin())
	if len(b.net.published) == 3 {
		select {
		case <-b.net.ready:
		default:
			close(b.net.ready)
		}
	}
	b.net.mu.Unlock()

	select {
	case <-b.net.ready:
	case <-time.After(time.Second):
		return nil, errors.New("bind publication barrier timed out")
	}

	if b.node == b.net.c && target == b.net.b {
		b.net.mu.Lock()
		aRef := b.net.published[b.net.a]
		b.net.mu.Unlock()

		deadline := time.After(time.Second)

		for !b.net.engines[target].index.Contains("ab").Contains(aRef) {
			select {
			case <-deadline:
				return nil, errors.New("A did not reach B before C")
			default:
				time.Sleep(time.Millisecond)
			}
		}
	}

	return b.net.engines[target].HandleRemote(n)
}

func TestPriorityChainCommitsDisjointEnds(t *testing.T) {
	nodes := []pb.NodeID{1, 2, 3}
	stamp := sTs(100)

	slices.SortFunc(nodes, func(a, b pb.NodeID) int {
		if ForkChoiceLess(ComputeForkChoiceHash(a, stamp), ComputeForkChoiceHash(b, stamp)) {
			return -1
		}

		return 1
	})
	// Priority C, B, A; conflict edges A--B--C.
	cID, bID, aID := nodes[0], nodes[1], nodes[2]
	net := &priorityTestNetwork{engines: make(map[pb.NodeID]*Engine), published: make(map[pb.NodeID]Tip), ready: make(chan struct{}), a: aID, b: bID, c: cID}
	keys := map[pb.NodeID][]string{aID: {"ab"}, bID: {"ab", "bc"}, cID: {"bc"}}
	contexts := make(map[pb.NodeID]*Context)

	for _, id := range nodes {
		e := newTxnTestEngine(nil)
		e.nodeID = id
		e.clock.SetClock(func() time.Time { return stamp.AsTime() })

		net.engines[id] = e
		for _, key := range keys[id] {
			base := Tip{99, 1}
			if key == "bc" {
				base[1] = 2
			}

			if err := e.effectCache.Put(base, scalarSet(key, "base")); err != nil {
				t.Fatal(err)
			}

			e.updateIndex(key, nil, base)

			state := &subscriptionState{ready: make(chan struct{})}
			state.markReady()
			e.subscriptions.Store(key, state)

			for _, peer := range nodes {
				if peer != id && slices.Contains(keys[peer], key) {
					e.addPeerSubscriber(key, peer)
				}
			}
		}

		bc := &priorityTestBroadcaster{net: net, node: id}
		bc.peerIDs = nodes
		bc.allRegionPeersReachable = true
		e.SetBroadcaster(bc)
		ctx := e.NewContext()
		ctx.BeginTx()

		for _, key := range keys[id] {
			if err := ctx.Emit(scalarSet(key, fmt.Sprint(id))); err != nil {
				t.Fatal(err)
			}
		}

		contexts[id] = ctx
	}

	type outcome struct {
		id  pb.NodeID
		err error
	}

	results := make(chan outcome, 3)

	for _, id := range nodes {
		go func() { results <- outcome{id, contexts[id].Flush()} }()
	}

	for range nodes {
		select {
		case result := <-results:
			if result.id == bID {
				if !errors.Is(result.err, ErrTxnAborted) {
					t.Errorf("B: %v, want definite abort", result.err)
				}
			} else if result.err != nil {
				t.Errorf("end node %d: %v, want commit", result.id, result.err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("priority chain failed to drain")
		}
	}

	if _, subscribed := net.engines[aID].subscriptions.Load("bc"); subscribed {
		t.Fatal("A subscribed to C's key")
	}
}

func TestPriorityChainReconstructionWithoutVerdicts(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)
	nodes := []pb.NodeID{1, 2, 3}
	slices.SortFunc(nodes, func(a, b pb.NodeID) int {
		if ForkChoiceLess(ComputeForkChoiceHash(a, sTs(100)), ComputeForkChoiceHash(b, sTs(100))) {
			return -1
		}

		return 1
	})

	for i, keys := range [][]string{{"bc"}, {"ab", "bc"}, {"ab"}} {
		id := nodes[i]
		txn := fmt.Sprintf("txn-%d", i)
		log.nodeID = uint64(id)
		bind := &pb.TransactionalBindEffect{OriginatorNodeId: uint64(id), TxnHlc: sTs(100)}

		for _, key := range keys {
			data := scalarSet(key, txn)
			data.NodeId = uint64(id)
			data.TxnId = txn
			data.Hlc = sTs(100)
			tip := log.putEffect(data)
			bind.Keys = append(bind.Keys, &pb.TransactionalBindEffect_KeyBind{Key: []byte(key), NewTip: toPbRef(tip)})
		}

		eff := &pb.Effect{Key: []byte(keys[0]), NodeId: uint64(id), Hlc: sTs(100), TxnId: txn, Kind: &pb.Effect_TxnBind{TxnBind: bind}}
		for _, kb := range bind.GetKeys() {
			eff.Deps = append(eff.Deps, kb.GetNewTip())
		}

		tip := log.putEffect(eff)
		for _, key := range keys {
			e.updateIndex(key, nil, tip)
		}
	}

	for _, cached := range []bool{false, true} {
		if cached {
			for _, key := range []string{"ab", "bc"} {
				tips := e.index.Contains(key)
				snap := log.putEffect(&pb.Effect{Key: []byte(key), Deps: toPbRefs(tips.Tips()), Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
					TxnVerdicts: map[string]pb.Verdict{"txn-0": pb.Verdict_WON, "txn-1": pb.Verdict_LOST, "txn-2": pb.Verdict_WON},
				}}})
				e.updateIndex(key, tips, snap)
			}
		}

		for key, want := range map[string]string{"ab": "txn-2", "bc": "txn-0"} {
			value, _, _, err := e.GetSnapshot(key)
			if err != nil {
				t.Fatal(err)
			}

			if got := string(value.GetScalar().GetRaw()); got != want {
				t.Errorf("cached=%t %s=%q, want %q", cached, key, got, want)
			}
		}

		values, err := e.GetSnapshots(context.Background(), []string{"ab", "bc"})
		if err != nil {
			t.Fatal(err)
		}

		if string(values[0].GetScalar().GetRaw()) != "txn-2" || string(values[1].GetScalar().GetRaw()) != "txn-0" {
			t.Fatalf("cached=%t batch disagrees: %v", cached, values)
		}
	}
}

// Closure discovery and each key walk can observe different live frontiers.
// A cached loss first encountered by a per-key walk must survive aggregation.
func TestPriorityReconstructionRetainsPerKeyVerdicts(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)
	loser := closureBind(log, "loser", 2, []string{"k"}, nil)
	snap := log.putEffect(&pb.Effect{Key: []byte("k"), Deps: toPbRefs([]Tip{loser}), Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
		TxnVerdicts: map[string]pb.Verdict{"loser": pb.Verdict_LOST},
	}}})

	lost, _ := e.losersOnKeysAt(map[string][]Tip{"k": nil}, func(string) []Tip { return []Tip{snap} }, nil, nil, false)
	if _, ok := lost["loser"]; !ok {
		t.Fatal("per-key cached loss disappeared from the combined result")
	}
}
