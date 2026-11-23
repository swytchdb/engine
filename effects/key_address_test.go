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
	"strings"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
	"google.golang.org/protobuf/proto"
)

func addressedValue(address, value string) *pb.Effect {
	return &pb.Effect{Key: []byte(wireKey(address)), IKey: []byte(address), Kind: &pb.Effect_Data{Data: &pb.DataEffect{
		Op: pb.EffectOp_INSERT_OP, Collection: pb.CollectionKind_SCALAR, Merge: pb.MergeRule_LAST_WRITE_WINS,
		Value: &pb.DataEffect_Raw{Raw: []byte(value)},
	}}}
}

func TestInternalAddressTransport(t *testing.T) {
	sender := NewEngine(EngineConfig{DefaultMode: UnsafeMode})
	receiver := NewEngine(EngineConfig{DefaultMode: UnsafeMode})

	t.Cleanup(func() {
		if err := sender.Close(); err != nil {
			t.Error(err)
		}

		if err := receiver.Close(); err != nil {
			t.Error(err)
		}
	})

	if err := receiver.Subscribe("animals"); err != nil {
		t.Fatal(err)
	}

	rootBefore := receiver.index.Contains("animals")
	address := keytrie.InternalKey("animals", "node\x00one")
	if err := receiver.ensureSubscribedBlocking(address); err != nil {
		t.Fatal(err)
	}
	ctx := sender.NewContext()

	effect := addressedValue(address, "otter")
	if err := ctx.Emit(effect); err != nil {
		t.Fatal(err)
	}

	if err := ctx.Flush(); err != nil {
		t.Fatal(err)
	}

	tips := sender.index.ContainsInternal(address)
	if tips.Len() != 1 {
		t.Fatalf("sender internal frontier = %v", tips)
	}

	data, err := MarshalEffect(effect)
	if err != nil {
		t.Fatal(err)
	}

	receiver.SetBroadcaster(&graphSource{source: sender})
	notify := BuildOffsetNotify(sender.NodeID(), tips.Tips()[0], effect, data, nil)
	if _, err := receiver.HandleRemote(notify); err != nil {
		t.Fatal(err)
	}

	if receiver.index.ContainsInternal(address) == nil || receiver.index.Contains(address) != nil {
		t.Fatal("receiver used the logical namespace for a graph node")
	}

	if receiver.index.Contains("animals") != rootBefore {
		t.Fatal("node receipt advanced the descriptor frontier")
	}

	state, _, _, err := receiver.GetSnapshotInternal(address)
	if err != nil || string(state.GetScalar().GetRaw()) != "otter" {
		t.Fatalf("internal read: %v, %v", state, err)
	}

	if got := receiver.MatchKeys("*"); len(got) != 1 || got[0] != "animals" {
		t.Fatalf("application scan exposed graph node: %q", got)
	}

	uninterested := NewEngine(EngineConfig{DefaultMode: UnsafeMode})

	t.Cleanup(func() {
		if err := uninterested.Close(); err != nil {
			t.Error(err)
		}
	})

	nacks, err := uninterested.HandleRemote(notify)
	if err != nil || len(nacks) != 1 || !nacks[0].GetNotSubscribed() {
		t.Fatalf("unsubscribed receiver accepted graph node: %v, %v", nacks, err)
	}

	if uninterested.index.ContainsInternal(address) != nil {
		t.Fatal("unsubscribed node was indexed")
	}
}

func TestApplicationAddressAndLegacyTransport(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		e := NewEngine(EngineConfig{DefaultMode: UnsafeMode})

		t.Cleanup(func() {
			if err := e.Close(); err != nil {
				t.Error(err)
			}
		})

		var notified string

		e.OnKeyDataAdded = func(key string) { notified = key }

		effect := addressedValue(logicalAddress("\x00animals"), "otter")
		if !explicit {
			effect.IKey = nil
		}

		ctx := e.NewContext()
		if err := ctx.Emit(effect); err != nil {
			t.Fatal(err)
		}

		if err := ctx.Flush(); err != nil {
			t.Fatal(err)
		}

		if notified != "\x00animals" || string(effect.GetKey()) != "\x00animals" {
			t.Fatalf("application identity changed: callback=%q key=%q", notified, effect.GetKey())
		}

		state, _, _, err := e.GetSnapshot("\x00animals")
		if err != nil || string(state.GetScalar().GetRaw()) != "otter" {
			t.Fatalf("application read: %v, %v", state, err)
		}
	}
}

func TestLogicalBindWireCompatibility(t *testing.T) {
	effect := &pb.Effect{Key: []byte("a"), IKey: []byte(logicalAddress("a")), Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{Keys: []*pb.TransactionalBindEffect_KeyBind{
		{Key: []byte(logicalAddress("a"))}, {Key: []byte(logicalAddress("\x00b"))},
	}}}}

	encoded, err := MarshalEffect(effect)
	if err != nil {
		t.Fatal(err)
	}

	old := new(pb.Effect)
	if err := proto.Unmarshal(encoded, old); err != nil {
		t.Fatal(err)
	}

	if len(old.GetIKey()) != 0 || !bytes.Equal(old.GetTxnBind().GetKeys()[0].GetKey(), []byte("a")) || !bytes.Equal(old.GetTxnBind().GetKeys()[1].GetKey(), []byte("\x00b")) {
		t.Fatalf("logical bind changed wire key identities: %v", old)
	}

	decoded := new(pb.Effect)
	if err := UnmarshalEffect(encoded, decoded); err != nil {
		t.Fatal(err)
	}

	if !proto.Equal(decoded, effect) {
		t.Fatalf("bind address normalization did not round trip: %v", decoded)
	}
}

func TestGraphAddressMixedTransactionAndCompaction(t *testing.T) {
	e := NewEngine(EngineConfig{DefaultMode: UnsafeMode})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})

	address := keytrie.InternalKey("animals", "root")

	for range 12 {
		c := e.NewContext()
		c.BeginTx()

		if err := c.Emit(addressedValue(address, "otter")); err != nil {
			t.Fatal(err)
		}

		if err := c.Emit(addressedValue(logicalAddress("counter"), "one")); err != nil {
			t.Fatal(err)
		}

		if err := c.Flush(); err != nil {
			t.Fatal(err)
		}

		r := e.NewContext()

		state, _, err := r.GetSnapshotInternal(address)
		if err != nil || string(state.GetScalar().GetRaw()) != "otter" {
			t.Fatalf("graph read after transaction: %v, %v", state, err)
		}

		if err := r.Flush(); err != nil {
			t.Fatal(err)
		}
	}

	state, _, _, err := e.GetSnapshot("counter")
	if err != nil || string(state.GetScalar().GetRaw()) != "one" {
		t.Fatalf("application read after mixed transaction: %v, %v", state, err)
	}

	if err := e.FlushIndex(); err != nil {
		t.Fatal(err)
	}

	if e.index.SizeInternal() != 0 {
		t.Fatal("FlushIndex left internal nodes")
	}
}

func TestInternalAddressDurability(t *testing.T) {
	dir := t.TempDir()
	e := durableTestEngine(t, dir)
	address := keytrie.InternalKey("animals", "root")
	c := e.NewContext()
	c.BeginTx()

	if err := c.Emit(addressedValue(address, "otter")); err != nil {
		t.Fatal(err)
	}

	if err := c.Emit(addressedValue(logicalAddress("counter"), "one")); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e = durableTestEngine(t, dir)

	state, _, _, err := e.GetSnapshotInternal(address)
	if err != nil || string(state.GetScalar().GetRaw()) != "otter" {
		t.Fatalf("restored graph = %v, error = %v", state, err)
	}

	if got := durableValue(t, e, "counter"); got != "one" {
		t.Fatal(got)
	}

	if e.index.Contains(address) != nil {
		t.Fatal("restored graph in logical namespace")
	}
}

// Internal leaves share their owner's admission authority, but dropping one
// leaf cannot remove the subscription that keeps its siblings current.
func TestInternalAddressUnsubscribePreservesOwner(t *testing.T) {
	e := NewEngine(EngineConfig{DefaultMode: UnsafeMode})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})

	if err := e.Subscribe("animals"); err != nil {
		t.Fatal(err)
	}

	address := keytrie.InternalKey("animals", "root")

	e.addPeerSubscriber(logicalAddress("animals"), 9)
	e.removePeerSubscriber(address, 9)

	if got := e.PeerSubscribers("animals"); len(got) != 1 || got[0] != 9 {
		t.Fatalf("node unsubscribe removed owner peer: %v", got)
	}

	e.dropSubscription(address)

	if _, ok := e.subscriptions.Load(logicalAddress("animals")); !ok {
		t.Fatal("node eviction removed owner subscription")
	}

	e.getOrCreatePeerFilter(9).hasBulk = true
	e.peerFilterAdd(9, logicalAddress("animals"))

	if e.clusterMaybeHasKey(address) {
		t.Fatal("main-key filter hit implied an internal node was held")
	}
}

func TestPrepareEffectAddressNoTemporaryAllocations(t *testing.T) {
	for _, name := range []string{"", "\x00\xff", strings.Repeat("k", 16), strings.Repeat("k", 128), strings.Repeat("k", 1024)} {
		for _, address := range []string{keytrie.LogicalAddress(name), keytrie.InternalKey(name, "node")} {
			eff := &pb.Effect{Key: []byte(keytrie.WireKey(address)), IKey: []byte(address)}
			if n := testing.AllocsPerRun(100, func() {
				if err := prepareEffectAddress(eff); err != nil {
					panic(err)
				}
			}); n != 0 {
				t.Fatalf("validation of %d-byte address allocated %g times", len(address), n)
			}

			eff.Key = append(eff.GetKey(), 'x')
			if err := prepareEffectAddress(eff); err == nil {
				t.Fatal("accepted mismatched wire key")
			}
		}
	}
}
