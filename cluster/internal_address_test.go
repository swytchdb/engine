package cluster

import (
	"context"
	"errors"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	dp "github.com/swytchdb/engine/cluster/proto/dataplane"
	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/engine/keytrie"
)

// Only the bind is sent eagerly. Both data branches must arrive through the
// real fetch/ancestry transport before the receiver can reconstruct the values.
func TestInternalAddressTransactionFetch(t *testing.T) {
	source, target := ancestryTestEngine(t), ancestryTestEngine(t)

	address := keytrie.InternalKey("animals", "root")
	if err := target.SubscribeInternal(address); err != nil {
		t.Fatal(err)
	}

	if err := target.Subscribe("animals"); err != nil {
		t.Fatal(err)
	}

	if err := target.Subscribe("counter"); err != nil {
		t.Fatal(err)
	}

	_, descriptorTips, _, err := target.GetSnapshot("animals")
	if err != nil {
		t.Fatal(err)
	}

	pms := startNNodeCluster(t,
		[]EffectHandler{NewEngineEffectHandler(source), NewEngineEffectHandler(target)},
		[]LogReader{NewEngineLogReader(source.EffectCache()), NewEngineLogReader(target.EffectCache())}, source.NodeID(), target.NodeID())
	target.SetBroadcaster(pms[1])

	var last effects.Tip

	var envelope *pb.Effect

	source.OnLocalEffect = func(tip effects.Tip, eff *pb.Effect) {
		if eff.GetTxnBind() != nil {
			last, envelope = tip, eff
		}
	}
	tx := source.NewContext()
	tx.BeginTx()

	for _, entry := range []struct{ key, internal, value string }{
		{address, address, "otter"}, {"counter", "", "one"},
	} {
		if err := tx.Emit(&pb.Effect{Key: []byte(entry.key), IKey: []byte(entry.internal), Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op: pb.EffectOp_INSERT_OP, Collection: pb.CollectionKind_SCALAR, Merge: pb.MergeRule_LAST_WRITE_WINS,
			Value: &pb.DataEffect_Raw{Raw: []byte(entry.value)},
		}}}); err != nil {
			t.Fatal(err)
		}
	}

	if err := tx.Flush(); err != nil {
		t.Fatal(err)
	}

	if envelope.GetTxnBind() == nil {
		t.Fatal("expected transaction bind")
	}

	wire, err := effects.MarshalEffect(envelope)
	if err != nil {
		t.Fatal(err)
	}

	decoded := &pb.Effect{}
	if err := effects.UnmarshalEffect(wire, decoded); err != nil {
		t.Fatal(err)
	}

	if string(decoded.GetKey()) != address {
		t.Fatal("mixed bind exposed an application wire identity")
	}

	visited := make(map[effects.Tip]bool)

	if err := pms[1].FetchAncestry(address, last, []pb.NodeID{source.NodeID()}, func(effects.Tip) bool { return false }, func(tip effects.Tip, _ []byte, _ bool) error {
		visited[tip] = true

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if len(visited) < 2 {
		t.Fatal("graph ancestry omitted its data branch")
	}

	data, err := effects.MarshalEffect(envelope)
	if err != nil {
		t.Fatal(err)
	}

	notify := effects.BuildOffsetNotify(source.NodeID(), last, envelope, data, nil)
	if _, err := pms[0].ReplicateTo(notify, notify.GetEffectData(), target.NodeID()); err != nil {
		t.Fatal(err)
	}

	state, _, _, err := target.GetSnapshotInternal(address)
	if err != nil || string(state.GetScalar().GetRaw()) != "otter" {
		t.Fatalf("graph value = %v, error = %v", state, err)
	}

	state, _, _, err = target.GetSnapshot("counter")
	if err != nil || string(state.GetScalar().GetRaw()) != "one" {
		t.Fatalf("application value = %v, error = %v", state, err)
	}

	_, after, _, err := target.GetSnapshot("animals")
	if err != nil || len(after) != len(descriptorTips) {
		t.Fatalf("descriptor changed: %v, %v", after, err)
	}

	for i := range after {
		if after[i] != descriptorTips[i] {
			t.Fatal("graph receipt advanced descriptor")
		}
	}

	for _, key := range target.MatchKeys("*") {
		if key == address {
			t.Fatal("application scan exposed graph node")
		}
	}
}

func TestInternalAddressCloudReconcileAndPin(t *testing.T) {
	e := ancestryTestEngine(t)
	if err := e.Subscribe("animals"); err != nil {
		t.Fatal(err)
	}

	address := keytrie.InternalKey("animals", "root")
	tip := effects.Tip{42, 1000}

	eff := &pb.Effect{Key: []byte(address), IKey: []byte(address), Kind: &pb.Effect_Data{Data: &pb.DataEffect{
		Op: pb.EffectOp_INSERT_OP, Collection: pb.CollectionKind_SCALAR,
		Value: &pb.DataEffect_Raw{Raw: []byte("otter")},
	}}}
	if err := e.EffectCache().Put(tip, eff); err != nil {
		t.Fatal(err)
	}

	keyNameKey := DeriveKeyNameKey("graph-cloud-test")
	client := &sequencedTipsClient{responses: []*dp.GetTipsResponse{{Keys: []*dp.KeyTips{{
		Key: CloudKeyName(keyNameKey, eff.GetKey()), Tips: []*dp.EffectRef{{NodeId: tip[0], Offset: tip[1]}},
	}}}}}
	cs := &CloudSync{engine: e, keyNameKey: keyNameKey, client: client, ctx: context.Background(), wake: make(chan struct{}, 1)}
	cs.markPendingInternal(address)
	cs.reconcileBatch(cs.reconcileKeys())

	if len(cs.reconcileKeys()) != 0 {
		t.Fatal("graph reconciliation did not complete")
	}

	state, _, _, err := e.GetSnapshotInternal(address)
	if err != nil || string(state.GetScalar().GetRaw()) != "otter" {
		t.Fatalf("cloud graph = %v, error = %v", state, err)
	}

	if e.PinKey(address) {
		t.Fatal("cloud installed graph into application namespace")
	}

	cs.handleLocalEffect(tip, eff)

	if !cs.pendingKeys[address].pinned {
		t.Fatal("outbox failed to pin graph leaf")
	}
	// A failed cloud consult must retain the full address in the existing
	// reconciliation set, while looking up the same wire key in the outbox.
	cs.client = &sequencedTipsClient{err: errors.New("offline")}

	tips, _, _, err := cs.CloudTipsInternal(context.Background(), address)
	if err != nil || len(tips) != 1 || tips[0] != tip {
		t.Fatalf("outbox read: %v, %v", tips, err)
	}

	if got := cs.reconcileKeys(); len(got) != 1 || got[0] != address {
		t.Fatalf("lost reconcile address: %q", got)
	}

	if !cs.retire(tip) {
		t.Fatal("outbox did not retire")
	}

	if !e.PinKeyInternal(address) {
		t.Fatal("lost graph leaf")
	}

	e.UnpinKeyInternal(address)
}
