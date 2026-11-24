package cluster

import (
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/engine/keytrie"
)

func TestBoundaryAuditCloudOutboxIdentity(t *testing.T) {
	e := ancestryTestEngine(t)
	internal := keytrie.InternalKey("main", "a")
	application := keytrie.LogicalAddress(internal)
	keyNameKey := DeriveKeyNameKey("boundary-audit")
	cs := &CloudSync{engine: e, keyNameKey: keyNameKey, wake: make(chan struct{}, 1)}

	var (
		values []*pb.Effect
		tips   []effects.Tip
	)

	e.SetOnLocalEffect(func(tip effects.Tip, eff *pb.Effect) {
		if eff.GetData() != nil {
			values = append(values, eff)
			tips = append(tips, tip)
		}
	})

	c := e.NewContext()
	for _, address := range []string{application, internal} {
		if err := c.Emit(&pb.Effect{Key: []byte(keytrie.WireKey(address)), IKey: []byte(address), Kind: &pb.Effect_Data{Data: &pb.DataEffect{Op: pb.EffectOp_INSERT_OP, Collection: pb.CollectionKind_SCALAR, Value: &pb.DataEffect_Raw{Raw: []byte(address)}}}}); err != nil {
			t.Fatal(err)
		}
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	for i, eff := range values {
		if !cs.enqueueUpload(tips[i], eff, true, false) {
			t.Fatal("failed to enqueue")
		}
	}

	if len(cs.pendingKeys) != 2 {
		t.Errorf("two distinct indexed keys share %d outbox entries", len(cs.pendingKeys))
	}

	for _, tip := range tips {
		if !cs.retire(tip) {
			t.Fatal("outbox retirement failed")
		}
	}

	if len(cs.pendingKeys) != 0 {
		t.Fatal("outbox retained retired keys")
	}

	for _, address := range []string{application, internal} {
		if !e.PinKeyInternal(address) {
			t.Fatal("retiring distinct outbox entries lost a key")
		}

		e.UnpinKeyInternal(address)
	}
}
