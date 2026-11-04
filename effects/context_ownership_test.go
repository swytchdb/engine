package effects

import (
	"encoding/binary"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

func TestBuildOffsetNotifyOwnsKey(t *testing.T) {
	key := []byte("original")
	eff := &pb.Effect{Key: key}

	data, err := MarshalEffect(eff)
	if err != nil {
		t.Fatal(err)
	}

	notify := BuildOffsetNotify(1, Tip{1, 1}, eff, data, nil)

	copy(key, "recycled")

	wire := notify.GetEffectData()

	keyLen := binary.LittleEndian.Uint32(wire[:4])
	if got := string(notify.GetKey()); got != "original" || got != string(wire[4:4+keyLen]) {
		t.Fatalf("notification key %q differs from the original wire key", got)
	}
}

func TestTransactionOwnsBorrowedElementIDs(t *testing.T) {
	e := NewEngine(EngineConfig{DefaultMode: UnsafeMode})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})

	c := e.NewContext()
	c.BeginTx()

	defer c.Abort()

	id := []byte("field1")
	for _, field := range []string{"field1", "field2"} {
		copy(id, field)

		eff := &pb.Effect{Key: []byte("k"), Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op: pb.EffectOp_INSERT_OP, Collection: pb.CollectionKind_KEYED, Id: id,
		}}}
		if err := c.Emit(eff); err != nil {
			t.Fatal(err)
		}
	}

	clear(id)

	for i, want := range []string{"field1", "field2"} {
		if got := string(c.keys["k"].elementIDs[i]); got != want {
			t.Fatalf("retained element ID %d = %q, want %q after buffer reuse", i, got, want)
		}
	}
}
