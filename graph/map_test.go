package graph

import (
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"google.golang.org/protobuf/proto"
)

func mapInts(b *MapEffect[*IntEffect]) map[string]int64 {
	out := make(map[string]int64)
	for key, entry := range b.entries() {
		out[key] = entry.GetData().GetIntVal()
	}

	return out
}

func TestMapRawKeysAndComposition(t *testing.T) {
	b := &MapEffect[*IntEffect]{}

	for _, key := range []string{"", "plain", "\x00\xff", "slegacy", "i01"} {
		value := ScalarInt(10).AsEffect().Add(3).Add(-1)
		messages := slices.Clone(value.getEffects())
		offset := len(b.inner)

		b = b.Set(key, value)
		for i, message := range messages {
			if b.inner[offset+i] != message || string(message.GetData().GetId()) != key {
				t.Fatal("Set copied effects or encoded key")
			}
		}

		if got := b.entries()[key].GetData().GetIntVal(); got != 12 {
			t.Fatalf("%q = %d", key, got)
		}
	}

	b = b.Remove("plain").Remove("missing").Set("plain", ScalarInt(7).AsEffect())
	if got := mapInts(b); !maps.Equal(got, map[string]int64{"": 12, "plain": 7, "\x00\xff": 12, "slegacy": 12, "i01": 12}) {
		t.Fatalf("map = %v", got)
	}
}

func TestMapFromExistingSnapshot(t *testing.T) {
	seed := &pb.ReducedEffect{Collection: pb.CollectionKind_KEYED, NetAdds: map[string]*pb.ReducedElement{
		"count":    {Data: &pb.DataEffect{Id: []byte("count"), Merge: pb.MergeRule_LAST_WRITE_WINS, Value: &pb.DataEffect_IntVal{IntVal: 10}}},
		"\x00\xff": {Data: &pb.DataEffect{Id: []byte("\x00\xff"), Value: &pb.DataEffect_IntVal{IntVal: 8}}},
	}}
	before := proto.Clone(seed)

	b, err := MapFromSnap[*IntEffect](seed)
	if err != nil || b.base != seed || len(b.inner) != 0 {
		t.Fatalf("seed: %v", err)
	}

	b = b.Set("count", (&IntEffect{}).Add(2).Add(-1)).Remove("\x00\xff")
	if !maps.Equal(mapInts(b), map[string]int64{"count": 11}) {
		t.Fatalf("existing raw map = %v", mapInts(b))
	}

	b = b.Replace(map[string]*IntEffect{"fresh": ScalarInt(4).AsEffect()})
	if b.base != seed || len(b.inner) != 5 || !maps.Equal(mapInts(b), map[string]int64{"fresh": 4}) {
		t.Fatal("replacement did not retain history and reset through an effect")
	}

	if !proto.Equal(seed, before) {
		t.Fatal("modified borrowed snapshot")
	}

	b = b.Replace(nil)
	if len(b.entries()) != 0 {
		t.Fatal("empty replacement retained entries")
	}

	for _, empty := range []*pb.ReducedEffect{nil, {Op: pb.EffectOp_REMOVE_OP}} {
		b, err := MapFromSnap[*IntEffect](empty)
		if err != nil || len(b.entries()) != 0 || len(b.inner) != 0 {
			t.Fatalf("empty seed: %v", err)
		}
	}
}

func TestMapComposesBuilders(t *testing.T) {
	seed := effects.ReduceChain(nil, ScalarInt(100).AsEffect().getEffects())
	value, _ := IntFromSnap(seed)

	m := (&MapEffect[*IntEffect]{}).Set("untouched", value)
	if len(m.inner) != 0 {
		t.Fatal("composition emitted an input snapshot")
	}

	m = m.Set("count", value.Add(2))
	if !maps.Equal(mapInts(m), map[string]int64{"count": 2}) {
		t.Fatal("composition must include only pending effects")
	}

	// A composed map is itself an Emits. Retarget its pending delta sequence.
	nested := (&MapEffect[*IntEffect]{}).Set("inner", ScalarInt(5).AsEffect().Add(2))

	outer := (&MapEffect[*MapEffect[*IntEffect]]{}).Set("outer", nested)
	if len(outer.inner) != 2 || outer.entries()["outer"].GetData().GetIntVal() != 7 {
		t.Fatal("nested builder lost composed merge semantics")
	}
}

func TestMapOwnership(t *testing.T) {
	key := "key"
	input := ScalarInt(1).AsEffect()

	m := (&MapEffect[*IntEffect]{}).Set(key, input)
	if m.inner[0] != input.inner[0] || string(m.inner[0].GetData().GetId()) != key {
		t.Fatal("Set must retain the transferred message and raw key")
	}

	removed := m.Remove(key)
	if string(removed.inner[1].GetData().GetId()) != key {
		t.Fatal("Remove must retain the raw key")
	}

	m.inner = append(make([]*pb.Effect, 0, 8), m.inner...)
	left := m.Set("left", ScalarInt(2).AsEffect())
	_ = m.Set("right", ScalarInt(3).AsEffect())

	if !maps.Equal(mapInts(left), map[string]int64{"left": 2, "right": 3}) {
		t.Fatal("expected documented slice aliasing")
	}
}

func TestMapReplacementKeysAreUnencoded(t *testing.T) {
	m := (&MapEffect[*StringEffect]{}).Replace(map[string]*StringEffect{"": ScalarString("empty").AsEffect(), "\x00\xff": ScalarString("binary").AsEffect()})
	if len(m.inner) != 3 || m.inner[0].GetData().GetOp() != pb.EffectOp_REMOVE_OP {
		t.Fatal("expected removal followed by entries")
	}

	entries := m.entries()
	if string(entries[""].GetData().Decompress()) != "empty" || string(entries["\x00\xff"].GetData().Decompress()) != "binary" {
		t.Fatalf("map = %v", entries)
	}
}

func TestMapGetReadsAndComposesPendingChanges(t *testing.T) {
	for _, key := range []string{"count", "", "\x00\xff"} {
		t.Run(key, func(t *testing.T) {
			seed := &pb.ReducedEffect{Collection: pb.CollectionKind_KEYED, NetAdds: map[string]*pb.ReducedElement{
				key: {NodeId: 42, Data: &pb.DataEffect{Id: []byte(key), Value: &pb.DataEffect_IntVal{IntVal: 10}}},
			}}
			before := proto.Clone(seed)
			m, _ := MapFromSnap[*IntEffect](seed)
			decode := func(entry *pb.ReducedElement) (*IntEffect, error) {
				if string(entry.GetData().GetId()) != key {
					t.Fatal("Get changed the element's raw ID")
				}

				return IntFromElement(entry)
			}

			original, err := m.Get(key, func(entry *pb.ReducedElement) (*IntEffect, error) {
				if entry.GetNodeId() != 42 {
					t.Fatal("Get lost element metadata")
				}

				return decode(entry)
			})
			if err != nil || original.Value() != 10 || len(m.inner) != 0 {
				t.Fatalf("snapshot read: value=%v err=%v", original, err)
			}

			m.Set(key, (&IntEffect{}).Add(2))

			value, err := m.Get(key, IntFromElement)
			if err != nil || value.Value() != 12 || len(value.getEffects()) != 0 {
				t.Fatalf("pending read: value=%v err=%v", value, err)
			}

			value.Add(3)

			unchanged, err := m.Get(key, IntFromElement)
			if err != nil || unchanged.Value() != 12 {
				t.Fatal("editing decoded builder changed the map before Set")
			}

			m.Set(key, value)

			updated, err := m.Get(key, IntFromElement)
			if err != nil || updated.Value() != 15 || len(m.inner) != 2 {
				t.Fatal("Set must compose only the decoded builder's new delta")
			}

			if original.Value() != 10 || unchanged.Value() != 12 || !proto.Equal(seed, before) {
				t.Fatal("Get or Set changed a previous read or the borrowed snapshot")
			}
		})
	}
}

func TestMapGetMissingAndDecoderErrors(t *testing.T) {
	m := &MapEffect[*IntEffect]{}
	missing := func(key string) {
		t.Helper()

		value, err := m.Get(key, func(*pb.ReducedElement) (*IntEffect, error) {
			t.Fatal("decoder called for absent key")

			return nil, nil
		})
		if value != nil || err != nil {
			t.Fatalf("missing %q: value=%v err=%v", key, value, err)
		}
	}
	missing("key")
	m.Set("key", ScalarInt(1).AsEffect()).Remove("key")
	missing("key")
	m.Set("key", ScalarInt(2).AsEffect())

	wantErr := errors.New("cannot decode entry")

	value, err := m.Get("key", func(entry *pb.ReducedElement) (*IntEffect, error) {
		if entry.GetData().GetIntVal() != 2 {
			t.Fatal("Get did not observe reinserted value")
		}

		return nil, wantErr
	})
	if value != nil || err != wantErr {
		t.Fatalf("decoder error: value=%v err=%v", value, err)
	}

	m.Replace(map[string]*IntEffect{"other": ScalarInt(3).AsEffect()})
	missing("key")
	m.Replace(nil)
	missing("other")
}

func TestMapGetStringFromElement(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		name := "raw"
		if compressed {
			name = "compressed"
		}

		t.Run(name, func(t *testing.T) {
			want := strings.Repeat("hello\x00\xff", 64)

			input := ScalarString(want).AsEffect()
			if compressed {
				effects.CompressEffectValues(input.inner[0])

				if input.inner[0].GetData().GetCompressed() == nil {
					t.Fatal("test input was not compressed")
				}
			}

			m := (&MapEffect[*StringEffect]{}).Set("value", input)

			value, err := m.Get("value", StringFromElement)
			if err != nil || value.Value() != want || len(value.inner) != 0 {
				t.Fatalf("Get: value=%v err=%v", value, err)
			}

			m.Set("value", value.Prepend("[").Append("]"))

			updated, err := m.Get("value", StringFromElement)
			if err != nil || updated.Value() != "["+want+"]" || len(m.inner) != 3 {
				t.Fatal("decoded string edits must compose without emitting the base")
			}
		})
	}
}

func TestMapGetFloatFromElement(t *testing.T) {
	m := (&MapEffect[*FloatEffect]{}).Set("value", ScalarFloat(1.5).AsEffect().Add(0.25))

	value, err := m.Get("value", FloatFromElement)
	if err != nil || value.Value() != 1.75 || len(value.inner) != 0 {
		t.Fatalf("Get: value=%v err=%v", value, err)
	}

	m.Set("value", value.Add(0.5))

	updated, err := m.Get("value", FloatFromElement)
	if err != nil || updated.Value() != 2.25 || len(m.inner) != 3 {
		t.Fatal("decoded float edits must compose without emitting the base")
	}
}
