package effects

import (
	"reflect"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

func TestSafetyGlobPrecedence(t *testing.T) {
	modes := map[string]SafetyMode{
		"*": UnsafeMode, "sessions::*": SafeMode, "sessions::public*": UnsafeMode,
		"sessions::public-admin": SafeMode, `literal\*`: SafeMode,
		"orders:[ab]": SafeMode, "orders:?": UnsafeMode,
	}
	want := []string{"sessions::public-admin", `literal\*`, "sessions::public*", "sessions::*", "orders:?", "orders:[ab]", "*"}

	for range 20 {
		rules := SafetyRules(modes)

		got := make([]string, 0, len(rules))
		for _, rule := range rules {
			got = append(got, rule.Pattern)
		}

		if !reflect.DeepEqual(got, want) {
			t.Fatalf("precedence: %v", got)
		}
	}

	e := NewEngine(EngineConfig{})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})
	e.UpdateSafetyRules(SafeMode, SafetyRules(modes))

	for key, wantMode := range map[string]SafetyMode{
		"sessions::a": SafeMode, "sessions::public1": UnsafeMode,
		"sessions::public-admin": SafeMode, "literal*": SafeMode,
		"other": UnsafeMode, ClusterSafetyKey: UnsafeMode, KeySafetyKey: UnsafeMode,
	} {
		if got := e.SafetyModeForKey(key); got != wantMode {
			t.Errorf("%s: %s want %s", key, got, wantMode)
		}
	}
}

func TestSafetyDecodersRejectInvalidState(t *testing.T) {
	if mode, err := ClusterSafetyMode(nil); err != nil || mode != SafeMode {
		t.Fatal(mode, err)
	}

	for _, value := range []string{"", "SAFE", "true", "1"} {
		if _, err := ParseSafetyMode(value); err == nil {
			t.Fatalf("accepted %q", value)
		}
	}

	if _, err := ClusterSafetyMode(&pb.ReducedEffect{TypeTag: pb.ValueType_TYPE_HASH}); err == nil {
		t.Fatal("accepted hash as cluster default")
	}

	if _, err := KeySafetyModes(&pb.ReducedEffect{TypeTag: pb.ValueType_TYPE_STRING}); err == nil {
		t.Fatal("accepted string as key overrides")
	}

	state := &pb.ReducedEffect{Collection: pb.CollectionKind_KEYED, NetAdds: map[string]*pb.ReducedElement{
		"sessions::*": {Data: &pb.DataEffect{Value: &pb.DataEffect_Raw{Raw: []byte("invalid")}}},
	}}
	if _, err := KeySafetyModes(state); err == nil {
		t.Fatal("accepted invalid override")
	}

	state.TypeTag = pb.ValueType_TYPE_JSON_OBJECT

	state.NetAdds["sessions::*"].Data.Value = &pb.DataEffect_Raw{Raw: []byte("safe")}
	if _, err := KeySafetyModes(state); err == nil {
		t.Fatal("accepted JSON object as hash overrides")
	}
}
