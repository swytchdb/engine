package beacon

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
)

func emitSafetyTest(t *testing.T, e *effects.Engine, key, field, value string, remove bool) {
	t.Helper()

	ctx := e.NewContext()

	data := &pb.DataEffect{Op: pb.EffectOp_INSERT_OP, Merge: pb.MergeRule_LAST_WRITE_WINS, Value: &pb.DataEffect_Raw{Raw: []byte(value)}}
	if field != "" {
		data.Collection = pb.CollectionKind_KEYED
		data.Id = []byte(field)
	}

	if remove {
		data.Op = pb.EffectOp_REMOVE_OP
	}

	if err := ctx.Emit(&pb.Effect{Key: []byte(key), Kind: &pb.Effect_Data{Data: data}}); err != nil {
		t.Fatal(err)
	}

	if err := ctx.Flush(); err != nil {
		t.Fatal(err)
	}
}

func waitSafetyTest(t *testing.T, e *effects.Engine, want map[string]effects.SafetyMode) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)

	for {
		match := true
		got := make(map[string]effects.SafetyMode, len(want))

		for key, mode := range want {
			got[key] = e.SafetyModeForKey(key)
			if got[key] != mode {
				match = false
			}
		}

		if match {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("node %d policy did not update: got %v, want %v", e.NodeID(), got, want)
		}

		time.Sleep(time.Millisecond)
	}
}

func TestSafetyWatcherLoadsAndTracksOrdinaryEffects(t *testing.T) {
	e := effects.NewEngine(effects.EngineConfig{})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})
	emitSafetyTest(t, e, effects.ClusterSafetyKey, "", "unsafe", false)
	emitSafetyTest(t, e, effects.KeySafetyKey, "sessions::*", "safe", false)

	w := newSafetyWatcher(e)
	if err := w.start(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer w.stop()

	waitSafetyTest(t, e, map[string]effects.SafetyMode{"other": effects.UnsafeMode, "sessions::a": effects.SafeMode})
	emitSafetyTest(t, e, effects.KeySafetyKey, "sessions::a", "unsafe", false)
	waitSafetyTest(t, e, map[string]effects.SafetyMode{"sessions::a": effects.UnsafeMode, "sessions::b": effects.SafeMode})
	emitSafetyTest(t, e, effects.KeySafetyKey, "sessions::a", "", true)
	waitSafetyTest(t, e, map[string]effects.SafetyMode{"sessions::a": effects.SafeMode})
	emitSafetyTest(t, e, effects.KeySafetyKey, "", "", true)
	waitSafetyTest(t, e, map[string]effects.SafetyMode{"sessions::a": effects.UnsafeMode})
	emitSafetyTest(t, e, effects.ClusterSafetyKey, "", "", true)
	waitSafetyTest(t, e, map[string]effects.SafetyMode{"other": effects.SafeMode, "sessions::a": effects.SafeMode})
}

func TestSafetyWatcherInvalidConfigurationUsesDefaults(t *testing.T) {
	for _, tc := range []struct {
		name, key, field, value string
	}{
		{"cluster mode", effects.ClusterSafetyKey, "", "typo"},
		{"cluster type", effects.ClusterSafetyKey, "mode", "unsafe"},
		{"override mode", effects.KeySafetyKey, "bad:*", "typo"},
		{"override type", effects.KeySafetyKey, "", "unsafe"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := effects.NewEngine(effects.EngineConfig{})

			t.Cleanup(func() {
				if err := e.Close(); err != nil {
					t.Error(err)
				}
			})

			w := newSafetyWatcher(e)
			emitSafetyTest(t, e, effects.ClusterSafetyKey, "", "unsafe", false)
			emitSafetyTest(t, e, effects.KeySafetyKey, "sessions::*", "unsafe", false)

			if err := w.refresh(); err != nil {
				t.Fatal(err)
			}

			emitSafetyTest(t, e, tc.key, "", "", true)
			emitSafetyTest(t, e, tc.key, tc.field, tc.value, false)

			if err := w.refresh(); err != nil {
				t.Fatalf("invalid stored policy should fall back, not fail: %v", err)
			}

			// Neither a previous policy nor the valid sibling key may leave
			// application keys unsafe. Internal keys retain their exception.
			want := map[string]effects.SafetyMode{
				"other": effects.SafeMode, "sessions::a": effects.SafeMode,
				"bad:a": effects.SafeMode, effects.ClusterSafetyKey: effects.UnsafeMode,
				effects.KeySafetyKey: effects.UnsafeMode,
			}
			for key, mode := range want {
				if got := e.SafetyModeForKey(key); got != mode {
					t.Fatalf("fallback policy for %s = %s, want %s", key, got, mode)
				}
			}

			if err := w.start(t.Context()); err != nil {
				t.Fatalf("invalid stored policy prevented watcher startup: %v", err)
			}
			defer w.stop()

			// Repair both keys after startup. The subscription worker must
			// still be alive and apply the repaired policy.
			emitSafetyTest(t, e, effects.ClusterSafetyKey, "", "", true)
			emitSafetyTest(t, e, effects.KeySafetyKey, "", "", true)
			emitSafetyTest(t, e, effects.ClusterSafetyKey, "", "unsafe", false)
			emitSafetyTest(t, e, effects.KeySafetyKey, "sessions::*", "safe", false)
			waitSafetyTest(t, e, map[string]effects.SafetyMode{"other": effects.UnsafeMode, "sessions::a": effects.SafeMode})
		})
	}
}

func TestStandaloneRuntimeWatchesSafetyKeys(t *testing.T) {
	rt, err := NewRuntime(t.Context(), RuntimeConfig{})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := rt.Stop(); err != nil {
			t.Error(err)
		}
	})
	emitSafetyTest(t, rt.Engine, effects.ClusterSafetyKey, "", "unsafe", false)
	waitSafetyTest(t, rt.Engine, map[string]effects.SafetyMode{"other": effects.UnsafeMode})
	emitSafetyTest(t, rt.Engine, effects.ClusterSafetyKey, "", "typo", false)
	waitSafetyTest(t, rt.Engine, map[string]effects.SafetyMode{"other": effects.SafeMode})
	emitSafetyTest(t, rt.Engine, effects.ClusterSafetyKey, "", "unsafe", false)
	waitSafetyTest(t, rt.Engine, map[string]effects.SafetyMode{"other": effects.UnsafeMode})
	emitSafetyTest(t, rt.Engine, effects.KeySafetyKey, "sessions::*", "typo", false)
	waitSafetyTest(t, rt.Engine, map[string]effects.SafetyMode{"other": effects.SafeMode, "sessions::a": effects.SafeMode})
	emitSafetyTest(t, rt.Engine, effects.KeySafetyKey, "sessions::*", "safe", false)
	waitSafetyTest(t, rt.Engine, map[string]effects.SafetyMode{"other": effects.UnsafeMode, "sessions::a": effects.SafeMode})

	if err := rt.Engine.FlushIndex(); err != nil {
		t.Fatalf("FlushIndex: %v", err)
	}

	waitSafetyTest(t, rt.Engine, map[string]effects.SafetyMode{"other": effects.SafeMode})
}

func TestSafetyWatcherRefreshDoesNotEmitCompaction(t *testing.T) {
	e := effects.NewEngine(effects.EngineConfig{})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})

	for range 20 {
		emitSafetyTest(t, e, effects.ClusterSafetyKey, "", "unsafe", false)
	}

	w := newSafetyWatcher(e)
	if err := w.refresh(); err != nil {
		t.Fatal(err)
	}

	var minted atomic.Int32

	e.SetOnLocalEffect(func(_ effects.Tip, _ *pb.Effect) { minted.Add(1) })

	for range 20 {
		if err := w.refresh(); err != nil {
			t.Fatal(err)
		}
	}

	if minted.Load() != 0 {
		t.Fatalf("observation emitted %d effects", minted.Load())
	}
}

func TestRuntimeSafetySubscriptionsAndLateJoin(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	start := func(join string) (*Runtime, string) {
		t.Helper()

		listener, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}

		port := listener.LocalAddr().(*net.UDPAddr).Port
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}

		address := fmt.Sprintf("127.0.0.1:%d", port)
		if join == "" {
			join = address
		}

		rt, err := NewRuntime(ctx, RuntimeConfig{
			ClusterPassphrase: t.Name(), ClusterPort: port, AdvertiseAddr: address, JoinAddr: join,
		})
		if err != nil {
			t.Fatal(err)
		}

		t.Cleanup(func() {
			if err := rt.Stop(); err != nil {
				t.Error(err)
			}
		})

		return rt, address
	}

	first, address := start("")
	second, _ := start(address)
	// Both policies were absent when these nodes started. Only the watcher
	// subscribes: do not read the backing keys to make propagation happen.
	emitSafetyTest(t, second.Engine, effects.ClusterSafetyKey, "", "unsafe", false)
	emitSafetyTest(t, second.Engine, effects.KeySafetyKey, "sessions::*", "safe", false)

	want := map[string]effects.SafetyMode{"other": effects.UnsafeMode, "sessions::a": effects.SafeMode}
	waitSafetyTest(t, first.Engine, want)
	waitSafetyTest(t, second.Engine, want)

	third, _ := start(address)
	for key, mode := range want {
		if got := third.Engine.SafetyModeForKey(key); got != mode {
			t.Fatalf("startup policy for %s = %s, want %s", key, got, mode)
		}
	}

	emitSafetyTest(t, first.Engine, effects.ClusterSafetyKey, "", "safe", false)
	emitSafetyTest(t, first.Engine, effects.KeySafetyKey, "sessions::*", "unsafe", false)

	for _, rt := range []*Runtime{first, second, third} {
		waitSafetyTest(t, rt.Engine, map[string]effects.SafetyMode{"other": effects.SafeMode, "sessions::a": effects.UnsafeMode})
	}
}
