package beacon

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/swytchdb/engine/effects"
)

func TestRuntimeRestartsWithInvalidSafetyConfiguration(t *testing.T) {
	for _, key := range []string{effects.ClusterSafetyKey, effects.KeySafetyKey} {
		t.Run(key, func(t *testing.T) {
			dir := t.TempDir()

			e, err := effects.OpenEngine(t.Context(), effects.EngineConfig{DataDir: dir, MemoryLimit: 256 << 20})
			if err != nil {
				t.Fatal(err)
			}

			t.Cleanup(func() {
				if err := e.Close(); err != nil {
					t.Error(err)
				}
			})
			emitSafetyTest(t, e, effects.ClusterSafetyKey, "", "unsafe", false)
			emitSafetyTest(t, e, effects.KeySafetyKey, "sessions::*", "unsafe", false)

			field := ""
			if key == effects.KeySafetyKey {
				field = "sessions::*"
			}

			emitSafetyTest(t, e, key, field, "typo", false)

			if err := e.Close(); err != nil {
				t.Fatal(err)
			}

			rt, err := NewRuntime(t.Context(), RuntimeConfig{DataDir: dir, MemoryLimit: 256 << 20})
			if err != nil {
				t.Fatalf("invalid recovered policy prevented startup: %v", err)
			}

			t.Cleanup(func() {
				if err := rt.Stop(); err != nil {
					t.Error(err)
				}
			})

			for _, applicationKey := range []string{"other", "sessions::a"} {
				if got := rt.Engine.SafetyModeForKey(applicationKey); got != effects.SafeMode {
					t.Fatalf("startup policy for %s = %s, want safe", applicationKey, got)
				}
			}

			// A correction to the restored key must resume normal policy updates.
			emitSafetyTest(t, rt.Engine, key, field, "unsafe", false)
			waitSafetyTest(t, rt.Engine, map[string]effects.SafetyMode{"other": effects.UnsafeMode, "sessions::a": effects.UnsafeMode})
		})
	}
}

func TestRuntimeJoinsWithInvalidSafetyConfiguration(t *testing.T) {
	for _, async := range []bool{false, true} {
		for _, key := range []string{effects.ClusterSafetyKey, effects.KeySafetyKey} {
			t.Run(fmt.Sprintf("async=%t/%s", async, key), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
				defer cancel()

				first, address := startInvalidSafetyRuntimeTest(t, ctx, "", false)
				emitSafetyTest(t, first.Engine, effects.ClusterSafetyKey, "", "unsafe", false)
				emitSafetyTest(t, first.Engine, effects.KeySafetyKey, "sessions::*", "unsafe", false)
				waitSafetyTest(t, first.Engine, map[string]effects.SafetyMode{"other": effects.UnsafeMode, "sessions::a": effects.UnsafeMode})

				field := ""
				if key == effects.KeySafetyKey {
					field = "sessions::*"
				}

				emitSafetyTest(t, first.Engine, key, field, "typo", false)
				second, _ := startInvalidSafetyRuntimeTest(t, ctx, address, async)
				joined := make(chan struct{})

				go func() {
					second.joinWG.Wait()
					close(joined)
				}()

				select {
				case <-joined:
				case <-ctx.Done():
					t.Fatal("asynchronous join did not finish")
				}

				for _, rt := range []*Runtime{first, second} {
					waitSafetyTest(t, rt.Engine, map[string]effects.SafetyMode{"other": effects.SafeMode, "sessions::a": effects.SafeMode})
				}

				// Defaults alone cannot prove that an async join kept its watcher.
				// Repair on the peer and require both subscriptions to apply it.
				emitSafetyTest(t, first.Engine, key, field, "unsafe", false)

				for _, rt := range []*Runtime{first, second} {
					waitSafetyTest(t, rt.Engine, map[string]effects.SafetyMode{"other": effects.UnsafeMode, "sessions::a": effects.UnsafeMode})
				}
			})
		}
	}
}

func startInvalidSafetyRuntimeTest(t *testing.T, ctx context.Context, join string, async bool) (*Runtime, string) {
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
		ClusterPassphrase: t.Name(), ClusterPort: port, AdvertiseAddr: address, JoinAddr: join, AsyncJoin: async,
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
