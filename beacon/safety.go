package beacon

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/swytchdb/engine/effects"
)

// safetyWatcher projects ordinary subscribed keys into the engine's immutable
// safety configuration. Callbacks only enqueue; reconstruction runs off-path.
type safetyWatcher struct {
	engine  *effects.Engine
	changed chan struct{}
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

func newSafetyWatcher(engine *effects.Engine) *safetyWatcher {
	w := &safetyWatcher{engine: engine, changed: make(chan struct{}, 1)}
	engine.SetOnKeyChanged(func(key string) {
		if key != effects.ClusterSafetyKey && key != effects.KeySafetyKey && key != effects.FlushKey {
			return
		}

		select {
		case w.changed <- struct{}{}:
		default:
		}
	})

	return w
}

func (w *safetyWatcher) refresh() error {
	// A normal read can answer a definite miss without subscribing. Policies
	// must stay subscribed even when no node has written them yet.
	for _, key := range []string{effects.ClusterSafetyKey, effects.KeySafetyKey} {
		if err := w.engine.Subscribe(key); err != nil {
			return err
		}
	}

	// Observation must not compact: a compaction effect triggers observers on
	// every node and can turn concurrent snapshots into a feedback loop.
	ctx := w.engine.NewReadOnlyContext()

	cluster, _, err := ctx.GetSnapshot(effects.ClusterSafetyKey)
	if err != nil {
		return err
	}

	keys, _, err := ctx.GetSnapshot(effects.KeySafetyKey)
	if err != nil {
		return err
	}

	mode, err := effects.ClusterSafetyMode(cluster)
	if err != nil {
		return err
	}

	overrides, err := effects.KeySafetyModes(keys)
	if err != nil {
		return err
	}

	w.engine.UpdateSafetyRules(mode, effects.SafetyRules(overrides))

	return nil
}

func (w *safetyWatcher) start(parent context.Context) error {
	ctx, cancel := context.WithCancel(parent)

	w.cancel = cancel
	if err := w.refresh(); err != nil {
		cancel()

		return fmt.Errorf("load safety configuration: %w", err)
	}

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()

		var retry <-chan time.Time

		for {
			select {
			case <-ctx.Done():
				return
			case <-w.changed:
			case <-retry:
			}

			retry = nil

			if err := w.refresh(); err != nil {
				slog.Warn("beacon: safety configuration update failed; retaining previous policy", "error", err)

				retry = time.After(time.Second)
			}
		}
	}()

	return nil
}

func (w *safetyWatcher) stop() {
	if w.cancel != nil {
		w.cancel()
	}

	w.wg.Wait()
}

// startSafety runs after Start has completed membership bootstrap. Replacing
// temporary peer IDs with real membership can reconnect the transport; wait
// for that connection before the first policy subscription.
func (b *Beacon) startSafety(ctx context.Context) error {
	if b.expectedPeers > 0 {
		waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		if err := b.pm.WaitForAnyPeer(waitCtx); err != nil {
			return fmt.Errorf("wait for post-bootstrap policy peer: %w", err)
		}
	}

	return b.safety.start(ctx)
}
