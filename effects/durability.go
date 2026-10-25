package effects

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"runtime/metrics"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/durability"
	"github.com/swytchdb/engine/keytrie"
)

var errRecoveryMemory = errors.New("not enough free memory to recover state")

const recoveryMemoryWarning = "not enough free memory to recover state, please restart with more memory. Deleting storage and continuing in 30s"

type recoveryRun struct {
	cancel context.CancelCauseFunc
	count  atomic.Uint64
}

func (r *recoveryRun) checkMemory(target int64) {
	if target <= 0 {
		return
	}
	// Unlike heap/live (last GC), heap/objects includes allocations made by
	// recovery since the last collection. Sample without a stop-the-world read.
	samples := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(samples)

	if samples[0].Value.Uint64() > uint64(target) {
		r.cancel(errRecoveryMemory)
	}
}

// NewEngine preserves the original convenience constructor. Applications using
// DataDir should call OpenEngine to handle startup errors and cancellation.
// NewEngine panics if configured durable storage cannot be opened or recovered.
func NewEngine(cfg EngineConfig) *Engine {
	e, err := OpenEngine(context.Background(), cfg)
	if err != nil {
		panic(err)
	}

	return e
}

// OpenEngine restores local effects before returning an engine ready to serve.
// A new incarnation ID is minted on every start; restored effects keep their
// original IDs, so the new offset sequence never reuses a persisted identity.
func OpenEngine(ctx context.Context, cfg EngineConfig) (*Engine, error) {
	e := newEngine(cfg)
	if cfg.DataDir == "" {
		e.startMemoryGovernor(e.memTarget)

		return e, nil
	}

	if err := e.openDurability(ctx, cfg.DataDir, 30*time.Second); err != nil {
		return nil, errors.Join(err, e.Close())
	}

	return e, nil
}

func (e *Engine) openDurability(ctx context.Context, dir string, grace time.Duration) error {
	recoverCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	r := &recoveryRun{cancel: cancel}
	e.recovery.Store(r)
	e.startMemoryGovernor(e.memTarget)
	// Opening TOCs allocates too. Watch this phase as well as protobuf decoding,
	// without waiting for the normal one-second governor tick.
	watchCtx, stopWatch := context.WithCancel(recoverCtx)

	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)

		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-watchCtx.Done():
				return
			case <-ticker.C:
				r.checkMemory(e.memTarget)
			}
		}
	}()

	defer func() { stopWatch(); <-watchDone }()

	s, err := durability.OpenContext(recoverCtx, dir)

	owned := false
	defer func() {
		if !owned && s != nil {
			if err := s.Close(); err != nil {
				slog.Error("close durability after recovery failure", "error", err)
			}
		}
	}()

	start := time.Now()

	if err == nil {
		err = e.restoreEffects(recoverCtx, s, r)
	}

	stopWatch()
	<-watchDone
	// End the recovery/governor handshake before swapping any in-memory state.
	if e.memGovStop != nil {
		close(e.memGovStop)
		e.memGovWg.Wait()
		e.memGovStop = nil
	}

	if err == nil {
		err = context.Cause(recoverCtx)
	}

	if errors.Is(err, errRecoveryMemory) {
		// Do not use FlushIndex: this discards partial RAM only, preserving the
		// mirror until the grace period expires without cancellation.
		if closeErr := e.index.Close(); closeErr != nil {
			return closeErr
		}

		e.index = keytrie.NewCritbit[leafState]()
		e.effectCache = newVertexPool()
		e.evictBudget.Store(0)
		e.ownKeyFilter = CuckooChain{}
		e.ownFilterVer = 0
		e.ownFilterBytes = nil
		e.ownFilterBytesVer = 0

		runtime.GC()
		slog.Error(recoveryMemoryWarning, "data_dir", dir, "memory_limit", e.memTarget)

		timer := time.NewTimer(grace)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		if s != nil {
			if err := s.Reset(); err != nil {
				return fmt.Errorf("delete mirror after recovery memory pressure: %w", err)
			}
		} else {
			if err := durability.Discard(dir); err != nil {
				return err
			}

			s, err = durability.Open(dir)
			if err != nil {
				return err
			}
		}

		err = nil
	}

	if err != nil {
		return fmt.Errorf("recover durability: %w", err)
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	e.effectCache.mirror = s
	owned = true

	e.recovery.Store(nil)
	e.startMemoryGovernor(e.memTarget)
	slog.Info("durability recovery complete", "effects", e.VertexCount(), "keys", e.KeyCount(), "elapsed", time.Since(start))

	return nil
}

// A transient per-key edge set makes frontier construction independent of file
// and worker order. An edge can arrive before its vertex. This avoids repeatedly
// copying a growing tipset for a large hot key (quadratic recovery).
type recoveryShard struct {
	mu   sync.Mutex
	keys map[string]map[Tip]bool // true: present frontier candidate; false: consumed
}

func (e *Engine) restoreEffects(ctx context.Context, s *durability.Store, rn *recoveryRun) error {
	var shards [256]recoveryShard

	add := func(key []byte, id Tip, deps []*pb.EffectRef, extra *pb.EffectRef) {
		h := uint32(2166136261)
		for _, v := range key {
			h = (h ^ uint32(v)) * 16777619
		}

		sh := &shards[h%256]
		sh.mu.Lock()
		defer sh.mu.Unlock()

		if sh.keys == nil {
			sh.keys = make(map[string]map[Tip]bool)
		}

		k := string(key)

		tips := sh.keys[k]
		if tips == nil {
			tips = make(map[Tip]bool)
			sh.keys[k] = tips
		}

		if _, seen := tips[id]; !seen {
			tips[id] = true
		}

		for _, dep := range deps {
			tips[r(dep)] = false
		}

		if extra != nil {
			tips[r(extra)] = false
		}
	}

	err := s.Recover(ctx, 0, func(id durability.ID, data []byte) error {
		if rn.count.Add(1)%256 == 1 {
			rn.checkMemory(e.memTarget)
		}

		if err := ctx.Err(); err != nil {
			return context.Cause(ctx)
		}

		eff := new(pb.Effect)
		if err := UnmarshalEffect(data, eff); err != nil {
			return fmt.Errorf("decode effect %v: %w", id, err)
		}

		if err := e.effectCache.PutSized(id, eff, len(data)); err != nil {
			return err
		}

		if bind := eff.GetTxnBind(); bind != nil && len(bind.GetKeys()) > 0 {
			for _, kb := range bind.GetKeys() {
				add(kb.GetKey(), id, kb.GetConsumedTips(), kb.GetNewTip())
			}
		} else {
			add(eff.GetKey(), id, eff.GetDeps(), nil)
		}

		return nil
	})
	if err != nil {
		return err
	}
	// Install complete frontiers in parallel; there is no serving or eviction
	// during this phase. Memory pressure still cancels the entire recovery.
	jobs := make(chan *recoveryShard)

	var wg sync.WaitGroup
	for range runtime.GOMAXPROCS(0) {
		wg.Go(func() {
			for sh := range jobs {
				for key, candidates := range sh.keys {
					if ctx.Err() != nil {
						break
					}

					tips := make([]Tip, 0, 1)

					for id, live := range candidates {
						if live {
							tips = append(tips, id)
						}
					}

					if len(tips) > 0 {
						e.index.Insert(key, nil, keytrie.NewTipSet(tips...))
						e.ownFilterAdd(key)
					}

					delete(sh.keys, key)

					if rn.count.Add(1)%256 == 1 {
						rn.checkMemory(e.memTarget)
					}
				}

				sh.keys = nil
			}
		})
	}

	for i := range shards {
		jobs <- &shards[i]
	}

	close(jobs)
	wg.Wait()
	rn.checkMemory(e.memTarget)

	return context.Cause(ctx)
}
