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

package beacon

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/swytchdb/engine/cluster"
	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
)

// Config holds beacon configuration parsed from CLI flags.
type Config struct {
	JoinAddr      string    // DNS name to resolve for peer discovery (empty = solo)
	ClusterPort   int       // QUIC listen port for cluster traffic
	AdvertiseAddr string    // host:port this node advertises (empty = auto-detect)
	NodeID        pb.NodeID // ephemeral node ID
	Passphrase    string    // shared mTLS passphrase

	// Cloud, when set, replaces DNS peer discovery: candidate addresses come
	// from the cloud's membership roster instead of resolving JoinAddr.
	Cloud *cluster.CloudSync
}

// Beacon manages cluster discovery and dynamic membership via effects.
type Beacon struct {
	cfg    Config
	engine *effects.Engine
	pm     *cluster.PeerManager

	members       map[uint64]string
	expectedPeers int // DNS-discovered non-self candidate count, set during bootstrap
	mu            sync.RWMutex

	// topoRefresh coalesces "membership key changed" signals. Capacity 1: a
	// pending signal already means "re-read on the next loop iteration", so
	// additional signals during that window collapse into it.
	topoRefresh chan struct{}

	// removeQ accumulates cloud-pushed member removals for memberRemoveLoop.
	// A dashboard cleanup can push thousands in one burst, so they are applied
	// in batches — whatever accumulated while the previous batch flushed drains
	// as one flush — rather than one flush per command. removeSignal coalesces
	// like topoRefresh.
	removeQ      []uint64
	removeMu     sync.Mutex
	removeSignal chan struct{}

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	selfRegistered bool
	stopOnce       sync.Once
	safety         *safetyWatcher
}

// New creates a beacon. Call Start to begin discovery and membership.
func New(cfg Config, engine *effects.Engine, pm *cluster.PeerManager) *Beacon {
	b := &Beacon{
		cfg:          cfg,
		engine:       engine,
		pm:           pm,
		topoRefresh:  make(chan struct{}, 1),
		removeSignal: make(chan struct{}, 1),
		safety:       newSafetyWatcher(engine),
	}
	// Both projections observe index changes, including snapshots and backfill.
	// Callbacks only signal: reconstruction here would reenter the engine.
	engine.SetOnKeyChanged(func(key string) {
		b.safety.notify(key)

		if key == MembershipKey || key == effects.FlushKey {
			b.NotifyMembershipChanged()
		}
	})

	// A cloud-pushed "delete this server" command applies through the same
	// membership REMOVE_OP path the local sweeps use.
	if cfg.Cloud != nil {
		cfg.Cloud.SetMemberRemoveHandler(b.removeMember)
	}

	return b
}

// Start discovers peers, subscribes to membership, registers this
// node, and begins the background heartbeat + membership watch. Blocks
// until initial join completes or ctx is cancelled. The caller is
// expected to gate transport listeners (SQL/redis) on Start returning
// nil — a node that starts serving client traffic before bootstrap
// completes will route writes against a synthetic topology and miss
// remote effects that arrive before its first subscription.
func (b *Beacon) Start(ctx context.Context) (err error) {
	b.ctx, b.cancel = context.WithCancel(ctx)
	// A failed Start owns the same one-shot cleanup as normal shutdown. This
	// rolls back a completed self-registration before returning the error and
	// prevents a later Runtime.Stop from removing it twice.
	defer func() {
		if err != nil {
			b.Stop()
		}
	}()

	// Phase 1: peer discovery (DNS or cloud) + temporary topology + peer
	// reachability. Does not read authoritative membership — that would
	// deadlock when every node waits for the others before registering itself.
	if b.cfg.JoinAddr != "" || b.cfg.Cloud != nil {
		if err := b.bootstrap(b.ctx); err != nil {
			return err
		}
	}

	if b.expectedPeers > 0 {
		// Phase 1.5: Wait for at least one peer to become alive+symmetric
		// via heartbeat exchange. With immediate heartbeats sent on
		// connection (PeerManager wiring), this completes in <100ms.
		// Required so registerSelf's SafeMode replication has valid targets.
		symCtx, symCancel := context.WithTimeout(b.ctx, 10*time.Second)
		err := b.waitForSymmetricPeers(symCtx)

		symCancel()

		if err != nil {
			slog.Warn("beacon: no symmetric peers available, skipping prime subscription", "error", err)
		} else {
			// Phase 1.75: Subscribe to the membership key before registering
			// self. NACKs from peers carry their existing membership tips so
			// the local engine has peer data before registerSelf runs.
			ctx := b.engine.NewContext()
			if _, _, err := ctx.GetSnapshot(MembershipKey); err != nil {
				slog.Debug("beacon: prime subscription incomplete (will retry)", "error", err)
			}

			if err := ctx.Flush(); err != nil {
				slog.Warn("beacon: prime subscription flush failed", "error", err)
			}
		}
	}

	// Phase 2: Register self in membership. Local emit — fast.
	if err := b.registerSelf(); err != nil {
		return err
	}

	// Phase 3: Exchange membership and attempt each discovered peer before
	// opening the transport. Unreachable members need not converge first.
	if err := b.joinMembership(b.ctx); err != nil {
		return err
	}

	// Phase 4: Reactive topology projection. The membership key is a normal
	// key; the connection table is just a projection of it, rebuilt whenever
	// the index changes (signalled via SetOnKeyChanged → NotifyMembershipChanged).
	// No polling, and crucially no write-back: we never re-register to "fix" a
	// stale read — a stale read is a coherence problem to solve in the engine,
	// not paper over here.
	b.wg.Go(b.topologyLoop)

	// Draining only after convergence leaves bootstrap the sole membership
	// writer, so a removal burst can't branch alongside registerSelf. Nothing
	// is missed meanwhile: New wires the handler, and removeQ holds the burst.
	if b.cfg.Cloud != nil {
		b.wg.Go(b.memberRemoveLoop)
	}

	slog.Info("beacon started",
		"node_id", b.cfg.NodeID,
		"advertise", b.cfg.AdvertiseAddr,
		"join", b.cfg.JoinAddr,
		"cloud", b.cfg.Cloud != nil,
	)

	return nil
}

// Stop sends a REMOVE_OP for this node and stops background loops.
func (b *Beacon) Stop() {
	b.stopOnce.Do(func() {
		b.safety.stop()
		b.unregisterSelf()

		if b.cancel != nil {
			b.cancel()
		}

		b.wg.Wait()

		slog.Info("beacon stopped", "node_id", b.cfg.NodeID)
	})
}

// waitForSymmetricPeers blocks until at least one peer is alive+symmetric
// in the health table, or ctx is cancelled.
func (b *Beacon) waitForSymmetricPeers(ctx context.Context) error {
	ht := b.pm.HealthTable()
	if ht == nil {
		return nil
	}

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		if len(ht.AlivePeerIDs()) > 0 {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// registerSelf publishes this node's membership entry. Dead predecessors at
// our address are not handled here: refreshTopology's collision sweep
// evaluates that predicate on every membership change, so it fires whenever
// the stale entry becomes visible — a one-shot sweep at registration would
// silently miss an entry that hasn't bootstrapped in yet.
func (b *Beacon) registerSelf() error {
	if b.selfRegistered {
		return nil
	}

	ctx := b.engine.NewContext()
	if err := ctx.Emit(buildMemberInsert(uint64(b.cfg.NodeID), b.cfg.AdvertiseAddr)); err != nil {
		return err
	}

	if err := ctx.Emit(&pb.Effect{
		Key:  []byte(MembershipKey),
		Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{TypeTag: pb.ValueType_TYPE_HASH}},
	}); err != nil {
		return err
	}

	if err := ctx.Flush(); err != nil {
		return err
	}

	b.selfRegistered = true

	return nil
}

// unregisterSelf removes this node's membership entry after a successful
// registration. Stop's sync.Once makes this shared normal-shutdown and
// failed-start rollback path one-shot.
func (b *Beacon) unregisterSelf() {
	if !b.selfRegistered {
		return
	}

	// A failure here means peers keep our entry and redial us until an operator
	// intervenes, so it must be loud.
	ctx := b.engine.NewContext()
	if err := ctx.Emit(buildMemberRemove(uint64(b.cfg.NodeID))); err != nil {
		slog.Error("beacon: departure remove emit failed", "node_id", b.cfg.NodeID, "error", err)

		return
	}

	if err := ctx.Flush(); err != nil {
		slog.Error("beacon: departure remove flush failed", "node_id", b.cfg.NodeID, "error", err)

		return
	}

	b.selfRegistered = false
}
