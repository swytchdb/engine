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
	"errors"
	"fmt"
	"log/slog"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/swytchdb/engine/cluster"
	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
)

// bootstrap performs peer discovery (DNS, or the cloud membership roster when
// --cloud is set), installs a temporary topology, and waits for at least one
// peer to become reachable. It does NOT read authoritative membership — that
// happens in joinMembership after self-registration, because
// every node's bootstrap waits for the others to be visible and nobody is
// visible until they've registered themselves. Reading membership before
// registerSelf deadlocks the fleet when nodes start simultaneously.
//
// On return: PeerManager is connected to at least one candidate, or
// we've fallen back to solo mode with a warning.
func (b *Beacon) bootstrap(ctx context.Context) error {
	var candidates []string

	if b.cfg.Cloud != nil {
		var err error

		candidates, err = b.cloudCandidates(ctx)
		if err != nil {
			// Shutdown during discovery. Unlike the DNS path there is no solo
			// fallback to take: we never learned whether a fleet exists.
			return err
		}
	} else {
		var err error

		candidates, err = b.resolveJoinAddrWithRetry(ctx)
		if err != nil {
			slog.Warn("beacon: DNS resolution failed after retries, starting solo", "error", err)

			return nil
		}
	}

	if b.cfg.AdvertiseAddr != "" {
		selfHost, selfPort, _ := net.SplitHostPort(b.cfg.AdvertiseAddr)
		candidates = slices.DeleteFunc(candidates, func(addr string) bool {
			host, port, _ := net.SplitHostPort(addr)

			return host == selfHost && port == selfPort
		})
	}

	if len(candidates) == 0 {
		slog.Info("beacon: no peers found, starting solo")

		return nil
	}

	slog.Info("beacon: discovered candidates", "candidates", candidates)
	b.setTemporaryTopology(candidates)
	b.expectedPeers = len(candidates)

	// Wait for at least one candidate to become reachable so membership
	// subscriptions have a peer to exchange NACKs with.
	waitCtx, waitCancel := context.WithTimeout(ctx, 30*time.Second)
	defer waitCancel()

	if err := b.pm.WaitForAnyPeer(waitCtx); err != nil {
		slog.Warn("beacon: no candidates reachable, starting solo", "error", err)
		// Bootstrap candidates are DNS guesses, not members. Conceding solo
		// ends their relevance: keeping them would dial dead addresses forever
		// (stale DNS from a rescheduled pod, our own other-address-family
		// record) and expectedPeers > 0 would stall the symmetric-peer wait
		// and membership convergence against peers that never come. Joining a
		// cluster that was merely unreachable here is the same human
		// adjudication as any membership repair — restart a node. Real members
		// are never dropped by this: they arrive later through membership
		// effects, which own the topology from then on.
		b.setTemporaryTopology(nil)
		b.expectedPeers = 0

		return nil
	}

	return nil
}

// cloudCandidates reads peer addresses from the cloud roster, excluding self.
// Only a successful empty read permits solo startup. Discovery errors, including
// missing durable blobs, must be retried to avoid silently forking the cluster.
func (b *Beacon) cloudCandidates(ctx context.Context) ([]string, error) {
	reduced, err := b.discoverMembersWithRetry(ctx)
	if err != nil {
		return nil, err
	}

	members := parseMembership(reduced)
	selfID := uint64(b.cfg.NodeID)

	addrs := make([]string, 0, len(members))
	for id, addr := range members {
		if id == selfID {
			continue
		}

		addrs = append(addrs, addr)
	}

	return addrs, nil
}

// discoverMembersWithRetry retries cloud discovery until success or cancellation.
// Unlike DNS discovery, it has no deadline: work grows with membership history,
// and a slow walk does not establish that the cluster is empty. Warn periodically
// so a stalled startup stays visible while cloud reconciliation repairs it.
func (b *Beacon) discoverMembersWithRetry(ctx context.Context) (*pb.ReducedEffect, error) {
	const (
		backoffStart  = 250 * time.Millisecond
		backoffMax    = 3 * time.Second
		complainEvery = 30 * time.Second
	)

	started := time.Now()

	var lastComplaint time.Time // zero value: the first failure warns

	backoff := backoffStart

	for attempt := 1; ; attempt++ {
		reduced, err := b.cfg.Cloud.DiscoverMembers(ctx, MembershipKey)
		if err == nil {
			if attempt > 1 {
				slog.Info("beacon: cloud member discovery succeeded after retries",
					"attempts", attempt, "elapsed", time.Since(started))
			}

			return reduced, nil
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if time.Since(lastComplaint) >= complainEvery {
			slog.Warn("beacon: cloud member discovery failing, still retrying — will NOT start solo",
				"error", err, "attempts", attempt, "elapsed", time.Since(started))

			lastComplaint = time.Now()
		} else {
			slog.Debug("beacon: cloud member discovery failed, retrying", "error", err, "backoff", backoff)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}

		backoff = min(backoff*2, backoffMax)
	}
}

// resolveJoinAddrWithRetry resolves JoinAddr, retrying on error with capped
// backoff until it succeeds or the deadline passes. A resolution error at
// startup is almost always a transient race, not a permanent failure: a
// Kubernetes headless service returns NXDOMAIN ("no such host") until at least
// one peer's endpoint is Ready, so an error means "peers aren't up yet", not
// "there is no cluster". Conceding solo on the first error is what stranded
// nodes in a split-brain membership. A *successful* resolution that returns no
// usable peers is authoritative (we may be the first node) — the caller starts
// solo, and late joiners then converge via the reactive membership projection.
func (b *Beacon) resolveJoinAddrWithRetry(ctx context.Context) ([]string, error) {
	const (
		perAttemptTimeout = 10 * time.Second
		resolveDeadline   = 30 * time.Second
		backoffStart      = 250 * time.Millisecond
		backoffMax        = 3 * time.Second
	)

	deadlineCtx, cancel := context.WithTimeout(ctx, resolveDeadline)
	defer cancel()

	backoff := backoffStart

	for {
		attemptCtx, attemptCancel := context.WithTimeout(deadlineCtx, perAttemptTimeout)
		candidates, err := ResolveJoinAddr(attemptCtx, nil, b.cfg.JoinAddr, b.cfg.ClusterPort)

		attemptCancel()

		if err == nil {
			return candidates, nil
		}

		slog.Debug("beacon: DNS resolution failed, retrying", "error", err, "backoff", backoff)

		select {
		case <-deadlineCtx.Done():
			return nil, err
		case <-time.After(backoff):
		}

		backoff = min(backoff*2, backoffMax)
	}
}

// joinMembership attempts a membership exchange with every known peer before
// serving traffic. A seed's address count is not the cluster's membership count:
// project the exchanged roster and also attempt peers learned through it.
// Failed peers do not hold join open waiting for full convergence; normal
// reconnection and membership observation continue after this initial attempt.
// Self-registration precedes this so simultaneous joiners can discover each other.
func (b *Beacon) joinMembership(ctx context.Context) error {
	attempted := make(map[cluster.NodeId]bool)

	for {
		if err := b.pm.WaitForInitialConnections(ctx); err != nil {
			return err
		}

		peers := slices.DeleteFunc(b.pm.PeerIDs(), func(peer cluster.NodeId) bool {
			return attempted[peer]
		})

		if len(peers) > 0 {
			if err := b.exchangeMembership(ctx, peers); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}

				slog.Warn("beacon: membership exchange incomplete; continuing after attempt", "error", err)
			}

			for _, peer := range peers {
				attempted[peer] = true
			}
		}

		snapshot, _, err := b.engine.NewReadOnlyContext().GetSnapshot(MembershipKey)
		if err != nil {
			return err
		}

		b.syncMembership(parseMembership(snapshot))

		if len(peers) == 0 {
			// Projection can discover peers even without an exchange (e.g.
			// an inbound arrival during bootstrap). Attempt those as well.
			pending := false
			for _, peer := range b.pm.PeerIDs() {
				pending = pending || !attempted[peer]
			}

			if !pending {
				return ctx.Err()
			}
		}
	}
}

// exchangeMembership reannounces the existing membership tips, including our
// registration, and ingests the peers' NACK frontiers before returning. A cached
// read alone does neither. Reuse normal effect replication; no new membership
// records or compaction snapshots are minted by the exchange.
func (b *Beacon) exchangeMembership(ctx context.Context, peers []cluster.NodeId) error {
	_, tips, err := b.engine.NewReadOnlyContext().GetSnapshot(MembershipKey)
	if err != nil {
		return err
	}

	notifies := make([]*pb.OffsetNotify, 0, len(tips))

	for _, tip := range tips {
		eff, ok := b.engine.EffectCache().Get(tip)
		if !ok {
			return fmt.Errorf("membership tip %v unavailable", tip)
		}

		wire, err := effects.MarshalEffect(eff)
		if err != nil {
			return err
		}

		notifies = append(notifies, effects.BuildOffsetNotify(pb.NodeID(tip[0]), tip, eff, wire, ctx))
	}

	type response struct {
		nacks []*pb.NackNotify
		err   error
	}

	// Each worker owns one result; Wait publishes all results before ingestion.
	responses := make([]response, len(peers))

	var wg sync.WaitGroup
	for i, peer := range peers {
		wg.Go(func() {
			res := &responses[i]

			for _, notify := range notifies {
				if ctx.Err() != nil {
					res.err = ctx.Err()

					break
				}

				nacks, err := b.pm.ReplicateTo(notify, notify.GetEffectData(), peer)

				res.nacks = append(res.nacks, nacks...)
				if err != nil {
					res.err = fmt.Errorf("peer %d: %w", peer, err)

					break
				}
			}
		})
	}

	wg.Wait()

	var errs []error

	for _, res := range responses {
		if res.err != nil {
			errs = append(errs, res.err)
		}

		for _, nack := range res.nacks {
			if err := b.engine.HandleNack(nack); err != nil {
				errs = append(errs, err)
			}
		}
	}

	return errors.Join(errs...)
}

// setTemporaryTopology builds a ClusterConfig with the given addresses as
// peers (using synthetic NodeIDs) and applies it to the PeerManager.
// These are temporary — once real membership effects arrive, the topology
// is replaced with actual NodeIDs.
func (b *Beacon) setTemporaryTopology(candidates []string) {
	nodes := make([]cluster.NodeConfig, 1, 1+len(candidates))
	nodes[0] = cluster.NodeConfig{
		ID:      b.cfg.NodeID,
		Address: b.cfg.AdvertiseAddr,
	}

	for i, addr := range candidates {
		// Use synthetic NodeIDs for temporary peers. These are distinguishable
		// from real NodeIDs (which have timestamp in upper 32 bits) by using
		// small sequential values.
		nodes = append(nodes, cluster.NodeConfig{
			ID:      cluster.NodeId(i + 1),
			Address: addr,
		})
	}

	b.updateTopology(nodes)
	slog.Debug("beacon: set temporary topology", "candidates", len(candidates))
}
