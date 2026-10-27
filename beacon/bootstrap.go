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

	candidates = b.filterSelf(candidates)
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

// cloudCandidates discovers peer addresses from the cloud's membership
// roster. A discovery *error* is retried with capped backoff, same as the DNS
// path — a transient cloud hiccup at startup must not strand this node solo
// while the rest of the fleet joins. A *successful* read that yields no
// members is authoritative (fresh cluster, or we're first) and starts solo.
//
// Starting solo is a claim about the whole fleet, so only an authoritative
// answer may produce one: a successful read. "We could not find out" is not
// an answer — conceding solo on it forks the cluster, and a fork is silent (a
// solo node's members count looks exactly like a legitimately fresh one)
// where a stalled boot is loud. A durable frontier hole (ErrCDNBlobMissing)
// is not grounds for solo either: Cloud's own reconcile/billing sweep owns
// healing or pruning holed markers, so this just keeps retrying with the
// existing loud complaint until the cloud does so. So the only error that
// reaches here is shutdown.
func (b *Beacon) cloudCandidates(ctx context.Context) ([]string, error) {
	reduced, err := b.discoverMembersWithRetry(ctx)
	if err != nil {
		return nil, err
	}

	members := parseMembership(reduced)
	selfID := uint64(b.cfg.NodeID)

	addrs := make([]string, 0, len(members))
	for _, m := range members {
		if m.NodeID == selfID {
			continue
		}

		addrs = append(addrs, m.Addr)
	}

	return addrs, nil
}

// discoverMembersWithRetry reads the cloud membership roster, retrying on
// error with capped backoff until it succeeds or ctx is done. Same rationale
// as resolveJoinAddrWithRetry: an error means "the cloud isn't reachable yet",
// not "there is no cluster", and conceding solo on the first error is what
// strands nodes in a split-brain membership.
//
// There is deliberately no overall deadline, and no per-attempt one. Neither
// bounds anything real: a discovery walk's cost is the membership DAG's depth,
// which grows with the fleet's history, so a fixed budget is a bound on
// unbounded work and every cluster eventually outgrows it. What the budget
// actually produced on expiry was an error indistinguishable from an outage,
// which the caller resolved by forking the cluster. Taking a long time is a
// latency problem and belongs in the logs; it is never grounds to answer a
// question we did not get an answer to. The one hard stop is ctx — the node is
// shutting down, and then there is nothing to be correct about.
func (b *Beacon) discoverMembersWithRetry(ctx context.Context) (*pb.ReducedEffect, error) {
	const (
		backoffStart = 250 * time.Millisecond
		backoffMax   = 3 * time.Second
		// A boot that cannot reach the cloud is stuck until an operator acts,
		// so complain on the first failure and keep complaining. Silence here
		// is what let the old deadline pass for normal startup.
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

	var lastErr error

	for {
		attemptCtx, attemptCancel := context.WithTimeout(deadlineCtx, perAttemptTimeout)
		candidates, err := ResolveJoinAddr(attemptCtx, nil, b.cfg.JoinAddr, b.cfg.ClusterPort)

		attemptCancel()

		if err == nil {
			return candidates, nil
		}

		lastErr = err
		slog.Debug("beacon: DNS resolution failed, retrying", "error", err, "backoff", backoff)

		select {
		case <-deadlineCtx.Done():
			return nil, lastErr
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

		var peers []cluster.NodeId

		for _, peer := range b.pm.PeerIDs() {
			if !attempted[peer] {
				peers = append(peers, peer)
			}
		}

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

		b.syncMembership(snapshot)

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

	var notifies []*pb.OffsetNotify

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

	responses := make(chan response, len(peers))

	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Go(func() {
			var res response

			for _, notify := range notifies {
				if ctx.Err() != nil {
					res.err = errors.Join(res.err, ctx.Err())

					break
				}

				nacks, err := b.pm.ReplicateTo(notify, notify.GetEffectData(), peer)

				res.nacks = append(res.nacks, nacks...)
				if err != nil {
					res.err = fmt.Errorf("peer %d: %w", peer, err)

					break
				}
			}

			responses <- res
		})
	}

	wg.Wait()
	close(responses)

	var errs []error
	for res := range responses {
		errs = append(errs, res.err)
		for _, nack := range res.nacks {
			errs = append(errs, b.engine.HandleNack(nack))
		}
	}

	return errors.Join(errs...)
}

// filterSelf removes addresses that match this node's advertise address.
func (b *Beacon) filterSelf(candidates []string) []string {
	selfAddr := b.cfg.AdvertiseAddr
	if selfAddr == "" {
		return candidates
	}

	selfHost, selfPort, _ := net.SplitHostPort(selfAddr)

	filtered := make([]string, 0, len(candidates))
	for _, addr := range candidates {
		host, port, _ := net.SplitHostPort(addr)
		if host == selfHost && port == selfPort {
			continue
		}

		filtered = append(filtered, addr)
	}

	return filtered
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
		syntheticID := cluster.NodeId(uint64(i + 1))
		nodes = append(nodes, cluster.NodeConfig{
			ID:      syntheticID,
			Address: addr,
		})
	}

	cfg := &cluster.ClusterConfig{
		NodeID:        b.cfg.NodeID,
		Nodes:         nodes,
		TLSPassphrase: b.cfg.Passphrase,
	}

	b.pm.UpdateTopology(cfg)
	slog.Debug("beacon: set temporary topology", "candidates", len(candidates))
}
