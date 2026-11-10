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
	"slices"
	"sync"
	"time"

	"github.com/swytchdb/engine/cluster"
	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
)

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

		if err := b.projectJoinMembership(ctx); err != nil {
			return err
		}

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

// projectJoinMembership keeps the startup gate closed while ancestry is
// temporarily unavailable. Match topologyLoop's backoff, but give startup a
// finite retry budget. Snapshot reads are synchronous: an in-flight fetch still
// uses its transport timeout, and must finish before startup can clean up.
func (b *Beacon) projectJoinMembership(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	backoff := 100 * time.Millisecond

	var lastErr error

	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("initial membership projection: %w", errors.Join(err, lastErr))
		}

		snapshot, _, err := b.engine.NewReadOnlyContext().GetSnapshot(MembershipKey)
		// A read that completes after the deadline must not open the gate.
		if ctx.Err() != nil {
			return fmt.Errorf("initial membership projection: %w", errors.Join(ctx.Err(), err, lastErr))
		}

		if err == nil {
			b.syncMembership(snapshot)

			return nil
		}

		lastErr = err
		slog.Debug("beacon: initial membership projection failed; retrying", "error", err, "backoff", backoff)
		timer := time.NewTimer(backoff)

		select {
		case <-ctx.Done():
			timer.Stop()
		case <-b.topoRefresh:
			timer.Stop()
		case <-timer.C:
		}

		backoff = min(2*backoff, 3*time.Second)
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

		wire, err := b.engine.EffectCache().MarshalEffect(eff)
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
