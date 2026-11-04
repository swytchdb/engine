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
	"log/slog"

	"github.com/swytchdb/engine/cluster"
	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
)

// Members returns the current known membership.
func (b *Beacon) Members() []Member {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]Member, 0, len(b.members))
	for id, addr := range b.members {
		out = append(out, Member{NodeID: id, Addr: addr})
	}

	return out
}

// topologyLoop projects membership at startup and after index changes.
// Reads run outside the synchronous engine callback to avoid reentering the
// index. Missing self-membership must not cause re-registration: writing back
// to compensate for a stale read can create a re-registration storm.
func (b *Beacon) topologyLoop() {
	// Initial projection: don't wait for the first signal, or the topology
	// stays pinned to the DNS-synthetic view until something writes the key.
	b.refreshTopology()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-b.topoRefresh:
			b.refreshTopology()
		}
	}
}

// NotifyMembershipChanged signals that the membership key changed. Non-blocking
// and coalescing: a pending signal already means "re-read on the next
// iteration", so extras collapse into it. Safe to call from inside an engine
// callback — it never reenters the engine.
func (b *Beacon) NotifyMembershipChanged() {
	signal(b.topoRefresh)
}

// signal coalesces work without blocking synchronous engine or cloud callbacks.
func signal(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// refreshTopology projects membership and removes predecessors at our address.
// A different ID at our bound host:port belongs to a dead incarnation. Check
// on every change because bootstrap or backfill may reveal it after startup.
// Once removal commits, the entry disappears and the sweep becomes a no-op.
func (b *Beacon) refreshTopology() {
	// Observation must not emit compaction snapshots: their arrival triggers
	// the same observer on every peer and can create a refresh feedback loop.
	snapshot, _, err := b.engine.NewReadOnlyContext().GetSnapshot(MembershipKey)
	if err != nil {
		slog.Debug("beacon: membership snapshot read failed", "error", err)

		return
	}

	var ctx *effects.Context

	selfID := uint64(b.cfg.NodeID)
	for id, addr := range membershipEntries(snapshot) {
		if string(addr) != b.cfg.AdvertiseAddr || id == selfID {
			continue
		}

		slog.Info("beacon: evicting dead predecessor at our address", "old_node_id", id, "addr", b.cfg.AdvertiseAddr)

		if ctx == nil {
			ctx = b.engine.NewContext()
		}

		if err := ctx.Emit(buildMemberRemove(id)); err != nil {
			slog.Error("beacon: predecessor eviction emit failed", "old_node_id", id, "error", err)
		}
	}

	if ctx != nil {
		if err := ctx.Flush(); err != nil {
			slog.Error("beacon: topology refresh flush failed", "error", err)
		}
	}

	b.syncMembership(snapshot)
}

// syncMembership copies the roster and updates the PeerManager only when
// membership changes. Unchanged observations need no map or address copies.
func (b *Beacon) syncMembership(snapshot *pb.ReducedEffect) {
	b.mu.Lock()
	if matchesMembership(snapshot, b.members) {
		b.mu.Unlock()

		return
	}

	members := parseMembership(snapshot)
	b.members = members
	b.mu.Unlock()

	nodes := make([]cluster.NodeConfig, 0, len(members))
	for id, addr := range members {
		nodes = append(nodes, cluster.NodeConfig{
			ID:      cluster.NodeId(id),
			Address: addr,
		})
	}

	b.updateTopology(nodes)

	slog.Info("beacon: topology updated", "members", len(members))
}

// updateTopology applies either discovered candidates or authoritative members.
func (b *Beacon) updateTopology(nodes []cluster.NodeConfig) {
	b.pm.UpdateTopology(&cluster.ClusterConfig{
		NodeID:        b.cfg.NodeID,
		Nodes:         nodes,
		TLSPassphrase: b.cfg.Passphrase,
	})
}
