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

import "log/slog"

// removeMember queues a cloud-pushed "delete this server" command for
// memberRemoveLoop. Applying it here would run inside the cloud stream's
// readLoop, one flush per command — a burst of thousands would serialize
// against it.
func (b *Beacon) removeMember(nodeID uint64) {
	b.removeMu.Lock()
	b.removeQ = append(b.removeQ, nodeID)
	b.removeMu.Unlock()

	select {
	case b.removeSignal <- struct{}{}:
	default:
	}
}

// memberRemoveLoop applies queued cloud-pushed removals in batches.
func (b *Beacon) memberRemoveLoop() {
	defer b.wg.Done()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-b.removeSignal:
		}

		b.applyQueuedRemovals()
	}
}

// applyQueuedRemovals emits a batch with one flush, then compacts it through a
// fresh context. The emitting context cannot compact a key with pending effects.
// Compaction bounds future cloud discovery work after large removal bursts.
func (b *Beacon) applyQueuedRemovals() {
	b.removeMu.Lock()
	batch := b.removeQ
	b.removeQ = nil
	b.removeMu.Unlock()

	if len(batch) == 0 {
		return
	}

	// Cloud repeats removals until their TTL expires. Already-absent members
	// must be a no-op. If the roster read fails, apply unfiltered: redundant
	// removals are safer than dropping a requested removal.
	roster, _, err := b.engine.NewReadOnlyContext().GetSnapshot(MembershipKey)
	if err != nil {
		slog.Warn("beacon: roster read before removals failed, applying unfiltered", "error", err)
	}

	offered := len(batch)

	toApply := batch
	if err == nil {
		// The drained batch is exclusively ours. Filter it in place and look
		// up only the offered IDs instead of decoding the entire roster.
		toApply = batch[:0]
		for _, nodeID := range batch {
			elem := roster.GetNetAdds()[string(nodeIDBytes(nodeID))]
			if nodeID != 0 && len(elem.GetData().Decompress()) > 0 {
				toApply = append(toApply, nodeID)
			}
		}

		if len(toApply) == 0 {
			slog.Debug("beacon: member removals already absent", "offered", offered)

			return
		}
	}

	ctx := b.engine.NewContext()
	for _, nodeID := range toApply {
		if err := ctx.Emit(buildMemberRemove(nodeID)); err != nil {
			slog.Error("beacon: member remove emit failed", "node_id", nodeID, "error", err)
		}
	}

	if err := ctx.Flush(); err != nil {
		slog.Error("beacon: member remove flush failed", "batch", len(toApply), "error", err)

		return
	}

	slog.Info("beacon: applied member removals",
		"batch", len(toApply), "skipped_absent", offered-len(toApply))

	cctx := b.engine.NewContext()
	if _, _, err := cctx.GetSnapshot(MembershipKey); err != nil {
		slog.Error("beacon: post-removal compaction read failed", "error", err)
	}

	if err := cctx.Flush(); err != nil {
		slog.Error("beacon: post-removal compaction flush failed", "error", err)
	}
}
