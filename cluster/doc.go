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

// Package cluster connects engine instances and replicates their effects.
//
// PeerManager owns topology and connection lifecycle. PeerConn maintains one
// outbound connection and shares fetch/forward framing through requestOverConn.
// QUICNotifyTransport carries notifications and dispatches incoming packets;
// Replicator owns acknowledgement, retry, and completion policy.
// HeartbeatManager measures peers; PeerHealthTable exposes their health.
// PubSubRouter routes ephemeral subscriptions and messages through RouterTransport.
// CloudSync separately owns the durable cloud upload outbox and cloud reads.
// Engine integration uses the EffectHandler, LogReader, and KeyFilterProvider
// interfaces rather than putting engine behavior in the transport.
//
// Callers serialize startup and shutdown and wire callbacks before startup,
// unless a setter explicitly supports later updates. Peer lifecycle callbacks
// must run outside PeerManager.mu because they can re-enter the manager.
// PeerConn publishes its connection atomically; it never holds a mutex during
// network I/O. Replication and heartbeat membership snapshots are immutable:
// membership writers replace them, while sends retain the old snapshot.
// Replication readers load their snapshot atomically; only writers take a lock.
// PubSubRouter keeps sorted subscription slices under one lock, releasing peak
// capacity after churn. Heartbeat observations reuse a sorted slice; encoding
// finishes under its lock before the independent packet is sent.
//
// CloudSync.mu protects outbox membership, queue ordering, and key pin ownership
// together. Its filter and known-cloud caches have separate locks so reads do
// not wait on outbox work. An upload keeps its effect and ancestry policy in
// one entry; draining the outbox releases its map and queue storage.
// cloud_outbox.go owns that lifecycle, cloud_read.go owns reads and reconciliation,
// and cloud_discovery.go reconstructs membership without mutating the engine.
package cluster
