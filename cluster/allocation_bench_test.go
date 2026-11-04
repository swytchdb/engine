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

package cluster

import (
	"bytes"
	"testing"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
)

func BenchmarkNotifyPacket(b *testing.B) {
	notify := &pb.OffsetNotify{Key: []byte("key"), EffectData: bytes.Repeat([]byte("x"), 4096)}

	b.ReportAllocs()

	for b.Loop() {
		if _, err := MarshalNotifyPacket(42, notify); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNotifyNACKPacket(b *testing.B) {
	nacks := make([]*pb.NackNotify, 16)
	for i := range nacks {
		nacks[i] = &pb.NackNotify{Key: bytes.Repeat([]byte("k"), 128), NotSubscribed: true}
	}

	b.ReportAllocs()

	for b.Loop() {
		MarshalNotifyNACKPacket(1, 42, nacks, 0)
	}
}

func BenchmarkDiffTopology(b *testing.B) {
	old := &ClusterConfig{NodeID: 1}
	updated := &ClusterConfig{NodeID: 1}

	for id := NodeId(1); id <= 64; id++ {
		old.Nodes = append(old.Nodes, NodeConfig{ID: id, Region: "local"})
		updated.Nodes = append(updated.Nodes, NodeConfig{ID: id, Region: "remote"})
	}

	b.ReportAllocs()

	for b.Loop() {
		DiffTopology(old, updated)
	}
}

func BenchmarkReplicationReachability(b *testing.B) {
	health := NewPeerHealthTable(ClockConfig{})

	r := NewReplicator(1, "local", health, nil, nil, time.Second)
	for id := NodeId(2); id < 66; id++ {
		r.AddPeer(id, "local")
		peer := health.GetOrCreate(id)
		peer.alive.Store(true)
		peer.symmetric.Store(true)
	}

	b.ReportAllocs()

	for b.Loop() {
		if !r.AllRegionPeersReachable() {
			b.Fatal("peer unexpectedly unreachable")
		}
	}
}

func BenchmarkPubSubTargets(b *testing.B) {
	r := NewPubSubRouter(1, nil, nil)
	for id := uint64(2); id < 66; id++ {
		r.HandleEphemeralSubscribe(id, routingKeyFor("events", false), false)
		r.HandleEphemeralSubscribe(id, routingKeyFor("ev*", true), false)
	}

	b.ReportAllocs()

	for b.Loop() {
		if got := len(r.targetsFor("events")); got != 64 {
			b.Fatalf("duplicate or missing targets: %d", got)
		}
	}
}

func BenchmarkHeartbeatFanout(b *testing.B) {
	hm := NewHeartbeatManager(1, NewPeerHealthTable(ClockConfig{}), ClockConfig{})
	for id := NodeId(2); id < 66; id++ {
		hm.AddPeer(id, "local")
	}

	hm.sendFunc = func(NodeId, []byte) error { return nil }

	b.ReportAllocs()

	for b.Loop() {
		hm.sendTick()
	}
}
