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
	"encoding/binary"
	"iter"

	pb "github.com/swytchdb/engine/cluster/proto"
)

// MembershipKey is the reserved effects key for cluster membership.
const MembershipKey = "__swytch:members"

// Member represents a single cluster node.
type Member struct {
	NodeID uint64
	Addr   string // host:port
}

// nodeIDBytes encodes a NodeID as 8-byte little-endian for use as a KEYED element ID.
func nodeIDBytes(nodeID uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, nodeID)

	return b
}

// nodeIDFromBytes decodes a NodeID from 8-byte little-endian element ID.
func nodeIDFromBytes(b []byte) uint64 {
	if len(b) != 8 {
		return 0
	}

	return binary.LittleEndian.Uint64(b)
}

// buildMemberInsert creates an INSERT_OP DataEffect for initial membership registration.
// This is the equivalent of HSET __swytch:members <nodeID> <addr>.
func buildMemberInsert(nodeID uint64, addr string) *pb.Effect {
	return &pb.Effect{
		Key: []byte(MembershipKey),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_KEYED,
			Placement:  pb.Placement_PLACE_NONE,
			Id:         nodeIDBytes(nodeID),
			Value:      &pb.DataEffect_Raw{Raw: []byte(addr)},
		}},
	}
}

// buildMemberRemove creates a REMOVE_OP DataEffect for graceful departure.
func buildMemberRemove(nodeID uint64) *pb.Effect {
	return &pb.Effect{
		Key: []byte(MembershipKey),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_REMOVE_OP,
			Collection: pb.CollectionKind_KEYED,
			Id:         nodeIDBytes(nodeID),
		}},
	}
}

// membershipEntries decodes valid members without copying their addresses.
// Address bytes belong to the snapshot and must be treated as read-only.
func membershipEntries(snapshot *pb.ReducedEffect) iter.Seq2[uint64, []byte] {
	return func(yield func(uint64, []byte) bool) {
		for id, elem := range snapshot.GetNetAdds() {
			nodeID := nodeIDFromBytes([]byte(id))
			if nodeID == 0 {
				continue
			}

			addr := elem.GetData().Decompress()
			if len(addr) > 0 && !yield(nodeID, addr) {
				return
			}
		}
	}
}

// matchesMembership compares decoded entries without allocating a second map.
// Count only valid entries so malformed data cannot force repeated updates.
func matchesMembership(snapshot *pb.ReducedEffect, members map[uint64]string) bool {
	count := 0

	for id, addr := range membershipEntries(snapshot) {
		if members[id] != string(addr) {
			return false
		}

		count++
	}

	return count == len(members)
}

// parseMembership copies valid members into an owned map.
func parseMembership(snapshot *pb.ReducedEffect) map[uint64]string {
	if len(snapshot.GetNetAdds()) == 0 {
		return nil
	}

	members := make(map[uint64]string, len(snapshot.GetNetAdds()))
	for id, addr := range membershipEntries(snapshot) {
		members[id] = string(addr)
	}

	return members
}
