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
	"encoding/binary"
	"fmt"
	"log/slog"

	pb "github.com/swytchdb/engine/cluster/proto"
	"google.golang.org/protobuf/proto"
)

// PacketHandler dispatches parsed inbound packets to the appropriate handler.
type PacketHandler interface {
	HandleClockTick(peerID NodeId, tick *ClockTick)
	HandleNotify(peerID NodeId, requestID uint64, notify *pb.OffsetNotify)
	HandleNotifyACK(peerID NodeId, requestID uint64, status byte, credits uint32)
	HandleNotifyACKWithData(peerID NodeId, requestID uint64, status byte, credits uint32, fullPacket []byte)
	HandleNackNotify(peerID NodeId, nack *pb.NackNotify)
	HandleKeyFilter(peerID NodeId, version uint64, filter []byte)
}

// Notify plaintext layout:
// [1:type][8:requestID LE][proto-marshaled OffsetNotify]
const (
	notifyHeaderSize    = 1 + 8 // 9 bytes before protobuf payload
	notifyACKHeaderSize = 1 + 8 + 8 + 1 + 4
)

// --- Packet marshaling/parsing helpers ---

// MarshalNotifyPacket builds a notify plaintext body:
// [1:type][8:requestID LE][proto-marshaled OffsetNotify]
func MarshalNotifyPacket(requestID uint64, notify *pb.OffsetNotify) ([]byte, error) {
	buf := make([]byte, notifyHeaderSize, notifyHeaderSize+proto.Size(notify))
	buf[0] = PacketTypeNotify
	binary.LittleEndian.PutUint64(buf[1:9], requestID)

	buf, err := proto.MarshalOptions{}.MarshalAppend(buf, notify)
	if err != nil {
		return nil, fmt.Errorf("marshal OffsetNotify: %w", err)
	}

	return buf, nil
}

// AssembleNotifyPacket frames a pre-marshalled OffsetNotify body into a notify
// packet with the per-request header. The proto body is identical for every
// recipient of the same notify, so a fan-out (e.g. a bind to all subscribers)
// marshals it once and assembles a packet per peer here — only the requestID
// in the header varies.
func AssembleNotifyPacket(requestID uint64, body []byte) []byte {
	buf := make([]byte, notifyHeaderSize+len(body))
	buf[0] = PacketTypeNotify
	binary.LittleEndian.PutUint64(buf[1:9], requestID)
	copy(buf[9:], body)

	return buf
}

// parseNotifyPacket parses the plaintext body of a Notify packet.
func parseNotifyPacket(data []byte) (requestID uint64, notify *pb.OffsetNotify, err error) {
	if len(data) < notifyHeaderSize {
		return 0, nil, fmt.Errorf("notify packet too short: %d", len(data))
	}

	requestID = binary.LittleEndian.Uint64(data[1:9])

	notify = &pb.OffsetNotify{}
	if err := proto.Unmarshal(data[9:], notify); err != nil {
		return 0, nil, fmt.Errorf("unmarshal OffsetNotify: %w", err)
	}

	return requestID, notify, nil
}

// MarshalNotifyACKPacket builds a notify ACK plaintext body:
// [1:type][8:nodeID LE][8:requestID LE][1:status][4:credits LE]
func MarshalNotifyACKPacket(nodeID NodeId, requestID uint64, status byte, credits uint32) []byte {
	buf := make([]byte, notifyACKHeaderSize)
	writeNotifyACKHeader(buf, nodeID, requestID, status, credits)

	return buf
}

func writeNotifyACKHeader(buf []byte, nodeID NodeId, requestID uint64, status byte, credits uint32) {
	buf[0] = PacketTypeNotifyACK
	binary.LittleEndian.PutUint64(buf[1:9], uint64(nodeID))
	binary.LittleEndian.PutUint64(buf[9:17], requestID)
	buf[17] = status
	binary.LittleEndian.PutUint32(buf[18:22], credits)
}

// parseNotifyACKPacket parses the plaintext body of a NotifyACK packet.
func parseNotifyACKPacket(data []byte) (peerID NodeId, requestID uint64, status byte, credits uint32, err error) {
	if len(data) < notifyACKHeaderSize {
		return 0, 0, 0, 0, fmt.Errorf("notify ACK packet too short: %d", len(data))
	}

	peerID = NodeId(binary.LittleEndian.Uint64(data[1:9]))
	requestID = binary.LittleEndian.Uint64(data[9:17])
	status = data[17]
	credits = binary.LittleEndian.Uint32(data[18:22])

	return peerID, requestID, status, credits, nil
}

// MarshalNotifyNACKPacket builds a NotifyACK with status=0x01 and embedded NACKs:
// [1:0x04][8:nodeID LE][8:requestID LE][1:0x01][4:credits LE][4:nack_count LE][for each: [4:len LE][proto NackNotify]]
func MarshalNotifyNACKPacket(nodeID NodeId, requestID uint64, nacks []*pb.NackNotify, credits uint32) []byte {
	size := notifyACKHeaderSize + 4
	for _, nack := range nacks {
		size += 4 + proto.Size(nack)
	}

	buf := make([]byte, notifyACKHeaderSize+4, size)
	writeNotifyACKHeader(buf, nodeID, requestID, 0x01, credits)

	var count uint32

	for _, nack := range nacks {
		start := len(buf)
		buf = binary.LittleEndian.AppendUint32(buf, 0)

		encoded, err := proto.MarshalOptions{}.MarshalAppend(buf, nack)
		if err != nil {
			slog.Error("failed to marshal NackNotify in NACK response", "error", err)

			buf = buf[:start]

			continue
		}

		buf = encoded
		binary.LittleEndian.PutUint32(buf[start:start+4], uint32(len(buf)-start-4))

		count++
	}

	binary.LittleEndian.PutUint32(buf[notifyACKHeaderSize:], count)

	return buf
}

// parseNotifyNACKPayload parses the NACK payload after the 22-byte ACK header.
// Input is the bytes AFTER the header: [4:nack_count LE][for each: [4:len LE][proto]]
func parseNotifyNACKPayload(data []byte) ([]*pb.NackNotify, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("NACK payload too short: %d", len(data))
	}

	count := binary.LittleEndian.Uint32(data[:4])
	offset := 4

	nacks := make([]*pb.NackNotify, 0, count)
	for i := range count {
		if offset+4 > len(data) {
			return nacks, fmt.Errorf("NACK payload truncated at entry %d", i)
		}

		nLen := binary.LittleEndian.Uint32(data[offset : offset+4])

		offset += 4
		if offset+int(nLen) > len(data) {
			return nacks, fmt.Errorf("NACK entry %d truncated", i)
		}

		nack := &pb.NackNotify{}
		if err := proto.Unmarshal(data[offset:offset+int(nLen)], nack); err != nil {
			return nacks, fmt.Errorf("unmarshal NACK entry %d: %w", i, err)
		}

		nacks = append(nacks, nack)
		offset += int(nLen)
	}

	return nacks, nil
}

// MarshalKeyFilterPacket builds a key-filter plaintext body:
// [1:type][8:version LE][marshaled CuckooChain]
// Sent to a peer at connection establishment so it learns which keys we hold;
// until it arrives, the peer treats us as holding everything.
func MarshalKeyFilterPacket(version uint64, filter []byte) []byte {
	buf := make([]byte, 1+8+len(filter))
	buf[0] = PacketTypeKeyFilter
	binary.LittleEndian.PutUint64(buf[1:9], version)
	copy(buf[9:], filter)

	return buf
}

// parseKeyFilterPacket parses the plaintext body of a key-filter packet.
func parseKeyFilterPacket(data []byte) (version uint64, filter []byte, err error) {
	if len(data) < 9 {
		return 0, nil, fmt.Errorf("key filter packet too short: %d", len(data))
	}

	return binary.LittleEndian.Uint64(data[1:9]), data[9:], nil
}

// MarshalNackPacket builds a NACK plaintext body:
// [1:type][proto-marshaled NackNotify]
func MarshalNackPacket(nack *pb.NackNotify) ([]byte, error) {
	buf := make([]byte, 1, 1+proto.Size(nack))
	buf[0] = PacketTypeNack

	buf, err := proto.MarshalOptions{}.MarshalAppend(buf, nack)
	if err != nil {
		return nil, fmt.Errorf("marshal NackNotify: %w", err)
	}

	return buf, nil
}
