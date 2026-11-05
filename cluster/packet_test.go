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
	"encoding/binary"
	"fmt"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"google.golang.org/protobuf/proto"
)

func TestNotifyPacketWireCompatibility(t *testing.T) {
	for _, size := range []int{0, 64, 4096} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			notify := &pb.OffsetNotify{
				Key:        []byte("key"),
				Origin:     &pb.EffectRef{NodeId: 7, Offset: 19},
				EffectData: bytes.Repeat([]byte("x"), size),
			}
			// Preserve fields from newer senders when forwarding notifications.
			notify.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})

			body, err := proto.Marshal(notify)
			if err != nil {
				t.Fatal(err)
			}

			want := make([]byte, 9, 9+len(body))
			copy(want, []byte{PacketTypeNotify, 42, 0, 0, 0, 0, 0, 0, 0})
			want = append(want, body...)

			packet, err := MarshalNotifyPacket(42, notify)
			if err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(packet, want) || !bytes.Equal(AssembleNotifyPacket(42, body), want) {
				t.Fatal("notification wire format changed")
			}

			requestID, decoded, err := parseNotifyPacket(packet)
			if err != nil || requestID != 42 || !proto.Equal(decoded, notify) {
				t.Fatalf("round trip: request=%d, error=%v, notify=%v", requestID, err, decoded)
			}
		})
	}
}

func TestNACKPacketWireCompatibility(t *testing.T) {
	for _, count := range []int{0, 1, 16} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			nacks := make([]*pb.NackNotify, count)
			// Node 7, request 42, NACK status, 9 credits.
			want := []byte{PacketTypeNotifyACK, 7, 0, 0, 0, 0, 0, 0, 0, 42, 0, 0, 0, 0, 0, 0, 0, 1, 9, 0, 0, 0}
			want = binary.LittleEndian.AppendUint32(want, uint32(count))

			for i := range nacks {
				nacks[i] = &pb.NackNotify{
					Key:           bytes.Repeat([]byte("k"), i*128),
					NotSubscribed: i%2 == 0,
					Tips:          []*pb.EffectRef{{NodeId: 3, Offset: uint64(i)}},
				}

				body, err := proto.Marshal(nacks[i])
				if err != nil {
					t.Fatal(err)
				}

				want = binary.LittleEndian.AppendUint32(want, uint32(len(body)))
				want = append(want, body...)

				packet, err := MarshalNackPacket(nacks[i])
				if err != nil {
					t.Fatal(err)
				}

				if packet[0] != PacketTypeNack || !bytes.Equal(packet[1:], body) {
					t.Fatal("standalone NACK wire format changed")
				}
			}

			packet := MarshalNotifyNACKPacket(7, 42, nacks, 9)
			if !bytes.Equal(packet, want) {
				t.Fatal("NACK batch wire format changed")
			}

			peer, request, status, credits, err := parseNotifyACKPacket(packet)
			if err != nil || peer != 7 || request != 42 || status != 1 || credits != 9 {
				t.Fatalf("ACK header: peer=%d request=%d status=%d credits=%d error=%v", peer, request, status, credits, err)
			}

			decoded, err := parseNotifyNACKPayload(packet[notifyACKHeaderSize:])
			if err != nil || len(decoded) != len(nacks) {
				t.Fatalf("NACK payload: count=%d error=%v", len(decoded), err)
			}

			for i, nack := range decoded {
				if !proto.Equal(nack, nacks[i]) {
					t.Fatalf("NACK %d changed", i)
				}
			}
		})
	}
}
