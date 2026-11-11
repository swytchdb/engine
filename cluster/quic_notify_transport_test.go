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
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/quic-go/quic-go"
	pb "github.com/swytchdb/engine/cluster/proto"
)

// TestDispatchPlaintextEmptyPayload guards the remote-input path: a zstd
// frame can decompress to zero bytes, which must not panic the dispatcher.
func TestDispatchPlaintextEmptyPayload(t *testing.T) {
	transport := NewQUICNotifyTransport(1, nil, func(NodeId) *quic.Conn { return nil }, nil)

	transport.dispatchPlaintext(2, nil)
	transport.dispatchPlaintext(2, []byte{})
}

type gatedNotifyHandler struct {
	recordingHandler

	gate  chan struct{}
	nacks []*pb.NackNotify
}

func (h *gatedNotifyHandler) HandleOffsetNotify(_ *pb.OffsetNotify) ([]*pb.NackNotify, error) {
	h.count.Add(1)
	<-h.gate

	return h.nacks, nil
}

func TestDispatchNotifyShedsActiveRetries(t *testing.T) {
	for _, nack := range []bool{false, true} {
		name := "ACK"
		if nack {
			name = "NACK"
		}

		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				handler := &gatedNotifyHandler{gate: make(chan struct{})}
				want := MarshalNotifyACKPacket(1, 42, 0, 0)

				if nack {
					handler.nacks = []*pb.NackNotify{{Key: []byte("key"), NotSubscribed: true}}
					want = MarshalNotifyNACKPacket(1, 42, handler.nacks, 0)
				}

				responseGate := make(chan struct{})

				var responses atomic.Int32

				r := NewReplicator(1, "local", NewPeerHealthTable(ClockConfig{}), nil, handler, time.Second)
				r.transport = notificationTransportFunc(func(peer NodeId, packet []byte) (int, error) {
					responses.Add(1)

					if peer != 2 || !bytes.Equal(packet, want) {
						t.Error("original ACK/NACK response changed")
					}

					<-responseGate

					return 0, ErrPeerUnavailable // A later retry must be admitted after a lost response.
				})
				transport := NewQUICNotifyTransport(1, &PeerManager{replicator: r}, nil, nil)

				packet, err := MarshalNotifyPacket(42, &pb.OffsetNotify{Key: []byte("key")})
				if err != nil {
					t.Fatal(err)
				}

				dispatchRetries := func() {
					var wg sync.WaitGroup
					for range 32 {
						wg.Go(func() { transport.dispatchPlaintext(2, packet) })
					}

					wg.Wait()
					synctest.Wait()
				}
				dispatchRetries()

				if got := handler.count.Load(); got != 1 {
					t.Errorf("active handler invocations = %d, want 1", got)
				}

				if got := responses.Load(); got != 0 {
					t.Errorf("premature responses = %d, want 0", got)
				}

				close(handler.gate)
				synctest.Wait()
				dispatchRetries()

				if handler.count.Load() != 1 || responses.Load() != 1 {
					t.Errorf("response in flight: handlers=%d responses=%d, want 1 each", handler.count.Load(), responses.Load())
				}

				close(responseGate)
				synctest.Wait()
				transport.dispatchPlaintext(2, packet)
				synctest.Wait()

				if handler.count.Load() != 2 || responses.Load() != 2 {
					t.Errorf("retry after completion: handlers=%d responses=%d, want 2 each", handler.count.Load(), responses.Load())
				}
			})
		})
	}
}

func TestDispatchNotifyIndependentRequests(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		handler := &gatedNotifyHandler{gate: make(chan struct{})}
		r := NewReplicator(1, "local", NewPeerHealthTable(ClockConfig{}), nil, handler, time.Second)
		r.transport = notificationTransportFunc(func(_ NodeId, packet []byte) (int, error) { return len(packet), nil })
		transport := NewQUICNotifyTransport(1, &PeerManager{replicator: r}, nil, nil)
		// Requests are scoped to peers; zero identifies distinct one-way notifications.
		requests := [][2]uint64{{2, 42}, {3, 42}, {2, 43}, {2, 0}, {2, 0}}
		for _, request := range requests {
			packet, err := MarshalNotifyPacket(request[1], &pb.OffsetNotify{Key: []byte("key")})
			if err != nil {
				t.Fatal(err)
			}

			transport.dispatchPlaintext(NodeId(request[0]), packet)
		}

		synctest.Wait()

		if got := handler.count.Load(); got != int32(len(requests)) {
			t.Errorf("independent handlers = %d, want %d", got, len(requests))
		}

		close(handler.gate)
	})
}
