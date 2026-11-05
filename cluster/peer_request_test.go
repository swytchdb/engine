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
	"context"
	"io"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	pb "github.com/swytchdb/engine/cluster/proto"
	"google.golang.org/protobuf/proto"
)

func TestPeerRequests(t *testing.T) {
	for _, tc := range []struct {
		name       string
		streamType byte
		request    proto.Message
		response   proto.Message
		call       func(context.Context, *PeerConn) (proto.Message, error)
	}{
		{
			name: "fetch", streamType: streamTypeFetch,
			request:  &pb.FetchRequest{Ref: &pb.EffectRef{NodeId: 2, Offset: 42}},
			response: &pb.FetchResponse{EffectData: []byte("effect")},
			call: func(ctx context.Context, pc *PeerConn) (proto.Message, error) {
				data, err := pc.Fetch(ctx, &pb.EffectRef{NodeId: 2, Offset: 42})

				return &pb.FetchResponse{EffectData: data}, err
			},
		},
		{
			name: "forward", streamType: streamTypeForward,
			request:  &pb.ForwardedTransaction{OriginNode: 1, AuthorizedUser: "alice"},
			response: &pb.ForwardedResponse{RespData: []byte("+OK\r\n")},
			call: func(ctx context.Context, pc *PeerConn) (proto.Message, error) {
				return pc.ForwardTransaction(ctx, &pb.ForwardedTransaction{OriginNode: 1, AuthorizedUser: "alice"})
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tlsConfig := generateTestTLSConfig(t)
			tlsConfig.NextProtos = []string{"swytch-cluster"}

			listener, err := quic.ListenAddr("127.0.0.1:0", tlsConfig, clusterQUICConfig())
			if err != nil {
				t.Fatal(err)
			}
			// Best-effort teardown after assertions; connections are test-local.
			t.Cleanup(func() { _ = listener.Close() })

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			conn, err := quic.DialAddr(ctx, listener.Addr().String(), tlsConfig, clusterQUICConfig())
			if err != nil {
				t.Fatal(err)
			}

			pc := &PeerConn{}
			pc.quicConn.Store(conn)
			t.Cleanup(pc.Stop)

			server, err := listener.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}

			t.Cleanup(func() { _ = server.CloseWithError(0, "test complete") })

			// Cancel a request after the server has read it, then successfully
			// use the same connection. Cancellation must affect only its stream.
			for _, canceled := range []bool{true, false} {
				requestCtx, cancelRequest := context.WithCancel(ctx)
				t.Cleanup(cancelRequest)

				done := make(chan error, 1)

				go func() {
					response, callErr := tc.call(requestCtx, pc)
					if callErr == nil && !proto.Equal(response, tc.response) {
						t.Errorf("response = %v, want %v", response, tc.response)
					}

					done <- callErr
				}()

				stream, err := server.AcceptStream(ctx)
				if err != nil {
					t.Fatal(err)
				}

				if err := stream.SetDeadline(time.Now().Add(time.Second)); err != nil {
					t.Fatal(err)
				}

				var prefix [1]byte
				if _, err := io.ReadFull(stream, prefix[:]); err != nil {
					t.Fatal(err)
				}

				request := tc.request.ProtoReflect().New().Interface()
				if err := readMessage(stream, request); err != nil {
					t.Fatal(err)
				}

				if prefix[0] != tc.streamType || !proto.Equal(request, tc.request) {
					t.Fatalf("unexpected request type=%d message=%v", prefix[0], request)
				}

				if canceled {
					cancelRequest()
				} else {
					if err := writeMessage(stream, tc.response); err != nil {
						t.Fatal(err)
					}

					if err := stream.Close(); err != nil {
						t.Fatal(err)
					}
				}

				select {
				case err := <-done:
					if (err != nil) != canceled {
						t.Fatalf("canceled=%v: request error=%v", canceled, err)
					}
				case <-time.After(time.Second):
					t.Fatal("request did not finish")
				}

				stream.CancelRead(0)
				stream.CancelWrite(0)
			}
		})
	}
}
