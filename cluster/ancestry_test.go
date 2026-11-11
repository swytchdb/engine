package cluster

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
)

type countedAncestryReader struct {
	*EngineLogReader

	calls atomic.Int32
}

func (r *countedAncestryReader) ReadAncestry(key string, root effects.Tip, send func(*pb.AncestryResponse) error) error {
	r.calls.Add(1)

	return r.EngineLogReader.ReadAncestry(key, root, send)
}

func ancestryTestEngine(t *testing.T) *effects.Engine {
	t.Helper()

	e := effects.NewEngine(effects.EngineConfig{NodeID: 42})

	t.Cleanup(func() { _ = e.Close() })

	return e
}

func ancestryTestEffect(offset uint64) *pb.Effect {
	eff := &pb.Effect{
		Key: []byte("k"), NodeId: 42,
		Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
	}
	if offset > 1 {
		eff.Deps = []*pb.EffectRef{{NodeId: 42, Offset: offset - 1}}
	}

	return eff
}

func TestAncestryTransferHolderFallbackStreamingAndAbsence(t *testing.T) {
	empty, holder := ancestryTestEngine(t), ancestryTestEngine(t)

	for i := uint64(1); i <= 130; i++ {
		eff := ancestryTestEffect(i)
		if i == 1 {
			eff.Deps = []*pb.EffectRef{{NodeId: 42, Offset: 999}}
		}

		if i == 100 {
			eff.Kind = &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{State: &pb.ReducedEffect{}}}
		}

		if err := holder.EffectCache().Put(effects.Tip{42, i}, eff); err != nil {
			t.Fatal(err)
		}
	}

	readers := []*countedAncestryReader{
		{EngineLogReader: NewEngineLogReader(empty.EffectCache())},
		{EngineLogReader: NewEngineLogReader(empty.EffectCache())},
		{EngineLogReader: NewEngineLogReader(holder.EffectCache())},
		{EngineLogReader: NewEngineLogReader(holder.EffectCache())},
	}
	pms := startNNodeCluster(t, []EffectHandler{&recordingHandler{}, &recordingHandler{}, &recordingHandler{}, &recordingHandler{}},
		[]LogReader{readers[0], readers[1], readers[2], readers[3]})
	local := map[effects.Tip]bool{}

	var boundaries []effects.Tip

	have := func(tip effects.Tip) bool { return local[tip] }
	receive := func(tip effects.Tip, data []byte, isRoot bool) error {
		if len(data) == 0 {
			boundaries = append(boundaries, tip)

			return nil
		}

		local[tip] = true

		return nil
	}
	// First holder misses; the next serves multiple generations, crossing a
	// snapshot. The unrelated fourth node must never be queried.
	for _, root := range []uint64{130} {
		if err := pms[0].FetchAncestry("k", effects.Tip{42, root}, []pb.NodeID{1, 2}, have, receive); err != nil {
			t.Fatal(err)
		}
	}

	if len(local) != 130 || readers[1].calls.Load() != 1 || readers[2].calls.Load() != 1 || readers[3].calls.Load() != 0 {
		t.Fatalf("effects=%d calls=%d/%d/%d", len(local), readers[1].calls.Load(), readers[2].calls.Load(), readers[3].calls.Load())
	}

	if len(boundaries) != 1 || boundaries[0] != (effects.Tip{42, 999}) {
		t.Fatalf("boundaries = %v", boundaries)
	}
	// A resident vertex does not stop the stream: its dependencies may still
	// be missing locally. All 130 records must arrive in one request.
	clear(local)
	local[effects.Tip{42, 125}] = true

	if err := pms[0].FetchAncestry("k", effects.Tip{42, 130}, []pb.NodeID{2}, have, receive); err != nil {
		t.Fatal(err)
	}

	if len(local) != 130 {
		t.Fatalf("transfer stopped at local join: %d effects", len(local))
	}

	missing := effects.Tip{42, 999}
	if err := pms[0].FetchAncestry("k", missing, []pb.NodeID{1, 2}, have, receive); !errors.Is(err, effects.ErrEffectNotFound) {
		t.Fatal(err)
	}
	// A missing response cannot hide an unavailable holder, including when
	// the CDN also responds with a missing-blob error.
	pms[0].SetCDNFetcher(&recordingCDN{err: effects.ErrCDNBlobMissing})

	err := pms[0].FetchAncestry("k", missing, []pb.NodeID{99, 1}, have, receive)
	if err == nil || errors.Is(err, effects.ErrEffectNotFound) || errors.Is(err, effects.ErrCDNBlobMissing) {
		t.Fatalf("unavailable holder masked: %v", err)
	}
}

func TestAncestryResidentReaderScopesBindAndReportsBoundary(t *testing.T) {
	e := ancestryTestEngine(t)
	root := effects.Tip{42, 3}

	bind := &pb.Effect{Key: []byte("k"), NodeId: 42, Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
		Keys: []*pb.TransactionalBindEffect_KeyBind{
			{Key: []byte("k"), NewTip: &pb.EffectRef{NodeId: 42, Offset: 2}},
			{Key: []byte("other"), NewTip: &pb.EffectRef{NodeId: 42, Offset: 50}},
		},
	}}}
	if err := e.EffectCache().Put(root, bind); err != nil {
		t.Fatal(err)
	}

	var records []*pb.AncestryResponse

	err := NewEngineLogReader(e.EffectCache()).ReadAncestry("k", root, func(r *pb.AncestryResponse) error {
		records = append(records, r)

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(records) != 2 || records[1].GetRef().GetOffset() != 2 || len(records[1].GetEffectData()) != 0 {
		t.Fatalf("unexpected bind ancestry: %v", records)
	}
}

func TestAncestryStreamCancellationAndTruncation(t *testing.T) {
	tls := generateTestTLSConfig(t)
	tls.NextProtos = []string{"swytch-cluster"}

	listener, err := quic.ListenAddr("127.0.0.1:0", tls, clusterQUICConfig())
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = listener.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	client, err := quic.DialAddr(ctx, listener.Addr().String(), tls, clusterQUICConfig())
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = client.CloseWithError(0, "done") })

	server, err := listener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = server.CloseWithError(0, "done") })

	for _, mode := range []string{"cancel", "truncate", "complete", "resident-root"} {
		requestCtx, cancelRequest := context.WithCancel(ctx)

		var boundaries, roots atomic.Int32

		done := make(chan error, 1)
		go func() {
			done <- ancestryOverConn(requestCtx, client, "k", effects.Tip{42, 1}, func(effects.Tip) bool { return mode == "resident-root" }, func(_ effects.Tip, data []byte, isRoot bool) error {
				if isRoot {
					roots.Add(1)
				} else if len(data) == 0 {
					boundaries.Add(1)
				}

				return nil
			})
		}()

		stream, err := server.AcceptStream(ctx)
		if err != nil {
			t.Fatal(err)
		}

		var prefix [1]byte
		if _, err := io.ReadFull(stream, prefix[:]); err != nil {
			t.Fatal(err)
		}

		if err := readMessage(stream, &pb.AncestryRequest{}); err != nil {
			t.Fatal(err)
		}

		if mode == "cancel" {
			cancelRequest()
		} else {
			for _, resp := range []*pb.AncestryResponse{
				{Ref: &pb.EffectRef{NodeId: 42, Offset: 1}, EffectData: []byte{1}},
				{Ref: &pb.EffectRef{NodeId: 42, Offset: 2}, EffectData: []byte{1}, IsRoot: true},
				{Ref: &pb.EffectRef{NodeId: 42, Offset: 3}},
			} {
				if err := writeMessage(stream, resp); err != nil {
					t.Fatal(err)
				}
			}

			if mode == "complete" || mode == "resident-root" {
				if err := writeMessage(stream, &pb.AncestryResponse{Done: true}); err != nil {
					t.Fatal(err)
				}
			}
			// EOF without an explicit terminal record is not definitive absence.
			if err := stream.Close(); err != nil {
				t.Fatal(err)
			}
		}

		select {
		case err := <-done:
			if mode == "complete" || mode == "resident-root" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || errors.Is(err, effects.ErrEffectNotFound) {
				t.Fatalf("incomplete stream classified as absence: %v", err)
			}
		case <-ctx.Done():
			t.Fatal("stream did not cancel")
		}

		if mode == "complete" || mode == "resident-root" {
			if boundaries.Load() != 1 || roots.Load() != 1 {
				t.Fatalf("boundaries=%d roots=%d", boundaries.Load(), roots.Load())
			}
		} else if boundaries.Load() != 0 {
			t.Fatal("incomplete stream published missing boundaries")
		}

		cancelRequest()
		stream.CancelRead(0)
		stream.CancelWrite(0)
	}
}
