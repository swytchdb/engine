package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net/http"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"google.golang.org/protobuf/proto"
)

// The proof's ordinary ancestry is deliberately unavailable. Neither peer
// streaming nor cloud transport may try to reconstruct that foreign key.
func TestSnapshotEvidencePeerAndCloudTransport(t *testing.T) {
	source := ancestryTestEngine(t)
	member, owner := effects.Tip{42, 1}, effects.Tip{42, 2}
	ref := func(t effects.Tip) *pb.EffectRef { return &pb.EffectRef{NodeId: t[0], Offset: t[1]} }

	proof := &pb.Effect{Key: []byte("foreign"), Deps: []*pb.EffectRef{{NodeId: 99, Offset: 99}}, Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}}}
	if err := source.EffectCache().Put(member, proof); err != nil {
		t.Fatal(err)
	}

	snapshot := &pb.Effect{Key: []byte("k"), Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{Evidence: &pb.BindEvidence{Deps: []*pb.EffectRef{ref(member)}}}}}
	if err := source.EffectCache().Put(owner, snapshot); err != nil {
		t.Fatal(err)
	}

	verify := func(t *testing.T, data []byte) {
		t.Helper()

		keyLen := int(binary.LittleEndian.Uint32(data))

		wire := &pb.Effect{}
		if err := effects.UnmarshalEffect(data[4+keyLen:], wire); err != nil {
			t.Fatal(err)
		}

		if len(wire.GetEvidencePackage()) != 1 {
			t.Fatal("proof omitted from wire package")
		}

		target := ancestryTestEngine(t)
		if err := target.EffectCache().Put(owner, wire); err != nil {
			t.Fatal(err)
		}

		stored, ok := target.EffectCache().Get(member)
		if !ok || !proto.Equal(stored, proof) {
			t.Fatal("proof payload changed in transit")
		}
	}
	reader := NewEngineLogReader(source.EffectCache())

	t.Run("fetch", func(t *testing.T) {
		data, err := reader.ReadEffect(ref(owner))
		if err != nil {
			t.Fatal(err)
		}

		verify(t, data)
	})
	t.Run("ancestry", func(t *testing.T) {
		count := 0

		err := reader.ReadAncestry("k", owner, func(record *pb.AncestryResponse) error {
			count++

			verify(t, record.GetEffectData())

			return nil
		})
		if err != nil || count != 1 {
			t.Fatalf("stream walked proof ancestry: count=%d error=%v", count, err)
		}
	})
	t.Run("cloud", func(t *testing.T) {
		enc, err := effects.NewEncryptorFromIKM([]byte("snapshot-proof-test"))
		if err != nil {
			t.Fatal(err)
		}

		uploader := &CloudSync{engine: source, enc: enc, keyNameKey: DeriveKeyNameKey("snapshot-proof-test")}

		env, err := uploader.buildEnvelope(owner, snapshot)
		if err != nil {
			t.Fatal(err)
		}

		data, err := proto.Marshal(env)
		if err != nil {
			t.Fatal(err)
		}

		downloader := &CloudSync{enc: enc, httpClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(data))}, nil
		})}}

		wire, err := downloader.FetchFromCDN(context.Background(), ref(owner))
		if err != nil {
			t.Fatal(err)
		}

		verify(t, wire)
	})
}
