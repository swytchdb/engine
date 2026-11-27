package effects

import (
	"errors"
	"fmt"
	"io"
	"slices"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
	"google.golang.org/protobuf/proto"
)

type batchBroadcaster struct {
	mockBroadcaster

	version    string
	bind       *pb.OffsetNotify
	records    []*pb.OffsetNotify
	batchCount int
}

func (b *batchBroadcaster) MinimumVersion() string { return b.version }
func (b *batchBroadcaster) ReplicateTransaction(bind *pb.OffsetNotify, _ []byte, records []*pb.OffsetNotify, _ pb.NodeID) ([]*pb.NackNotify, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.bind, b.records = bind, slices.Clone(records)
	b.batchCount++

	return nil, nil
}

func captureTransactionBatch(t *testing.T, version string, count int) (*Engine, *batchBroadcaster) {
	t.Helper()

	b := &batchBroadcaster{version: version, peerIDs: []pb.NodeID{77}}
	e := newTestEngine(b)

	t.Cleanup(func() { _ = e.Close() })

	c := e.NewContext()
	c.BeginTx()

	for i := range count {
		key := fmt.Sprintf("batch:%d", i)
		preSubscribe(e, key)
		e.index.Insert(key, nil, keytrie.NewTipSet())

		for _, value := range []string{"first", "second"} {
			if err := c.Emit(scalarSet(key, value)); err != nil {
				t.Fatal(err)
			}
		}

		e.addPeerSubscriber(logicalAddress(key), 77)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	return e, b
}

func TestTransactionBatchVersionAndPublication(t *testing.T) {
	for _, version := range []string{"", "1.7.9", "1.8.0", "dev"} {
		t.Run(version, func(t *testing.T) {
			_, b := captureTransactionBatch(t, version, 8)
			if version == "" || version == "1.7.9" {
				if b.batchCount != 0 || len(b.broadcasts) < 16 {
					t.Fatal("legacy cluster lost individual delivery")
				}

				return
			}

			if b.batchCount != 1 || len(b.records) != 16 {
				t.Fatalf("batches=%d records=%d", b.batchCount, len(b.records))
			}

			for _, notify := range b.broadcasts {
				eff, err := parseWireEffect(notify.GetEffectData())
				if err != nil {
					t.Fatal(err)
				}

				if eff.GetData() != nil {
					t.Fatal("batch also broadcast an individual mutation")
				}
			}
		})
	}
}

func batchReceiver(records []*pb.OffsetNotify, end error) TransactionReceiver {
	return func(accept func(*pb.FetchResponse) error) error {
		for _, record := range records {
			if err := accept(&pb.FetchResponse{Ref: record.GetOrigin(), EffectData: record.GetEffectData()}); err != nil {
				return err
			}
		}

		return end
	}
}

func TestTransactionBatchSurvivesOriginLoss(t *testing.T) {
	source, b := captureTransactionBatch(t, "dev", 8)
	receiver := newTestEngine(nil)

	t.Cleanup(func() { _ = receiver.Close() })

	for i := range 8 {
		preSubscribe(receiver, fmt.Sprintf("batch:%d", i))
	}

	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	if nacks, err := receiver.HandleTransaction(b.bind, batchReceiver(b.records, nil)); err != nil || len(nacks) != 0 {
		t.Fatalf("batch response: %v %v", nacks, err)
	}

	receiver.reclaimUnreferenced()

	for i := range 8 {
		key := fmt.Sprintf("batch:%d", i)

		value, _, _, err := receiver.GetSnapshot(key)
		if err != nil || string(value.GetScalar().GetRaw()) != "second" {
			t.Fatalf("%s: value=%v err=%v", key, value, err)
		}
	}
}

func TestTransactionBatchIncompleteDoesNotPublish(t *testing.T) {
	_, b := captureTransactionBatch(t, "dev", 2)
	for _, tc := range []struct {
		name    string
		records []*pb.OffsetNotify
		end     error
	}{
		{"truncated", b.records[:1], io.ErrUnexpectedEOF},
		{"reset after last record", b.records, io.ErrUnexpectedEOF},
		{"omitted bound tip", b.records[:1], nil},
		{"duplicate", append(slices.Clone(b.records), b.records[0]), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newTestEngine(nil)

			t.Cleanup(func() { _ = e.Close() })
			preSubscribe(e, "batch:0", "batch:1")

			_, err := e.HandleTransaction(b.bind, batchReceiver(tc.records, tc.end))
			if err == nil {
				t.Fatal("incomplete transaction accepted")
			}

			if tc.end != nil && !errors.Is(err, tc.end) {
				t.Fatal(err)
			}

			if e.index.Contains("batch:0") != nil || e.index.Contains("batch:1") != nil {
				t.Fatal("partial transfer published a frontier")
			}

			if _, ok := e.effectCache.Get(r(b.bind.GetOrigin())); ok {
				t.Fatal("partial transfer installed bind")
			}

			e.reclaimUnreferenced()

			for _, record := range tc.records {
				if _, ok := e.effectCache.Get(r(record.GetOrigin())); ok {
					t.Fatal("failed transfer retained an effect pin")
				}
			}
		})
	}
}

func TestBindNACKDoesNotEchoIncomingBind(t *testing.T) {
	previous := 0

	for _, count := range []int{16, 64} {
		_, batch := captureTransactionBatch(t, "dev", count)
		e := newTestEngine(nil)

		t.Cleanup(func() { _ = e.Close() })

		for i := range count {
			key := fmt.Sprintf("batch:%d", i)
			preSubscribe(e, key)

			tip := Tip{88, uint64(i + 1)}

			eff := &pb.Effect{Key: []byte(key), NodeId: 88, Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{SubscriberNodeId: 88}}}
			if err := e.effectCache.Put(tip, eff); err != nil {
				t.Fatal(err)
			}

			e.index.Insert(key, nil, keytrie.NewTipSet(tip))
		}

		nacks, err := e.HandleTransaction(batch.bind, batchReceiver(batch.records, nil))
		if err != nil || len(nacks) != count {
			t.Fatalf("nacks=%d err=%v", len(nacks), err)
		}

		size := 0

		for _, nack := range nacks {
			if len(nack.GetTips()) != 1 || nack.GetTips()[0].GetNodeId() != 88 {
				t.Fatal("NACK lost peer evidence or echoed the incoming bind")
			}

			for _, detail := range nack.GetTipDetails() {
				if r(detail.GetRef()) == r(batch.bind.GetOrigin()) {
					t.Fatal("NACK echoed bind metadata")
				}
			}

			size += proto.Size(nack)
		}

		if previous != 0 && size > 5*previous {
			t.Fatalf("4x keys grew NACK size from %d to %d", previous, size)
		}

		previous = size
	}
}

func TestBindClosureFailureIsNotAcceptance(t *testing.T) {
	_, b := captureTransactionBatch(t, "dev", 1)
	e := newTestEngine(&mockBroadcaster{}) // empty fetch payload is an error

	t.Cleanup(func() { _ = e.Close() })
	preSubscribe(e, "batch:0")

	if _, err := e.HandleRemote(b.bind); err == nil {
		t.Fatal("bind with unavailable data was accepted")
	}
}

func TestTransactionBatchRetainsReadObservations(t *testing.T) {
	b := &batchBroadcaster{version: "dev", peerIDs: []pb.NodeID{77}}
	e := newTestEngine(b)

	t.Cleanup(func() { _ = e.Close() })
	preSubscribe(e, "observed", "written")
	e.index.Insert("observed", nil, keytrie.NewTipSet())
	e.index.Insert("written", nil, keytrie.NewTipSet())
	c := e.NewContext()
	c.BeginTx()

	if _, _, err := c.GetSnapshot("observed"); err != nil {
		t.Fatal(err)
	}

	if err := c.Emit(scalarSet("written", "value")); err != nil {
		t.Fatal(err)
	}

	e.addPeerSubscriber(logicalAddress("observed"), 77)

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	if b.bind == nil {
		t.Fatal("missing batch")
	}

	observed := false

	for _, record := range b.records {
		eff, err := parseWireEffect(record.GetEffectData())
		if err != nil {
			t.Fatal(err)
		}

		if string(eff.GetKey()) == "observed" && eff.GetNoop() != nil {
			observed = true
		}
	}

	if !observed {
		t.Fatal("batch omitted read-only observation")
	}

	receiver := newTestEngine(nil)

	t.Cleanup(func() { _ = receiver.Close() })
	preSubscribe(receiver, "observed", "written")

	if _, err := receiver.HandleTransaction(b.bind, batchReceiver(b.records, nil)); err != nil {
		t.Fatal(err)
	}

	if receiver.index.Contains("observed") == nil {
		t.Fatal("read-only bind projection was not installed")
	}
}

func TestTransactionBatchDoesNotOwnUnsubscribedKeys(t *testing.T) {
	_, batch := captureTransactionBatch(t, "dev", 2)
	e := newTestEngine(nil)

	t.Cleanup(func() { _ = e.Close() })
	preSubscribe(e, "batch:1")

	if _, err := e.HandleTransaction(batch.bind, batchReceiver(batch.records, nil)); err != nil {
		t.Fatal(err)
	}

	if e.index.Contains("batch:0") != nil || e.index.Contains("batch:1") == nil {
		t.Fatal("batch changed per-key ownership")
	}

	e.reclaimUnreferenced()

	for _, record := range batch.records {
		if string(record.GetKey()) == "batch:0" {
			if _, ok := e.effectCache.Get(r(record.GetOrigin())); ok {
				t.Fatal("unsubscribed batch data retained creation ownership")
			}
		}
	}
}
