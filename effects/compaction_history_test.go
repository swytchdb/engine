package effects

import (
	"context"
	"fmt"
	"slices"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
)

// Compaction must preserve the history selected by an existing transaction,
// including keys it has not accessed yet. Exercise the normal snapshot emission
// and reclamation paths so future history releases must preserve this contract.
func TestCompactionPreservesTransactionHistory(t *testing.T) {
	for _, readBefore := range []bool{false, true} {
		t.Run(fmt.Sprintf("read-before=%t", readBefore), func(t *testing.T) {
			e := newTestEngine(nil)

			t.Cleanup(func() {
				if err := e.Close(); err != nil {
					t.Error(err)
				}
			})
			preSubscribe(e, "k")
			historicalTestWrite(t, e, map[string]string{"k": "old"}, false)
			oldTip := e.index.Contains("k").Tips()[0]

			tx := e.NewContext()

			tx.BeginTx()
			defer tx.Abort()

			if readBefore {
				got, _, err := tx.GetSnapshot("k")
				if err != nil || string(got.GetScalar().GetRaw()) != "old" {
					t.Fatalf("initial transaction read: value=%v error=%v", got, err)
				}
			}

			// Exceed the standalone compaction threshold using public writes.
			for range 6 {
				historicalTestWrite(t, e, map[string]string{"k": "new"}, false)
			}

			compactor := e.NewContext()
			defer compactor.Abort()

			if _, _, err := compactor.GetSnapshot("k"); err != nil {
				t.Fatal(err)
			}

			ck := compactor.keys["k"]
			if ck == nil {
				t.Fatal("read did not emit a compaction snapshot")
			}

			snapshot, ok := e.effectCache.Get(ck.lastOffset)
			if !ok || snapshot.GetSnapshot().GetState() == nil {
				t.Fatal("compaction did not emit a state snapshot")
			}

			if err := compactor.Flush(); err != nil {
				t.Fatal(err)
			}

			// A second snapshot makes the first an eligible reclamation
			// boundary. Simulate the pacing interval without sleeping.
			leaf := e.index.LoadData("k")
			leaf.lastCompaction.Add(-int64(compactionInterval))

			for range 6 {
				historicalTestWrite(t, e, map[string]string{"k": "new"}, false)
			}

			if _, _, err := compactor.GetSnapshot("k"); err != nil {
				t.Fatal(err)
			}

			if compactor.keys["k"] == nil {
				t.Fatal("read did not emit the second compaction snapshot")
			}

			if err := compactor.Flush(); err != nil {
				t.Fatal(err)
			}

			// Adopt the compacted DAG, dropping the previous cached DAG's pins,
			// then drain any references compaction made reclaimable.
			got, _, _, err := e.GetSnapshot("k")
			if err != nil || string(got.GetScalar().GetRaw()) != "new" {
				t.Fatalf("live read after compaction: value=%v error=%v", got, err)
			}

			e.reclaimUnreferenced()

			got, _, err = tx.GetSnapshot("k")
			if err != nil || string(got.GetScalar().GetRaw()) != "old" {
				t.Fatalf("transaction history lost after compaction: value=%v error=%v", got, err)
			}

			tx.Abort()
			// Ending the transaction only removes its overlay. The following
			// write forces a new live DAG build rather than a reduced-cache hit.
			historicalTestWrite(t, e, map[string]string{"k": "later"}, false)

			if _, _, _, err := e.GetSnapshot("k"); err != nil {
				t.Fatal(err)
			}

			e.reclaimUnreferenced()

			if _, present := e.effectCache.Get(oldTip); present {
				t.Fatal("history remained pinned after the transaction ended and compaction ran")
			}
		})
	}
}

type historyBroadcaster struct {
	mockBroadcaster

	progress  []HistoryProgress
	processed []Tip
}

func (b *historyBroadcaster) HistoryProgress() []HistoryProgress {
	return slices.Clone(b.progress)
}
func (b *historyBroadcaster) RecordHistorySent(pb.NodeID, uint64) {}
func (b *historyBroadcaster) RecordHistoryProcessed(node pb.NodeID, count uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.processed = append(b.processed, Tip{uint64(node), count})
}

func TestCompactionRequiresSubscriberEvidence(t *testing.T) {
	for _, tc := range []struct {
		name       string
		members    []pb.NodeID
		progress   []HistoryProgress
		witness    Tip
		reclaimOld bool
	}{
		{name: "disconnected", members: []pb.NodeID{2}, witness: Tip{42, 9}},
		{name: "unrelated-higher-completion", members: []pb.NodeID{2}, progress: []HistoryProgress{{Node: 2, Processed: 10}}, witness: Tip{42, 9}},
		{name: "processed-witness", members: []pb.NodeID{2}, progress: []HistoryProgress{{Node: 2, Processed: 9}}, witness: Tip{42, 9}, reclaimOld: true},
		{name: "peer-authored-witness", members: []pb.NodeID{2}, progress: []HistoryProgress{{Node: 2, Minted: 3}}, witness: Tip{2, 3}, reclaimOld: true},
		{name: "unknown-other-subscriber", members: []pb.NodeID{2, 3}, progress: []HistoryProgress{{Node: 2, Minted: 3}}, witness: Tip{2, 3}},
		{name: "other-subscriber-removed", members: []pb.NodeID{2}, progress: []HistoryProgress{{Node: 2, Minted: 3}}, witness: Tip{2, 3}, reclaimOld: true},
		{name: "all-subscribers-removed", witness: Tip{2, 3}, reclaimOld: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := &historyBroadcaster{progress: tc.progress}
			b.peerIDs = tc.members

			e := newTestEngine(b)
			defer func() { _ = e.Close() }()

			old, cut, latest := Tip{42, 1}, Tip{42, 8}, Tip{42, 20}
			for tip, eff := range map[Tip]*pb.Effect{
				old:        {Key: []byte("k"), Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("old"))}},
				cut:        {Key: []byte("k"), Deps: toPbRefs([]Tip{old}), Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{State: &pb.ReducedEffect{}}}},
				tc.witness: {Key: []byte("k"), Deps: toPbRefs([]Tip{cut}), Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}}},
				latest:     {Key: []byte("k"), Deps: toPbRefs([]Tip{tc.witness}), Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{State: &pb.ReducedEffect{}}}},
			} {
				if err := e.effectCache.Put(tip, eff); err != nil {
					t.Fatal(err)
				}
			}

			e.index.Insert("k", nil, keytrie.NewTipSet(latest))
			e.addPeerSubscriber("k", 2)
			e.addPeerSubscriber("k", 3)
			d := newDag(e, "k", "")

			d.compact = true
			if err := d.prepare([]Tip{latest}); err != nil {
				t.Fatal(err)
			}

			e.reclaimUnreferenced()

			if v := e.effectCache.getVertex(cut); v == nil || v.root() != tc.reclaimOld {
				t.Fatalf("retained snapshot root marker does not match reclamation: %v", tc.reclaimOld)
			}

			if _, present := e.effectCache.Get(old); present == tc.reclaimOld {
				t.Fatalf("old history present=%t, want reclaimed=%t", present, tc.reclaimOld)
			}

			if _, present := e.effectCache.Get(latest); !present {
				t.Fatal("reclaimed the live LCA snapshot")
			}

			if len(tc.members) != 0 {
				if _, present := e.effectCache.Get(cut); !present {
					t.Fatal("reclaimed the snapshot needed beneath the peer's witness")
				}
			}
		})
	}
}

func TestCompactionRetainsTwoSnapshots(t *testing.T) {
	e := newTestEngine(nil)
	defer func() { _ = e.Close() }()

	old, previous, between, latest := Tip{42, 1}, Tip{42, 2}, Tip{42, 3}, Tip{42, 4}
	for tip, eff := range map[Tip]*pb.Effect{
		old:      {Key: []byte("k"), Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("old"))}},
		previous: {Key: []byte("k"), Deps: toPbRefs([]Tip{old}), Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{State: &pb.ReducedEffect{}}}},
		between:  {Key: []byte("k"), Deps: toPbRefs([]Tip{previous}), Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("new"))}},
		latest:   {Key: []byte("k"), Deps: toPbRefs([]Tip{between}), Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{State: &pb.ReducedEffect{}}}},
	} {
		if err := e.effectCache.Put(tip, eff); err != nil {
			t.Fatal(err)
		}
	}

	for _, current := range []Tip{previous, latest} {
		if _, ok := e.index.Insert("k", e.index.Contains("k"), keytrie.NewTipSet(current)); !ok {
			t.Fatal("failed to install snapshot frontier")
		}

		d := newDag(e, "k", "")

		d.compact = true
		if err := d.prepare([]Tip{current}); err != nil {
			t.Fatal(err)
		}

		e.reclaimUnreferenced()

		if _, present := e.effectCache.Get(old); present != (current == previous) {
			t.Fatalf("frontier %v: oldest history present=%t", current, present)
		}

		for _, retained := range []Tip{previous, between, latest} {
			if _, present := e.effectCache.Get(retained); !present {
				t.Fatalf("frontier %v: reclaimed retained snapshot or intervening history %v", current, retained)
			}
		}
	}
}

func TestHistoryOverlaysConstrainEvidenceUntilTransactionEnds(t *testing.T) {
	b := &historyBroadcaster{progress: []HistoryProgress{{Node: 2, Minted: 22, Processed: 5, Received: 17}}}
	b.peerIDs = []pb.NodeID{2}

	e := newTestEngine(b)
	defer func() { _ = e.Close() }()

	e.nextOff.Store(7)
	older := e.NewContext()

	older.BeginTx()
	defer older.Abort()

	e.nextOff.Store(9)

	b.progress[0].Received = 22
	newer := e.NewContext()

	newer.BeginTx()
	defer newer.Abort()

	check := func(wantMy, wantYour uint64) {
		t.Helper()

		my, your := e.ConstrainHistoryProgress(2, 12, 30)
		if my != wantMy || your != wantYour {
			t.Fatalf("effective counts=(%d,%d), want=(%d,%d)", my, your, wantMy, wantYour)
		}
	}
	check(7, 17)
	older.Abort()
	check(9, 22)

	if err := newer.Flush(); err != nil {
		t.Fatal(err)
	}

	check(12, 30)

	if e.historyOverlays != nil {
		t.Fatal("completed transactions retained their overlays")
	}
}

func TestHistoryProgressRetriesIncompleteIngestion(t *testing.T) {
	b := &historyBroadcaster{}

	e := newTestEngine(b)
	defer func() { _ = e.Close() }()

	snapshot, missing := Tip{7, 3}, Tip{8, 2}
	if err := e.effectCache.Put(snapshot, &pb.Effect{Key: []byte("k"), Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{State: &pb.ReducedEffect{}}}}); err != nil {
		t.Fatal(err)
	}

	e.installIngestedTips("k", snapshot, missing)

	if len(b.processed) != 0 {
		t.Fatal("incomplete ingestion advertised processed history")
	}

	if err := e.effectCache.Put(missing, &pb.Effect{Key: []byte("k"), Deps: toPbRefs([]Tip{snapshot}), Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("new"))}}); err != nil {
		t.Fatal(err)
	}
	// Retrying the same arrivals repairs ancestry and advances evidence.
	e.installIngestedTips("k", snapshot, missing)

	if !slices.Contains(b.processed, missing) {
		t.Fatal("a repaired duplicate arrival did not advance progress")
	}
}

func TestCompactionAdvancesPastEndedOverlayWhileNewerTransactionLives(t *testing.T) {
	e := newTestEngine(nil)
	defer func() { _ = e.Close() }()

	preSubscribe(e, "k")
	historicalTestWrite(t, e, map[string]string{"k": "old"}, false)
	oldTip := e.index.Contains("k").Tips()[0]
	older := e.NewContext()

	older.BeginTx()
	defer older.Abort()

	compact := func(value string) Tip {
		t.Helper()

		// Exercise successive compaction passes without waiting in the test.
		leaf := e.index.LoadData("k")
		if leaf != nil && leaf.lastCompaction.Load() != 0 {
			leaf.lastCompaction.Add(-int64(compactionInterval))
		}

		for range 6 {
			historicalTestWrite(t, e, map[string]string{"k": value}, false)
		}

		c := e.NewContext()
		defer c.Abort()

		if _, _, err := c.GetSnapshot("k"); err != nil {
			t.Fatal(err)
		}

		if c.keys["k"] == nil {
			t.Fatal("compaction did not emit a snapshot")
		}

		tip := c.keys["k"].lastOffset
		if err := c.Flush(); err != nil {
			t.Fatal(err)
		}

		if _, _, _, err := e.GetSnapshot("k"); err != nil {
			t.Fatal(err)
		}

		return tip
	}
	boundary := compact("old")
	newer := e.NewContext()

	newer.BeginTx()
	defer newer.Abort()

	compact("new")
	e.reclaimUnreferenced()

	if _, ok := e.effectCache.Get(oldTip); !ok {
		t.Fatal("oldest transaction lost its history")
	}

	older.Abort()
	historicalTestWrite(t, e, map[string]string{"k": "newer"}, false)

	if _, _, _, err := e.GetSnapshot("k"); err != nil {
		t.Fatal(err)
	}

	e.reclaimUnreferenced()

	if _, ok := e.effectCache.Get(oldTip); ok {
		t.Fatal("newer transaction unnecessarily retained history below its snapshot")
	}

	if _, ok := e.effectCache.Get(boundary); !ok {
		t.Fatal("reclamation removed the newer transaction's snapshot boundary")
	}

	got, _, err := newer.GetSnapshot("k")
	if err != nil || string(got.GetScalar().GetRaw()) != "old" {
		t.Fatalf("newer transaction read after reclamation: value=%v error=%v", got, err)
	}
}

func TestCompactionRetainsAncestryForDelayedNotification(t *testing.T) {
	e := newTestEngine(nil)
	defer func() { _ = e.Close() }()

	preSubscribe(e, "k")

	var delayed *pb.OffsetNotify

	for i := range 8 {
		historicalTestWrite(t, e, map[string]string{"k": fmt.Sprint(i)}, false)

		if i == 2 {
			tip := e.index.Contains("k").Tips()[0]
			eff, _ := e.effectCache.Get(tip)

			data, err := MarshalEffect(eff)
			if err != nil {
				t.Fatal(err)
			}

			delayed = BuildOffsetNotify(e.nodeID, tip, eff, data, context.Background())
		}
	}

	compactor := e.NewContext()
	defer compactor.Abort()

	if _, _, err := compactor.GetSnapshot("k"); err != nil {
		t.Fatal(err)
	}

	if err := compactor.Flush(); err != nil {
		t.Fatal(err)
	}

	if _, _, _, err := e.GetSnapshot("k"); err != nil {
		t.Fatal(err)
	}

	e.reclaimUnreferenced()

	if _, err := e.HandleRemote(delayed); err != nil {
		t.Fatal(err)
	}

	got, _, _, err := e.GetSnapshot("k")
	if err != nil || string(got.GetScalar().GetRaw()) != "7" {
		t.Fatalf("delayed notification broke compacted state: value=%v error=%v", got, err)
	}
}

func TestCompactionRetainsCommonSnapshotsDuringRejoin(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		for _, newerWitness := range []bool{false, true} {
			t.Run(fmt.Sprintf("reverse=%t/newerWitness=%t", reverse, newerWitness), func(t *testing.T) {
				b := &historyBroadcaster{progress: []HistoryProgress{{Node: 2, Minted: 3}}}
				b.peerIDs = []pb.NodeID{2}

				e := newTestEngine(b)
				defer func() { _ = e.Close() }()

				old, previous, witness, common := Tip{42, 1}, Tip{42, 2}, Tip{2, 3}, Tip{42, 4}
				left, leftSnapshot, right, rightSnapshot, latest := Tip{42, 5}, Tip{42, 6}, Tip{2, 7}, Tip{2, 8}, Tip{42, 9}
				snapshot := func(tip Tip, value int64, deps ...Tip) *pb.Effect {
					eff := cutEffect(tip, deps...)
					eff.Kind = &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{State: &pb.ReducedEffect{
						Op: pb.EffectOp_INSERT_OP, Merge: pb.MergeRule_ADDITIVE_INT,
						Collection: pb.CollectionKind_SCALAR, Commutative: true,
						Scalar: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, value),
					}}}

					return eff
				}

				nodes := map[Tip]*pb.Effect{
					old: cutEffect(old), previous: snapshot(previous, 1, old), witness: cutEffect(witness, previous),
					common: snapshot(common, 2, witness), left: cutEffect(left, common), leftSnapshot: snapshot(leftSnapshot, 3, left),
					right: cutEffect(right, common), rightSnapshot: snapshot(rightSnapshot, 3, right),
					latest: snapshot(latest, 4, leftSnapshot, rightSnapshot),
				}
				if reverse {
					slices.Reverse(nodes[latest].GetDeps())
					nodes[leftSnapshot].ForkChoiceHash, nodes[rightSnapshot].ForkChoiceHash = nodes[rightSnapshot].GetForkChoiceHash(), nodes[leftSnapshot].GetForkChoiceHash()
				}

				if newerWitness {
					b.progress[0].Minted = rightSnapshot[1]
				}

				putCutEffects(t, e, nodes)
				e.index.Insert("k", nil, keytrie.NewTipSet(latest))
				e.addPeerSubscriber("k", 2)
				d := newDag(e, "k", "")

				d.compact = true
				if err := d.prepare([]Tip{latest}); err != nil {
					t.Fatal(err)
				}

				e.reclaimUnreferenced()

				retained := previous
				if newerWitness {
					retained = common
				}

				if v := e.effectCache.getVertex(retained); v == nil || !v.root() {
					t.Fatalf("missing retained common boundary %v", retained)
				}

				for _, tip := range []Tip{common, left, leftSnapshot, right, rightSnapshot, latest} {
					if _, ok := e.effectCache.Get(tip); !ok {
						t.Fatalf("reclaimed rejoining history %v", tip)
					}
				}

				for _, tip := range []Tip{leftSnapshot, rightSnapshot} {
					if e.effectCache.getVertex(tip).root() {
						t.Fatalf("branch snapshot marked as root: %v", tip)
					}
				}

				if _, ok := e.effectCache.Get(old); ok {
					t.Fatal("eligible history below common boundary was retained")
				}

				if _, ok := e.effectCache.Get(witness); ok == newerWitness {
					t.Fatalf("subscriber boundary ignored: witness resident=%t", ok)
				}

				// The peer writes against its branch while the merged snapshot is arriving.
				delayed := Tip{2, 10}
				putCutEffects(t, e, map[Tip]*pb.Effect{delayed: cutEffect(delayed, rightSnapshot)})

				state, _, err := e.reconstruct("k", []Tip{latest, delayed}, "", false)
				if err != nil || state.GetScalar().GetIntVal() != 5 {
					t.Fatalf("rejoining reconstruction=%v error=%v, want 5", state, err)
				}
			})
		}
	}
}
