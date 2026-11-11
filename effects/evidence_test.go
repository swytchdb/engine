package effects

import (
	"context"
	"sync"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

func evidenceSnapshot(key string, refs ...Tip) *pb.Effect {
	return &pb.Effect{Key: []byte(key), Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{Evidence: &pb.BindEvidence{Deps: toPbRefs(refs)}}}}
}

func bindEvidenceSnapshot(key string, proof *pb.BindEvidence) *pb.Effect {
	return &pb.Effect{Key: []byte(key), Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{Evidence: proof}}}
}

func TestSnapshotEvidenceReconstructsPriorityChainWithoutLabels(t *testing.T) {
	log := newSnapshotLog()
	source := newSnapshotEngine(log)

	t.Cleanup(func() { _ = source.Close() })

	c := closureBind(log, "C", 0, []string{"bc"}, nil)
	b := closureBind(log, "B", 1, []string{"winner", "bc"}, nil)
	a := closureBind(log, "A", 2, []string{"winner"}, nil)
	// Unrelated history is available at the sender but is not proof.
	unrelated := log.putEffect(scalarSet("bc", "not part of the proof"))

	cProof, err := source.bindProof(c, map[string]Tip{"B": b}, nil)
	if err != nil {
		t.Fatal(err)
	}

	cSnapshot := log.putEffect(bindEvidenceSnapshot("bc", cProof))
	source.effectCache.releaseEvidence(evidenceTips(cProof))

	aProof, err := source.bindProof(a, nil, []Tip{cSnapshot})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := bindEvidenceSnapshot("winner", aProof)
	snapshot.Deps = toPbRefs([]Tip{a})
	owner := log.putEffect(snapshot)

	source.effectCache.releaseEvidence(evidenceTips(aProof))
	snapshot, _ = source.effectCache.Get(owner)

	data, err := source.effectCache.MarshalEffect(snapshot)
	if err != nil {
		t.Fatal(err)
	}

	fresh := newTestEngine(nil)

	t.Cleanup(func() { _ = fresh.Close() })
	preSubscribe(fresh, "winner")

	if _, err := fresh.HandleRemote(&pb.OffsetNotify{Origin: toPbRef(owner), Key: []byte("winner"), EffectData: data}); err != nil {
		t.Fatal(err)
	}

	got, err := fresh.GetSnapshots(context.Background(), []string{"winner"})
	if err != nil || len(got) != 1 || string(got[0].GetScalar().GetRaw()) != "A" {
		t.Fatalf("fresh-node reconstruction=%v err=%v, want A", got, err)
	}

	if _, ok := fresh.subscriptions.Load("bc"); ok {
		t.Fatal("proof evaluation subscribed to another key")
	}

	if _, ok := fresh.effectCache.Get(unrelated); ok {
		t.Fatal("proof package included unrelated key history")
	}

	stored, _ := fresh.effectCache.Get(owner)

	verdicts, err := fresh.proofVerdicts(owner, stored)
	if err != nil || verdicts["A"].verdict != pb.Verdict_WON || verdicts["B"].verdict != pb.Verdict_LOST || verdicts["C"].verdict != pb.Verdict_WON {
		t.Fatalf("derived verdicts=%v err=%v", verdicts, err)
	}

	fresh.releaseChainRefs("winner", fresh.index.Contains("winner").Tips(), fresh.index.LoadData("winner"))
	drainEvidence(fresh.effectCache)

	if fresh.effectCache.EntryCount() != 0 {
		t.Fatal("reclaiming the final proof owner leaked evidence")
	}
}

func drainEvidence(p *VertexPool) {
	for p.reclaimPending.Load() != 0 {
		p.drainReclaim()
	}
}

func TestSnapshotEvidenceOwnership(t *testing.T) {
	p := newVertexPool()

	proof, first, second := Tip{1, 1}, Tip{1, 2}, Tip{1, 3}
	if err := p.PutSizedCache(proof, scalarSet("foreign", "proof"), 0); err != nil {
		t.Fatal(err)
	}

	for _, tip := range []Tip{first, second, first} {
		if err := p.Put(tip, evidenceSnapshot("k", proof, proof)); err != nil {
			t.Fatal(err)
		}
	}

	drainEvidence(p)

	if got := p.getVertex(proof).refs.Load(); got != 2 {
		t.Fatalf("evidence refs=%d, want one per snapshot vertex", got)
	}

	if !p.Incref(first) {
		t.Fatal("pin first snapshot")
	}

	p.decrefChainNode("k", first)
	p.decrefChainNode("k", second)
	drainEvidence(p)

	if got := p.getVertex(proof).refs.Load(); got != 1 {
		t.Fatalf("evidence refs after key release=%d, want independent snapshot pin to keep one", got)
	}

	p.Decref(first)
	drainEvidence(p)

	if p.EntryCount() != 0 || p.vertexBytes.Load() != 0 || p.owned.residentBytes() != 0 {
		t.Fatal("snapshot reclamation leaked its proof or accounting")
	}
}

func TestSnapshotEvidenceMissingMemberRollsBack(t *testing.T) {
	p := newVertexPool()

	proof, missing, owner := Tip{1, 1}, Tip{1, 2}, Tip{1, 3}
	if err := p.PutSizedCache(proof, scalarSet("foreign", "proof"), 0); err != nil {
		t.Fatal(err)
	}

	if err := p.Put(owner, evidenceSnapshot("k", proof, missing)); err == nil {
		t.Fatal("published snapshot with missing evidence")
	}

	drainEvidence(p)

	if p.EntryCount() != 0 {
		t.Fatal("failed evidence installation retained partial ownership")
	}
}

func TestSnapshotEvidencePackage(t *testing.T) {
	source := newVertexPool()
	proof, previous, owner := Tip{1, 1}, Tip{1, 2}, Tip{1, 3}
	// This unavailable ordinary dependency must never be fetched merely
	// because the proof package includes this vertex.
	payload := scalarSet("foreign", "proof")

	payload.Deps = toPbRefs([]Tip{{99, 99}})
	if err := source.Put(proof, payload); err != nil {
		t.Fatal(err)
	}

	if err := source.Put(previous, evidenceSnapshot("foreign", proof)); err != nil {
		t.Fatal(err)
	}

	snapshot := evidenceSnapshot("k", previous, proof)

	data, err := source.MarshalEffect(snapshot)
	if err != nil {
		t.Fatal(err)
	}

	decoded := &pb.Effect{}
	if err := UnmarshalEffect(data, decoded); err != nil {
		t.Fatal(err)
	}

	if len(decoded.GetEvidencePackage()) != 2 {
		t.Fatalf("package has %d records, want shared proof once", len(decoded.GetEvidencePackage()))
	}

	e := newTestEngine(nil)

	t.Cleanup(func() { _ = e.Close() })
	preSubscribe(e, "k")

	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			if err := e.putIngested(owner, decoded, len(data)); err != nil {
				t.Error(err)
			}
		})
	}

	done := make(chan struct{})

	go func() { wg.Wait(); close(done) }()

reclaim:
	for {
		select {
		case <-done:
			break reclaim
		default:
			e.effectCache.drainReclaim()
		}
	}

	stored, _ := e.effectCache.Get(owner)
	if stored == nil || len(stored.GetEvidencePackage()) != 0 {
		t.Fatal("pool retained transport package or failed to install owner")
	}

	if _, complete, err := e.closeIngestedFrontier("k", []Tip{owner}, make(map[Tip]error)); err != nil || !complete {
		t.Fatalf("closure tried to reconstruct the foreign proof: complete=%t err=%v", complete, err)
	}

	if _, subscribed := e.subscriptions.Load("foreign"); subscribed {
		t.Fatal("receiving proof subscribed to the foreign key")
	}

	if got := e.effectCache.getVertex(proof).refs.Load(); got != 2 {
		t.Fatalf("duplicate package installation added evidence ownership: %d", got)
	}

	e.effectCache.decrefChainNode("k", owner)
	drainEvidence(e.effectCache)

	if e.effectCache.EntryCount() != 0 {
		t.Fatal("nested proof leaked after owner reclamation")
	}
}

func TestSnapshotEvidenceSupportingBindIsNotCandidate(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)

	t.Cleanup(func() { _ = e.Close() })

	old := closureBind(log, "old", 1, []string{"k"}, nil)

	oldProof, err := e.bindProof(old, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	prior := log.putEffect(bindEvidenceSnapshot("k", oldProof))
	e.effectCache.releaseEvidence(evidenceTips(oldProof))

	connector := closureBind(log, "connector", 0, []string{"k"}, map[string]Tip{"k": old})
	current := closureBind(log, "current", 2, []string{"k"}, map[string]Tip{"k": connector})

	proof, err := e.bindProof(current, nil, []Tip{prior})
	if err != nil {
		t.Fatal(err)
	}

	owner := log.putEffect(bindEvidenceSnapshot("k", proof))
	e.effectCache.releaseEvidence(evidenceTips(proof))
	eff, _ := e.effectCache.Get(owner)

	data, err := e.effectCache.MarshalEffect(eff)
	if err != nil {
		t.Fatal(err)
	}

	fresh := newSnapshotEngine(nil)

	t.Cleanup(func() { _ = fresh.Close() })

	wire := &pb.Effect{}
	if err := UnmarshalEffect(data, wire); err != nil {
		t.Fatal(err)
	}

	if err := fresh.effectCache.Put(owner, wire); err != nil {
		t.Fatal(err)
	}

	stored, _ := fresh.effectCache.Get(owner)

	verdicts, err := fresh.proofVerdicts(owner, stored)
	if err != nil {
		t.Fatal(err)
	}

	if len(verdicts) != 2 || verdicts["old"].verdict != pb.Verdict_WON || verdicts["current"].verdict != pb.Verdict_WON {
		t.Fatalf("ordered proof binds must both win, connector must not be evaluated: %v", verdicts)
	}

	if _, ok := fresh.effectCache.Get(connector); !ok {
		t.Fatal("proof omitted connecting ancestry")
	}
}

func TestSnapshotEvidenceMalformedPackageRollsBack(t *testing.T) {
	p := newVertexPool()
	member, nested, owner := Tip{1, 1}, Tip{1, 2}, Tip{1, 3}

	data, err := MarshalEffect(scalarSet("foreign", "proof"))
	if err != nil {
		t.Fatal(err)
	}

	broken, err := MarshalEffect(evidenceSnapshot("foreign", member, Tip{99, 99}))
	if err != nil {
		t.Fatal(err)
	}

	eff := evidenceSnapshot("k", nested)

	eff.EvidencePackage = []*pb.EvidenceRecord{
		{Ref: toPbRef(member), EffectData: data},
		{Ref: toPbRef(nested), EffectData: broken},
	}
	if err := p.Put(owner, eff); err == nil {
		t.Fatal("installed incomplete nested evidence")
	}

	drainEvidence(p)

	if p.EntryCount() != 0 || p.owned.residentBytes() != 0 {
		t.Fatal("failed package retained partial ownership")
	}
}

func TestSnapshotEvidenceDurabilityKeepsProofOutOfFrontiers(t *testing.T) {
	dir := t.TempDir()
	source := newVertexPool()

	member, owner := Tip{1, 1}, Tip{1, 2}
	if err := source.Put(member, scalarSet("foreign", "proof")); err != nil {
		t.Fatal(err)
	}

	data, err := source.MarshalEffect(evidenceSnapshot("k", member))
	if err != nil {
		t.Fatal(err)
	}

	wire := &pb.Effect{}
	if err := UnmarshalEffect(data, wire); err != nil {
		t.Fatal(err)
	}

	e := durableTestEngine(t, dir)
	if err := e.effectCache.Put(owner, wire); err != nil {
		t.Fatal(err)
	}

	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e = durableTestEngine(t, dir)
	if !e.index.Contains("k").Contains(owner) {
		t.Fatal("proof owner not restored")
	}

	if e.index.Contains("foreign").Len() != 0 {
		t.Fatal("proof became foreign value frontier after restart")
	}

	if v := e.effectCache.getVertex(member); v == nil || v.refs.Load() != 1 {
		t.Fatal("proof ownership not restored")
	}

	e.releaseChainRefs("k", e.index.Contains("k").Tips(), nil)
	e.index.Delete("k", e.index.Contains("k"))
	drainEvidence(e.effectCache)

	if _, ok := e.effectCache.Get(member); ok {
		t.Fatal("restored proof leaked after owner reclaim")
	}
}
