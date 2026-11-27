package effects

import (
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

func TestBindEvidenceUsesReconstructionBoundary(t *testing.T) {
	e := newTestEngine(nil)
	missing := Tip{1, 1}
	foreign := Tip{1, 2}
	boundary := Tip{1, 3}
	current := Tip{1, 4}

	for tip, eff := range map[Tip]*pb.Effect{
		foreign: {
			Key: []byte("descriptor"), Deps: []*pb.EffectRef{toPbRef(missing)},
			Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{State: &pb.ReducedEffect{}}},
		},
		boundary: {
			Key:  []byte("node"),
			Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{State: &pb.ReducedEffect{}}},
		},
		current: {
			Key: []byte("node"), Deps: []*pb.EffectRef{toPbRef(boundary), toPbRef(foreign)},
			Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("value"))},
		},
	} {
		if err := e.effectCache.Put(tip, eff); err != nil {
			t.Fatal(err)
		}
	}

	visited := make(map[Tip]bool)

	err := e.walkBindEvidence(logicalAddress("node"), []Tip{current}, func(tip Tip, _ *pb.Effect) {
		visited[tip] = true
	})
	if err != nil {
		t.Fatalf("evidence discovery reopened foreign snapshot ancestry: %v", err)
	}

	if len(visited) != 2 || !visited[current] || !visited[boundary] {
		t.Fatalf("evidence discovery left the reconstruction graph: %v", visited)
	}
}
