package effects

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
)

func TestIngestedFrontierResolvesUncachedHistory(t *testing.T) {
	for _, remote := range []bool{false, true} {
		for _, cached := range []bool{false, true} {
			t.Run(fmt.Sprintf("remote=%t/cached=%t", remote, cached), func(t *testing.T) {
				e := newSnapshotEngine(nil)
				defer func() { _ = e.Close() }()

				preSubscribe(e, "k")

				bc := &frontierBroadcaster{fetched: make(map[Tip]bool)}
				bc.fetchable = map[Tip][]byte{{7, 2}: wireEffect(frontierEffect(2, 1))}
				e.broadcaster = bc

				if err := e.effectCache.Put(Tip{7, 1}, frontierEffect(1)); err != nil {
					t.Fatal(err)
				}

				if cached {
					if err := e.effectCache.Put(Tip{7, 2}, frontierEffect(2, 1)); err != nil {
						t.Fatal(err)
					}
				}

				e.index.Insert("k", nil, keytrie.NewTipSet(Tip{7, 1}))

				notify := &pb.OffsetNotify{Origin: toPbRef(Tip{7, 3}), EffectData: wireEffect(frontierEffect(3, 2))}

				var err error
				if remote {
					_, err = e.HandleRemote(notify)
				} else {
					err = e.handleBackfill(notify)
				}

				if err != nil {
					t.Fatal(err)
				}

				requireFrontier(t, e, Tip{7, 3})

				if bc.fetched[Tip{7, 2}] == cached {
					t.Fatalf("dependency fetched=%t, cached=%t", bc.fetched[Tip{7, 2}], cached)
				}
			})
		}
	}
}

// A failed branch must neither hide another failure nor prevent the walk from
// proving that a candidate on an available branch is superseded. Callers can
// retain unresolved tips and report the errors without discarding valid proof.
func TestNormalizeTipCandidatesPreservesPartialProofAndErrors(t *testing.T) {
	firstErr := errors.New("first dependency unavailable")
	secondErr := errors.New("second dependency unavailable")
	firstMissing, secondMissing := Tip{7, 9}, Tip{7, 10}
	ancestor, descendant := Tip{7, 1}, Tip{7, 3}

	live, covered, err := NormalizeCloudTipCandidates("k", []Tip{firstMissing, ancestor, secondMissing, descendant}, nil, func(tip Tip) (*pb.Effect, error) {
		switch tip {
		case firstMissing:
			return nil, firstErr
		case secondMissing:
			return nil, secondErr
		case descendant:
			return frontierEffect(3, 2), nil
		case (Tip{7, 2}):
			return frontierEffect(2, 1), nil
		default:
			return frontierEffect(1), nil
		}
	})
	if !errors.Is(err, firstErr) || !errors.Is(err, secondErr) {
		t.Fatalf("normalization lost a fetch error: %v", err)
	}

	if !slices.Equal(live, []Tip{firstMissing, secondMissing, descendant}) || !slices.Equal(covered, []Tip{ancestor}) {
		t.Fatalf("partial normalization: live=%v covered=%v", live, covered)
	}
}
