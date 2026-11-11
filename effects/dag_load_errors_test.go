package effects

import (
	"errors"
	"fmt"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

func TestWalkKeyHistoryTransportErrorPrecedesMissing(t *testing.T) {
	transportErr := errors.New("dependency fetch timed out")

	for _, dependencies := range []bool{false, true} {
		for _, missingFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("dependencies=%t/missing-first=%t", dependencies, missingFirst), func(t *testing.T) {
				tips := []Tip{{7, 1}, {7, 2}}
				if !missingFirst {
					tips[0], tips[1] = tips[1], tips[0]
				}

				root := &pb.Effect{Deps: toPbRefs(tips)}
				if dependencies {
					tips = []Tip{{7, 3}}
				}

				_, err := walkKeyHistory("k", tips, func(tip Tip) (*pb.Effect, error) {
					switch tip {
					case (Tip{7, 1}):
						return nil, fmt.Errorf("fetch %v: %w", tip, ErrCDNBlobMissing)
					case (Tip{7, 2}):
						return nil, fmt.Errorf("fetch %v: %w", tip, transportErr)
					default:
						return root, nil
					}
				})
				if !errors.Is(err, transportErr) || errors.Is(err, ErrCDNBlobMissing) {
					t.Fatalf("mixed load failures must classify as transport failure: %v", err)
				}
			})
		}
	}
}

type mixedFailureBroadcaster struct {
	mockBroadcaster

	transportErr error
}

func (b *mixedFailureBroadcaster) FetchFromAny(ref *pb.EffectRef, _ FetchHint) ([]byte, error) {
	switch r(ref) {
	case (Tip{7, 4}):
		return wireEffect(frontierEffect(4, 2, 3)), nil
	case (Tip{7, 2}):
		return nil, ErrCDNBlobMissing
	default:
		return nil, b.transportErr
	}
}

func TestCloudReadMixedWalkFailuresDoesNotServeSubset(t *testing.T) {
	transportErr := errors.New("dependency fetch connection reset")
	bc := &mixedFailureBroadcaster{transportErr: transportErr}
	bc.allRegionPeersReachable = true
	bc.peerIDs = []pb.NodeID{7}

	e := newTestEngine(bc)
	defer func() { _ = e.Close() }()

	readable, mixed := Tip{7, 1}, Tip{7, 5}
	fake := &fakeCloudReader{
		tips: map[string][]Tip{"k": {readable, mixed}},
		sidecar: map[string][]CloudEffect{"k": {
			cloudEffect(t, readable, wireEffect(frontierEffect(1))),
			cloudEffect(t, mixed, wireEffect(frontierEffect(5, 4))),
		}},
		// Cloud lacks this intermediate effect, but a peer still has it.
		// Resolving it during DAG validation exposes both a hole and an
		// unavailable dependency beneath the same frontier tip.
		missing: map[string][]Tip{"k": {{7, 4}}},
	}
	e.SetCloudReader(fake)

	result, _, err := e.NewReadOnlyContext().GetSnapshot("k")
	if !errors.Is(err, ErrCloudUnavailable) || !errors.Is(err, transportErr) {
		t.Fatalf("read must preserve transport failure, got result=%v err=%v", result, err)
	}

	if len(fake.pending) != 0 {
		t.Fatalf("transport failure was classified as a pending hole: %v", fake.pending)
	}
}
