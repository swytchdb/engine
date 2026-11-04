package effects

import (
	"errors"
	"slices"
	"sync"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
)

type bootstrapReplyBroadcaster struct {
	mockBroadcaster

	started sync.WaitGroup
}

func (b *bootstrapReplyBroadcaster) ReplicateTo(_ *pb.OffsetNotify, _ []byte, peer pb.NodeID) ([]*pb.NackNotify, error) {
	// All requests must start before any completes, exercising independent
	// result slots and ensuring bootstrap still probes peers concurrently.
	b.started.Done()
	b.started.Wait()

	switch peer {
	case 2:
		return nil, nil // ACK without state
	case 3:
		return []*pb.NackNotify{
			{Tips: []*pb.EffectRef{{NodeId: 3, Offset: 10}}},
			{Tips: []*pb.EffectRef{{NodeId: 3, Offset: 11}}},
		}, nil
	default:
		return []*pb.NackNotify{{Tips: []*pb.EffectRef{{NodeId: 4, Offset: 12}}}}, errors.New("unreachable")
	}
}

func TestCollectPeerTipsCountsACKsAndDiscardsFailedReplies(t *testing.T) {
	e := newHorizonTestEngine()
	e.index.Insert("key", nil, keytrie.NewTipSet())

	b := &bootstrapReplyBroadcaster{}
	e.broadcaster = b
	peers := []pb.NodeID{2, 3, 4}

	for range 2 {
		b.started.Add(len(peers))

		tips, acknowledged := e.collectPeerTips("key", &pb.OffsetNotify{}, peers, []Tip{{1, 1}})
		if acknowledged != 2 || !slices.Equal(tips, []Tip{{1, 1}, {3, 10}, {3, 11}}) {
			t.Fatalf("unexpected replies: %d acknowledged, tips %v", acknowledged, tips)
		}

		if got := e.PeerSubscribers("key"); !slices.Equal(got, []pb.NodeID{3}) {
			t.Fatalf("only successful NACKs establish subscribers: %v", got)
		}
	}
}
