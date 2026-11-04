package effects

import (
	"slices"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

func TestNestedSavepointsPreserveSubscriptions(t *testing.T) {
	e := newHorizonTestEngine()
	c := e.NewContext()
	emit := func(peer uint64) {
		t.Helper()

		if err := c.Emit(&pb.Effect{
			Key: []byte("key"),
			Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{
				SubscriberNodeId: peer,
			}},
		}); err != nil {
			t.Fatal(err)
		}
	}

	for _, peer := range []uint64{2, 3, 4} {
		emit(peer)
	}

	first := c.TakeSavepoint()

	emit(5)

	second := c.TakeSavepoint()
	c.RestoreSavepoint(first)
	emit(6)
	c.RestoreSavepoint(second)

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	if got := e.PeerSubscribers("key"); !slices.Equal(got, []pb.NodeID{2, 3, 4, 5}) {
		t.Fatalf("restore changed saved subscription operations: %v", got)
	}

	// Restoring and flushing again must not mutate the retained savepoint.
	c.RestoreSavepoint(second)

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	if got := e.PeerSubscribers("key"); !slices.Equal(got, []pb.NodeID{2, 3, 4, 5}) {
		t.Fatalf("repeated restore changed subscription operations: %v", got)
	}
}

func TestContextReleasesLargeCommandState(t *testing.T) {
	e := newHorizonTestEngine()
	c := e.NewContext()
	empty := c.TakeSavepoint()

	// A large aborted command must not leave its map buckets on a reused
	// context. Subsequent emission must also work after an empty restore.
	for range 128 {
		if err := c.Emit(dataEffect(e.generateTxnID())); err != nil {
			t.Fatal(err)
		}
	}

	c.Abort()

	if c.keys != nil {
		t.Fatal("large aborted command retained its key map")
	}

	c.RestoreSavepoint(empty)

	if err := c.Emit(dataEffect("next")); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	if e.index.Contains("next") == nil {
		t.Fatal("reused context did not publish the next command")
	}
}
