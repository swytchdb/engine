package effects

import (
	"errors"
	"fmt"
	"testing"
	"testing/synctest"

	pb "github.com/swytchdb/engine/cluster/proto"
)

type mixedWriteBroadcaster struct {
	bindPublicationBroadcaster

	beforeBind func(*pb.OffsetNotify) error
	responses  []*pb.NackNotify
}

func (b *mixedWriteBroadcaster) Replicate(n *pb.OffsetNotify, _ []byte) error {
	_, err := b.peer.HandleRemote(n)

	return err
}

func (b *mixedWriteBroadcaster) ReplicateMarshalled(n *pb.OffsetNotify, _ []byte, _ pb.NodeID) ([]*pb.NackNotify, error) {
	if b.beforeBind != nil {
		if err := b.beforeBind(n); err != nil {
			return nil, err
		}
	}

	nacks, err := b.peer.HandleRemote(n)
	b.responses = nacks

	return nacks, err
}

// Control the delivery order between two engines: A reads x and writes y;
// B writes x and reads the old y before receiving A's bind. Mixed operations
// have no isolation guarantee, but the transaction result must match its reads.
func TestNonTransactionalWriteAtBindPublication(t *testing.T) {
	for _, withHorizon := range []bool{false, true} {
		for _, afterPublication := range []bool{false, true} {
			t.Run(fmt.Sprintf("horizon=%t/after-publication=%t", withHorizon, afterPublication), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					var (
						engines [2]*Engine
						links   [2]*mixedWriteBroadcaster
					)
					for i := range engines {
						links[i] = &mixedWriteBroadcaster{}
						links[i].peerIDs = []pb.NodeID{pb.NodeID(2 - i)}
						links[i].allRegionPeersReachable = true
						engines[i] = newTxnTestEngine(links[i])
						engines[i].nodeID = pb.NodeID(i + 1)
						engines[i].UpdateSafetyRules(SafeMode, nil)

						if withHorizon {
							engines[i].horizon = newHorizonSet(engines[i])
						}

						for j, key := range []string{"x", "y"} {
							base := Tip{99, uint64(j + 1)}
							if err := engines[i].effectCache.Put(base, scalarSet(key, "base")); err != nil {
								t.Fatal(err)
							}

							engines[i].updateIndex(key, nil, base)
							preSubscribe(engines[i], key)
							engines[i].addPeerSubscriber(key, pb.NodeID(2-i))
						}
					}

					links[0].peer, links[1].peer = engines[1], engines[0]
					deliverQueued := func(i int) error {
						link := links[i]
						link.mu.Lock()
						queued := link.broadcasts
						link.broadcasts = nil
						link.mu.Unlock()

						for _, n := range queued {
							if _, err := engines[1-i].HandleRemote(n); err != nil {
								return err
							}
						}

						return nil
					}
					tx := engines[0].NewContext()
					tx.BeginTx()

					initial, _, err := tx.GetSnapshot("x")
					if err != nil || string(initial.GetScalar().GetRaw()) != "base" {
						t.Fatalf("initial read: value=%v err=%v", initial, err)
					}

					if err := tx.Emit(scalarSet("y", "committed")); err != nil {
						t.Fatal(err)
					}

					writeOutside := func() error {
						other := engines[1].NewContext()
						if err := other.Emit(scalarSet("x", "outside")); err != nil {
							return err
						}

						if err := other.Flush(); err != nil {
							return err
						}

						if err := deliverQueued(1); err != nil {
							return err
						}

						reader := engines[1].NewContext()

						value, _, err := reader.GetSnapshot("y")
						if err != nil || string(value.GetScalar().GetRaw()) != "base" {
							return fmt.Errorf("read before bind arrival: value=%v err=%v", value, err)
						}

						return nil
					}

					var bindTip Tip

					links[0].beforeBind = func(n *pb.OffsetNotify) error {
						bindTip = r(n.GetOrigin())
						if !afterPublication || !engines[0].index.Contains("y").Contains(bindTip) {
							return errors.New("unexpected bind publication")
						}

						return writeOutside()
					}

					if !afterPublication {
						if err := writeOutside(); err != nil {
							t.Fatal(err)
						}
					}

					err = tx.Flush()
					wantY := "base"

					if afterPublication {
						if err != nil || bindTip == (Tip{}) || len(links[0].responses) == 0 {
							t.Fatalf("published bind must commit despite non-transactional NACK: bind=%v nacks=%v err=%v", bindTip, links[0].responses, err)
						}

						wantY = "committed"
					} else if !errors.Is(err, ErrTxnAborted) || bindTip != (Tip{}) {
						t.Fatalf("pre-publication conflict must abort without a bind: bind=%v err=%v", bindTip, err)
					}

					// Drain asynchronous effects and verdict snapshots before reads.
					for i := range links {
						if err := deliverQueued(i); err != nil {
							t.Fatal(err)
						}
					}

					for i, e := range engines {
						for key, want := range map[string]string{"x": "outside", "y": wantY} {
							reader := e.NewContext()

							value, _, err := reader.GetSnapshot(key)
							if err != nil || string(value.GetScalar().GetRaw()) != want {
								t.Errorf("node%d key=%s: value=%v err=%v, want %q", i, key, value, err, want)
							}
						}
					}
				})
			})
		}
	}
}
