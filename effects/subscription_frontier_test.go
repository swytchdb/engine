package effects

import (
	"fmt"
	"slices"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
)

// Observing a subscription does not add a data branch beside the snapshot.
// Its folded history is deliberately absent from both nodes and the transport.
func TestSubscriptionObservationSnapshotBoundary(t *testing.T) {
	for _, tips := range [][]string{{"d"}, {"sub", "c", "d"}, {"s", "d", "sub"}} {
		for _, reverseTips := range []bool{false, true} {
			for _, reverseDeps := range []bool{false, true} {
				for _, duplicates := range []bool{false, true} {
					t.Run(fmt.Sprintf("tips-%v/reverse-%t/deps-%t/duplicates-%t", tips, reverseTips, reverseDeps, duplicates), func(t *testing.T) {
						checkNackFrontier(t, nackFrontierCase{
							nodes: []nackFrontierNode{
								{"sub", "subscription", nil, 0},
								{"s", "snapshot", []string{"pruned"}, 10},
								{"c", "data", []string{"s"}, 1},
								{"d", "data", []string{"c", "sub"}, 1},
							},
							tips: tips, lca: "s", value: 12,
							required: []string{"sub", "c"}, exhausted: []string{"pruned"},
						}, reverseTips, reverseDeps, duplicates)
					})
				}
			}
		}
	}
}

func TestSubscriptionMetadataChainPreservesDataRoot(t *testing.T) {
	for _, reverseTips := range []bool{false, true} {
		for _, reverseDeps := range []bool{false, true} {
			t.Run(fmt.Sprintf("reverse-%t/deps-%t", reverseTips, reverseDeps), func(t *testing.T) {
				checkNackFrontier(t, nackFrontierCase{
					nodes: []nackFrontierNode{
						{"sub", "subscription", nil, 0},
						{"observed", "subscription", []string{"sub"}, 0},
						{"a", "data", []string{"observed"}, 1},
						{"b", "data", nil, 2},
						{"s", "snapshot", []string{"b"}, 2},
					},
					tips: []string{"a", "s"}, value: 3, required: []string{"b"},
				}, reverseTips, reverseDeps, false)
			})
		}
	}
}

func TestSubscriptionCarriesObservedData(t *testing.T) {
	for _, tips := range [][]string{{"d"}, {"sub", "d"}, {"sub"}} {
		for _, reverseTips := range []bool{false, true} {
			t.Run(fmt.Sprintf("tips-%v/reverse-%t", tips, reverseTips), func(t *testing.T) {
				value := int64(11)
				if len(tips) == 1 && tips[0] == "sub" {
					value = 10
				}

				checkNackFrontier(t, nackFrontierCase{
					nodes: []nackFrontierNode{
						{"s", "snapshot", []string{"pruned"}, 10},
						{"sub", "subscription", []string{"s"}, 0},
						{"d", "data", []string{"sub"}, 1},
					},
					tips: tips, lca: "s", value: value,
					required: []string{"s", "sub"}, exhausted: []string{"pruned"},
				}, reverseTips, false, false)
			})
		}
	}
}

func TestSubscriptionObservationNormalization(t *testing.T) {
	for _, unsubscribe := range []bool{false, true} {
		for _, reverseDeps := range []bool{false, true} {
			for _, arrival := range []string{"normalize", "remote", "backfill"} {
				t.Run(fmt.Sprintf("unsubscribe-%t/reverse-%t/%s", unsubscribe, reverseDeps, arrival), func(t *testing.T) {
					e := newSnapshotEngine(nil)
					preSubscribe(e, "k")

					bc := &frontierBroadcaster{fetchable: make(map[Tip][]byte), fetched: make(map[Tip]bool)}
					e.broadcaster = bc
					u, s, c, d, pruned := Tip{7, 1}, Tip{7, 2}, Tip{7, 3}, Tip{7, 4}, Tip{7, 99}
					sub := frontierEffect(u[1])

					sub.Kind = &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{SubscriberNodeId: 7, Unsubscribe: unsubscribe}}
					if unsubscribe {
						// Unsubscribes remain transparent to the data walk, but
						// their observed data dependencies must still be followed.
						sub.Deps = toPbRefs([]Tip{c})
					}

					snap := frontierEffect(s[1], pruned[1])
					snap.Kind = &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{State: &pb.ReducedEffect{
						Scalar: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 10),
					}}}

					child := frontierEffect(d[1], c[1], u[1])
					if unsubscribe {
						child.Deps = toPbRefs([]Tip{u})
					}

					if reverseDeps {
						slices.Reverse(child.GetDeps())
					}

					for tip, eff := range map[Tip]*pb.Effect{u: sub, s: snap, c: frontierEffect(c[1], s[1]), d: child} {
						if err := e.effectCache.Put(tip, eff); err != nil {
							t.Fatal(err)
						}
					}

					e.index.Insert("k", nil, keytrie.NewTipSet(u, c))

					for range 2 { // retries must preserve the same frontier
						switch arrival {
						case "normalize":
							tips, _, err := NormalizeCloudTipCandidates("k", []Tip{u, c, d}, nil, func(tip Tip) (*pb.Effect, error) {
								return e.getEffect("k", tip)
							})
							if err != nil || !slices.Equal(tips, []Tip{d}) {
								t.Fatalf("normalization = %v, %v; want [%v], nil", tips, err, d)
							}
						case "remote":
							if _, err := e.HandleRemote(&pb.OffsetNotify{Origin: toPbRef(d), EffectData: wireEffect(child)}); err != nil {
								t.Fatal(err)
							}

							requireFrontier(t, e, d)
						case "backfill":
							if err := e.handleBackfill(&pb.OffsetNotify{Origin: toPbRef(d), EffectData: wireEffect(child)}); err != nil {
								t.Fatal(err)
							}

							requireFrontier(t, e, d)
						}
					}

					// Ingestion exhausts the missing boundary. Cloud normalization
					// still uses the reconstruction walk and stops at its LCA.
					if want := arrival != "normalize"; bc.fetched[pruned] != want {
						t.Fatalf("boundary fetch=%t, want %t for %s", bc.fetched[pruned], want, arrival)
					}
				})
			}
		}
	}
}
