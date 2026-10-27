package effects

import (
	"fmt"
	"slices"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

// These fixtures describe history independently of any walker's bookkeeping.
// Edges point backwards in time. Snapshot values include their full ancestry.
type nackFrontierNode struct {
	name, kind string
	deps       []string
	value      int64
}

type nackFrontierCase struct {
	name     string
	nodes    []nackFrontierNode
	tips     []string
	lca      string
	value    int64
	missing  bool
	required []string // refs needed even when another branch cannot be resolved
	skipped  []string // history behind a valid common snapshot, or another key
}

type frontierBroadcaster struct {
	selectiveBroadcaster

	fetched map[Tip]bool
}

func (b *frontierBroadcaster) FetchFromAny(ref *pb.EffectRef, hint FetchHint) ([]byte, error) {
	b.fetched[r(ref)] = true

	return b.selectiveBroadcaster.FetchFromAny(ref, hint)
}

// Replay answers what state is visible; collection advertises fetchable ancestry;
// ingestion makes that ancestry locally available. Their outputs differ, but
// collection must cover replay, and ingestion must recover every available ref
// collection advertises. Neither may mistake an exhausted branch for convergence.
func TestNackWalkersDependencyFrontier(t *testing.T) {
	cases := []nackFrontierCase{
		{
			name: "plain",
			nodes: []nackFrontierNode{
				{"a", "data", nil, 1},
				{"b", "data", []string{"a"}, 1},
				{"l", "data", []string{"a"}, 1},
				{"r", "data", []string{"b"}, 1},
			},
			tips: []string{"l", "r"}, value: 4,
		},
		{
			name: "snapshot-bypassed",
			nodes: []nackFrontierNode{
				{"a", "data", nil, 1},
				{"b", "data", []string{"a"}, 1},
				{"s", "snapshot", []string{"b"}, 2},
				{"l", "data", []string{"a"}, 1},
				{"r", "data", []string{"s"}, 1},
			},
			tips: []string{"l", "r"}, value: 4, required: []string{"b"},
		},
		{
			name: "nested-snapshots-bypassed",
			nodes: []nackFrontierNode{
				{"a", "data", nil, 1},
				{"b", "data", []string{"a"}, 1},
				{"s", "snapshot", []string{"b"}, 2},
				{"s2", "snapshot", []string{"s"}, 2},
				{"l", "data", []string{"a"}, 1},
				{"r", "data", []string{"s2"}, 1},
			},
			tips: []string{"l", "r"}, value: 4, required: []string{"b"},
		},
		{
			name: "disconnected-root",
			nodes: []nackFrontierNode{
				{"a", "data", nil, 1},
				{"b", "data", nil, 2},
				{"s", "snapshot", []string{"b"}, 2},
			},
			tips: []string{"a", "s"}, value: 3, required: []string{"b"},
		},
		{
			name: "missing-branch",
			nodes: []nackFrontierNode{
				{"b", "data", nil, 2}, {"s", "snapshot", []string{"b"}, 2},
			},
			tips: []string{"missing", "s"}, missing: true, required: []string{"missing", "b"},
		},
		{
			name: "missing-dependency",
			nodes: []nackFrontierNode{
				{"a", "data", []string{"missing"}, 1},
				{"b", "data", nil, 2},
				{"s", "snapshot", []string{"b"}, 2},
				{"r", "data", []string{"s"}, 1},
			},
			tips: []string{"a", "r"}, missing: true, required: []string{"missing", "b"},
		},
		{
			name: "common-snapshot-with-pruned-history",
			nodes: []nackFrontierNode{
				{"s", "snapshot", []string{"pruned"}, 10},
				{"l", "data", []string{"s"}, 1},
				{"r", "data", []string{"s"}, 1},
				{"m", "data", []string{"l", "r"}, 1},
			},
			tips: []string{"s", "m"}, lca: "s", value: 13, skipped: []string{"pruned"},
		},
		{
			name: "verdict-only-snapshot",
			nodes: []nackFrontierNode{
				{"b", "data", nil, 2}, {"s", "verdict", []string{"b"}, 0},
			},
			tips: []string{"s"}, value: 2, required: []string{"b"},
		},
		{
			name: "common-root-snapshot-reached-late",
			nodes: []nackFrontierNode{
				{"s", "snapshot", nil, 10},
				{"a", "data", []string{"s"}, 1},
				{"s2", "snapshot", []string{"a"}, 11},
				{"l", "data", []string{"s"}, 1},
				{"r", "data", []string{"s2"}, 1},
			},
			tips: []string{"l", "r"}, lca: "s", value: 13, required: []string{"a"},
		},
		{
			name: "standalone-subscription",
			nodes: []nackFrontierNode{
				{"sub", "subscription", nil, 0}, {"s", "snapshot", []string{"pruned"}, 10},
			},
			tips: []string{"sub", "s"}, lca: "s", value: 10, skipped: []string{"pruned"},
		},
		{
			name: "referenced-subscription",
			nodes: []nackFrontierNode{
				{"sub", "subscription", nil, 0},
				{"a", "data", []string{"sub"}, 1},
				{"b", "data", nil, 2},
				{"s", "snapshot", []string{"b"}, 2},
			},
			tips: []string{"sub", "a", "s"}, value: 3, required: []string{"b"},
		},
		{
			name: "per-key-bind",
			nodes: []nackFrontierNode{
				{"a", "tx-data", nil, 1}, {"b", "bind", []string{"a", "other-key"}, 0},
			},
			tips: []string{"b"}, value: 1, required: []string{"a"}, skipped: []string{"other-key"},
		},
	}
	for _, tc := range cases {
		for _, reverseTips := range []bool{false, true} {
			for _, reverseDeps := range []bool{false, true} {
				for _, duplicates := range []bool{false, true} {
					name := fmt.Sprintf("%s/tips-%t/deps-%t/duplicates-%t", tc.name, reverseTips, reverseDeps, duplicates)
					t.Run(name, func(t *testing.T) {
						checkNackFrontier(t, tc, reverseTips, reverseDeps, duplicates)
					})
				}
			}
		}
	}
}

func checkNackFrontier(t *testing.T, tc nackFrontierCase, reverseTips, reverseDeps, duplicates bool) {
	t.Helper()

	refs := make(map[string]Tip)
	ref := func(name string) Tip {
		if name == "" {
			return Tip{}
		}

		if tip, ok := refs[name]; ok {
			return tip
		}

		tip := Tip{7, uint64(len(refs) + 1)}
		refs[name] = tip

		return tip
	}
	local := newSnapshotEngine(nil)
	remote := newSnapshotEngine(nil)

	preSubscribe(local, "k")
	preSubscribe(remote, "k")

	bc := &frontierBroadcaster{
		fetchable: make(map[Tip][]byte),
		fetched:   make(map[Tip]bool),
	}
	remote.broadcaster = bc

	for _, node := range tc.nodes {
		tip := ref(node.name)
		eff := &pb.Effect{Key: []byte("k"), NodeId: 7, Hlc: sTs(int64(tip[1]))}

		for _, dep := range node.deps {
			eff.Deps = append(eff.Deps, toPbRef(ref(dep)))
		}

		switch node.kind {
		case "data", "tx-data":
			eff.Kind = &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, node.value)}
			if node.kind == "tx-data" {
				eff.TxnId = "txn"
			}
		case "snapshot", "verdict":
			snap := &pb.SnapshotEffect{Collection: pb.CollectionKind_SCALAR}
			if node.kind == "snapshot" {
				snap.State = &pb.ReducedEffect{
					Op: pb.EffectOp_INSERT_OP, Merge: pb.MergeRule_ADDITIVE_INT,
					Collection: pb.CollectionKind_SCALAR, Commutative: true,
					Scalar: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, node.value),
				}
			}

			eff.Kind = &pb.Effect_Snapshot{Snapshot: snap}
		case "subscription":
			eff.Kind = &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{SubscriberNodeId: 7}}
		case "bind":
			eff.TxnId = "txn"
			eff.Kind = &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
				TxnHlc: eff.GetHlc(), OriginatorNodeId: 7,
				Keys: []*pb.TransactionalBindEffect_KeyBind{
					{Key: []byte("k"), NewTip: toPbRef(ref(node.deps[0]))},
					{Key: []byte("other"), NewTip: toPbRef(ref(node.deps[1]))},
				},
			}}
		default:
			t.Fatalf("unknown fixture kind %q", node.kind)
		}

		if reverseDeps {
			slices.Reverse(eff.GetDeps())
		}

		if duplicates {
			eff.Deps = append(eff.GetDeps(), eff.GetDeps()...)
		}

		eff.ForkChoiceHash = ComputeForkChoiceHash(7, eff.GetHlc())
		if err := local.effectCache.Put(tip, eff); err != nil {
			t.Fatal(err)
		}

		bc.fetchable[tip] = wireEffect(eff)
	}

	var tips []Tip
	for _, name := range tc.tips {
		tips = append(tips, ref(name))
	}

	if reverseTips {
		slices.Reverse(tips)
	}

	if duplicates {
		tips = append(tips, tips...)
	}

	chain := local.collectCausalChain("k", tips)
	advertised := make(map[Tip]bool)

	for _, entry := range chain {
		if advertised[r(entry)] {
			t.Errorf("duplicate advertised ref: %v", r(entry))
		}

		advertised[r(entry)] = true
	}

	nack := &pb.NackNotify{Key: []byte("k")}
	for _, tip := range tips {
		nack.TipDetails = append(nack.TipDetails, &pb.NackTipDetail{Ref: toPbRef(tip)})
	}
	// No prefetch list: require the synchronous walk to discover ancestry.
	remote.ingestNackTips(nack)

	for tip := range advertised {
		if !bc.fetched[tip] {
			t.Errorf("ingestion never attempted advertised ref %v", tip)
		}

		if _, available := bc.fetchable[tip]; available {
			if _, cached := remote.effectCache.Get(tip); !cached {
				t.Errorf("ingestion omitted available advertised ref %v", tip)
			}
		}
	}

	for _, name := range tc.required {
		if !advertised[ref(name)] || !bc.fetched[ref(name)] {
			t.Errorf("required ancestry %s: advertised=%t fetched=%t", name, advertised[ref(name)], bc.fetched[ref(name)])
		}
	}

	for _, name := range tc.skipped {
		if advertised[ref(name)] || bc.fetched[ref(name)] {
			t.Errorf("unneeded ancestry %s: advertised=%t fetched=%t", name, advertised[ref(name)], bc.fetched[ref(name)])
		}
	}

	// Disable fetching so replay cannot silently repair incomplete ingestion.
	remote.broadcaster = nil
	for _, engine := range []*Engine{local, remote} {
		d := newDag(engine, "k", "")
		err := d.prepare(tips)

		if tc.missing {
			if err == nil {
				t.Error("replay accepted an unresolved dependency")
			}

			continue
		}

		if err != nil {
			t.Fatal(err)
		}

		if d.lcaTip != ref(tc.lca) {
			t.Errorf("LCA = %v, want %v", d.lcaTip, ref(tc.lca))
		}

		for tip := range d.nodes {
			if !advertised[tip] {
				t.Errorf("replay requires unadvertised ref %v", tip)
			}
		}

		got, _, err := engine.reconstruct("k", tips, "", false)
		if err != nil {
			t.Fatal(err)
		}

		if got.GetScalar().GetIntVal() != tc.value {
			t.Errorf("replay value = %d, want %d", got.GetScalar().GetIntVal(), tc.value)
		}
	}
}
