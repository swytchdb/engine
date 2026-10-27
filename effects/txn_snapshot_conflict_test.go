package effects

import (
	"fmt"
	"slices"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

// A snapshot changes how history is represented, not whether our transaction
// conflicts with that history. Exercise the same forks and causal chains with
// zero, one, and two snapshot wrappers, with and without reduced state.
func TestIsRealConflictSnapshotPlacement(t *testing.T) {
	for _, relation := range []string{"concurrent", "ancestor", "descendant", "merged-with-ours", "missing"} {
		for _, ourWins := range []bool{false, true} {
			for depth := 0; depth <= 2; depth++ {
				for _, stateful := range []bool{false, true} {
					for _, reverse := range []bool{false, true} {
						name := fmt.Sprintf("%s/win-%t/depth-%d/state-%t/reverse-%t", relation, ourWins, depth, stateful, reverse)
						t.Run(name, func(t *testing.T) {
							testSnapshotConflict(t, relation, ourWins, depth, stateful, reverse)
						})
					}
				}
			}
		}
	}
}

func testSnapshotConflict(t *testing.T, relation string, ourWins bool, depth int, stateful, reverse bool) {
	t.Helper()

	log := newSnapshotLog()
	e := newSnapshotEngine(log)
	rivalHLC, ourHLC := sTs(100), sTs(200)
	rivalHash := ComputeForkChoiceHash(2, rivalHLC)
	ourNode := pb.NodeID(3)

	for ForkChoiceLess(ComputeForkChoiceHash(ourNode, ourHLC), rivalHash) != ourWins {
		ourNode++
	}

	base := log.putEffect(&pb.Effect{
		Key: []byte("k"), NodeId: 1, Hlc: sTs(1),
		Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
	})
	wrap := func(tip Tip, extra ...Tip) Tip {
		for range depth {
			deps := append([]Tip{tip}, extra...)
			if reverse {
				slices.Reverse(deps)
			}

			snap := &pb.SnapshotEffect{}
			if stateful {
				snap.State = &pb.ReducedEffect{Collection: pb.CollectionKind_SCALAR}
			}

			tip = log.putEffect(&pb.Effect{
				Key: []byte("k"), NodeId: 1, Hlc: sTs(int64(log.nextOff)), Deps: toPbRefs(deps),
				Kind: &pb.Effect_Snapshot{Snapshot: snap},
			})
		}

		return tip
	}
	bind := func(txn string, parent Tip) (Tip, Tip) {
		node, hlc := pb.NodeID(2), rivalHLC
		if txn == "ours" {
			node, hlc = ourNode, ourHLC
		}

		log.nodeID = uint64(node)
		data := log.putEffect(&pb.Effect{
			Key: []byte("k"), NodeId: uint64(node), Hlc: hlc, TxnId: txn,
			Deps: toPbRefs([]Tip{parent}), Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		})
		anchor := log.putEffect(&pb.Effect{
			Key: []byte("k"), NodeId: uint64(node), Hlc: hlc, TxnId: txn, Deps: toPbRefs([]Tip{data}),
			Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
				TxnHlc: hlc, OriginatorNodeId: uint64(node),
				Keys: []*pb.TransactionalBindEffect_KeyBind{{
					Key: []byte("k"), NewTip: toPbRef(data), ConsumedTips: toPbRefs([]Tip{parent}),
				}},
			}},
		})
		log.nodeID = 1

		return anchor, data
	}

	var ours, ourData, rival, peer Tip

	switch relation {
	case "ancestor":
		rival, _ = bind("rival", base)
		ours, ourData = bind("ours", wrap(rival))
		peer = rival
	case "descendant":
		ours, ourData = bind("ours", base)
		rival, _ = bind("rival", wrap(ours))
		peer = rival
	default:
		ours, ourData = bind("ours", base)
		rival, _ = bind("rival", base)
		peer = rival

		if relation == "missing" && depth > 0 {
			peer = Tip{99, 123}
		}

		if relation == "merged-with-ours" {
			peer = wrap(peer, ours)
		} else {
			peer = wrap(peer)
		}
	}

	ptxn := &pendingTxn{
		txnID: "ours", bindOffset: ours, forkChoiceHash: ComputeForkChoiceHash(ourNode, ourHLC),
		keys: []pendingTxnKey{{key: "k", newTip: ourData, collection: pb.CollectionKind_SCALAR}},
		done: make(chan struct{}),
	}
	nack := e.buildEnrichedNack("k", toPbRef(ours), []Tip{peer})

	if len(nack.GetTipDetails()) != 1 {
		t.Fatal("fixture did not advertise its frontier")
	}

	want := !ourWins && relation != "ancestor" && relation != "descendant"
	// Replay's fork choice over the underlying binds is the reference answer;
	// advertising those same binds through wrappers must preserve the decision.
	losers, _ := e.losersOnKeyAt("k", []Tip{ours, rival}, nil, nil, nil, false)
	_, replayLoses := losers["ours"]

	if replayLoses != want {
		t.Errorf("reference replay loses=%t, want %t", replayLoses, want)
	}

	if relation == "missing" && depth > 0 {
		want = true // incomplete evidence cannot authorize a commit
	}

	if got := e.isRealConflict(ptxn, "k", nack.GetTipDetails()[0]); got != want {
		t.Errorf("conflict = %t, want %t", got, want)
	}

	conflict, defeated := e.checkNackConflict(ptxn, "k", nack.GetTipDetails()[0])
	if conflict != want {
		t.Errorf("commit-path conflict = %t, want %t", conflict, want)
	}

	wantDefeated := ourWins && !want && relation != "ancestor" && relation != "descendant"
	if wantDefeated {
		if len(defeated) != 1 || defeated["rival"] != rival {
			t.Errorf("defeated = %v, want underlying rival bind %v", defeated, rival)
		}
	} else if len(defeated) != 0 {
		t.Errorf("unexpected defeated binds: %v", defeated)
	}
}

// A common snapshot really is a safe boundary, even if its folded history is
// no longer fetchable. A snapshot merely at the peer's frontier is not.
func TestIsRealConflictCommonSnapshotBoundary(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)
	missing := Tip{99, 123}
	common := log.putEffect(&pb.Effect{
		Key: []byte("k"), NodeId: 1, Hlc: sTs(1), Deps: toPbRefs([]Tip{missing}),
		Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{State: &pb.ReducedEffect{}}},
	})
	ours := log.putEffect(&pb.Effect{Key: []byte("k"), Deps: toPbRefs([]Tip{common})})
	bc := &frontierBroadcaster{fetched: make(map[Tip]bool)}
	e.broadcaster = bc
	ptxn := &pendingTxn{bindOffset: ours}
	nack := e.buildEnrichedNack("k", toPbRef(ours), []Tip{common})

	if e.isRealConflict(ptxn, "k", nack.GetTipDetails()[0]) {
		t.Fatal("common ancestry is not a conflict")
	}

	if bc.fetched[missing] {
		t.Fatal("conflict walk fetched below proven common snapshot")
	}
}

func TestNackConflictFetchesWrappedCompetitors(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprintf("reverse-%t", reverse), func(t *testing.T) {
			log := newSnapshotLog()
			peer := newSnapshotEngine(log)
			bc := &frontierBroadcaster{
				fetchable: make(map[Tip][]byte),
				fetched:   make(map[Tip]bool),
			}
			binds := make(map[string]Tip)

			deps := make([]Tip, 0, 2)

			for _, txn := range []string{"a", "b"} {
				hlc := sTs(int64(log.nextOff))
				eff := &pb.Effect{
					Key: []byte("k"), NodeId: 1, Hlc: hlc, TxnId: txn,
					Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
						TxnHlc: hlc, OriginatorNodeId: 1,
					}},
				}
				tip := log.putEffect(eff)
				binds[txn] = tip
				deps = append(deps, tip)
				bc.fetchable[tip] = wireEffect(eff)
			}

			if reverse {
				slices.Reverse(deps)
			}

			snap := &pb.Effect{
				Key: []byte("k"), NodeId: 1, Hlc: sTs(int64(log.nextOff)), Deps: toPbRefs(deps),
				Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{State: &pb.ReducedEffect{}}},
			}
			tip := log.putEffect(snap)
			bc.fetchable[tip] = wireEffect(snap)
			nack := peer.buildEnrichedNack("k", nil, []Tip{tip})
			nack.CausalChain = nil // synchronous correctness cannot rely on prefetch timing
			local := newTxnTestEngine(bc)
			preSubscribe(local, "k")
			local.ingestNackTips(nack)

			// A single state snapshot is enough for ingestion/replay, but conflict
			// classification must compare its underlying binds with our branch.
			ptxn := &pendingTxn{forkChoiceHash: []byte{0}, bindOffset: Tip{42, 1}}
			conflict, defeated := local.checkNackConflict(ptxn, "k", nack.GetTipDetails()[0])

			if conflict || len(defeated) != len(binds) {
				t.Fatalf("conflict=%t defeated=%v, want both underlying binds", conflict, defeated)
			}

			for txn, anchor := range binds {
				if defeated[txn] != anchor || !bc.fetched[anchor] {
					t.Errorf("%s: defeated=%v fetched=%t, want %v", txn, defeated[txn], bc.fetched[anchor], anchor)
				}
			}
		})
	}
}

func TestReachesFromTxBindNewTips(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprintf("missing-%t", missing), func(t *testing.T) {
			log := newSnapshotLog()
			e := newSnapshotEngine(log)
			target := Tip{8, 1}
			intermediate := Tip{8, 2}

			if !missing {
				intermediate = log.putEffect(&pb.Effect{Deps: toPbRefs([]Tip{target})})
			}

			bind := log.putEffect(&pb.Effect{
				Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
					Keys: []*pb.TransactionalBindEffect_KeyBind{{Key: []byte("other"), NewTip: toPbRef(intermediate)}},
				}},
			})
			if got := e.reachesFromTx(&pendingTxn{bindOffset: bind}, target); got == missing {
				t.Errorf("reaches target = %t, want %t", got, !missing)
			}
		})
	}
}

func TestIsRealConflictWrappedData(t *testing.T) {
	for _, tc := range []struct {
		name, element, txn string
		want               bool
	}{
		{"same-element", "a", "", true},
		{"disjoint-element", "b", "", false},
		{"in-progress", "a", "pending", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log := newSnapshotLog()
			e := newSnapshotEngine(log)
			tip := log.putEffect(&pb.Effect{
				Key: []byte("k"), NodeId: 1, Hlc: sTs(1), TxnId: tc.txn,
				Kind: &pb.Effect_Data{Data: &pb.DataEffect{Collection: pb.CollectionKind_KEYED, Id: []byte(tc.element)}},
			})
			ptxn := newTestPendingTxn(pendingTxnKey{key: "k", collection: pb.CollectionKind_KEYED, elementIDs: [][]byte{[]byte("a")}})

			for depth := range 3 {
				nack := e.buildEnrichedNack("k", nil, []Tip{tip})
				if got := e.isRealConflict(ptxn, "k", nack.GetTipDetails()[0]); got != tc.want {
					t.Errorf("depth %d: conflict=%t, want %t", depth, got, tc.want)
				}

				tip = log.putEffect(&pb.Effect{
					Key: []byte("k"), NodeId: 1, Hlc: sTs(int64(log.nextOff)), Deps: toPbRefs([]Tip{tip}),
					Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{State: &pb.ReducedEffect{}}},
				})
			}
		})
	}
}

func TestNackConflictOtherKeyAncestry(t *testing.T) {
	for _, tc := range []struct {
		key        string
		descendant bool
	}{{"k", false}, {"k", true}, {"", false}, {"", true}} {
		t.Run(fmt.Sprintf("key-%q/descendant-%t", tc.key, tc.descendant), func(t *testing.T) {
			key := tc.key
			log := newSnapshotLog()
			e := newSnapshotEngine(log)
			base := log.putEffect(&pb.Effect{Key: []byte(key), Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}}})
			makeBind := func(txn string, otherParent Tip) (Tip, Tip, Tip) {
				data := log.putEffect(&pb.Effect{Key: []byte(key), TxnId: txn, Deps: toPbRefs([]Tip{base})})
				other := log.putEffect(&pb.Effect{Key: []byte("other"), TxnId: txn, Deps: toPbRefs([]Tip{otherParent})})
				hlc := sTs(int64(log.nextOff))
				bind := log.putEffect(&pb.Effect{
					Key: []byte(key), TxnId: txn, NodeId: 1, Hlc: hlc, Deps: toPbRefs([]Tip{data, other}),
					Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
						TxnHlc: hlc, OriginatorNodeId: 1,
						Keys: []*pb.TransactionalBindEffect_KeyBind{
							{Key: []byte(key), NewTip: toPbRef(data)},
							{Key: []byte("other"), NewTip: toPbRef(other)},
						},
					}},
				})

				return bind, data, other
			}

			var ours, data, other, rival Tip
			if tc.descendant {
				ours, data, other = makeBind("ours", base)
				rival, _, _ = makeBind("rival", ours)
			} else {
				rival, _, _ = makeBind("rival", base)
				ours, data, other = makeBind("ours", rival)
			}

			ptxn := &pendingTxn{
				txnID: "ours", bindOffset: ours, forkChoiceHash: []byte{0xff},
				keys: []pendingTxnKey{{key: key, newTip: data}, {key: "other", newTip: other}},
			}
			nack := e.buildEnrichedNack(key, nil, []Tip{rival})

			if !e.isRealConflict(ptxn, key, nack.GetTipDetails()[0]) {
				t.Fatal("ordering through another key cannot excuse a concurrent fork on k")
			}

			// Global ancestry is still meaningful to other callers.
			if !tc.descendant && !e.reachesFromTx(ptxn, rival) {
				t.Fatal("global ancestry proof was lost")
			}
		})
	}
}
