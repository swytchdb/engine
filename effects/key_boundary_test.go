package effects

import (
	"errors"
	"slices"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
)

func auditEngine(t *testing.T) *Engine {
	t.Helper()
	e := NewEngine(EngineConfig{DefaultMode: UnsafeMode})
	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})
	return e
}

func auditAddresses() map[string][2]string {
	return map[string][2]string{
		"logical":  {logicalAddress("main/a"), logicalAddress("main/b")},
		"internal": {keytrie.InternalKey("main", "a"), keytrie.InternalKey("main", "b")},
	}
}

func auditWrite(t *testing.T, e *Engine, key, value string) {
	t.Helper()
	c := e.NewContext()
	if err := c.Emit(addressedValue(key, value)); err != nil {
		t.Fatal(err)
	}
	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
}

func TestBoundaryAuditSubscriptionIdentity(t *testing.T) {
	for name, addresses := range auditAddresses() {
		t.Run(name, func(t *testing.T) {
			e := auditEngine(t)
			if err := e.ensureSubscribedBlocking(addresses[0]); err != nil {
				t.Fatal(err)
			}
			if tips := e.index.ContainsInternal(addresses[0]); tips == nil {
				t.Error("subscription was not indexed under the requested address")
			}
			if _, ok := e.subscription(addresses[1]); ok {
				t.Error("subscribing to A also marks unrelated B subscribed")
			}
		})
	}
}

func TestBoundaryAuditIndependentSubscribers(t *testing.T) {
	for name, addresses := range auditAddresses() {
		t.Run(name, func(t *testing.T) {
			e := auditEngine(t)
			for _, address := range addresses {
				if err := e.ensureSubscribedBlocking(address); err != nil {
					t.Fatal(err)
				}
			}
			e.addPeerSubscriber(addresses[0], 2)
			e.addPeerSubscriber(addresses[1], 3)
			if got := e.PeerSubscribersInternal(addresses[0]); !slices.Equal(got, []pb.NodeID{2}) {
				t.Errorf("A subscribers=%v; want [2]", got)
			}
			e.removePeerSubscriber(addresses[0], 2)
			if got := e.PeerSubscribersInternal(addresses[0]); len(got) != 0 {
				t.Errorf("A subscribers after unsubscribe=%v; want []", got)
			}
			if got := e.PeerSubscribersInternal(addresses[1]); !slices.Equal(got, []pb.NodeID{3}) {
				t.Errorf("B subscribers=%v; want [3]", got)
			}
		})
	}
}

func TestBoundaryAuditTransactionParticipants(t *testing.T) {
	for name, addresses := range auditAddresses() {
		t.Run(name, func(t *testing.T) {
			e := auditEngine(t)
			for _, address := range addresses {
				if err := e.ensureSubscribedBlocking(address); err != nil {
					t.Fatal(err)
				}
			}
			e.SetBroadcaster(&mockBroadcaster{peerIDs: []pb.NodeID{2, 3}})
			e.addPeerSubscriber(addresses[0], 2)
			e.addPeerSubscriber(addresses[1], 3)
			c := e.NewContext()
			c.keys = map[string]*contextKey{addresses[0]: {}}
			if got := c.collectSubscribers(); !slices.Equal(got, []pb.NodeID{2}) {
				t.Fatalf("transaction only touching A coordinates with %v; want [2]", got)
			}
		})
	}
}

func TestBoundaryAuditSnapshotSubscribers(t *testing.T) {
	for name, addresses := range auditAddresses() {
		t.Run(name, func(t *testing.T) {
			e := auditEngine(t)
			for _, address := range addresses {
				if err := e.ensureSubscribedBlocking(address); err != nil {
					t.Fatal(err)
				}
			}
			e.addPeerSubscriber(addresses[0], 2)
			e.addPeerSubscriber(addresses[1], 3)
			got := e.snapshotSubscribers(addresses[0])
			if len(got) != 1 || !got[2] {
				t.Fatalf("A snapshot subscriber set=%v; want only peer 2", got)
			}
		})
	}
}

func TestBoundaryAuditEvidenceOwnership(t *testing.T) {
	for name, addresses := range auditAddresses() {
		t.Run(name, func(t *testing.T) {
			e := auditEngine(t)
			if err := e.ensureSubscribedBlocking(addresses[0]); err != nil {
				t.Fatal(err)
			}
			if e.isServed([]byte(addresses[1])) {
				t.Fatal("unsubscribed B is treated as owned state after subscribing only to A")
			}
		})
	}
}

func TestBoundaryAuditOrdinaryPublicationHistory(t *testing.T) {
	for name, addresses := range auditAddresses() {
		t.Run(name, func(t *testing.T) {
			e := auditEngine(t)
			if err := e.ensureSubscribedBlocking(addresses[0]); err != nil {
				t.Fatal(err)
			}
			b := &graphRecorder{mockBroadcaster: mockBroadcaster{peerIDs: []pb.NodeID{2}}}
			e.SetBroadcaster(b)
			e.addPeerSubscriber(addresses[0], 2)
			auditWrite(t, e, addresses[0], "value")
			if len(b.broadcasts) != 1 {
				t.Fatalf("expected one ordinary publication, got %d", len(b.broadcasts))
			}
			if len(b.historySent[2]) != 1 {
				t.Fatalf("published ordinary key but recorded history %v", b.historySent)
			}
		})
	}
}

func TestBoundaryAuditOrdinaryNoopRouting(t *testing.T) {
	for name, addresses := range auditAddresses() {
		t.Run(name, func(t *testing.T) {
			e := auditEngine(t)
			auditWrite(t, e, logicalAddress("main"), "ordinary scalar, not GRAPH")
			observed := e.index.ContainsInternal(logicalAddress("main")).Tips()
			c := e.NewContext()
			// Use the shared emit implementation solely to attach a cross-key dependency.
			// The value has no graph type, regions, predicates, or selected graph route.
			if err := c.emitEffect(addressedValue(addresses[0], "child"), observed, nil, []Tip{}); err != nil {
				t.Fatal(err)
			}
			if err := c.Flush(); err != nil {
				t.Fatal(err)
			}
			e.SetBroadcaster(&mockBroadcaster{peerIDs: []pb.NodeID{2}})
			e.addPeerSubscriber(addresses[0], 2)
			c = e.NewContext()
			defer c.Abort()
			if err := c.Emit(&pb.Effect{Key: []byte(wireKey(addresses[0])), IKey: []byte(addresses[0]), Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}}}); err != nil {
				t.Fatal(err)
			}
			if len(c.keys[addresses[0]].routes) != 0 {
				t.Error("ordinary NOOP invented graph predicate routing solely from internal address and cross-key dependency")
			}
			if got := c.collectSubscribers(); !slices.Equal(got, []pb.NodeID{2}) {
				t.Errorf("ordinary observation excludes its actual subscriber: %v", got)
			}
		})
	}
}

func TestBoundaryAuditSafetyPolicy(t *testing.T) {
	e := auditEngine(t)
	e.UpdateSafetyRules(SafeMode, []KeyRangeRule{{Pattern: "main", Mode: UnsafeMode}})
	if e.modeForKey(logicalAddress("main")) != UnsafeMode {
		t.Fatal("logical policy control failed")
	}
	if got := e.modeForKey(keytrie.InternalKey("main", "a")); got != SafeMode {
		t.Fatalf("distinct internal key inherited main key policy: %v", got)
	}
}

func TestBoundaryAuditMVCCAndValidation(t *testing.T) {
	for name, addresses := range auditAddresses() {
		t.Run(name, func(t *testing.T) {
			e := auditEngine(t)
			key := addresses[0]
			auditWrite(t, e, key, "old")
			tx := e.NewContext()
			tx.BeginTx()
			state, _, err := tx.GetSnapshotInternal(key)
			if err != nil || string(state.GetScalar().GetRaw()) != "old" {
				t.Fatalf("initial read %v %v", state, err)
			}
			auditWrite(t, e, key, "new")
			state, _, err = tx.GetSnapshotInternal(key)
			if err != nil || string(state.GetScalar().GetRaw()) != "old" {
				t.Fatalf("unstable snapshot %v %v", state, err)
			}
			if err := tx.Emit(addressedValue(key, "stale write")); err != nil {
				t.Fatal(err)
			}
			if err := tx.Flush(); !errors.Is(err, ErrTxnAborted) {
				t.Fatalf("stale write outcome=%v; want aborted", err)
			}
		})
	}
}

func TestBoundaryAuditCompactionValue(t *testing.T) {
	for name, addresses := range auditAddresses() {
		t.Run(name, func(t *testing.T) {
			e := auditEngine(t)
			key := addresses[0]
			for range 20 {
				auditWrite(t, e, key, "value")
			}
			c := e.NewContext()
			state, _, err := c.GetSnapshotInternal(key)
			if err != nil || string(state.GetScalar().GetRaw()) != "value" {
				t.Fatalf("pre-compaction %v %v", state, err)
			}
			if err := c.Flush(); err != nil {
				t.Fatal(err)
			}
			eff, ok := e.effectCache.Get(e.index.ContainsInternal(key).Tips()[0])
			if !ok || eff.GetSnapshot().GetState() == nil {
				t.Fatal("compaction did not run")
			}
			state, _, _, err = e.GetSnapshotInternal(key)
			if err != nil || string(state.GetScalar().GetRaw()) != "value" {
				t.Fatalf("post-compaction %v %v", state, err)
			}
		})
	}
}

func TestBoundaryAuditNackWireAlias(t *testing.T) {
	e := auditEngine(t)
	internal := keytrie.InternalKey("main", "a")
	application := logicalAddress(internal)
	bind := &pb.Effect{Key: []byte(internal), IKey: []byte(internal), Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{Keys: []*pb.TransactionalBindEffect_KeyBind{{Key: []byte(application)}, {Key: []byte(internal)}}}}}
	tip := Tip{uint64(e.nodeID), 123456}
	if err := e.effectCache.Put(tip, bind); err != nil {
		t.Fatal(err)
	}
	reply := e.buildEnrichedNack(application, toPbRef(tip), nil)
	if got := e.nackKey(reply); got != application {
		t.Fatalf("NACK for application key resolved to different internal key: got=%x want=%x", got, application)
	}
}

func TestBoundaryAuditAncestryKeepsAdvertisedOrigin(t *testing.T) {
	for name, addresses := range auditAddresses() {
		t.Run(name, func(t *testing.T) {
			e := auditEngine(t)
			e.SetBroadcaster(&mockBroadcaster{peerIDs: []pb.NodeID{2, 3, 4, 5}})
			for range 64 {
				peers := e.ancestryPeers(addresses[0], Tip{5, 1})
				if len(peers) != 2 || peers[0] != 5 {
					t.Fatalf("origin excluded from bounded fetch candidates: %v", peers)
				}
			}
		})
	}
}

func TestBoundaryAuditConflictIndexIdentity(t *testing.T) {
	e := auditEngine(t)
	internal := keytrie.InternalKey("main", "a")
	application := logicalAddress(internal)
	if conflictIndexAddress(internal) == conflictIndexAddress(application) {
		t.Fatal("conflict indexes alias")
	}
	c := e.NewContext()
	for _, address := range []string{internal, application} {
		if err := e.appendConflictValueInternal(c, conflictIndexAddress(address), []byte(address)); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
	for _, address := range []string{internal, application} {
		state, _, _, err := e.GetSnapshotInternal(conflictIndexAddress(address))
		if err != nil || len(state.GetOrderedElements()) != 1 || string(state.GetOrderedElements()[0].GetData().GetRaw()) != address {
			t.Fatalf("conflict index mixed keys: %v %v", state, err)
		}
	}
}

func TestBoundaryAuditBinaryApplicationSubscription(t *testing.T) {
	e := auditEngine(t)
	internal := keytrie.InternalKey("main", "a")
	application := logicalAddress(internal)
	for _, address := range []string{internal, application} {
		if err := e.SubscribeInternal(address); err != nil {
			t.Fatal(err)
		}
	}
	e.dropSubscription(internal)
	if _, ok := e.subscription(internal); ok {
		t.Fatal("internal subscription survived teardown")
	}
	if _, ok := e.subscription(application); !ok {
		t.Fatal("internal teardown removed binary application subscription")
	}
}
