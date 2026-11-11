package effects

import (
	"errors"
	"log/slog"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
)

// Count actual expansion and dependency enumeration, not fetches: the old
// publication loop reused resident payloads but walked them again on every CAS.
func countClosureWork(op *closureOperation) (map[Tip]int, map[[2]Tip]int) {
	vertices, edges := make(map[Tip]int), make(map[[2]Tip]int)
	op.observe = func(tip Tip, ref *pb.EffectRef) {
		if ref == nil {
			vertices[tip]++
		} else {
			edges[[2]Tip{tip, r(ref)}]++
		}
	}

	return vertices, edges
}

func TestClosurePublicationCrossKeyErrorSurvivesCASRetry(t *testing.T) {
	e := newSnapshotEngine(nil)
	defer func() { _ = e.Close() }()

	preSubscribe(e, "k", "other")

	base, bind, missing := Tip{7, 1}, Tip{8, 1}, Tip{9, 1}
	putCutEffects(t, e, map[Tip]*pb.Effect{
		base: cutEffect(base),
		bind: {
			Key: []byte("k"), ForkChoiceHash: []byte{8},
			Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
				Keys: []*pb.TransactionalBindEffect_KeyBind{
					{Key: []byte("k"), NewTip: toPbRef(base)},
					{Key: []byte("other"), NewTip: toPbRef(missing)},
				},
			}},
		},
	})
	e.installTips("k", []Tip{base})

	failure := errors.New("peer unavailable")
	loads := 0
	e.broadcaster = &cutBroadcaster{load: func(tip Tip) ([]byte, error) {
		loads++

		if tip != missing {
			t.Fatalf("unexpected fetch %v", tip)
		}

		return nil, failure
	}}

	op := newClosureOperation(e, make(map[Tip]error))
	defer op.release()

	attempts := 0
	op.cas = func(key string, old, next *keytrie.TipSet) (*keytrie.TipSet, bool) {
		attempts++
		if attempts <= 3 {
			if _, ok := e.index.Insert(key, old, keytrie.NewTipSet(old.Tips()...)); !ok {
				t.Fatal("could not force competing publication")
			}
		}

		return e.index.Insert(key, old, next)
	}

	_, complete, err := op.publish("k", []Tip{bind}, false)
	if complete || !errors.Is(err, failure) {
		t.Fatalf("cross-key transport failure lost across retry: complete=%t err=%v", complete, err)
	}

	if attempts != 4 || loads != 1 {
		t.Fatalf("attempts=%d loads=%d, want 4 and 1", attempts, loads)
	}

	requireFrontier(t, e, bind)

	if e.index.Contains("other").Len() != 0 {
		t.Fatal("cross-key discovery published another key's frontier")
	}
}

func TestClosurePublicationRetryResumesUnresolvedBoundary(t *testing.T) {
	e := newSnapshotEngine(nil)
	defer func() { _ = e.Close() }()

	base, incoming, missing := Tip{7, 1}, Tip{8, 1}, Tip{9, 1}
	records := map[Tip]*pb.Effect{
		base: cutEffect(base), incoming: cutEffect(incoming, missing), missing: cutEffect(missing, base),
	}
	putCutEffects(t, e, map[Tip]*pb.Effect{base: records[base], incoming: records[incoming]})
	e.installTips("k", []Tip{base})

	loads := 0
	e.broadcaster = &cutBroadcaster{load: func(tip Tip) ([]byte, error) {
		loads++

		if tip != missing {
			t.Fatalf("unexpected fetch %v", tip)
		}

		return nil, errors.New("peer unavailable")
	}}

	op := newClosureOperation(e, make(map[Tip]error))
	defer op.release()

	vertices, edges := countClosureWork(op)
	attempts := 0

	op.cas = func(key string, old, next *keytrie.TipSet) (*keytrie.TipSet, bool) {
		attempts++
		if attempts == 1 {
			putCutEffects(t, e, map[Tip]*pb.Effect{missing: records[missing]})

			if _, ok := e.index.Insert(key, old, keytrie.NewTipSet(old.Tips()...)); !ok {
				t.Fatal("could not force competing publication")
			}
		}

		return e.index.Insert(key, old, next)
	}
	if _, complete, err := op.publish("k", []Tip{incoming}, false); !complete || err != nil {
		t.Fatalf("available boundary was not resumed: complete=%t err=%v", complete, err)
	}

	if attempts != 2 || loads != 1 {
		t.Fatalf("attempts=%d loads=%d, want 2 and 1", attempts, loads)
	}

	requireFrontier(t, e, incoming)
	requireClosureWorkOnce(t, records, vertices, edges)
}

func requireClosureWorkOnce(t *testing.T, records map[Tip]*pb.Effect, vertices map[Tip]int, edges map[[2]Tip]int) {
	t.Helper()

	for tip, eff := range records {
		if got := vertices[tip]; got != 1 {
			t.Errorf("vertex %v expanded %d times, want 1", tip, got)
		}

		for ref := range keyDeps("k", eff) {
			if got := edges[[2]Tip{tip, r(ref)}]; got != 1 {
				t.Errorf("edge %v -> %v traversed %d times, want 1", tip, r(ref), got)
			}
		}
	}
}

func TestClosurePublicationCASRetriesReuseDiscovery(t *testing.T) {
	e := newSnapshotEngine(nil)
	defer func() { _ = e.Close() }()

	base, middle, incoming := Tip{7, 1}, Tip{7, 2}, Tip{8, 1}
	records := map[Tip]*pb.Effect{
		base: cutEffect(base), middle: cutEffect(middle, base), incoming: cutEffect(incoming, middle),
	}
	putCutEffects(t, e, records)
	e.installTips("k", []Tip{base})

	op := newClosureOperation(e, make(map[Tip]error))
	defer op.release()

	vertices, edges := countClosureWork(op)

	const failures = 8

	attempts := 0

	op.cas = func(key string, old, next *keytrie.TipSet) (*keytrie.TipSet, bool) {
		attempts++
		if attempts <= failures {
			// Replace the pointer without changing causal history. Force the
			// actual CAS to fail after closure, without scheduler timing.
			if _, ok := e.index.Insert(key, old, keytrie.NewTipSet(old.Tips()...)); !ok {
				t.Fatal("could not force competing publication")
			}
		}

		return e.index.Insert(key, old, next)
	}
	if _, complete, err := op.publish("k", []Tip{incoming}, false); err != nil || !complete {
		t.Fatalf("publication: complete=%t err=%v", complete, err)
	}

	if attempts != failures+1 {
		t.Fatalf("CAS attempts = %d, want %d", attempts, failures+1)
	}

	requireFrontier(t, e, incoming)
	requireClosureWorkOnce(t, records, vertices, edges)
}

func TestClosurePublicationConcurrentTipExtendsCuts(t *testing.T) {
	for _, consumesIncoming := range []bool{false, true} {
		name := "concurrent sibling"
		if consumesIncoming {
			name = "consumes discovered tip"
		}

		t.Run(name, func(t *testing.T) {
			e := newSnapshotEngine(nil)
			defer func() { _ = e.Close() }()

			base, left, incoming, right, concurrent := Tip{7, 1}, Tip{8, 1}, Tip{8, 2}, Tip{9, 1}, Tip{9, 2}

			rightDep := base
			if consumesIncoming {
				rightDep = incoming
			}

			records := map[Tip]*pb.Effect{
				base: cutEffect(base), left: cutEffect(left, base), incoming: cutEffect(incoming, left),
				right: cutEffect(right, rightDep), concurrent: cutEffect(concurrent, right),
			}
			putCutEffects(t, e, records)
			e.installTips("k", []Tip{base})

			op := newClosureOperation(e, make(map[Tip]error))
			defer op.release()

			vertices, edges := countClosureWork(op)
			attempts := 0

			op.cas = func(key string, old, next *keytrie.TipSet) (*keytrie.TipSet, bool) {
				attempts++
				if attempts == 1 {
					// This local publication consumes the former indexed tip.
					// Its branch was not supplied to the initial closure walk.
					e.updateIndex(key, old, concurrent)
				}

				return e.index.Insert(key, old, next)
			}
			if _, complete, err := op.publish("k", []Tip{incoming}, false); err != nil || !complete {
				t.Fatalf("publication: complete=%t err=%v", complete, err)
			}

			if consumesIncoming {
				requireFrontier(t, e, concurrent)
			} else {
				requireFrontier(t, e, incoming, concurrent)
			}

			wantAttempts := 2
			if consumesIncoming {
				// The competing publication already installed the final set.
				wantAttempts = 1
			}

			if attempts != wantAttempts {
				t.Fatalf("CAS attempts = %d, want %d", attempts, wantAttempts)
			}

			requireClosureWorkOnce(t, records, vertices, edges)
		})
	}
}

// installTips merges a whole tip frontier into the key's index in one CAS
// transition, so a concurrent reader observes either the prior frontier or the
// full merged one — never a partial install. Same accounting as updateIndex:
// Insert fires the trie's refDelta hook for the diff, and NewTipSet dedupes
// tips the index already holds.
//
//nolint:unparam // Keep the fixture helper usable for any key; current callers use "k".
func (e *Engine) installTips(key string, tips []Tip) {
	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			slog.Debug("installTips: CAS retry", "key", key, "attempt", attempt)
		}

		current := e.index.Contains(key)
		cur := current.Tips()
		offsets := make([]Tip, 0, len(cur)+len(tips))
		offsets = append(offsets, cur...)
		offsets = append(offsets, tips...)

		newTips := keytrie.NewTipSet(offsets...)
		if _, ok := e.index.Insert(key, current, newTips); ok {
			if current.Len() != newTips.Len() || !current.ContainsAll(newTips.Tips()) {
				e.notifyKeyChanged(key)
			}

			return
		}

		if e.index.Closed() {
			// Not a CAS race: Insert fails unconditionally on a closed trie,
			// so retrying would spin forever during shutdown.
			return
		}
	}
}
