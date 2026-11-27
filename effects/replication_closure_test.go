package effects

import (
	"fmt"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
)

func replicationClosureFixture(tb testing.TB, count int) (*Engine, []string, Tip, *pb.OffsetNotify) {
	tb.Helper()

	keys := make([]string, count)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%04d", i)
	}

	e, binds, records := backfillHistory(1, keys)

	tb.Cleanup(func() { _ = e.Close() })

	bind := binds[0]
	eff := records[bind]

	wire, err := MarshalEffect(eff)
	if err != nil {
		tb.Fatal(err)
	}

	return e, keys, bind, BuildOffsetNotify(pb.NodeID(bind[0]), bind, eff, wire, nil)
}

func TestClosureEnumeratesSharedBindOnce(t *testing.T) {
	for _, count := range []int{16, 128} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			e, keys, bind, _ := replicationClosureFixture(t, count)

			op := newClosureOperation(e, make(map[Tip]error))
			defer op.release()

			vertices, edges := countClosureWork(op)
			retried := false
			op.cas = func(key string, old, next *keytrie.TipSet) (*keytrie.TipSet, bool) {
				if !retried {
					retried = true

					if _, ok := e.index.InsertInternal(key, old, keytrie.NewTipSet(old.Tips()...)); !ok {
						t.Fatal("could not force CAS retry")
					}
				}

				return e.index.InsertInternal(key, old, next)
			}

			scans := 0
			op.observeBind = func(tip Tip) {
				if tip != bind {
					t.Fatalf("unexpected bind %v", tip)
				}

				scans++
			}

			frontiers := make(ingestedFrontiers)
			for _, key := range keys {
				frontiers.add(logicalAddress(key), bind)
			}

			op.publishBatch(frontiers)
			// Re-publication must reuse the same discovery, including bind-wide
			// enumeration, while preserving every key's separate dependency.
			op.publishBatch(frontiers)

			if scans != 1 || vertices[bind] != count || len(edges) != count {
				t.Fatalf("scans=%d bind projections=%d edges=%d; want 1, %d, %d", scans, vertices[bind], len(edges), count, count)
			}

			for edge, visits := range edges {
				if visits != 1 {
					t.Errorf("edge %v visited %d times", edge, visits)
				}
			}

			for _, key := range keys {
				requireCutTips(t, e.index.Contains(key).Tips(), []Tip{bind})
			}
		})
	}
}

func TestRemoteBindClosureScalesWithKeys(t *testing.T) {
	var previous uint64

	for _, count := range []int{16, 64} {
		e, keys, bind, notice := replicationClosureFixture(t, count)
		before, _, _ := e.effectCache.Stats()
		nacks, err := e.HandleRemote(notice)
		after, _, _ := e.effectCache.Stats()

		if err != nil || len(nacks) != 0 {
			t.Fatalf("remote bind: nacks=%v err=%v", nacks, err)
		}

		for _, key := range keys {
			requireCutTips(t, e.index.Contains(key).Tips(), []Tip{bind})
		}

		reads := after - before
		t.Logf("keys=%d pool reads=%d", count, reads)
		// Four times the keys permits linear overhead, but not a fresh
		// all-key closure for every published projection (quadratic reads).
		if previous != 0 && reads > 5*previous {
			t.Fatalf("4x keys caused %d -> %d pool reads", previous, reads)
		}

		previous = reads
	}
}

func BenchmarkRemoteBindClosure(b *testing.B) {
	for _, count := range []int{16, 64, 256} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			e, _, _, notice := replicationClosureFixture(b, count)
			b.ReportAllocs()

			for b.Loop() {
				if nacks, err := e.HandleRemote(notice); err != nil || len(nacks) != 0 {
					b.Fatalf("remote bind: nacks=%v err=%v", nacks, err)
				}
			}
		})
	}
}
