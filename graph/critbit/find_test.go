package critbit

import (
	"errors"
	"reflect"
	"sync/atomic"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/engine/graph"
	"github.com/swytchdb/engine/graph/critbit/meta"
)

func TestFindAndExactScanDoNotDiscoverRouting(t *testing.T) {
	e := testEngine(t)
	createStored(t, e, graph.StringType)

	tree, c := openStored(t, e, graph.StringType)
	for _, key := range []string{"", "a\x00\xff", "deleted"} {
		if err := tree.Insert(key, graph.NewString(key)); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := tree.Delete("deleted"); err != nil {
		t.Fatal(err)
	}

	root, err := tree.getRoot(false)
	if err != nil {
		t.Fatal(err)
	}

	root.AttachLeft(graph.Address("unavailable-routing"))
	commit(t, tree, c)

	var emitted atomic.Int64

	e.SetOnLocalEffect(func(_ effects.Tip, effect *pb.Effect) {
		if effect.GetData() != nil || effect.GetMeta() != nil {
			emitted.Add(1)
		}
	})
	defer e.SetOnLocalEffect(nil)

	for _, key := range []string{"", "a\x00\xff", "deleted", "missing"} {
		t.Run(key, func(t *testing.T) {
			tree, c := openStored(t, e, graph.StringType)

			value, err := tree.Find(key)
			if err != nil {
				t.Fatal(err)
			}

			wantNodes := 1

			if key == "deleted" || key == "missing" {
				if value != nil {
					t.Fatal("missing/deleted key returned a value")
				}
			} else {
				wantNodes = 2 // the leaf and its payload

				if value == nil || value.Value() != key {
					t.Fatalf("Find(%q) = %v", key, value)
				}
			}

			cache := reflect.ValueOf(tree.Graph).Elem().FieldByName("pending")
			if tree.root != nil || cache.Len() != wantNodes {
				t.Fatalf("Find discovered routing: %d cached nodes, want %d", cache.Len(), wantNodes)
			}

			commit(t, tree, c)

			if wantNodes == 2 {
				tree, c = openStored(t, e, graph.StringType)

				entries, err := tree.Scan(key, "", 1)
				if err != nil || len(entries) != 1 || entries[0].Key != key || entries[0].Value.Value() != key {
					t.Fatalf("exact scan: %v, %v", entries, err)
				}

				cache = reflect.ValueOf(tree.Graph).Elem().FieldByName("pending")
				if tree.root != nil || cache.Len() != 2 {
					t.Fatal("exact scan discovered routing")
				}

				commit(t, tree, c)
			}
		})
	}

	if emitted.Load() != 0 {
		t.Fatal("direct reads emitted mutations")
	}
	// Successors still require routing discovery; failed scans return no partial result.
	tree, _ = openStored(t, e, graph.StringType)

	entries, err := tree.Scan("a\x00\xff", "", 2)
	if !errors.Is(err, ErrIncomplete) || entries != nil {
		t.Fatalf("continuation: %v, %v", entries, err)
	}
}

func TestDirectReadsAcrossCompetingRootPaths(t *testing.T) {
	e := testEngine(t)
	createStored(t, e, graph.StringType)
	a, ca := openStored(t, e, graph.StringType)

	z, cz := openStored(t, e, graph.StringType)
	if err := a.Insert("a", graph.NewString("a")); err != nil {
		t.Fatal(err)
	}

	if err := z.Insert("z", graph.NewString("z")); err != nil {
		t.Fatal(err)
	}

	commit(t, a, ca)
	commit(t, z, cz)
	tree, _ := openStored(t, e, graph.StringType)

	root, err := tree.getRoot(false)
	if err != nil {
		t.Fatal(err)
	}

	visible, err := tree.child(root, graph.Left, false)
	if err != nil {
		t.Fatal(err)
	}

	hidden := "a"
	if visible.Value().Key() == hidden {
		hidden = "z"
	}

	tree, _ = openStored(t, e, graph.StringType)
	requireValue(t, tree, "a", "a")
	requireValue(t, tree, "z", "z")

	if tree.root != nil {
		t.Fatal("point lookups traversed competing roots")
	}

	for _, limit := range []int{1, 2, 0} {
		tree, c := openStored(t, e, graph.StringType)

		entries, err := tree.Scan(hidden, "", limit)
		if err != nil || len(entries) == 0 || entries[0].Key != hidden || entries[0].Value.Value() != hidden {
			t.Fatalf("limit %d omitted the exact lower key: %v, %v", limit, entries, err)
		}

		for i := 1; i < len(entries); i++ {
			if entries[i-1].Key >= entries[i].Key {
				t.Fatal("scan duplicated the exact key or lost ordering")
			}
		}

		root, err := tree.getRoot(false)
		if err != nil || len(tree.addresses(root, graph.Left)) != 2 {
			t.Fatalf("scan reconciled the roots: %v", err)
		}

		commit(t, tree, c)
	}
}

func TestDirectScanFallsBackForMissingAndDeletedLower(t *testing.T) {
	tree := Create("test", graph.StringType)
	for _, key := range []string{"a", "b", "c"} {
		if err := tree.Insert(key, graph.NewString(key)); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := tree.Delete("b"); err != nil {
		t.Fatal(err)
	}

	for lower, want := range map[string]string{"": "a", "aa": "c", "b": "c"} {
		entries, err := tree.Scan(lower, "", 1)
		if err != nil || len(entries) != 1 || entries[0].Key != want {
			t.Fatalf("Scan(%q): %v, %v; want %q", lower, entries, err, want)
		}
	}

	if entries, err := tree.Scan("b", "c", 1); err != nil || len(entries) != 0 {
		t.Fatalf("exclusive upper: %v, %v", entries, err)
	}
}

func TestFindRejectsMalformedDeterministicLeaf(t *testing.T) {
	for _, value := range []*meta.CritMeta{
		meta.NewCritMeta().SetIsLeaf(false).SetKey("key"),
		meta.NewCritMeta().SetIsLeaf(true).SetKey("different"),
	} {
		tree := Create("test", graph.StringType)
		tree.CreateNode("leaf/key", value)

		if got, err := tree.Find("key"); !errors.Is(err, ErrIncomplete) || got != nil {
			t.Fatalf("malformed leaf: %v, %v", got, err)
		}

		if entries, err := tree.Scan("key", "", 1); !errors.Is(err, ErrIncomplete) || entries != nil {
			t.Fatalf("malformed exact lower: %v, %v", entries, err)
		}
	}
}
