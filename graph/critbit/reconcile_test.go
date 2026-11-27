package critbit

import (
	"errors"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/engine/graph"
	"github.com/swytchdb/engine/graph/critbit/meta"
	"github.com/swytchdb/engine/keytrie"
)

func TestReconcileLeavesSharedSubtreeUndiscovered(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		name := "forward"
		if reverse {
			name = "reverse"
		}

		t.Run(name, func(t *testing.T) {
			e := testEngine(t)
			createStored(t, e, graph.StringType)
			tree, c := openStored(t, e, graph.StringType)

			root, err := tree.getRoot(true)
			if err != nil {
				t.Fatal(err)
			}
			// These are the two paths produced by concurrent insertions of a
			// and z around a shared m/n subtree. That subtree is intentionally
			// unavailable: reconciling the wrappers must not try to discover it.
			shared := graph.Address(keytrie.InternalKey("test", branchName("m", 0, 0xfd)))
			pos, bits := tree.findCritBit("a", "m")

			a, err := tree.branch("a", pos, bits)
			if err != nil {
				t.Fatal(err)
			}

			pos, bits = tree.findCritBit("m", "z")

			z, err := tree.branch("z", pos, bits)
			if err != nil {
				t.Fatal(err)
			}

			for _, key := range []string{"a", "z"} {
				leaf := tree.CreateNode("leaf/"+key, meta.NewCritMeta().SetIsLeaf(true).SetKey(key))
				tree.replaceValue(leaf, graph.NewString(key))

				if key == "a" {
					tree.Link(a, leaf, graph.Left)
				} else {
					tree.Link(z, leaf, graph.Right)
				}
			}

			a.AttachRight(shared)
			z.AttachLeft(shared)

			wrappers := []*CritNode{a, z}
			if reverse {
				slices.Reverse(wrappers)
			}

			for _, node := range wrappers {
				tree.Link(root, node, graph.Left)
			}

			commit(t, tree, c)

			tree, c = openStored(t, e, graph.StringType)
			if err := tree.Insert("a", graph.NewString("updated")); err != nil {
				t.Fatalf("writer tried to discover shared subtree: %v", err)
			}

			cache := reflect.ValueOf(tree.Graph).Elem().FieldByName("pending")
			if cache.MapIndex(reflect.ValueOf(string(shared))).IsValid() {
				t.Fatal("shared subtree was loaded")
			}

			commit(t, tree, c)
			tree, _ = openStored(t, e, graph.StringType)
			requireValue(t, tree, "a", "updated")
			requireValue(t, tree, "z", "z")

			if _, err := tree.Scan("m", "o", 0); !errors.Is(err, ErrIncomplete) {
				t.Fatalf("shared subtree should be discovered only on traversal: %v", err)
			}
		})
	}
}

func TestReconcileDisjointSubtreesWithoutDiscoveringChildren(t *testing.T) {
	for _, keys := range [][4]string{
		{"ba", "bc", "za", "zc"},
		{"\x60\x00", "\x62\x00", "\x70\x00", "\x72\x00"},
		{"a", "a\x00", "b", "b\x00"},
	} {
		t.Run(keys[0]+"/"+keys[2], func(t *testing.T) {
			e := testEngine(t)
			createStored(t, e, graph.StringType)
			tree, c := openStored(t, e, graph.StringType)

			root, err := tree.getRoot(true)
			if err != nil {
				t.Fatal(err)
			}

			var (
				oldRoots     []string
				undiscovered []string
			)

			for i := 0; i < len(keys); i += 2 {
				pos, bits := tree.findCritBit(keys[i], keys[i+1])

				node, err := tree.branch(keys[i], pos, bits)
				if err != nil {
					t.Fatal(err)
				}

				left := keytrie.InternalKey("test", "leaf/"+keys[i])
				right := keytrie.InternalKey("test", "leaf/"+keys[i+1])
				node.AttachLeft(graph.Address(left)).AttachRight(graph.Address(right))
				undiscovered = append(undiscovered, left, right)

				tree.Link(root, node, graph.Left)
				oldRoots = append(oldRoots, string(node.Address()))
			}

			commit(t, tree, c)
			tree, c = openStored(t, e, graph.StringType)

			root, err = tree.getRoot(false)
			if err != nil {
				t.Fatal(err)
			}

			merged, err := tree.reconcile(root, graph.Left, tree.addresses(root, graph.Left))
			if err != nil {
				t.Fatalf("merge opened disjoint subtree contents: %v", err)
			}

			if got := tree.addresses(merged, graph.Left); !slices.Equal(got, oldRoots[:1]) {
				t.Fatalf("left subtree identity changed: %q", got)
			}

			if got := tree.addresses(merged, graph.Right); !slices.Equal(got, oldRoots[1:]) {
				t.Fatalf("right subtree identity changed: %q", got)
			}

			cache := reflect.ValueOf(tree.Graph).Elem().FieldByName("pending")
			for _, address := range undiscovered {
				if cache.MapIndex(reflect.ValueOf(address)).IsValid() {
					t.Fatalf("loaded descendant %q", address)
				}
			}

			commit(t, tree, c)
		})
	}
}

func TestReconcileDefersConflictsInsideSharedNode(t *testing.T) {
	e := testEngine(t)
	createStored(t, e, graph.StringType)

	tree, c := openStored(t, e, graph.StringType)
	if err := tree.Insert("a", graph.NewString("a")); err != nil {
		t.Fatal(err)
	}

	commit(t, tree, c)
	aa, ca := openStored(t, e, graph.StringType)

	ab, cb := openStored(t, e, graph.StringType)
	if err := aa.Insert("aa", graph.NewString("aa")); err != nil {
		t.Fatal(err)
	}

	if err := ab.Insert("ab", graph.NewString("ab")); err != nil {
		t.Fatal(err)
	}

	commit(t, aa, ca)
	commit(t, ab, cb)

	sharedAddress := keytrie.InternalKey("test", branchName("a", 1, 0))
	// Both competing root slots reference the same presence-test branch.
	// Deleting its Left leaf must not rewrite its competing Right slots.
	var changedRight atomic.Bool

	e.SetOnLocalEffect(func(_ effects.Tip, effect *pb.Effect) {
		if effect.GetData() != nil && string(effect.GetKey()) == sharedAddress && string(effect.GetVirtual()) == "right" {
			changedRight.Store(true)
		}
	})
	defer e.SetOnLocalEffect(nil)

	tree, c = openStored(t, e, graph.StringType)
	if deleted, err := tree.Delete("a"); err != nil || !deleted {
		t.Fatalf("delete: %t, %v", deleted, err)
	}

	commit(t, tree, c)

	if changedRight.Load() {
		t.Fatal("outer reconciliation rewrote the unvisited child list")
	}

	e.SetOnLocalEffect(nil)
	tree, c = openStored(t, e, graph.StringType)

	shared, err := tree.GetNode(sharedAddress, c, meta.CritMetaType)
	if err != nil {
		t.Fatal(err)
	}

	if got := tree.addresses(shared, graph.Right); len(got) != 2 {
		t.Fatalf("independent child conflict should remain: %q", got)
	}

	if deleted, err := tree.Delete("aa\x00"); err != nil || deleted {
		t.Fatalf("visit Right subtree: %t, %v", deleted, err)
	}

	if got := tree.addresses(shared, graph.Right); len(got) != 1 {
		t.Fatalf("writer did not reconcile the visited child: %q", got)
	}

	commit(t, tree, c)
	tree, _ = openStored(t, e, graph.StringType)
	requireValue(t, tree, "aa", "aa")
	requireValue(t, tree, "ab", "ab")
}

func TestStructuralMergePreservesCompetingPaths(t *testing.T) {
	groups := [][][]string{
		{{"ba", "bc"}, {"aa", "az"}, {"za", "zc"}},
		{{"", "a", "ab"}, {"", "a\x00", "b"}, {"\x00", "\x80", "\xff"}},
		{{"a", "aa", "ab"}, {"a", "aa\x00", "ab\x00"}, {"a", "aa\x00b", "ab\x00b"}},
		{{"\x60", "\x62", "\x80"}, {"\x50", "\x52", "\x80"}, {"\x70", "\x72", "\x80"}},
	}
	for i, group := range groups {
		for _, reverse := range []bool{false, true} {
			name := "forward"
			if reverse {
				name = "reverse"
			}

			t.Run(string(rune('A'+i))+"/"+name, func(t *testing.T) {
				e := testEngine(t)
				createStored(t, e, graph.StringType)

				var (
					writers  []*Crit[*graph.String]
					contexts []*effects.Context
					keys     []string
				)

				for _, source := range group {
					tree, c := openStored(t, e, graph.StringType)
					for _, key := range source {
						if err := tree.Insert(key, graph.NewString(key)); err != nil {
							t.Fatal(err)
						}

						keys = append(keys, key)
					}

					writers = append(writers, tree)
					contexts = append(contexts, c)
				}

				if reverse {
					slices.Reverse(writers)
					slices.Reverse(contexts)
				}

				for j := range writers {
					commit(t, writers[j], contexts[j])
				}

				slices.Sort(keys)
				keys = slices.Compact(keys)
				tree, c := openStored(t, e, graph.StringType)

				root, err := tree.getRoot(false)
				if err != nil {
					t.Fatal(err)
				}

				for _, key := range keys {
					leaf, err := tree.findBestMatch(root, key, true)
					if err != nil || leaf == nil || leaf.Value().Key() != key {
						t.Fatalf("merge lost %q: %v, %v", key, leaf, err)
					}
				}

				commit(t, tree, c)
				tree, _ = openStored(t, e, graph.StringType)

				entries, err := tree.Scan("", "", 0)
				if err != nil || len(entries) != len(keys) {
					t.Fatalf("merged scan: %v, %v", entries, err)
				}

				for j, entry := range entries {
					if entry.Key != keys[j] || entry.Value.Value() != keys[j] {
						t.Fatalf("entry %d = %q/%q, want %q", j, entry.Key, entry.Value.Value(), keys[j])
					}
				}
			})
		}
	}
}
