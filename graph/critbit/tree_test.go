package critbit

import (
	"fmt"
	"math/rand"
	"slices"
	"sync/atomic"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/engine/graph/critbit/meta"

	"github.com/swytchdb/engine/graph"
)

func TestInsert(t *testing.T) {
	tree := Create("test", graph.StringType)
	if err := tree.Insert("hello", graph.NewString("world")); err != nil {
		t.Fatal(err)
	}

	got, err := tree.Find("hello")
	if err != nil {
		t.Fatal(err)
	}

	value := got
	if value == nil {
		t.Fatal("Find returned nil")
	}

	if got := value.Value(); got != "world" {
		t.Fatalf("Find returned %q, want %q", got, "world")
	}
}

func TestBinaryKeysReplaceDelete(t *testing.T) {
	keys := []string{"", "a", "a\x00", "a\x00\x00", "a\x00b", "ab", "abc", "b", "\x00", "\x00\x00", "\x80", "\xff", "\xff\x00"}

	random := rand.New(rand.NewSource(42))
	for range 80 {
		raw := make([]byte, 1+random.Intn(6))
		random.Read(raw)
		keys = append(keys, string(raw))
	}

	for _, order := range []string{"forward", "reverse", "shuffled"} {
		t.Run(order, func(t *testing.T) {
			sequence := slices.Clone(keys)
			if order == "reverse" {
				slices.Reverse(sequence)
			}

			if order == "shuffled" {
				random.Shuffle(len(sequence), func(i, j int) { sequence[i], sequence[j] = sequence[j], sequence[i] })
			}

			tree := Create("test", graph.StringType)
			if v, err := tree.Find("absent"); err != nil || v != nil {
				t.Fatalf("empty lookup: %v %v", v, err)
			}

			want := make(map[string]string)

			for i, key := range sequence {
				value := fmt.Sprint(i)
				if err := tree.Insert(key, graph.NewString(value)); err != nil {
					t.Fatal(err)
				}

				want[key] = value
				for key, expected := range want {
					got, err := tree.Find(key)
					if err != nil || got == nil || got.Value() != expected {
						t.Fatalf("key %q: got %v, err %v; want %q", key, got, err, expected)
					}
				}
			}

			if err := tree.Insert("a", graph.NewString("replacement")); err != nil {
				t.Fatal(err)
			}

			if got, err := tree.Find("a"); err != nil || got.Value() != "replacement" {
				t.Fatalf("replacement: %v", err)
			}

			for key := range want {
				deleted, err := tree.Delete(key)
				if err != nil || !deleted {
					t.Fatalf("delete %q: %t %v", key, deleted, err)
				}

				got, err := tree.Find(key)
				if err != nil || got != nil {
					t.Fatalf("deleted key %q still exists: %v", key, err)
				}

				deleted, err = tree.Delete(key)
				if err != nil || deleted {
					t.Fatalf("repeat delete %q: %t %v", key, deleted, err)
				}
			}

			if err := tree.Insert("a\x00", graph.NewString("recreated")); err != nil {
				t.Fatal(err)
			}

			got, err := tree.Find("a\x00")
			if err != nil || got.Value() != "recreated" {
				t.Fatalf("reuse deleted leaf: %v", err)
			}
		})
	}
}

func testEngine(t *testing.T) *effects.Engine {
	t.Helper()

	e := effects.NewTestEngine()

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})

	return e
}

func testContext(t *testing.T, e *effects.Engine) *effects.Context {
	t.Helper()

	c := e.NewContext()
	t.Cleanup(c.Abort)

	return c
}

func createStored[T graph.Value](t *testing.T, e *effects.Engine, typ graph.Type[T]) {
	t.Helper()

	c := testContext(t, e)
	if err := Create("test", typ).Flush(c); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
}

func openStored[T graph.Value](t *testing.T, e *effects.Engine, typ graph.Type[T]) (*Crit[T], *effects.Context) {
	t.Helper()
	c := testContext(t, e)

	tree, err := Open("test", c, typ)
	if err != nil {
		t.Fatal(err)
	}

	return tree, c
}

func commit[T graph.Value](t *testing.T, tree *Crit[T], c *effects.Context) {
	t.Helper()

	if err := tree.Flush(c); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
}

func requireValue(t *testing.T, tree *Crit[*graph.String], key, expected string) {
	t.Helper()

	got, err := tree.Find(key)
	if err != nil || got == nil || got.Value() != expected {
		t.Fatalf("Find(%q): got %v, err %v; want %q", key, got, err, expected)
	}
}

func TestCommittedRoundTrip(t *testing.T) {
	e := testEngine(t)
	createStored(t, e, graph.StringType)

	keys := []string{"m", "a", "z", "", "a\x00", "a\x00\x00", "\x00", "\xff"}
	for i, key := range keys {
		tree, c := openStored(t, e, graph.StringType)
		if err := tree.Insert(key, graph.NewString(key+"!")); err != nil {
			t.Fatal(err)
		}

		commit(t, tree, c)

		tree, _ = openStored(t, e, graph.StringType)
		for _, key := range keys[:i+1] {
			requireValue(t, tree, key, key+"!")
		}
	}

	tree, c := openStored(t, e, graph.StringType)
	// A write through the retained payload participates without another Insert.
	got, err := tree.Find("a")
	if err != nil {
		t.Fatal(err)
	}

	got.Append(" updated")

	if deleted, err := tree.Delete("m"); err != nil || !deleted {
		t.Fatalf("delete: %t %v", deleted, err)
	}

	commit(t, tree, c)
	tree, c = openStored(t, e, graph.StringType)
	requireValue(t, tree, "a", "a! updated")

	if got, err := tree.Find("m"); err != nil || got != nil {
		t.Fatalf("deleted lookup: %v %v", got, err)
	}

	var emitted atomic.Int64

	e.SetOnLocalEffect(func(_ effects.Tip, effect *pb.Effect) {
		// Engine snapshot materialization is not a graph mutation.
		if effect.GetData() != nil || effect.GetMeta() != nil {
			emitted.Add(1)
		}
	})
	defer e.SetOnLocalEffect(nil)

	for _, key := range []string{"a", "z", "absent", "m"} {
		if _, err := tree.Find(key); err != nil {
			t.Fatal(err)
		}
	}

	commit(t, tree, c)

	if emitted.Load() != 0 {
		t.Fatal("reading emitted effects")
	}
}

func TestConcurrentPayloadSlotsAndWriterCleanup(t *testing.T) {
	e := testEngine(t)
	createStored(t, e, graph.StringType)

	tree, c := openStored(t, e, graph.StringType)
	if err := tree.Insert("key", graph.NewString("old")); err != nil {
		t.Fatal(err)
	}

	commit(t, tree, c)
	a, ca := openStored(t, e, graph.StringType)

	b, cb := openStored(t, e, graph.StringType)
	if err := a.Insert("key", graph.NewString("a")); err != nil {
		t.Fatal(err)
	}

	if err := b.Insert("key", graph.NewString("b")); err != nil {
		t.Fatal(err)
	}

	commit(t, a, ca)
	commit(t, b, cb)
	tree, c = openStored(t, e, graph.StringType)

	root, err := tree.getRoot(false)
	if err != nil {
		t.Fatal(err)
	}

	leaf, err := tree.findBestMatch(root, "key", false)
	if err != nil {
		t.Fatal(err)
	}

	slots := tree.addresses(leaf, graph.Down)
	if len(slots) != 2 {
		t.Fatalf("concurrent payload slots = %v", slots)
	}

	values := make([]string, 0, len(slots))

	for _, address := range slots {
		node, err := tree.GetNode(address, c, graph.StringType)
		if err != nil {
			t.Fatal(err)
		}

		values = append(values, node.Value().Value())
	}

	if !slices.Contains(values, "a") || !slices.Contains(values, "b") {
		t.Fatalf("lost competing values: %v", values)
	}

	var emitted atomic.Int64

	e.SetOnLocalEffect(func(_ effects.Tip, effect *pb.Effect) {
		// Engine snapshot materialization is not a graph mutation.
		if effect.GetData() != nil || effect.GetMeta() != nil {
			emitted.Add(1)
		}
	})
	requireValue(t, tree, "key", values[len(values)-1])
	commit(t, tree, c)
	e.SetOnLocalEffect(nil)

	if emitted.Load() != 0 {
		t.Fatal("reader reconciled competing values")
	}

	tree, c = openStored(t, e, graph.StringType)
	if err := tree.Insert("key", graph.NewString("resolved")); err != nil {
		t.Fatal(err)
	}

	commit(t, tree, c)
	tree, _ = openStored(t, e, graph.StringType)
	requireValue(t, tree, "key", "resolved")

	root, err = tree.getRoot(false)
	if err != nil {
		t.Fatal(err)
	}

	leaf, err = tree.findBestMatch(root, "key", false)
	if err != nil || len(tree.addresses(leaf, graph.Down)) != 1 {
		t.Fatalf("writer did not clean observed slots: %v", err)
	}
}

func TestConcurrentRoutingReconciliation(t *testing.T) {
	for _, initial := range []bool{false, true} {
		t.Run(fmt.Sprintf("existing=%t", initial), func(t *testing.T) {
			e := testEngine(t)
			createStored(t, e, graph.StringType)

			if initial {
				tree, c := openStored(t, e, graph.StringType)
				if err := tree.Insert("m", graph.NewString("m")); err != nil {
					t.Fatal(err)
				}

				commit(t, tree, c)
			}

			a, ca := openStored(t, e, graph.StringType)

			b, cb := openStored(t, e, graph.StringType)
			if err := a.Insert("a", graph.NewString("a")); err != nil {
				t.Fatal(err)
			}

			if err := b.Insert("z", graph.NewString("z")); err != nil {
				t.Fatal(err)
			}

			commit(t, a, ca)
			commit(t, b, cb)
			tree, c := openStored(t, e, graph.StringType)

			root, err := tree.getRoot(false)
			if err != nil {
				t.Fatal(err)
			}

			if len(tree.addresses(root, graph.Left)) != 2 {
				t.Fatal("competing paths were not retained")
			}

			var emitted atomic.Int64

			e.SetOnLocalEffect(func(_ effects.Tip, effect *pb.Effect) {
				// Engine snapshot materialization is not a graph mutation.
				if effect.GetData() != nil || effect.GetMeta() != nil {
					emitted.Add(1)
				}
			})

			for _, key := range []string{"a", "z", "m", "missing"} {
				if _, err := tree.Find(key); err != nil {
					t.Fatal(err)
				}
			}

			commit(t, tree, c)
			e.SetOnLocalEffect(nil)

			if emitted.Load() != 0 {
				t.Fatal("reader reconciled competing paths")
			}

			tree, c = openStored(t, e, graph.StringType)
			if err := tree.Insert("middle", graph.NewString("middle")); err != nil {
				t.Fatal(err)
			}

			commit(t, tree, c)

			tree, _ = openStored(t, e, graph.StringType)
			for _, key := range []string{"a", "z", "middle"} {
				requireValue(t, tree, key, key)
			}

			if initial {
				requireValue(t, tree, "m", "m")
			}

			root, err = tree.getRoot(false)
			if err != nil || len(tree.addresses(root, graph.Left)) != 1 {
				t.Fatalf("writer left competing roots: %v", err)
			}
		})
	}
}

func TestGeneratedPayload(t *testing.T) {
	e := testEngine(t)
	createStored(t, e, meta.CritMetaType)
	tree, c := openStored(t, e, meta.CritMetaType)

	payload := meta.NewCritMeta().SetKey("payload").SetBytePos(7)
	if err := tree.Insert("key", payload); err != nil {
		t.Fatal(err)
	}

	if got, err := tree.Find("key"); err != nil || got != payload {
		t.Fatal("payload identity was lost")
	}

	commit(t, tree, c)
	tree, _ = openStored(t, e, meta.CritMetaType)

	got, err := tree.Find("key")
	if err != nil || got.Key() != "payload" || got.BytePos() != 7 {
		t.Fatalf("generated payload round trip: %v", err)
	}
}

func TestConcurrentFirstWritesToSameKey(t *testing.T) {
	e := testEngine(t)
	createStored(t, e, graph.StringType)
	a, ca := openStored(t, e, graph.StringType)

	b, cb := openStored(t, e, graph.StringType)
	if err := a.Insert("", graph.NewString("a")); err != nil {
		t.Fatal(err)
	}

	if err := b.Insert("", graph.NewString("b")); err != nil {
		t.Fatal(err)
	}

	commit(t, a, ca)
	commit(t, b, cb)
	tree, c := openStored(t, e, graph.StringType)

	root, err := tree.getRoot(false)
	if err != nil {
		t.Fatal(err)
	}

	roots := tree.addresses(root, graph.Left)
	if len(roots) != 2 || roots[0] != roots[1] {
		t.Fatal("concurrent same-key insertions did not share the stable leaf")
	}

	leaf, err := tree.findBestMatch(root, "", false)
	if err != nil || len(tree.addresses(leaf, graph.Down)) != 2 {
		t.Fatalf("competing payloads were lost: %v", err)
	}

	if err := tree.Insert("", graph.NewString("resolved")); err != nil {
		t.Fatal(err)
	}

	commit(t, tree, c)
	tree, _ = openStored(t, e, graph.StringType)
	requireValue(t, tree, "", "resolved")
}

func TestConcurrentInteriorPathsAndDelete(t *testing.T) {
	e := testEngine(t)
	createStored(t, e, graph.StringType)

	tree, c := openStored(t, e, graph.StringType)
	for _, key := range []string{"a", "z"} {
		if err := tree.Insert(key, graph.NewString(key)); err != nil {
			t.Fatal(err)
		}
	}

	commit(t, tree, c)
	a, ca := openStored(t, e, graph.StringType)

	b, cb := openStored(t, e, graph.StringType)
	if err := a.Insert("aa", graph.NewString("aa")); err != nil {
		t.Fatal(err)
	}

	if err := b.Insert("ab", graph.NewString("ab")); err != nil {
		t.Fatal(err)
	}

	commit(t, a, ca)
	commit(t, b, cb)

	tree, c = openStored(t, e, graph.StringType)
	if deleted, err := tree.Delete("a"); err != nil || !deleted {
		t.Fatalf("delete while reconciling: %t %v", deleted, err)
	}

	commit(t, tree, c)
	tree, c = openStored(t, e, graph.StringType)
	// The shared presence-test branch's Right list is independent of deleting
	// its Left leaf. A writer entering that side reconciles aa/ab on demand.
	if deleted, err := tree.Delete("ab\x00"); err != nil || deleted {
		t.Fatalf("visit competing child paths: %t %v", deleted, err)
	}

	commit(t, tree, c)

	tree, _ = openStored(t, e, graph.StringType)
	for _, key := range []string{"aa", "ab", "z"} {
		requireValue(t, tree, key, key)
	}

	if got, err := tree.Find("a"); err != nil || got != nil {
		t.Fatalf("reconciliation resurrected deleted leaf: %v %v", got, err)
	}
}

func TestDeletePreservesUnobservedPayload(t *testing.T) {
	for _, deletionFirst := range []bool{false, true} {
		t.Run(fmt.Sprint(deletionFirst), func(t *testing.T) {
			e := testEngine(t)
			createStored(t, e, graph.StringType)

			tree, c := openStored(t, e, graph.StringType)
			if err := tree.Insert("key", graph.NewString("old")); err != nil {
				t.Fatal(err)
			}

			commit(t, tree, c)
			deleting, cd := openStored(t, e, graph.StringType)

			writing, cw := openStored(t, e, graph.StringType)
			if ok, err := deleting.Delete("key"); err != nil || !ok {
				t.Fatalf("delete: %t %v", ok, err)
			}

			if err := writing.Insert("key", graph.NewString("new")); err != nil {
				t.Fatal(err)
			}

			if deletionFirst {
				commit(t, deleting, cd)
				commit(t, writing, cw)
			} else {
				commit(t, writing, cw)
				commit(t, deleting, cd)
			}

			tree, _ = openStored(t, e, graph.StringType)
			requireValue(t, tree, "key", "new")
		})
	}
}

func TestOpenValidationAndDescriptorLifecycle(t *testing.T) {
	e := testEngine(t)

	c := testContext(t, e)
	if _, err := Open("missing", c, graph.StringType); err == nil {
		t.Fatal("opened missing graph")
	}

	if _, err := Open("missing", c, graph.Type[*graph.String]{}); err == nil {
		t.Fatal("accepted missing factory")
	}

	creation := Create("test", graph.StringType)
	if err := creation.Insert("key", graph.NewString("value")); err != nil {
		t.Fatal(err)
	}

	if err := creation.Flush(c); err == nil {
		t.Fatal("nodes emitted before descriptor commit")
	}

	c.Abort()
	createStored(t, e, graph.StringType)
	tree, c := openStored(t, e, graph.StringType)
	// Traversal errors propagate instead of silently treating a missing route as
	// an empty tree or creating replacement data during a read.
	root, err := tree.getRoot(true)
	if err != nil {
		t.Fatal(err)
	}

	root.AttachLeft(graph.Address("missing-node"))
	commit(t, tree, c)

	tree, _ = openStored(t, e, graph.StringType)
	if _, err := tree.Scan("", "", 0); err == nil {
		t.Fatal("missing routing node was silently accepted")
	}
}

func TestReconciliationPreservesStaleDescendantWriter(t *testing.T) {
	e := testEngine(t)
	createStored(t, e, graph.StringType)

	tree, c := openStored(t, e, graph.StringType)
	for _, key := range []string{"a", "m"} {
		if err := tree.Insert(key, graph.NewString(key)); err != nil {
			t.Fatal(err)
		}
	}

	commit(t, tree, c)

	stale, cs := openStored(t, e, graph.StringType)
	if err := stale.Insert("m\x00", graph.NewString("late")); err != nil {
		t.Fatal(err)
	}
	// Both writers wrap the old root. Their subsequent consolidation must
	// retain the branch on which the stale writer queued its descendant edit.
	z, cz := openStored(t, e, graph.StringType)

	high, ch := openStored(t, e, graph.StringType)
	if err := z.Insert("z", graph.NewString("z")); err != nil {
		t.Fatal(err)
	}

	if err := high.Insert("\x80", graph.NewString("high")); err != nil {
		t.Fatal(err)
	}

	commit(t, z, cz)
	commit(t, high, ch)

	rebuilding, cr := openStored(t, e, graph.StringType)
	if err := rebuilding.Insert("b", graph.NewString("b")); err != nil {
		t.Fatal(err)
	}

	commit(t, rebuilding, cr)
	commit(t, stale, cs)

	tree, c = openStored(t, e, graph.StringType)
	if err := tree.Insert("m", graph.NewString("updated")); err != nil {
		t.Fatal(err)
	}

	commit(t, tree, c)

	tree, _ = openStored(t, e, graph.StringType)
	for _, key := range []string{"a", "b", "z"} {
		requireValue(t, tree, key, key)
	}

	requireValue(t, tree, "\x80", "high")
	requireValue(t, tree, "m", "updated")
	requireValue(t, tree, "m\x00", "late")
}

func TestConcurrentSharedBranches(t *testing.T) {
	e := testEngine(t)
	createStored(t, e, graph.StringType)

	keys := []string{"", "a", "a\x00", "ab", "b", "m", "z", "\x00", "\x80", "\xff"}
	writers := make([]*Crit[*graph.String], len(keys))
	contexts := make([]*effects.Context, len(keys))
	// Each writer sees an empty tree and builds overlapping branch identities
	// locally, then publishes without observing the other writers.
	for i, key := range keys {
		writers[i], contexts[i] = openStored(t, e, graph.StringType)
		for _, inserted := range []string{"shared", key} {
			if err := writers[i].Insert(inserted, graph.NewString(inserted)); err != nil {
				t.Fatal(err)
			}
		}
	}

	for i := range writers {
		commit(t, writers[i], contexts[i])
	}

	tree, c := openStored(t, e, graph.StringType)
	if err := tree.Insert("shared", graph.NewString("resolved")); err != nil {
		t.Fatal(err)
	}

	commit(t, tree, c)
	tree, c = openStored(t, e, graph.StringType)

	root, err := tree.getRoot(false)
	if err != nil {
		t.Fatal(err)
	}
	// Shared branch identities stop the outer merge. Visit each writer path to
	// resolve its independent conflicts without reinserting or copying values.
	for _, key := range keys {
		leaf, err := tree.findBestMatch(root, key, true)
		if err != nil || leaf == nil || leaf.Value().Key() != key {
			t.Fatalf("writer path %q: %v, %v", key, leaf, err)
		}
	}

	commit(t, tree, c)

	tree, _ = openStored(t, e, graph.StringType)
	for _, key := range keys {
		requireValue(t, tree, key, key)
	}

	requireValue(t, tree, "shared", "resolved")
}
