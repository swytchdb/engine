package bplus

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
)

func testEngine(t testing.TB) *effects.Engine {
	t.Helper()

	e := effects.NewEngine(effects.EngineConfig{DefaultMode: effects.SafeMode})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})

	return e
}

func flush(t testing.TB, c *effects.Context) {
	t.Helper()

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
}

func newTree(t testing.TB, e *effects.Engine, order int) *Tree {
	t.Helper()

	c := e.NewContext()

	tree, err := Create(c, "animals", Options{MaxKeys: order})
	if err != nil {
		t.Fatal(err)
	}

	flush(t, c)

	c = e.NewContext()
	if err := tree.Initialize(c); err != nil {
		t.Fatal(err)
	}

	flush(t, c)

	return tree
}

func TestInitializationAndRollback(t *testing.T) {
	e := testEngine(t)
	c := e.NewContext()

	tree, err := Create(c, "animals", Options{})
	if err != nil {
		t.Fatal(err)
	}

	if err := tree.Initialize(c); err == nil {
		t.Fatal("initialized against pending descriptor")
	}
	// A failed initialization restores the caller's pending descriptor writes.
	flush(t, c)

	c = e.NewContext()
	if _, _, err := tree.Get(c, []byte("cat")); !errors.Is(err, ErrUninitialized) {
		t.Fatalf("uninitialized read: %v", err)
	}

	c.Abort()

	c = e.NewContext()
	if err := tree.Initialize(c); err != nil {
		t.Fatal(err)
	}

	if err := tree.Put(c, []byte("cat"), []byte("mammal")); err != nil {
		t.Fatal(err)
	}

	c.Abort()

	state, _, _, err := e.GetSnapshotInternal(tree.RootAddress())
	if err != nil || state != nil {
		t.Fatalf("aborted root visible: %v %v", state, err)
	}

	c = e.NewContext()
	if err := tree.Initialize(c); err != nil {
		t.Fatal(err)
	}

	flush(t, c)
	assertContents(t, e, tree, nil)

	c = e.NewContext()
	defer c.Abort()

	if err := tree.Initialize(c); !errors.Is(err, ErrExists) {
		t.Fatalf("duplicate initialization: %v", err)
	}
}

func TestConcurrentInitialization(t *testing.T) {
	e := testEngine(t)
	c := e.NewContext()

	tree, err := Create(c, "animals", Options{MaxKeys: 3})
	if err != nil {
		t.Fatal(err)
	}

	flush(t, c)

	first, second := e.NewContext(), e.NewContext()
	defer first.Abort()
	defer second.Abort()

	if err := tree.Initialize(first); err != nil {
		t.Fatal(err)
	}

	if err := tree.Initialize(second); err != nil {
		t.Fatal(err)
	}

	flush(t, first)

	if err := second.Flush(); !errors.Is(err, effects.ErrTxnAborted) {
		t.Fatalf("competing initialization: %v", err)
	}

	assertContents(t, e, tree, nil)
}

func TestSnapshotAcrossSplitAndStaleWriter(t *testing.T) {
	e := testEngine(t)
	tree := newTree(t, e, 3)
	want := map[string][]byte{"ant": []byte("a"), "cat": []byte("c"), "fox": []byte("f")}

	c := e.NewContext()
	for key, value := range want {
		if err := tree.Put(c, []byte(key), value); err != nil {
			t.Fatal(err)
		}
	}

	flush(t, c)

	reader := e.NewContext()
	defer reader.Abort()

	before, err := tree.Scan(reader, nil, nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	c = e.NewContext()
	if err := tree.Put(c, []byte("dog"), []byte("d")); err != nil {
		t.Fatal(err)
	}

	flush(t, c)

	after, err := tree.Scan(reader, nil, nil, 0)
	if err != nil || !slices.EqualFunc(before, after, func(a, b Entry) bool { return bytes.Equal(a.Key, b.Key) && bytes.Equal(a.Value, b.Value) }) {
		t.Fatalf("reader crossed split: %v %v", after, err)
	}

	if err := tree.Put(reader, []byte("cat"), []byte("stale")); err != nil {
		t.Fatal(err)
	}
	// An unrelated application write shares the same transaction outcome.
	if err := reader.Emit(&pb.Effect{Key: []byte("row"), Kind: &pb.Effect_Data{Data: &pb.DataEffect{
		Op: pb.EffectOp_INSERT_OP, Collection: pb.CollectionKind_SCALAR,
		Value: &pb.DataEffect_Raw{Raw: []byte("stale row")},
	}}}); err != nil {
		t.Fatal(err)
	}

	if err := reader.Flush(); !errors.Is(err, effects.ErrTxnAborted) {
		t.Fatalf("stale transaction: %v", err)
	}

	want["dog"] = []byte("d")
	assertContents(t, e, tree, want)

	state, _, _, err := e.GetSnapshot("row")
	if err != nil || state != nil {
		t.Fatalf("aborted row visible: %v %v", state, err)
	}
}

func assertContents(t *testing.T, e *effects.Engine, tree *Tree, want map[string][]byte) treeStats {
	t.Helper()

	c := e.NewContext()
	defer c.Abort()

	stats, err := checkTree(tree, c)
	if err != nil {
		t.Fatal(err)
	}

	got, err := tree.Scan(c, nil, nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	keys := make([]string, 0, len(want))
	for key := range want {
		keys = append(keys, key)
	}

	slices.Sort(keys)

	if len(got) != len(keys) || stats.Entries != len(keys) {
		t.Fatalf("entries: scan=%d stats=%d want=%d", len(got), stats.Entries, len(keys))
	}

	for i, key := range keys {
		if string(got[i].Key) != key || !bytes.Equal(got[i].Value, want[key]) {
			t.Fatalf("entry %d: got=%v want=%q:%q", i, got[i], key, want[key])
		}
	}

	return stats
}

func pageAddresses(t *testing.T, e *effects.Engine, tree *Tree) []string {
	t.Helper()

	c := e.NewContext()
	defer c.Abort()

	o, err := tree.operation(c)
	if err != nil {
		t.Fatal(err)
	}

	addresses := []string{tree.RootAddress()}
	for i := 0; i < len(addresses); i++ {
		n, err := o.load(addresses[i])
		if err != nil {
			t.Fatal(err)
		}

		addresses = append(addresses, n.Children...)
	}

	return addresses
}

func TestSplitMergeAndRegrow(t *testing.T) {
	for _, order := range []int{3, 4, 5} {
		t.Run(fmt.Sprint(order), func(t *testing.T) {
			e := testEngine(t)
			c := e.NewContext()

			tree, err := Create(c, "animals", Options{MaxKeys: order})
			if err != nil {
				t.Fatal(err)
			}

			flush(t, c)

			c = e.NewContext()
			if err := tree.Initialize(c); err != nil {
				t.Fatal(err)
			}

			flush(t, c)

			want := make(map[string][]byte)
			retired := make(map[string]bool)

			rng := rand.New(rand.NewPCG(71, 92))
			for round := range 2 {
				for step, i := range rng.Perm(32) {
					key := fmt.Sprintf("%03d", i)
					value := []byte("animal/" + key)

					c = e.NewContext()
					if err := tree.Put(c, []byte(key), value); err != nil {
						t.Fatalf("round %d insert %d: %v", round, step, err)
					}

					flush(t, c)

					want[key] = value
					assertContents(t, e, tree, want)
				}

				if stats := assertContents(t, e, tree, want); stats.Height < 3 {
					t.Fatalf("expected multilevel tree: %+v", stats)
				}

				addresses := pageAddresses(t, e, tree)
				for _, address := range addresses {
					if retired[address] {
						t.Fatal("regrowth reused a retired page address")
					}
				}

				for step, i := range rng.Perm(32) {
					key := fmt.Sprintf("%03d", i)
					c = e.NewContext()

					found, err := tree.Delete(c, []byte(key))
					if err != nil || !found {
						t.Fatalf("round %d delete %d: %v %v", round, step, found, err)
					}

					flush(t, c)
					delete(want, key)
					assertContents(t, e, tree, want)
				}

				if stats := assertContents(t, e, tree, want); stats.Nodes != 1 || stats.Height != 1 {
					t.Fatalf("root did not shrink: %+v", stats)
				}

				for _, address := range addresses[1:] {
					state, _, _, err := e.GetSnapshotInternal(address)
					if err != nil || state != nil {
						t.Fatalf("unlinked page was not deleted: %q %v %v", address, state, err)
					}

					retired[address] = true
				}
			}
		})
	}
}

func TestBinaryKeysAndOuterTransaction(t *testing.T) {
	e := testEngine(t)
	c := e.NewContext()
	c.BeginTx()

	tree, err := Create(c, "binary\x00\xffindex", Options{MaxKeys: 3})
	if err != nil {
		t.Fatal(err)
	}

	flush(t, c)
	c = e.NewContext()
	c.BeginTx()

	if err := tree.Initialize(c); err != nil {
		t.Fatal(err)
	}

	want := make(map[string][]byte)

	for _, key := range []string{"", "\x00", "\x00\xff", "\xff", "cat", "dog", "ant", "bat"} {
		value := []byte("value/" + key)
		if err := tree.Put(c, []byte(key), value); err != nil {
			t.Fatal(err)
		}

		want[key] = value
	}

	if err := tree.Put(c, []byte("cat"), []byte("updated")); err != nil {
		t.Fatal(err)
	}

	want["cat"] = []byte("updated")

	got, found, err := tree.Get(c, []byte("cat"))
	if err != nil || !found || string(got) != "updated" {
		t.Fatalf("pending read: %q %v %v", got, found, err)
	}

	flush(t, c)
	assertContents(t, e, tree, want)

	c = e.NewContext()
	defer c.Abort()

	entries, err := tree.Scan(c, []byte("ant"), []byte("dog"), 2)
	if err != nil || len(entries) != 2 || string(entries[0].Key) != "ant" || string(entries[1].Key) != "bat" {
		t.Fatalf("bounded scan: %v %v", entries, err)
	}

	entries, err = tree.Scan(c, nil, []byte{}, 0)
	if err != nil || len(entries) != 0 {
		t.Fatalf("empty upper bound: %v %v", entries, err)
	}

	if err := tree.Put(c, []byte("cat"), []byte("aborted")); err != nil {
		t.Fatal(err)
	}

	c.Abort()
	assertContents(t, e, tree, want)
}
