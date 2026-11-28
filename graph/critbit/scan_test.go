package critbit

import (
	"errors"
	"math/rand"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/engine/graph"
	"github.com/swytchdb/engine/graph/critbit/meta"
)

func TestScanBinaryKeysAndPendingEdits(t *testing.T) {
	tree := Create("test", graph.StringType)
	if entries, err := tree.Scan("", "", 0); err != nil || len(entries) != 0 {
		t.Fatalf("empty scan: %v, %v", entries, err)
	}

	keys := []string{"z", "a\x00b", "\xff", "", "a\x00", "\x80", "a", "\x00", "a\x00\x00"}
	for _, key := range keys {
		if err := tree.Insert(key, graph.NewString(key)); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := tree.Delete("a\x00"); err != nil {
		t.Fatal(err)
	}

	if err := tree.Insert("a", graph.NewString("replacement")); err != nil {
		t.Fatal(err)
	}

	entries, err := tree.Scan("", "", 0)
	if err != nil {
		t.Fatal(err)
	}

	want := slices.DeleteFunc(slices.Clone(keys), func(key string) bool { return key == "a\x00" })
	slices.Sort(want)

	got := make([]string, 0, len(entries))
	for _, entry := range entries {
		got = append(got, entry.Key)

		value, err := tree.Find(entry.Key)
		if err != nil || value != entry.Value {
			t.Fatalf("Scan/Find identity for %q: %v", entry.Key, err)
		}

		expected := entry.Key
		if entry.Key == "a" {
			expected = "replacement"
		}

		if entry.Value.Value() != expected {
			t.Fatalf("value for %q: %q, want %q", entry.Key, entry.Value.Value(), expected)
		}
	}

	if !slices.Equal(got, want) {
		t.Fatalf("keys = %q, want %q", got, want)
	}

	for _, key := range keys {
		if _, err := tree.Delete(key); err != nil {
			t.Fatal(err)
		}
	}

	if entries, err := tree.Scan("", "", 0); err != nil || len(entries) != 0 {
		t.Fatalf("all deleted: %v, %v", entries, err)
	}
}

func TestScanStoredValuesAndReadOnly(t *testing.T) {
	e := testEngine(t)
	createStored(t, e, graph.StringType)

	tree, c := openStored(t, e, graph.StringType)
	if entries, err := tree.Scan("", "", 0); err != nil || len(entries) != 0 {
		t.Fatalf("empty stored scan: %v, %v", entries, err)
	}

	for _, key := range []string{"b", "", "a"} {
		if err := tree.Insert(key, graph.NewString(key)); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := tree.Delete("a"); err != nil {
		t.Fatal(err)
	}

	commit(t, tree, c)

	var emitted atomic.Int64

	e.SetOnLocalEffect(func(_ effects.Tip, effect *pb.Effect) {
		if effect.GetData() != nil || effect.GetMeta() != nil {
			emitted.Add(1)
		}
	})
	defer e.SetOnLocalEffect(nil)

	tree, c = openStored(t, e, graph.StringType)

	entries, err := tree.Scan("", "", 0)
	if err != nil || len(entries) != 2 {
		t.Fatalf("stored scan: %v, %v", entries, err)
	}

	if entries[0].Key != "" || entries[0].Value.Value() != "" || entries[1].Key != "b" || entries[1].Value.Value() != "b" {
		t.Fatalf("unexpected stored entries: %v", entries)
	}

	commit(t, tree, c)

	if emitted.Load() != 0 {
		t.Fatal("scan emitted graph mutations")
	}
	// Scan values participate in the same pending edits as Find values.
	tree, c = openStored(t, e, graph.StringType)

	entries, err = tree.Scan("", "", 0)
	if err != nil {
		t.Fatal(err)
	}

	entries[1].Value.Append("!")
	commit(t, tree, c)
	tree, _ = openStored(t, e, graph.StringType)
	requireValue(t, tree, "b", "b!")
}

func TestScanUsesLastLiveSlots(t *testing.T) {
	e := testEngine(t)
	createStored(t, e, graph.StringType)

	tree, c := openStored(t, e, graph.StringType)
	if err := tree.Insert("a", graph.NewString("old")); err != nil {
		t.Fatal(err)
	}

	if err := tree.Insert("z", graph.NewString("z")); err != nil {
		t.Fatal(err)
	}

	root, err := tree.getRoot(false)
	if err != nil {
		t.Fatal(err)
	}

	branch, err := tree.child(root, graph.Left, false)
	if err != nil {
		t.Fatal(err)
	}

	leaf, err := tree.findBestMatch(root, "a", false)
	if err != nil {
		t.Fatal(err)
	}
	// Older roots, child paths, and payloads may coexist during a rewrite.
	// Leave all these slots live and assert scanning does not consolidate them.
	tree.Link(root, leaf, graph.Left)
	tree.Link(root, branch, graph.Left)
	replacement := tree.CreateNode("leaf/b", meta.NewCritMeta().SetIsLeaf(true).SetKey("b"))
	tree.Link(branch, replacement, graph.Left)
	first := tree.CreateNode("scan-value-first", graph.NewString("first"))
	last := tree.CreateNode("scan-value-last", graph.NewString("last"))
	tree.Link(replacement, first, graph.Down)
	tree.Link(replacement, last, graph.Down)
	commit(t, tree, c)

	var emitted atomic.Int64

	e.SetOnLocalEffect(func(_ effects.Tip, effect *pb.Effect) {
		if effect.GetData() != nil || effect.GetMeta() != nil {
			emitted.Add(1)
		}
	})
	defer e.SetOnLocalEffect(nil)

	tree, c = openStored(t, e, graph.StringType)

	entries, err := tree.Scan("", "", 0)
	if err != nil || len(entries) != 2 {
		t.Fatalf("scan with alternatives: %v, %v", entries, err)
	}

	if entries[0].Key != "b" || entries[0].Value.Value() != "last" || entries[1].Key != "z" {
		t.Fatalf("scan followed older slots: %v", entries)
	}

	for _, entry := range entries {
		value, err := tree.Find(entry.Key)
		if err != nil || value != entry.Value {
			t.Fatalf("Scan/Find disagree for %q: %v", entry.Key, err)
		}
	}

	bounded, err := tree.Scan("b", "z", 1)
	if err != nil || len(bounded) != 1 || bounded[0].Value != entries[0].Value {
		t.Fatalf("bounded scan with alternatives: %v, %v", bounded, err)
	}

	commit(t, tree, c)

	if emitted.Load() != 0 {
		t.Fatal("scan reconciled earlier slots")
	}
}

func TestScanIncompleteRouting(t *testing.T) {
	for _, fault := range []string{"missing node", "missing child", "cycle"} {
		t.Run(fault, func(t *testing.T) {
			e := testEngine(t)
			createStored(t, e, graph.StringType)

			tree, c := openStored(t, e, graph.StringType)
			for _, key := range []string{"a", "z"} {
				if err := tree.Insert(key, graph.NewString(key)); err != nil {
					t.Fatal(err)
				}
			}

			root, err := tree.getRoot(false)
			if err != nil {
				t.Fatal(err)
			}

			branch, err := tree.child(root, graph.Left, false)
			if err != nil {
				t.Fatal(err)
			}

			switch fault {
			case "missing node":
				branch.AttachRight(graph.Address("missing"))
			case "missing child":
				tree.UnlinkAt(branch, graph.Right, 0)
			case "cycle":
				tree.Link(branch, branch, graph.Right)
			}

			commit(t, tree, c)

			tree, _ = openStored(t, e, graph.StringType)
			for _, lower := range []string{"", "z"} {
				entries, err := tree.Scan(lower, "", 0)
				if !errors.Is(err, ErrIncomplete) || entries != nil {
					t.Fatalf("Scan(%q): %v, %v; want nil, ErrIncomplete", lower, entries, err)
				}
			}
		})
	}
}

func TestScanBoundsAndLimit(t *testing.T) {
	e := testEngine(t)
	createStored(t, e, graph.StringType)
	tree, c := openStored(t, e, graph.StringType)

	keys := []string{"", "\x00", "a", "a\x00", "ab", "b", "z", "\x80", "\xff"}
	for _, key := range slices.Backward(keys) {
		if err := tree.Insert(key, graph.NewString(key)); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := tree.Delete("ab"); err != nil {
		t.Fatal(err)
	}

	all := slices.DeleteFunc(slices.Clone(keys), func(key string) bool { return key == "ab" })
	tests := []struct {
		name         string
		lower, upper string
		limit        int
		want         []string
	}{
		{name: "unbounded", want: all},
		{name: "negative limit", limit: -1, want: all},
		{name: "inclusive lower", lower: "a", want: []string{"a", "a\x00", "b", "z", "\x80", "\xff"}},
		{name: "exclusive upper", upper: "a", want: []string{"", "\x00"}},
		{name: "both", lower: "a", upper: "z", want: []string{"a", "a\x00", "b"}},
		{name: "limit", limit: 1, want: []string{""}},
		{name: "limit skips tombstone", lower: "a\x00", limit: 2, want: []string{"a\x00", "b"}},
		{name: "upper before limit", lower: "a", upper: "b", limit: 100, want: []string{"a", "a\x00"}},
		{name: "empty lower", lower: "", want: all},
		{name: "empty upper", upper: "", want: all},
		{name: "empty key only", upper: "\x00", want: []string{""}},
		{name: "absent endpoints", lower: "aa", upper: "y", want: []string{"b"}},
		{name: "equal", lower: "a", upper: "a"},
		{name: "reversed", lower: "z", upper: "a"},
		{name: "past end", lower: "\xff\x00"},
		{name: "binary", lower: "a\x00", upper: "\xff", want: []string{"a\x00", "b", "z", "\x80"}},
	}

	for _, stored := range []bool{false, true} {
		if stored {
			commit(t, tree, c)
			tree, _ = openStored(t, e, graph.StringType)
		}

		name := "pending"
		if stored {
			name = "stored"
		}

		t.Run(name, func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					entries, err := tree.Scan(tt.lower, tt.upper, tt.limit)
					if err != nil {
						t.Fatal(err)
					}

					var got []string
					for _, entry := range entries {
						got = append(got, entry.Key)
						if entry.Value.Value() != entry.Key {
							t.Fatalf("wrong payload for %q: %q", entry.Key, entry.Value.Value())
						}
					}

					if !slices.Equal(got, tt.want) {
						t.Fatalf("keys = %q, want %q", got, tt.want)
					}
				})
			}
		})
	}
}

func TestScanStopsAtLimit(t *testing.T) {
	e := testEngine(t)
	createStored(t, e, graph.StringType)

	tree, c := openStored(t, e, graph.StringType)
	for _, key := range []string{"a", "z"} {
		if err := tree.Insert(key, graph.NewString(key)); err != nil {
			t.Fatal(err)
		}
	}

	root, err := tree.getRoot(false)
	if err != nil {
		t.Fatal(err)
	}

	branch, err := tree.child(root, graph.Left, false)
	if err != nil {
		t.Fatal(err)
	}

	branch.AttachRight(graph.Address("missing"))
	commit(t, tree, c)
	tree, _ = openStored(t, e, graph.StringType)

	entries, err := tree.Scan("", "", 1)
	if err != nil || len(entries) != 1 || entries[0].Key != "a" {
		t.Fatalf("limited scan: %v, %v", entries, err)
	}

	if _, err := tree.Scan("", "", 0); !errors.Is(err, ErrIncomplete) {
		t.Fatalf("unlimited scan did not reach missing node: %v", err)
	}
}

func TestScanSeekCompressedPrefixes(t *testing.T) {
	tests := []struct {
		name  string
		keys  []string
		lower string
		want  []string
	}{
		{"skipped byte before root", []string{"ba", "bc"}, "ab", []string{"ba", "bc"}},
		{"skipped byte after root", []string{"ba", "bc"}, "zb", nil},
		{"skipped bit before root", []string{"\x60", "\x62"}, "\x52", []string{"\x60", "\x62"}},
		{"skipped bit after root", []string{"\x60", "\x62"}, "\x70", nil},
		{"shorter prefix", []string{"abc0", "abc1", "z"}, "ab", []string{"abc0", "abc1", "z"}},
		{"zero extension", []string{"a", "a\x00\x00", "a\x00b", "b"}, "a\x00", []string{"a\x00\x00", "a\x00b", "b"}},
		{"retain ancestor right", []string{"aa0", "aa1", "ab0", "ab1", "z"}, "aa2", []string{"ab0", "ab1", "z"}},
		{"retain whole subtree and ancestor right", []string{"aa0", "aa1", "ab0", "ab1", "z"}, "aa", []string{"aa0", "aa1", "ab0", "ab1", "z"}},
		{"exact leaf", []string{"a", "b", "c", "d"}, "b", []string{"b", "c", "d"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree := Create("test", graph.StringType)
			for _, key := range tt.keys {
				if err := tree.Insert(key, graph.NewString(key)); err != nil {
					t.Fatal(err)
				}
			}

			entries, err := tree.Scan(tt.lower, "", 0)
			if err != nil {
				t.Fatal(err)
			}

			var got []string
			for _, entry := range entries {
				got = append(got, entry.Key)
			}

			if !slices.Equal(got, tt.want) {
				t.Fatalf("Scan(%q): %q, want %q", tt.lower, got, tt.want)
			}
		})
	}
}

func TestScanSeekMatchesSortedKeys(t *testing.T) {
	random := rand.New(rand.NewSource(73))
	randomKey := func() string {
		key := make([]byte, random.Intn(6))
		random.Read(key)

		return string(key)
	}
	tree := Create("test", graph.StringType)

	keys := []string{"", "a", "a\x00", "a\x00b", "\x00", "\xff"}
	for range 100 {
		keys = append(keys, randomKey())
	}

	for _, key := range keys {
		if err := tree.Insert(key, graph.NewString(key)); err != nil {
			t.Fatal(err)
		}
	}

	slices.Sort(keys)
	keys = slices.Compact(keys)
	// Keep the deleted keys as seek targets, to check continuation past tombstones.
	bounds := slices.Clone(keys)

	var live []string

	for i, key := range keys {
		if i%5 == 0 {
			if _, err := tree.Delete(key); err != nil {
				t.Fatal(err)
			}
		} else {
			live = append(live, key)
		}
	}

	for range 100 {
		bounds = append(bounds, randomKey())
	}

	for _, lower := range bounds {
		for _, upper := range []string{"", randomKey()} {
			for _, limit := range []int{0, 1, 7} {
				var want []string

				for _, key := range live {
					if key >= lower && (upper == "" || key < upper) {
						want = append(want, key)
						if limit > 0 && len(want) == limit {
							break
						}
					}
				}

				entries, err := tree.Scan(lower, upper, limit)
				if err != nil {
					t.Fatal(err)
				}

				var got []string
				for _, entry := range entries {
					got = append(got, entry.Key)
				}

				if !slices.Equal(got, want) {
					t.Fatalf("Scan(%q, %q, %d) = %q, want %q", lower, upper, limit, got, want)
				}
			}
		}
	}
}

func TestScanSeekReconstructsOnlySearchAndResultPaths(t *testing.T) {
	const (
		height = 10
		size   = 1 << height
	)

	keyAt := func(i int) string { return string([]byte{byte(i >> 8), byte(i)}) }
	e := testEngine(t)
	createStored(t, e, graph.StringType)

	tree, c := openStored(t, e, graph.StringType)
	for i := range size {
		if err := tree.Insert(keyAt(i), graph.NewString(keyAt(i))); err != nil {
			t.Fatal(err)
		}
	}

	commit(t, tree, c)

	for _, tt := range []struct {
		name, lower string
		count       int
	}{
		{"last key", keyAt(size - 1), 1},
		{"last ten", keyAt(size - 10), 10},
		{"absent lower", keyAt(size-11) + "\x00", 10},
		{"past end", "\xff", 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			// A fresh graph has no cached routing nodes. GetNode retains each
			// reconstructed node in Graph.pending, including sentinel and payloads.
			tree, _ := openStored(t, e, graph.StringType)

			entries, err := tree.Scan(tt.lower, "", max(1, tt.count))
			if err != nil || len(entries) != tt.count {
				t.Fatalf("scan last %d: %v, %v", tt.count, entries, err)
			}

			for i, entry := range entries {
				if entry.Key != keyAt(size-tt.count+i) || entry.Value.Value() != entry.Key {
					t.Fatalf("entry %d = %q", i, entry.Key)
				}
			}
			// Inspect cache size without exposing a production instrumentation API.
			loaded := reflect.ValueOf(tree.Graph).Elem().FieldByName("pending").Len()

			routing := loaded - tt.count // exclude payloads and, if loaded, root sentinel
			if tree.root != nil {
				routing--
			}

			budget := 2*height + 2*tt.count
			if routing > budget {
				t.Fatalf("reconstructed %d routing nodes for %d results; budget %d (tree has %d)", routing, tt.count, budget, 2*size-1)
			}

			t.Logf("%d results: reconstructed %d of %d routing nodes", tt.count, routing, 2*size-1)
		})
	}
}
