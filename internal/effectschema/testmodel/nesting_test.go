package testmodel

import (
	"bytes"
	"fmt"
	"maps"
	"slices"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/engine/graph"
	"github.com/swytchdb/engine/keytrie"
	"google.golang.org/protobuf/proto"
)

func TestAttachedFieldsAndCollections(t *testing.T) {
	a := &Account{}
	p := a.Profile()
	contact := p.Contact()
	attributes := a.Attributes()

	list := a.Messages()
	if a.Snapshot() != nil {
		t.Fatal("accessors queued effects")
	}

	p.SetDisplayName("Rob")
	contact.SetEmail("rob@example.test")
	a.Backup().SetDisplayName("Backup")
	a.SetName("Account")
	attributes.Set("", "empty").Set("\x00\xff/slash", "binary").Set("locale", "en")
	attributes.Remove("absent")

	if got, ok := a.Attributes().Get("locale"); !ok || got != "en" {
		t.Fatal("map accessor detached")
	}

	if _, ok := attributes.Get("missing"); ok {
		t.Fatal("missing map entry found")
	}

	a.Tags().Add("", "active", "active", "\xff/slash").Remove("absent")

	if !slices.Equal(a.Tags().Values(), []string{"", "active", "\xff/slash"}) {
		t.Fatal("set membership mismatch")
	}

	a.Tags().Remove("active")

	if a.Tags().Contains("active") {
		t.Fatal("set removal lost")
	}

	list.Append("a", "b", "b").Prepend("first", "second").Insert(2, "middle").Set(3, "A").Remove(4)

	if got := a.Messages().Values(); !slices.Equal(got, []string{"first", "second", "middle", "A", "b"}) {
		t.Fatalf("list = %v", got)
	}

	if list.Get(2) != "middle" {
		t.Fatal("list read mismatch")
	}

	values := list.Values()
	values[0] = "changed"

	if list.Get(0) != "first" {
		t.Fatal("Values aliases the list")
	}

	if a.Profile().DisplayName() != "Rob" || a.Profile().Contact().Email() != "rob@example.test" || a.Backup().DisplayName() != "Backup" {
		t.Fatal("nested accessor detached or crossed sibling scopes")
	}

	key := []byte{0, 255}
	a.Payloads().Append(key)
	a.Blobs().Set("key", key)
	key[0] = 7
	item := a.Payloads().Get(0)
	item[1] = 0

	blob, ok := a.Blobs().Get("key")
	if !ok {
		t.Fatal("blob missing")
	}

	blob[1] = 0

	if !bytes.Equal(a.Payloads().Get(0), []byte{0, 255}) {
		t.Fatal("list byte value aliases input/output")
	}

	blob, _ = a.Blobs().Get("key")
	if !bytes.Equal(blob, []byte{0, 255}) {
		t.Fatal("map byte value aliases input/output")
	}

	list.Replace("replacement")
	attributes.Replace(map[string]string{"fresh": "value"})
	a.Tags().Replace("new", "new")

	if !slices.Equal(list.Values(), []string{"replacement"}) || !maps.Equal(attributes.Values(), map[string]string{"fresh": "value"}) || !slices.Equal(a.Tags().Values(), []string{"new"}) {
		t.Fatal("collection replacements failed")
	}

	if a.Name() != "Account" || p.DisplayName() != "Rob" {
		t.Fatal("collection replacement removed scalar siblings")
	}
}

func TestNestedCompositeRoundTrip(t *testing.T) {
	for _, mode := range []string{"root", "partition", "node"} {
		for _, tx := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/transaction=%t", mode, tx), func(t *testing.T) {
				e := effects.NewTestEngine()

				t.Cleanup(func() {
					if err := e.Close(); err != nil {
						t.Error(err)
					}
				})

				newContext := func() *effects.Context {
					c := e.NewContext()
					t.Cleanup(c.Abort)

					if tx {
						c.BeginTx()
					}

					return c
				}

				key, prefix := "account", ""
				if mode == "partition" {
					prefix = "envelope/account"
				}

				if mode == "node" {
					prefix = "value"
					key = keytrie.InternalKey("accounts", "root")

					c := newContext()
					if err := graph.CreateGraph("accounts", "test").Flush(c); err != nil {
						t.Fatal(err)
					}

					if err := c.Flush(); err != nil {
						t.Fatal(err)
					}
				}

				firstNodeLoad := true
				load := func() (*Account, func()) {
					c := newContext()
					if mode == "node" {
						desc, tips, err := c.GetSnapshot("accounts")
						if err != nil {
							t.Fatal(err)
						}

						g, err := graph.GraphFromSnapshot("accounts", desc, tips)
						if err != nil {
							t.Fatal(err)
						}

						n, err := g.GetNode(key, c, AccountType)
						if err != nil {
							t.Fatal(err)
						}

						if firstNodeLoad {
							n.AttachDown(graph.Address("neighbor")).SetRegion("zone", "west")

							firstNodeLoad = false
						}

						return n.Value(), func() {
							if err := g.Flush(c); err != nil {
								t.Fatal(err)
							}

							if err := c.Flush(); err != nil {
								t.Fatal(err)
							}
						}
					}

					snap, tips, err := c.GetSnapshot(key)
					if err != nil {
						t.Fatal(err)
					}

					a, err := AccountFromSnap(graph.PartitionSnapshot(snap, prefix))
					if err != nil {
						t.Fatal(err)
					}

					return a, func() {
						if err := a.EmitPartition(key, prefix, c, tips); err != nil {
							t.Fatal(err)
						}

						if err := c.Flush(); err != nil {
							t.Fatal(err)
						}
					}
				}
				a, commit := load()
				// No root scalar fields: nested partitions alone must survive storage.
				a.Profile().Contact().SetEmail("original")
				a.Messages().Append("one", "two")
				a.Tags().Add("active", "active")
				a.Attributes().Set("locale", "en")
				commit()

				a, commit = load()
				if a.Profile().Contact().Email() != "original" || a.Messages().Get(1) != "two" || !a.Tags().Contains("active") {
					t.Fatal("partition-only value did not reload")
				}

				seed := a.Snapshot()
				before := proto.Clone(seed)

				a.Profile().SetDisplayName("Rob")
				a.Profile().Contact().SetEmail("updated")
				a.Backup().Contact().SetEmail("backup")
				a.SetName("Account").AddBalance(5)
				a.Messages().Prepend("zero").Set(1, "ONE").Remove(2)
				a.Attributes().Set("locale", "nl")
				a.Tags().Remove("active").Add("new")
				unknown := a.Profile().Child("unknown")
				unknown.ApplyField("x", &pb.DataEffect{Op: pb.EffectOp_INSERT_OP, Value: &pb.DataEffect_IntVal{IntVal: 1}})

				if !proto.Equal(seed, before) {
					t.Fatal("mutated snapshot during edits")
				}

				commit()

				a, commit = load()
				if a.Name() != "Account" || a.Balance() != 5 || a.Profile().DisplayName() != "Rob" || a.Profile().Contact().Email() != "updated" || !slices.Equal(a.Messages().Values(), []string{"zero", "ONE"}) || !maps.Equal(a.Attributes().Values(), map[string]string{"locale": "nl"}) || !slices.Equal(a.Tags().Values(), []string{"new"}) {
					t.Fatal("edits did not persist")
				}

				profile := a.Profile()
				contact := profile.Contact()
				profile.Clear()

				if profile.DisplayName() != "" || contact.Email() != "" {
					t.Fatal("subtree clear left known descendants")
				}

				contact.SetEmail("recreated") // Existing child view stays attached.
				commit()

				a, commit = load()
				if a.Profile().Contact().Email() != "recreated" || a.Profile().DisplayName() != "" || a.Backup().Contact().Email() != "backup" || a.Name() != "Account" || len(a.Messages().Values()) != 2 {
					t.Fatal("subtree clear affected sibling data or lost recreation")
				}

				if unknown := a.Profile().Child("unknown"); unknown.Field("x") != nil {
					t.Fatal("subtree clear retained unknown descendant")
				}

				a.Clear()
				a.Profile().Contact().SetEmail("fresh")
				commit()

				a, _ = load()
				if a.Profile().Contact().Email() != "fresh" || a.Name() != "" || len(a.Messages().Values()) != 0 || a.Backup().Contact().Email() != "" {
					t.Fatal("whole composite clear did not remove descendants")
				}

				read := newContext()

				var (
					stored *pb.ReducedEffect
					err    error
				)
				if mode == "node" {
					stored, _, err = read.GetSnapshotInternal(key)
				} else {
					stored, _, err = read.GetSnapshot(key)
				}

				if err != nil {
					t.Fatal(err)
				}

				path := "profile/contact"
				if prefix != "" {
					path = prefix + "/" + path
				}

				if string(stored.GetPartitions()[path].GetNetAdds()["email"].GetData().Decompress()) != "fresh" {
					t.Fatalf("missing raw partition %q", path)
				}

				if mode == "node" {
					if string(stored.GetPartitions()["down"].GetOrderedElements()[0].GetData().Decompress()) != "neighbor" || string(stored.GetPartitions()[pb.GraphRegionsKey].GetNetAdds()["zone"].GetData().Decompress()) != "west" {
						t.Fatal("clearing composite value removed node relationships or regions")
					}
				}
			})
		}
	}
}
