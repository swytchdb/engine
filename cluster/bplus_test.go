package cluster

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/engine/graph/bplus"
)

func TestBPlusFivePeers(t *testing.T) {
	const (
		count = 5
		key   = "animal-index"
	)

	engines := make([]*effects.Engine, count)
	handlers := make([]EffectHandler, count)
	readers := make([]LogReader, count)

	identities := make([]NodeId, count)
	for i := range count {
		e := effects.NewEngine(effects.EngineConfig{DefaultMode: effects.SafeMode})
		engines[i], identities[i] = e, e.NodeID()
		handlers[i], readers[i] = NewEngineEffectHandler(e), NewEngineLogReader(e.EffectCache())

		t.Cleanup(func() {
			if err := e.Close(); err != nil {
				t.Error(err)
			}
		})
	}

	peers := startNNodeCluster(t, handlers, readers, identities...)
	for i, e := range engines {
		e.SetBroadcaster(peers[i])
	}

	for _, e := range engines {
		if err := e.Subscribe(key); err != nil {
			t.Fatal(err)
		}
	}

	write := func(e *effects.Engine, f func(*effects.Context) error) {
		t.Helper()

		c := e.NewContext()
		defer c.Abort()

		if err := f(c); err != nil {
			t.Fatal(err)
		}

		if err := c.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	wait := func(description string, f func() bool) {
		t.Helper()

		deadline := time.Now().Add(10 * time.Second)
		for !f() {
			if time.Now().After(deadline) {
				t.Fatal(description)
			}

			time.Sleep(time.Millisecond)
		}
	}

	var tree *bplus.Tree

	write(engines[0], func(c *effects.Context) (err error) {
		tree, err = bplus.Create(c, key, bplus.Options{MaxKeys: 3})

		return err
	})
	write(engines[0], func(c *effects.Context) error {
		if err := tree.Initialize(c); err != nil {
			return err
		}

		for _, animal := range []string{"ant", "bat", "cat"} {
			if err := tree.Put(c, []byte(animal), []byte("row/"+animal)); err != nil {
				return err
			}
		}

		return nil
	})

	for i := 1; i < count; i++ {
		e := engines[i]
		wait(fmt.Sprintf("peer %d missing entry point", i), func() bool {
			state, _, _, err := e.GetSnapshot(key)
			if err != nil {
				t.Fatal(err)
			}

			return len(state.GetPartitions()[pb.GraphEntryPointKey].GetOrderedElements()) == 1
		})
		// A tree walk explicitly discovers an existing node through its address.
		c := e.NewContext()
		value, found, err := tree.Get(c, []byte("cat"))
		c.Abort()

		if err != nil || !found || string(value) != "row/cat" {
			t.Fatalf("peer %d cold read: %q %v %v", i, value, found, err)
		}
	}

	// Pin a reader's view before another peer splits the root.
	old := engines[4].NewContext()
	defer old.Abort()

	before, err := tree.Scan(old, nil, nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	write(engines[0], func(c *effects.Context) error { return tree.Put(c, []byte("dog"), []byte("row/dog")) })

	after, err := tree.Scan(old, nil, nil, 0)
	if err != nil || !slices.EqualFunc(before, after, func(a, b bplus.Entry) bool { return bytes.Equal(a.Key, b.Key) && bytes.Equal(a.Value, b.Value) }) {
		t.Fatalf("snapshot crossed a remote split: %v %v", after, err)
	}

	if err := tree.Put(old, []byte("cat"), []byte("stale")); err != nil {
		t.Fatal(err)
	}

	if err := old.Flush(); !errors.Is(err, effects.ErrTxnAborted) {
		t.Fatalf("stale distributed write: %v", err)
	}

	animals := []string{"ant", "bat", "cat", "dog", "eel", "fox", "gnu", "hare", "ibis", "jay", "kiwi", "lynx"}
	checkAll := func(want []string) {
		t.Helper()

		for i, e := range engines {
			wait(fmt.Sprintf("peer %d has stale entries", i), func() bool {
				c := e.NewContext()
				defer c.Abort()

				entries, err := tree.Scan(c, nil, nil, 0)
				if err != nil {
					t.Fatal(err)
				}

				if len(entries) != len(want) {
					return false
				}

				for j, animal := range want {
					if string(entries[j].Key) != animal || string(entries[j].Value) != "row/"+animal {
						return false
					}
				}

				return true
			})
		}
	}
	checkAll(animals[:4])

	for i, animal := range animals[4:] {
		write(engines[i%count], func(c *effects.Context) error { return tree.Put(c, []byte(animal), []byte("row/"+animal)) })
		checkAll(animals[:i+5])
	}

	for i, animal := range animals {
		write(engines[(i+1)%count], func(c *effects.Context) error {
			found, err := tree.Delete(c, []byte(animal))
			if err == nil && !found {
				return fmt.Errorf("missing animal %s", animal)
			}

			return err
		})
		checkAll(animals[i+1:])
	}

	for i, e := range engines {
		c := e.NewContext()
		state, _, err := c.GetSnapshotInternal(tree.RootAddress())
		c.Abort()

		if err != nil {
			t.Fatal(err)
		}

		var root struct {
			Leaf     bool
			Keys     [][]byte
			Children [][]byte
		}
		if err := json.Unmarshal(state.GetScalar().Decompress(), &root); err != nil {
			t.Fatal(err)
		}

		if !root.Leaf || len(root.Keys) != 0 || len(root.Children) != 0 {
			t.Fatalf("peer %d root did not shrink to an empty leaf: %+v", i, root)
		}
	}
}
