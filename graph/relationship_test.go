package graph

import (
	"fmt"
	"slices"
	"testing"

	"github.com/swytchdb/engine/effects"
)

func loadRelationshipGraph(t *testing.T, e *effects.Engine) (*Graph, *effects.Context) {
	t.Helper()

	c := e.NewContext()
	t.Cleanup(c.Abort)

	snap, tips, err := c.GetSnapshot("links")
	if err != nil {
		t.Fatal(err)
	}

	g, err := GraphFromSnapshot("links", snap, tips)
	if err != nil {
		t.Fatal(err)
	}

	return g, c
}

func commitRelationshipGraph(t *testing.T, g *Graph, c *effects.Context) {
	t.Helper()

	if err := g.Flush(c); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
}

func TestGraphRelationshipSlotOrder(t *testing.T) {
	for _, direction := range []Direction{Up, Down, Left, Right} {
		t.Run(fmt.Sprint(direction), func(t *testing.T) {
			e := scalarTestEngine(t)
			seedAPIGraph(t, e, "links")
			g, c := loadRelationshipGraph(t, e)
			from := g.CreateNode("from", NewString("source"))
			z := g.CreateNode("z", NewString("old"))
			a := g.CreateNode("a", NewString("replacement"))
			fromAddress, zAddress, aAddress := string(from.Address()), string(z.Address()), string(a.Address())
			g.Link(from, z, direction)
			g.Link(from, a, direction)
			g.Link(from, z, direction)
			commitRelationshipGraph(t, g, c)

			g, c = loadRelationshipGraph(t, e)

			from, err := g.GetNode(fromAddress, c, StringType)
			if err != nil {
				t.Fatal(err)
			}

			addresses, count := g.NeighborAddresses(from, direction)
			if count != 3 || !slices.Equal(slices.Collect(addresses), []string{zAddress, aAddress, zAddress}) {
				t.Fatal("slot order or duplicate addresses were lost on reload")
			}

			neighbors, err := g.GetNeighbors(from, c, direction, StringType)
			if err != nil {
				t.Fatal(err)
			}

			if len(neighbors) != 3 || neighbors[0] != neighbors[2] || neighbors[1].Value().Value() != "replacement" {
				t.Fatal("neighbor loading changed slot order or removed duplicates")
			}

			if len(from.fields.getEffects()) != 0 {
				t.Fatal("reading relationships queued reconciliation effects")
			}
			// Publish a new last slot before removing the old slots. The same
			// address already occurs earlier; removal must target slot identity.
			g.Link(from, neighbors[1], direction)

			for i := 2; i >= 0; i-- {
				g.UnlinkAt(from, direction, i)
			}

			commitRelationshipGraph(t, g, c)

			g, c = loadRelationshipGraph(t, e)

			from, err = g.GetNode(fromAddress, c, StringType)
			if err != nil {
				t.Fatal(err)
			}

			addresses, count = g.NeighborAddresses(from, direction)
			if count != 1 || !slices.Equal(slices.Collect(addresses), []string{aAddress}) {
				t.Fatal("removing earlier slots removed the appended replacement")
			}

			old, err := g.GetNode(zAddress, c, StringType)
			if err != nil || old.Value().Value() != "old" {
				t.Fatalf("unlink deleted the target node: %v", err)
			}
		})
	}
}

func TestGraphUnlinkPreservesConcurrentAppend(t *testing.T) {
	for _, bySlot := range []bool{false, true} {
		t.Run(fmt.Sprintf("bySlot=%t", bySlot), func(t *testing.T) {
			e := scalarTestEngine(t)
			seedAPIGraph(t, e, "links")
			g, c := loadRelationshipGraph(t, e)
			from := g.CreateNode("from", NewString("source"))
			to := g.CreateNode("to", NewString("target"))
			fromAddress, toAddress := string(from.Address()), string(to.Address())
			g.Link(from, to, Down)
			commitRelationshipGraph(t, g, c)

			removeGraph, removeCtx := loadRelationshipGraph(t, e)

			observed, err := removeGraph.GetNode(fromAddress, removeCtx, StringType)
			if err != nil {
				t.Fatal(err)
			}

			target, err := removeGraph.GetNode(toAddress, removeCtx, StringType)
			if err != nil {
				t.Fatal(err)
			}

			if bySlot {
				removeGraph.UnlinkAt(observed, Down, 0)
			} else {
				removeGraph.Unlink(observed, target, Down)
			}

			// Another writer appends the same address after the remover's read.
			// Neither operation uses a transaction.
			g, c = loadRelationshipGraph(t, e)

			from, err = g.GetNode(fromAddress, c, StringType)
			if err != nil {
				t.Fatal(err)
			}

			from.AttachDown(Address(toAddress))
			commitRelationshipGraph(t, g, c)
			commitRelationshipGraph(t, removeGraph, removeCtx)

			g, c = loadRelationshipGraph(t, e)

			from, err = g.GetNode(fromAddress, c, StringType)
			if err != nil {
				t.Fatal(err)
			}

			addresses, count := g.NeighborAddresses(from, Down)
			if count != 1 || !slices.Equal(slices.Collect(addresses), []string{toAddress}) {
				t.Fatal("removing an observed slot erased a concurrent append")
			}
		})
	}
}
