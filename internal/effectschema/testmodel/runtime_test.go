package testmodel_test

import (
	"slices"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/engine/graph"
	model "github.com/swytchdb/engine/internal/effectschema/testmodel"
)

// Deliberately does not embed a graph type: the public protocol is sufficient.
type person struct{ state graph.Composite }

func (p *person) Runtime() *graph.Composite { return &p.state }
func (p *person) Name() *graph.String       { return graph.Bind(&p.state, "name", graph.StringType) }
func (p *person) Rename(name string)        { p.Name().Set(name) }

var (
	personType             = graph.Type[*person]{New: func() *person { return &person{} }}
	_          graph.Value = (*person)(nil)
)

func TestStandaloneAndDynamicComposition(t *testing.T) {
	s := graph.NewString("Hello")
	s.Append(" world")

	if s.Value() != "Hello world" {
		t.Fatal("standalone mutation failed")
	}

	root := graph.NewComposite()
	if err := root.Set("greeting", s); err != nil {
		t.Fatal(err)
	}

	if value, ok := root.Lookup("greeting"); !ok || value != s {
		t.Fatal("composition did not retain identity")
	}

	s.Append("!")
	graph.Bind(root, "greeting", graph.StringType).Prepend("[").Append("]")

	if s.Value() != "[Hello world!]" {
		t.Fatal("composed references disagree")
	}

	nested := graph.NewComposite()

	before := graph.NewString("before")
	if err := nested.Set("text", before); err != nil {
		t.Fatal(err)
	}

	if err := root.Set("nested", nested); err != nil {
		t.Fatal(err)
	}

	if graph.Bind(nested, "text", graph.StringType) != before {
		t.Fatal("nested identity lost while composing owner")
	}

	before.Append(" after")

	if graph.Bind(nested, "text", graph.StringType).Value() != "before after" {
		t.Fatal("nested mutation lost")
	}

	if err := nested.Set("cycle", root); err == nil {
		t.Fatal("cycle was accepted")
	}

	other := graph.NewComposite()
	if err := other.Set("duplicate", s); err == nil {
		t.Fatal("same instance attached to two owners")
	}

	replacement := graph.NewString("replacement")
	if err := root.Set("greeting", replacement); err != nil {
		t.Fatal(err)
	}

	if value, _ := root.Lookup("greeting"); value != replacement {
		t.Fatal("replacement identity not retained")
	}

	replacement.Append("!")

	if replacement.Value() != "replacement!" {
		t.Fatal("replacement's edits lost")
	}

	snap := root.Snapshot()

	loaded, err := graph.CompositeType.FromSnapshot(snap)
	if err != nil {
		t.Fatal(err)
	}

	value, ok := loaded.Lookup("greeting")
	if !ok || value.(*graph.String).Value() != "replacement!" {
		t.Fatal("dynamic snapshot scalar read failed")
	}

	child, ok := loaded.Lookup("nested")
	if !ok {
		t.Fatal("dynamic snapshot nested read failed")
	}

	text, ok := child.(*graph.Composite).Lookup("text")
	if !ok || text.(*graph.String).Value() != "before after" {
		t.Fatal("nested snapshot read failed")
	}
}

func TestStandaloneCollectionsComposeThroughValue(t *testing.T) {
	list := graph.NewList(graph.StringCodec, "one")
	m := graph.NewMap(graph.StringCodec).Set("locale", "en")
	set := graph.NewStringSet("active")

	a := model.NewAccount()
	if err := a.SetMessages(list); err != nil {
		t.Fatal(err)
	}

	if err := a.SetAttributes(m); err != nil {
		t.Fatal(err)
	}

	if err := a.SetTags(set); err != nil {
		t.Fatal(err)
	}

	list.Append("two")
	a.Messages().Append("three")
	m.Set("locale", "nl")
	set.Add("new")

	if a.Messages() != list || a.Attributes() != m || a.Tags() != set {
		t.Fatal("collection identity lost")
	}

	if !slices.Equal(list.Values(), []string{"one", "two", "three"}) || !set.Contains("new") {
		t.Fatal("collection mutations detached")
	}

	if value, _ := a.Attributes().Get("locale"); value != "nl" {
		t.Fatal("map mutations detached")
	}
}

func TestLegacyBuildersRemainLiveWhenComposed(t *testing.T) {
	root := graph.NewComposite()
	s := graph.ScalarString("Hello").AsEffect()
	m := (&graph.MapEffect[*graph.IntEffect]{}).Set("count", graph.ScalarInt(1).AsEffect())

	l := (&graph.ListEffect[*graph.StringEffect]{}).Append(graph.ScalarString("first").AsEffect())
	for name, value := range map[string]graph.Value{"text": s, "map": m, "list": l} {
		if err := root.Set(name, value); err != nil {
			t.Fatal(err)
		}
	}

	s.Append(" world")
	m.Set("count", (&graph.IntEffect{}).Add(2))
	l.Append(graph.ScalarString("second").AsEffect()).Set(graph.ScalarString("FIRST").AsEffect(), 0)

	if s.Value() != "Hello world" {
		t.Fatal("legacy scalar bridge lost local edits")
	}

	count, err := m.Get("count", graph.IntFromElement)
	if err != nil || count.Value() != 3 {
		t.Fatalf("legacy map bridge: %v", err)
	}

	list := l.Runtime().Snapshot().GetOrderedElements()
	if len(list) != 2 || string(list[0].GetData().Decompress()) != "FIRST" || string(list[1].GetData().Decompress()) != "second" {
		t.Fatal("legacy list bridge lost element identity or updates")
	}
}

func TestGeneratedGenericsAndHandwrittenValues(t *testing.T) {
	p := personType.New()
	p.Rename("Rob")
	name := p.Name()
	box := model.NewBox(personType)
	// A prior read should not prevent composing a standalone replacement.
	_ = box.Value()
	if err := box.SetValue(p); err != nil {
		t.Fatal(err)
	}

	if box.Value() != p {
		t.Fatal("generic value identity lost")
	}

	p.Rename("Robert")

	if name.Value() != "Robert" || box.Value().Name() != name {
		t.Fatal("handwritten value and aliases diverged")
	}

	catalog := model.NewCatalog()
	catalog.Title().Value().Set("Title").Append("!")
	catalog.Account().Value().SetName("Account")

	pair := model.NewPair(graph.StringType, model.AccountType)
	if err := pair.SetFirst(graph.NewString("first")); err != nil {
		t.Fatal(err)
	}

	if err := pair.SetSecond(catalog.Account().Value()); err == nil {
		t.Fatal("attached value should not be silently copied")
	}

	pair.Second().SetName("second")

	if pair.First().Value() != "first" || pair.Second().Name() != "second" || catalog.Title().Value().Value() != "Title!" {
		t.Fatal("logical/runtime generic mapping failed")
	}

	loaded, err := model.BoxType(personType).FromSnapshot(box.Snapshot())
	if err != nil || loaded.Value().Name().Value() != "Robert" {
		t.Fatalf("generic snapshot load: %v", err)
	}

	link := model.NewLink(graph.StringType)
	link.Value().Set("first")
	link.Next().Value().Set("second")
	link.Next().Next().Value().Set("third")

	reloaded, err := model.LinkType(graph.StringType).FromSnapshot(link.Snapshot())
	if err != nil || reloaded.Next().Next().Value().Value() != "third" {
		t.Fatalf("recursive generic load: %v", err)
	}

	listBox := model.NewBox(graph.ListType(graph.StringCodec))
	listBox.Value().Append("one", "two")

	listReloaded, err := model.BoxType(graph.ListType(graph.StringCodec)).FromSnapshot(listBox.Snapshot())
	if err != nil || !slices.Equal(listReloaded.Value().Values(), []string{"one", "two"}) {
		t.Fatalf("collection type argument: %v", err)
	}
}

func TestStandaloneStringIntoNode(t *testing.T) {
	e := effects.NewTestEngine()

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})

	c := e.NewContext()
	defer c.Abort()

	if err := graph.CreateGraph("tree", "test").Flush(c); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	write := e.NewContext()
	defer write.Abort()

	desc, tips, err := write.GetSnapshot("tree")
	if err != nil {
		t.Fatal(err)
	}

	g, err := graph.GraphFromSnapshot("tree", desc, tips)
	if err != nil {
		t.Fatal(err)
	}

	s := graph.NewString("Hello")
	s.Append(" world")
	node := g.CreateNode("greeting", s)
	node.Value().Append("!")

	if node.Value() != s || s.Value() != "Hello world!" {
		t.Fatal("node did not retain the standalone string")
	}

	p := personType.New()
	p.Rename("Rob")
	personNode := g.CreateNode("person", p)
	personNode.Value().Name().Append("!")

	boxed := model.NewBox(personType)
	q := personType.New()
	q.Rename("Nested")

	if err := boxed.SetValue(q); err != nil {
		t.Fatal(err)
	}

	genericNode := g.CreateNode("generic", boxed)
	if err := g.Flush(write); err != nil {
		t.Fatal(err)
	}

	if err := write.Flush(); err != nil {
		t.Fatal(err)
	}

	read := e.NewContext()
	defer read.Abort()

	desc, tips, err = read.GetSnapshot("tree")
	if err != nil {
		t.Fatal(err)
	}

	g, err = graph.GraphFromSnapshot("tree", desc, tips)
	if err != nil {
		t.Fatal(err)
	}

	loaded, err := g.GetNode(string(node.Address()), read, graph.StringType)
	if err != nil || loaded.Value().Value() != "Hello world!" {
		t.Fatalf("string node round trip: %v", err)
	}

	handwritten, err := g.GetNode(string(personNode.Address()), read, personType)
	if err != nil || handwritten.Value().Name().Value() != "Rob!" {
		t.Fatalf("handwritten node round trip: %v", err)
	}

	generic, err := g.GetNode(string(genericNode.Address()), read, model.BoxType(personType))
	if err != nil || generic.Value().Value().Name().Value() != "Nested" {
		t.Fatalf("generic handwritten node round trip: %v", err)
	}

	loaded.Value().Append(" again")
	handwritten.Value().Rename("Updated")
	generic.Value().Value().Rename("Updated nested")

	if err := g.Flush(read); err != nil {
		t.Fatal(err)
	}

	if err := read.Flush(); err != nil {
		t.Fatal(err)
	}

	final := e.NewContext()
	defer final.Abort()

	snap, _, err := final.GetSnapshotInternal(string(node.Address()))
	if err != nil {
		t.Fatal(err)
	}

	if string(graph.FieldSnapshot(snap, "value").GetScalar().Decompress()) != "Hello world! again" {
		t.Fatal("loaded string mutation lost")
	}

	snap, _, err = final.GetSnapshotInternal(string(genericNode.Address()))
	if err != nil {
		t.Fatal(err)
	}

	if string(snap.GetPartitions()["value/value"].GetNetAdds()["name"].GetData().Decompress()) != "Updated nested" {
		t.Fatal("generic partition mapping failed")
	}
}

func TestLoadedDynamicValueDoesNotEmitSnapshot(t *testing.T) {
	seed := &pb.ReducedEffect{Scalar: &pb.DataEffect{Value: &pb.DataEffect_Raw{Raw: []byte("seed")}}}

	s, err := graph.StringType.FromSnapshot(seed)
	if err != nil {
		t.Fatal(err)
	}

	root := graph.NewComposite()
	if err := root.Set("text", s); err != nil {
		t.Fatal(err)
	}

	if s.Value() != "seed" {
		t.Fatal("composition lost loaded base")
	}

	e := effects.NewTestEngine()

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})

	c := e.NewContext()
	defer c.Abort()

	if err := root.Emit("key", c, nil); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	read := e.NewContext()
	defer read.Abort()

	snap, _, err := read.GetSnapshot("key")
	if err != nil {
		t.Fatal(err)
	}

	if snap != nil {
		t.Fatal("composition emitted the borrowed seed")
	}
}
