package graph

import (
	"slices"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"google.golang.org/protobuf/proto"
)

func stringEffects(values ...string) []*StringEffect {
	out := make([]*StringEffect, len(values))
	for i, value := range values {
		out[i] = ScalarString(value).AsEffect()
	}

	return out
}

func listStrings(s *ListEffect[*StringEffect]) []string {
	elements := s.elements()
	if len(elements) == 0 {
		return nil
	}

	out := make([]string, 0, len(elements))
	for _, elem := range elements {
		out = append(out, string(elem.GetData().Decompress()))
	}

	return out
}

func TestListComposition(t *testing.T) {
	b := (&ListEffect[*StringEffect]{}).Append(stringEffects("b", "d")...).Prepend(stringEffects("first", "a")...)
	b = b.Insert(ScalarString("c").AsEffect(), 3).Set(ScalarString("D").AsEffect(), 4).Remove(0)

	b = b.Insert(ScalarString("last").AsEffect(), -1)
	if got := listStrings(b); !slices.Equal(got, []string{"a", "b", "c", "D", "last"}) {
		t.Fatalf("list = %q", got)
	}

	before := proto.Clone(effects.ReduceChain(b.base, b.inner))

	_ = b.elements()
	if !proto.Equal(before, effects.ReduceChain(b.base, b.inner)) {
		t.Fatal("reading changed list")
	}
}

// Composed SELF effects must apply their merge rules to the existing element.
func TestListComposesElementUpdates(t *testing.T) {
	b := (&ListEffect[*IntEffect]{}).Append(ScalarInt(10).AsEffect(), ScalarInt(10).AsEffect())

	first, second := b.elements()[0].GetData().GetId(), b.elements()[1].GetData().GetId()
	if slices.Equal(first, second) {
		t.Fatal("equal values need distinct element identities")
	}

	delta := (&IntEffect{}).Add(3).Add(-1)
	messages := slices.Clone(delta.getEffects())

	b = b.Set(delta, 0)
	for i, message := range messages {
		if b.inner[i+2] != message || !slices.Equal(message.GetData().GetId(), first) || message.GetData().GetMerge() != pb.MergeRule_ADDITIVE_INT {
			t.Fatal("composition lost message identity, target ID, or merge rule")
		}
	}

	elems := b.elements()
	if elems[0].GetData().GetIntVal() != 12 || elems[1].GetData().GetIntVal() != 10 {
		t.Fatalf("elements = %v", elems)
	}

	b = b.Remove(0)
	if elems := b.elements(); len(elems) != 1 || !slices.Equal(elems[0].GetData().GetId(), second) {
		t.Fatal("remove targeted the wrong duplicate")
	}
}

func TestListCompositionUsesPendingEffectsOnly(t *testing.T) {
	inputBase := effects.ReduceChain(nil, ScalarInt(100).AsEffect().getEffects())
	input, _ := IntFromSnap(inputBase)

	b := (&ListEffect[*IntEffect]{}).Append(input)
	if len(b.getEffects()) != 0 {
		t.Fatal("composition emitted a snapshot")
	}

	b = b.Append(input.Add(2).Add(3))

	elems := b.elements()
	if len(elems) != 2 || elems[0].GetData().GetIntVal() != 2 || elems[1].GetData().GetIntVal() != 3 {
		t.Fatalf("each inserted effect should have its own ID: %v", elems)
	}
}

func TestListFromExistingSnapshot(t *testing.T) {
	seed := &pb.ReducedEffect{Collection: pb.CollectionKind_ORDERED, OrderedElements: []*pb.ReducedElement{
		{Data: &pb.DataEffect{Id: []byte("external-a"), Value: &pb.DataEffect_Raw{Raw: []byte("a")}}},
		{Data: &pb.DataEffect{Id: []byte("external-b"), Value: &pb.DataEffect_Raw{Raw: []byte("b")}}},
	}}
	before := proto.Clone(seed)

	b, err := ListFromSnap[*StringEffect](seed)
	if err != nil || b.base != seed || len(b.getEffects()) != 0 {
		t.Fatalf("seed: %v", err)
	}

	b = b.Insert(ScalarString("middle").AsEffect(), 1).Remove(0)
	if got := listStrings(b); !slices.Equal(got, []string{"middle", "b"}) {
		t.Fatalf("list = %q", got)
	}

	if !slices.Equal(b.inner[0].GetData().GetReference(), []byte("external-b")) {
		t.Fatal("insert lost external element ID")
	}

	b = b.Replace(stringEffects("new"))
	if b.base != seed || len(b.inner) != 4 || !slices.Equal(listStrings(b), []string{"new"}) {
		t.Fatal("replacement must retain the history and reset it through an effect")
	}

	if !proto.Equal(seed, before) {
		t.Fatal("modified borrowed snapshot")
	}

	b = b.Replace(nil)
	if len(b.elements()) != 0 {
		t.Fatal("empty replacement did not remove list")
	}

	for _, empty := range []*pb.ReducedEffect{nil, {Op: pb.EffectOp_REMOVE_OP}} {
		b, err := ListFromSnap[*StringEffect](empty)
		if err != nil || len(b.elements()) != 0 || len(b.inner) != 0 {
			t.Fatalf("empty seed: %v", err)
		}
	}
}

func TestListBounds(t *testing.T) {
	for name, edit := range map[string]func(*ListEffect[*StringEffect]){
		"negative insert": func(b *ListEffect[*StringEffect]) { b.Insert(ScalarString("x").AsEffect(), -2) },
		"past-end insert": func(b *ListEffect[*StringEffect]) { b.Insert(ScalarString("x").AsEffect(), 2) },
		"negative set":    func(b *ListEffect[*StringEffect]) { b.Set(ScalarString("x").AsEffect(), -1) },
		"past-end set":    func(b *ListEffect[*StringEffect]) { b.Set(ScalarString("x").AsEffect(), 1) },
		"negative remove": func(b *ListEffect[*StringEffect]) { b.Remove(-1) },
		"past-end remove": func(b *ListEffect[*StringEffect]) { b.Remove(1) },
	} {
		t.Run(name, func(t *testing.T) {
			b := (&ListEffect[*StringEffect]{}).Append(ScalarString("a").AsEffect())

			defer func() {
				if recover() == nil {
					t.Fatal("invalid index did not panic")
				}

				if !slices.Equal(listStrings(b), []string{"a"}) {
					t.Fatal("failed edit changed original list")
				}
			}()

			edit(b)
		})
	}
}

func TestListBuilderAliasing(t *testing.T) {
	base := (&ListEffect[*StringEffect]{}).Append(ScalarString("base").AsEffect())
	base.inner = append(make([]*pb.Effect, 0, 8), base.inner...)
	left := base.Append(ScalarString("left").AsEffect())
	_ = base.Append(ScalarString("right").AsEffect())

	if !slices.Equal(listStrings(left), []string{"base", "left", "right"}) {
		t.Fatal("expected documented shared slice storage")
	}
}

func TestListConcurrentAppendsStayContiguous(t *testing.T) {
	e := scalarTestEngine(t)
	key := "lists:concurrent"

	seed := e.NewContext()
	defer seed.Abort()

	if err := (&ListEffect[*StringEffect]{}).Append(ScalarString("base").AsEffect()).Emit(key, seed, nil); err != nil {
		t.Fatal(err)
	}

	if err := seed.Flush(); err != nil {
		t.Fatal(err)
	}

	_, tips, err := seed.GetSnapshot(key)
	if err != nil {
		t.Fatal(err)
	}

	for _, values := range [][]string{{"a1", "a2"}, {"b1", "b2"}} {
		writer := e.NewContext()
		defer writer.Abort()

		if err := (&ListEffect[*StringEffect]{}).Append(stringEffects(values...)...).Emit(key, writer, tips); err != nil {
			t.Fatal(err)
		}

		if err := writer.Flush(); err != nil {
			t.Fatal(err)
		}
	}

	reader := e.NewContext()
	defer reader.Abort()

	snap, _, err := reader.GetSnapshot(key)
	if err != nil {
		t.Fatal(err)
	}

	b, _ := ListFromSnap[*StringEffect](snap)

	got := listStrings(b)
	if !slices.Equal(got, []string{"base", "a1", "a2", "b1", "b2"}) && !slices.Equal(got, []string{"base", "b1", "b2", "a1", "a2"}) {
		t.Fatalf("interleaved or missing append blocks: %q", got)
	}
}
