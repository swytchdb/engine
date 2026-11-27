package graph

import (
	"math"
	"slices"
	"strings"
	"sync"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"google.golang.org/protobuf/proto"
)

func TestStringEffectValueAndReduction(t *testing.T) {
	tests := []struct {
		name  string
		build func() *StringEffect
		want  string
	}{
		{"zero", func() *StringEffect { return &StringEffect{} }, ""},
		{"replacement", func() *StringEffect { return ScalarString("abc").AsEffect() }, "abc"},
		{"append", func() *StringEffect { return ScalarString("a").AsEffect().Append("b").Append("c") }, "abc"},
		{"prepend", func() *StringEffect { return ScalarString("c").AsEffect().Prepend("b").Prepend("a") }, "abc"},
		{"mixed", func() *StringEffect { return ScalarString("b").AsEffect().Append("d").Prepend("a").Insert("c", 2) }, "abcd"},
		{"insert boundaries", func() *StringEffect { return ScalarString("b").AsEffect().Insert("a", 0).Insert("c", 2) }, "abc"},
		{"insert after", func() *StringEffect {
			s := ScalarString("ac").AsEffect().Insert("b", 0)
			s.inner[1].GetData().Placement = pb.Placement_PLACE_AFTER

			return s
		}, "abc"},
		{"empty operands", func() *StringEffect { return ScalarString("a").AsEffect().Append("").Prepend("").Insert("", 1) }, "a"},
		{"empty base", func() *StringEffect { return (&StringEffect{}).Insert("b", 0).Prepend("a").Append("c") }, "abc"},
		{"binary", func() *StringEffect { return ScalarString("\x00\xff").AsEffect().Insert("\x80", 1).Append("\x00") }, "\x00\x80\xff\x00"},
		{"byte offset", func() *StringEffect { return ScalarString("é!").AsEffect().Insert("x", 1) }, "\xc3x\xa9!"},
		{"replace discards pending", func() *StringEffect {
			return ScalarString("old").AsEffect().Append("discard").Replace("new").Append("!")
		}, "new!"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := tt.build()
			for range 2 {
				if got := s.Value(); got != tt.want {
					t.Fatalf("Value() = %q, want %q", got, tt.want)
				}
			}

			if got := string(effects.ReduceChain(s.base, s.inner).GetScalar().Decompress()); got != tt.want {
				t.Fatalf("engine reduction = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestStringEffectInsertInvalidOffset(t *testing.T) {
	for _, at := range []int{-1, 4} {
		// Building is lazy: bounds are checked when the value is evaluated.
		s := ScalarString("abc").AsEffect().Insert("x", at)

		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("Value() with offset %d did not panic", at)
				}
			}()

			_ = s.Value()
		}()
	}
}

func TestStringFromSnap(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		name := "raw"
		if compressed {
			name = "compressed"
		}

		t.Run(name, func(t *testing.T) {
			baseValue := strings.Repeat("seed", 64)

			seed := ScalarString(baseValue).AsEffect().inner[0]
			if compressed {
				effects.CompressEffectValues(seed)

				if seed.GetData().GetCompressed() == nil {
					t.Fatal("test seed was not compressed")
				}
			}

			snap := effects.ReduceBranch([]*pb.Effect{seed})
			before := proto.Clone(snap)

			s, err := StringFromSnap(snap)
			if err != nil {
				t.Fatal(err)
			}

			if s.base != snap || len(s.inner) != 0 {
				t.Fatal("snapshot must be borrowed as the base, with no pending effects")
			}

			s = s.Append("!").Prepend("[").Insert("]", len(baseValue)+2)

			want := "[" + baseValue + "!]"
			for range 2 {
				if got := s.Value(); got != want {
					t.Fatalf("Value() = %q, want %q", got, want)
				}
			}

			if got := string(effects.ReduceChain(snap, s.inner).GetScalar().Decompress()); got != want {
				t.Fatalf("engine reduction = %q, want %q", got, want)
			}

			if !proto.Equal(snap, before) {
				t.Fatal("evaluating the builder mutated the snapshot")
			}

			replaced := s.Replace("fresh")
			if replaced.base != nil || len(replaced.inner) != 1 || replaced.Value() != "fresh" {
				t.Fatal("Replace retained the base or pending mutations")
			}
		})
	}

	missing, err := StringFromSnap(nil)
	if err != nil || missing.Value() != "" || missing.Append("new").Value() != "new" {
		t.Fatalf("nil snapshot did not seed an empty string: %v", err)
	}
}

func TestScalarFromElementBorrowsData(t *testing.T) {
	for _, element := range []*pb.ReducedElement{nil, {}, {
		Data: &pb.DataEffect{Id: []byte("key"), Value: &pb.DataEffect_Raw{Raw: []byte("seed")}},
	}} {
		before := proto.Clone(element)

		s, err := StringFromElement(element)
		if err != nil || s.base.GetScalar() != element.GetData() || len(s.inner) != 0 {
			t.Fatalf("string decoder did not borrow data without effects: %v", err)
		}

		i, err := IntFromElement(element)
		if err != nil || i.base.GetScalar() != element.GetData() || len(i.inner) != 0 {
			t.Fatalf("integer decoder did not borrow data without effects: %v", err)
		}

		f, err := FloatFromElement(element)
		if err != nil || f.base.GetScalar() != element.GetData() || len(f.inner) != 0 {
			t.Fatalf("float decoder did not borrow data without effects: %v", err)
		}

		want := string(element.GetData().Decompress())
		if s.Append("!").Value() != want+"!" || i.Add(2).Value() != 2 || f.Add(0.5).Value() != 0.5 {
			t.Fatal("decoded builders did not start from the expected values")
		}

		if !proto.Equal(element, before) {
			t.Fatal("decoded builders mutated the borrowed element")
		}
	}
}

func TestIntEffectValueAndReduction(t *testing.T) {
	seed := effects.ReduceBranch(ScalarInt(10).AsEffect().inner)
	before := proto.Clone(seed)

	fromSnap, err := IntFromSnap(seed)
	if err != nil {
		t.Fatal(err)
	}

	if fromSnap.base != seed || len(fromSnap.inner) != 0 {
		t.Fatal("snapshot must be borrowed as the base, with no pending effects")
	}

	missing, err := IntFromSnap(nil)
	if err != nil {
		t.Fatal(err)
	}

	overflowSeed := &pb.ReducedEffect{Scalar: &pb.DataEffect{Value: &pb.DataEffect_IntVal{IntVal: math.MaxInt64}}}

	overflow, err := IntFromSnap(overflowSeed)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		s    *IntEffect
		want int64
	}{
		{"zero", &IntEffect{}, 0},
		{"replacement", ScalarInt(-10).AsEffect(), -10},
		{"deltas", ScalarInt(10).AsEffect().Add(4).Add(-3).Add(0), 11},
		{"snapshot", fromSnap.Add(4).Add(-3), 11},
		{"missing", missing.Add(2).Add(-3), -1},
		{"overflow", overflow.Add(1), math.MinInt64},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for range 2 {
				if got := tt.s.Value(); got != tt.want {
					t.Fatalf("Value() = %d, want %d", got, tt.want)
				}
			}

			if got := effects.ReduceChain(tt.s.base, tt.s.inner).GetScalar().GetIntVal(); got != tt.want {
				t.Fatalf("engine reduction = %d, want %d", got, tt.want)
			}
		})
	}

	if !proto.Equal(seed, before) {
		t.Fatal("evaluating the builder mutated the snapshot")
	}
}

func TestFloatEffectValueAndReduction(t *testing.T) {
	seed := effects.ReduceBranch(ScalarFloat(10.5).AsEffect().inner)
	before := proto.Clone(seed)

	fromSnap, err := FloatFromSnap(seed)
	if err != nil {
		t.Fatal(err)
	}

	if fromSnap.base != seed || len(fromSnap.inner) != 0 {
		t.Fatal("snapshot must be borrowed as the base, with no pending effects")
	}

	missing, err := FloatFromSnap(nil)
	if err != nil {
		t.Fatal(err)
	}

	if missing.Value() != 0 {
		t.Fatal("nil snapshot did not seed zero")
	}

	tests := []struct {
		name string
		s    *FloatEffect
		want float64
	}{
		{"zero", &FloatEffect{}, 0},
		{"replacement", ScalarFloat(-10.5).AsEffect(), -10.5},
		{"negative zero", ScalarFloat(math.Copysign(0, -1)).AsEffect(), math.Copysign(0, -1)},
		{"deltas", ScalarFloat(10.5).AsEffect().Add(4.25).Add(-3.5).Add(0), 11.25},
		{"snapshot", fromSnap.Add(4.25).Add(-3.5), 11.25},
		{"missing", missing.Add(2.5).Add(-3.25), -0.75},
		{"rounding order", ScalarFloat(1e16).AsEffect().Add(-1e16).Add(1), 1},
		{"rounding loss", ScalarFloat(1e16).AsEffect().Add(1).Add(-1e16), 0},
		{"overflow", ScalarFloat(math.MaxFloat64).AsEffect().Add(math.MaxFloat64), math.Inf(1)},
		{"negative infinity", ScalarFloat(math.Inf(-1)).AsEffect().Add(1), math.Inf(-1)},
		{"nan delta", ScalarFloat(1).AsEffect().Add(math.NaN()), math.NaN()},
		{"nan result", ScalarFloat(math.Inf(1)).AsEffect().Add(math.Inf(-1)), math.NaN()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for range 2 {
				got := tt.s.Value()
				if !(math.IsNaN(got) && math.IsNaN(tt.want)) && math.Float64bits(got) != math.Float64bits(tt.want) {
					t.Fatalf("Value() = %g, want %g", got, tt.want)
				}
			}

			got := effects.ReduceChain(tt.s.base, tt.s.inner).GetScalar().GetFloatVal()
			if !(math.IsNaN(got) && math.IsNaN(tt.want)) && math.Float64bits(got) != math.Float64bits(tt.want) {
				t.Fatalf("engine reduction = %g, want %g", got, tt.want)
			}
		})
	}

	if !proto.Equal(seed, before) {
		t.Fatal("evaluating the builder mutated the snapshot")
	}
}

func TestScalarBuilderAliasing(t *testing.T) {
	// Pointer aliases share one evolving builder; AsEffect creates independent messages.
	t.Run("string", func(t *testing.T) {
		base := ScalarString("base").AsEffect()
		base.inner = append(make([]*pb.Effect, 0, 4), base.inner...)
		left := base.Append("left")
		_ = base.Append("right")

		if got := left.Value(); got != "baseleftright" {
			t.Fatalf("expected documented shared-storage behavior, got %q", got)
		}

		independent := ScalarString("base").AsEffect().Append("left")
		if independent.Value() != "baseleft" || independent.inner[0] == base.inner[0] {
			t.Fatal("AsEffect did not create independent messages")
		}
	})
	t.Run("integer", func(t *testing.T) {
		base := ScalarInt(10).AsEffect()
		base.inner = append(make([]*pb.Effect, 0, 4), base.inner...)
		left := base.Add(1)
		_ = base.Add(2)

		if got := left.Value(); got != 13 {
			t.Fatalf("expected documented shared-storage behavior, got %d", got)
		}

		independent := ScalarInt(10).AsEffect().Add(1)
		if independent.Value() != 11 || independent.inner[0] == base.inner[0] {
			t.Fatal("AsEffect did not create independent messages")
		}
	})
	t.Run("float", func(t *testing.T) {
		base := ScalarFloat(10.5).AsEffect()
		base.inner = append(make([]*pb.Effect, 0, 4), base.inner...)
		left := base.Add(0.25)
		_ = base.Add(0.5)

		if got := left.Value(); got != 11.25 {
			t.Fatalf("expected documented shared-storage behavior, got %g", got)
		}

		independent := ScalarFloat(10.5).AsEffect().Add(0.25)
		if independent.Value() != 10.75 || independent.inner[0] == base.inner[0] {
			t.Fatal("AsEffect did not create independent messages")
		}
	})
}

func scalarTestEngine(t *testing.T) *effects.Engine {
	t.Helper()

	e := effects.NewTestEngine()

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})

	return e
}

func scalarTestDependsOn(t *testing.T, e *effects.Engine, deps []effects.Tip, want effects.Tip) bool {
	t.Helper()

	pending := slices.Clone(deps)
	seen := make(map[effects.Tip]bool)

	for len(pending) > 0 {
		tip := pending[len(pending)-1]
		pending = pending[:len(pending)-1]

		if tip == want {
			return true
		}

		if seen[tip] {
			continue
		}

		seen[tip] = true

		eff, ok := e.EffectCache().Get(tip)
		if !ok {
			t.Fatalf("missing dependency %v", tip)
		}

		for _, dep := range eff.GetDeps() {
			pending = append(pending, effects.Tip{dep.GetNodeId(), dep.GetOffset()})
		}
	}

	return false
}

func TestScalarEmissionsReuseSnapshotTips(t *testing.T) {
	for _, seeded := range []bool{false, true} {
		for _, transactional := range []bool{false, true} {
			name := "missing"
			if seeded {
				name = "seeded"
			}

			if transactional {
				name += "/transaction"
			} else {
				name += "/context"
			}

			t.Run(name, func(t *testing.T) {
				e := scalarTestEngine(t)
				stringKey := "scalar-test:text"
				intKey := "scalar-test:number"
				floatKey := "scalar-test:float"
				initialString, initialInt := "", int64(0)
				initialFloat := float64(0)

				if seeded {
					seed := e.NewContext()
					defer seed.Abort()

					if err := ScalarString("seed").AsEffect().Emit(stringKey, seed, nil); err != nil {
						t.Fatal(err)
					}

					if err := ScalarInt(10).AsEffect().Emit(intKey, seed, nil); err != nil {
						t.Fatal(err)
					}

					if err := ScalarFloat(10.5).AsEffect().Emit(floatKey, seed, nil); err != nil {
						t.Fatal(err)
					}

					if err := seed.Flush(); err != nil {
						t.Fatal(err)
					}

					initialString, initialInt = "seed", 10
					initialFloat = 10.5
				}

				c := e.NewContext()
				defer c.Abort()

				if transactional {
					c.BeginTx()
				}

				stringSnap, stringTips, err := c.GetSnapshot(stringKey)
				if err != nil {
					t.Fatal(err)
				}

				intSnap, intTips, err := c.GetSnapshot(intKey)
				if err != nil {
					t.Fatal(err)
				}

				floatSnap, floatTips, err := c.GetSnapshot(floatKey)
				if err != nil {
					t.Fatal(err)
				}

				originalStringTips, originalIntTips := slices.Clone(stringTips), slices.Clone(intTips)

				originalFloatTips := slices.Clone(floatTips)
				if seeded && (len(stringTips) == 0 || len(intTips) == 0 || len(floatTips) == 0) {
					t.Fatal("seeded reads returned no tips")
				}

				s, err := StringFromSnap(stringSnap)
				if err != nil {
					t.Fatal(err)
				}

				n, err := IntFromSnap(intSnap)
				if err != nil {
					t.Fatal(err)
				}

				f, err := FloatFromSnap(floatSnap)
				if err != nil {
					t.Fatal(err)
				}

				type emission struct {
					tip  effects.Tip
					deps []effects.Tip
				}

				var mu sync.Mutex

				emitted := make(map[string][]emission)

				e.SetOnLocalEffect(func(tip effects.Tip, eff *pb.Effect) {
					if eff.GetData() == nil {
						return
					}

					entry := emission{tip: tip}
					for _, dep := range eff.GetDeps() {
						entry.deps = append(entry.deps, effects.Tip{dep.GetNodeId(), dep.GetOffset()})
					}

					mu.Lock()

					emitted[string(eff.GetKey())] = append(emitted[string(eff.GetKey())], entry)
					mu.Unlock()
				})
				defer e.SetOnLocalEffect(nil)
				// Reuse each key's original tips both within a builder's Emit loop
				// and across distinct builders. Interleave keys to verify that the
				// context chains each key independently. Never reuse emitted builders.
				if err := s.Append("A").Prepend("(").Insert("_", 1).Emit(stringKey, c, stringTips); err != nil {
					t.Fatal(err)
				}

				if err := n.Add(3).Add(-1).Emit(intKey, c, intTips); err != nil {
					t.Fatal(err)
				}

				if err := f.Add(0.5).Add(-0.25).Emit(floatKey, c, floatTips); err != nil {
					t.Fatal(err)
				}

				if err := (&StringEffect{}).Append("B").Emit(stringKey, c, stringTips); err != nil {
					t.Fatal(err)
				}

				if err := (&IntEffect{}).Add(4).Emit(intKey, c, intTips); err != nil {
					t.Fatal(err)
				}

				if err := (&FloatEffect{}).Add(1.25).Emit(floatKey, c, floatTips); err != nil {
					t.Fatal(err)
				}

				if err := (&StringEffect{}).Append(")").Prepend("[").Append("]").Emit(stringKey, c, stringTips); err != nil {
					t.Fatal(err)
				}

				if err := (&IntEffect{}).Add(-2).Emit(intKey, c, intTips); err != nil {
					t.Fatal(err)
				}

				if err := (&FloatEffect{}).Add(-0.5).Emit(floatKey, c, floatTips); err != nil {
					t.Fatal(err)
				}

				e.SetOnLocalEffect(nil)

				for _, check := range []struct {
					key   string
					tips  []effects.Tip
					count int
				}{
					{stringKey, originalStringTips, 7},
					{intKey, originalIntTips, 4},
					{floatKey, originalFloatTips, 4},
				} {
					chain := emitted[check.key]
					if len(chain) != check.count {
						t.Fatalf("%q: emitted %d data effects, want %d", check.key, len(chain), check.count)
					}

					for _, tip := range check.tips {
						// Transactional reads can insert an observation effect between
						// the snapshot tips and the first write; ancestry must survive.
						if !scalarTestDependsOn(t, e, chain[0].deps, tip) {
							t.Errorf("%q: first emission does not depend on observed tip %v: %v", check.key, tip, chain[0].deps)
						}
					}

					for i := 1; i < len(chain); i++ {
						if !slices.Equal(chain[i].deps, []effects.Tip{chain[i-1].tip}) {
							t.Errorf("%q emission %d: deps = %v, want previous emission %v", check.key, i, chain[i].deps, chain[i-1].tip)
						}
					}
				}

				if !slices.Equal(stringTips, originalStringTips) || !slices.Equal(intTips, originalIntTips) || !slices.Equal(floatTips, originalFloatTips) {
					t.Fatal("Emit mutated the supplied tips")
				}

				if err := c.Flush(); err != nil {
					t.Fatal(err)
				}

				reader := e.NewContext()
				defer reader.Abort()

				text, _, err := reader.GetSnapshot(stringKey)
				if err != nil {
					t.Fatal(err)
				}

				if got, want := string(text.GetScalar().Decompress()), "[(_"+initialString+"AB)]"; got != want {
					t.Fatalf("reconstructed string = %q, want %q", got, want)
				}

				number, _, err := reader.GetSnapshot(intKey)
				if err != nil {
					t.Fatal(err)
				}

				if got, want := number.GetScalar().GetIntVal(), initialInt+4; got != want {
					t.Fatalf("reconstructed integer = %d, want %d", got, want)
				}

				floating, _, err := reader.GetSnapshot(floatKey)
				if err != nil {
					t.Fatal(err)
				}

				if got, want := floating.GetScalar().GetFloatVal(), initialFloat+1; got != want {
					t.Fatalf("reconstructed float = %g, want %g", got, want)
				}
			})
		}
	}
}

func TestScalarValuesCreateFreshBuildersForRepeatedEmissions(t *testing.T) {
	e := scalarTestEngine(t)

	c := e.NewContext()
	defer c.Abort()

	s, n := ScalarString("reusable"), ScalarInt(42)
	f := ScalarFloat(42.5)

	for _, address := range []string{"first", "second"} {
		if err := s.AsEffect().Emit("text:"+address, c, nil); err != nil {
			t.Fatal(err)
		}

		if err := n.AsEffect().Emit("number:"+address, c, nil); err != nil {
			t.Fatal(err)
		}

		if err := f.AsEffect().Emit("float:"+address, c, nil); err != nil {
			t.Fatal(err)
		}

		if err := c.Flush(); err != nil {
			t.Fatal(err)
		}
	}

	reader := e.NewContext()
	defer reader.Abort()

	for _, address := range []string{"first", "second"} {
		text, _, err := reader.GetSnapshot("text:" + address)
		if err != nil {
			t.Fatal(err)
		}

		if got := string(text.GetScalar().Decompress()); got != s.Value() {
			t.Fatalf("%s: string = %q, want %q", address, got, s.Value())
		}

		number, _, err := reader.GetSnapshot("number:" + address)
		if err != nil {
			t.Fatal(err)
		}

		if got := number.GetScalar().GetIntVal(); got != n.Value() {
			t.Fatalf("%s: integer = %d, want %d", address, got, n.Value())
		}

		floating, _, err := reader.GetSnapshot("float:" + address)
		if err != nil {
			t.Fatal(err)
		}

		if got := floating.GetScalar().GetFloatVal(); got != f.Value() {
			t.Fatalf("%s: float = %g, want %g", address, got, f.Value())
		}
	}
}
