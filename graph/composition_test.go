package graph

import (
	"fmt"
	"maps"
	"slices"
	"sync"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"google.golang.org/protobuf/proto"
)

var (
	_ Emits = &StringEffect{}
	_ Emits = &IntEffect{}
	_ Emits = &FloatEffect{}
	_ Emits = &ListEffect[*StringEffect]{}
	_ Emits = &MapEffect[*IntEffect]{}
)

func TestComposedEmissionsReuseSnapshotTips(t *testing.T) {
	cases := []struct {
		name    string
		seed    func() Emits
		batches func(*pb.ReducedEffect) []Emits
		check   func(*testing.T, *pb.ReducedEffect, bool)
	}{
		{
			"string", func() Emits { return ScalarString("seed").AsEffect() },
			func(s *pb.ReducedEffect) []Emits {
				b, _ := StringFromSnap(s)

				return []Emits{b.Append("a"), (&StringEffect{}).Append("b"), (&StringEffect{}).Prepend("!")}
			},
			func(t *testing.T, s *pb.ReducedEffect, seeded bool) {
				want := "!ab"
				if seeded {
					want = "!seedab"
				}

				if got := string(s.GetScalar().Decompress()); got != want {
					t.Fatalf("string = %q, want %q", got, want)
				}
			},
		},
		{
			"int", func() Emits { return ScalarInt(10).AsEffect() },
			func(s *pb.ReducedEffect) []Emits {
				b, _ := IntFromSnap(s)

				return []Emits{b.Add(2), (&IntEffect{}).Add(3), (&IntEffect{}).Add(-1)}
			},
			func(t *testing.T, s *pb.ReducedEffect, seeded bool) {
				want := int64(4)
				if seeded {
					want += 10
				}

				if got := s.GetScalar().GetIntVal(); got != want {
					t.Fatalf("int = %d, want %d", got, want)
				}
			},
		},
		{
			"float", func() Emits { return ScalarFloat(10.5).AsEffect() },
			func(s *pb.ReducedEffect) []Emits {
				b, _ := FloatFromSnap(s)

				return []Emits{b.Add(.5), (&FloatEffect{}).Add(.25), (&FloatEffect{}).Add(-.125)}
			},
			func(t *testing.T, s *pb.ReducedEffect, seeded bool) {
				want := .625
				if seeded {
					want += 10.5
				}

				if got := s.GetScalar().GetFloatVal(); got != want {
					t.Fatalf("float = %g, want %g", got, want)
				}
			},
		},
		{
			"list", func() Emits { return (&ListEffect[*StringEffect]{}).Append(ScalarString("seed").AsEffect()) },
			func(s *pb.ReducedEffect) []Emits {
				b, _ := ListFromSnap[*StringEffect](s)

				return []Emits{b.Append(ScalarString("a").AsEffect()), (&ListEffect[*StringEffect]{}).Append(ScalarString("b").AsEffect()), (&ListEffect[*StringEffect]{}).Prepend(ScalarString("!").AsEffect())}
			},
			func(t *testing.T, s *pb.ReducedEffect, seeded bool) {
				want := []string{"!", "a", "b"}
				if seeded {
					want = []string{"!", "seed", "a", "b"}
				}

				b, _ := ListFromSnap[*StringEffect](s)
				if got := listStrings(b); !slices.Equal(got, want) {
					t.Fatalf("list = %q, want %q", got, want)
				}
			},
		},
		{
			"map", func() Emits { return (&MapEffect[*IntEffect]{}).Set("seed", ScalarInt(9).AsEffect()) },
			func(s *pb.ReducedEffect) []Emits {
				b, _ := MapFromSnap[*IntEffect](s)

				return []Emits{b.Remove("absent").Set("\x00\xff", ScalarInt(2).AsEffect()), (&MapEffect[*IntEffect]{}).Set("\x00\xff", (&IntEffect{}).Add(3)), (&MapEffect[*IntEffect]{}).Set("", ScalarInt(4).AsEffect())}
			},
			func(t *testing.T, s *pb.ReducedEffect, seeded bool) {
				want := map[string]int64{"\x00\xff": 5, "": 4}
				if seeded {
					want["seed"] = 9
				}

				b, _ := MapFromSnap[*IntEffect](s)
				if got := mapInts(b); !maps.Equal(got, want) {
					t.Fatalf("map = %v, want %v", got, want)
				}
			},
		},
	}
	for _, tc := range cases {
		for _, seeded := range []bool{false, true} {
			for _, tx := range []bool{false, true} {
				for _, partition := range []string{"", "component"} {
					t.Run(fmt.Sprintf("%s/seed=%t/tx=%t/partition=%s", tc.name, seeded, tx, partition), func(t *testing.T) {
						e := scalarTestEngine(t)
						key := "composition:node"
						emit := func(b Emits, c *effects.Context, tips []effects.Tip) error {
							if partition != "" {
								return b.EmitPartition(key, partition, c, tips)
							}

							return b.Emit(key, c, tips)
						}
						// Component mutations use an existing root; the separate partition-only
						// regression below covers reads when no root data exists.
						if partition != "" {
							root := e.NewContext()
							defer root.Abort()

							if err := ScalarString("root").AsEffect().Emit(key, root, nil); err != nil {
								t.Fatal(err)
							}

							if err := root.Flush(); err != nil {
								t.Fatal(err)
							}
						}

						if seeded {
							c := e.NewContext()
							defer c.Abort()

							if err := emit(tc.seed(), c, nil); err != nil {
								t.Fatal(err)
							}

							if err := c.Flush(); err != nil {
								t.Fatal(err)
							}
						}

						c := e.NewContext()
						defer c.Abort()

						if tx {
							c.BeginTx()
						}

						snap, tips, err := c.GetSnapshot(key)
						if err != nil {
							t.Fatal(err)
						}

						originalTips := slices.Clone(tips)

						base := snap
						if partition != "" {
							base = snap.GetPartitions()[partition]
						}

						batches := tc.batches(base)

						type observation struct {
							tip  effects.Tip
							deps []effects.Tip
						}

						var (
							mu       sync.Mutex
							observed []observation
						)

						e.SetOnLocalEffect(func(tip effects.Tip, eff *pb.Effect) {
							if eff.GetData() == nil || string(eff.GetKey()) != key {
								return
							}

							o := observation{tip: tip}
							for _, dep := range eff.GetDeps() {
								o.deps = append(o.deps, effects.Tip{dep.GetNodeId(), dep.GetOffset()})
							}

							mu.Lock()

							observed = append(observed, o)
							mu.Unlock()
						})
						defer e.SetOnLocalEffect(nil)

						count := 0
						for _, b := range batches {
							count += len(b.getEffects())
							if err := emit(b, c, tips); err != nil {
								t.Fatal(err)
							}
						}

						e.SetOnLocalEffect(nil)
						mu.Lock()
						observations := slices.Clone(observed)
						mu.Unlock()

						if len(observations) != count {
							t.Fatalf("emitted %d effects, want %d", len(observations), count)
						}

						for _, tip := range originalTips {
							if !scalarTestDependsOn(t, e, observations[0].deps, tip) {
								t.Fatalf("lost observed tip %v", tip)
							}
						}

						for i := 1; i < len(observations); i++ {
							if !slices.Equal(observations[i].deps, []effects.Tip{observations[i-1].tip}) {
								t.Fatalf("effect %d did not depend on predecessor: %v", i, observations[i].deps)
							}
						}

						if !slices.Equal(tips, originalTips) {
							t.Fatal("modified caller's tips")
						}

						if err := c.Flush(); err != nil {
							t.Fatal(err)
						}

						reader := e.NewContext()
						defer reader.Abort()

						result, _, err := reader.GetSnapshot(key)
						if err != nil {
							t.Fatal(err)
						}

						if partition != "" {
							result = result.GetPartitions()[partition]
						}

						tc.check(t, result, seeded)
					})
				}
			}
		}
	}
}

func TestPartitionReplacementPreservesOtherComponents(t *testing.T) {
	for _, kind := range []string{"list", "map"} {
		t.Run(kind, func(t *testing.T) {
			e := scalarTestEngine(t)
			key := "partition:" + kind

			c := e.NewContext()
			defer c.Abort()

			if err := ScalarString("root").AsEffect().Emit(key, c, nil); err != nil {
				t.Fatal(err)
			}

			if err := ScalarString("other").AsEffect().EmitPartition(key, "other", c, nil); err != nil {
				t.Fatal(err)
			}

			if err := c.Flush(); err != nil {
				t.Fatal(err)
			}

			for _, empty := range []bool{false, true, false} {
				var b Emits

				if kind == "list" {
					var values []*StringEffect
					if !empty {
						values = stringEffects("new")
					}

					b = (&ListEffect[*StringEffect]{}).Replace(values)
				} else {
					values := map[string]*StringEffect{}
					if !empty {
						values["raw"] = ScalarString("new").AsEffect()
					}

					b = (&MapEffect[*StringEffect]{}).Replace(values)
				}

				if err := b.EmitPartition(key, "target", c, nil); err != nil {
					t.Fatal(err)
				}

				if err := c.Flush(); err != nil {
					t.Fatal(err)
				}

				snap, _, err := c.GetSnapshot(key)
				if err != nil {
					t.Fatal(err)
				}

				if string(snap.GetScalar().Decompress()) != "root" || string(snap.GetPartitions()["other"].GetScalar().Decompress()) != "other" {
					t.Fatal("replacement deleted another component")
				}

				target := snap.GetPartitions()["target"]
				if empty {
					if target.GetOp() != pb.EffectOp_REMOVE_OP {
						t.Fatal("empty replacement did not remove target")
					}
				} else if kind == "list" {
					if len(target.GetOrderedElements()) != 1 || string(target.GetOrderedElements()[0].GetData().Decompress()) != "new" {
						t.Fatal("list replacement missing")
					}
				} else if string(target.GetNetAdds()["raw"].GetData().Decompress()) != "new" {
					t.Fatal("map replacement missing")
				}
			}
		})
	}
}

// A key with partition data must remain visible without a root scalar or collection.
func TestPartitionOnlyKeySurvivesEngineRead(t *testing.T) {
	e := scalarTestEngine(t)

	c := e.NewContext()
	defer c.Abort()

	key := "partition-only:node"
	b := ScalarString("payload").AsEffect()
	expected := proto.Clone(b.getEffects()[0]).(*pb.Effect)
	expected.Virtual = []byte("value")

	local := effects.ReduceChain(nil, []*pb.Effect{expected})
	if string(local.GetPartitions()["value"].GetScalar().Decompress()) != "payload" {
		t.Fatal("local reduction lost partition")
	}

	if err := b.EmitPartition(key, "value", c, nil); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	reader := e.NewContext()
	defer reader.Abort()

	snap, _, err := reader.GetSnapshot(key)
	if err != nil {
		t.Fatal(err)
	}

	if snap == nil {
		t.Fatal("engine returned nil for a key with a populated value partition")
	}

	if string(snap.GetPartitions()["value"].GetScalar().Decompress()) != "payload" {
		t.Fatal("engine read lost partition payload")
	}
}
