package bplus

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"testing"

	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/engine/internal/compatversion"
)

func benchmarkKey(i int) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(i))

	return key
}

// These measure serialization in isolation, without any engine operations.
func BenchmarkBPlusPageCodec(b *testing.B) {
	for _, entries := range []int{8, 32, 128} {
		b.Run(fmt.Sprintf("Entries%d", entries), func(b *testing.B) {
			p := page{Leaf: true, Parent: Open("index").RootAddress()}
			for i := range entries {
				p.Keys = append(p.Keys, benchmarkKey(i))
				p.Values = append(p.Values, bytes.Repeat([]byte{byte(i)}, 32))
			}

			raw, err := encodePage(&p)
			if err != nil {
				b.Fatal(err)
			}

			b.Run("Encode", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(raw)))

				for b.Loop() {
					got, err := encodePage(&p)
					if err != nil || len(got) != len(raw) {
						b.Fatalf("encode: %v", err)
					}
				}
			})
			b.Run("Decode", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(raw)))

				for b.Loop() {
					var got page
					if err := decodePage(raw, &got); err != nil {
						b.Fatal(err)
					}

					if len(got.Keys) != entries {
						b.Fatal("lost entries")
					}
				}
			})
		})
	}
}

// Each read gets a fresh context and includes Abort. Build modes preserve the
// same insertion order and topology while varying transaction boundaries.
func BenchmarkBPlusRead(b *testing.B) {
	for _, entries := range []int{32, 128} {
		for _, build := range []string{"SingleTx", "Incremental"} {
			for _, kind := range []string{"Get", "Scan16"} {
				b.Run(fmt.Sprintf("Entries%d/Order8/Build%s/%s", entries, build, kind), func(b *testing.B) {
					e, tree, keys, value := benchmarkReadFixture(b, entries, build)
					b.ReportAllocs()

					i := 0

					for b.Loop() {
						c := e.NewContext()
						if kind == "Get" {
							got, found, err := tree.Get(c, keys[i%entries])
							c.Abort()

							if err != nil || !found || !bytes.Equal(got, value) {
								b.Fatalf("read: %v %v", found, err)
							}
						} else {
							got, err := tree.Scan(c, keys[i%(entries-15)], nil, 16)
							c.Abort()

							if err != nil || len(got) != 16 {
								b.Fatalf("scan: %d %v", len(got), err)
							}
						}

						i++
					}
				})
			}
		}
	}
}

func benchmarkReadFixture(t testing.TB, entries int, build string) (*effects.Engine, *Tree, [][]byte, []byte) {
	t.Helper()

	version := os.Getenv("BPLUS_SCALE_VERSION")
	if version != "" && compatversion.Normalize(version) == "" {
		t.Fatal("invalid BPLUS_SCALE_VERSION")
	}

	e := effects.NewEngine(effects.EngineConfig{DefaultMode: effects.SafeMode, BuildVersion: version})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})

	if version != "" {
		if e.MinimumVersion() != compatversion.Normalize(version) {
			t.Fatal("unexpected fixture version")
		}

		fmt.Fprintf(os.Stderr, "BPLUS_VERSION minimum=%s\n", e.MinimumVersion())
	}

	c := e.NewContext()

	tree, err := Create(c, "animals", Options{MaxKeys: 8})
	if err != nil {
		t.Fatal(err)
	}

	flush(t, c)

	c = e.NewContext()
	defer func() { c.Abort() }()

	if err := tree.Initialize(c); err != nil {
		t.Fatal(err)
	}

	keys := make([][]byte, entries)
	value := bytes.Repeat([]byte{7}, 32)

	for i := range entries {
		keys[i] = benchmarkKey(i)
		if err := tree.Put(c, keys[i], value); err != nil {
			t.Fatal(err)
		}

		if build == "Incremental" {
			flush(t, c)
			c = e.NewContext()
		}
	}

	flush(t, c)

	read := e.NewContext()
	stats, err := checkTree(tree, read)
	read.Abort()

	if err != nil || stats.Entries != entries {
		t.Fatalf("fixture: %+v %v", stats, err)
	}

	return e, tree, keys, value
}
