package effectschema

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

func TestGeneratedFilesAreCurrent(t *testing.T) {
	for _, fixture := range []struct {
		schema, output, pkg string
		runtime             bool
	}{
		{"../../graph/critbit/meta/critmeta.effects", "../../graph/critbit/meta/critmeta_gen.go", "meta", false},
		{"testmodel/schema.effects", "testmodel/account_gen.go", "testmodel", false},
		{"../../graph/node.effects", "../../graph/node_fields_gen.go", "graph", true},
		{"../../graph/descriptor.effects", "../../graph/descriptor_fields_gen.go", "graph", true},
	} {
		t.Run(fixture.pkg, func(t *testing.T) {
			source, err := os.ReadFile(fixture.schema)
			if err != nil {
				t.Fatal(err)
			}

			generate := Generate
			if fixture.runtime {
				generate = GenerateRuntime
			}

			got, err := generate(source, fixture.pkg)
			if err != nil {
				t.Fatal(err)
			}

			want, err := os.ReadFile(fixture.output)
			if err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(got, want) {
				t.Fatal("generated file is stale; run go generate")
			}
		})
	}
}

func TestRejectInvalidSchemas(t *testing.T) {
	for _, tc := range []struct{ source, message string }{
		{"", "at least one type"},
		{"type", "expected identifier"},
		{"type private {}", "must be exported"},
		{"type T { x: }", "unsupported field type"},
		{"type T { x: int64", "expected identifier"},
		{"type T { x: set<int64> }", "set members must be string"},
		{"type T { x: map<int64,string> }", "map keys must be string"},
		{"type T { x: list<Unknown> }", "collection elements must be scalar"},
		{"type T { x: Unknown }", "unknown composite type"},
		{"type T { x: list<string }", "expected >"},
		{"type T { x: map<string> }", "expected ,"},
		{"type T { child: T }", "duplicate or reserved method"},
		{"type T { x: bool additive }", "additive requires"},
		{"type T { x: float64 max }", "max requires int64"},
		{"type T { x: int64 mystery }", "unsupported merge policy"},
		{"type T { x: int64 x: string }", "duplicate or reserved method"},
		{"type T { x: int64 X: string }", "duplicate or reserved method"},
		{"type T { emit: string }", "duplicate or reserved method"},
		{"type T { composite: string }", "duplicate or reserved method"},
		{"type T { x: bool setX: bool }", "duplicate or reserved method"},
		{"type T {} type T {}", "duplicate generated name"},
		{"type T {} type NewT {}", "duplicate generated name"},
		{"type T {} trailing", "expected type"},
		{"type T { /*", "comment not terminated"},
		{"type Box<T,T> { value: T }", "duplicate type parameter"},
		{"type Box<graph> { value: graph }", "type parameter"},
		{"type Box<T> { value: T } type Use { box: Box }", "requires 1 type arguments"},
		{"type Box<T> { value: T } type Use { box: Box<Unknown> }", "invalid type argument"},
		{"type T { x: string<int64> }", "cannot have type arguments"},
		{"type T { x: string xValue: string }", "duplicate or reserved method"},
		{"type T { x: list<string> setX: bool }", "duplicate or reserved method"},
		{"type T { runtime: string }", "duplicate or reserved method"},
		{"type T { entry_point: string entryPoint: string }", "duplicate or reserved method"},
		{"type T { entry_point: map<string,string> set_entry_point: bool }", "duplicate or reserved method"},
		{"type T { last_seen: int64 max max_last_seen: bool }", "duplicate or reserved method"},
		{"type T { display_name: string display_name_value: string }", "duplicate or reserved method"},
		{"type T { emit_partition: string }", "duplicate or reserved method"},
		{"type T { __: string }", "exported Go name"},
		{"type T { _1: string }", "exported Go name"},
	} {
		t.Run(tc.source, func(t *testing.T) {
			_, err := Generate([]byte(tc.source), "example")
			if err == nil || !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("error = %v, want %q", err, tc.message)
			}
		})
	}

	for _, pkg := range []string{"", "_", "type", "bad-name"} {
		if _, err := Generate([]byte("type T {}"), pkg); err == nil {
			t.Fatalf("accepted package %q", pkg)
		}
	}
}

func TestSnakeCaseAccessorsPreserveStorageNames(t *testing.T) {
	code, err := Generate([]byte(`type Names {
		entry_point: map<string,string>
		display_name: string
		last_seen: int64 max
		total_count: int64 additive
		nested_value: Nested
	}
	type Nested { is_ready: bool }`), "example")
	if err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{
		"EntryPoint() *graph.MapField[string]",
		`graph.Bind(&v.Composite, "entry_point", graph.MapType(graph.StringCodec))`,
		"SetEntryPoint(value *graph.MapField[string]) error",
		`v.Composite.Set("entry_point", value)`,
		"DisplayName() string", "DisplayNameValue() *graph.String", "SetDisplayName(value string)",
		`graph.Bind(&v.Composite, "display_name", graph.StringType)`,
		"MaxLastSeen(value int64)", "AddTotalCount(value int64)",
		`graph.Bind(&v.Composite, "last_seen", graph.Int64Type)`,
		`graph.Bind(&v.Composite, "total_count", graph.Int64Type)`,
		"NestedValue() *Nested", `graph.Bind(&v.Composite, "nested_value", NestedType)`,
		"IsReady() bool", `graph.Bind(&v.Composite, "is_ready", graph.BoolType)`,
	} {
		if !strings.Contains(string(code), want) {
			t.Errorf("generated code missing %q", want)
		}
	}
}

func TestSchemaWhitespaceAndComments(t *testing.T) {
	plain, err := Generate([]byte("type T { a: bool b: bytes c: int64 additive } type U {}"), "example")
	if err != nil {
		t.Fatal(err)
	}

	decorated, err := Generate([]byte("// header\ntype T {\n a: bool; // flag\n b: bytes,\n c: int64 additive\n}\n/* next */ type U {}"), "example")
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(plain, decorated) {
		t.Fatal("formatting changed generated code")
	}
}
