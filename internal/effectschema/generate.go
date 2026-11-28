// Package effectschema compiles partitioned composite schemas into effect builders.
package effectschema

import (
	"bytes"
	"fmt"
	"go/token"
	"text/scanner"
	"unicode"
)

// Schema type and merge-policy keywords shared by parsing and code generation.
const (
	typeBool       = "bool"
	typeUint8      = "uint8"
	typeUint32     = "uint32"
	typeInt64      = "int64"
	typeFloat64    = "float64"
	typeString     = "string"
	typeBytes      = "bytes"
	typeMap        = "map"
	typeList       = "list"
	typeSet        = "set"
	policyReplace  = "replace"
	policyAdditive = "additive"
	policyMax      = "max"
)

type lexeme struct {
	text string
	pos  scanner.Position
}

type field struct {
	name, typ, policy string
	element, keyType  string
	args              []string
}

type declaration struct {
	name   string
	fields []field
	params []string
}

func exported(name string) string {
	var result []rune

	start := true

	for _, r := range name {
		if r == '_' {
			start = true

			continue
		}

		if start {
			r = unicode.ToUpper(r)
			start = false
		}

		result = append(result, r)
	}

	return string(result)
}

func parse(source []byte) ([]declaration, error) {
	var s scanner.Scanner
	s.Init(bytes.NewReader(source))
	s.Mode = scanner.ScanIdents | scanner.ScanComments | scanner.SkipComments

	var scanErr error

	s.Error = func(s *scanner.Scanner, message string) {
		if scanErr == nil {
			scanErr = fmt.Errorf("%s: %s", s.Position, message)
		}
	}

	var tokens []lexeme
	for kind := s.Scan(); kind != scanner.EOF; kind = s.Scan() {
		tokens = append(tokens, lexeme{s.TokenText(), s.Position})
	}

	if scanErr != nil {
		return nil, scanErr
	}

	tokens = append(tokens, lexeme{"<eof>", s.Pos()})
	i := 0
	fail := func(message string) error {
		return fmt.Errorf("%s: %s (got %q)", tokens[i].pos, message, tokens[i].text)
	}
	expect := func(want string) error {
		if tokens[i].text != want {
			return fail("expected " + want)
		}

		i++

		return nil
	}
	identifier := func() (string, error) {
		name := tokens[i].text
		if name == "_" || !token.IsIdentifier(name) {
			return "", fail("expected identifier")
		}

		i++

		return name, nil
	}

	var declarations []declaration

	globalNames := map[string]bool{}

	for tokens[i].text != "<eof>" {
		if err := expect("type"); err != nil {
			return nil, err
		}

		name, err := identifier()
		if err != nil {
			return nil, err
		}

		if !token.IsExported(name) {
			return nil, fail("type names must be exported")
		}

		for _, symbol := range []string{name, "New" + name, name + "FromSnap", name + "Type"} {
			if globalNames[symbol] {
				return nil, fail("duplicate generated name " + symbol)
			}

			globalNames[symbol] = true
		}

		d := declaration{name: name}

		if tokens[i].text == "<" {
			i++
			seen := map[string]bool{}

			for {
				param, err := identifier()
				if err != nil {
					return nil, err
				}

				if seen[param] || scalarType(param) || param == name || !token.IsExported(param) {
					return nil, fail("invalid or duplicate type parameter " + param)
				}

				seen[param] = true
				d.params = append(d.params, param)

				if tokens[i].text != "," {
					break
				}

				i++
			}

			if err := expect(">"); err != nil {
				return nil, err
			}
		}

		if err := expect("{"); err != nil {
			return nil, err
		}
		// Do not shadow the embedded buffer or its effect/low-level methods.
		methods := map[string]bool{"Composite": true, "MapEffect": true, "Emit": true, "EmitPartition": true, "Field": true, "ApplyField": true, "Get": true, "Set": true, "Remove": true, "Replace": true, "Child": true, "Snapshot": true, "Clear": true, "Runtime": true, "Lookup": true}

		for tokens[i].text != "}" {
			f := field{policy: policyReplace}

			f.name, err = identifier()
			if err != nil {
				return nil, err
			}

			if err := expect(":"); err != nil {
				return nil, err
			}

			f.typ = tokens[i].text
			if f.typ != typeMap && !token.IsIdentifier(f.typ) {
				return nil, fail("unsupported field type " + f.typ)
			}

			i++

			switch f.typ {
			case typeMap, typeList, typeSet:
				if err := expect("<"); err != nil {
					return nil, err
				}

				if f.typ == typeMap {
					f.keyType, err = identifier()
					if err != nil {
						return nil, err
					}

					if f.keyType != typeString {
						return nil, fail("map keys must be string")
					}

					if err := expect(","); err != nil {
						return nil, err
					}
				}

				f.element, err = identifier()
				if err != nil {
					return nil, err
				}

				if !scalarType(f.element) {
					return nil, fail("collection elements must be scalar")
				}

				if f.typ == typeSet && f.element != typeString {
					return nil, fail("set members must be string")
				}

				if err := expect(">"); err != nil {
					return nil, err
				}
			}

			if f.element == "" && tokens[i].text == "<" {
				i++

				for {
					arg, err := identifier()
					if err != nil {
						return nil, err
					}

					f.args = append(f.args, arg)

					if tokens[i].text != "," {
						break
					}

					i++
				}

				if err := expect(">"); err != nil {
					return nil, err
				}
			}

			if token.IsIdentifier(tokens[i].text) && tokens[i+1].text != ":" {
				f.policy = tokens[i].text
				i++
			}

			switch f.policy {
			case policyReplace:
			case policyAdditive:
				if f.typ != typeInt64 && f.typ != typeFloat64 {
					return nil, fail("additive requires int64 or float64")
				}
			case policyMax:
				if f.typ != typeInt64 {
					return nil, fail("max requires int64")
				}
			default:
				return nil, fail("unsupported merge policy " + f.policy)
			}

			if !scalarType(f.typ) && f.policy != policyReplace {
				return nil, fail("merge policies apply only to scalar fields")
			}

			getter := exported(f.name)
			if !token.IsExported(getter) {
				return nil, fail("field must produce an exported Go name")
			}

			names := []string{getter}
			if scalarType(f.typ) {
				names = append(names, operation(f)+getter, getter+"Value")
			} else {
				names = append(names, "Set"+getter)
			}

			for _, method := range names {
				if methods[method] {
					return nil, fail("duplicate or reserved method " + method)
				}

				methods[method] = true
			}

			d.fields = append(d.fields, f)

			if tokens[i].text == ";" || tokens[i].text == "," {
				i++
			}
		}

		i++

		declarations = append(declarations, d)
	}

	if len(declarations) == 0 {
		return nil, fmt.Errorf("schema must declare at least one type")
	}

	declared := map[string]declaration{}
	for _, d := range declarations {
		declared[d.name] = d
	}

	for _, d := range declarations {
		params := map[string]bool{}
		for _, param := range d.params {
			params[param] = true
		}

		for _, f := range d.fields {
			if scalarType(f.typ) && len(f.args) > 0 {
				return nil, fmt.Errorf("scalar %s cannot have type arguments", f.typ)
			}

			if scalarType(f.typ) || f.element != "" {
				continue
			}

			if params[f.typ] {
				if len(f.args) > 0 {
					return nil, fmt.Errorf("type parameter %s cannot have arguments", f.typ)
				}

				continue
			}

			ref, ok := declared[f.typ]
			if !ok {
				return nil, fmt.Errorf("%s.%s: unknown composite type %s", d.name, f.name, f.typ)
			}

			if len(f.args) != len(ref.params) {
				return nil, fmt.Errorf("%s requires %d type arguments", f.typ, len(ref.params))
			}

			for _, arg := range f.args {
				if scalarType(arg) || params[arg] {
					continue
				}

				known, ok := declared[arg]
				if !ok || len(known.params) > 0 {
					return nil, fmt.Errorf("invalid type argument %s", arg)
				}
			}
		}
	}

	return declarations, nil
}

func operation(f field) string {
	switch f.policy {
	case policyAdditive:
		return "Add"
	case policyMax:
		return "Max"
	default:
		return "Set"
	}
}
