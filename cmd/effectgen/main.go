// Command effectgen generates typed composite effect builders from a schema.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/swytchdb/engine/internal/effectschema"
)

func run() error {
	schema := flag.String("schema", "", "input schema file")
	output := flag.String("out", "", "generated Go file")
	packageName := flag.String("package", "", "Go package name")
	runtimePackage := flag.Bool("runtime", false, "generate within the runtime package without a self import")

	flag.Parse()

	if *schema == "" || *output == "" || *packageName == "" || flag.NArg() != 0 {
		return fmt.Errorf("usage: effectgen -schema type.effects -package name -out type_gen.go [-runtime]")
	}

	source, err := os.ReadFile(*schema)
	if err != nil {
		return err
	}

	generate := effectschema.Generate
	if *runtimePackage {
		generate = effectschema.GenerateRuntime
	}

	generated, err := generate(source, *packageName)
	if err != nil {
		return fmt.Errorf("%s: %w", *schema, err)
	}

	return writeGenerated(*output, generated)
}

func writeGenerated(output string, generated []byte) (err error) {
	file, err := os.CreateTemp(filepath.Dir(output), ".effectgen-*.go")
	if err != nil {
		return fmt.Errorf("create temporary output for %q: %w", output, err)
	}

	closed, published := false, false
	defer func() {
		if !closed {
			if closeErr := file.Close(); closeErr != nil {
				err = errors.Join(err, fmt.Errorf("close temporary output %q: %w", file.Name(), closeErr))
			}
		}

		if !published {
			if removeErr := os.Remove(file.Name()); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				err = errors.Join(err, fmt.Errorf("remove temporary output %q: %w", file.Name(), removeErr))
			}
		}
	}()

	if _, err := file.Write(generated); err != nil {
		return fmt.Errorf("write temporary output %q: %w", file.Name(), err)
	}

	if err := file.Chmod(0o644); err != nil {
		return fmt.Errorf("set temporary output permissions %q: %w", file.Name(), err)
	}

	err = file.Close()
	closed = true

	if err != nil {
		return fmt.Errorf("close temporary output %q: %w", file.Name(), err)
	}

	if err := os.Rename(file.Name(), output); err != nil {
		return fmt.Errorf("publish generated output %q: %w", output, err)
	}

	published = true

	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
