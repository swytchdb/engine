package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteGeneratedReplacesOutput(t *testing.T) {
	dir := t.TempDir()

	output := filepath.Join(dir, "generated.go")
	if err := os.WriteFile(output, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := writeGenerated(output, []byte("new")); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(output)
	if err != nil || string(got) != "new" {
		t.Fatalf("output = %q, error = %v", got, err)
	}

	info, err := os.Stat(output)
	if err != nil {
		t.Fatal(err)
	}

	if info.Mode().Perm() != 0o644 {
		t.Fatalf("permissions = %o, want 644", info.Mode().Perm())
	}

	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("temporary output remains: entries = %v, error = %v", entries, err)
	}
}

func TestWriteGeneratedCleansUpFailedPublish(t *testing.T) {
	dir := t.TempDir()

	output := filepath.Join(dir, "destination")
	if err := os.Mkdir(output, 0o755); err != nil {
		t.Fatal(err)
	}

	err := writeGenerated(output, []byte("generated"))

	var renameErr *os.LinkError
	if !errors.As(err, &renameErr) || renameErr.Op != "rename" {
		t.Fatalf("error = %v, want wrapped rename failure", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 || entries[0].Name() != "destination" || !entries[0].IsDir() {
		t.Fatalf("failed publish changed destination or leaked temporary file: entries = %v, error = %v", entries, err)
	}
}

func TestWriteGeneratedReportsCreateFailure(t *testing.T) {
	output := filepath.Join(t.TempDir(), "missing", "generated.go")
	if err := writeGenerated(output, nil); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("error = %v, want missing directory", err)
	}
}
