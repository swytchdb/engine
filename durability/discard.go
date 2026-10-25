package durability

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Discard removes mirrors without loading their TOCs. This is the low-memory
// fallback when even opening the inventory exceeded the recovery budget.
// It takes the directory lock and verifies each file header before removal.
func Discard(dir string) error {
	lock, err := lockDirectory(dir)
	if err != nil {
		return err
	}
	defer func() { _ = lock.Close() }()

	nodes, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, node := range nodes {
		if !node.IsDir() {
			continue
		}

		n, err := strconv.ParseUint(node.Name(), 10, 64)
		if err != nil {
			continue
		}

		path := filepath.Join(dir, node.Name())

		files, err := os.ReadDir(path)
		if err != nil {
			return err
		}

		for _, entry := range files {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".swytch") {
				continue
			}

			bucket, err := strconv.ParseUint(strings.TrimSuffix(entry.Name(), ".swytch"), 10, 64)
			if err != nil || bucket >= fileCount {
				continue
			}

			name := filepath.Join(path, entry.Name())

			f, err := os.Open(name)
			if err != nil {
				return err
			}

			var header [48]byte

			_, readErr := io.ReadFull(f, header[:])
			if err := errors.Join(readErr, f.Close()); err != nil {
				return err
			}

			if string(header[:8]) != "SWYTCH01" || binary.LittleEndian.Uint64(header[24:]) != n || binary.LittleEndian.Uint64(header[32:]) != bucket {
				return errors.New("refusing to discard unrecognized mirror")
			}

			if err := os.Remove(name); err != nil {
				return err
			}
		}

		if err := syncDirectory(path); err != nil {
			return err
		}
	}

	return nil
}
