//go:build unix

package durability

import (
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
)

func mapFile(f *os.File, size uint64) ([]byte, error) {
	if size > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("mapping too large: %d", size)
	}

	return unix.Mmap(int(f.Fd()), 0, int(size), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
}
func unmapFile(data []byte) error           { return unix.Munmap(data) }
func syncMapped(f *os.File, _ []byte) error { return f.Sync() }
func lockDirectory(dir string) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(dir, ".swytch.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	if err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()

		return nil, fmt.Errorf("data directory already in use: %w", err)
	}

	return f, nil
}
func allocationBlock(f *os.File) (uint64, error) {
	var st unix.Statfs_t
	if err := unix.Fstatfs(int(f.Fd()), &st); err != nil {
		return 0, err
	}

	return uint64(st.Bsize), nil
}
func syncDirectory(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}

	return errors.Join(f.Sync(), f.Close())
}
