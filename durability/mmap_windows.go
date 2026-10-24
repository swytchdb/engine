//go:build windows

package durability

import (
	"errors"
	"fmt"
	"golang.org/x/sys/windows"
	"os"
	"path/filepath"
	"unsafe"
)

func mapFile(f *os.File, size uint64) ([]byte, error) {
	if size > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("mapping too large: %d", size)
	}
	h, err := windows.CreateFileMapping(windows.Handle(f.Fd()), nil, windows.PAGE_READWRITE, uint32(size>>32), uint32(size), nil)
	if err != nil {
		return nil, err
	}
	defer windows.CloseHandle(h)
	p, err := windows.MapViewOfFile(h, windows.FILE_MAP_READ|windows.FILE_MAP_WRITE, 0, 0, uintptr(size))
	if err != nil {
		return nil, err
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(p)), int(size)), nil
}
func unmapFile(data []byte) error { return windows.UnmapViewOfFile(uintptr(unsafe.Pointer(&data[0]))) }
func syncMapped(f *os.File, data []byte) error {
	return errors.Join(windows.FlushViewOfFile(uintptr(unsafe.Pointer(&data[0])), uintptr(len(data))), f.Sync())
}
func allocationBlock(_ *os.File) (uint64, error) { return 65536, nil }
func syncDirectory(_ string) error               { return nil }
func lockDirectory(dir string) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(dir, ".swytch.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	var overlapped windows.Overlapped
	err = windows.LockFileEx(windows.Handle(f.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &overlapped)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("data directory already in use: %w", err)
	}
	return f, nil
}
