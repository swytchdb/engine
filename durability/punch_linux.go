//go:build linux

package durability

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func punchFile(f *os.File, pos, size uint64) error {
	err := unix.Fallocate(int(f.Fd()), unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, int64(pos), int64(size))
	if errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EINVAL) {
		return nil
	}

	return err
}
