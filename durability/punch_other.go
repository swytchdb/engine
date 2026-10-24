//go:build !linux

package durability

import "os"

func punchFile(_ *os.File, _, _ uint64) error { return nil }
