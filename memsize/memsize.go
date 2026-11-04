// Package memsize provides conservative capacity estimates for cache budgeting.
// These describe owned allocations, not RSS or bytes returned to the OS.
package memsize

import "math/bits"

// Current Go small-object classes. Larger allocations use whole heap pages.
// A slice iteration avoids copying this table on each capacity estimate.
var allocationClasses = [...]int64{
	8, 16, 24, 32, 48, 64, 80, 96, 112, 128, 144, 160, 176, 192, 208, 224,
	240, 256, 288, 320, 352, 384, 416, 448, 480, 512, 576, 640, 704, 768,
	896, 1024, 1152, 1280, 1408, 1536, 1792, 2048, 2304, 2688, 3072, 3200,
	3456, 4096, 4864, 5376, 6144, 6528, 6784, 6912, 8192, 9472, 9728, 10240,
	10880, 12288, 13568, 14336, 16384, 18432, 19072, 20480, 21760, 24576,
	27264, 28672, 32768,
}

// Alloc rounds an allocation to Go's size classes, or whole heap pages for large
// objects. Keep estimates separate from ownership: readers can retain an
// allocation after its cache owner releases it.
func Alloc(n int64) int64 {
	if n <= 0 {
		return 0
	}

	for _, size := range allocationClasses[:] {
		if n <= size {
			return size
		}
	}

	return (n + 8191) &^ 8191
}

// Map budgets at most 50% occupancy, including control bytes and a header.
// Callers must retain the high-water entry count: deleting entries does not
// shrink a Go map's backing allocation. This is an estimate, not runtime ABI.
func Map(entries int, entryBytes int64) int64 {
	if entries == 0 {
		return 64
	}

	slots := uint64(8)
	if entries > 4 {
		slots = uint64(1) << bits.Len64(uint64(entries-1)*2)
	}

	return 64 + Alloc(int64(slots)*(entryBytes+1))
}
