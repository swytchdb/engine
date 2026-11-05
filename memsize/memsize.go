/*
 * Copyright 2026 Swytch Labs BV
 *
 * This file is part of Swytch.
 *
 * Swytch is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Swytch is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Swytch. If not, see <https://www.gnu.org/licenses/>.
 *
 * Additional permissions under GNU AGPL version 3 section 7 apply to
 * this file. See the NOTICE.md file distributed with this source code
 * for the current set of additional permissions.
 */

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

// Alloc conservatively rounds an allocation to Go's size classes, or whole
// heap pages for large objects. Since callers supply no pointer metadata, small
// objects above Go's header threshold include the 8-byte malloc header even if
// pointer-free. Keep estimates separate from ownership: readers can retain an
// allocation after its cache owner releases it.
func Alloc(n int64) int64 {
	if n <= 0 {
		return 0
	}

	// Go's internal/runtime/gc uses PtrSize*PtrBits as the exclusive header
	// threshold (512 on 64-bit, 128 on 32-bit). Larger objects use heap pages
	// without an inline header; see runtime.mallocgc and runtime.roundupsize.
	const headerSize = 8
	if n > bits.UintSize*bits.UintSize/8 && n <= 32768-headerSize {
		n += headerSize
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
