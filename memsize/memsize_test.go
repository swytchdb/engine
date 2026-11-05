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

import (
	"math/bits"
	"testing"
)

func TestAllocConservativeHeaders(t *testing.T) {
	const threshold = bits.UintSize * bits.UintSize / 8

	for _, tc := range []struct {
		bytes int64
		want  int64
	}{
		{-1, 0},
		{0, 0},
		{1, 8},
		{8, 8},
		{9, 16},
		{threshold, threshold},
		{threshold + 1, threshold + threshold/8},
		{568, 576}, // header still fits in the same class
		{569, 640}, // header crosses a class boundary
		{1024, 1152},
		{32760, 32768}, // last small allocation with a header
		{32761, 32768}, // first large allocation: no header
		{32768, 32768}, // adding a header here would waste an entire page
		{32769, 40960},
		{65536, 65536},
	} {
		if got := Alloc(tc.bytes); got != tc.want {
			t.Errorf("Alloc(%d) = %d, want %d", tc.bytes, got, tc.want)
		}
	}
}
