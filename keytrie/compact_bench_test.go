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

package keytrie

import (
	"fmt"
	"runtime"
	"testing"
)

// Evacuate one sparse chunk while increasing the unrelated live keyspace.
// Setup is excluded: this measures the compactor, not population/deletion.
// Use -benchtime=1x to keep the large fixtures bounded.
func BenchmarkCompactInternalSparseChunk(b *testing.B) {
	for _, size := range []int{1 << 14, 1 << 17, 1 << 20} {
		b.Run(fmt.Sprintf("keys=%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()

				c := NewCritbit[struct{}]()
				c.reapRunning.Store(true) // run maintenance synchronously

				keys := make([]string, size)
				for k := range size {
					keys[k] = fmt.Sprintf("key-%08d", k)
					c.Insert(keys[k], nil, NewTipSet(tip(uint64(k+1))))
				}

				for _, key := range keys[10:defaultCritbitArenaChunkSize] {
					c.DeleteAndSnapshot(key, c.Contains(key))
				}

				drainAllReaps(c)
				runtime.GC()
				b.StartTimer()

				evacuated := c.compactInternalChunks(1)

				b.StopTimer()

				if evacuated != 1 {
					b.Fatalf("evacuated %d chunks, want 1", evacuated)
				}

				for k, key := range keys {
					if k >= 10 && k < defaultCritbitArenaChunkSize {
						continue
					}

					if ts := c.Contains(key); ts == nil || !ts.Contains(tip(uint64(k+1))) {
						b.Fatalf("surviving key %s lost or changed", key)
					}
				}

				if err := c.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
