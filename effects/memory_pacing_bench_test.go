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

package effects

import (
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// Replay concurrent producers against a bounded cache. Report peak and ending
// owned memory as well as elapsed time; throughput alone can reward overshoot.
// Use -benchtime=1x. The workload writes 1 GiB through a 64 MiB cache.
func BenchmarkMemorySustainedWrites(b *testing.B) {
	oldLimit := debug.SetMemoryLimit(1 << 30)

	b.Cleanup(func() { debug.SetMemoryLimit(oldLimit) })

	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()

		e := NewEngine(EngineConfig{DefaultMode: UnsafeMode, MemoryLimit: 64 << 20})
		value := strings.Repeat("v", 64<<10)

		var (
			peak    atomic.Int64
			workers sync.WaitGroup
		)

		b.StartTimer()

		for worker := range 16 {
			workers.Go(func() {
				for i := range 1024 {
					c := e.NewContext()
					if err := c.Emit(scalarSet(fmt.Sprintf("load:%d:%d", worker, i), value)); err != nil {
						b.Error(err)

						return
					}

					if err := c.Flush(); err != nil {
						b.Error(err)

						return
					}

					if i%16 == 0 {
						current := e.MemoryStats().ResidentBytes
						for previous := peak.Load(); current > previous; previous = peak.Load() {
							if peak.CompareAndSwap(previous, current) {
								break
							}
						}
					}
				}
			})
		}

		workers.Wait()
		b.StopTimer()
		b.ReportMetric(float64(peak.Load())/(1<<20), "peak-owned-MiB")
		b.ReportMetric(float64(e.MemoryStats().ResidentBytes)/(1<<20), "end-owned-MiB")

		if err := e.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
