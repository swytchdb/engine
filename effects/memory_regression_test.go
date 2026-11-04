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
	"runtime"
	"runtime/debug"
	"testing"
	"time"
)

// Uses the pre-port API so the same regression can run against main. Two
// governor ticks expose the old behavior: one schedules a heap-based deficit,
// the next evicts otherwise healthy keys to satisfy unrelated allocations.
func TestMemoryUnrelatedHeapDoesNotEvict(t *testing.T) {
	previousLimit := debug.SetMemoryLimit(1 << 30)
	previousGC := debug.SetGCPercent(-1)

	t.Cleanup(func() {
		debug.SetMemoryLimit(previousLimit)
		debug.SetGCPercent(previousGC)
	})

	e := NewEngine(EngineConfig{DefaultMode: UnsafeMode, MemoryLimit: 32 << 20})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})

	for i := range 64 {
		c := e.NewContext()
		if err := c.Emit(scalarSet(fmt.Sprintf("unrelated:%d", i), "value")); err != nil {
			t.Fatal(err)
		}

		if err := c.Flush(); err != nil {
			t.Fatal(err)
		}
	}

	external := make([]byte, 64<<20)
	for i := 0; i < len(external); i += 4096 {
		external[i] = 1
	}

	runtime.GC()
	time.Sleep(2500 * time.Millisecond)

	if victims := e.effectCache.ColdEvictions(); victims != 0 {
		t.Fatalf("unrelated heap caused %d cache evictions", victims)
	}

	if e.KeyCount() != 64 {
		t.Fatalf("lost cached keys: %d", e.KeyCount())
	}

	runtime.KeepAlive(external)
}
