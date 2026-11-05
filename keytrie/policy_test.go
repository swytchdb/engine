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

import "testing"

func TestAdaptiveThresholdFeedback(t *testing.T) {
	for _, tt := range []struct {
		name                         string
		dir                          int32
		hits                         uint64
		low, high, wantLow, wantHigh uint32
	}{
		{"raised improved", 1, 1500, 2500, 5000, 2500, 4000},
		{"raised worsened", 1, 500, 2500, 5000, 2500, 6000},
		{"raised unchanged", 1, 1000, 2500, 5000, 2500, 6000},
		{"lowered improved", -1, 1500, 2500, 5000, 3500, 5000},
		{"lowered worsened", -1, 500, 2500, 5000, 1500, 5000},
		{"lowered unchanged", -1, 1000, 2500, 5000, 1500, 5000},
		{"high lower bound", 1, 1500, 2500, 4000, 2500, 4000},
		{"high upper bound", 1, 500, 2500, 7000, 2500, 7000},
		{"low lower bound", -1, 500, 1500, 5000, 1500, 5000},
		{"low upper bound", -1, 1500, 3000, 5000, 3000, 5000},
		{"no previous change", 0, 1500, 2500, 5000, 2500, 5000},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := NewCritbit[struct{}]()

			t.Cleanup(func() { closeTestTrie(t, c) })
			c.evictedUnprot.Store(adaptiveCheckInterval)
			c.windowOps.shards[0].v.Store(hitRateWindowSize)
			c.windowHits.shards[0].v.Store(tt.hits)
			c.prevHitRate.Store(5000)
			c.lastKDir.Store(tt.dir)
			c.rateLow.Store(tt.low)
			c.rateHigh.Store(tt.high)
			c.maybeAdaptK()

			if c.rateLow.Load() != tt.wantLow || c.rateHigh.Load() != tt.wantHigh {
				t.Fatalf("thresholds = %d/%d, want %d/%d", c.rateLow.Load(), c.rateHigh.Load(), tt.wantLow, tt.wantHigh)
			}
		})
	}
}
