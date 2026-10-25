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

package crdt

import (
	"time"
)

// HLC projects the OS monotonic clock onto a fixed wall-time origin. Its
// serialized timestamps retain elapsed intervals across wall-clock steps.
// A new engine incarnation establishes a new clock domain on restart.
type HLC struct {
	clock func() time.Time
}

// NewHLC creates a new clock.
func NewHLC() *HLC {
	origin := time.Now()

	return newElapsedClock(origin, func() time.Duration { return time.Since(origin) })
}

func newElapsedClock(origin time.Time, elapsed func() time.Duration) *HLC {
	return &HLC{clock: func() time.Time { return origin.Add(elapsed()) }}
}

// Now returns a timestamp whose wall representation advances with monotonic
// elapsed time. Unlike serializing time.Now directly, dropping Go's private
// monotonic component does not reintroduce subsequent wall-clock adjustments.
func (h *HLC) Now() time.Time {
	return h.clock()
}

// SetClock replaces the final timestamp source for testing. Except in tests of
// malformed clock evidence, the source must preserve the elapsed-clock contract.
func (h *HLC) SetClock(clock func() time.Time) {
	h.clock = clock
}
