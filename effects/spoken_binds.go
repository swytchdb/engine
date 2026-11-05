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

import "sync/atomic"

// spokenBindHistory keeps every distinct consumer of one tip. Writers hold
// Engine.spokenBindsMu; readers only use snapshot and never access seen or tips.
type spokenBindHistory struct {
	seen      map[Tip]struct{}
	tips      []Tip
	published atomic.Value // []Tip: immutable, capacity-clipped snapshot
}

func (h *spokenBindHistory) add(bind Tip) {
	if _, exists := h.seen[bind]; exists {
		return
	}

	if h.seen == nil {
		h.seen = make(map[Tip]struct{})
	}

	h.seen[bind] = struct{}{}
	h.tips = append(h.tips, bind)
	// Append writes only beyond all previously published lengths. Growing
	// the backing array leaves old views intact; clipping capacity also
	// prevents a reader's append from overwriting a later published entry.
	h.published.Store(h.tips[:len(h.tips):len(h.tips)])
}

func (h *spokenBindHistory) snapshot() []Tip {
	if tips := h.published.Load(); tips != nil {
		return tips.([]Tip)
	}

	return nil
}
