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
