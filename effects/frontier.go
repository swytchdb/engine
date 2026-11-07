package effects

import (
	"log/slog"
	"slices"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
)

// installIngestedTips merges arrivals with the live frontier using the shared
// causal normalization. An arrival may be an ancestor of an existing tip, including
// when it fills a previously missing link. Consuming only its immediate deps
// would resurrect that ancestor on out-of-order delivery or replay.
//
// Missing history is not evidence of dominance: keep unresolved candidates and
// report the error without rejecting the received effect. getEffect uses the
// same cache-first dependency resolution as reads and cloud normalization.
func (e *Engine) installIngestedTips(key string, arrivals ...Tip) {
	for {
		current := e.index.Contains(key)
		candidates := keytrie.NewTipSet(slices.Concat(current.Tips(), arrivals)...)

		tips := candidates.Tips()
		if len(tips) > 1 {
			live, _, err := normalizeTipCandidates(key, tips, nil, func(tip Tip) (*pb.Effect, error) {
				return e.getEffect(key, tip)
			})
			if err != nil {
				slog.Warn("frontier normalization incomplete; retaining unresolved tips", "key", key, "error", err)
			}

			candidates = keytrie.NewTipSet(live...)
		}

		// Preserve index identity for an unchanged frontier. Readers use that
		// identity to detect concurrent writes, so duplicate recovery must not
		// invalidate their snapshots.
		if current.Len() == candidates.Len() && current.ContainsAll(candidates.Tips()) {
			if e.index.Contains(key) == current {
				return
			}

			continue
		}

		if _, ok := e.index.Insert(key, current, candidates); ok {
			e.notifyKeyChanged(key)

			return
		}

		if e.index.Closed() {
			return
		}
	}
}
