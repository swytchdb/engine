package effects

import (
	"errors"

	pb "github.com/swytchdb/engine/cluster/proto"
	"google.golang.org/protobuf/proto"
)

type keyNacks struct {
	key     string
	wireKey []byte
	details []*pb.NackTipDetail
	chain   []Tip
}

// groupNacks combines the complete response set before any key is walked.
// Identity is (internal key, effect ref): the same bind can conflict differently
// on different keys. Preserve richer duplicate details without mutating replies.
func (e *Engine) groupNacks(nacks []*pb.NackNotify) []keyNacks {
	var groups []keyNacks

	keys := make(map[string]int)
	details := make(map[string]map[Tip]int)
	chains := make(map[string]map[Tip]bool)

	for _, nack := range nacks {
		if nack == nil {
			continue
		}

		key := e.nackKey(nack)

		idx, ok := keys[key]
		if !ok {
			idx = len(groups)
			keys[key] = idx
			groups = append(groups, keyNacks{key: key, wireKey: nack.GetKey()})
			details[key] = make(map[Tip]int)
			chains[key] = make(map[Tip]bool)
		}

		group := &groups[idx]

		for _, detail := range nack.GetTipDetails() {
			if detail == nil {
				continue
			}

			tip := r(detail.GetRef())
			if previous, exists := details[key][tip]; exists && tip != (Tip{}) {
				if !proto.Equal(group.details[previous], detail) {
					merged := proto.Clone(group.details[previous]).(*pb.NackTipDetail)
					proto.Merge(merged, detail)
					group.details[previous] = merged
				}

				continue
			}

			details[key][tip] = len(group.details)
			group.details = append(group.details, detail)
		}

		for _, ref := range nack.GetCausalChain() {
			tip := r(ref)
			if tip != (Tip{}) && !chains[key][tip] {
				chains[key][tip] = true
				group.chain = append(group.chain, tip)
			}
		}
	}

	return groups
}

// responseVerdicts reads advertised snapshot envelopes before any candidate
// ancestry is examined. A later key's response can adjudicate an earlier bind.
// Missing envelopes remain the responsibility of normal recovery/discovery.
func (e *Engine) responseVerdicts(groups []keyNacks) (verdictMap, error) {
	verdicts := make(verdictMap)
	seen := make(map[Tip]bool)

	for _, group := range groups {
		for _, detail := range group.details {
			tip := r(detail.GetRef())
			if seen[tip] || detail.GetIsBind() || detail.GetIsData() {
				continue
			}

			seen[tip] = true

			eff, err := e.getEffect(group.key, tip)
			if err != nil || eff.GetSnapshot() == nil {
				continue
			}

			if err := e.harvestResponseVerdicts(tip, eff, verdicts); err != nil {
				return nil, err
			}
		}
	}

	return verdicts, nil
}

func (e *Engine) ingestNackBatch(nacks []*pb.NackNotify) (verdictMap, error) {
	groups := e.groupNacks(nacks)

	verdicts, err := e.responseVerdicts(groups)
	if e.broadcaster == nil || len(nacks) == 0 {
		return verdicts, err
	}

	seen := make(map[Tip]bool)

	var prefetch []Tip

	for _, group := range groups {
		for _, tip := range group.chain {
			if !seen[tip] {
				seen[tip] = true
				prefetch = append(prefetch, tip)
			}
		}
	}

	if len(prefetch) != 0 {
		go e.prefetchEffects(prefetch)
	}

	frontiers := make(ingestedFrontiers)
	for _, group := range groups {
		err = errors.Join(err, e.ingestNackKey(group, frontiers, verdicts))
	}

	e.installIngestedFrontiers(frontiers, verdicts)

	return verdicts, err
}

// deferNack and takeDeferredNacks serialize the handoff to commit evaluation.
// Late replies fall back to normal ingestion rather than being lost in a queue.
func (p *pendingTxn) deferNack(nack *pb.NackNotify) bool {
	p.nackMu.Lock()
	defer p.nackMu.Unlock()

	if p.nackBatchClosed {
		return false
	}

	p.deferredNacks = append(p.deferredNacks, nack)

	return true
}

func (p *pendingTxn) takeDeferredNacks() []*pb.NackNotify {
	p.nackMu.Lock()
	defer p.nackMu.Unlock()

	p.nackBatchClosed = true
	nacks := p.deferredNacks
	p.deferredNacks = nil

	return nacks
}
