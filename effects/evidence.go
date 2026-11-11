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
	"slices"

	pb "github.com/swytchdb/engine/cluster/proto"
	"google.golang.org/protobuf/proto"
)

// proofRecords reads explicit ownership edges only. Value ancestry is neither
// fetched nor interpreted here; the snapshot already owns these vertices.
func (p *VertexPool) proofRecords(eff *pb.Effect) (map[Tip]*pb.Effect, error) {
	records := make(map[Tip]*pb.Effect)

	queue := uniqueEvidence(eff)
	for len(queue) != 0 {
		tip := queue[len(queue)-1]
		queue = queue[:len(queue)-1]

		if records[tip] != nil {
			continue
		}

		proof, ok := p.Get(tip)
		if !ok {
			return nil, fmt.Errorf("snapshot evidence %v: %w", tip, errEffectNotResident)
		}

		records[tip] = proof
		queue = append(queue, uniqueEvidence(proof)...)
	}

	return records, nil
}

// proofVerdicts uses the ordinary bind evaluator against the packaged graph.
// Cached verdict labels are deliberately excluded from this computation.
func (e *Engine) proofVerdicts(owner Tip, eff *pb.Effect) (verdictMap, error) {
	if !e.effectCache.Incref(owner) {
		return nil, fmt.Errorf("proof owner %v: %w", owner, errEffectNotResident)
	}
	defer e.effectCache.Decref(owner)

	records, err := e.effectCache.proofRecords(eff)
	if err != nil {
		return nil, err
	}

	binds := make(map[Tip]*pb.Effect)

	for _, ref := range eff.GetSnapshot().GetEvidence().GetBinds() {
		tip := r(ref)

		bind := records[tip]
		if bind.GetTxnBind() == nil || bind.GetTxnId() == "" {
			return nil, fmt.Errorf("invalid proof bind %v", tip)
		}

		binds[tip] = bind
	}

	keys := make(map[string][]Tip)

	for tip, record := range binds {
		for _, kb := range record.GetTxnBind().GetKeys() {
			key := string(kb.GetKey())
			keys[key] = append(keys[key], tip)
		}
	}

	lost, _ := e.losersOnKeysAt(keys, func(string) []Tip { return nil }, nil, nil, false, records)
	verdicts := make(verdictMap)

	for _, record := range binds {
		verdict := pb.Verdict_WON
		if _, loses := lost[record.GetTxnId()]; loses {
			verdict = pb.Verdict_LOST
		}

		verdicts[record.GetTxnId()] = verdictEntry{verdict: verdict, snapshotTip: owner, proven: true}
	}

	return verdicts, nil
}

func (e *Engine) harvestProofVerdicts(tip Tip, eff *pb.Effect, verdicts verdictMap) error {
	if eff.GetSnapshot().GetEvidence() == nil {
		verdicts.harvest(tip, eff.GetSnapshot())

		return nil
	}

	derived, err := e.proofVerdicts(tip, eff)
	if err != nil {
		return err
	}

	for txn, entry := range derived {
		verdicts.retain(txn, entry)
	}

	return nil
}

// bindProof retains the transaction records used for predicate evaluation and
// predecessor proofs that allowed this bind to proceed. It does not retain a
// key's reconstructed value or unconnected history. The returned temporary pins
// last until the emitted snapshots acquire their own references.
func (e *Engine) bindProof(bind Tip, defeated map[string]Tip, sources []Tip) (proof *pb.BindEvidence, err error) {
	var pins []Tip

	seen := make(map[Tip]bool)

	defer func() {
		if err != nil {
			e.effectCache.releaseEvidence(pins)
			pins = nil
		}
	}()

	retain := func(tip Tip) (*pb.Effect, error) {
		eff, loadErr := e.getEffect("", tip)
		if loadErr != nil {
			return nil, loadErr
		}

		if !seen[tip] {
			if !e.effectCache.Incref(tip) {
				return nil, fmt.Errorf("proof member %v: %w", tip, errEffectNotResident)
			}

			seen[tip] = true
			pins = append(pins, tip)
		}

		return eff, nil
	}
	binds := []Tip{bind}

	for _, source := range sources {
		eff, loadErr := retain(source)
		if loadErr != nil {
			return nil, loadErr
		}

		if eff.GetSnapshot().GetEvidence() == nil {
			// Old peers supply cached labels without a recomputable proof.
			// Preserve legacy resolution, but do not claim it is a new proof.
			e.effectCache.releaseEvidence(pins)

			return nil, nil
		}

		binds = append(binds, fromPbRefs(eff.GetSnapshot().GetEvidence().GetBinds())...)
	}

	for _, tip := range defeated {
		binds = append(binds, tip)
	}

	for _, tip := range binds {
		eff, loadErr := retain(tip)
		if loadErr != nil {
			return nil, loadErr
		}

		for _, kb := range eff.GetTxnBind().GetKeys() {
			boundary := make(map[Tip]bool)
			for _, ref := range kb.GetConsumedTips() {
				boundary[r(ref)] = true
			}

			visited := make(map[Tip]bool)

			queue := []Tip{r(kb.GetNewTip())}
			for len(queue) != 0 {
				next := queue[len(queue)-1]
				queue = queue[:len(queue)-1]

				if next == (Tip{}) || boundary[next] || visited[next] {
					continue
				}

				visited[next] = true

				data, loadErr := e.getEffect(string(kb.GetKey()), next)
				if loadErr != nil {
					return nil, loadErr
				}

				if data.GetTxnId() != eff.GetTxnId() {
					continue
				}

				if _, loadErr := retain(next); loadErr != nil {
					return nil, loadErr
				}

				for ref := range keyDeps(string(kb.GetKey()), data) {
					queue = append(queue, r(ref))
				}
			}
		}
	}
	// Retain a connecting path when two proof binds are ordered. Keeping only
	// their envelopes would otherwise turn missing connectors into competition.
	all := make(map[Tip]*pb.Effect, len(binds))
	for _, tip := range binds {
		all[tip], _ = e.effectCache.Get(tip)
	}

	for from, first := range all {
		for to, second := range all {
			if from == to {
				continue
			}

			for _, kb := range first.GetTxnBind().GetKeys() {
				key := string(kb.GetKey())

				if !slices.ContainsFunc(second.GetTxnBind().GetKeys(), func(k *pb.TransactionalBindEffect_KeyBind) bool { return string(k.GetKey()) == key }) {
					continue
				}

				for _, t := range e.bindProofPath(key, from, to) {
					if _, loadErr := retain(t); loadErr != nil {
						return nil, loadErr
					}
				}
			}
		}
	}

	slices.SortFunc(pins, func(a, b Tip) int { return slices.Compare(a[:], b[:]) })

	proof = &pb.BindEvidence{}

	for _, tip := range pins {
		if slices.Contains(binds, tip) {
			proof.Binds = append(proof.Binds, toPbRef(tip))
		} else {
			proof.Deps = append(proof.Deps, toPbRef(tip))
		}
	}

	return proof, nil
}

func (e *Engine) bindProofPath(key string, from, to Tip) []Tip {
	parents := map[Tip]Tip{from: {}}

	queue := []Tip{from}
	for i := 0; i < len(queue); i++ {
		tip := queue[i]
		if tip == to {
			var path []Tip
			for tip != from {
				path = append(path, tip)
				tip = parents[tip]
			}

			return path
		}

		eff, _ := e.effectCache.Get(tip)
		for ref := range keyDeps(key, eff) {
			dep := r(ref)
			if _, visited := parents[dep]; !visited && dep != (Tip{}) {
				parents[dep] = tip
				queue = append(queue, dep)
			}
		}
	}

	return nil
}

func uniqueEvidence(eff *pb.Effect) []Tip {
	return evidenceTips(eff.GetSnapshot().GetEvidence())
}

func evidenceTips(proof *pb.BindEvidence) []Tip {
	refs := slices.Concat(proof.GetBinds(), proof.GetDeps())
	if len(refs) == 0 {
		return nil
	}

	seen := make(map[Tip]bool, len(refs))

	tips := make([]Tip, 0, len(refs))
	for _, ref := range refs {
		tip := r(ref)
		if !seen[tip] {
			seen[tip] = true
			tips = append(tips, tip)
		}
	}

	return tips
}

func (p *VertexPool) releaseEvidence(tips []Tip) {
	for _, tip := range tips {
		p.Decref(tip)
	}
}

func (p *VertexPool) pinEvidence(owner Tip, eff *pb.Effect) ([]Tip, error) {
	tips := uniqueEvidence(eff)
	for i, tip := range tips {
		if tip == (Tip{}) || tip == owner || !p.Incref(tip) {
			p.releaseEvidence(tips[:i])

			return nil, fmt.Errorf("snapshot %v: evidence %v is not resident or is self-referential", owner, tip)
		}
	}

	return tips, nil
}

// MarshalEffect packages only explicit proof references. Ordinary dependencies
// retain their existing wire semantics. Records precede their proof owners and
// shared nested proofs appear once, avoiding repeated serialization of history.
func (p *VertexPool) MarshalEffect(eff *pb.Effect) ([]byte, error) {
	if eff.GetSnapshot().GetEvidence() == nil || len(eff.GetEvidencePackage()) != 0 {
		return MarshalEffect(eff)
	}

	if p == nil {
		return nil, fmt.Errorf("cannot package snapshot evidence without a vertex pool")
	}

	seen := make(map[Tip]uint8)

	var pins []Tip
	defer func() { p.releaseEvidence(pins) }()

	var (
		records []*pb.EvidenceRecord
		visit   func(Tip) error
	)

	visit = func(tip Tip) error {
		if seen[tip] == 2 {
			return nil
		}

		if seen[tip] == 1 {
			return fmt.Errorf("cyclic snapshot evidence at %v", tip)
		}

		if !p.Incref(tip) {
			return fmt.Errorf("snapshot evidence %v is not resident", tip)
		}

		pins = append(pins, tip)
		seen[tip] = 1

		proof, _ := p.Get(tip)
		for _, dep := range uniqueEvidence(proof) {
			if err := visit(dep); err != nil {
				return err
			}
		}

		data, err := MarshalEffect(proof)
		if err != nil {
			return err
		}

		records = append(records, &pb.EvidenceRecord{Ref: toPbRef(tip), EffectData: data})
		seen[tip] = 2

		return nil
	}
	for _, tip := range uniqueEvidence(eff) {
		if err := visit(tip); err != nil {
			return nil, err
		}
	}

	wire := proto.Clone(eff).(*pb.Effect)
	wire.EvidencePackage = records

	return MarshalEffect(wire)
}

func (p *VertexPool) installEvidencePackage(owner Tip, eff *pb.Effect) (*pb.Effect, func(), error) {
	var pins []Tip

	release := func() { p.releaseEvidence(pins) }

	for _, record := range eff.GetEvidencePackage() {
		tip := r(record.GetRef())
		if tip == (Tip{}) || tip == owner {
			release()

			return nil, nil, fmt.Errorf("invalid evidence identity %v for %v", tip, owner)
		}

		proof := &pb.Effect{}
		if err := UnmarshalEffect(record.GetEffectData(), proof); err != nil {
			release()

			return nil, nil, err
		}

		if len(proof.GetEvidencePackage()) != 0 {
			release()

			return nil, nil, fmt.Errorf("nested evidence packages must be flattened")
		}

		if err := p.putWithPin(tip, proof, false, nil, true); err != nil {
			release()

			return nil, nil, err
		}

		pins = append(pins, tip)
	}

	clean := proto.Clone(eff).(*pb.Effect)
	clean.EvidencePackage = nil

	return clean, release, nil
}
