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
	"slices"
	"sort"

	pb "github.com/swytchdb/engine/cluster/proto"
)

// causalIndex is a read-local reachability index. Each chain contains only
// vertices connected by actual dependency paths, never inferred node/offset
// order. A vertex records the furthest ancestor on each chain. This replaces
// one full ancestor set per transaction with O(vertices * chains) integers;
// chains can be reused after branches merge, even across different writers.
type causalIndex struct {
	engine *Engine
	key    string
	load   func(Tip) (*pb.Effect, error)
	ids    map[Tip]int
	nodes  []causalNode
	heads  []int
	cyclic bool

	// Malformed cyclic histories retain the original visited-set semantics.
	fallback map[Tip]map[Tip]struct{}
}

type causalNode struct {
	clock []int // nil while the vertex is on the DFS stack
	chain int
	seq   int
}

func newCausalIndex(e *Engine, key string) *causalIndex {
	return &causalIndex{engine: e, key: key, ids: make(map[Tip]int), load: func(t Tip) (*pb.Effect, error) { return e.getEffect(key, t) }}
}

func (c *causalIndex) deps(eff *pb.Effect) []*pb.EffectRef {
	for _, kb := range eff.GetTxnBind().GetKeys() {
		if string(kb.GetKey()) == c.key {
			return []*pb.EffectRef{kb.GetNewTip()}
		}
	}
	// Conflict reporting can extend the index with an envelope from another
	// key. Such an envelope has no per-key projection in this index.
	return eff.GetDeps()
}

func (c *causalIndex) add(tips ...Tip) bool {
	if c.cyclic {
		return false
	}

	type frame struct {
		id   int
		deps []*pb.EffectRef
		next int
	}

	var stack []frame

	push := func(tip Tip) {
		id := len(c.nodes)
		c.ids[tip] = id
		c.nodes = append(c.nodes, causalNode{})
		eff, _ := c.load(tip)
		// As in the original walk, a missing effect is reachable but its
		// unavailable parents are not. Do not infer ancestry past the gap.
		stack = append(stack, frame{id: id, deps: c.deps(eff)})
	}

	for _, tip := range tips {
		if _, done := c.ids[tip]; done {
			continue
		}

		push(tip)

		for len(stack) > 0 {
			f := &stack[len(stack)-1]
			if f.next < len(f.deps) {
				dep := r(f.deps[f.next])
				f.next++

				id, seen := c.ids[dep]
				if !seen {
					push(dep)
				} else if c.nodes[id].clock == nil {
					c.cyclic = true

					return false
				}

				continue
			}

			clock := make([]int, len(c.heads)+1)
			for _, dep := range f.deps {
				for chain, seq := range c.nodes[c.ids[r(dep)]].clock {
					clock[chain] = max(clock[chain], seq)
				}
			}

			chain := len(c.heads)
			for i, head := range c.heads {
				if clock[i] == head {
					chain = i

					break
				}
			}

			if chain == len(c.heads) {
				c.heads = append(c.heads, 1)
			} else {
				c.heads[chain]++
			}

			clock[chain] = c.heads[chain]
			c.nodes[f.id] = causalNode{clock: clock[:len(c.heads)], chain: chain, seq: clock[chain]}
			stack = stack[:len(stack)-1]
		}
	}

	return true
}

func (c *causalIndex) reaches(from, target Tip) bool {
	if from == target {
		return true
	}

	if !c.add(from) {
		return c.reachesCyclic(from, target)
	}

	id, seen := c.ids[target]
	if !seen {
		return false
	}

	ancestor := c.nodes[id]
	clock := c.nodes[c.ids[from]].clock

	return ancestor.chain < len(clock) && clock[ancestor.chain] >= ancestor.seq
}

// candidateChains groups candidate indices in dependency order. A candidate's
// ancestors occupy a prefix of each chain and its descendants a suffix, so
// only the interval between them needs conflict checks.
func (c *causalIndex) candidateChains(tips []Tip) [][]int {
	if !c.add(tips...) {
		all := make([]int, len(tips))
		for i := range all {
			all[i] = i
		}

		return [][]int{all}
	}

	chains := make([][]int, len(c.heads))
	for i, tip := range tips {
		chain := c.nodes[c.ids[tip]].chain
		chains[chain] = append(chains[chain], i)
	}

	for _, chain := range chains {
		slices.SortFunc(chain, func(a, b int) int {
			return c.nodes[c.ids[tips[a]]].seq - c.nodes[c.ids[tips[b]]].seq
		})
	}

	return chains
}

func (c *causalIndex) concurrentRange(tip Tip, chain []int, tips []Tip) (int, int) {
	if c.cyclic {
		return 0, len(chain)
	}

	first := sort.Search(len(chain), func(i int) bool { return !c.reaches(tip, tips[chain[i]]) })
	last := sort.Search(len(chain), func(i int) bool { return c.reaches(tips[chain[i]], tip) })

	return first, max(first, last)
}

func (c *causalIndex) reachesCyclic(from, target Tip) bool {
	if c.fallback == nil {
		c.fallback = make(map[Tip]map[Tip]struct{})
	}

	seen, cached := c.fallback[from]
	if !cached {
		seen = map[Tip]struct{}{from: {}}

		stack := []Tip{from}
		for len(stack) > 0 {
			cur := stack[len(stack)-1]
			stack = stack[:len(stack)-1]

			eff, err := c.load(cur)
			if err != nil {
				continue
			}

			for _, dep := range c.deps(eff) {
				tip := r(dep)
				if _, visited := seen[tip]; !visited {
					seen[tip] = struct{}{}
					stack = append(stack, tip)
				}
			}
		}

		c.fallback[from] = seen
	}

	_, found := seen[target]

	return found
}
