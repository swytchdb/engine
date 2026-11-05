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

import pb "github.com/swytchdb/engine/cluster/proto"

// tipsOnCausalPath proves that every distinct tip lies on a single dependency
// path. It counts target tips on the longest path through each vertex, taking
// the maximum across branches rather than summing concurrent ancestors.
// Each vertex and edge is visited once, with O(vertices) scratch space.
// Missing history or a cycle declines the optimization.
func (e *Engine) tipsOnCausalPath(key string, tips []Tip) bool {
	if len(tips) < 2 {
		return true
	}

	targets := make(map[Tip]struct{}, len(tips))
	for _, tip := range tips {
		targets[tip] = struct{}{}
	}

	if len(targets) == 1 {
		return true
	}

	type frame struct {
		tip     Tip
		deps    []*pb.EffectRef
		longest int
	}

	// -1 marks an active DFS frame; nonnegative values are completed paths.
	lengths := make(map[Tip]int)

	var stack []frame

	for _, tip := range tips {
		if _, done := lengths[tip]; done {
			continue
		}

		stack = append(stack, frame{tip: tip})
		for len(stack) > 0 {
			f := &stack[len(stack)-1]
			if _, started := lengths[f.tip]; !started {
				eff, err := e.getEffect(key, f.tip)
				if err != nil {
					return false
				}

				lengths[f.tip] = -1
				f.deps = eff.GetDeps()
			}

			if len(f.deps) > 0 {
				dep := r(f.deps[0])

				length, done := lengths[dep]
				if !done {
					stack = append(stack, frame{tip: dep})

					continue
				}

				if length < 0 {
					return false
				}

				f.longest = max(f.longest, length)
				f.deps = f.deps[1:]

				continue
			}

			if _, target := targets[f.tip]; target {
				f.longest++
			}

			if f.longest == len(targets) {
				return true
			}

			lengths[f.tip] = f.longest
			stack = stack[:len(stack)-1]
		}
	}

	return false
}
