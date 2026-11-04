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
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

func BenchmarkDAGPrepare(b *testing.B) {
	for _, width := range []int{1, 4} {
		for _, depth := range []int{16, 256} {
			b.Run(fmt.Sprintf("width=%d/depth=%d", width, depth), func(b *testing.B) {
				e := newEngine(EngineConfig{})

				b.Cleanup(func() {
					if err := e.Close(); err != nil {
						b.Error(err)
					}
				})

				var tips []Tip

				for level := range depth {
					next := make([]Tip, 0, width)

					for branch := range width {
						tip := Tip{uint64(branch + 1), uint64(level + 1)}
						eff := &pb.Effect{Key: []byte("key"), Deps: toPbRefs(tips), ForkChoiceHash: []byte{byte(branch)}}

						if err := e.effectCache.Put(tip, eff); err != nil {
							b.Fatal(err)
						}

						next = append(next, tip)
					}

					tips = next
				}

				b.ReportAllocs()
				b.ResetTimer()

				for b.Loop() {
					d := newDag(e, "key", "")
					if err := d.prepare(tips); err != nil {
						b.Fatal(err)
					}

					if len(d.topoOrder) != width*depth {
						b.Fatal("incomplete traversal")
					}
				}
			})
		}
	}
}
