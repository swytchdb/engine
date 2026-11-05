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
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

func TestTipsOnCausalPath_AllSmallDAGs(t *testing.T) {
	// Enumerate every four-vertex DAG in one topological ordering and every
	// subset of target tips. The independent oracle computes transitive
	// closure and checks pairwise comparability, including forks and merges.
	const vertices = 4
	for edges := range 1 << (vertices * (vertices - 1) / 2) {
		log := newSnapshotLog()
		e := newSnapshotEngine(log)

		var tips [vertices]Tip

		var reaches [vertices][vertices]bool

		bit := 0

		for i := range vertices {
			var deps []*pb.EffectRef

			for j := range i {
				if edges&(1<<bit) != 0 {
					deps = append(deps, toPbRef(tips[j]))
					reaches[i][j] = true
				}

				bit++
			}

			tips[i] = log.putEffect(&pb.Effect{
				Key: []byte("k"), NodeId: 1, Hlc: sTs(int64(i)), Deps: deps,
				Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("v"))},
			})
		}

		for k := range vertices {
			for i := range vertices {
				for j := range vertices {
					reaches[i][j] = reaches[i][j] || reaches[i][k] && reaches[k][j]
				}
			}
		}

		for subset := range 1 << vertices {
			var selected []Tip

			want := true

			for i := range vertices {
				if subset&(1<<i) == 0 {
					continue
				}

				selected = append(selected, tips[i], tips[i]) // duplicate bind tips
				for j := range i {
					if subset&(1<<j) != 0 && !reaches[i][j] && !reaches[j][i] {
						want = false
					}
				}
			}

			for range 2 {
				if got := e.tipsOnCausalPath("k", selected); got != want {
					t.Fatalf("edges=%b subset=%b tips=%v: got %v, want %v", edges, subset, selected, got, want)
				}

				slices.Reverse(selected)
			}
		}
	}
}

func TestTipsOnCausalPath_IncompleteOrCyclicHistory(t *testing.T) {
	for _, cycle := range []bool{false, true} {
		t.Run(fmt.Sprintf("cycle=%v", cycle), func(t *testing.T) {
			log := newSnapshotLog()
			e := newSnapshotEngine(log)

			first, second := Tip{1, 100}, Tip{1, 200}
			if err := log.effectCache.Put(first, &pb.Effect{Deps: []*pb.EffectRef{toPbRef(second)}}); err != nil {
				t.Fatal(err)
			}

			if cycle {
				if err := log.effectCache.Put(second, &pb.Effect{Deps: []*pb.EffectRef{toPbRef(first)}}); err != nil {
					t.Fatal(err)
				}
			}

			if e.tipsOnCausalPath("k", []Tip{first, second}) {
				t.Fatal("incomplete or cyclic history must use the general resolver")
			}
		})
	}
}

func TestLosersOnKey_SequentialHistoryPreservesLostVerdicts(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)

	var tip Tip

	for i := range 128 {
		parents := make(map[string]Tip)
		if i > 0 {
			parents["winner"] = tip
		}

		tip = closureBind(log, fmt.Sprint(i), byte(128-i), []string{"winner"}, parents)
	}

	for _, verdicts := range []verdictMap{nil, {
		"0": {verdict: pb.Verdict_LOST},
		"1": {verdict: pb.Verdict_WON},
	}} {
		losers, walked := e.losersOnKeyAt("winner", []Tip{tip}, nil, verdicts, nil, true)
		if walked != 256 {
			t.Fatalf("walked %d effects, want 256", walked)
		}

		if verdicts == nil {
			if len(losers) != 0 {
				t.Fatalf("sequential history has losers: %v", losers)
			}
		} else if _, lost := losers["0"]; !lost || len(losers) != 1 {
			t.Fatalf("lost verdict not preserved: %v", losers)
		}
	}
}

func BenchmarkLosersOnKeySequentialHistory(b *testing.B) {
	for _, n := range []int{100, 1000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			log := newSnapshotLog()
			e := newSnapshotEngine(log)

			var tip Tip

			for i := range n {
				parents := make(map[string]Tip)
				if i > 0 {
					parents["winner"] = tip
				}

				tip = closureBind(log, fmt.Sprint(i), byte(i%255+1), []string{"winner"}, parents)
			}

			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				losers, _ := e.losersOnKeyAt("winner", []Tip{tip}, nil, nil, nil, false)
				if len(losers) != 0 {
					b.Fatal(losers)
				}
			}
		})
	}
}
