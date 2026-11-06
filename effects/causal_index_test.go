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

func TestCausalIndex_AllSmallDAGs(t *testing.T) {
	// Enumerate every five-vertex DAG. An independent transitive-closure
	// oracle checks both reachability and the candidate intervals, including
	// diamonds, merges, disconnected components, duplicate tips, and ties.
	const vertices = 5
	for edges := range 1 << (vertices * (vertices - 1) / 2) {
		log := newSnapshotLog()
		e := &Engine{effectCache: log.effectCache}

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

			// Decreasing offsets and repeated node IDs must not imply order.
			tips[i] = Tip{uint64(i%2 + 1), uint64(100 - i*3)}
			if err := log.effectCache.Put(tips[i], &pb.Effect{
				Key: []byte("k"), NodeId: tips[i][0], Hlc: sTs(int64(i)), Deps: deps,
				Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("v"))},
			}); err != nil {
				t.Fatal(err)
			}

			reaches[i][i] = true
		}

		for k := range vertices {
			for i := range vertices {
				for j := range vertices {
					reaches[i][j] = reaches[i][j] || reaches[i][k] && reaches[k][j]
				}
			}
		}

		selected := append(slices.Clone(tips[:]), tips[0], tips[2])

		for range 2 {
			index := newCausalIndex(e, "k")
			before := e.effectCache.hits.Load()
			chains := index.candidateChains(selected)

			for i, from := range tips {
				for j, target := range tips {
					if got := index.reaches(from, target); got != reaches[i][j] {
						t.Fatalf("edges=%b %v -> %v: got %v, want %v", edges, from, target, got, reaches[i][j])
					}
				}

				concurrent := make(map[int]bool)

				for _, chain := range chains {
					first, last := index.concurrentRange(from, chain, selected)
					for _, candidate := range chain[first:last] {
						concurrent[candidate] = true
					}
				}

				for candidate, target := range selected {
					j := slices.Index(tips[:], target)

					want := !reaches[i][j] && !reaches[j][i]
					if concurrent[candidate] != want {
						t.Fatalf("edges=%b concurrent %v/%v: got %v, want %v", edges, from, target, concurrent[candidate], want)
					}
				}
			}

			if got := e.effectCache.hits.Load() - before; got != vertices {
				t.Fatalf("fetched %d vertices, want exactly %d", got, vertices)
			}

			slices.Reverse(selected)
		}
	}
}

func TestCausalIndex_IncompleteOrCyclicHistory(t *testing.T) {
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

			index := newCausalIndex(e, "k")
			if !index.reaches(first, second) || index.reaches(second, first) != cycle {
				t.Fatal("missing history or cycle changed reachability")
			}

			if index.reaches(first, Tip{2, 100}) || index.reaches(second, Tip{2, 100}) {
				t.Fatal("invented ancestry through a missing effect or a cycle")
			}

			if index.cyclic != cycle {
				t.Fatalf("cycle fallback=%v, want %v", index.cyclic, cycle)
			}
		})
	}
}

func TestCausalIndex_ExtendsForConflictReporting(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)
	a := closureBind(log, "a", 1, []string{"winner"}, nil)
	b := closureBind(log, "b", 2, []string{"winner"}, nil)
	index := newCausalIndex(e, "winner")
	tips := []Tip{a, b}
	chains := index.candidateChains(tips)
	// Reporting may discover a head from another key after candidate indexing.
	head := closureBind(log, "later", 3, []string{"elsewhere"}, map[string]Tip{"elsewhere": a})
	if !index.reaches(head, a) || index.reaches(a, head) || index.reaches(head, b) {
		t.Fatal("late reporting head has incorrect ancestry")
	}

	var got []int

	for _, chain := range chains {
		first, last := index.concurrentRange(a, chain, tips)
		got = append(got, chain[first:last]...)
	}

	if !slices.Equal(got, []int{1}) {
		t.Fatalf("late head changed candidate intervals: %v", got)
	}
}

func TestCausalIndex_MergesReuseChains(t *testing.T) {
	const writers = 128

	e, tips := concurrentHistory(100, writers, 8, true)

	index := newCausalIndex(e, "winner")
	if !index.add(tips...) {
		t.Fatal("acyclic history used fallback")
	}

	if len(index.heads) > writers {
		t.Fatalf("chain count grew past writer concurrency: %d", len(index.heads))
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
