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
	"maps"
	"slices"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

// concurrentHistory models a hot key after replication: a shared prefix,
// independently authored transaction branches, and optional fan-in between
// rounds. A writer's node ID does not imply any dependency on another writer.
func concurrentHistory(prefix, writers, rounds int, merge bool) (*Engine, []Tip) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)

	parents := make(map[string]Tip)
	for i := range prefix {
		parents["winner"] = closureBind(log, fmt.Sprintf("prefix-%d", i), 255, []string{"winner"}, parents)
	}

	tips := make([]Tip, writers)
	for round := range rounds {
		for writer := range writers {
			log.nodeID = uint64(writer + 2)

			deps := parents
			if round > 0 && !merge {
				deps = map[string]Tip{"winner": tips[writer]}
			}

			tips[writer] = closureBind(log, fmt.Sprintf("writer-%d-%d", writer, round), byte(writer+1), []string{"winner"}, deps)
		}

		if merge {
			deps := make([]*pb.EffectRef, len(tips))
			for i, tip := range tips {
				deps[i] = toPbRef(tip)
			}

			parents["winner"] = log.putEffect(&pb.Effect{
				Key: []byte("winner"), NodeId: log.nodeID, Hlc: sTs(int64(log.nextOff)),
				Deps: deps, Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("merge"))},
			})
		}
	}

	return e, tips
}

func TestLosersOnKey_ConcurrentHistory(t *testing.T) {
	for _, merge := range []bool{false, true} {
		t.Run(fmt.Sprintf("merge=%v", merge), func(t *testing.T) {
			const writers, rounds = 16, 8

			e, tips := concurrentHistory(100, writers, rounds, merge)
			before := e.effectCache.hits.Load()
			losers, _ := e.losersOnKeyAt("winner", tips, nil, nil, nil, false)
			// Bound history reads structurally, rather than using a timing
			// assertion. Rebuilding per-candidate ancestry exceeds this bound.
			if reads := e.effectCache.hits.Load() - before; reads > 10*(100+writers*rounds) {
				t.Fatalf("repeated history traversal: %d effect reads", reads)
			}

			if len(losers) != (writers-1)*rounds {
				t.Fatalf("got %d losers, want %d", len(losers), (writers-1)*rounds)
			}

			for writer := range writers {
				for round := range rounds {
					txn := fmt.Sprintf("writer-%d-%d", writer, round)
					if _, lost := losers[txn]; lost != (writer != 0) {
						t.Fatalf("%s lost=%v", txn, lost)
					}
				}
			}
		})
	}
}

func BenchmarkLosersOnKeyConcurrentHistory(b *testing.B) {
	for _, tc := range []struct {
		name    string
		prefix  int
		writers int
		rounds  int
		merge   bool
	}{
		{"shared-prefix/128", 1000, 128, 1, false},
		{"independent-branches/128", 0, 128, 8, false},
		{"replicated-waves/128", 0, 128, 8, true},
		{"replicated-waves/256", 0, 256, 8, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			e, tips := concurrentHistory(tc.prefix, tc.writers, tc.rounds, tc.merge)

			b.ReportAllocs()

			for b.Loop() {
				losers, _ := e.losersOnKeyAt("winner", tips, nil, nil, nil, false)
				if len(losers) == 0 {
					b.Fatal("concurrent transactions were not adjudicated")
				}
			}
		})
	}
}

type predicateCandidate struct {
	txn      string
	bind     Tip
	observes bool
	writes   bool
	observed int
	written  int
	round    int
	hash     byte
}

// predicateHistory combines append-only writes, observing consumers, and
// observation-only transactions. Redis stream binds currently use the
// conservative no-predicate path exercised by concurrentHistory; this fixture
// also protects the engine's predicate-aware callers from over-adjudication.
func predicateHistory(writers, rounds int, legacy bool) (*Engine, []Tip, []predicateCandidate) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)
	tips := make([]Tip, writers)
	candidates := make([]predicateCandidate, 0, writers*rounds)

	var base Tip

	for round := range rounds {
		for writer := range writers {
			log.nodeID = uint64(writer + 2)

			candidate := predicateCandidate{
				txn:      fmt.Sprintf("txn-%d-%d", writer, round),
				observes: writer%3 != 0, writes: writer%3 != 2,
				observed: writer % 8, written: (writer / 3) % 8,
				round: round, hash: byte(writer%250 + 1),
			}
			if legacy && writer%7 == 0 {
				candidate.observes, candidate.writes = false, false
			}

			var deps []*pb.EffectRef
			if round > 0 {
				deps = []*pb.EffectRef{toPbRef(base)}
			}

			if candidate.observes {
				observation := log.putEffect(&pb.Effect{
					Key: []byte("winner"), NodeId: log.nodeID, Hlc: sTs(int64(log.nextOff)),
					TxnId: candidate.txn, Deps: deps,
					Kind: &pb.Effect_Observation{Observation: &pb.ObservationEffect{
						Predicate: &pb.Predicate{
							Kind: pb.Predicate_CMP, Col: 0, Op: pb.PredicateCmpOp_CMP_EQ,
							Literal: &pb.TypedValue{Kind: pb.TypedValue_INT, IntVal: int64(candidate.observed)},
						},
					}},
				})
				deps = []*pb.EffectRef{toPbRef(observation)}
			}

			if candidate.writes {
				write := log.putEffect(&pb.Effect{
					Key: []byte("winner"), NodeId: log.nodeID, Hlc: sTs(int64(log.nextOff)),
					TxnId: candidate.txn, Deps: deps,
					Kind: &pb.Effect_RowWrite{RowWrite: &pb.RowWriteEffect{
						Kind:    pb.RowWriteEffect_INSERT,
						Columns: []*pb.TypedValue{{Kind: pb.TypedValue_INT, IntVal: int64(candidate.written)}},
					}},
				})
				deps = []*pb.EffectRef{toPbRef(write)}
			}

			parents := make(map[string]Tip)
			if len(deps) > 0 {
				parents["winner"] = r(deps[0])
			}

			candidate.bind = closureBind(log, candidate.txn, candidate.hash, []string{"winner"}, parents)
			tips[writer] = candidate.bind
			candidates = append(candidates, candidate)
		}

		deps := make([]*pb.EffectRef, len(tips))
		for i, tip := range tips {
			deps[i] = toPbRef(tip)
		}

		base = log.putEffect(&pb.Effect{
			Key: []byte("winner"), NodeId: log.nodeID, Hlc: sTs(int64(log.nextOff)), Deps: deps,
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		})
	}

	return e, tips, candidates
}

func TestLosersOnKey_ConcurrentPredicatesMatchOracle(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, adjudicated := range []bool{false, true} {
			t.Run(fmt.Sprintf("legacy=%v/adjudicated=%v", legacy, adjudicated), func(t *testing.T) {
				e, tips, candidates := predicateHistory(32, 3, legacy)
				verdicts := make(verdictMap)
				invisible := make(map[string]bool)
				want := make(map[string]struct{})

				if adjudicated {
					for i, candidate := range candidates {
						switch {
						case i%11 == 0:
							invisible[candidate.txn] = true
						case i%13 == 0:
							verdicts[candidate.txn] = verdictEntry{verdict: pb.Verdict_LOST}
							want[candidate.txn] = struct{}{}
						default:
							verdicts[candidate.txn] = verdictEntry{verdict: pb.Verdict_WON}
						}
					}
				}

				// The fixture's barriers order all different rounds. Within a
				// round, use plain integer equality as an independent predicate
				// oracle. Candidates are in ascending hash order within each round;
				// only surviving predecessors can eliminate the next candidate.
				for _, a := range candidates {
					if invisible[a.txn] || verdicts[a.txn].verdict == pb.Verdict_LOST {
						continue
					}

					for _, b := range candidates {
						if _, eliminated := want[b.txn]; eliminated {
							continue
						}
						if b.round != a.round || b.hash >= a.hash || invisible[b.txn] || verdicts[b.txn].verdict == pb.Verdict_LOST {
							continue
						}

						if !a.observes && !a.writes || !b.observes && !b.writes ||
							a.observes && b.writes && a.observed == b.written ||
							b.observes && a.writes && b.observed == a.written {
							want[a.txn] = struct{}{}
						}
					}
				}

				for range 2 {
					got, _ := e.losersOnKeyAt("winner", tips, nil, verdicts, func(txn string) bool { return invisible[txn] }, false)
					if !maps.Equal(got, want) {
						t.Fatalf("got %v, want %v", got, want)
					}

					slices.Reverse(tips)
				}
			})
		}
	}
}

func BenchmarkLosersOnKeyConcurrentPredicates(b *testing.B) {
	e, tips, _ := predicateHistory(128, 8, false)

	b.ReportAllocs()

	for b.Loop() {
		e.losersOnKeyAt("winner", tips, nil, nil, nil, false)
	}
}
