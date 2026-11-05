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
	"strings"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

func TestConflictEffects_ManyWonTipsPreserveAdjudication(t *testing.T) {
	names := make([]string, 16)
	for i := range names {
		names[i] = fmt.Sprintf("txn-%02d", i)
	}

	e, _, _ := conflictLottery(t, names...)
	tips := e.index.Contains("winner").Tips()
	want, _ := e.losersOnKeyAt("winner", tips, nil, nil, nil, false)
	documents := make(map[string]int)
	indexes := make(map[string]int)

	e.SetOnLocalEffect(func(_ Tip, eff *pb.Effect) {
		if eff.GetData() == nil {
			return
		}

		key := string(eff.GetKey())
		if strings.HasPrefix(key, conflictDocumentPrefix) {
			documents[key]++
		}

		if strings.HasPrefix(key, conflictIndexPrefix) {
			indexes[key]++
			// Notifications must not precede the referenced document.
			if key != conflictDirectory && e.index.Contains(conflictDocumentPrefix+string(eff.GetData().GetRaw())) == nil {
				t.Error("notification emitted before its document was published")
			}
		}
	})

	got, _ := e.losersOnKeyAt("winner", tips, nil, nil, nil, true)
	if len(want) != len(names)-1 || !maps.Equal(got, want) {
		t.Fatalf("reporting changed adjudication: got %v, want %v", got, want)
	}

	for _, txn := range names {
		if documents[conflictDocumentPrefix+txn] != 1 {
			t.Fatalf("document for %s emitted %d times", txn, documents[conflictDocumentPrefix+txn])
		}
	}

	before := maps.Clone(indexes)

	e.losersOnKeyAt("winner", tips, nil, nil, nil, true)

	if !maps.Equal(before, indexes) {
		t.Fatal("existing documents did not suppress repeat notifications")
	}
}

func TestConflictEffects_BatchRetriesMissingHistory(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)
	missing := Tip{1, 42}
	addConflictLottery(e, log, "alice", 1, map[string]Tip{"winner": missing})
	addConflictLottery(e, log, "bob", 2, nil)
	addConflictLottery(e, log, "charlie", 3, nil)
	tips := e.index.Contains("winner").Tips()

	e.losersOnKeyAt("winner", tips, nil, nil, nil, true)

	if e.index.Contains(conflictDocumentPrefix+"alice") != nil {
		t.Fatal("incomplete evidence created a document gate")
	}

	for _, txn := range []string{"bob", "charlie"} {
		conflictDocument(t, e, txn)
	}

	if err := log.effectCache.Put(missing, &pb.Effect{
		Key: []byte("winner"), NodeId: 1, Hlc: sTs(1),
		Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("undecided"))},
	}); err != nil {
		t.Fatal(err)
	}

	e.losersOnKeyAt("winner", tips, nil, nil, nil, true)
	conflictDocument(t, e, "alice")
}

func BenchmarkConflictEffectsReportedWonTips(b *testing.B) {
	for _, n := range []int{8, 32} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			log := newSnapshotLog()

			e := newSnapshotEngine(log)
			for i := range n {
				addConflictLottery(e, log, fmt.Sprintf("txn-%02d", i), byte(i+1), nil)
			}

			tips := e.index.Contains("winner").Tips()
			e.losersOnKeyAt("winner", tips, nil, nil, nil, true)
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				e.losersOnKeyAt("winner", tips, nil, nil, nil, true)
			}
		})
	}
}
