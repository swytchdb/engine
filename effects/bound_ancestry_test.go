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
)

func TestInstallBoundSubdags_ReusesLocalAncestry(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)
	parents := make(map[string]Tip)

	const binds = 128
	for i := range binds {
		bind := closureBind(log, fmt.Sprint(i), 1, []string{"winner", "other"}, parents)
		parents["winner"], parents["other"] = bind, bind
	}

	if err := e.Subscribe("other"); err != nil {
		t.Fatal(err)
	}

	e.updateIndex("other", e.index.Contains("other"), parents["other"])
	frontier := e.index.Contains("other")

	root := newDag(e, "winner", "")
	if err := root.prepare([]Tip{parents["winner"]}); err != nil {
		t.Fatal(err)
	}

	before := e.effectCache.hits.Load()
	e.installBoundSubdags(root)

	if reads := e.effectCache.hits.Load() - before; reads > 10*binds {
		t.Fatalf("repeated the same associated-key history: %d reads for %d binds", reads, binds)
	}

	if e.index.Contains("other") != frontier {
		t.Fatal("covered ancestors changed the live frontier")
	}
}

func TestInstallBoundSubdags_ValidatesNewBranches(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprintf("missing=%v", missing), func(t *testing.T) {
			log := newSnapshotLog()
			e := newSnapshotEngine(log)
			base := closureBind(log, "base", 1, []string{"winner", "other"}, nil)

			parents := map[string]Tip{"winner": base, "other": base}
			if missing {
				parents["other"] = Tip{99, 1}
			}

			branch := closureBind(log, "new", 2, []string{"winner", "other"}, parents)

			if err := e.Subscribe("other"); err != nil {
				t.Fatal(err)
			}

			e.updateIndex("other", e.index.Contains("other"), base)
			frontier := e.index.Contains("other")

			root := newDag(e, "winner", "")
			if err := root.prepare([]Tip{branch}); err != nil {
				t.Fatal(err)
			}

			e.installBoundSubdags(root)

			if missing {
				if e.index.Contains("other") != frontier {
					t.Fatal("incomplete branch changed the live frontier")
				}
			} else if !e.index.Contains("other").Contains(branch) {
				t.Fatal("new reachable branch was not installed")
			}
		})
	}
}
