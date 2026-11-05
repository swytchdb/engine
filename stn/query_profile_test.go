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

package stn

import (
	"encoding/json"
	"os"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"
)

// Optional profiling of the unchanged real-engine scaling fixture. Build is
// outside the profile so Network.Upper's query cost is isolated from Connect.
func TestProfileRealHistoryQueries(t *testing.T) {
	path := os.Getenv("STN_PROFILE_HISTORY")
	if path == "" {
		t.Skip("set STN_PROFILE_HISTORY to an emitted-history fixture")
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	var h struct {
		Groups []struct {
			Domains int
			Edges   []struct{ Before, After Sample }
		}
	}
	if err := json.NewDecoder(f).Decode(&h); err != nil {
		t.Fatal(err)
	}

	runtime.GOMAXPROCS(1)

	c, err := NewClocks(1000)
	if err != nil {
		t.Fatal(err)
	}

	first := h.Groups[0].Edges[0].Before

	var last Sample

	for _, g := range h.Groups {
		if g.Domains > 1000 {
			break
		}

		for _, o := range g.Edges {
			c, err = c.Before(o.Before, o.After)
			if err != nil {
				t.Fatal(err)
			}

			last = o.After
		}
	}

	out, err := os.Create(os.Getenv("STN_PROFILE_OUT"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = out.Close() }()

	if err := pprof.StartCPUProfile(out); err != nil {
		t.Fatal(err)
	}

	start := time.Now()

	var lo, hi int64

	queries := 0
	for queries < 100 || time.Since(start) < time.Second {
		lo, hi, err = c.Difference(first, last)
		if err != nil {
			t.Fatal(err)
		}

		queries++
	}

	elapsed := time.Since(start)

	pprof.StopCPUProfile()
	t.Logf("%d real-history queries: %s (%s/query), final bounds [%d,%d]", queries, elapsed, elapsed/time.Duration(queries), lo, hi)
}
