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
	"context"
	"fmt"
	"testing"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
)

func historicalTestWrite(t *testing.T, e *Engine, values map[string]string, transactional bool) {
	t.Helper()

	c := e.NewContext()
	if transactional {
		c.BeginTx()
	}

	for k, value := range values {
		if err := c.Emit(&pb.Effect{Key: []byte(k), Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte(value))}}); err != nil {
			t.Fatal(err)
		}
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
}

// For one domain the cut is exact and does not need cross-clock assumptions.
// This deliberately exercises the selector and existing reconstruction before
// wiring the new snapshot policy into client-facing Context reads.
func historicalTestRead(t *testing.T, e *Engine, at time.Time, keys ...string) []*pb.ReducedEffect {
	t.Helper()

	ctx := context.Background()
	frontier := func(key string) ([]Tip, error) {
		return e.historicalFrontier(ctx, key, e.resolveTipDeps(e.index.Contains(key).Tips()), func(_ Tip, eff *pb.Effect) (bool, error) {
			if eff.GetNodeId() != uint64(e.NodeID()) {
				return false, fmt.Errorf("test cut has no foreign clock mapping")
			}

			return !eff.GetHlc().AsTime().After(at), nil
		})
	}

	v, err := e.historicalReadView(ctx, keys, frontier)
	if err != nil {
		t.Fatal(err)
	}

	results := make([]*pb.ReducedEffect, len(keys))
	for i, k := range keys {
		tips := append(append([]Tip(nil), v.frontiers[k].resolved...), v.anchors[k]...)

		results[i], _, err = e.reconstructWithView(k, tips, "", false, v)
		if err != nil {
			t.Fatal(err)
		}
	}

	return results
}

func TestHistoricalSelectsEarlierValueAndAbsence(t *testing.T) {
	e := NewEngine(EngineConfig{DefaultMode: UnsafeMode})
	defer func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	}()

	historicalTestWrite(t, e, map[string]string{"a": "old"}, false)
	cut := e.clock.Now()
	historicalTestWrite(t, e, map[string]string{"a": "new", "later": "created"}, false)

	for range 2 {
		got := historicalTestRead(t, e, cut, "a", "later")
		if string(got[0].GetScalar().GetRaw()) != "old" || got[1] != nil {
			t.Fatalf("historical results: %v", got)
		}
	}

	if live, _, _, err := e.GetSnapshot("a"); err != nil || string(live.GetScalar().GetRaw()) != "new" {
		t.Fatalf("historical read changed live state: %v %v", live, err)
	}
}

func TestHistoricalBindVisibilityAndCompaction(t *testing.T) {
	e := NewEngine(EngineConfig{DefaultMode: UnsafeMode})
	defer func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	}()

	historicalTestWrite(t, e, map[string]string{"a": "old-a", "b": "old-b"}, true)

	cut := e.clock.Now()
	for i := range 8 {
		historicalTestWrite(t, e, map[string]string{"a": fmt.Sprint(i), "b": fmt.Sprint(i)}, true)
	}
	// Ask the normal Context read path to produce real compaction snapshots.
	for _, key := range []string{"a", "b"} {
		c := e.NewContext()
		if _, _, err := c.GetSnapshot(key); err != nil {
			t.Fatal(err)
		}

		if err := c.Flush(); err != nil {
			t.Fatal(err)
		}
	}

	got := historicalTestRead(t, e, cut, "b", "a")
	if string(got[0].GetScalar().GetRaw()) != "old-b" || string(got[1].GetScalar().GetRaw()) != "old-a" {
		t.Fatalf("partial or newer bind visible: %v", got)
	}
}
