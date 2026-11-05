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
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

func TestNestedSavepointsPreserveSubscriptions(t *testing.T) {
	e := newHorizonTestEngine()
	c := e.NewContext()
	emit := func(peer uint64) {
		t.Helper()

		if err := c.Emit(&pb.Effect{
			Key: []byte("key"),
			Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{
				SubscriberNodeId: peer,
			}},
		}); err != nil {
			t.Fatal(err)
		}
	}

	for _, peer := range []uint64{2, 3, 4} {
		emit(peer)
	}

	first := c.TakeSavepoint()

	emit(5)

	second := c.TakeSavepoint()
	c.RestoreSavepoint(first)
	emit(6)
	c.RestoreSavepoint(second)

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	if got := e.PeerSubscribers("key"); !slices.Equal(got, []pb.NodeID{2, 3, 4, 5}) {
		t.Fatalf("restore changed saved subscription operations: %v", got)
	}

	// Restoring and flushing again must not mutate the retained savepoint.
	c.RestoreSavepoint(second)

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	if got := e.PeerSubscribers("key"); !slices.Equal(got, []pb.NodeID{2, 3, 4, 5}) {
		t.Fatalf("repeated restore changed subscription operations: %v", got)
	}
}

func TestContextReleasesLargeCommandState(t *testing.T) {
	e := newHorizonTestEngine()
	c := e.NewContext()
	empty := c.TakeSavepoint()

	// A large aborted command must not leave its map buckets on a reused
	// context. Subsequent emission must also work after an empty restore.
	for range 128 {
		if err := c.Emit(dataEffect(e.generateTxnID())); err != nil {
			t.Fatal(err)
		}
	}

	c.Abort()

	if c.keys != nil {
		t.Fatal("large aborted command retained its key map")
	}

	c.RestoreSavepoint(empty)

	if err := c.Emit(dataEffect("next")); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	if e.index.Contains("next") == nil {
		t.Fatal("reused context did not publish the next command")
	}
}
