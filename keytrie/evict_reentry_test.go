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

package keytrie

import (
	"testing"
	"time"
)

func TestEvictionCallbacksCanEvict(t *testing.T) {
	for _, hook := range []string{"notify", "refDelta"} {
		t.Run(hook, func(t *testing.T) {
			c := NewCritbit[struct{}]()

			t.Cleanup(func() { closeTestTrie(t, c) })

			c.Insert("a", nil, NewTipSet(tip(1)))
			c.Insert("b", nil, NewTipSet(tip(2)))

			nested := false
			reenter := func() {
				if !nested {
					nested = true

					c.EvictBatch(1)
				}
			}

			if hook == "notify" {
				c.SetEvictHooks(nil, func(string, []EffectRef, *struct{}) { reenter() }, nil)
			} else {
				c.SetRefDelta(func(_, _ []EffectRef, _ *struct{}) { reenter() })
			}

			done := make(chan struct{})

			go func() {
				c.EvictBatch(1)
				close(done)
			}()

			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("eviction callback deadlocked while re-entering eviction")
			}

			if !nested || c.Size() != 0 {
				t.Fatalf("nested eviction did not remove both keys: size=%d", c.Size())
			}
		})
	}
}
