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
	"bytes"
	"fmt"
	"slices"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
)

// prepareEffectAddress is the ingress boundary for local and received effects.
// After it returns, IKey and bind keys are complete trie addresses; Key remains
// the wire/application identity. Legacy effects are normalized once here.
func prepareEffectAddress(eff *pb.Effect) error {
	if len(eff.GetIKey()) == 0 {
		eff.IKey = keytrie.LogicalAddressBytes(eff.GetKey())
		for _, kb := range eff.GetTxnBind().GetKeys() {
			kb.Key = keytrie.LogicalAddressBytes(kb.GetKey())
		}

		return nil
	}

	if keytrie.LogicalKey(eff.GetIKey()) == nil {
		return fmt.Errorf("invalid internal key address")
	}

	if !bytes.Equal(eff.GetKey(), keytrie.WireKeyBytes(eff.GetIKey())) {
		return fmt.Errorf("effect key does not match its internal address")
	}

	return nil
}

func logicalAddress(key string) string { return keytrie.LogicalAddress(key) }
func wireKey(key string) string        { return keytrie.WireKey(key) }

func (e *Engine) subscription(key string) (*subscriptionState, bool) {
	return e.subscriptions.Load(key)
}

// nackKey resolves legacy reply names at the transport boundary. Mixed binds
// use canonical addresses because their projected names need not be unique.
func (e *Engine) nackKey(nack *pb.NackNotify) string {
	key := string(nack.GetKey())
	if request, ok := e.pendingBootstraps.Load(r(nack.GetConflicting())); ok {
		return request.key
	}
	if e.effectCache != nil {
		if eff, ok := e.effectCache.Get(r(nack.GetConflicting())); ok {
			if eff.GetTxnBind() != nil && !logicalOnlyBind(eff) {
				return key
			}
			if string(eff.GetKey()) == key {
				return string(eff.GetIKey())
			}
			for _, kb := range eff.GetTxnBind().GetKeys() {
				if bytes.Equal(keytrie.WireKeyBytes(kb.GetKey()), nack.GetKey()) {
					return string(kb.GetKey())
				}
			}
		}
	}
	return logicalAddress(key)
}

func (e *Engine) nackWireKey(key string, conflicting *pb.EffectRef) []byte {
	if e.effectCache != nil {
		if eff, ok := e.effectCache.Get(r(conflicting)); ok && eff.GetTxnBind() != nil && !logicalOnlyBind(eff) {
			return []byte(key)
		}
	}
	return []byte(wireKey(key))
}

// effectHasAddress includes shared binds in every key they bind.
func effectHasAddress(eff *pb.Effect, key string) bool {
	if bind := eff.GetTxnBind(); bind != nil {
		return slices.ContainsFunc(bind.GetKeys(), func(k *pb.TransactionalBindEffect_KeyBind) bool { return string(k.GetKey()) == key })
	}

	return string(eff.GetIKey()) == key
}
