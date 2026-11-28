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
	"errors"
	"fmt"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/keytrie"
)

// GraphTip identifies the observed node and graph-descriptor frontiers. An empty
// Tips slice means a new node; GraphTips must identify an observed descriptor.
// Key is the main logical key. The effect uses its complete internal address
// in both Key and IKey.
type GraphTip struct {
	Key       string
	Tips      []Tip
	GraphTips []Tip
}

// EmitGraph emits a graph node using the descriptor frontier the caller observed.
// Node effects use ordinary exact-key subscriptions and replication.
// Emit's ownership contract applies. Key and IKey both carry the node's full address.
func (c *Context) EmitGraph(deps GraphTip, v *pb.Effect) error {
	if v == nil || len(v.GetIKey()) == 0 || len(deps.GraphTips) == 0 {
		return errors.New("EmitGraph requires an internal address and observed graph frontier")
	}

	if err := prepareEffectAddress(v); err != nil {
		return err
	}

	key := string(v.GetIKey())
	if !isInternalAddress(key) || keytrie.LogicalKeyString(key) != deps.Key {
		return errors.New("EmitGraph address does not belong to the descriptor")
	}

	if v.GetData() == nil && v.GetMeta() == nil {
		return errors.New("EmitGraph accepts data or metadata observations")
	}

	descriptor := logicalAddress(deps.Key)

	c.engine.pinHistory(&c.history)

	if c.readErr != nil {
		return c.readErr
	}

	var subscribeErr error
	if c.inTx {
		subscribeErr = c.engine.ensureSubscribedBlocking(descriptor)
	} else {
		subscribeErr = c.engine.ensureSubscribed(descriptor)
	}

	if subscribeErr != nil {
		return subscribeErr
	}

	for _, group := range []struct {
		key  string
		tips []Tip
	}{{descriptor, deps.GraphTips}, {key, deps.Tips}} {
		for _, tip := range group.tips {
			eff, err := c.engine.getEffect(group.key, tip)
			if err != nil {
				return err
			}

			if !effectHasAddress(eff, group.key) {
				return fmt.Errorf("frontier %v does not belong to the observed graph address", tip)
			}
		}
	}

	view, err := c.engine.historicalReadView(c.TraceCtx(), []string{descriptor}, func(key string) ([]Tip, error) {
		if key == descriptor {
			return deps.GraphTips, nil
		}

		return nil, nil
	}, nil)
	if err != nil {
		return err
	}

	state, _, err := c.engine.reconstructWithView(descriptor, deps.GraphTips, c.txnID, false, view)
	if err != nil {
		return err
	}

	if state.GetTypeTag() != pb.ValueType_TYPE_GRAPH {
		return errors.New("observed descriptor is not a graph")
	}

	// A non-nil empty frontier prevents Emit from substituting the live index.
	tips := append(make([]Tip, 0, len(deps.Tips)), deps.Tips...)

	return c.Emit(v, tips)
}

// isInternalAddress validates the graph API address namespace.
func isInternalAddress(key string) bool { return len(key) > 0 && key[0] != keytrie.LogicalKeyPrefix[0] }
