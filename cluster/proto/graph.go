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

package proto

import "bytes"

// Canonical fields of a TYPE_GRAPH descriptor. Container fields also name
// their virtual partitions within the descriptor's logical key.
const (
	GraphAlgorithmKey           = "algorithm"
	GraphRegionSubscriptionsKey = "region_subscriptions"
	GraphEntryPointKey          = "entry_point"

	// GraphRegionsKey names the node partition containing algorithm-defined labels.
	GraphRegionsKey = "regions"
)

// NewGraphEffects builds the initialization effects for a new graph descriptor.
// The caller checks that the key is available and emits these effects through
// the normal engine context, which supplies timestamps and causal dependencies.
// This helper neither publishes effects nor provides atomicity by itself.
//
// The root has three named fields: an opaque algorithm scalar, a child marker
// for the region subscription map, and a child marker for the entry-point array.
// Both containers reuse virtual partitions, as JSON does. Directory mutations
// use KEYED elements with independent subscription IDs; entry-point mutations
// use ORDERED elements with independent list element IDs and raw node identities
// as values. Node identities are not EffectRefs or child-partition markers.
//
// Subscription entries use "<decimal peer ID>/<hex subscription ID>" as their
// keyed ID and point to a child partition of label-to-regex fields. Fields are
// ANDed, entries are ORed, and patterns match whole label values. Node labels
// live in the node's GraphRegionsKey partition. Algorithm identifiers are opaque.
// This helper initializes storage; Context.EmitGraph performs predicate routing.
// Inputs are copied so callers can reuse their buffers after construction.
func NewGraphEffects(key, algorithm []byte) []*Effect {
	return []*Effect{
		{
			Key:  bytes.Clone(key),
			Kind: &Effect_Meta{Meta: &MetaEffect{TypeTag: ValueType_TYPE_GRAPH}},
		},
		{
			Key: bytes.Clone(key),
			Kind: &Effect_Data{Data: &DataEffect{
				Op: EffectOp_INSERT_OP, Collection: CollectionKind_KEYED,
				Merge: MergeRule_LAST_WRITE_WINS, Id: []byte(GraphAlgorithmKey),
				Value: &DataEffect_Raw{Raw: bytes.Clone(algorithm)},
			}},
		},
		{
			Key: bytes.Clone(key),
			Kind: &Effect_Data{Data: &DataEffect{
				Op: EffectOp_INSERT_OP, Collection: CollectionKind_KEYED,
				Merge: MergeRule_LAST_WRITE_WINS, Id: []byte(GraphRegionSubscriptionsKey),
				Value: &DataEffect_Child{Child: []byte(GraphRegionSubscriptionsKey)},
			}},
		},
		{
			Key: bytes.Clone(key),
			Kind: &Effect_Data{Data: &DataEffect{
				Op: EffectOp_INSERT_OP, Collection: CollectionKind_KEYED,
				Merge: MergeRule_LAST_WRITE_WINS, Id: []byte(GraphEntryPointKey),
				Value: &DataEffect_Child{Child: []byte(GraphEntryPointKey)},
			}},
		},
		{
			Key: bytes.Clone(key), Virtual: []byte(GraphRegionSubscriptionsKey),
			Kind: &Effect_Meta{Meta: &MetaEffect{TypeTag: ValueType_TYPE_JSON_OBJECT}},
		},
		{
			Key: bytes.Clone(key), Virtual: []byte(GraphEntryPointKey),
			Kind: &Effect_Meta{Meta: &MetaEffect{TypeTag: ValueType_TYPE_JSON_ARRAY}},
		},
	}
}
