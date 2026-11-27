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
	GraphAlgorithmKey  = "algorithm"
	GraphEntryPointKey = "entry_point"

	// GraphRegionsKey names the node partition containing algorithm-defined labels.
	GraphRegionsKey = "regions"
)

// NewGraphEffects builds the initialization effects for a new graph descriptor.
// The caller checks that the key is available and emits these effects through
// the normal engine context, which supplies timestamps and causal dependencies.
// This helper neither publishes effects nor provides atomicity by itself.
//
// The root has an opaque algorithm scalar and a child marker for the entry-point
// array. Entry points reuse virtual partitions and ORDERED elements with raw node
// identities as values. Node identities are not EffectRefs or causal dependencies.
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
				Merge: MergeRule_LAST_WRITE_WINS, Id: []byte(GraphEntryPointKey),
				Value: &DataEffect_Child{Child: []byte(GraphEntryPointKey)},
			}},
		},
		{
			Key: bytes.Clone(key), Virtual: []byte(GraphEntryPointKey),
			Kind: &Effect_Meta{Meta: &MetaEffect{TypeTag: ValueType_TYPE_JSON_ARRAY}},
		},
	}
}
