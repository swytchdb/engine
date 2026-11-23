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
	"encoding/binary"
	"strings"
)

const (
	internalKeyPrefix = "\x01"
	addressMagic      = "\xa7\x5c"
)

// InternalKey creates an opaque address scoped to a logical key. Construct it
// once when addressing an internal area, then pass the result to the
// *Internal APIs. Application APIs treat every input as a logical key,
// including strings with the same bytes as an internal address.
//
// The format is namespace, magic, key length, key, magic, internal length,
// internal key, magic. Lengths are canonical unsigned varints and both key
// components may contain arbitrary bytes, including empty strings.
func InternalKey(key, internal string) string {
	var address strings.Builder
	address.Grow(1 + 3*len(addressMagic) + 2*binary.MaxVarintLen64 + len(key) + len(internal))
	address.WriteString(internalKeyPrefix)
	address.WriteString(addressMagic)

	var length [binary.MaxVarintLen64]byte

	n := binary.PutUvarint(length[:], uint64(len(key)))
	address.Write(length[:n])
	address.WriteString(key)
	address.WriteString(addressMagic)

	n = binary.PutUvarint(length[:], uint64(len(internal)))
	address.Write(length[:n])
	address.WriteString(internal)
	address.WriteString(addressMagic)

	return address.String()
}

// splitInternalKey recognizes a complete internal address and returns borrowed
// component views. Ordinary application APIs never auto-detect this format.
func splitInternalKey(address string) (key, internal string, ok bool) {
	if !strings.HasPrefix(address, internalKeyPrefix+addressMagic) || !strings.HasSuffix(address, addressMagic) {
		return "", "", false
	}

	key, rest, ok := addressComponent(address[len(internalKeyPrefix+addressMagic):])
	if !ok {
		return "", "", false
	}

	internal, rest, ok = addressComponent(rest)
	if !ok || rest != "" {
		return "", "", false
	}

	return key, internal, true
}

func addressComponent(address string) (value, rest string, ok bool) {
	length, n := binary.Uvarint([]byte(address))
	// Reject overflow, truncation and noncanonical length encodings.
	if n <= 0 || n > 1 && address[n-1] == 0 {
		return "", "", false
	}

	address = address[n:]
	if length > uint64(len(address)) {
		return "", "", false
	}

	end := int(length)
	if !strings.HasPrefix(address[end:], addressMagic) {
		return "", "", false
	}

	return address[:end], address[end+len(addressMagic):], true
}

// LogicalAddress returns the trie address for an application key. Call once at
// the application boundary; internal code passes the address unchanged.
func LogicalAddress(key string) string { return LogicalKeyPrefix + key }

// LogicalAddressBytes returns an owned trie address without an intermediate string.
func LogicalAddressBytes(key []byte) []byte {
	address := make([]byte, len(LogicalKeyPrefix)+len(key))
	copy(address, LogicalKeyPrefix)
	copy(address[len(LogicalKeyPrefix):], key)

	return address
}

// WireKey returns the externally visible key for a trie address. Logical keys
// lose their namespace prefix; graph node addresses remain opaque wire keys.
func WireKey(address string) string { return strings.TrimPrefix(address, LogicalKeyPrefix) }

// WireKeyBytes returns a borrowed wire-key view. Clone it when independent
// ownership is needed; the returned bytes alias address.
func WireKeyBytes(address []byte) []byte {
	if len(address) > 0 && address[0] == LogicalKeyPrefix[0] {
		return address[len(LogicalKeyPrefix):]
	}

	return address
}

// LogicalKey returns the owning application key as a view into address. It is
// used only when the transport explicitly supplies an internal address. Invalid
// headers or lengths return nil. It does not decode the graph-local component.
func LogicalKey(address []byte) []byte {
	if len(address) > 0 && address[0] == LogicalKeyPrefix[0] {
		return address[1:]
	}

	return internalLogicalKey(address)
}

func internalLogicalKey(address []byte) []byte {
	if len(address) < 4 || address[0] != internalKeyPrefix[0] || address[1] != addressMagic[0] || address[2] != addressMagic[1] {
		return nil
	}

	length, n := binary.Uvarint(address[3:])
	if n <= 0 || n > 1 && address[3+n-1] == 0 {
		return nil
	}

	start := 3 + n
	if length > uint64(len(address)-start) {
		return nil
	}

	return address[start : start+int(length)]
}

// LogicalKeyString is the string view of LogicalKey, retaining the original allocation.
func LogicalKeyString(address string) string {
	if len(address) > 0 && address[0] == LogicalKeyPrefix[0] {
		return address[1:]
	}

	return internalLogicalKeyString(address)
}

func internalLogicalKeyString(address string) string {
	if len(address) < 4 || address[0] != internalKeyPrefix[0] || address[1] != addressMagic[0] || address[2] != addressMagic[1] {
		return ""
	}

	length, n := binary.Uvarint([]byte(address[3:]))
	if n <= 0 || n > 1 && address[3+n-1] == 0 {
		return ""
	}

	start := 3 + n
	if length > uint64(len(address)-start) {
		return ""
	}

	return address[start : start+int(length)]
}
