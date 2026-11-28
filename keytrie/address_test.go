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
	"slices"
	"strings"
	"testing"
)

func TestInternalAddressFraming(t *testing.T) {
	for _, pair := range [][2]string{
		{"", ""},
		{"animals", "root"},
		{"a", "bc"},
		{"ab", "c"},
		{"a\x00\xff", "\x01\xa7\x5c"},
		{strings.Repeat("k", 128), strings.Repeat("i", 256)},
	} {
		address := InternalKey(pair[0], pair[1])

		key, internal, ok := splitInternalKey(address)
		if !ok || key != pair[0] || internal != pair[1] {
			t.Fatalf("address did not round trip: %q => %q, %q, %v", pair, key, internal, ok)
		}

		for n := range len(address) {
			if _, _, ok := splitInternalKey(address[:n]); ok {
				t.Fatalf("accepted truncated address at %d", n)
			}
		}
	}

	if InternalKey("a", "bc") == InternalKey("ab", "c") {
		t.Fatal("component boundary collision")
	}

	if got, want := InternalKey("a", "b"), "\x01\xa7\x5c\x01a\xa7\x5c\x01b\xa7\x5c"; got != want {
		t.Fatalf("frame = %q, want %q", got, want)
	}

	for _, bad := range []string{
		"", "\x01", InternalKey("a", "b") + "x", "x" + InternalKey("a", "b"),
		"\x01\xa7\x5c\x80\x00\xa7\x5c\x00\xa7\x5c",               // Noncanonical length.
		"\x01\xa7\x5c" + strings.Repeat("\xff", 11) + "\xa7\x5c", // Overflow.
		"\x01\xa7\x5c\x7fshort\xa7\x5c\x00\xa7\x5c",              // Length beyond frame.
	} {
		if _, _, ok := splitInternalKey(bad); ok {
			t.Fatalf("accepted malformed frame %q", bad)
		}
	}
}

func TestInternalAddressNamespaces(t *testing.T) {
	c := NewCritbit[int]()

	t.Cleanup(func() { closeTestTrie(t, c) })

	address := InternalKey("animals", "root")
	logicalTips, internalTips := NewTipSet(tip(1)), NewTipSet(tip(2))
	c.InsertInternal(address, nil, internalTips)

	if c.Contains(address) != nil {
		t.Fatal("application lookup recognized an internal address")
	}

	c.Insert(address, nil, logicalTips)

	if c.Contains(address) != logicalTips || c.ContainsInternal(address) != internalTips {
		t.Fatal("identical key bytes crossed namespaces")
	}

	if !slices.Equal(c.Keys(), []string{address}) || c.Size() != 1 || c.SizeInternal() != 2 {
		t.Fatal("logical scan did not preserve the application's binary key")
	}

	logicalSnapshot := c.Snapshot().(*Critbit[int])

	t.Cleanup(func() { closeTestTrie(t, logicalSnapshot) })

	if logicalSnapshot.Contains(address) != logicalTips || logicalSnapshot.ContainsInternal(address) != nil {
		t.Fatal("logical snapshot changed namespace")
	}

	fullSnapshot := c.SnapshotInternal().(*Critbit[int])

	t.Cleanup(func() { closeTestTrie(t, fullSnapshot) })

	if fullSnapshot.Contains(address) != logicalTips || fullSnapshot.ContainsInternal(address) != internalTips {
		t.Fatal("internal snapshot lost a namespace")
	}

	if !c.Delete(address, logicalTips) || c.ContainsInternal(address) != internalTips {
		t.Fatal("application deletion touched the internal key")
	}

	if !c.DeleteInternal(address, internalTips) || c.SizeInternal() != 0 {
		t.Fatal("internal deletion failed")
	}
}

func FuzzInternalAddress(f *testing.F) {
	f.Add("animals", "root")
	f.Add("", "")
	f.Add("\x00\xff", "\x01\xa7\x5c")
	f.Fuzz(func(t *testing.T, key, internal string) {
		encoded := InternalKey(key, internal)

		gotKey, gotInternal, ok := splitInternalKey(encoded)
		if !ok || gotKey != key || gotInternal != internal {
			t.Fatal("address failed to round trip")
		}
		// Arbitrary strings must either be rejected or have one canonical encoding.
		if k, i, ok := splitInternalKey(key); ok && InternalKey(k, i) != key {
			t.Fatal("accepted a noncanonical frame")
		}
	})
}

func TestLogicalKeyAddressView(t *testing.T) {
	for _, key := range []string{"", "animals", "\x00\xff", strings.Repeat("x", 300)} {
		for _, address := range []string{LogicalAddress(key), InternalKey(key, "node")} {
			buffer := []byte(address)
			if got := LogicalKey(buffer); got == nil || string(got) != key {
				t.Fatalf("owner of %q = %q", address, got)
			}

			if got := LogicalKeyString(address); got != key {
				t.Fatalf("string owner of %q = %q", address, got)
			}

			if n := testing.AllocsPerRun(100, func() {
				if string(LogicalKey(buffer)) != key || LogicalKeyString(address) != key {
					panic("wrong owner")
				}
			}); n != 0 {
				t.Fatalf("owner extraction allocated %g times", n)
			}
		}
	}

	for _, bad := range [][]byte{nil, {}, {1}, {1, 0xa7, 0x5c}, {1, 0xa7, 0x5c, 127}, {2, 0xa7, 0x5c, 0}} {
		if got := LogicalKey(bad); got != nil {
			t.Fatalf("accepted invalid header %x", bad)
		}
	}
}

func TestByteAddressOwnership(t *testing.T) {
	for _, name := range []string{"", "\x00\xff", strings.Repeat("k", 1024)} {
		key := []byte(name)

		address := LogicalAddressBytes(key)
		if string(address) != LogicalAddress(name) {
			t.Fatal("byte constructor changed logical address")
		}

		if len(key) > 0 {
			key[0] ^= 0xff

			if string(WireKeyBytes(address)) != name {
				t.Fatal("logical address aliases its input")
			}
		}

		for _, encoded := range []string{LogicalAddress(name), InternalKey(name, "node")} {
			buffer := []byte(encoded)

			view := WireKeyBytes(buffer)
			if string(view) != WireKey(encoded) {
				t.Fatal("byte wire projection changed identity")
			}

			if len(view) > 0 {
				buffer[len(buffer)-1] ^= 0xff
				if view[len(view)-1] != buffer[len(buffer)-1] {
					t.Fatal("wire projection copied its input")
				}
			}
		}
	}
}
