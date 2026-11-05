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

import "testing"

func TestMatchGlobBacktracking(t *testing.T) {
	for _, tt := range []struct {
		text, pattern string
		want          bool
	}{
		{"", "*", true},
		{"abc", "*bc", true},
		{"ababc", "*ab?", true},
		{"abc", "*[a-c]", true},
		{"abc", "*[^a-b]", true},
		{"abc", "*[^a-c]", false},
		{"abc", "*[", false},
		{"abc", "[abc", false},
		{"abc", "*a[", false},
		{"abc", "*\\", false},
		{"abc*", "*\\*", true},
		{"a\x00b", "*\x00?", true},
	} {
		t.Run(tt.text+"/"+tt.pattern, func(t *testing.T) {
			if got := MatchGlob(tt.text, tt.pattern); got != tt.want {
				t.Fatalf("MatchGlob(%q, %q) = %v, want %v", tt.text, tt.pattern, got, tt.want)
			}
		})
	}
}
