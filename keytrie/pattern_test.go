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
