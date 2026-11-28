package compatversion

import "testing"

func TestCompatibilityFloor(t *testing.T) {
	for _, tc := range []struct {
		version string
		modern  bool
	}{
		{"", false},
		{"garbage", false},
		{"1.7.9", false},
		{"v1.8.0-rc.1", false},
		{"1.8.0", true},
		{"v1.8.0+build.5", true},
		{"1.9.0-rc.1", true},
		{"1.10.0", true},
		{"2.0.0", true},
		{"dev", true},
	} {
		t.Run(tc.version, func(t *testing.T) {
			if got := AtLeast(tc.version, "1.8.0"); got != tc.modern {
				t.Fatalf("AtLeast(%q) = %t", tc.version, got)
			}
		})
	}

	for _, tc := range []struct{ a, b, want string }{
		{"dev", "dev", "dev"},
		{"dev", "1.7.9", "v1.7.9"},
		{"dev", "1.8.0", "v1.8.0"},
		{"1.10.0", "v1.9.0", "v1.9.0"},
		{"dev", "", ""},
		{"bad", "1.8.0", ""},
	} {
		if got := Minimum(tc.a, tc.b); got != tc.want {
			t.Errorf("Minimum(%q, %q) = %q, want %q", tc.a, tc.b, got, tc.want)
		}

		if got := Minimum(tc.b, tc.a); got != tc.want {
			t.Errorf("reverse Minimum(%q, %q) = %q, want %q", tc.b, tc.a, got, tc.want)
		}
	}
}
