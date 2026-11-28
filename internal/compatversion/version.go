// Package compatversion compares announced application versions for protocol
// compatibility. Unknown versions require legacy behavior; dev is current.
package compatversion

import (
	"strings"

	"golang.org/x/mod/semver"
)

const development = "dev"

func Normalize(version string) string {
	if version == development {
		return version
	}

	version = "v" + strings.TrimPrefix(version, "v")
	if !semver.IsValid(version) {
		return ""
	}

	return semver.Canonical(version)
}

// Minimum includes unknown members and treats dev as newer than releases.
func Minimum(a, b string) string {
	a, b = Normalize(a), Normalize(b)
	if a == "" || b == "" {
		return ""
	}

	if a == development {
		return b
	}

	if b == development || semver.Compare(a, b) <= 0 {
		return a
	}

	return b
}

func AtLeast(version, minimum string) bool {
	version, minimum = Normalize(version), Normalize(minimum)

	return version != "" && minimum != "" &&
		(version == development || minimum != development && semver.Compare(version, minimum) >= 0)
}
