/*
 * Copyright 2026 Swytch Labs BV
 *
 * This file is part of Swytch.
 *
 * Swytch is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 */

package effects

import "testing"

func TestSafetyConfigurationIsOwnedByEngine(t *testing.T) {
	rules := []KeyRangeRule{{Pattern: "tenant:*", Mode: SafeMode}}
	e := NewEngine(EngineConfig{KeyRangeRules: rules})
	defer func() { _ = e.Close() }()

	if got := e.modeForKey("unmatched"); got != SafeMode {
		t.Fatalf("zero-value default mode = %v, want SafeMode", got)
	}

	rules[0].Mode = UnsafeMode
	if got := e.modeForKey("tenant:key"); got != SafeMode {
		t.Fatalf("mutating EngineConfig rules changed running mode to %v", got)
	}

	updated := []KeyRangeRule{{Pattern: "tenant:*", Mode: SafeMode}}
	e.UpdateSafetyRules(SafeMode, updated)
	updated[0].Mode = UnsafeMode
	if got := e.modeForKey("tenant:key"); got != SafeMode {
		t.Fatalf("mutating updated rules changed running mode to %v", got)
	}

	e.UpdateSafetyRules(UnsafeMode, nil)
	if got := e.modeForKey("tenant:key"); got != UnsafeMode {
		t.Fatalf("explicit UnsafeMode update = %v, want UnsafeMode", got)
	}
}

func TestUpdateSafetyRulesPreservesSystemKeyException(t *testing.T) {
	e := NewEngine(EngineConfig{})
	defer func() { _ = e.Close() }()

	e.UpdateSafetyRules(SafeMode, []KeyRangeRule{{Pattern: "__swytch:*", Mode: SafeMode}})
	if got := e.modeForKey("__swytch:members"); got != UnsafeMode {
		t.Fatalf("system key mode = %v, want UnsafeMode", got)
	}
}
