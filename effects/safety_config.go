package effects

import (
	"cmp"
	"fmt"
	"slices"

	pb "github.com/swytchdb/engine/cluster/proto"
)

const (
	ClusterSafetyKey = "__swytch::dc"
	KeySafetyKey     = "__swytch::kc"
)

func ParseSafetyMode(value string) (SafetyMode, error) {
	switch value {
	case "safe":
		return SafeMode, nil
	case "unsafe":
		return UnsafeMode, nil
	default:
		return SafeMode, fmt.Errorf("invalid safety mode %q: expected safe or unsafe", value)
	}
}

func (m SafetyMode) String() string {
	if m == UnsafeMode {
		return "unsafe"
	}

	return "safe"
}

// ClusterSafetyMode decodes the ordinary string key. Absence restores safe.
func ClusterSafetyMode(state *pb.ReducedEffect) (SafetyMode, error) {
	if state == nil || state.GetOp() == pb.EffectOp_REMOVE_OP {
		return SafeMode, nil
	}

	if state.GetCollection() != pb.CollectionKind_SCALAR ||
		(state.GetTypeTag() != pb.ValueType_TYPE_UNSPECIFIED && state.GetTypeTag() != pb.ValueType_TYPE_STRING) {
		return SafeMode, fmt.Errorf("%s must be a string", ClusterSafetyKey)
	}

	if state.GetScalar() == nil {
		return SafeMode, nil
	}

	return ParseSafetyMode(string(state.GetScalar().GetRaw()))
}

// KeySafetyModes decodes hash fields as Redis glob patterns, not an ordered
// rule list. Ordering is derived by SafetyRules, identically on every node.
func KeySafetyModes(state *pb.ReducedEffect) (map[string]SafetyMode, error) {
	modes := make(map[string]SafetyMode)
	if state == nil || state.GetOp() == pb.EffectOp_REMOVE_OP {
		return modes, nil
	}

	if tag := state.GetTypeTag(); tag != pb.ValueType_TYPE_UNSPECIFIED && tag != pb.ValueType_TYPE_HASH {
		return nil, fmt.Errorf("%s must be a hash", KeySafetyKey)
	}

	if state.GetCollection() != pb.CollectionKind_KEYED && state.GetTypeTag() != pb.ValueType_TYPE_HASH {
		if state.GetScalar() == nil && state.GetTypeTag() == pb.ValueType_TYPE_UNSPECIFIED {
			return modes, nil
		}

		return nil, fmt.Errorf("%s must be a hash", KeySafetyKey)
	}

	for pattern, element := range state.GetNetAdds() {
		mode, err := ParseSafetyMode(string(element.GetData().GetRaw()))
		if err != nil {
			return nil, fmt.Errorf("%s field %q: %w", KeySafetyKey, pattern, err)
		}

		modes[pattern] = mode
	}

	return modes, nil
}

// SafetyRules orders exact patterns before globs, then longer literal prefixes
// before shorter ones. Lexical order breaks ties; map iteration never decides.
func SafetyRules(modes map[string]SafetyMode) []KeyRangeRule {
	rules := make([]KeyRangeRule, 0, len(modes))
	for pattern, mode := range modes {
		rules = append(rules, KeyRangeRule{Pattern: pattern, Mode: mode})
	}

	slices.SortFunc(rules, func(a, b KeyRangeRule) int {
		ap, ae := safetyPatternPrefix(a.Pattern)

		bp, be := safetyPatternPrefix(b.Pattern)
		if ae != be {
			if ae {
				return -1
			}

			return 1
		}

		if ap != bp {
			return cmp.Compare(bp, ap)
		}

		return cmp.Compare(a.Pattern, b.Pattern)
	})

	return rules
}

func safetyPatternPrefix(pattern string) (int, bool) {
	length := 0

	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '*', '?', '[':
			return length, false
		case '\\':
			if i+1 < len(pattern) {
				i++
			}
		}

		length++
	}

	return length, true
}

// SafetyModeForKey reports the active projection, including pinned system keys.
func (e *Engine) SafetyModeForKey(key string) SafetyMode {
	return e.modeForKey(key)
}

// Subscribe announces interest even in a currently absent key, and collects
// existing peer tips before returning. Ordinary read misses may skip this.
func (e *Engine) Subscribe(key string) error {
	return e.ensureSubscribedBlocking(key)
}

// SetOnKeyChanged installs an index-change observer. It includes metadata,
// snapshots and backfill, unlike the data-insertion wakeup callback. The hook
// must only signal work: it runs synchronously and must not reenter the engine.
// FlushKey signals an index-wide flush. Installation is safe during ingestion.
func (e *Engine) SetOnKeyChanged(hook func(string)) {
	if hook == nil {
		e.keyChanged.Store(nil)

		return
	}

	e.keyChanged.Store(&hook)
}

func (e *Engine) notifyKeyChanged(key string) {
	if hook := e.keyChanged.Load(); hook != nil {
		(*hook)(key)
	}
}
