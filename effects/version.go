package effects

import "github.com/swytchdb/engine/internal/compatversion"

// MinimumVersion is the application's cluster compatibility floor. A
// broadcaster without version negotiation keeps the floor unknown.
func (e *Engine) MinimumVersion() string {
	if e.broadcaster == nil {
		return compatversion.Normalize(e.buildVersion)
	}

	if provider, ok := e.broadcaster.(interface{ MinimumVersion() string }); ok {
		return compatversion.Normalize(provider.MinimumVersion())
	}

	return ""
}

func (e *Engine) needsVerdictEvidence() bool {
	return !compatversion.AtLeast(e.MinimumVersion(), "1.8.0")
}

func (e *Engine) transactionBroadcaster() TransactionBroadcaster {
	if !compatversion.AtLeast(e.MinimumVersion(), "1.8.0") {
		return nil
	}

	b, _ := e.broadcaster.(TransactionBroadcaster)

	return b
}
