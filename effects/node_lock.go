package effects

import "sync"

// takeNodeLock orders local bind publication against the promises made by
// incoming bind responses. Either the remote bind is recorded before our final
// conflict check, or our bind is published on every touched key before the
// incoming handler constructs its response. A better fork-choice hash cannot
// override a promise already made to a conflicting transaction.
//
// This is one lock per Engine, not per key or per process. Keep the protocol
// boundary behind these helpers so its implementation can change without
// changing that ordering guarantee.
//
// The lock is not reentrant. Hooks called while holding it (including local
// effect and key-change hooks) must not enter a path that takes it, directly or
// by waiting for another goroutine. Dependency fetches may read remote effect
// caches, but must not depend on bind processing or transaction completion.
// Release it before replicating a bind or waiting for replication responses.
func (e *Engine) takeNodeLock() *nodeLockGuard {
	e.nodeMu.Lock()

	return &nodeLockGuard{mu: &e.nodeMu}
}

func (c *Context) takeNodeLock() *nodeLockGuard {
	return c.engine.takeNodeLock()
}

// A guard belongs to its acquiring goroutine. release is nil-safe and
// idempotent so an explicit release before networking can coexist with deferred
// release on every error path. Do not copy or share guards between goroutines.
type nodeLockGuard struct {
	mu *sync.Mutex
}

func (g *nodeLockGuard) release() {
	if g != nil && g.mu != nil {
		g.mu.Unlock()
		g.mu = nil
	}
}
