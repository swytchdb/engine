package effects

import (
	"runtime"
	"slices"
	"sync/atomic"
)

// contextChange records one pending key mutation. Previous states share slice
// prefixes: forward emission only appends, and restoration clips capacities
// before a saved state can be extended again.
type contextChange struct {
	prev          *contextChange
	depth         uint64
	key           string
	before, after *contextKey
	tip           Tip // zero for metadata changes
	pool          *VertexPool
	published     bool
	refs          atomic.Int32 // context head, successor link, and saved positions
}

// ContextSavepoint retains a position in the pending history. It does not
// snapshot committed state or undo values already observed by a transaction.
// A savepoint can be restored repeatedly, including after Flush or Abort.
type ContextSavepoint struct {
	state *savedContextState
}

// Keep cleanup on the shared state so copying the public savepoint value does
// not release its history when the original handle becomes unreachable.
type savedContextState struct {
	position *contextChange
	readErr  error
}

// TakeSavepoint captures pending state in constant time, without cloning keys
// or queued effects. Unreachable savepoints release their retained history.
func (c *Context) TakeSavepoint() *ContextSavepoint {
	state := &savedContextState{position: c.pending, readErr: c.readErr}
	if state.position != nil {
		state.position.refs.Add(1)
		runtime.AddCleanup(state, releaseContextHistory, state.position)
	}

	return &ContextSavepoint{state: state}
}

// RestoreSavepoint undoes/replays only the changes between the current and saved
// positions. Discarded, unpublished effects become eligible for reclamation
// when neither the context nor a retained savepoint needs them.
func (c *Context) RestoreSavepoint(sp *ContextSavepoint) {
	if sp == nil {
		return
	}

	var target *contextChange

	c.readErr = nil

	if sp.state != nil {
		target = sp.state.position
		c.readErr = sp.state.readErr
	}

	if target != nil {
		target.refs.Add(1)
	}

	runtime.KeepAlive(sp) // cleanup must not release target before we retain it

	current, saved := c.pending, target

	var replay []*contextChange

	for current != saved {
		if current != nil && (saved == nil || current.depth >= saved.depth) {
			c.restoreKey(current.key, current.before)
			current = current.prev
		} else {
			replay = append(replay, saved)
			saved = saved.prev
		}
	}

	for _, r := range slices.Backward(replay) {
		c.restoreKey(r.key, r.after)
	}

	previous := c.pending
	c.pending = target

	releaseContextHistory(previous)
}

func (c *Context) restoreKey(key string, state *contextKey) {
	if state == nil {
		delete(c.keys, key)

		return
	}

	copyState := *state
	copyState.notifies = slices.Clip(state.notifies)
	copyState.elementIDs = slices.Clip(state.elementIDs)
	copyState.subOps = slices.Clip(state.subOps)

	if c.keys == nil {
		c.keys = make(map[string]*contextKey)
	}

	c.keys[key] = &copyState
}

func (c *Context) recordChange(key string, before, after *contextKey, tip Tip) {
	change := &contextChange{
		prev: c.pending, depth: 1, key: key, before: before, after: after,
		tip: tip, pool: c.engine.effectCache,
	}
	change.refs.Store(1)

	if c.pending != nil {
		change.depth += c.pending.depth
	}
	// The context's previous head reference transfers to the successor link.
	c.pending = change
	if after == nil {
		delete(c.keys, key)
	} else {
		after.change = change

		if c.keys == nil {
			c.keys = make(map[string]*contextKey)
		}

		c.keys[key] = after
	}
}

// Publication transfers creation ownership to the indexed chain. Saved history
// retains its separate pin until released. Re-publication can follow compaction
// or eviction, so restore any creation ownership those operations released.
func (c *Context) publishPending(state *contextKey) {
	for state != nil && state.change != nil {
		change := state.change
		if change.tip != (Tip{}) && change.pool != nil {
			change.pool.acquireCreation(change.key, change.tip)
			change.published = true
		}

		state = change.before
	}
}

func (c *Context) releasePending() {
	previous := c.pending
	c.pending = nil

	releaseContextHistory(previous)
}

// A retained prefix can share an array with discarded appends. Go scans the
// entire backing array, even beyond len/cap of that prefix; clear our appended
// slots so they cannot keep reclaimed payloads alive off the pool's books.
// No live successor can use these slots once the record's last owner is gone.
func clearContextAppends(previous, after *contextKey) {
	if after == nil {
		return
	}

	var before contextKey
	if previous != nil {
		before = *previous
	}

	clear(after.notifies[len(before.notifies):])
	clear(after.elementIDs[len(before.elementIDs):])
	clear(after.subOps[len(before.subOps):])
}

// Only the last owner touches a released record. Atomic ownership allows Go's
// savepoint cleanup to run concurrently with emission, rollback, and Flush.
func releaseContextHistory(change *contextChange) {
	for change != nil && change.refs.Add(-1) == 0 {
		if change.tip != (Tip{}) && change.pool != nil {
			if !change.published {
				change.pool.decrefChainNode(change.key, change.tip)
			}

			change.pool.decref(change.tip)
		}

		clearContextAppends(change.before, change.after)
		previous := change.prev
		change.prev, change.before, change.after, change.pool = nil, nil, nil, nil
		change = previous
	}
}
