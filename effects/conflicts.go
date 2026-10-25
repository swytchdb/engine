package effects

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"

	pb "github.com/swytchdb/engine/cluster/proto"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	conflictKeyPrefix      = "__swytch::conflict"
	conflictDocumentPrefix = "__swytch::conflicts::"
	conflictIndexPrefix    = "__swytch::conflict::"
	conflictDirectory      = "__swytch::conflict::__swytch"
)

// Identifiers are strings so JSON readers do not round uint64 node IDs.
type conflictRef struct {
	Node   string `json:"node"`
	Offset string `json:"offset"`
}

func conflictReference(t Tip) conflictRef {
	return conflictRef{strconv.FormatUint(t[0], 10), strconv.FormatUint(t[1], 10)}
}

type conflictTransaction struct {
	Version        int           `json:"version"`
	TxID           string        `json:"txid"`
	Bind           conflictRef   `json:"bind"`
	MapKeyEncoding string        `json:"state_map_key_encoding"`
	Keys           []conflictKey `json:"keys"`
}

type conflictKey struct {
	Key          []byte            `json:"key"`
	ConsumedTips []conflictRef     `json:"consumed_tips"`
	Before       json.RawMessage   `json:"before"`
	After        json.RawMessage   `json:"after"`
	Effects      []json.RawMessage `json:"effects"`
}

// conflictHeads treats certifying snapshots as wrappers around their bind.
// Application effects remain heads: depending on a bind resolves its report,
// even if an old bind or snapshot is still a redundant index tip.
func (e *Engine) conflictHeads(key string, tips []Tip) []Tip {
	var heads []Tip

	seen := make(map[Tip]bool)

	stack := append([]Tip(nil), tips...)
	for len(stack) > 0 {
		t := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if seen[t] {
			continue
		}

		seen[t] = true

		eff, err := e.getEffect(key, t)
		if err == nil && eff.GetSnapshot() != nil && len(eff.GetSnapshot().GetTxnVerdicts()) > 0 {
			stack = append(stack, fromPbRefs(eff.GetDeps())...)

			continue
		}

		heads = append(heads, t)
	}

	return heads
}

func (e *Engine) unresolvedConflictTip(key string, bind Tip, tips []Tip, reaches func(Tip, Tip) bool) bool {
	heads := e.conflictHeads(key, tips)
	if !slices.Contains(heads, bind) {
		return false
	}

	eff, err := e.getEffect(key, bind)
	if err != nil {
		return false
	}
	// A dependency on any associated key resolves this envelope, including
	// when another key still indexes its old WON snapshot.
	for _, kb := range eff.GetTxnBind().GetKeys() {
		heads = append(heads, e.conflictHeads(string(kb.GetKey()), e.index.Contains(string(kb.GetKey())).Tips())...)
	}

	for _, head := range heads {
		if head != bind && reaches(head, bind) {
			return false
		}
	}

	return true
}

// hasConflictDocument reads the ordinary JSON document. There is no separate
// seen set. Concurrent readers may both miss and emit; their duplicates remain
// independently identified list elements and are intentionally preserved.
func (e *Engine) hasConflictDocument(txn string) (bool, error) {
	snapshot, _, _, err := e.GetSnapshot(conflictDocumentPrefix + txn)
	if err != nil {
		return false, err
	}

	return snapshot.GetScalar() != nil, nil
}

type conflictDocumentCheck struct {
	exists bool
	err    error
}

// checks belongs to one reporting pass. Independent observations still read
// the ordinary documents and may emit the intentionally preserved duplicates.
func (e *Engine) emitConflict(key string, a, b Tip, checks map[string]conflictDocumentCheck) error {
	if checks == nil {
		checks = make(map[string]conflictDocumentCheck)
	}

	binds := []Tip{a, b}
	envelopes := make([]*pb.Effect, 2)
	covered := make([]bool, 2)

	for i, bind := range binds {
		eff, err := e.getEffect(key, bind)
		if err != nil {
			return err
		}

		envelopes[i] = eff
	}

	for i := range binds {
		txn := envelopes[i].GetTxnId()

		check, ok := checks[txn]
		if !ok {
			check.exists, check.err = e.hasConflictDocument(txn)
			checks[txn] = check
		}

		if check.err != nil {
			return check.err
		}

		covered[i] = check.exists
	}

	if covered[0] && covered[1] {
		return nil
	}

	// Build complete documents before emitting anything. A missing historical
	// effect leaves the document absent so a later read can retry.
	documents := make([][]byte, len(binds))
	for i, bind := range binds {
		if covered[i] {
			continue
		}

		txn := conflictTransaction{Version: 1, TxID: envelopes[i].GetTxnId(), Bind: conflictReference(bind), MapKeyEncoding: "hex"}
		for _, kb := range envelopes[i].GetTxnBind().GetKeys() {
			state := conflictKey{Key: kb.GetKey()}
			for _, tip := range fromPbRefs(kb.GetConsumedTips()) {
				state.ConsumedTips = append(state.ConsumedTips, conflictReference(tip))
			}

			if err := e.describeConflictKey(envelopes[i], kb, &state); err != nil {
				return err
			}

			txn.Keys = append(txn.Keys, state)
		}

		data, err := json.Marshal(txn)
		if err != nil {
			return err
		}

		documents[i] = data
	}

	ctx := e.NewContext()

	for i, envelope := range envelopes {
		if !covered[i] {
			if err := emitConflictDocument(ctx, conflictDocumentPrefix+envelope.GetTxnId(), documents[i]); err != nil {
				return err
			}
		}
	}

	if err := ctx.Flush(); err != nil {
		return err
	}

	// Match the ordinary document gate even if a later notification flush
	// fails: these documents have already been published.
	for _, envelope := range envelopes {
		checks[envelope.GetTxnId()] = conflictDocumentCheck{exists: true}
	}

	keys := make(map[string]struct{})

	for _, envelope := range envelopes {
		for _, kb := range envelope.GetTxnBind().GetKeys() {
			if err := e.appendConflictValue(ctx, conflictIndexPrefix+string(kb.GetKey()), []byte(envelope.GetTxnId())); err != nil {
				return err
			}

			keys[string(kb.GetKey())] = struct{}{}
		}
	}

	if err := ctx.Flush(); err != nil {
		return err
	}

	for key := range keys {
		if err := e.appendConflictValue(ctx, conflictDirectory, []byte(key)); err != nil {
			return err
		}
	}

	return ctx.Flush()
}

// Each document is one JSON scalar containing the transaction's immutable
// evidence. Repeated observations write the same value, not a report history.
func emitConflictDocument(ctx *Context, key string, value []byte) error {
	if err := ctx.Emit(&pb.Effect{Key: []byte(key), Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{TypeTag: pb.ValueType_TYPE_JSON}}}); err != nil {
		return err
	}

	return ctx.Emit(&pb.Effect{
		Key: []byte(key), Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op: pb.EffectOp_INSERT_OP, Merge: pb.MergeRule_LAST_WRITE_WINS, Collection: pb.CollectionKind_SCALAR,
			Value: &pb.DataEffect_Raw{Raw: value},
		}},
	})
}

func (e *Engine) appendConflictValue(ctx *Context, key string, value []byte) error {
	if err := ctx.Emit(&pb.Effect{Key: []byte(key), Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{TypeTag: pb.ValueType_TYPE_LIST}}}); err != nil {
		return err
	}

	id := e.nextOffset()

	return ctx.Emit(&pb.Effect{
		Key: []byte(key), Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op: pb.EffectOp_INSERT_OP, Merge: pb.MergeRule_LAST_WRITE_WINS, Collection: pb.CollectionKind_ORDERED,
			Placement: pb.Placement_PLACE_TAIL, Id: []byte(fmt.Sprintf("%d:%d", id[0], id[1])), Value: &pb.DataEffect_Raw{Raw: value},
		}},
	})
}

func (e *Engine) describeConflictKey(bind *pb.Effect, kb *pb.TransactionalBindEffect_KeyBind, result *conflictKey) error {
	operations, err := e.collectBindEffects(bind, string(kb.GetKey()))
	if err != nil {
		return err
	}

	for _, operation := range operations {
		cloned := proto.Clone(operation).(*pb.Effect)
		if state := cloned.GetSnapshot().GetState(); state != nil {
			sanitizeConflictState(state)
		}

		encoded, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(cloned)
		if err != nil {
			return err
		}

		result.Effects = append(result.Effects, encoded)
	}

	before, err := e.conflictHistoricalState(string(kb.GetKey()), fromPbRefs(kb.GetConsumedTips()))
	if err != nil {
		return err
	}

	result.Before, err = marshalConflictState(before)
	if err != nil {
		return err
	}

	result.After, err = marshalConflictState(ReduceChain(before, operations))

	return err
}

// conflictHistoricalState reconstructs only the recorded causal closure. Live
// index tips, present-day horizon state, and today's expiry filtering must not
// erase the losing transaction's historical inputs or outputs. No tips are
// installed, no reports are generated, and the recorded expiry metadata stays
// in the returned state.
func (e *Engine) conflictHistoricalState(key string, tips []Tip) (*pb.ReducedEffect, error) {
	view := &snapshotReadView{engine: e, historical: true, resolved: map[string][]Tip{key: tips}, lost: make(map[string]struct{})}

	var err error

	view.keys, view.anchors, view.verdicts, err = e.bindKeyClosureFrom([]string{key}, func(k string) ([]Tip, error) { return view.resolved[k], nil }, true)
	if err != nil {
		return nil, err
	}

	for txn, entry := range view.verdicts {
		if entry.verdict == pb.Verdict_LOST {
			view.lost[txn] = struct{}{}
		}
	}

	for k := range view.keys {
		losers, _ := e.losersOnKeyAt(k, view.resolved[k], view.anchors[k], view.verdicts, nil, false)
		for txn := range losers {
			view.lost[txn] = struct{}{}
		}
	}

	state, _, err := e.reconstructWithView(key, tips, "", false, view)

	return state, err
}

func marshalConflictState(state *pb.ReducedEffect) (json.RawMessage, error) {
	if state == nil {
		return json.RawMessage("null"), nil
	}

	cloned := proto.Clone(state).(*pb.ReducedEffect)
	sanitizeConflictState(cloned)

	return protojson.MarshalOptions{UseProtoNames: true}.Marshal(cloned)
}

// Proto JSON requires UTF-8 map keys. Match the wire state's hex convention,
// including nested virtual partitions, while protobuf bytes stay base64.
func sanitizeConflictState(state *pb.ReducedEffect) {
	sanitizeReducedKeys(state)

	if len(state.GetPartitions()) == 0 {
		return
	}

	partitions := make(map[string]*pb.ReducedEffect, len(state.GetPartitions()))
	for key, partition := range state.GetPartitions() {
		sanitizeConflictState(partition)
		partitions[hex.EncodeToString([]byte(key))] = partition
	}

	state.Partitions = partitions
}
