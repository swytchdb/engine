package effects

import (
	"bytes"
	"fmt"

	pb "github.com/swytchdb/engine/cluster/proto"
)

// HandleTransaction receives a complete set of tentative effects before
// admitting the bind. Temporary pins protect the transfer from reclamation;
// an incomplete transfer releases them before bind or ownership publication.
// Effects are stored without per-record frontier updates. HandleRemote then
// performs one shared closure operation for the bind's served projections.
func (e *Engine) HandleTransaction(notify *pb.OffsetNotify, receive TransactionReceiver) ([]*pb.NackNotify, error) {
	if notify.GetOrigin() == nil || receive == nil || e.effectCache == nil {
		return nil, fmt.Errorf("invalid transaction batch")
	}

	bind, err := parseWireEffect(notify.GetEffectData())
	if err != nil {
		return nil, err
	}

	if bind.GetTxnBind() == nil || bind.GetTxnId() == "" {
		return nil, fmt.Errorf("transaction batch has no bind")
	}

	if !bytes.Equal(bind.GetForkChoiceHash(), ComputeForkChoiceHash(pb.NodeID(bind.GetNodeId()), bind.GetHlc())) {
		return nil, fmt.Errorf("transaction bind hash mismatch")
	}

	records := make(map[Tip]*pb.Effect)

	var order []Tip

	defer func() {
		for tip := range records {
			e.effectCache.decref(tip)
		}
	}()

	if err := receive(func(record *pb.FetchResponse) error {
		if record.GetRef() == nil || len(record.GetEffectData()) == 0 {
			return fmt.Errorf("transaction record missing identity or data")
		}

		tip := r(record.GetRef())
		if _, duplicate := records[tip]; duplicate || tip == r(notify.GetOrigin()) {
			return fmt.Errorf("duplicate transaction record %v", tip)
		}

		eff, err := parseWireEffect(record.GetEffectData())
		if err != nil {
			return err
		}

		if eff.GetTxnId() != bind.GetTxnId() || eff.GetTxnBind() != nil || eff.GetNodeId() != tip[0] ||
			!bytes.Equal(eff.GetForkChoiceHash(), ComputeForkChoiceHash(pb.NodeID(eff.GetNodeId()), eff.GetHlc())) {
			return fmt.Errorf("invalid effect in transaction batch: %v", tip)
		}

		if err := e.effectCache.putWithPin(tip, eff, false, nil, true); err != nil {
			return err
		}

		records[tip] = eff
		order = append(order, tip)

		return nil
	}); err != nil {
		return nil, err
	}

	// Every bound tip must have arrived in this transfer. A partial batch
	// must not fall through to closure's older-history missing boundaries.
	for _, kb := range bind.GetTxnBind().GetKeys() {
		eff := records[r(kb.GetNewTip())]
		if eff == nil || !effectHasAddress(eff, string(kb.GetKey())) {
			return nil, fmt.Errorf("transaction batch omitted bound tip %v", kb.GetNewTip())
		}
	}

	for _, tip := range order {
		eff := records[tip]
		if err := e.putIngested(tip, eff, 0); err != nil {
			return nil, err
		}

		if eff.GetNoop() == nil && e.isServed(eff.GetIKey()) {
			e.pendingTxTips.Store(tip, fromPbRefs(eff.GetDeps()))
		}

		if sub := eff.GetSubscription(); sub != nil && eff.GetNodeId() != uint64(e.nodeID) {
			key := string(eff.GetIKey())
			if sub.GetUnsubscribe() {
				e.removePeerSubscriber(key, pb.NodeID(eff.GetNodeId()))
			} else {
				e.addPeerSubscriber(key, pb.NodeID(eff.GetNodeId()))
			}
		}
	}

	return e.HandleRemote(notify)
}
