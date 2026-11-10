/*
 * Copyright 2026 Swytch Labs BV
 *
 * This file is part of Swytch.
 *
 * Swytch is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Swytch is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Swytch. If not, see <https://www.gnu.org/licenses/>.
 *
 * Additional permissions under GNU AGPL version 3 section 7 apply to
 * this file. See the NOTICE.md file distributed with this source code
 * for the current set of additional permissions.
 */

package effects

import (
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestNormalizeCloudTipCandidatesDropsMissingStaleMarker(t *testing.T) {
	stale := Tip{111, 1}
	realTip := Tip{222, 2}
	realEff := &pb.Effect{Deps: []*pb.EffectRef{toPbRef(stale)}}

	realTips, superseded, err := NormalizeCloudTipCandidates("k", []Tip{stale, realTip}, nil, func(tip Tip) (*pb.Effect, error) {
		if tip == realTip {
			return realEff, nil
		}

		return nil, fmt.Errorf("fetch %v: %w", tip, ErrCDNBlobMissing)
	})
	if err != nil {
		t.Fatalf("normalize candidates: %v", err)
	}

	if !slices.Equal(realTips, []Tip{realTip}) {
		t.Fatalf("real tips = %v, want %v", realTips, []Tip{realTip})
	}

	if !slices.Equal(superseded, []Tip{stale}) {
		t.Fatalf("superseded candidates = %v, want %v", superseded, []Tip{stale})
	}
}

// TestNormalizeCloudTipCandidatesDropsMarkerCoveredByLocalAncestry: a
// candidate already superseded by this node's own local tips — not yet
// reflected in Cloud's index — must drop as superseded too, the same as if
// another candidate had covered it. Local roots seed the walk but never
// classify as superseded themselves: they aren't entries in Cloud's index.
func TestNormalizeCloudTipCandidatesDropsMarkerCoveredByLocalAncestry(t *testing.T) {
	stale := Tip{111, 1}
	localTip := Tip{222, 2}
	localEff := &pb.Effect{Deps: []*pb.EffectRef{toPbRef(stale)}}

	realTips, superseded, err := NormalizeCloudTipCandidates("k", []Tip{stale}, []Tip{localTip}, func(tip Tip) (*pb.Effect, error) {
		if tip == localTip {
			return localEff, nil
		}

		return nil, fmt.Errorf("fetch %v: %w", tip, ErrCDNBlobMissing)
	})
	if err != nil {
		t.Fatalf("normalize candidates: %v", err)
	}

	if len(realTips) != 0 {
		t.Fatalf("real tips = %v, want none: the only candidate is covered by local ancestry", realTips)
	}

	if !slices.Equal(superseded, []Tip{stale}) {
		t.Fatalf("superseded candidates = %v, want %v", superseded, []Tip{stale})
	}
}

// holedFetchBroadcaster answers every fetch with the CDN-404 sentinel: any
// offset not already in the engine's cache is a provable durable hole.
type holedFetchBroadcaster struct {
	mockBroadcaster
}

func (b *holedFetchBroadcaster) FetchFromAny(ref *pb.EffectRef, _ FetchHint) ([]byte, error) {
	return nil, fmt.Errorf("cdn fetch %v: %w", r(ref), ErrCDNBlobMissing)
}

// Cloud ingestion retains unresolved tips without minting a replacement value
// or discarding existing local history. Reconstruction reports missing data.
func TestInstallCloudTipsNeverMints(t *testing.T) {
	e := newTxnTestEngine(&holedFetchBroadcaster{})
	defer func() { _ = e.Close() }()

	ctx := e.NewContext()
	if err := ctx.Emit(scalarSet("k", "local")); err != nil {
		t.Fatal(err)
	}

	if err := ctx.Flush(); err != nil {
		t.Fatal(err)
	}

	before := e.index.Contains("k").Tips()
	beforeOffset := e.nextOff.Load()

	holedTip := Tip{12345, 7}

	err := e.InstallCloudTips("k", []Tip{holedTip}, nil)
	if err != nil {
		t.Fatalf("install over holed frontier: %v", err)
	}

	if e.nextOff.Load() != beforeOffset {
		t.Fatal("cloud ingestion minted a local effect")
	}
	after := e.index.Contains("k").Tips()
	if len(after) != len(before)+1 || !slices.Contains(after, holedTip) {
		t.Fatalf("frontier = %v, want local tips %v plus %v", after, before, holedTip)
	}
	for _, tip := range before {
		if !slices.Contains(after, tip) {
			t.Fatalf("lost local tip %v from %v", tip, after)
		}
	}

	if _, _, _, err := e.GetSnapshot("k"); !errors.Is(err, errEffectNotResident) {
		t.Fatalf("reconstruction must report the unavailable branch, got %v", err)
	}
}

// A readable sibling cannot justify dropping an unavailable branch from the
// closure frontier. Value reconstruction must see both.
func TestInstallCloudTipsPreservesAllSiblings(t *testing.T) {
	e := newTxnTestEngine(&holedFetchBroadcaster{})
	defer func() { _ = e.Close() }()

	hlc := timestamppb.New(time.Now())
	readableTip := Tip{1111, 1}
	readableEff := &pb.Effect{
		Key:            []byte("k"),
		Hlc:            hlc,
		ForkChoiceHash: ComputeForkChoiceHash(1111, hlc),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_SCALAR,
			Value:      &pb.DataEffect_Raw{Raw: []byte("cloud")},
		}},
	}
	holedTip := Tip{12345, 7}

	sidecar := []CloudEffect{{Tip: readableTip, Eff: readableEff, ProtoLen: proto.Size(readableEff)}}

	err := e.InstallCloudTips("k", []Tip{readableTip, holedTip}, sidecar)
	if err != nil {
		t.Fatalf("install over partially holed frontier: %v", err)
	}

	ts := e.index.Contains("k")
	if ts == nil || !slices.Contains(ts.Tips(), readableTip) {
		t.Fatalf("post-install tips = %v, want the readable tip installed", ts.Tips())
	}

	if !slices.Contains(ts.Tips(), holedTip) {
		t.Fatalf("post-install tips = %v, missing branch must remain installed", ts.Tips())
	}

	if _, _, _, err := e.GetSnapshot("k"); !errors.Is(err, errEffectNotResident) {
		t.Fatalf("reconstruction must not serve just the readable sibling: %v", err)
	}
}
