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

package cluster

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
	dp "github.com/swytchdb/engine/cluster/proto/dataplane"
	"github.com/swytchdb/engine/effects"
	gproto "google.golang.org/protobuf/proto"
)

// FetchFromCDN implements CDNFetcher: FetchFromAny orders this against peer
// fetch by the caller's FetchHint, so every recursive dep fetch (NACK ingest,
// backfill, subscription bootstrap) transparently falls back to the cloud for
// effects no live peer holds. Returns wire-format bytes, same as a peer fetch.
func (cs *CloudSync) FetchFromCDN(ctx context.Context, ref *pb.EffectRef) ([]byte, error) {
	eff, err := cs.fetchEffect(ctx, effects.Tip{ref.GetNodeId(), ref.GetOffset()})
	if err != nil {
		return nil, err
	}

	var pool *effects.VertexPool
	if cs.engine != nil {
		pool = cs.engine.EffectCache()
	}

	return MarshalEffectWire(eff, pool)
}

// MayHold implements effects.CloudReader: the free, no-RPC filter gate the
// engine consults before deciding a read-miss can skip the subscribe +
// CloudTips path entirely. Own un-acked uploads are covered: filterOwn is fed
// at outbox enqueue.
func (cs *CloudSync) MayHold(key string) bool {
	return cs.cloudMayHold(CloudKeyName(cs.keyNameKey, []byte(key)))
}

// CloudTips merges Cloud's frontier with unacknowledged local uploads so reads
// can see writes before the outbox drains. It implements effects.CloudReader;
// the engine reconstructs the returned frontier without reducing it here.
// Sidecars supply cloud effect bytes inline; outbox bytes are already local.
// Missing refs identify known cloud holes and avoid redundant CDN fetches.
func (cs *CloudSync) CloudTips(ctx context.Context, key string) ([]effects.Tip, []effects.CloudEffect, []effects.Tip, error) {
	name := CloudKeyName(cs.keyNameKey, []byte(key))
	if !cs.cloudMayHold(name) {
		return nil, nil, nil, nil
	}

	tips := cs.pendingTipsFor(key)

	ctx, cancel := context.WithTimeout(ctx, cloudBackstopTimeout)
	defer cancel()

	resp, err := cs.client.GetTips(ctx, &dp.GetTipsRequest{
		AuthKey: cs.authKey,
		Keys:    [][]byte{name},
	})
	if err != nil {
		if len(tips) > 0 {
			// Cloud unreachable, but the outbox holds the key's un-acked
			// frontier locally — serve our own writes rather than failing
			// the read. The answer is incomplete (the cloud may hold tips
			// we've never seen), so the key is marked pending: reconcileLoop
			// re-consults until the cloud answers and merges the missed
			// frontier into the DAG.
			slog.Warn("cloud sync: get tips failed, serving outbox frontier", "key", key, "error", err)
			cs.MarkPending(key)

			return tips, nil, nil, nil
		}

		return nil, nil, nil, fmt.Errorf("cloud get tips: %w", err)
	}

	seen := make(map[effects.Tip]struct{}, len(tips))
	for _, t := range tips {
		seen[t] = struct{}{}
	}

	var (
		sidecar []effects.CloudEffect
		missing []effects.Tip
	)

	for _, kt := range resp.GetKeys() {
		for _, ref := range kt.GetTips() {
			t := effects.Tip{ref.GetNodeId(), ref.GetOffset()}
			if _, ok := seen[t]; !ok {
				seen[t] = struct{}{}
				tips = append(tips, t)
			}
		}

		sidecar = cs.appendSidecar(sidecar, kt)
		for _, ref := range kt.GetMissing() {
			missing = append(missing, effects.Tip{ref.GetNodeId(), ref.GetOffset()})
		}
	}

	return tips, sidecar, missing, nil
}

// MarkPending schedules a re-consult when the outbox was served without a
// complete GetTips response. It does not classify the graph's reconstructability.
func (cs *CloudSync) MarkPending(key string) {
	cs.mu.Lock()
	if cs.reconcile == nil {
		cs.reconcile = make(map[string]struct{})
	}

	cs.reconcile[key] = struct{}{}
	cs.mu.Unlock()
}

// reconcileKeys snapshots the pending reconcile set.
func (cs *CloudSync) reconcileKeys() []string {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if len(cs.reconcile) == 0 {
		return nil
	}

	keys := make([]string, 0, len(cs.reconcile))
	for k := range cs.reconcile {
		keys = append(keys, k)
	}

	return keys
}

func (cs *CloudSync) clearReconcile(key string) {
	cs.mu.Lock()
	delete(cs.reconcile, key)

	if len(cs.reconcile) == 0 {
		cs.reconcile = nil
	}

	cs.mu.Unlock()
}

// reconcileLoop drives eventual convergence for keys whose reads were served
// from the outbox while GetTips was failing: once you've answered a read from
// your own un-acked writes, you owe the DAG whatever the cloud held that you
// couldn't see. Each tick re-consults every pending key in GetTips batches; a
// key stays pending until the cloud gives a complete answer and its frontier
// installs cleanly.
func (cs *CloudSync) reconcileLoop() {
	defer cs.wg.Done()

	ticker := time.NewTicker(cloudReconcileRetry)
	defer ticker.Stop()

	for {
		select {
		case <-cs.ctx.Done():
			return
		case <-ticker.C:
		}

		keys := cs.reconcileKeys()
		for start := 0; start < len(keys); start += cloudReconcileBatch {
			if cs.ctx.Err() != nil {
				return
			}

			end := min(start+cloudReconcileBatch, len(keys))
			cs.reconcileBatch(keys[start:end])
		}
	}
}

// reconcileBatch re-consults GetTips and ingests the returned closure. Transport
// failures leave the key scheduled for retry. Definitive absence is an exhausted
// closure boundary; whether a read needs that history belongs to reconstruction.
func (cs *CloudSync) reconcileBatch(keys []string) {
	names := make([][]byte, len(keys))

	byName := make(map[string]string, len(keys))
	for i, key := range keys {
		name := CloudKeyName(cs.keyNameKey, []byte(key))
		names[i] = name
		byName[string(name)] = key
	}

	ctx, cancel := context.WithTimeout(cs.ctx, cloudBackstopTimeout)
	defer cancel()

	resp, err := cs.client.GetTips(ctx, &dp.GetTipsRequest{AuthKey: cs.authKey, Keys: names})
	if err != nil {
		return // cloud still down; keep every key pending
	}

	for _, kt := range resp.GetKeys() {
		key, ok := byName[string(kt.GetKey())]
		if !ok {
			continue
		}

		tips := cloudTips(kt.GetTips())

		if len(tips) == 0 {
			cs.clearReconcile(key) // the cloud holds nothing we haven't already served

			continue
		}

		if err := cs.engine.InstallCloudTips(key, tips, cs.appendSidecar(nil, kt)); err != nil {
			slog.Warn("cloud reconcile: install failed; will retry", "key", key, "error", err)

			continue
		}

		slog.Info("cloud reconcile: merged cloud frontier", "key", key, "tips", len(tips))
		cs.clearReconcile(key)
	}
}

// fetchEffect pulls one effect blob from the CDN and peels it: envelope →
// open → inner effect.
func (cs *CloudSync) fetchEffect(ctx context.Context, tip effects.Tip) (*pb.Effect, error) {
	// Our own un-acked mints are served from the outbox: the CDN cannot have
	// an effect that hasn't been uploaded yet, and asking it anyway both fails
	// the read (404) and risks the CDN negative-caching the miss. The read
	// path lands here when a key's effects were evicted from the pool while
	// still waiting in the upload queue — CloudTips names the outbox tip as
	// frontier, and this is where its bytes come from.
	cs.mu.Lock()
	eff := cs.pending[tip].effect
	cs.mu.Unlock()

	if eff != nil {
		return eff, nil
	}

	url := fmt.Sprintf("%s/%s/%016x-%016x", CloudCDNEndpoint, cs.folder, tip[0], tip[1])

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	cloudCDNFetchesTotal.Inc()

	resp, err := cs.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	// Response-body cleanup cannot change the status or the checked read result.
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("cdn fetch %s: %w", url, effects.ErrCDNBlobMissing)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("cdn fetch %s: status %d", url, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var env dp.Effect
	if err := gproto.Unmarshal(body, &env); err != nil {
		return nil, fmt.Errorf("cdn blob decode: %w", err)
	}

	fetched, _, err := cs.peelEnvelope(&env)
	if err != nil {
		return nil, err
	}
	// The CDN served it, so the cloud holds it: the ancestry walk stops here.
	cs.markCloudHolds(tip)

	return fetched, nil
}

// peelEnvelope opens one cloud effect envelope — seal off, decompress,
// unmarshal the inner effect. Returns the inner effect and its marshaled
// proto length (the pool's cache-accounting size).
func (cs *CloudSync) peelEnvelope(env *dp.Effect) (*pb.Effect, int, error) {
	raw, err := cs.enc.OpenAndDecompress(env.GetRawEffect(), cloudEffectInfo)
	if err != nil {
		return nil, 0, fmt.Errorf("cloud blob open: %w", err)
	}

	eff := &pb.Effect{}
	if err := effects.UnmarshalEffect(raw, eff); err != nil {
		return nil, 0, err
	}

	return eff, len(raw), nil
}

// cloudTips converts cloud wire references without growing an intermediate slice.
func cloudTips(refs []*dp.EffectRef) []effects.Tip {
	if len(refs) == 0 {
		return nil
	}

	tips := make([]effects.Tip, len(refs))
	for i, ref := range refs {
		tips[i] = effects.Tip{ref.GetNodeId(), ref.GetOffset()}
	}

	return tips
}

// appendSidecar decodes a KeyTips' inline closure into engine-installable
// effects. An envelope that fails to peel is skipped with a warning — the
// install walk fetches it like any other straggler.
func (cs *CloudSync) appendSidecar(sidecar []effects.CloudEffect, kt *dp.KeyTips) []effects.CloudEffect {
	closure := kt.GetClosure()
	start := len(sidecar)

	sidecar = slices.Grow(sidecar, len(closure))
	for _, env := range closure {
		eff, protoLen, err := cs.peelEnvelope(env)
		if err != nil {
			slog.Warn("cloud sync: sidecar envelope unusable, walk will fetch it",
				"tip", effects.Tip{env.GetId().GetNodeId(), env.GetId().GetOffset()}, "error", err)

			continue
		}

		tip := effects.Tip{env.GetId().GetNodeId(), env.GetId().GetOffset()}
		// The cloud handed us this effect, so it holds it: stop the ancestry
		// walk here rather than re-uploading a whole rehydrated chain on the
		// next local write to the key.
		cs.markCloudHolds(tip)
		sidecar = append(sidecar, effects.CloudEffect{
			Tip:      tip,
			Eff:      eff,
			ProtoLen: protoLen,
		})
	}

	cloudSidecarEffectsTotal.Add(float64(len(sidecar) - start))

	return sidecar
}
