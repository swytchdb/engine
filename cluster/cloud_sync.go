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
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"

	pb "github.com/swytchdb/engine/cluster/proto"
	dp "github.com/swytchdb/engine/cluster/proto/dataplane"
	"github.com/swytchdb/engine/effects"
)

// Swytch Cloud endpoints. Deliberately constants: first-party service URLs are
// not configurable (host entries are the local-testing path).
const (
	CloudEndpoint    = "ingress.swytch.earth:443"
	CloudCDNEndpoint = "https://swytch.earth"
)

// cloudEffectInfo is the domain-separation info for sealed effect payloads.
var cloudEffectInfo = []byte("effect")

// Version is the swytch build, set from main.Version at startup (ldflags). The
// cloud stream announces it — with the node id — as gRPC metadata, which is how
// the Cloud dashboard knows each cluster's node count and running version.
var Version = "dev"

// Metadata keys the dataplane reads off the WriteEffects stream; must match
// cloud's internal/dataplane.
const (
	nodeIDMetadataKey      = "swytch-node-id"
	nodeVersionMetadataKey = "swytch-version"
)

const (
	cloudRetryMin = time.Second
	cloudRetryMax = 30 * time.Second
	// cloudNackRetry delays re-sending an effect the cloud nacked (its storage
	// error is not going to clear instantly).
	cloudNackRetry = 5 * time.Second
	// cloudDrainTimeout bounds how long a graceful Stop waits for the outbox
	// to be acked before abandoning it.
	cloudDrainTimeout = 5 * time.Second
	// cloudReconcileRetry paces the retry loop that re-consults GetTips for
	// keys whose read was served from the outbox during a cloud outage.
	cloudReconcileRetry = 15 * time.Second
	// cloudReconcileBatch bounds keys per reconcile GetTips call. One RPC per
	// key melted down for real: a proxy request-per-connection budget was
	// spent on reconcile churn, GOAWAYing everything else on the connection.
	cloudReconcileBatch = 256
	// cloudBackstopTimeout caps any single cloud round-trip: GetTips (read
	// consult and reconcile batches) and CDN blob fetches. Liveness is
	// keepalive's job — both cloud connections ping, so a dead or half-open
	// path errors out of the RPC on its own. This exists only for the case
	// keepalive cannot see: a wedged-but-alive stream (healthy connection,
	// stuck handler). Deliberately generous, because a tight per-RPC deadline
	// treats slow as down — a cold dataplane rebuilding its store legitimately
	// answers in seconds, and failing at 5s manufactured errors for reads that
	// were about to succeed.
	cloudBackstopTimeout = 60 * time.Second
	// discoverWalkProgressInterval paces the membership-walk progress line.
	// The walk has no overall deadline (see DiscoverMembers), and it runs
	// before the metrics listener is up, so this log is the ONLY way to tell a
	// deep chain apart from a wedge while a node is blocked in bootstrap.
	discoverWalkProgressInterval = 10 * time.Second
	// backlogWarnEvery paces the "cloud unreachable, backlog growing" warning.
	backlogWarnEvery = 1024
	// cloudProgressInterval paces the outbox drain log line: pending/queued and
	// the interval's send/ack deltas, so a backlog is always
	// attributable to a stage (nothing queued vs sends stalled vs acks
	// stalled) instead of just a growing pending gauge.
	cloudProgressInterval = 30 * time.Second
	// cloudHasGenSize bounds one generation of knownInCloud before it rotates.
	// Overflowing costs a re-upload of effects the cloud already holds — which
	// it re-acks — so the bound trades memory against bandwidth and never
	// against completeness.
	cloudHasGenSize = 1 << 16
)

// CloudSync uploads this node's locally-authored effects to Swytch Cloud over
// the dataplane WriteEffects stream.
//
// The engine side is fire-and-forget: handleLocalEffect only records the mint
// and never blocks a write. The sync side is durable: an effect stays in the
// outbox — holding a pool refcount so eviction can't drop it — until the cloud
// acks it as stored. Nacks and stream failures re-send; only process exit loses
// the un-acked tail (the documented durability lag, recoverable via cluster
// replication).
//
// The response stream also carries FetchRequests: the cloud asking the cluster
// for an effect it's missing. Those are admitted to the same pinned, retrying
// outbox as local uploads — any resident effect, not just our own, since the
// cloud broadcasts the ask to whichever streams are open. Silence is the
// correct reply for an effect we no longer hold.
type CloudSync struct {
	engine     *effects.Engine
	enc        *effects.Encryptor
	keyNameKey []byte
	authKey    []byte

	target string
	conn   *grpc.ClientConn
	client dp.DataPlaneClient
	// streamConn carries ONLY the WriteEffects stream. Unary traffic (GetTips)
	// stays on cs.conn: a proxy that budgets requests per connection (nginx
	// answers the ~1000th HTTP/2 request with GOAWAY) must never count unary
	// churn against the connection holding the long-lived upload stream —
	// that coupling killed the stream every ~30s and the outbox never drained.
	streamConn   *grpc.ClientConn
	streamClient dp.DataPlaneClient
	folder       string // opaque CDN path segment, CloudFolder(authKey)
	httpClient   *http.Client

	mu      sync.Mutex
	pending map[effects.Tip]pendingUpload
	// pendingKeys indexes pending by plaintext key, so CloudTips can answer a
	// read-miss from the outbox: an evicted-before-ack key has no index tips
	// and no cloud tips, but its effects sit refcount-pinned in the pool — the
	// outbox tips are the frontier the cloud will eventually hold, and the
	// walk fetches their bytes locally. pinned records whether the entry holds
	// the key's eviction pin (PinKey succeeded — false when the key's leaf was
	// already gone at enqueue), so the drain never unpins a hold it never took:
	// the key could have been re-created since, and stealing the fresh leaf's
	// pin count would underflow.
	pendingKeys map[string]*pendingKeyEntry
	sendQ       []effects.Tip
	// reconcile holds keys whose read was answered from the outbox while
	// GetTips was failing. The cloud may hold tips for them we've never seen
	// (another node's uploads); reconcileLoop re-consults until the cloud
	// answers and the missed frontier is merged into the DAG.
	reconcile map[string]struct{}

	// Cloud-pushed key-name filter gating the read-miss consult (CloudTips).
	// filterBulk is replaced wholesale per KeyFilter frame; filterOwn is
	// append-only with our own uploads' key names, covering the window until
	// the cloud's next frame reflects them (frames arrive only at stream
	// attach and after a cloud-side rebuild, so that window is long — never
	// reset filterOwn). Both hold PRF images (CloudKeyName). A nil filterBulk
	// (no frame received yet) leaves the gate open — every miss consults the
	// cloud, the pre-filter behavior. Guarded by filterMu, not mu: the gate
	// runs on the read path and must not contend with the outbox.
	filterMu   sync.RWMutex
	filterBulk *effects.Bloom
	filterOwn  ownFilter

	// knownInCloud is the stop rule for the unsent-history walk: tips the cloud
	// demonstrably holds, being our own acked uploads plus every effect the
	// cloud itself served us (GetTips sidecars, CDN blobs). Without it a
	// rehydrated key's entire chain would re-upload on the next local write to
	// it. Its own mutex, not mu: the walk runs on the emit path and the feed
	// runs on the read path, and neither should queue behind the outbox.
	knownMu      sync.Mutex
	knownInCloud tipGenSet

	// memberRemoveHandler applies a cloud-pushed "delete this server" command
	// (WriteResponse.MemberRemove) by routing its node id into the membership
	// removal path. Wired by the beacon after Start, so the readLoop may already
	// be running when it lands — stored atomically. Nil until wired (and in
	// embedder setups with no beacon), in which case the command is a no-op.
	memberRemoveHandler atomic.Pointer[func(nodeID uint64)]

	// sentCount/ackedCount mirror the prometheus counters so the progress
	// logger can read interval deltas (prometheus counters aren't readable).
	sentCount  atomic.Uint64
	ackedCount atomic.Uint64

	wake   chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewCloudSync derives the cloud key material from the connection secret and
// drops the secret: only the auth key (wire identity), the key-name PRF key,
// and the payload encryption key are retained.
func NewCloudSync(engine *effects.Engine, connectionSecret string) (*CloudSync, error) {
	enc, err := effects.NewEncryptorFromIKM(DeriveEncryptionKey(connectionSecret))
	if err != nil {
		return nil, fmt.Errorf("cloud sync: derive encryption keypair: %w", err)
	}

	authKey := DeriveAuthKeyBytes(DeriveCloudSecret(connectionSecret))
	cs := &CloudSync{
		engine:     engine,
		enc:        enc,
		keyNameKey: DeriveKeyNameKey(connectionSecret),
		authKey:    authKey,
		target:     CloudEndpoint,
		folder:     CloudFolder(authKey),
		httpClient: &http.Client{Timeout: cloudBackstopTimeout},
		wake:       make(chan struct{}, 1),
	}
	creds := credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13})
	// GetTips responses carry the closure effects inline; the closure is
	// bounded by the dataplane's walk (closureCap), not by gRPC's default
	// 4MiB receive cap, which would artificially truncate a legit response.
	recvCap := grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32))
	// Keepalive is load-bearing for the outbox: a dataplane restart behind the
	// edge leaves a half-open conn with no RST, and without pings the sender
	// parks in Send flow control forever — the outbox freezes silently while
	// pending grows (a 2026-07-06 bench wedged ~1M effects/node this way, with
	// zero reconnect attempts). With pings the dead path errors out of
	// Send/Recv and run()'s reconnect loop takes over.
	kal := grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                20 * time.Second,
		Timeout:             10 * time.Second,
		PermitWithoutStream: true,
	})

	conn, err := grpc.NewClient(cs.target, grpc.WithTransportCredentials(creds), recvCap, kal)
	if err != nil {
		return nil, fmt.Errorf("cloud sync: dial %s: %w", cs.target, err)
	}

	streamConn, err := grpc.NewClient(cs.target, grpc.WithTransportCredentials(creds), recvCap, kal)
	if err != nil {
		err = errors.Join(err, conn.Close())

		return nil, fmt.Errorf("cloud sync: dial %s (stream): %w", cs.target, err)
	}

	cs.conn = conn
	cs.client = dp.NewDataPlaneClient(conn)
	cs.streamConn = streamConn
	cs.streamClient = dp.NewDataPlaneClient(streamConn)

	return cs, nil
}

// Start installs the engine's OnLocalEffect hook and launches the upload loop.
func (cs *CloudSync) Start() error {
	cs.ctx, cs.cancel = context.WithCancel(context.Background())
	cs.engine.SetOnLocalEffect(cs.handleLocalEffect)

	cs.wg.Add(3)
	go cs.run()
	go cs.reconcileLoop()
	go cs.progressLoop()

	return nil
}

// progressLoop runs the outbox watchdog on its own goroutine — the sender can
// park in stream.Send flow control for the entire duration of a stall, so the
// log that reports the stall must not share its loop.
func (cs *CloudSync) progressLoop() {
	defer cs.wg.Done()

	t := time.NewTicker(cloudProgressInterval)
	defer t.Stop()

	lastSent, lastAcked := cs.sentCount.Load(), cs.ackedCount.Load()

	for {
		select {
		case <-cs.ctx.Done():
			return
		case <-t.C:
			lastSent, lastAcked = cs.logOutboxProgress(lastSent, lastAcked)
		}
	}
}

// Stop drains the outbox, then halts the upload loop. Graceful shutdown must
// not look like a crash to the cloud — the beacon's departure REMOVE is
// typically the last thing enqueued and peers may already be gone, so cloud is
// its only way out. The drain is bounded on STALL, not total time: a deep
// backlog on a live cloud flushes fully; only a cloud that stops acking gets
// abandoned, exactly as a crash would (a solo node has no peers to re-replicate
// from, so an abandoned tail is lost — run 2841213 abandoned 217 effects this
// way and every key in them missed forever).
func (cs *CloudSync) Stop() {
	// Detach first, waiting for any callback already inside handleLocalEffect.
	// Once this returns, pending is a closed producer set and an empty outbox
	// cannot race with a late enqueue while the sender is being cancelled.
	cs.engine.SetOnLocalEffect(nil)
	cs.waitDrained(cloudDrainTimeout)

	if cs.cancel != nil {
		cs.cancel()
	}

	cs.wg.Wait()

	for _, conn := range []*grpc.ClientConn{cs.conn, cs.streamConn} {
		if conn != nil {
			if err := conn.Close(); err != nil {
				slog.Warn("cloud sync: close connection", "error", err)
			}
		}
	}
}

// waitDrained polls until the outbox is empty, abandoning only when the count
// stops changing for a full timeout window — acks in flight keep resetting the
// stall clock.
func (cs *CloudSync) waitDrained(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	last := -1

	for {
		cs.mu.Lock()
		n := len(cs.pending)
		cs.mu.Unlock()

		if n == 0 {
			return
		}

		if n != last {
			deadline = time.Now().Add(timeout)
			last = n
		}

		if time.Now().After(deadline) {
			slog.Warn("cloud sync: shutdown drain stalled, abandoning outbox", "pending", n)

			return
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func (cs *CloudSync) run() {
	defer cs.wg.Done()

	backoff := cloudRetryMin

	for cs.ctx.Err() == nil {
		started := time.Now()

		err := cs.runStream()
		if cs.ctx.Err() != nil {
			return
		}
		// A stream that lived a while earned a fresh backoff; rapid failures
		// climb toward the cap.
		if time.Since(started) > time.Minute {
			backoff = cloudRetryMin
		}

		cloudStreamReconnectsTotal.Inc()
		slog.Warn("cloud sync: stream ended, reconnecting", "error", err, "retry_in", backoff)

		select {
		case <-cs.ctx.Done():
			return
		case <-time.After(backoff):
		}

		backoff = min(backoff*2, cloudRetryMax)
	}
}

// runStream drives one WriteEffects stream to completion: authenticate,
// re-queue everything still pending (acks from a previous stream may have
// retired some), then interleave sends with the reader's acks and
// fetch-requests until the stream breaks or we shut down.
func (cs *CloudSync) runStream() error {
	// Announce who we are alongside the auth_key: the node id and build version
	// ride the stream metadata so the cloud can report the cluster's fleet
	// (node count, running version) to its dashboard without ever looking
	// inside an effect.
	ctx := metadata.AppendToOutgoingContext(cs.ctx,
		nodeIDMetadataKey, strconv.FormatUint(uint64(cs.engine.NodeID()), 10),
		nodeVersionMetadataKey, Version,
	)

	stream, err := cs.streamClient.WriteEffects(ctx)
	if err != nil {
		return err
	}

	if err := stream.Send(&dp.WriteEffectsRequest{
		Payload: &dp.WriteEffectsRequest_AuthKey{AuthKey: cs.authKey},
	}); err != nil {
		return err
	}

	cs.mu.Lock()

	cs.sendQ = cs.sendQ[:0]
	for tip := range cs.pending {
		cs.sendQ = append(cs.sendQ, tip)
	}
	cs.mu.Unlock()
	cs.wakeSender()

	readerErr := make(chan error, 1)
	go func() { readerErr <- cs.readLoop(stream) }()

	for {
		select {
		case <-cs.ctx.Done():
			if err := stream.CloseSend(); err != nil {
				slog.Debug("cloud sync: close send", "error", err)
			}

			return errors.Join(cs.ctx.Err(), <-readerErr)
		case err := <-readerErr:
			return err
		case <-cs.wake:
		}

		for {
			env, ok := cs.nextEnvelope()
			if !ok {
				break
			}

			if err := stream.Send(&dp.WriteEffectsRequest{
				Payload: &dp.WriteEffectsRequest_Effect{Effect: env},
			}); err != nil {
				return err
			}
		}
	}
}

// logOutboxProgress emits the per-interval drain line and returns the new
// send/ack watermarks. Warn when a non-empty outbox moved nothing this
// interval — with keepalive that means the stream is dying (about to error
// out), without it that was the silent-wedge signature.
func (cs *CloudSync) logOutboxProgress(lastSent, lastAcked uint64) (uint64, uint64) {
	sent, acked := cs.sentCount.Load(), cs.ackedCount.Load()
	cs.mu.Lock()
	pending := len(cs.pending)
	queued := len(cs.sendQ)
	cs.mu.Unlock()

	if pending == 0 && queued == 0 {
		return sent, acked
	}

	if sent == lastSent && acked == lastAcked {
		slog.Warn("cloud sync: outbox stalled",
			"pending", pending, "queued", queued,
			"interval", cloudProgressInterval)
	} else {
		slog.Info("cloud sync: outbox progress",
			"pending", pending, "queued", queued,
			"sent_delta", sent-lastSent, "acked_delta", acked-lastAcked,
			"interval", cloudProgressInterval)
	}

	return sent, acked
}

// nextEnvelope pops the next outbox upload. Sealing happens here, on the sync
// goroutine, never on the emit path.
func (cs *CloudSync) nextEnvelope() (*dp.Effect, bool) {
	cs.mu.Lock()
	// Bound one pass to the uploads that were queued when it began. A blocked
	// descendant is put back for a dependency ACK to wake; without the bound, a
	// queue containing only blocked descendants would spin forever.
	remaining := len(cs.sendQ)
	cs.mu.Unlock()

	for {
		cs.mu.Lock()
		if len(cs.sendQ) == 0 || remaining == 0 {
			cs.mu.Unlock()

			return nil, false
		}

		tip := cs.sendQ[0]
		cs.sendQ = cs.sendQ[1:]
		upload := cs.pending[tip]
		cs.mu.Unlock()

		remaining--

		eff := upload.effect
		if eff == nil {
			continue // acked while queued
		}
		// Continue the unsent-history walk off the emit path. The outbox
		// reference on this effect keeps its deps resolvable here even if the
		// key was evicted since the mint.
		if upload.expand {
			if deps := cs.expandAncestry(eff); len(deps) > 0 {
				cs.deferUploadBehind(tip, deps)
				// These deps were not in the queue when the pass was bounded. Count
				// each newly pinned vertex once so a final-slot expansion cannot leave
				// ancestry queued with nobody left to wake the sender.
				remaining += len(deps)

				continue
			}
		}

		// Stream order is not Cloud commit order: its shared worker pool may
		// process a child before an earlier parent. Keep descendants queued until
		// every locally-uploadable dependency has been durably ACKed and retired
		// from the outbox. Independent effects can still fill the pipeline.
		cs.mu.Lock()
		blocked := false

		for _, dep := range eff.GetDeps() {
			depTip := effects.Tip{dep.GetNodeId(), dep.GetOffset()}
			if _, pending := cs.pending[depTip]; pending {
				blocked = true

				break
			}
		}

		if blocked {
			cs.sendQ = append(cs.sendQ, tip)
		}
		cs.mu.Unlock()

		if blocked {
			continue
		}

		env, err := cs.buildEnvelope(tip, eff)
		if err != nil {
			// Seal/marshal failures are transient (rand.Read) — the effect
			// already marshaled once for cluster broadcast. Keep it in the
			// outbox and re-queue after backoff; retiring here would silently
			// drop cloud durability for an effect the cluster still owes it.
			slog.Error("cloud sync: envelope build failed, will retry",
				"tip", tip, "error", err)
			cs.retryUpload(tip)

			continue
		}

		cloudEffectsSentTotal.Inc()
		cs.sentCount.Add(1)

		return env, true
	}
}

func (cs *CloudSync) readLoop(stream dp.DataPlane_WriteEffectsClient) error {
	for {
		resp, err := stream.Recv()
		if err != nil {
			return err
		}

		switch m := resp.GetMsg().(type) {
		case *dp.WriteResponse_Ack:
			cs.handleAck(m.Ack)
		case *dp.WriteResponse_Fetch:
			cs.handleFetch(m.Fetch)
		case *dp.WriteResponse_Filter:
			cs.handleFilter(m.Filter)
		case *dp.WriteResponse_MemberRemove:
			cs.handleMemberRemove(m.MemberRemove)
		}
	}
}

// handleAck retires a durably-stored effect, or schedules a nacked one for
// re-send. Local mints, ancestry backfills, and fetch replies all use the same
// outbox so Cloud's concurrent workers cannot strand one category on a NACK.
func (cs *CloudSync) handleAck(ack *dp.WriteAck) {
	tip := effects.Tip{ack.GetId().GetNodeId(), ack.GetId().GetOffset()}
	if ack.GetOk() {
		// Marked before the retire: an acked effect is one
		// the ancestry walk must stop at, whichever path put it on the wire.
		cs.markCloudHolds(tip)

		if cs.retire(tip) {
			cloudEffectsAckedTotal.Inc()
			cs.ackedCount.Add(1)
			// A descendant may be waiting for this dependency to become durable.
			cs.wakeSender()
		}

		return
	}

	cs.mu.Lock()
	_, held := cs.pending[tip]
	cs.mu.Unlock()

	if !held {
		return
	}

	cloudEffectsNackedTotal.Inc()
	slog.Warn("cloud sync: cloud failed to store effect, will retry",
		"tip", tip, "error", ack.GetError())
	cs.retryUpload(tip)
}

// retryUpload schedules another attempt only if the outbox still owns the tip.
// A successful ACK can retire it while the retry timer is pending.
func (cs *CloudSync) retryUpload(tip effects.Tip) {
	time.AfterFunc(cloudNackRetry, func() {
		cs.mu.Lock()
		if _, ok := cs.pending[tip]; ok {
			cs.sendQ = append(cs.sendQ, tip)
		}
		cs.mu.Unlock()
		cs.wakeSender()
	})
}

// handleFetch answers the cloud's ask for an effect it is missing. Served from
// the vertex pool regardless of author and admitted to the normal pinned
// outbox, so the exact requested effect survives NACK/reconnect retries. Its
// deps are not inferred as requests; Cloud asks for each deeper hole explicitly.
// A non-resident effect gets silence and the cloud re-asks elsewhere or later.
func (cs *CloudSync) handleFetch(req *dp.FetchRequest) {
	ref := req.GetRef()
	tip := effects.Tip{ref.GetNodeId(), ref.GetOffset()}

	eff, ok := cs.engine.EffectCache().Get(tip)
	if !ok {
		return
	}

	if !cs.enqueueUpload(tip, eff, false, false) {
		// Already pending means the request is scheduled or in flight. Any
		// other failure means the vertex could not be pinned long enough for an
		// asynchronous reply, so silence is the only safe response.
		return
	}
	// The ask authorizes this ref only. Cloud accepts it as repair ancestry and
	// explicitly requests any deeper hole after inspecting the landed envelope.
	cs.wakeSender()
}

// cloudMayHold is the read-miss filter gate: once the cloud has pushed its
// key-name filter, a PRF name absent from both it and our own-uploads filter
// cannot be in the cloud — free-miss without the round-trip. No filter yet
// (filterBulk nil) leaves the gate open. False positives fall through to a
// wasted consult; false negatives (stale filter) cost one premature miss and
// never hide a key a live peer holds — the cluster subscription path doesn't
// run through here.
func (cs *CloudSync) cloudMayHold(name []byte) bool {
	h := effects.BloomHash(name)

	cs.filterMu.RLock()
	defer cs.filterMu.RUnlock()

	if cs.filterBulk == nil {
		return true
	}

	return cs.filterBulk.HasHash(h) || cs.filterOwn.has(h)
}

// SetMemberRemoveHandler wires the callback that applies a cloud-pushed
// MemberRemove command. The beacon installs it so the removal reuses the
// membership REMOVE_OP path; without a handler the command is a no-op.
func (cs *CloudSync) SetMemberRemoveHandler(fn func(nodeID uint64)) {
	if fn == nil {
		cs.memberRemoveHandler.Store(nil)

		return
	}

	cs.memberRemoveHandler.Store(&fn)
}

// handleMemberRemove routes a cloud-pushed "delete this server" command into
// the membership removal path via the wired handler.
func (cs *CloudSync) handleMemberRemove(mr *dp.MemberRemove) {
	nodeID := mr.GetNodeId()

	fn := cs.memberRemoveHandler.Load()
	if fn == nil || *fn == nil {
		slog.Warn("cloud sync: member remove received with no handler wired, dropping", "node_id", nodeID)

		return
	}
	// Debug, not Info: dashboard cleanups push these in bursts of thousands;
	// the beacon's applier logs one batch summary instead.
	slog.Debug("cloud sync: applying member remove", "node_id", nodeID)
	(*fn)(nodeID)
}

// handleFilter installs the cloud's key-name filter, replacing the previous
// one wholesale (the stream is ordered and each reconnect re-delivers the
// current filter, so latest always wins). An undecodable frame is dropped and
// the prior filter kept — the filter is advisory, never worth failing over.
func (cs *CloudSync) handleFilter(kf *dp.KeyFilter) {
	decoded, err := effects.ParseBloomFrame(kf.GetFilter())
	if err != nil {
		slog.Warn("cloud sync: undecodable key filter frame, keeping previous", "error", err)

		return
	}

	cs.filterMu.Lock()
	cs.filterBulk = decoded
	cs.filterMu.Unlock()
	slog.Debug("cloud sync: key filter installed", "bytes", len(kf.GetFilter()))
}

// ownFilter is the append-only approximate set of this node's own uploaded
// key names: a bloom list that grows by doubling, so membership stays O(k)
// per bloom at any upload count (the CuckooChain it replaced degraded to a
// linear walk of ~500 segments at trace scale). Bits are never cleared and
// the list is never reset — a false negative here would free-miss a key
// whose upload the cloud frame doesn't cover yet.
type ownFilter struct {
	blooms []*effects.Bloom // newest last; earlier blooms are at capacity
}

func (f *ownFilter) add(name []byte) {
	h := effects.BloomHash(name)

	n := len(f.blooms)
	if n == 0 || f.blooms[n-1].Fill() > 0.5 {
		size := effects.BloomMinBytes
		if n > 0 {
			size = f.blooms[n-1].SizeBytes() * 2
		}

		f.blooms = append(f.blooms, effects.NewBloom(size))
		n++
	}

	f.blooms[n-1].SetHash(h)
}

func (f *ownFilter) has(h uint64) bool {
	for _, b := range f.blooms {
		if b.HasHash(h) {
			return true
		}
	}

	return false
}

// buildEnvelope maps a swytch effect to the cloud's structural envelope: the
// (NodeID, Offset) identity, deps, kind, and time stay readable (the cloud's
// retention scan walks them); the key name becomes its PRF image and the whole
// serialized effect is sealed into raw_effect. raw_size carries the pre-seal
// byte count — plus what value compression (--compress) hid from the marshal
// — so the cloud can bill the customer's data rather than the sealed blob it
// stores or the writer's compression choices.
func (cs *CloudSync) buildEnvelope(tip effects.Tip, eff *pb.Effect) (*dp.Effect, error) {
	var pool *effects.VertexPool
	if cs.engine != nil {
		pool = cs.engine.EffectCache()
	}

	raw, err := pool.MarshalEffect(eff)
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}

	sealed, err := cs.enc.SealAndCompress(raw, cloudEffectInfo)
	if err != nil {
		return nil, fmt.Errorf("seal: %w", err)
	}

	deps := make([]*dp.EffectRef, len(eff.GetDeps()))
	for i, d := range eff.GetDeps() {
		deps[i] = &dp.EffectRef{NodeId: d.GetNodeId(), Offset: d.GetOffset()}
	}

	var timeLocal int64
	if eff.GetHlc() != nil {
		timeLocal = eff.GetHlc().AsTime().UnixNano()
	}

	snap := eff.GetSnapshot()

	return &dp.Effect{
		Id:                    &dp.EffectRef{NodeId: tip[0], Offset: tip[1]},
		Deps:                  deps,
		Key:                   CloudKeyName(cs.keyNameKey, eff.GetKey()),
		RawEffect:             sealed,
		RawSize:               uint64(len(raw)) + effects.InflatedSizeDelta(eff),
		TimeLocal:             timeLocal,
		EffectType:            cloudEffectType(eff),
		SnapshotStateCarrying: snap != nil && snap.GetState() != nil,
	}, nil
}

// cloudEffectType maps the Effect.kind oneof to the cloud's structural enum.
func cloudEffectType(eff *pb.Effect) dp.EffectType {
	switch {
	case eff.GetData() != nil:
		return dp.EffectType_EFFECT_TYPE_DATA
	case eff.GetMeta() != nil:
		return dp.EffectType_EFFECT_TYPE_META
	case eff.GetTxnBind() != nil:
		return dp.EffectType_EFFECT_TYPE_TXN_BIND
	case eff.GetSnapshot() != nil:
		return dp.EffectType_EFFECT_TYPE_SNAPSHOT
	case eff.GetSubscription() != nil:
		return dp.EffectType_EFFECT_TYPE_SUBSCRIPTION
	case eff.GetSerialization() != nil:
		return dp.EffectType_EFFECT_TYPE_SERIALIZATION
	case eff.GetNoop() != nil:
		return dp.EffectType_EFFECT_TYPE_NOOP
	case eff.GetObservation() != nil:
		return dp.EffectType_EFFECT_TYPE_OBSERVATION
	case eff.GetRowWrite() != nil:
		return dp.EffectType_EFFECT_TYPE_ROW_WRITE
	case eff.GetPubsubMessage() != nil:
		return dp.EffectType_EFFECT_TYPE_PUBSUB_MESSAGE
	default:
		return dp.EffectType_EFFECT_TYPE_UNSPECIFIED
	}
}
