package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/engine/graph/bplus"
	"google.golang.org/protobuf/proto"
)

type bplusProfileRow struct {
	mu                   sync.Mutex
	Ordinal              int    `json:"ordinal"`
	Kind                 string `json:"kind"`
	PrepareNS            int64  `json:"prepare_ns"`
	FlushNS              int64  `json:"flush_ns"`
	TotalNS              int64  `json:"total_ns"`
	ObservedAddresses    int    `json:"observed_addresses"`
	NewPages             int    `json:"new_pages"`
	DeletedPages         int    `json:"deleted_pages"`
	PageWrites           int    `json:"page_writes"`
	BindKeys             int    `json:"bind_keys"`
	BindPeers            int    `json:"bind_peers"`
	ConsumedTips         int    `json:"consumed_tips"`
	ProofRefs            int    `json:"unique_local_proof_refs"`
	ProofRefBytes        int    `json:"local_proof_ref_bytes"`
	Notifies             int    `json:"received_notifies"`
	NotifyBytes          int    `json:"received_effect_bytes"`
	ReceiveNS            int64  `json:"receive_handler_ns_sum"`
	BindReceiveNS        int64  `json:"bind_handler_ns_max"`
	HistoryRecords       int    `json:"history_records"`
	HistoryBytes         int    `json:"history_bytes"`
	HistoryUnique        int    `json:"unique_history_refs"`
	NackRefVisits        int    `json:"nack_ref_visits"`
	NackUnique           int    `json:"unique_nack_refs"`
	NackBytes            int    `json:"nack_bytes"`
	peers                map[uint64]bool
	proof, history, nack map[effects.Tip]bool
}

type bplusProfileRef struct {
	row  *bplusProfileRow
	bind bool
}
type bplusProfileObserver struct {
	active atomic.Pointer[bplusProfileRow]
	refs   sync.Map // published origin -> request; used to attribute delayed notices
}

func (o *bplusProfileObserver) lookup(tip effects.Tip) bplusProfileRef {
	if v, ok := o.refs.Load(tip); ok {
		return v.(bplusProfileRef)
	}

	return bplusProfileRef{row: o.active.Load()}
}

func (o *bplusProfileObserver) local(tip effects.Tip, eff *pb.Effect) {
	r := o.active.Load()
	if r == nil {
		return
	}

	o.refs.Store(tip, bplusProfileRef{row: r, bind: eff.GetTxnBind() != nil})
	r.mu.Lock()
	defer r.mu.Unlock()

	if data := eff.GetData(); data != nil && len(eff.GetIKey()) != 0 {
		if string(eff.GetVirtual()) == pb.GraphRegionsKey && string(data.GetId()) == bplus.RegionLabel {
			r.NewPages++
		} else if len(eff.GetVirtual()) == 0 {
			r.PageWrites++
			if data.GetOp() == pb.EffectOp_REMOVE_OP {
				r.DeletedPages++
			}
		}
	}

	if bind := eff.GetTxnBind(); bind != nil {
		r.BindKeys = len(bind.GetKeys())
		for _, key := range bind.GetKeys() {
			r.ConsumedTips += len(key.GetConsumedTips())
		}
	}

	if proof := eff.GetSnapshot().GetEvidence(); proof != nil {
		r.ProofRefBytes += proto.Size(proof)
		for _, refs := range [][]*pb.EffectRef{proof.GetBinds(), proof.GetDeps()} {
			for _, ref := range refs {
				r.proof[effects.Tip{ref.GetNodeId(), ref.GetOffset()}] = true
			}
		}

		r.ProofRefs = len(r.proof)
	}
}

// Embed the concrete adapters to preserve all optional discovery and ancestry
// capabilities. No engine behavior or routing policy is replaced by the probes.
type bplusProfileHandler struct {
	*EngineEffectHandler

	observer *bplusProfileObserver
	peer     uint64
}

func (h *bplusProfileHandler) HandleOffsetNotify(n *pb.OffsetNotify) ([]*pb.NackNotify, error) {
	ref := h.observer.lookup(effects.Tip{n.GetOrigin().GetNodeId(), n.GetOrigin().GetOffset()})

	var (
		nacks []*pb.NackNotify
		err   error
	)

	start := time.Now()

	pprof.Do(context.Background(), pprof.Labels("phase", "receive"), func(context.Context) { nacks, err = h.EngineEffectHandler.HandleOffsetNotify(n) })

	if r := ref.row; r != nil {
		r.mu.Lock()
		defer r.mu.Unlock()

		r.Notifies++
		r.NotifyBytes += len(n.GetEffectData())

		r.ReceiveNS += time.Since(start).Nanoseconds()
		if ref.bind {
			r.peers[h.peer] = true
			r.BindPeers = len(r.peers)
			r.BindReceiveNS = max(r.BindReceiveNS, time.Since(start).Nanoseconds())
		}

		for _, nack := range nacks {
			r.NackBytes += proto.Size(nack)

			r.NackRefVisits += len(nack.GetCausalChain())
			for _, tip := range nack.GetCausalChain() {
				r.nack[effects.Tip{tip.GetNodeId(), tip.GetOffset()}] = true
			}
		}

		r.NackUnique = len(r.nack)
	}

	return nacks, err
}

type bplusProfileReader struct {
	*EngineLogReader

	observer *bplusProfileObserver
}

func recordBPlusHistory(r *bplusProfileRow, ref effects.Tip, size int) {
	if r == nil || size == 0 {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.HistoryRecords++
	r.HistoryBytes += size
	r.history[ref] = true
	r.HistoryUnique = len(r.history)
}

func (r *bplusProfileReader) ReadEffect(ref *pb.EffectRef) ([]byte, error) {
	tip := effects.Tip{ref.GetNodeId(), ref.GetOffset()}
	// Attribute old history to the request currently demanding it, not to the
	// earlier request that originally published the referenced effect.
	row := r.observer.active.Load()

	var (
		data []byte
		err  error
	)

	pprof.Do(context.Background(), pprof.Labels("phase", "serve-history"), func(context.Context) { data, err = r.EngineLogReader.ReadEffect(ref) })
	recordBPlusHistory(row, tip, len(data))

	return data, err
}

func (r *bplusProfileReader) ReadAncestry(key string, root effects.Tip, send func(*pb.AncestryResponse) error) error {
	row := r.observer.active.Load()

	var err error

	pprof.Do(context.Background(), pprof.Labels("phase", "serve-history"), func(context.Context) {
		err = r.EngineLogReader.ReadAncestry(key, root, func(resp *pb.AncestryResponse) error {
			recordBPlusHistory(row, effects.Tip{resp.GetRef().GetNodeId(), resp.GetRef().GetOffset()}, len(resp.GetEffectData()))

			return send(resp)
		})
	})

	return err
}

// Profiles exclude fixture construction and post-run checking. Rows are a
// diagnostic run with probes enabled, not replacements for baseline latency.
func TestBPlusTraceProfile(t *testing.T) {
	dir := os.Getenv("BPLUS_PROFILE_DIR")
	if dir == "" {
		t.Skip("set BPLUS_PROFILE_DIR for opt-in profiling")
	}

	build := os.Getenv("BPLUS_BUILD")
	if build != "SingleTx" && build != "Incremental" {
		t.Fatal("invalid BPLUS_BUILD")
	}

	peers, err := strconv.Atoi(os.Getenv("BPLUS_PEERS"))
	if err != nil || peers < 1 || peers > 5 {
		t.Fatal("invalid BPLUS_PEERS")
	}

	entries, err := strconv.Atoi(os.Getenv("BPLUS_ENTRIES"))
	if err != nil || entries < 16 {
		t.Fatal("invalid BPLUS_ENTRIES")
	}

	workload := bplusWorkload{name: os.Getenv("BPLUS_WORKLOAD")}
	switch workload.name {
	case "Replace":
		workload.replacements = 20
	case "Structural":
	case "Mixed":
		workload.reads, workload.replacements = 10, 5
	default:
		t.Fatal("invalid BPLUS_WORKLOAD")
	}

	logger := slog.Default()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer slog.SetDefault(logger)

	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	for repeat := range 3 {
		func() {
			scope := &bplusBenchScope{TB: t}
			observer := &bplusProfileObserver{}

			engines, tree := bplusBenchFixture(scope, peers, entries, build, observer)
			defer scope.close()

			engines[0].SetOnLocalEffect(observer.local)

			prefix := filepath.Join(dir, fmt.Sprintf("trace-%d", repeat))
			writeProfile := func(kind, suffix string) {
				f, err := os.Create(prefix + suffix)
				if err != nil {
					t.Fatal(err)
				}

				if err := pprof.Lookup(kind).WriteTo(f, 0); err != nil {
					t.Fatal(err)
				}

				if err := f.Close(); err != nil {
					t.Fatal(err)
				}
			}

			runtime.GC()
			runtime.GC()
			writeProfile("allocs", "-alloc-before.pprof")

			f, err := os.Create(prefix + "-cpu.pprof")
			if err != nil {
				t.Fatal(err)
			}

			if err := pprof.StartCPUProfile(f); err != nil {
				t.Fatal(err)
			}

			defer func() {
				pprof.StopCPUProfile()

				if err := f.Close(); err != nil {
					t.Error(err)
				}
			}()

			trace, want := bplusTrace(entries, workload)

			rows := make([]*bplusProfileRow, 0, len(trace))
			for i, request := range trace {
				r := &bplusProfileRow{Ordinal: i, Kind: string(request.kind), peers: make(map[uint64]bool), proof: make(map[effects.Tip]bool), history: make(map[effects.Tip]bool), nack: make(map[effects.Tip]bool)}
				rows = append(rows, r)
				observer.active.Store(r)

				start := time.Now()
				c := engines[0].NewContext()

				pprof.Do(context.Background(), pprof.Labels("phase", "prepare"), func(ctx context.Context) {
					c.SetTraceCtx(ctx)

					switch request.kind {
					case 'g':
						got, found, err := tree.Get(c, request.key)
						if err != nil || !found || !bytes.Equal(got, request.value) {
							t.Fatalf("read: %v %v", found, err)
						}
					case 'p':
						if err := tree.Put(c, request.key, request.value); err != nil {
							t.Fatal(err)
						}
					case 'd':
						found, err := tree.Delete(c, request.key)
						if err != nil || !found {
							t.Fatalf("delete: %v %v", found, err)
						}
					}
				})

				prepared := time.Since(start)

				r.mu.Lock()
				r.ObservedAddresses = len(c.PendingKeysInternal())
				r.mu.Unlock()

				flushStart := time.Now()

				pprof.Do(context.Background(), pprof.Labels("phase", "flush"), func(ctx context.Context) {
					c.SetTraceCtx(ctx)

					if request.kind != 'g' {
						bplusBenchFlush(t, c)
					} else {
						c.Abort()
					}
				})
				r.mu.Lock()
				r.PrepareNS = prepared.Nanoseconds()
				r.FlushNS = time.Since(flushStart).Nanoseconds()
				r.TotalNS = time.Since(start).Nanoseconds()
				r.mu.Unlock()
			}

			observer.active.Store(nil)
			pprof.StopCPUProfile()
			engines[0].SetOnLocalEffect(nil)
			runtime.GC()
			runtime.GC()
			writeProfile("allocs", "-alloc-after.pprof")
			bplusBenchContents(t, engines, tree, want)
			// Teardown drains handlers before serializing diagnostic rows.
			scope.close()
			scope.cleanups = nil

			data, err := json.MarshalIndent(map[string]any{"build": build, "entries": entries, "peers": peers, "workload": workload.name, "repeat": repeat, "mem_profile_rate": runtime.MemProfileRate, "rows": rows}, "", "  ")
			if err != nil {
				t.Fatal(err)
			}

			if err := os.WriteFile(prefix+"-requests.json", data, 0o644); err != nil {
				t.Fatal(err)
			}
		}()
	}
}
