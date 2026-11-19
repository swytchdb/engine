package effects

import (
	"runtime"
	"strings"
	"testing"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
)

type pausedReclaimMirror struct {
	entered chan struct{}
	resume  chan struct{}
}

func TestWithEffectRetainsEvidenceDuringKeyRelease(t *testing.T) {
	p := newVertexPool()

	proof, owner := Tip{2, 1}, Tip{2, 2}
	if err := p.PutSizedCache(proof, scalarSet("foreign", "proof"), 0); err != nil {
		t.Fatal(err)
	}

	if err := p.Put(owner, evidenceSnapshot("x", proof)); err != nil {
		t.Fatal(err)
	}

	err := p.WithEffect(owner, func(eff *pb.Effect) error {
		p.decrefChainNode("x", owner)
		drainEvidence(p)

		if eff == nil || p.getVertex(owner) == nil || p.getVertex(proof) == nil {
			t.Fatal("key release reclaimed a visitor's evidence")
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	drainEvidence(p)

	if p.EntryCount() != 0 {
		t.Fatal("visitor retained evidence after returning")
	}
}

func (*pausedReclaimMirror) Put(Tip, []byte) error { return nil }
func (*pausedReclaimMirror) Reset() error          { return nil }
func (*pausedReclaimMirror) Close() error          { return nil }
func (m *pausedReclaimMirror) Delete(Tip) error {
	close(m.entered)
	<-m.resume

	return nil
}

func TestDiskHydrationRetriesRetiredVertex(t *testing.T) {
	p := newVertexPool()
	tip := Tip{1, 1}

	eff := scalarSet("x", "retained")
	if err := p.PutSizedCache(tip, eff, 0); err != nil {
		t.Fatal(err)
	}

	old := p.getVertex(tip)
	mirror := &pausedReclaimMirror{entered: make(chan struct{}), resume: make(chan struct{})}
	p.mirror = mirror
	reclaimed := make(chan struct{})

	go func() { p.drainReclaim(); close(reclaimed) }()

	<-mirror.entered

	loaded := make(chan error, 1)
	go func() { loaded <- p.putInternal(tip, eff, false, nil, true, false) }()
	// Force the loader to have observed the old vertex, rather than merely
	// racing two goroutines and hoping to hit the retirement window. Reclaim
	// owns creationMu while paused in Delete; the loader must wait on it.
	deadline := time.Now().Add(3 * time.Second)
	blocked := false

	for time.Now().Before(deadline) {
		buf := make([]byte, 1<<20)

		buf = buf[:runtime.Stack(buf, true)]
		for _, stack := range strings.Split(string(buf), "\n\n") {
			if strings.Contains(stack, "(*VertexPool).putInternal") && strings.Contains(stack, "Mutex).lockSlow") {
				blocked = true

				break
			}
		}

		if blocked {
			break
		}

		runtime.Gosched()
	}

	close(mirror.resume)
	<-reclaimed

	if err := <-loaded; err != nil {
		t.Fatal(err)
	}

	if !blocked {
		t.Fatal("did not exercise loading a vertex during retirement")
	}

	fresh := p.getVertex(tip)
	if fresh == nil || fresh == old || fresh.refs.Load() != 1 {
		t.Fatal("hydration did not install and retain a new vertex")
	}

	p.mirror = nil
	p.decref(tip)
	p.drainReclaim()

	if p.EntryCount() != 0 {
		t.Fatal("reinstalled vertex leaked")
	}
}
