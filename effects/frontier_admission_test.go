package effects

import (
	"fmt"
	"testing"
	"time"
)

func TestClosureAdmissionSerializesShardAcrossEntryPoints(t *testing.T) {
	e := newSnapshotEngine(nil)
	defer func() { _ = e.Close() }()

	key, collision, independent := "k", "", ""
	for i := 0; collision == "" || independent == ""; i++ {
		candidate := fmt.Sprintf("key-%d", i)
		if e.closureLock(candidate) == e.closureLock(key) {
			collision = candidate
		} else {
			independent = candidate
		}
	}

	entered := make(chan Tip, 4)

	release := make(chan struct{})
	defer close(release)

	e.broadcaster = &cutBroadcaster{load: func(tip Tip) ([]byte, error) {
		entered <- tip

		if tip == (Tip{7, 1}) {
			<-release
		}

		return nil, ErrEffectNotFound
	}}

	done := make(chan struct{}, 4)

	go func() {
		e.installIngestedTips(key, Tip{7, 1})

		done <- struct{}{}
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first closure did not start")
	}

	// Recovery of the same key and ingestion of a colliding key both wait.
	go func() {
		_, _, _ = e.closeIngestedFrontier(key, []Tip{{8, 1}}, make(map[Tip]error))

		done <- struct{}{}
	}()
	go func() {
		e.installIngestedTips(collision, Tip{9, 1})

		done <- struct{}{}
	}()
	go func() {
		e.installIngestedTips(independent, Tip{10, 1})

		done <- struct{}{}
	}()

	select {
	case tip := <-entered:
		if tip != (Tip{10, 1}) {
			t.Fatalf("same-shard walk entered concurrently: %v", tip)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("independent shard could not progress")
	}

	select {
	case tip := <-entered:
		t.Fatalf("same-shard walk entered concurrently: %v", tip)
	case <-time.After(20 * time.Millisecond):
	}

	// Release by sending so the deferred close also unblocks on failure.
	release <- struct{}{}

	for range 4 {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("closure did not finish after admission")
		}
	}
}
