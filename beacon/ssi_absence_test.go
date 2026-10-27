package beacon

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
)

func TestRuntimeSSIAbsentKeyWriteSkew(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)

	start := func(join string) (*Runtime, string) {
		t.Helper()

		listener, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}

		port := listener.LocalAddr().(*net.UDPAddr).Port
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}

		address := fmt.Sprintf("127.0.0.1:%d", port)
		if join == "" {
			join = address
		}

		rt, err := NewRuntime(ctx, RuntimeConfig{
			ClusterPassphrase: t.Name(), ClusterPort: port, AdvertiseAddr: address, JoinAddr: join,
		})
		if err != nil {
			t.Fatal(err)
		}

		t.Cleanup(func() {
			if err := rt.Stop(); err != nil {
				t.Error(err)
			}
		})

		return rt, address
	}

	first, address := start("")
	second, _ := start(address)

	write := func(tx *effects.Context, key, value string) {
		t.Helper()

		if err := tx.Emit(&pb.Effect{Key: []byte(key), Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op: pb.EffectOp_INSERT_OP, Merge: pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_SCALAR, Value: &pb.DataEffect_Raw{Raw: []byte(value)},
		}}}); err != nil {
			t.Fatal(err)
		}
	}

	waitValue := func(engine *effects.Engine, key, want string) {
		t.Helper()

		for {
			result, _, _, err := engine.GetSnapshot(key)
			if err == nil && string(result.GetScalar().GetRaw()) == want {
				return
			}

			select {
			case <-ctx.Done():
				t.Fatalf("key %s never converged to %q: result=%v err=%v", key, want, result, err)
			case <-time.After(10 * time.Millisecond):
			}
		}
	}

	// Prove both replication directions before testing fresh, unrelated keys.
	for i, pair := range [][2]*Runtime{{first, second}, {second, first}} {
		key := fmt.Sprintf("ssi-ready-%d", i)
		tx := pair[0].Engine.NewContext()
		write(tx, key, "ready")

		if err := tx.Flush(); err != nil {
			t.Fatal(err)
		}

		waitValue(pair[1].Engine, key, "ready")
	}

	for attempt := range 4 {
		x, y := fmt.Sprintf("ssi-x-%d", attempt), fmt.Sprintf("ssi-y-%d", attempt)
		a, b := first.Engine.NewContext(), second.Engine.NewContext()
		a.BeginTx()
		b.BeginTx()

		// Finish both reads and writes before either commit. The conflict is
		// an rw cycle across different keys, not a same-key write collision.
		for _, read := range []struct {
			tx  *effects.Context
			key string
		}{{a, x}, {b, y}} {
			result, _, err := read.tx.GetSnapshot(read.key)
			if err != nil || result != nil {
				t.Fatalf("expected a fresh absent key: result=%v err=%v", result, err)
			}
		}

		write(a, y, "A")
		write(b, x, "B")

		doneA, doneB := make(chan error, 1), make(chan error, 1)
		go func() { doneA <- a.Flush() }()
		go func() { doneB <- b.Flush() }()

		errA, errB := <-doneA, <-doneB
		if errA == nil && errB == nil {
			t.Fatal("both transactions committed despite opposing absence reads")
		}

		for _, err := range []error{errA, errB} {
			if err != nil && !errors.Is(err, effects.ErrTxnAborted) {
				t.Fatalf("unexpected commit failure: %v", err)
			}
		}

		wantX, wantY := "", ""
		if errA == nil {
			wantY = "A"
		}

		if errB == nil {
			wantX = "B"
		}

		for _, rt := range []*Runtime{first, second} {
			waitValue(rt.Engine, x, wantX)
			waitValue(rt.Engine, y, wantY)
		}

		// Aborting both contenders is safe, but must still allow a new
		// uncontended transaction to read and write the same keys.
		for {
			retry := first.Engine.NewContext()
			retry.BeginTx()

			if _, err := retry.GetSnapshots([]string{x, y}); err != nil {
				t.Fatal(err)
			}

			write(retry, x, "retry")

			err := retry.Flush()
			if err == nil {
				break
			}

			if !errors.Is(err, effects.ErrTxnAborted) {
				t.Fatalf("unexpected retry failure: %v", err)
			}

			// Previous commit/abort verdicts can still arrive while this
			// attempt runs. Require eventual progress with fresh snapshots.
			select {
			case <-ctx.Done():
				t.Fatal("uncontended retries never committed")
			case <-time.After(10 * time.Millisecond):
			}
		}

		waitValue(second.Engine, x, "retry")
	}
}
