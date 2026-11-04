package beacon

import (
	"context"
	"errors"
	"net"
	"slices"
	"testing"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
)

func TestRuntimeFailedJoinReleasesResources(t *testing.T) {
	listener, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	address := listener.LocalAddr().String()

	port := listener.LocalAddr().(*net.UDPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	var ops []pb.EffectOp

	rt, err := NewRuntime(ctx, RuntimeConfig{
		ClusterPassphrase: t.Name(), ClusterPort: port, AdvertiseAddr: address,
		OnLocalEffect: func(_ effects.Tip, eff *pb.Effect) {
			if string(eff.GetKey()) == MembershipKey && eff.GetData() != nil {
				ops = append(ops, eff.GetData().GetOp())

				cancel()
			}
		},
	})
	if rt != nil {
		if err := rt.Stop(); err != nil {
			t.Error(err)
		}

		t.Fatal("runtime returned after cancellation during registration")
	}

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("startup error = %v, want context cancellation", err)
	}

	if !slices.Equal(ops, []pb.EffectOp{pb.EffectOp_INSERT_OP, pb.EffectOp_REMOVE_OP}) {
		t.Fatalf("membership operations = %v, want registration and one rollback", ops)
	}

	// Failed startup must close the QUIC listener before returning to its caller.
	listener, err = net.ListenPacket("udp", address)
	if err != nil {
		t.Fatalf("failed startup retained listener: %v", err)
	}

	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
}
