package cluster

import (
	"encoding/binary"
	"fmt"
	"slices"
	"testing"
	"time"
)

// Keep this decoder independent of ParseClockTick: the old receiver rejects
// v2 before recording liveness, and requires the exact v1 observation layout.
func parseV1TickForTest(body []byte) (*ClockTick, error) {
	if len(body) < 36 || body[1] != 1 {
		return nil, fmt.Errorf("not a version-1 tick")
	}

	count := int(binary.LittleEndian.Uint16(body[34:36]))
	if len(body) != 36+24*count {
		return nil, fmt.Errorf("invalid version-1 tick length")
	}

	tick := &ClockTick{
		NodeID:    NodeId(binary.LittleEndian.Uint64(body[2:10])),
		Seq:       binary.LittleEndian.Uint64(body[10:18]),
		WallClock: int64(binary.LittleEndian.Uint64(body[18:26])),
		JumpNs:    int64(binary.LittleEndian.Uint64(body[26:34])),
	}
	for off := 36; off < len(body); off += 24 {
		tick.Observed = append(tick.Observed, ObservedTick{
			NodeID:     NodeId(binary.LittleEndian.Uint64(body[off : off+8])),
			Seq:        binary.LittleEndian.Uint64(body[off+8 : off+16]),
			ObservedAt: int64(binary.LittleEndian.Uint64(body[off+16 : off+24])),
		})
	}

	return tick, nil
}

func TestHeartbeatMixedVersionRollout(t *testing.T) {
	health := NewPeerHealthTable(ClockConfig{})
	hm := NewHeartbeatManager(1, health, ClockConfig{})
	legacyHealth := NewPeerHealthTable(ClockConfig{})
	legacy := NewHeartbeatManager(2, legacyHealth, ClockConfig{})

	hm.AddPeer(2, "")
	hm.AddPeer(3, "")
	legacy.AddPeer(1, "")

	// A real v1 packet, constructed without the upgraded marshaler.
	v1 := make([]byte, 36)
	v1[0], v1[1] = PacketTypeClockTick, 1
	binary.LittleEndian.PutUint64(v1[2:10], 2)
	binary.LittleEndian.PutUint64(v1[10:18], 7)
	binary.LittleEndian.PutUint64(v1[18:26], uint64(time.Now().UnixNano()))

	tick, err := ParseClockTick(v1)
	if err != nil {
		t.Fatal(err)
	}

	hm.ProcessInboundTick(2, tick)

	v2, err := ParseClockTick(MarshalClockTick(&ClockTick{NodeID: 3, Seq: 9, WallClock: time.Now().UnixNano(), MyCount: 30, YourCount: 10}))
	if err != nil {
		t.Fatal(err)
	}

	hm.ProcessInboundTick(3, v2)

	for _, id := range []NodeId{2, 3} {
		health.Get(id).sent.Store(12)
		health.Get(id).processed.Store(24)
	}

	var (
		retained    [][]byte
		legacyTicks []*ClockTick
	)

	hm.sendFunc = func(id NodeId, packet []byte) error {
		retained = append(retained, packet)
		if id == 2 {
			got, err := parseV1TickForTest(packet)
			if err != nil {
				t.Fatal(err)
			}

			legacyTicks = append(legacyTicks, got)
			legacy.ProcessInboundTick(1, got)
		} else {
			got, err := ParseClockTick(packet)
			if err != nil || packet[1] != 2 || got.MyCount != 12 || got.YourCount != 24 {
				t.Fatalf("v2 peer lost progress: %+v, %v", got, err)
			}
		}

		return nil
	}
	hm.sendTick()
	hm.SendTickTo(2)
	hm.SendTickTo(3)
	hm.sendTick()

	if len(legacyTicks) != 3 || len(legacyTicks[0].Observed) != 2 || legacyTicks[0].Observed[0].Seq != 7 || legacyTicks[0].Observed[1].Seq != 9 {
		t.Fatalf("legacy observations changed: %+v", legacyTicks)
	}

	if len(legacyTicks[1].Observed) != 0 || len(legacyTicks[2].Observed) != 0 {
		t.Fatal("immediate or drained tick included stale observations")
	}

	legacyHealth.CheckLiveness()

	if got := legacyHealth.AliveSymmetricPeers("", map[NodeId]string{1: ""}); !slices.Equal(got, []NodeId{1}) {
		t.Fatalf("legacy peer excluded upgraded replication target: %v", got)
	}

	if health.Get(2).peerSent.Load() != 0 || health.Get(2).peerProcessed.Load() != 0 {
		t.Fatal("legacy ticks supplied reclamation evidence")
	}

	if _, err := parseV1TickForTest(retained[0]); err != nil {
		t.Fatalf("subsequent sends mutated a retained legacy packet: %v", err)
	}

	// Capability must not regress on delayed v1 packets after learning v2.
	hm.ProcessInboundTick(3, &ClockTick{version: 1, NodeID: 3, Seq: 1})

	if health.Get(3).tickVersion.Load() != 2 {
		t.Fatal("delayed legacy tick regressed peer capability")
	}

	hm.RemovePeer(2)
	hm.AddPeer(2, "")

	if health.Get(2).tickVersion.Load() != 0 {
		t.Fatal("removed peer retained wire version")
	}
}
