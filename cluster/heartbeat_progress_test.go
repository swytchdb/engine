package cluster

import (
	"testing"
)

func TestHeartbeatEffectCountersArePerRecipient(t *testing.T) {
	table := NewPeerHealthTable(ClockConfig{})
	hm := NewHeartbeatManager(1, table, ClockConfig{})
	hm.AddPeer(2, "")
	hm.AddPeer(3, "")
	table.Get(2).sent.Store(7)
	table.Get(2).processed.Store(22)
	table.Get(3).sent.Store(8)
	table.Get(3).processed.Store(17)

	packets := make(map[NodeId][]byte)
	hm.sendFunc = func(peer NodeId, packet []byte) error {
		packets[peer] = packet // The transport may retain each packet.

		return nil
	}

	for _, immediate := range []bool{false, true} {
		if immediate {
			hm.SendTickTo(2)
			hm.SendTickTo(3)
		} else {
			hm.sendTick()
		}

		for peer, want := range map[NodeId][2]uint64{2: {7, 22}, 3: {8, 17}} {
			tick, err := ParseClockTick(packets[peer])
			if err != nil {
				t.Fatal(err)
			}

			if tick.MyCount != want[0] || tick.YourCount != want[1] {
				t.Fatalf("peer %d: counts=(%d,%d), want %v", peer, tick.MyCount, tick.YourCount, want)
			}
		}
	}
}

func TestHeartbeatEffectEvidenceDoesNotRegress(t *testing.T) {
	table := NewPeerHealthTable(ClockConfig{})
	hm := NewHeartbeatManager(1, table, ClockConfig{})
	hm.AddPeer(2, "")
	hm.ProcessInboundTick(2, &ClockTick{NodeID: 2, Seq: 2, MyCount: 22, YourCount: 7})
	hm.ProcessInboundTick(2, &ClockTick{NodeID: 2, Seq: 1, MyCount: 17, YourCount: 5})

	peer := table.Get(2)
	if peer.peerSent.Load() != 22 || peer.peerProcessed.Load() != 7 {
		t.Fatal("a delayed heartbeat regressed effect evidence")
	}

	hm.RemovePeer(2)
	hm.AddPeer(3, "")

	if table.Get(2) != nil || table.Get(3).peerSent.Load() != 0 || table.Get(3).peerProcessed.Load() != 0 {
		t.Fatal("a new incarnation inherited the departed peer's evidence")
	}
}

func TestHeartbeatAppliesOverlayWithoutChangingTruth(t *testing.T) {
	table := NewPeerHealthTable(ClockConfig{})
	hm := NewHeartbeatManager(1, table, ClockConfig{})
	hm.AddPeer(2, "")

	peer := table.Get(2)
	peer.sent.Store(12)
	peer.processed.Store(30)

	hm.constrainProgress = func(id NodeId, my, your uint64) (uint64, uint64) {
		if id != 2 || my != 12 || your != 30 {
			t.Fatal("overlay did not receive the authoritative peer counters")
		}

		return 7, 17
	}

	var sent int

	hm.sendFunc = func(_ NodeId, data []byte) error {
		tick, err := ParseClockTick(data)
		if err != nil {
			t.Fatal(err)
		}

		if tick.MyCount != 7 || tick.YourCount != 17 {
			t.Fatalf("heartbeat ignored overlay: %+v", tick)
		}

		sent++

		return nil
	}
	hm.sendTick()
	hm.SendTickTo(2)

	if sent != 2 || peer.sent.Load() != 12 || peer.processed.Load() != 30 {
		t.Fatal("overlay changed authoritative progress")
	}
}
