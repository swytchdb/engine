package keytrie

import "testing"

func TestAdaptiveThresholdFeedback(t *testing.T) {
	for _, tt := range []struct {
		name                         string
		dir                          int32
		hits                         uint64
		low, high, wantLow, wantHigh uint32
	}{
		{"raised improved", 1, 1500, 2500, 5000, 2500, 4000},
		{"raised worsened", 1, 500, 2500, 5000, 2500, 6000},
		{"raised unchanged", 1, 1000, 2500, 5000, 2500, 6000},
		{"lowered improved", -1, 1500, 2500, 5000, 3500, 5000},
		{"lowered worsened", -1, 500, 2500, 5000, 1500, 5000},
		{"lowered unchanged", -1, 1000, 2500, 5000, 1500, 5000},
		{"high lower bound", 1, 1500, 2500, 4000, 2500, 4000},
		{"high upper bound", 1, 500, 2500, 7000, 2500, 7000},
		{"low lower bound", -1, 500, 1500, 5000, 1500, 5000},
		{"low upper bound", -1, 1500, 3000, 5000, 3000, 5000},
		{"no previous change", 0, 1500, 2500, 5000, 2500, 5000},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := NewCritbit[struct{}]()

			t.Cleanup(func() { closeTestTrie(t, c) })
			c.evictedUnprot.Store(adaptiveCheckInterval)
			c.windowOps.shards[0].v.Store(hitRateWindowSize)
			c.windowHits.shards[0].v.Store(tt.hits)
			c.prevHitRate.Store(5000)
			c.lastKDir.Store(tt.dir)
			c.rateLow.Store(tt.low)
			c.rateHigh.Store(tt.high)
			c.maybeAdaptK()

			if c.rateLow.Load() != tt.wantLow || c.rateHigh.Load() != tt.wantHigh {
				t.Fatalf("thresholds = %d/%d, want %d/%d", c.rateLow.Load(), c.rateHigh.Load(), tt.wantLow, tt.wantHigh)
			}
		})
	}
}
