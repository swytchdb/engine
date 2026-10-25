package crdt

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func roundTripClock(t *testing.T, at time.Time) time.Time {
	t.Helper()

	b, err := proto.Marshal(timestamppb.New(at))
	if err != nil {
		t.Fatal(err)
	}

	var stamp timestamppb.Timestamp
	if err := proto.Unmarshal(b, &stamp); err != nil {
		t.Fatal(err)
	}

	return stamp.AsTime()
}

func TestElapsedClockSerializedIntervals(t *testing.T) {
	origin := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)

	var elapsed time.Duration

	clock := newElapsedClock(origin, func() time.Duration { return elapsed })
	first := roundTripClock(t, clock.Now())
	// The wall reference can be changed in either direction after startup;
	// only elapsed time drives this clock. No system-wide clock is changed.
	for _, wallStep := range []time.Duration{-time.Hour, 2 * time.Hour, -24 * time.Hour} {
		origin = origin.Add(wallStep)
		elapsed += 17*time.Millisecond + 123*time.Nanosecond

		got := roundTripClock(t, clock.Now()).Sub(first)
		if got != elapsed {
			t.Fatalf("after wall step %v: serialized elapsed %v, want %v", wallStep, got, elapsed)
		}
	}
}

func TestElapsedClockRealMonotonicRoundTrip(t *testing.T) {
	clock := NewHLC()
	a := clock.Now()

	time.Sleep(time.Millisecond)

	b := clock.Now()
	if got, want := roundTripClock(t, b).Sub(roundTripClock(t, a)), b.Sub(a); got != want || got <= 0 {
		t.Fatalf("serialized elapsed %v, monotonic elapsed %v", got, want)
	}
}
