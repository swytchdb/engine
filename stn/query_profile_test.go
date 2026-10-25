package stn

import (
	"encoding/json"
	"os"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"
)

// Optional profiling of the unchanged real-engine scaling fixture. Build is
// outside the profile so Network.Upper's query cost is isolated from Connect.
func TestProfileRealHistoryQueries(t *testing.T) {
	path := os.Getenv("STN_PROFILE_HISTORY")
	if path == "" {
		t.Skip("set STN_PROFILE_HISTORY to an emitted-history fixture")
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	var h struct {
		Groups []struct {
			Domains int
			Edges   []struct{ Before, After Sample }
		}
	}
	if err := json.NewDecoder(f).Decode(&h); err != nil {
		t.Fatal(err)
	}

	runtime.GOMAXPROCS(1)

	c, err := NewClocks(1000)
	if err != nil {
		t.Fatal(err)
	}

	first := h.Groups[0].Edges[0].Before

	var last Sample

	for _, g := range h.Groups {
		if g.Domains > 1000 {
			break
		}

		for _, o := range g.Edges {
			c, err = c.Before(o.Before, o.After)
			if err != nil {
				t.Fatal(err)
			}

			last = o.After
		}
	}

	out, err := os.Create(os.Getenv("STN_PROFILE_OUT"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = out.Close() }()

	if err := pprof.StartCPUProfile(out); err != nil {
		t.Fatal(err)
	}

	start := time.Now()

	var lo, hi int64

	queries := 0
	for queries < 100 || time.Since(start) < time.Second {
		lo, hi, err = c.Difference(first, last)
		if err != nil {
			t.Fatal(err)
		}

		queries++
	}

	elapsed := time.Since(start)

	pprof.StopCPUProfile()
	t.Logf("%d real-history queries: %s (%s/query), final bounds [%d,%d]", queries, elapsed, elapsed/time.Duration(queries), lo, hi)
}
