package testmodel

import (
	"math"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"google.golang.org/protobuf/proto"
)

func TestAccountPolicies(t *testing.T) {
	e := effects.NewTestEngine()

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})

	c := e.NewContext()
	defer c.Abort()

	a := NewAccount().SetName("Rob").AddBalance(math.MaxInt64).MaxLastSeen(-10).MaxLastSeen(-20).AddWeight(1.25)
	if a.Name() != "Rob" || a.Balance() != math.MaxInt64 || a.LastSeen() != -10 || a.Weight() != 1.25 {
		t.Fatal("initial local reductions differ from field policies")
	}

	if err := a.Emit("account", c, nil); err != nil {
		t.Fatal(err)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	read := e.NewContext()
	defer read.Abort()

	snap, tips, err := read.GetSnapshot("account")
	if err != nil {
		t.Fatal(err)
	}

	before := proto.Clone(snap)

	a, err = AccountFromSnap(snap)
	if err != nil {
		t.Fatal(err)
	}

	a.AddBalance(-5).SetName("Updated").MaxLastSeen(20).MaxLastSeen(15).AddWeight(0.5)

	if a.Balance() != math.MaxInt64-5 || a.LastSeen() != 20 || a.Name() != "Updated" || a.Weight() != 1.75 {
		t.Fatal("snapshot plus local effects reduced incorrectly")
	}

	if !proto.Equal(snap, before) {
		t.Fatal("mutated seed")
	}

	if err := a.Emit("account", read, tips); err != nil {
		t.Fatal(err)
	}

	if err := read.Flush(); err != nil {
		t.Fatal(err)
	}

	final := e.NewContext()
	defer final.Abort()

	snap, _, err = final.GetSnapshot("account")
	if err != nil {
		t.Fatal(err)
	}

	a, err = AccountFromSnap(snap)
	if err != nil {
		t.Fatal(err)
	}

	if a.Balance() != math.MaxInt64-5 || a.LastSeen() != 20 || a.Name() != "Updated" || a.Weight() != 1.75 {
		t.Fatal("engine result differs from local preview")
	}

	for field, rule := range map[string]pb.MergeRule{"balance": pb.MergeRule_ADDITIVE_INT, "lastSeen": pb.MergeRule_MAX_INT, "weight": pb.MergeRule_ADDITIVE_FLOAT} {
		if snap.GetNetAdds()[field].GetData().GetMerge() != rule {
			t.Fatalf("wrong wire merge for %s", field)
		}
	}
}
