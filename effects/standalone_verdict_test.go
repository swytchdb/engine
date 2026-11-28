package effects

import (
	"fmt"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

// bindAtTip follows a verdict snapshot to the committed bind it retains.
func bindAtTip(t *testing.T, e *Engine, tip Tip) *pb.Effect {
	t.Helper()

	eff, err := e.getEffect("", tip)
	if err != nil {
		t.Fatal(err)
	}

	if snap := eff.GetSnapshot(); snap != nil {
		refs := snap.GetEvidence().GetBinds()
		if len(refs) != 1 {
			t.Fatalf("expected one committed bind at %v, got %v", tip, refs)
		}

		eff, err = e.getEffect("", r(refs[0]))
		if err != nil {
			t.Fatal(err)
		}
	}

	return eff
}

func TestFlushTx_StandaloneEmitsVerdictSnapshots(t *testing.T) {
	for _, tc := range []struct {
		broadcaster bool
		horizon     bool
	}{
		{},
		{broadcaster: true},
		{horizon: true},
		{broadcaster: true, horizon: true},
	} {
		t.Run(fmt.Sprintf("broadcaster=%t/horizon=%t", tc.broadcaster, tc.horizon), func(t *testing.T) {
			var bc Broadcaster
			if tc.broadcaster {
				bc = &mockBroadcaster{}
			}

			e := newTxnTestEngine(bc)
			if tc.horizon {
				e.horizon = newHorizonSet(e)
			}

			keys := []string{"first", "second"}

			// Repeated commits must preserve both the data and usable evidence.
			for round := range 3 {
				ctx := e.NewContext()
				ctx.BeginTx()
				txnID := ctx.txnID

				want := fmt.Sprintf("value-%d", round)
				for _, key := range keys {
					if err := ctx.Emit(scalarSet(key, want)); err != nil {
						t.Fatal(err)
					}
				}

				if err := ctx.Flush(); err != nil {
					t.Fatal(err)
				}

				if e.pendingTxns.Size() != 0 || e.pendingTxTips.Size() != 0 {
					t.Fatal("commit left pending transaction state")
				}

				for _, key := range keys {
					tips := e.index.Contains(key).Tips()
					if len(tips) != 1 {
						t.Fatalf("%s: expected one verdict snapshot tip, got %v", key, tips)
					}

					eff, err := e.getEffect(logicalAddress(key), tips[0])
					if err != nil {
						t.Fatal(err)
					}

					snap := eff.GetSnapshot()
					if verdict, ok := snap.GetTxnVerdicts()[txnID]; !ok || verdict != pb.Verdict_WON {
						t.Fatalf("%s: missing WON verdict for %s: %v", key, txnID, snap)
					}

					if len(snap.GetEvidence().GetBinds()) != 1 {
						t.Fatalf("%s: missing bind evidence: %v", key, snap)
					}

					verdicts, err := e.proofVerdicts(tips[0], eff)
					if err != nil || !verdicts[txnID].proven || verdicts[txnID].verdict != pb.Verdict_WON {
						t.Fatalf("%s: cannot recompute verdict: %v, %v", key, verdicts, err)
					}

					got, _, _, err := e.GetSnapshot(key)
					if err != nil || string(got.GetScalar().GetRaw()) != want {
						t.Fatalf("%s: value = %v, error = %v; want %s", key, got, err, want)
					}
				}
			}
		})
	}
}
