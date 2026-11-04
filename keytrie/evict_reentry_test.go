package keytrie

import (
	"testing"
	"time"
)

func TestEvictionCallbacksCanEvict(t *testing.T) {
	for _, hook := range []string{"notify", "refDelta"} {
		t.Run(hook, func(t *testing.T) {
			c := NewCritbit[struct{}]()

			t.Cleanup(func() { closeTestTrie(t, c) })

			c.Insert("a", nil, NewTipSet(tip(1)))
			c.Insert("b", nil, NewTipSet(tip(2)))

			nested := false
			reenter := func() {
				if !nested {
					nested = true

					c.EvictBatch(1)
				}
			}

			if hook == "notify" {
				c.SetEvictHooks(nil, func(string, []EffectRef, *struct{}) { reenter() }, nil)
			} else {
				c.SetRefDelta(func(_, _ []EffectRef, _ *struct{}) { reenter() })
			}

			done := make(chan struct{})

			go func() {
				c.EvictBatch(1)
				close(done)
			}()

			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("eviction callback deadlocked while re-entering eviction")
			}

			if !nested || c.Size() != 0 {
				t.Fatalf("nested eviction did not remove both keys: size=%d", c.Size())
			}
		})
	}
}
