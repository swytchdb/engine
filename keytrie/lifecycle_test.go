package keytrie

import "testing"

func TestReapQueueAllocatedOnlyForDeletion(t *testing.T) {
	c := NewCritbit[struct{}]()

	t.Cleanup(func() { closeTestTrie(t, c) })
	c.reapRunning.Store(true) // inspect the queue before maintenance consumes it

	tips := NewTipSet(tip(1))
	c.Insert("key", nil, tips)

	if c.reapQueue.Load() != nil {
		t.Fatal("inserting a live key allocated the deletion queue")
	}

	if !c.Delete("key", tips) || c.reapQueue.Load() == nil {
		t.Fatal("deletion did not publish its pending unlink")
	}

	if c.drainReap() != 1 || c.root.Load() != nil {
		t.Fatal("pending deletion was not unlinked")
	}

	closeTestTrie(t, c)

	if c.reapQueue.Load() != nil {
		t.Fatal("close retained the deletion queue")
	}
}

func TestDeleteCallbackCanClose(t *testing.T) {
	c := NewCritbit[struct{}]()

	t.Cleanup(func() { closeTestTrie(t, c) })
	c.SetRefDelta(func(_, removed []EffectRef, _ *struct{}) {
		if len(removed) > 0 {
			closeTestTrie(t, c)
		}
	})

	tips := NewTipSet(tip(1))
	c.Insert("key", nil, tips)

	if !c.Delete("key", tips) || !c.Closed() {
		t.Fatal("deletion callback did not close the trie")
	}

	if c.reapQueue.Load() != nil {
		t.Fatal("deletion queued work after the callback closed the trie")
	}
}
