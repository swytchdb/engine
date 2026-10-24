package durability

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func openTest(t testing.TB, dir string) *Store {
	t.Helper()

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})

	return s
}

func TestRoundTripGrowthAndEviction(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, dir)
	// All records share a file, exceeding a TOC page and the initial mapping.
	for i := range 300 {
		if err := s.Put(ID{7, uint64(i) * fileCount}, bytes.Repeat([]byte{byte(i)}, 5000)); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 300; i += 2 {
		if err := s.Delete(ID{7, uint64(i) * fileCount}); err != nil {
			t.Fatal(err)
		}
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = openTest(t, dir)

	var mu sync.Mutex

	seen := map[uint64]bool{}

	err := s.Recover(context.Background(), 8, func(id ID, data []byte) error {
		i := id[1] / fileCount
		if id[0] != 7 || i%2 == 0 || !bytes.Equal(data, bytes.Repeat([]byte{byte(i)}, 5000)) {
			return fmt.Errorf("bad record %v", id)
		}

		mu.Lock()
		seen[i] = true
		mu.Unlock()

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(seen) != 150 {
		t.Fatalf("restored %d records", len(seen))
	}

	for i := 1; i < 300; i += 2 {
		if err := s.Delete(ID{7, uint64(i) * fileCount}); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := os.Stat(filepath.Join(dir, "7", "0.swytch")); !os.IsNotExist(err) {
		t.Fatalf("empty file still exists: %v", err)
	}

	if err := s.Put(ID{7, 64}, []byte("recreated")); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentPublishDeleteAndRecreate(t *testing.T) {
	s := openTest(t, t.TempDir())

	var wg sync.WaitGroup
	for worker := range 12 {
		wg.Go(func() {
			id := ID{1, uint64(worker) * fileCount}
			for range 40 {
				if err := s.Put(id, []byte("payload")); err != nil {
					t.Error(err)

					return
				}

				if err := s.Delete(id); err != nil {
					t.Error(err)

					return
				}
			}

			if err := s.Put(id, []byte("final")); err != nil {
				t.Error(err)
			}
		})
	}

	wg.Wait()

	var mu sync.Mutex

	count := 0

	if err := s.Recover(context.Background(), 12, func(_ ID, p []byte) error {
		if string(p) != "final" {
			return fmt.Errorf("bad payload %q", p)
		}

		mu.Lock()
		count++
		mu.Unlock()

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if count != 12 {
		t.Fatalf("count=%d", count)
	}
}

func TestRecoveryChecksumsAndIncompleteRecords(t *testing.T) {
	dir := t.TempDir()

	s := openTest(t, dir)
	if err := s.Put(ID{1, 0}, []byte("valid")); err != nil {
		t.Fatal(err)
	}

	if err := s.Put(ID{1, 64}, []byte("unfinished")); err != nil {
		t.Fatal(err)
	}

	b := s.get(ID{1, 0})
	b.life.Lock()
	save(b.data, b.entries[64], 0)
	b.life.Unlock()

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = openTest(t, dir)
	count := 0

	if err := s.Recover(context.Background(), 1, func(_ ID, _ []byte) error {
		count++

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if count != 1 {
		t.Fatalf("count=%d", count)
	}

	b = s.get(ID{1, 0})
	b.life.Lock()
	b.data[load(b.data, b.entries[0]+16)] ^= 1
	b.life.Unlock()

	if err := s.Recover(context.Background(), 1, func(_ ID, _ []byte) error { return nil }); err == nil {
		t.Fatal("corruption accepted")
	}
}

func TestDirectoryLockResetAndCancellation(t *testing.T) {
	dir := t.TempDir()

	s := openTest(t, dir)
	if other, err := Open(dir); err == nil {
		_ = other.Close()

		t.Fatal("concurrent directory owner accepted")
	}

	if err := os.WriteFile(filepath.Join(dir, "unrelated"), []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}

	if err := s.Put(ID{2, 3}, []byte("effect")); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := s.Recover(ctx, 2, func(_ ID, _ []byte) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}

	if err := s.Reset(); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(filepath.Join(dir, "unrelated")); err != nil {
		t.Fatal(err)
	}

	if err := s.Recover(context.Background(), 2, func(_ ID, _ []byte) error { return errors.New("unexpected record") }); err != nil {
		t.Fatal(err)
	}
}

func TestSyncDeadlineDoesNotDebounce(t *testing.T) {
	s := openTest(t, t.TempDir())
	if err := s.Put(ID{1, 0}, []byte("first")); err != nil {
		t.Fatal(err)
	}

	b := s.get(ID{1, 0})
	b.life.RLock()
	b.mu.Lock()
	first := b.due
	b.mu.Unlock()
	b.life.RUnlock()

	if err := s.Put(ID{1, 64}, []byte("second")); err != nil {
		t.Fatal(err)
	}

	b.life.RLock()
	b.mu.Lock()
	second := b.due
	b.mu.Unlock()
	b.life.RUnlock()

	if !second.IsZero() && !first.Equal(second) {
		t.Fatalf("deadline moved: %v -> %v", first, second)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		b.life.RLock()
		b.mu.Lock()
		done := b.synced == b.generation
		b.mu.Unlock()
		b.life.RUnlock()

		if done {
			return
		}

		time.Sleep(time.Millisecond)
	}

	t.Fatal("background sync did not complete")
}

func TestFailedSyncRemainsDirty(t *testing.T) {
	s := openTest(t, t.TempDir())
	if err := s.Put(ID{1, 0}, []byte("record")); err != nil {
		t.Fatal(err)
	}

	b := s.get(ID{1, 0})
	b.life.Lock()
	if err := b.file.Close(); err != nil {
		t.Fatal(err)
	}

	b.dirty()
	b.life.Unlock()

	if err := s.Sync(); err == nil {
		t.Fatal("sync failure hidden")
	}

	b.life.Lock()
	if b.synced == b.generation || b.due.IsZero() {
		t.Error("failed sync marked clean")
	}

	f, err := os.OpenFile(b.path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}

	b.file = f
	b.life.Unlock()

	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
}

func TestDiscardWithoutLoadingTOC(t *testing.T) {
	dir := t.TempDir()

	s := openTest(t, dir)
	if err := s.Put(ID{1, 0}, []byte("record")); err != nil {
		t.Fatal(err)
	}

	if err := Discard(dir); err == nil {
		t.Fatal("discard bypassed directory ownership")
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	if err := Discard(dir); err != nil {
		t.Fatal(err)
	}

	s = openTest(t, dir)
	if err := s.Recover(context.Background(), 1, func(ID, []byte) error { return errors.New("unexpected effect") }); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkRecovery(b *testing.B) {
	s := openTest(b, b.TempDir())
	for i := range 10000 {
		if err := s.Put(ID{1, uint64(i)}, bytes.Repeat([]byte("x"), 256)); err != nil {
			b.Fatal(err)
		}
	}

	if err := s.Sync(); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for range b.N {
		if err := s.Recover(context.Background(), 0, func(_ ID, p []byte) error {
			_ = p[0]

			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}
}
