// Package durability mirrors resident immutable effects in mmap-backed files.
// It is a restart source, not a read tier. Callbacks must not retain mapped bytes.
package durability

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	fileCount     = 64
	formatVersion = 1
	entrySize     = 64
	syncDelay     = 10 * time.Millisecond
	published     = 1
	deleted       = 2
)

var ErrClosed = errors.New("durability store is closed")

// ID preserves the engine's (originating node, effect offset) identity.
type ID = [2]uint64

type bucketID struct{ node, bucket uint64 }

// Store owns its directory exclusively until Close. Unrelated files are never removed.
type Store struct {
	life    sync.RWMutex
	closed  bool
	dir     string
	lock    *os.File
	mu      sync.Mutex
	files   map[bucketID]*bucket
	stripes [256]sync.Mutex
	wake    chan struct{}
	stop    chan struct{}
	done    chan struct{}
	syncWG  sync.WaitGroup
}

type bucket struct {
	syncMu  sync.Mutex
	syncing atomic.Bool
	// RLock spans reservation through publication, and every mapped read.
	life                    sync.RWMutex
	mu                      sync.Mutex // TOC reservations and dirty generations, never payload copies
	file                    *os.File
	data                    []byte
	path                    string
	id                      bucketID
	block, cursor, lastPage uint64
	entries                 map[uint64]uint64 // effect offset -> TOC position
	free                    []uint64
	pages                   []uint64
	generation, synced      uint64
	due                     time.Time
	punches                 []extent
}

type extent struct{ pos, size uint64 }

// Open validates file metadata and starts deadline-based background syncing.
func Open(dir string) (*Store, error) {
	return OpenContext(context.Background(), dir)
}

// OpenContext also permits cancellation while loading a large TOC inventory.
func OpenContext(ctx context.Context, dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}

	lock, err := lockDirectory(dir)
	if err != nil {
		return nil, err
	}

	s := &Store{dir: dir, lock: lock, files: make(map[bucketID]*bucket), wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{})}

	if err := s.openFiles(ctx); err != nil {
		for _, b := range s.files {
			_ = b.close()
		}

		_ = lock.Close()

		return nil, err
	}
	go s.syncLoop()

	return s, nil
}

func (s *Store) openFiles(ctx context.Context) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	workers := runtime.GOMAXPROCS(0)
	jobs := make(chan *bucket, workers)

	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for b := range jobs {
				if ctx.Err() != nil {
					continue
				}

				if err := b.open(ctx, false); err != nil {
					cancel(fmt.Errorf("open %s: %w", b.path, err))

					continue
				}

				s.mu.Lock()

				_, duplicate := s.files[b.id]
				if !duplicate {
					s.files[b.id] = b
				}
				s.mu.Unlock()

				if duplicate {
					cancel(errors.Join(errors.New("duplicate durability bucket"), b.close()))
				}
			}
		})
	}

	err := s.visitFiles(ctx, func(b *bucket) error {
		select {
		case jobs <- b:
			return nil
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	})
	if err != nil {
		cancel(err)
	}

	close(jobs)
	wg.Wait()

	return context.Cause(ctx)
}

func (s *Store) visitFiles(ctx context.Context, visit func(*bucket) error) error {
	nodes, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}

	for _, node := range nodes {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}

		if !node.IsDir() {
			continue
		}

		n, err := strconv.ParseUint(node.Name(), 10, 64)
		if err != nil {
			continue
		}

		files, err := os.ReadDir(filepath.Join(s.dir, node.Name()))
		if err != nil {
			return err
		}

		for _, f := range files {
			if f.IsDir() || !strings.HasSuffix(f.Name(), ".swytch") {
				continue
			}

			v, err := strconv.ParseUint(strings.TrimSuffix(f.Name(), ".swytch"), 10, 64)
			if err != nil || v >= fileCount {
				return fmt.Errorf("invalid durability filename: %s", f.Name())
			}

			id := bucketID{n, v}

			b := &bucket{id: id, path: filepath.Join(s.dir, node.Name(), f.Name())}
			if err := visit(b); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *Store) get(id ID) *bucket {
	k := bucketID{id[0], id[1] % fileCount}

	s.mu.Lock()
	defer s.mu.Unlock()

	b := s.files[k]
	if b == nil {
		b = &bucket{id: k, path: filepath.Join(s.dir, strconv.FormatUint(k.node, 10), fmt.Sprintf("%d.swytch", k.bucket))}
		s.files[k] = b
	}

	return b
}

func align(n, block uint64) uint64         { return (n + block - 1) / block * block }
func word(data []byte, pos uint64) *uint64 { return (*uint64)(unsafe.Pointer(&data[pos])) }
func load(data []byte, pos uint64) uint64  { return atomic.LoadUint64(word(data, pos)) }
func save(data []byte, pos, value uint64)  { atomic.StoreUint64(word(data, pos), value) }

func recordChecksum(offset, pos, size, allocated uint64, data []byte) uint32 {
	var metadata [32]byte
	binary.LittleEndian.PutUint64(metadata[0:], offset)
	binary.LittleEndian.PutUint64(metadata[8:], pos)
	binary.LittleEndian.PutUint64(metadata[16:], size)
	binary.LittleEndian.PutUint64(metadata[24:], allocated)

	return crc32.Update(crc32.ChecksumIEEE(metadata[:]), crc32.IEEETable, data)
}

func (b *bucket) open(ctx context.Context, create bool) error {
	if create {
		if err := os.MkdirAll(filepath.Dir(b.path), 0o700); err != nil {
			return err
		}
	}

	f, err := os.OpenFile(b.path, os.O_RDWR, 0o600)

	fresh := create && os.IsNotExist(err)
	if fresh {
		// Publish the filename only after the complete header is synced. A crash
		// while initializing a new bucket must not break recovery of older files.
		f, err = os.OpenFile(b.path+".new", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	}

	if err != nil {
		return err
	}

	b.file = f

	ok := false
	defer func() {
		if !ok {
			_ = b.close()
		}
	}()

	st, err := f.Stat()
	if err != nil {
		return err
	}

	if st.Size() == 0 {
		if !fresh {
			return errors.New("empty durability header")
		}

		block, err := allocationBlock(f)
		if err != nil {
			return err
		}

		b.block = max(uint64(os.Getpagesize()), block)
		if err := f.Truncate(int64(2 * b.block)); err != nil {
			return err
		}

		b.data, err = mapFile(f, 2*b.block)
		if err != nil {
			return err
		}

		copy(b.data, "SWYTCH01")
		binary.LittleEndian.PutUint64(b.data[8:], formatVersion)
		binary.LittleEndian.PutUint64(b.data[16:], b.block)
		binary.LittleEndian.PutUint64(b.data[24:], b.id.node)
		binary.LittleEndian.PutUint64(b.data[32:], b.id.bucket)
		binary.LittleEndian.PutUint64(b.data[40:], fileCount)
		// TOC atomics are native endian; reject a different architecture on open.
		save(b.data, 48, 0x0102030405060708)

		if err := syncMapped(f, b.data); err != nil {
			return err
		}
		// Windows cannot rename a mapped file. No record is published yet, so
		// close the initialized temporary file before publishing its name.
		if err := b.close(); err != nil {
			return err
		}

		if err := os.Rename(b.path+".new", b.path); err != nil {
			return err
		}

		f, err = os.OpenFile(b.path, os.O_RDWR, 0o600)
		if err != nil {
			return err
		}

		b.file = f

		b.data, err = mapFile(f, 2*b.block)
		if err != nil {
			return err
		}

		if err := syncDirectory(filepath.Dir(b.path)); err != nil {
			return err
		}

		if err := syncDirectory(filepath.Dir(filepath.Dir(b.path))); err != nil {
			return err
		}
	} else {
		if st.Size() < 4096 {
			return errors.New("truncated durability header")
		}

		b.data, err = mapFile(f, uint64(st.Size()))
		if err != nil {
			return err
		}

		if string(b.data[:8]) != "SWYTCH01" || binary.LittleEndian.Uint64(b.data[8:]) != formatVersion || load(b.data, 48) != 0x0102030405060708 {
			return errors.New("unsupported durability format")
		}

		b.block = binary.LittleEndian.Uint64(b.data[16:])
		if b.block < 4096 || b.block > 1<<20 || b.block%uint64(os.Getpagesize()) != 0 || uint64(len(b.data)) < 2*b.block {
			return errors.New("invalid allocation boundary")
		}

		if binary.LittleEndian.Uint64(b.data[24:]) != b.id.node || binary.LittleEndian.Uint64(b.data[32:]) != b.id.bucket || binary.LittleEndian.Uint64(b.data[40:]) != fileCount {
			return errors.New("durability identity mismatch")
		}
	}

	b.entries = make(map[uint64]uint64)
	b.free = nil
	b.pages = nil

	b.cursor = 2 * b.block
	for page := b.block; page != 0; {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}

		if page%b.block != 0 || page > uint64(len(b.data))-b.block || (len(b.pages) > 0 && page <= b.lastPage) {
			return errors.New("invalid TOC page")
		}

		b.pages = append(b.pages, page)
		b.lastPage = page

		b.cursor = max(b.cursor, page+b.block)
		for pos := page + entrySize; pos < page+b.block; pos += entrySize {
			state := load(b.data, pos)
			if state != published {
				b.free = append(b.free, pos)

				continue
			}

			offset, p, n, allocated := load(b.data, pos+8), load(b.data, pos+16), load(b.data, pos+24), load(b.data, pos+32)
			if offset%fileCount != b.id.bucket || p%b.block != 0 || allocated == 0 || allocated%b.block != 0 || n > allocated || p > uint64(len(b.data)) || allocated > uint64(len(b.data))-p {
				return errors.New("invalid published TOC entry")
			}

			if _, exists := b.entries[offset]; exists {
				return errors.New("duplicate published effect")
			}

			b.entries[offset] = pos
			b.cursor = max(b.cursor, p+allocated)
		}

		page = load(b.data, page)
	}
	// Never reuse space that may contain a record interrupted before publication.
	b.cursor = max(b.cursor, uint64(len(b.data)))
	ok = true

	return nil
}

// grow is exclusive: no reader or writer may retain the old mapping.
func (b *bucket) grow(size uint64) error {
	if size <= uint64(len(b.data)) {
		return nil
	}

	if size > uint64(^uint(0)>>1) {
		return errors.New("durability file too large")
	}
	// Fixed-size growth avoids exponential sparse-file growth across restarts.
	size = align(size, 1<<20)
	oldSize := uint64(len(b.data))
	// Unmap before resizing for Windows. The exclusive file lock has already
	// drained every reader and publisher; nobody retains an address here.
	if err := unmapFile(b.data); err != nil {
		return err
	}

	b.data = nil

	err := b.file.Truncate(int64(size))
	if err == nil {
		b.data, err = mapFile(b.file, size)
	}

	if err != nil {
		// Preserve a usable old mapping on failure. If even that fails, close
		// the handle so a later attempt reopens instead of dereferencing nil.
		var restoreErr error

		b.data, restoreErr = mapFile(b.file, oldSize)
		if restoreErr != nil {
			return errors.Join(err, restoreErr, b.close())
		}

		return err
	}

	return nil
}

func (b *bucket) dirty() {
	b.generation++
	if b.due.IsZero() {
		b.due = time.Now().Add(syncDelay)
	}
}

// Put publishes an immutable record. Repeated IDs are idempotent.
func (s *Store) Put(id ID, data []byte) error {
	s.life.RLock()
	defer s.life.RUnlock()

	if s.closed {
		return ErrClosed
	}

	stripe := &s.stripes[(id[0]^id[1])%uint64(len(s.stripes))]
	stripe.Lock()
	defer stripe.Unlock()

	b := s.get(id)
	for {
		b.life.RLock()
		b.mu.Lock()
		if b.file != nil {
			if _, ok := b.entries[id[1]]; ok {
				b.mu.Unlock()
				b.life.RUnlock()

				return nil
			}

			n := align(max(uint64(len(data)), 1), b.block)
			if len(b.free) > 0 && b.cursor+n <= uint64(len(b.data)) {
				pos := b.free[len(b.free)-1]
				b.free = b.free[:len(b.free)-1]
				p := b.cursor
				b.cursor += n
				save(b.data, pos, 0)
				b.mu.Unlock()
				copy(b.data[p:p+uint64(len(data))], data)
				save(b.data, pos+8, id[1])
				save(b.data, pos+16, p)
				save(b.data, pos+24, uint64(len(data)))
				save(b.data, pos+32, n)
				save(b.data, pos+40, uint64(recordChecksum(id[1], p, uint64(len(data)), n, data)))
				b.mu.Lock()
				save(b.data, pos, published)
				b.entries[id[1]] = pos
				b.dirty()
				b.mu.Unlock()
				b.life.RUnlock()
				s.signal()

				return nil
			}
		}
		b.mu.Unlock()
		b.life.RUnlock()
		b.life.Lock()
		err := func() error {
			if b.file == nil {
				if err := b.open(context.Background(), true); err != nil {
					return err
				}
			}

			n := align(max(uint64(len(data)), 1), b.block)

			extra := uint64(0)
			if len(b.free) == 0 {
				extra = b.block
			}

			if err := b.grow(b.cursor + extra + n); err != nil {
				return err
			}

			if extra != 0 {
				page := b.cursor
				b.cursor += b.block
				clear(b.data[page : page+b.block])
				save(b.data, b.lastPage, page)
				b.lastPage = page

				b.pages = append(b.pages, page)
				for pos := page + entrySize; pos < page+b.block; pos += entrySize {
					b.free = append(b.free, pos)
				}
			}

			return nil
		}()
		b.life.Unlock()

		if err != nil {
			return err
		}
	}
}

// Delete removes the TOC entry immediately. Its allocation is punched only
// after the tombstone is synced, so a crash cannot resurrect punched data.
func (s *Store) Delete(id ID) error {
	s.life.RLock()
	defer s.life.RUnlock()

	if s.closed {
		return ErrClosed
	}

	stripe := &s.stripes[(id[0]^id[1])%uint64(len(s.stripes))]
	stripe.Lock()
	defer stripe.Unlock()

	b := s.get(id)
	b.life.RLock()
	b.mu.Lock()

	pos, ok := b.entries[id[1]]
	if ok {
		save(b.data, pos, deleted)
		b.punches = append(b.punches, extent{load(b.data, pos+16), load(b.data, pos+32)})
		delete(b.entries, id[1])
		b.free = append(b.free, pos)
		b.dirty()
	}

	empty := len(b.entries) == 0
	b.mu.Unlock()
	b.life.RUnlock()

	if !ok {
		return nil
	}

	s.signal()

	if empty {
		b.life.Lock()
		defer b.life.Unlock()

		if len(b.entries) == 0 && b.file != nil {
			return b.remove()
		}
	}

	return nil
}

func (b *bucket) remove() error {
	if err := syncMapped(b.file, b.data); err != nil {
		return err
	}

	if err := b.close(); err != nil {
		return err
	}

	if err := os.Remove(b.path); err != nil && !os.IsNotExist(err) {
		return err
	}

	b.entries = nil
	b.free = nil
	b.pages = nil
	b.punches = nil
	b.due = time.Time{}
	b.generation = 0
	b.synced = 0

	return syncDirectory(filepath.Dir(b.path))
}

func (s *Store) buckets() []*bucket {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := make([]*bucket, 0, len(s.files))
	for _, b := range s.files {
		result = append(result, b)
	}

	return result
}

// Recover parallelizes TOC pages, including pages within one file. The callback
// runs under the file's read lock; it must finish using data before returning.
// Recovery excludes writes and eviction. Callbacks must not call store methods.
func (s *Store) Recover(ctx context.Context, workers int, fn func(ID, []byte) error) error {
	s.life.Lock()
	defer s.life.Unlock()

	if s.closed {
		return ErrClosed
	}

	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	type job struct {
		b    *bucket
		page uint64
	}

	jobs := make(chan job, workers)

	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for j := range jobs {
				if ctx.Err() != nil {
					continue
				}

				j.b.life.RLock()

				err := func() error {
					b := j.b
					if b.file == nil {
						return nil
					}

					for pos := j.page + entrySize; pos < j.page+b.block; pos += entrySize {
						if err := ctx.Err(); err != nil {
							return err
						}

						b.mu.Lock()

						state := load(b.data, pos)
						if state != published {
							b.mu.Unlock()

							continue
						}

						id := ID{b.id.node, load(b.data, pos+8)}
						p, n, allocated, sum := load(b.data, pos+16), load(b.data, pos+24), load(b.data, pos+32), load(b.data, pos+40)
						b.mu.Unlock()

						data := b.data[p : p+n]
						if uint64(recordChecksum(id[1], p, n, allocated, data)) != sum {
							return fmt.Errorf("corrupt effect %v in %s", id, b.path)
						}

						if err := fn(id, data); err != nil {
							return err
						}
					}

					return nil
				}()

				j.b.life.RUnlock()

				if err != nil {
					cancel(err)
				}
			}
		})
	}

	for _, b := range s.buckets() {
		b.life.RLock()
		pages := append([]uint64(nil), b.pages...)
		b.life.RUnlock()

		for _, page := range pages {
			select {
			case jobs <- job{b, page}:
			case <-ctx.Done():
			}

			if ctx.Err() != nil {
				break
			}
		}

		if ctx.Err() != nil {
			break
		}
	}

	close(jobs)
	wg.Wait()

	return context.Cause(ctx)
}

func (s *Store) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (b *bucket) sync(force bool) error {
	b.syncMu.Lock()
	defer b.syncMu.Unlock()

	b.life.RLock()
	b.mu.Lock()
	if b.file == nil || b.generation == b.synced || (!force && time.Now().Before(b.due)) {
		b.mu.Unlock()
		b.life.RUnlock()

		return nil
	}

	file, gen, due, punches := b.file, b.generation, b.due, b.punches
	b.punches = nil
	b.due = time.Time{}
	b.mu.Unlock()
	err := syncMapped(file, b.data)
	b.mu.Lock()
	if err != nil {
		b.punches = append(b.punches, punches...)
		if b.due.IsZero() || due.Before(b.due) {
			b.due = due
		}
		b.mu.Unlock()
		b.life.RUnlock()

		return err
	}

	b.synced = gen
	b.mu.Unlock()
	b.life.RUnlock()

	if len(punches) == 0 {
		return nil
	}
	// Readers must finish before punching. If an empty bucket was deleted and
	// recreated while upgrading the lock, its new file must remain untouched.
	b.life.Lock()
	defer b.life.Unlock()

	if b.file != file {
		return nil
	}

	for i, p := range punches {
		if err := punchFile(file, p.pos, p.size); err != nil {
			b.punches = append(b.punches, punches[i:]...)
			b.dirty()

			return err
		}
	}

	return nil
}

func (s *Store) syncLoop() {
	defer close(s.done)

	for {
		delay := time.Hour

		for _, b := range s.buckets() {
			b.life.RLock()
			b.mu.Lock()
			if !b.due.IsZero() && !b.syncing.Load() {
				delay = min(delay, max(time.Until(b.due), time.Millisecond))
				if !time.Now().Before(b.due) && b.syncing.CompareAndSwap(false, true) {
					s.syncWG.Go(func() {
						defer func() { b.syncing.Store(false); s.signal() }()

						if err := b.sync(false); err != nil {
							slog.Error("durability sync failed", "path", b.path, "error", err)
						}
					})
				}
			}
			b.mu.Unlock()
			b.life.RUnlock()
		}

		timer := time.NewTimer(delay)
		select {
		case <-s.stop:
			timer.Stop()

			return
		case <-s.wake:
			timer.Stop()
		case <-timer.C:
		}
	}
}

// Sync waits for all currently published records to reach storage.
func (s *Store) Sync() error {
	s.life.RLock()
	defer s.life.RUnlock()

	if s.closed {
		return ErrClosed
	}

	var errs []error
	for _, b := range s.buckets() {
		errs = append(errs, b.sync(true))
	}

	return errors.Join(errs...)
}

// Reset deletes only this store's known mirror files. Caller must drain recovery first.
func (s *Store) Reset() error {
	s.life.Lock()
	defer s.life.Unlock()

	if s.closed {
		return ErrClosed
	}

	var errs []error

	for _, b := range s.buckets() {
		b.life.Lock()
		if b.file != nil {
			errs = append(errs, b.remove())
		}
		b.life.Unlock()
	}

	return errors.Join(errs...)
}

func (b *bucket) close() error {
	var errs []error
	if b.data != nil {
		errs = append(errs, unmapFile(b.data))
		b.data = nil
	}

	if b.file != nil {
		errs = append(errs, b.file.Close())
		b.file = nil
	}

	return errors.Join(errs...)
}

func (s *Store) Close() error {
	s.life.Lock()
	defer s.life.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true
	close(s.stop)
	<-s.done
	s.syncWG.Wait()

	var errs []error
	for _, b := range s.buckets() {
		errs = append(errs, b.sync(true))
		b.life.Lock()
		errs = append(errs, b.close())
		b.life.Unlock()
	}

	errs = append(errs, s.lock.Close())

	return errors.Join(errs...)
}
