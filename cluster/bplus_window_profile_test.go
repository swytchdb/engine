package cluster

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"
)

// Complete profile windows remain readable while a long Flush is still running.
// Heap/alloc profiles do not force GC. Contention profiles are cumulative.
func startBplusWindowProfiles(tb testing.TB, dir string) func() {
	if os.Getenv("BPLUS_PROFILE_WINDOWS") == "" {
		return nil
	}

	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)

		oldMutexRate := runtime.SetMutexProfileFraction(10)

		runtime.SetBlockProfileRate(1000000)

		defer runtime.SetMutexProfileFraction(oldMutexRate)
		defer runtime.SetBlockProfileRate(0)

		for window := 1; ; window++ {
			prefix := filepath.Join(dir, fmt.Sprintf("window-%03d", window))

			f, err := os.Create(prefix + "-cpu.pprof")
			if err != nil {
				tb.Error(err)

				return
			}

			if err := pprof.StartCPUProfile(f); err != nil {
				tb.Error(err)

				if err := f.Close(); err != nil {
					tb.Error(err)
				}

				return
			}

			timer := time.NewTimer(30 * time.Second)
			stopped := false

			select {
			case <-stop:
				stopped = true
			case <-timer.C:
			}

			timer.Stop()
			pprof.StopCPUProfile()

			if err := f.Close(); err != nil {
				tb.Error(err)

				return
			}

			for _, kind := range []string{"goroutine", "heap", "allocs", "mutex", "block"} {
				f, err := os.Create(prefix + "-" + kind + ".pprof")
				if err != nil {
					tb.Error(err)

					return
				}

				err = pprof.Lookup(kind).WriteTo(f, 0)

				closeErr := f.Close()
				if err != nil || closeErr != nil {
					tb.Errorf("write %s profile: %v; close: %v", kind, err, closeErr)

					return
				}
			}

			if stopped {
				return
			}
		}
	}()

	return func() { close(stop); <-done }
}
