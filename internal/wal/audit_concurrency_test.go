package wal

// Added by the 2026-08-17 resume audit.
//
// The README claims "group commit is *faster* with concurrent producers" and
// backs it with exactly two points: 1 producer and BenchmarkCommitGroup64.
// Two points do not show a curve, and BenchmarkCommitGroup64 does not actually
// run 64 producers: b.SetParallelism(64) means 64*GOMAXPROCS goroutines, which
// is 704 on this host. This sweeps an EXACT producer count and measures
// completed commits in a fixed wall-clock window, so the shape of the curve is
// visible instead of asserted.
//
//	go test ./internal/wal -run TestAuditGroupCommitSweep -v -timeout 600s

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAuditGroupCommitSweep(t *testing.T) {
	if testing.Short() {
		t.Skip("sweep is slow")
	}
	const window = 2 * time.Second
	producers := []int{1, 2, 4, 8, 16, 32, 64, 128}

	for _, p := range producers {
		walDir, _ := newDirs(t)
		l, err := OpenLog(walDir, 1, Options{})
		if err != nil {
			t.Fatal(err)
		}
		var aw sync.WaitGroup
		aw.Add(1)
		go drainApplier(l, &aw)

		frames := AppendEnqueue(nil, &Enqueue{QueueID: 1, MsgID: 1, Body: make([]byte, 128)})

		var ops atomic.Int64
		stop := make(chan struct{})
		var pw sync.WaitGroup
		start := time.Now()
		for i := 0; i < p; i++ {
			pw.Add(1)
			go func() {
				defer pw.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					req, err := l.Submit(frames, nil)
					if err != nil {
						return
					}
					req.Wait()
					ops.Add(1)
				}
			}()
		}
		time.Sleep(window)
		close(stop)
		pw.Wait()
		elapsed := time.Since(start)
		l.Close()
		aw.Wait()

		n := ops.Load()
		perOp := float64(elapsed.Nanoseconds()) / float64(n)
		fmt.Printf("AUDIT_SWEEP producers=%d ops=%d elapsed_ms=%d ns_per_op=%.0f records_per_sec=%.0f\n",
			p, n, elapsed.Milliseconds(), perOp, float64(n)/elapsed.Seconds())
	}
}
