package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func newDirs(t testing.TB) (walDir, snapDir string) {
	t.Helper()
	base := t.TempDir()
	walDir = filepath.Join(base, "wal")
	snapDir = filepath.Join(base, "snap")
	for _, d := range []string{walDir, snapDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return walDir, snapDir
}

// drainApplier consumes batches like the broker's applier does.
func drainApplier(l *Log, wg *sync.WaitGroup) {
	defer wg.Done()
	for b := range l.Batches() {
		for _, r := range b.Reqs {
			r.Apply()
			r.release(nil)
		}
	}
}

func TestLogCommitRecoverRoundtrip(t *testing.T) {
	walDir, snapDir := newDirs(t)
	l, err := OpenLog(walDir, 1, Options{SegmentBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go drainApplier(l, &wg)

	const n = 500
	var submitWg sync.WaitGroup
	applied := make([]bool, n+1)
	var mu sync.Mutex
	for i := 1; i <= n; i++ {
		submitWg.Add(1)
		go func(id int) {
			defer submitWg.Done()
			frames := AppendEnqueue(nil, &Enqueue{QueueID: 1, MsgID: uint64(id), EnqueuedAt: int64(id), Body: []byte(fmt.Sprintf("msg-%d", id))})
			req, err := l.Submit(frames, func() {
				mu.Lock()
				applied[id] = true
				mu.Unlock()
			})
			if err != nil {
				t.Error(err)
				return
			}
			if err := req.Wait(); err != nil {
				t.Error(err)
			}
		}(i)
	}
	submitWg.Wait()
	l.Close()
	wg.Wait()

	for i := 1; i <= n; i++ {
		if !applied[i] {
			t.Fatalf("msg %d committed but never applied", i)
		}
	}

	// Multiple segments must exist (SegmentBytes was tiny).
	ids, _ := listSegments(walDir)
	if len(ids) < 2 {
		t.Fatalf("expected rotation, got %d segments", len(ids))
	}

	// Recovery must see all n records, in order, exactly once.
	seen := make(map[uint64]bool)
	var last uint64
	res, err := Recover(walDir, snapDir, func(*Snapshot) error { return nil }, func(rec any) error {
		e, ok := rec.(*Enqueue)
		if !ok {
			t.Fatalf("unexpected record %T", rec)
		}
		if seen[e.MsgID] {
			t.Fatalf("msg %d replayed twice", e.MsgID)
		}
		seen[e.MsgID] = true
		last = e.MsgID
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Records != n || len(seen) != n {
		t.Fatalf("recovered %d records (%d unique), want %d", res.Records, len(seen), n)
	}
	_ = last
}

func TestSubmitAfterClose(t *testing.T) {
	walDir, _ := newDirs(t)
	l, err := OpenLog(walDir, 1, Options{})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go drainApplier(l, &wg)
	l.Close()
	wg.Wait()
	if _, err := l.Submit([]byte{1}, nil); err != ErrClosed {
		t.Fatalf("got %v, want ErrClosed", err)
	}
}

func TestSnapshotRoundtrip(t *testing.T) {
	s := &Snapshot{
		LastSeg:     42,
		NextMsgID:   1001,
		NextQueueID: 7,
		Queues: []SnapQueue{
			{ID: 1, Name: "orders", Cfg: []byte(`{"order":"fifo"}`), Msgs: []SnapMsg{
				{ID: 5, OriginID: 2, Priority: 9, EnqueuedAt: 111, VisibleAt: 222, Deliveries: 3, Body: []byte("hello")},
				{ID: 6, Priority: 0, EnqueuedAt: 112},
			}},
			{ID: 2, Name: "orders.dlq", Cfg: []byte(`{"order":"fifo"}`)},
		},
	}
	got, err := DecodeSnapshot(EncodeSnapshot(s))
	if err != nil {
		t.Fatal(err)
	}
	if got.LastSeg != 42 || got.NextMsgID != 1001 || got.NextQueueID != 7 || len(got.Queues) != 2 {
		t.Fatalf("header mismatch: %+v", got)
	}
	if got.Queues[0].Msgs[0].OriginID != 2 || string(got.Queues[0].Msgs[0].Body) != "hello" {
		t.Fatalf("message mismatch: %+v", got.Queues[0].Msgs[0])
	}
}

func TestSnapshotWriteLoadPrune(t *testing.T) {
	_, snapDir := newDirs(t)
	if err := WriteSnapshot(snapDir, &Snapshot{LastSeg: 3, NextMsgID: 10, NextQueueID: 2}); err != nil {
		t.Fatal(err)
	}
	if err := WriteSnapshot(snapDir, &Snapshot{LastSeg: 9, NextMsgID: 20, NextQueueID: 2}); err != nil {
		t.Fatal(err)
	}
	s, err := LoadLatestSnapshot(snapDir)
	if err != nil {
		t.Fatal(err)
	}
	if s == nil || s.LastSeg != 9 {
		t.Fatalf("loaded %+v, want LastSeg 9", s)
	}
	entries, _ := os.ReadDir(snapDir)
	count := 0
	for _, e := range entries {
		if _, ok := parseSnapshotName(e.Name()); ok {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("old snapshot not pruned: %d snapshots on disk", count)
	}

	// A corrupt newest snapshot must fall back to an older valid one.
	if err := WriteSnapshot(snapDir, &Snapshot{LastSeg: 15, NextMsgID: 30, NextQueueID: 2}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(snapDir, snapshotName(15))
	buf, _ := os.ReadFile(path)
	buf[len(buf)-1] ^= 0xFF
	os.WriteFile(path, buf, 0o644)
	// 9 was pruned when 15 was written, so write a fresh older one to fall back to.
	if err := WriteSnapshot(snapDir, &Snapshot{LastSeg: 12, NextMsgID: 25, NextQueueID: 2}); err != nil {
		t.Fatal(err)
	}
	// Re-corrupt check: newest valid must now be 12 (15 is corrupt).
	s, err = LoadLatestSnapshot(snapDir)
	if err != nil {
		t.Fatal(err)
	}
	if s == nil || s.LastSeg != 12 {
		t.Fatalf("fallback loaded %+v, want LastSeg 12", s)
	}
}

func TestSnapshotStorageFailuresKeepPreviousSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  func(error)
		err  error
	}{
		{name: "write-enospc", err: syscall.ENOSPC, set: func(err error) {
			snapshotWrite = func(*os.File, []byte) (int, error) { return 0, err }
		}},
		{name: "short-write", err: io.ErrShortWrite, set: func(error) {
			snapshotWrite = func(*os.File, []byte) (int, error) { return 1, nil }
		}},
		{name: "file-fsync", err: errors.New("injected fsync error"), set: func(err error) {
			snapshotSync = func(*os.File) error { return err }
		}},
		{name: "rename", err: errors.New("injected rename error"), set: func(err error) {
			snapshotRename = func(string, string) error { return err }
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, snapDir := newDirs(t)
			if err := WriteSnapshot(snapDir, &Snapshot{LastSeg: 3, NextMsgID: 10}); err != nil {
				t.Fatal(err)
			}
			oldWrite, oldSync, oldRename := snapshotWrite, snapshotSync, snapshotRename
			t.Cleanup(func() { snapshotWrite, snapshotSync, snapshotRename = oldWrite, oldSync, oldRename })
			tc.set(tc.err)
			if err := WriteSnapshot(snapDir, &Snapshot{LastSeg: 9, NextMsgID: 20}); !errors.Is(err, tc.err) {
				t.Fatalf("WriteSnapshot error = %v, want %v", err, tc.err)
			}
			snapshotWrite, snapshotSync, snapshotRename = oldWrite, oldSync, oldRename
			got, err := LoadLatestSnapshot(snapDir)
			if err != nil {
				t.Fatal(err)
			}
			if got == nil || got.LastSeg != 3 || got.NextMsgID != 10 {
				t.Fatalf("previous snapshot not retained: %+v", got)
			}
		})
	}
}

func TestSnapshotWriteFullLengthWithNilError(t *testing.T) {
	_, snapDir := newDirs(t)
	oldWrite := snapshotWrite
	t.Cleanup(func() { snapshotWrite = oldWrite })
	snapshotWrite = func(f *os.File, data []byte) (int, error) {
		return f.Write(data)
	}
	want := &Snapshot{LastSeg: 7, NextMsgID: 12}
	if err := WriteSnapshot(snapDir, want); err != nil {
		t.Fatal(err)
	}
	got, err := LoadLatestSnapshot(snapDir)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.LastSeg != want.LastSeg || got.NextMsgID != want.NextMsgID {
		t.Fatalf("loaded snapshot = %+v, want %+v", got, want)
	}
}

func TestAbandonedWaitDoesNotCancelAcceptedCommit(t *testing.T) {
	walDir, snapDir := newDirs(t)
	enteredSync := make(chan struct{})
	releaseSync := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(releaseSync) }) }
	oldSync := segmentSync
	segmentSync = func(f *os.File) error {
		close(enteredSync)
		<-releaseSync
		return f.Sync()
	}
	t.Cleanup(func() {
		segmentSync = oldSync
		unblock()
	})

	l, err := OpenLog(walDir, 1, Options{})
	if err != nil {
		t.Fatal(err)
	}
	var applyWG sync.WaitGroup
	applyWG.Add(1)
	go func() {
		defer applyWG.Done()
		for batch := range l.Batches() {
			for _, req := range batch.Reqs {
				req.Apply()
				req.Finish(nil)
			}
		}
	}()

	var applied atomic.Bool
	req, err := l.Submit(AppendEnqueue(nil, &Enqueue{QueueID: 1, MsgID: 7, Body: []byte("accepted before disconnect")}), func() {
		applied.Store(true)
	})
	if err != nil {
		t.Fatal(err)
	}
	<-enteredSync
	handlerDone := make(chan error, 1)
	go func() { handlerDone <- req.Wait() }()

	// Model the client disconnect by leaving the handler's response unread
	// while fsync is blocked. The accepted WAL request remains committer-owned.
	select {
	case err := <-handlerDone:
		t.Fatalf("handler completed before blocked fsync: %v", err)
	default:
	}
	if applied.Load() {
		t.Fatal("request was applied before fsync completed")
	}

	unblock()
	segmentSync = oldSync
	if err := <-handlerDone; err != nil {
		t.Fatal(err)
	}
	if !applied.Load() {
		t.Fatal("accepted request was not applied after fsync")
	}
	l.Close()
	applyWG.Wait()
	got, err := Recover(walDir, snapDir, func(*Snapshot) error { return nil }, func(rec any) error {
		if e, ok := rec.(*Enqueue); !ok || e.MsgID != 7 {
			t.Fatalf("recovered record = %#v, want enqueue 7", rec)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Records != 1 {
		t.Fatalf("recovered records = %d, want 1", got.Records)
	}
}

func TestRecoverTruncatesTornTail(t *testing.T) {
	walDir, snapDir := newDirs(t)
	l, err := OpenLog(walDir, 1, Options{})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go drainApplier(l, &wg)
	for i := 1; i <= 3; i++ {
		req, _ := l.Submit(AppendEnqueue(nil, &Enqueue{QueueID: 1, MsgID: uint64(i), Body: []byte("x")}), nil)
		req.Wait()
	}
	l.Close()
	wg.Wait()

	// Append garbage to simulate a torn write.
	path := filepath.Join(walDir, segmentName(1))
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	f.Write([]byte{9, 0, 0, 0, 1, 2, 3, 4, 5}) // claims 9-byte payload, delivers 1
	f.Close()

	count := 0
	res, err := Recover(walDir, snapDir, func(*Snapshot) error { return nil }, func(any) error { count++; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 || res.TornTails != 1 {
		t.Fatalf("recovered %d records, %d torn tails; want 3 and 1", count, res.TornTails)
	}
	// Second recovery must be clean - the tail was physically truncated.
	count = 0
	res, err = Recover(walDir, snapDir, func(*Snapshot) error { return nil }, func(any) error { count++; return nil })
	if err != nil || count != 3 || res.TornTails != 0 {
		t.Fatalf("second recovery: err=%v count=%d torn=%d", err, count, res.TornTails)
	}
}

// BenchmarkCommitSerial measures fsync-per-record throughput: one submitter,
// so every record pays a full fsync.
func BenchmarkCommitSerial(b *testing.B) {
	walDir, _ := newDirs(b)
	l, err := OpenLog(walDir, 1, Options{})
	if err != nil {
		b.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go drainApplier(l, &wg)
	frames := AppendEnqueue(nil, &Enqueue{QueueID: 1, MsgID: 1, Body: make([]byte, 128)})
	b.ResetTimer()
	for range b.N {
		req, err := l.Submit(frames, nil)
		if err != nil {
			b.Fatal(err)
		}
		req.Wait()
	}
	b.StopTimer()
	l.Close()
	wg.Wait()
}

// BenchmarkCommitGroup64 measures group commit: 64 concurrent submitters
// share fsyncs, so per-record cost collapses.
func BenchmarkCommitGroup64(b *testing.B) {
	walDir, _ := newDirs(b)
	l, err := OpenLog(walDir, 1, Options{})
	if err != nil {
		b.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go drainApplier(l, &wg)
	frames := AppendEnqueue(nil, &Enqueue{QueueID: 1, MsgID: 1, Body: make([]byte, 128)})
	b.SetParallelism(64)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			req, err := l.Submit(frames, nil)
			if err != nil {
				b.Fatal(err)
			}
			req.Wait()
		}
	})
	b.StopTimer()
	l.Close()
	wg.Wait()
}

// BenchmarkCommitMatrix compares fsync-per-request with bounded group commit
// under the same 128-byte append workload and local filesystem.
func BenchmarkCommitMatrix(b *testing.B) {
	for _, batchCap := range []int{1, 16, 64} {
		for _, producers := range []int{1, 8, 64} {
			b.Run(fmt.Sprintf("batch_cap=%d/producers=%d", batchCap, producers), func(b *testing.B) {
				walDir, _ := newDirs(b)
				l, err := OpenLog(walDir, 1, Options{MaxBatchRecords: batchCap})
				if err != nil {
					b.Fatal(err)
				}
				var applyWG sync.WaitGroup
				applyWG.Add(1)
				go drainApplier(l, &applyWG)
				frames := AppendEnqueue(nil, &Enqueue{QueueID: 1, MsgID: 1, Body: make([]byte, 128)})
				b.SetBytes(int64(len(frames)))
				b.ReportAllocs()
				b.ResetTimer()
				var next atomic.Uint64
				var producersWG sync.WaitGroup
				producersWG.Add(producers)
				for range producers {
					go func() {
						defer producersWG.Done()
						for {
							if next.Add(1) > uint64(b.N) {
								return
							}
							req, err := l.Submit(frames, nil)
							if err != nil {
								b.Error(err)
								return
							}
							if err := req.Wait(); err != nil {
								b.Error(err)
								return
							}
						}
					}()
				}
				producersWG.Wait()
				b.StopTimer()
				l.Close()
				applyWG.Wait()
				b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "records/s")
			})
		}
	}
}

func TestCommitLatencyMatrix(t *testing.T) {
	if os.Getenv("B7_LATENCY_STUDY") != "1" {
		t.Skip("set B7_LATENCY_STUDY=1 to run the serialized latency study")
	}
	for _, cap := range []int{1, 16, 64} {
		for _, producers := range []int{1, 8, 64} {
			for repetition := 1; repetition <= 3; repetition++ {
				t.Run(fmt.Sprintf("batch_cap=%d/producers=%d/repetition=%d", cap, producers, repetition), func(t *testing.T) {
					latencies := runCommitLatencyTrial(t, cap, producers, 2*time.Second)
					sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
					if len(latencies) == 0 {
						t.Fatal("latency trial recorded no committed requests")
					}
					quantile := func(q float64) float64 {
						index := int(q * float64(len(latencies)-1))
						return float64(latencies[index]) / float64(time.Millisecond)
					}
					t.Logf("B7_LATENCY_RESULT cap=%d producers=%d repetition=%d requests=%d p50_ms=%.3f p95_ms=%.3f p99_ms=%.3f", cap, producers, repetition, len(latencies), quantile(.50), quantile(.95), quantile(.99))
				})
			}
		}
	}
}

func runCommitLatencyTrial(t *testing.T, batchCap, producers int, duration time.Duration) []time.Duration {
	t.Helper()
	walDir, _ := newDirs(t)
	l, err := OpenLog(walDir, 1, Options{MaxBatchRecords: batchCap})
	if err != nil {
		t.Fatal(err)
	}
	var applier sync.WaitGroup
	applier.Add(1)
	go func() {
		defer applier.Done()
		for batch := range l.Batches() {
			for _, req := range batch.Reqs {
				req.Apply()
				req.Finish(nil)
			}
		}
	}()
	start := make(chan struct{})
	stop := make(chan struct{})
	perProducer := make([][]time.Duration, producers)
	var workers sync.WaitGroup
	var nextID atomic.Uint64
	workers.Add(producers)
	for i := 0; i < producers; i++ {
		go func(i int) {
			defer workers.Done()
			<-start
			for {
				select {
				case <-stop:
					return
				default:
				}
				began := time.Now()
				req, err := l.Submit(AppendEnqueue(nil, &Enqueue{QueueID: 1, MsgID: nextID.Add(1), Body: []byte("latency")}), nil)
				if err != nil {
					t.Errorf("Submit: %v", err)
					return
				}
				if err := req.Wait(); err != nil {
					t.Errorf("Wait: %v", err)
					return
				}
				perProducer[i] = append(perProducer[i], time.Since(began))
			}
		}(i)
	}
	close(start)
	time.Sleep(duration)
	close(stop)
	workers.Wait()
	l.Close()
	applier.Wait()
	var all []time.Duration
	for _, samples := range perProducer {
		all = append(all, samples...)
	}
	return all
}
