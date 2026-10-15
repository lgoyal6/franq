package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// ErrClosed is returned by Submit after Close.
var ErrClosed = errors.New("wal: log closed")

var segmentSync = func(f *os.File) error { return f.Sync() }

// Options tunes the log. Zero values pick sane defaults.
type Options struct {
	SegmentBytes    int64 // rotate when a segment reaches this size (default 64 MiB)
	RetainBytes     int64 // keep at least this many bytes of sealed segments for replay; 0 = keep everything
	MaxBatchRecords int   // group-commit batch cap (default 256)
	MaxBatchBytes   int64 // group-commit byte cap (default 1 MiB)
}

func (o *Options) defaults() {
	if o.SegmentBytes <= 0 {
		o.SegmentBytes = 64 << 20
	}
	if o.MaxBatchRecords <= 0 {
		o.MaxBatchRecords = 256
	}
	if o.MaxBatchBytes <= 0 {
		o.MaxBatchBytes = 1 << 20
	}
}

// Req is one submission: pre-encoded frames plus the state mutation to run
// after those frames are durable. The applier runs Apply in log order.
type Req struct {
	frames []byte
	apply  func()
	done   chan error
}

func (r *Req) Apply() {
	if r.apply != nil {
		r.apply()
	}
}

// Wait blocks until the frames are fsynced AND applied, or the log poisons.
func (r *Req) Wait() error { return <-r.done }

// Finish releases the submitter after Apply has run. Called by the applier.
func (r *Req) Finish(err error) { r.done <- err }

func (r *Req) release(err error) { r.done <- err }

// Batch is what one fsync covered. Sealed is nonzero when the active segment
// was sealed immediately after this batch - the applier uses that as its
// consistent snapshot point (state == exactly end of segment Sealed).
type Batch struct {
	Reqs   []*Req
	Sealed uint64
}

// Log is the group-commit append pipeline. Handlers Submit concurrently; one
// committer goroutine batches them into a single write+fsync; batches are
// handed to the consumer of Batches() (the applier) in log order.
type Log struct {
	dir  string
	opts Options

	mu       sync.RWMutex // guards closed vs concurrent Submit sends
	closed   bool
	commitCh chan *Req
	batchCh  chan Batch

	seg     *os.File
	segID   uint64
	segSize int64

	stateMu   sync.Mutex // (activeSeg, committedLen) read by View, written post-fsync
	activeSeg uint64
	activeLen int64

	retention sync.Mutex // serializes segment deletion vs replay scans
	buf       []byte
}

// OpenLog creates segment nextSeg and starts the committer. Recovery must be
// finished first: franq always starts appends in a brand-new segment, which
// removes every reopen-after-truncation edge case.
func OpenLog(dir string, nextSeg uint64, opts Options) (*Log, error) {
	opts.defaults()
	f, err := createSegment(dir, nextSeg)
	if err != nil {
		return nil, err
	}
	l := &Log{
		dir:       dir,
		opts:      opts,
		commitCh:  make(chan *Req, 4096),
		batchCh:   make(chan Batch, 64),
		seg:       f,
		segID:     nextSeg,
		segSize:   segHeaderLen,
		activeSeg: nextSeg,
		activeLen: segHeaderLen,
	}
	go l.run()
	return l, nil
}

// Submit queues frames for durability. The returned Req's Wait unblocks after
// fsync + apply. frames may contain multiple records; they commit atomically
// within one write (ordering within the batch is preserved).
func (l *Log) Submit(frames []byte, apply func()) (*Req, error) {
	r := &Req{frames: frames, apply: apply, done: make(chan error, 1)}
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.closed {
		return nil, ErrClosed
	}
	l.commitCh <- r
	return r, nil
}

// Batches delivers committed batches in log order. Closed after Close once
// everything in flight has been flushed and delivered.
func (l *Log) Batches() <-chan Batch { return l.batchCh }

// Close stops intake and flushes. Callers must then drain Batches().
func (l *Log) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return
	}
	l.closed = true
	close(l.commitCh)
}

func (l *Log) run() {
	for first := range l.commitCh {
		reqs := []*Req{first}
		size := int64(len(first.frames))
	drain:
		for len(reqs) < l.opts.MaxBatchRecords && size < l.opts.MaxBatchBytes {
			select {
			case r, ok := <-l.commitCh:
				if !ok {
					break drain
				}
				reqs = append(reqs, r)
				size += int64(len(r.frames))
			default:
				break drain
			}
		}
		l.buf = l.buf[:0]
		for _, r := range reqs {
			l.buf = append(l.buf, r.frames...)
		}
		if _, err := l.seg.Write(l.buf); err != nil {
			l.poison(reqs, err)
		}
		if err := segmentSync(l.seg); err != nil {
			l.poison(reqs, err)
		}
		l.segSize += int64(len(l.buf))
		failpoint("after-commit")

		var sealed uint64
		if l.segSize >= l.opts.SegmentBytes {
			sealed = l.segID
			if err := l.rotate(); err != nil {
				l.poison(reqs, err)
			}
			failpoint("after-rotate")
		}
		l.stateMu.Lock()
		l.activeSeg, l.activeLen = l.segID, l.segSize
		l.stateMu.Unlock()

		l.batchCh <- Batch{Reqs: reqs, Sealed: sealed}
	}
	l.seg.Sync()
	l.seg.Close()
	close(l.batchCh)
}

func (l *Log) rotate() error {
	if err := l.seg.Close(); err != nil {
		return err
	}
	f, err := createSegment(l.dir, l.segID+1)
	if err != nil {
		return err
	}
	l.seg = f
	l.segID++
	l.segSize = segHeaderLen
	return nil
}

// poison handles a failed write or fsync. After a failed fsync the page cache
// is in an unknowable state (retrying would silently lie about durability),
// so franq fails every waiter and dies; recovery re-reads the truth from disk.
func (l *Log) poison(reqs []*Req, err error) {
	for _, r := range reqs {
		r.release(err)
	}
	fmt.Fprintf(os.Stderr, "franq: wal write failed, refusing to continue: %v\n", err)
	os.Exit(1)
}

// View pins the segment set for a replay scan: sealed segment paths in order
// plus the active segment path and its committed (fsynced) length. Segment
// deletion is blocked until release is called.
func (l *Log) View() (sealed []string, active string, activeLen int64, release func()) {
	l.retention.Lock()
	l.stateMu.Lock()
	seg, ln := l.activeSeg, l.activeLen
	l.stateMu.Unlock()
	ids, _ := listSegments(l.dir)
	for _, id := range ids {
		if id < seg {
			sealed = append(sealed, filepath.Join(l.dir, segmentName(id)))
		}
	}
	return sealed, filepath.Join(l.dir, segmentName(seg)), ln, l.retention.Unlock
}

// DeleteSealedUpTo removes sealed segments <= maxSeg (oldest first) while the
// total sealed bytes exceed RetainBytes. RetainBytes == 0 keeps everything -
// retention is the time-travel replay window, so the default is maximal.
func (l *Log) DeleteSealedUpTo(maxSeg uint64) {
	if l.opts.RetainBytes == 0 {
		return
	}
	l.retention.Lock()
	defer l.retention.Unlock()
	ids, err := listSegments(l.dir)
	if err != nil {
		return
	}
	l.stateMu.Lock()
	active := l.activeSeg
	l.stateMu.Unlock()

	type segInfo struct {
		id   uint64
		size int64
	}
	var sealedSegs []segInfo
	var total int64
	for _, id := range ids {
		if id >= active {
			continue
		}
		fi, err := os.Stat(filepath.Join(l.dir, segmentName(id)))
		if err != nil {
			continue
		}
		sealedSegs = append(sealedSegs, segInfo{id, fi.Size()})
		total += fi.Size()
	}
	for _, s := range sealedSegs {
		if total <= l.opts.RetainBytes || s.id > maxSeg {
			return
		}
		if err := os.Remove(filepath.Join(l.dir, segmentName(s.id))); err != nil {
			return
		}
		total -= s.size
	}
}
