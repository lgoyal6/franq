package core

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lgoyal6/franq/internal/wal"
)

var nameRe = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$`)

// Options configures the broker's storage engine.
type Options struct {
	SegmentBytes int64 // WAL segment size before rotation (default 64 MiB)
	RetainBytes  int64 // sealed bytes kept for time-travel replay; 0 = keep everything
}

// Broker owns the queue registry and the WAL pipeline. The architecture in
// one line: handler → encode record → committer (batch + one fsync) →
// applier (single goroutine, mutates state in log order) → handler returns.
// The single applier is what makes snapshot cuts trivially consistent.
type Broker struct {
	dataDir string
	snapDir string
	log     *wal.Log

	mu     sync.RWMutex
	byName map[string]*Queue
	byID   map[uint32]*Queue

	nextMsg atomic.Uint64 // last used message id
	nextQ   atomic.Uint32 // last used queue id

	applierDone chan struct{}
	snapBusy    atomic.Bool
	snapWG      sync.WaitGroup
	closeOnce   sync.Once

	// recovery bookkeeping
	recovering bool
	maxMsgSeen uint64
	maxQSeen   uint32

	Recovery RecoveryInfo
}

// RecoveryInfo summarizes what startup found, for the log line and tests.
type RecoveryInfo struct {
	HadSnapshot bool
	TailRecords int
	TornTails   int
	Queues      int
	Messages    int
}

// Open recovers state from dataDir (creating it if empty) and starts the
// pipeline. franq always begins appending in a brand-new segment: recovery
// truncated any torn tail, and never reopening a file for append removes a
// whole class of edge cases.
func Open(dataDir string, opts Options) (*Broker, error) {
	walDir := filepath.Join(dataDir, "wal")
	snapDir := filepath.Join(dataDir, "snap")
	for _, d := range []string{walDir, snapDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}
	b := &Broker{
		dataDir:     dataDir,
		snapDir:     snapDir,
		byName:      make(map[string]*Queue),
		byID:        make(map[uint32]*Queue),
		applierDone: make(chan struct{}),
	}
	b.recovering = true
	res, err := wal.Recover(walDir, snapDir, b.restoreSnapshot, b.recoverApply)
	if err != nil {
		return nil, err
	}
	b.recovering = false
	if b.maxMsgSeen > b.nextMsg.Load() {
		b.nextMsg.Store(b.maxMsgSeen)
	}
	if b.maxQSeen > b.nextQ.Load() {
		b.nextQ.Store(b.maxQSeen)
	}

	log, err := wal.OpenLog(walDir, res.LastSeg+1, wal.Options{
		SegmentBytes: opts.SegmentBytes,
		RetainBytes:  opts.RetainBytes,
	})
	if err != nil {
		return nil, err
	}
	b.log = log

	b.Recovery = RecoveryInfo{
		HadSnapshot: res.Snap != nil,
		TailRecords: res.Records,
		TornTails:   res.TornTails,
		Queues:      len(b.byID),
	}
	for _, q := range b.byID {
		b.Recovery.Messages += len(q.index)
		q.startTimer()
	}
	go b.applier()
	return b, nil
}

// Close stops timers, flushes the log and waits for the applier to drain.
// The HTTP server must already be shut down.
func (b *Broker) Close() {
	b.closeOnce.Do(func() {
		b.mu.RLock()
		for _, q := range b.byID {
			q.stop()
		}
		b.mu.RUnlock()
		b.log.Close()
		<-b.applierDone
		b.snapWG.Wait()
	})
}

// applier is the single goroutine that mutates queue state from committed
// records, in exact log order. Snapshotting happens here, at sealed-segment
// boundaries, which is what makes the snapshot's (state, log position) pair
// exact with zero coordination.
func (b *Broker) applier() {
	defer close(b.applierDone)
	for batch := range b.log.Batches() {
		for _, r := range batch.Reqs {
			r.Apply()
			r.Finish(nil)
		}
		if batch.Sealed != 0 {
			b.maybeSnapshot(batch.Sealed)
		}
	}
}

// ---- snapshotting ----

// maybeSnapshot captures state exactly at the end of sealed segment `sealed`
// (the applier calls this between batches, so nothing from a later segment
// has been applied). Capture is a brief in-memory serialization; the fsync
// and rename happen in the background.
func (b *Broker) maybeSnapshot(sealed uint64) {
	if !b.snapBusy.CompareAndSwap(false, true) {
		return // one in flight; the next seal will try again
	}
	snap := &wal.Snapshot{
		LastSeg:     sealed,
		NextMsgID:   b.nextMsg.Load() + 1,
		NextQueueID: b.nextQ.Load() + 1,
	}
	b.mu.RLock()
	names := make([]string, 0, len(b.byName))
	for n := range b.byName {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		q := b.byName[n]
		q.mu.Lock()
		sq := wal.SnapQueue{ID: q.id, Name: q.name, Cfg: q.cfgJSON()}
		for _, m := range q.index {
			// Leased and pending messages serialize like ready ones:
			// leases are not persisted, and any in-flight transition
			// record lives after this cut and re-applies on recovery.
			sq.Msgs = append(sq.Msgs, wal.SnapMsg{
				ID:         m.id,
				OriginID:   m.originID,
				Priority:   m.priority,
				EnqueuedAt: m.enqueuedAt,
				VisibleAt:  m.visibleAt,
				Deliveries: m.deliveries,
				Body:       m.body,
			})
		}
		q.mu.Unlock()
		snap.Queues = append(snap.Queues, sq)
	}
	b.mu.RUnlock()

	b.snapWG.Add(1)
	go func() {
		defer b.snapWG.Done()
		if err := wal.WriteSnapshot(b.snapDir, snap); err != nil {
			fmt.Fprintf(os.Stderr, "franq: snapshot failed (will retry at next seal): %v\n", err)
		} else {
			b.log.DeleteSealedUpTo(sealed)
		}
		b.snapBusy.Store(false)
	}()
}

// ---- recovery ----

func (b *Broker) restoreSnapshot(s *wal.Snapshot) error {
	if s.NextMsgID > 0 {
		b.nextMsg.Store(s.NextMsgID - 1)
	}
	if s.NextQueueID > 0 {
		b.nextQ.Store(s.NextQueueID - 1)
	}
	for _, sq := range s.Queues {
		if err := b.applyQCreate(sq.ID, sq.Name, sq.Cfg, false); err != nil {
			return err
		}
	}
	now := time.Now().UnixMicro()
	for _, sq := range s.Queues {
		q := b.byID[sq.ID]
		q.mu.Lock()
		for _, sm := range sq.Msgs {
			m := &message{
				id:         sm.ID,
				originID:   sm.OriginID,
				priority:   sm.Priority,
				enqueuedAt: sm.EnqueuedAt,
				visibleAt:  sm.VisibleAt,
				deliveries: sm.Deliveries,
				key:        effKey(sm.Priority, q.cfg.AgingRate, ageStart(sm.EnqueuedAt, sm.VisibleAt)),
				body:       sm.Body,
			}
			q.enqTotal++
			q.insertLocked(m, now)
		}
		q.mu.Unlock()
	}
	return nil
}

// recoverApply replays one tail record. Every branch is idempotent-by-lookup
// (unknown queue: skip; duplicate message: skip; absent message: skip), so a
// snapshot-covered record accidentally replayed converges to the same state.
func (b *Broker) recoverApply(rec any) error {
	switch r := rec.(type) {
	case *wal.Enqueue:
		if r.MsgID > b.maxMsgSeen {
			b.maxMsgSeen = r.MsgID
		}
		q := b.byID[r.QueueID]
		if q == nil {
			return nil // queue was deleted later in the log, or never existed
		}
		m := &message{
			id:         r.MsgID,
			originID:   r.OriginID,
			priority:   r.Priority,
			enqueuedAt: r.EnqueuedAt,
			visibleAt:  r.VisibleAt,
			key:        effKey(r.Priority, q.cfg.AgingRate, ageStart(r.EnqueuedAt, r.VisibleAt)),
			body:       r.Body,
		}
		q.applyEnqueue([]*message{m})
	case *wal.Ack:
		if q := b.byID[r.QueueID]; q != nil {
			q.applyAck(r.MsgID)
		}
	case *wal.Requeue:
		if q := b.byID[r.QueueID]; q != nil {
			q.applyRequeue(r.MsgID, r.VisibleAt, r.Deliveries)
		}
	case *wal.QCreate:
		if r.QueueID > b.maxQSeen {
			b.maxQSeen = r.QueueID
		}
		// First-wins on name collisions, same as the live path.
		_ = b.applyQCreate(r.QueueID, r.Name, r.Cfg, false)
	case *wal.QDelete:
		b.applyQDelete(r.QueueID)
	}
	return nil
}

// ---- queue lifecycle ----

func (b *Broker) applyQCreate(id uint32, name string, cfgJSON []byte, live bool) error {
	var cfg Config
	if err := json.Unmarshal(cfgJSON, &cfg); err != nil {
		return fmt.Errorf("bad queue config in log for %q: %w", name, err)
	}
	if err := cfg.normalize(); err != nil {
		return err
	}
	b.mu.Lock()
	if _, exists := b.byName[name]; exists {
		b.mu.Unlock()
		return ErrQueueExists
	}
	q := newQueue(b, id, name, cfg)
	b.byName[name] = q
	b.byID[id] = q
	if parentName, ok := strings.CutSuffix(name, ".dlq"); ok {
		if parent := b.byName[parentName]; parent != nil {
			parent.dlqID = id
		}
	}
	b.mu.Unlock()
	if live {
		q.startTimer()
	}
	return nil
}

func (b *Broker) applyQDelete(id uint32) {
	b.mu.Lock()
	q := b.byID[id]
	if q == nil {
		b.mu.Unlock()
		return
	}
	delete(b.byID, id)
	delete(b.byName, q.name)
	b.mu.Unlock()
	q.applyDelete()
}

// CreateQueue durably creates a queue and its dead-letter sibling
// ("name.dlq") in one atomic submission.
func (b *Broker) CreateQueue(name string, cfg Config) error {
	if !nameRe.MatchString(name) {
		return fmt.Errorf("%w: queue name must match %s", ErrInvalid, nameRe)
	}
	if strings.HasSuffix(name, ".dlq") {
		return ErrNameReserved
	}
	if err := cfg.normalize(); err != nil {
		return err
	}
	b.mu.RLock()
	_, exists := b.byName[name]
	b.mu.RUnlock()
	if exists {
		return ErrQueueExists
	}
	qid := b.nextQ.Add(1)
	did := b.nextQ.Add(1)
	cfgJSON, _ := json.Marshal(cfg)
	dlqCfg := Config{Order: "fifo", DefaultLeaseMS: cfg.DefaultLeaseMS}
	dlqJSON, _ := json.Marshal(dlqCfg)

	frames := wal.AppendQCreate(nil, &wal.QCreate{QueueID: qid, Name: name, Cfg: cfgJSON})
	frames = wal.AppendQCreate(frames, &wal.QCreate{QueueID: did, Name: name + ".dlq", Cfg: dlqJSON})

	var applyErr error
	req, err := b.log.Submit(frames, func() {
		if e := b.applyQCreate(qid, name, cfgJSON, true); e != nil {
			applyErr = e // lost a create race; recovery skips it identically
			return
		}
		applyErr = b.applyQCreate(did, name+".dlq", dlqJSON, true)
	})
	if err != nil {
		return err
	}
	if err := req.Wait(); err != nil {
		return err
	}
	return applyErr
}

// DeleteQueue durably deletes a queue and its DLQ. Destructive: all messages
// in both are dropped.
func (b *Broker) DeleteQueue(name string) error {
	if strings.HasSuffix(name, ".dlq") {
		return ErrNameReserved
	}
	b.mu.RLock()
	q := b.byName[name]
	b.mu.RUnlock()
	if q == nil {
		return ErrQueueNotFound
	}
	frames := wal.AppendQDelete(nil, &wal.QDelete{QueueID: q.id})
	dlqID := q.dlqID
	if dlqID != 0 {
		frames = wal.AppendQDelete(frames, &wal.QDelete{QueueID: dlqID})
	}
	req, err := b.log.Submit(frames, func() {
		b.applyQDelete(q.id)
		if dlqID != 0 {
			b.applyQDelete(dlqID)
		}
	})
	if err != nil {
		return err
	}
	return req.Wait()
}

// GetQueue resolves a queue by name (DLQs included — they're inspectable).
func (b *Broker) GetQueue(name string) (*Queue, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	q, ok := b.byName[name]
	return q, ok
}

func (b *Broker) Stats(q *Queue) Stats {
	q.mu.Lock()
	s := q.statsLocked()
	dlqID := q.dlqID
	q.mu.Unlock()
	if dlqID != 0 {
		b.mu.RLock()
		d := b.byID[dlqID]
		b.mu.RUnlock()
		if d != nil {
			d.mu.Lock()
			s.DLQDepth = d.ready.len() + d.delayed.len() + len(d.leases)
			d.mu.Unlock()
		}
	}
	return s
}

func (b *Broker) ListStats() []Stats {
	b.mu.RLock()
	qs := make([]*Queue, 0, len(b.byName))
	for _, q := range b.byName {
		qs = append(qs, q)
	}
	b.mu.RUnlock()
	sort.Slice(qs, func(i, j int) bool { return qs[i].name < qs[j].name })
	out := make([]Stats, 0, len(qs))
	for _, q := range qs {
		out = append(out, b.Stats(q))
	}
	return out
}

// ---- producing ----

// EnqueueItem is one producer message.
type EnqueueItem struct {
	Body     []byte
	Priority uint8
	DelayMS  int64
	originID uint64 // internal: set by DLQ moves and replay
}

// Enqueue durably appends a batch of messages. IDs are returned after — and
// only after — the records are fsynced and applied: nothing is externally
// observable before its record is durable.
func (b *Broker) Enqueue(q *Queue, items []EnqueueItem) ([]uint64, error) {
	if len(items) == 0 {
		return nil, nil
	}
	now := time.Now().UnixMicro()
	var frames []byte
	ids := make([]uint64, 0, len(items))
	msgs := make([]*message, 0, len(items))
	for _, it := range items {
		id := b.nextMsg.Add(1)
		var visibleAt int64
		if it.DelayMS > 0 {
			visibleAt = now + it.DelayMS*1000
		}
		frames = wal.AppendEnqueue(frames, &wal.Enqueue{
			QueueID:    q.id,
			MsgID:      id,
			OriginID:   it.originID,
			Priority:   it.Priority,
			EnqueuedAt: now,
			VisibleAt:  visibleAt,
			Body:       it.Body,
		})
		msgs = append(msgs, &message{
			id:         id,
			originID:   it.originID,
			priority:   it.Priority,
			enqueuedAt: now,
			visibleAt:  visibleAt,
			key:        effKey(it.Priority, q.cfg.AgingRate, ageStart(now, visibleAt)),
			body:       it.Body,
		})
		ids = append(ids, id)
	}
	req, err := b.log.Submit(frames, func() { q.applyEnqueue(msgs) })
	if err != nil {
		return nil, err
	}
	if err := req.Wait(); err != nil {
		return nil, err
	}
	return ids, nil
}

// ---- consumer record submission ----

func (b *Broker) submitAck(q *Queue, id uint64) error {
	frames := wal.AppendAck(nil, &wal.Ack{QueueID: q.id, MsgID: id})
	req, err := b.log.Submit(frames, func() { q.applyAck(id) })
	if err != nil {
		return err
	}
	return req.Wait()
}

// submitRequeue logs a reserved lease transition: either back into the queue
// (REQUEUE) or into the DLQ. The DLQ move is ENQUEUE-into-dlq followed by
// ACK-of-original in that order — add before remove — so a torn tail between
// the two yields a duplicate, never a loss.
func (b *Broker) submitRequeue(q *Queue, act requeueAction) error {
	if act.toDLQ {
		b.mu.RLock()
		dlq := b.byID[q.dlqID]
		b.mu.RUnlock()
		if dlq != nil {
			return b.submitDLQMove(q, dlq, act)
		}
		// No DLQ (deleted?): fall through to a plain requeue.
	}
	frames := wal.AppendRequeue(nil, &wal.Requeue{
		QueueID:    q.id,
		MsgID:      act.m.id,
		VisibleAt:  act.visibleAt,
		Deliveries: act.deliveries,
		Reason:     act.reason,
	})
	req, err := b.log.Submit(frames, func() { q.applyRequeue(act.m.id, act.visibleAt, act.deliveries) })
	if err != nil {
		return err
	}
	return req.Wait()
}

func (b *Broker) submitDLQMove(q, dlq *Queue, act requeueAction) error {
	now := time.Now().UnixMicro()
	newID := b.nextMsg.Add(1)
	m := act.m
	copyMsg := &message{
		id:         newID,
		originID:   m.id,
		priority:   m.priority,
		enqueuedAt: now,
		visibleAt:  0,
		deliveries: act.deliveries,
		key:        effKey(m.priority, dlq.cfg.AgingRate, now),
		body:       m.body,
	}
	frames := wal.AppendEnqueue(nil, &wal.Enqueue{
		QueueID:    dlq.id,
		MsgID:      newID,
		OriginID:   m.id,
		Priority:   m.priority,
		EnqueuedAt: now,
		Body:       m.body,
	})
	frames = wal.AppendAck(frames, &wal.Ack{QueueID: q.id, MsgID: m.id})
	req, err := b.log.Submit(frames, func() {
		dlq.applyEnqueue([]*message{copyMsg})
		// DLQ copies keep the delivery count that condemned them.
		dlq.mu.Lock()
		if mm := dlq.index[newID]; mm != nil {
			mm.deliveries = act.deliveries
		}
		dlq.mu.Unlock()
		q.applyAck(m.id)
		q.mu.Lock()
		q.dlqTotal++
		q.mu.Unlock()
	})
	if err != nil {
		return err
	}
	return req.Wait()
}

// AckBatch acks several deliveries in one durable submission. Per-item
// results; stale leases fail their item only.
func (b *Broker) AckBatch(q *Queue, ids []uint64, tokens []string) []error {
	errs := make([]error, len(ids))
	var frames []byte
	var acked []uint64
	q.mu.Lock()
	for i, id := range ids {
		l := q.leases[id]
		if l == nil || l.token != tokens[i] {
			errs[i] = ErrLeaseLost
			continue
		}
		delete(q.leases, id)
		q.leaseH.remove(l)
		l.m.where = locPending
		frames = wal.AppendAck(frames, &wal.Ack{QueueID: q.id, MsgID: id})
		acked = append(acked, id)
	}
	q.mu.Unlock()
	if len(acked) == 0 {
		return errs
	}
	req, err := b.log.Submit(frames, func() {
		for _, id := range acked {
			q.applyAck(id)
		}
	})
	if err == nil {
		err = req.Wait()
	}
	if err != nil {
		for i := range errs {
			if errs[i] == nil {
				errs[i] = err
			}
		}
	}
	return errs
}
