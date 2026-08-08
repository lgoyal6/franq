package core

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/lgoyal6/franq/internal/wal"
)

// Config is a queue's immutable configuration.
type Config struct {
	Order          string  `json:"order"`            // "fifo" | "lifo" — the ready-heap tie-break
	AgingRate      float64 `json:"aging_rate"`       // priority points gained per second of waiting; 0 disables aging
	MaxDeliveries  uint32  `json:"max_deliveries"`   // move to DLQ after this many delivery attempts; 0 = unlimited
	DefaultLeaseMS int64   `json:"default_lease_ms"` // lease when dequeue doesn't specify one
}

func (c *Config) normalize() error {
	switch c.Order {
	case "":
		c.Order = "fifo"
	case "fifo", "lifo":
	default:
		return fmt.Errorf("%w: order must be fifo or lifo", ErrInvalid)
	}
	if c.AgingRate < 0 || c.AgingRate > 1000 {
		return fmt.Errorf("%w: aging_rate must be in [0, 1000]", ErrInvalid)
	}
	if c.MaxDeliveries > 1000 {
		return fmt.Errorf("%w: max_deliveries must be <= 1000", ErrInvalid)
	}
	if c.DefaultLeaseMS == 0 {
		c.DefaultLeaseMS = 30_000
	}
	if c.DefaultLeaseMS < 100 || c.DefaultLeaseMS > 3_600_000 {
		return fmt.Errorf("%w: default_lease_ms must be in [100, 3600000]", ErrInvalid)
	}
	return nil
}

type location uint8

const (
	locNone    location = iota
	locReady            // in the ready heap, eligible for dequeue
	locDelayed          // in the delay heap, invisible until visibleAt
	locLeased           // delivered, awaiting ack under a lease
	locPending          // reserved: a transition record is committed-in-flight
)

type message struct {
	id         uint64
	originID   uint64
	priority   uint8
	enqueuedAt int64 // unix micros
	visibleAt  int64 // unix micros; 0 = immediately visible
	deliveries uint32
	key        float64
	body       []byte

	where   location
	heapIdx int
}

type lease struct {
	m        *message
	token    string
	deadline time.Time // monotonic; leases are never persisted
	heapIdx  int
}

func newToken() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err) // crypto/rand failure means the host is broken
	}
	return hex.EncodeToString(b[:])
}

type waiter struct {
	ch chan struct{}
}

// Delivery is what a consumer receives: message fields plus the lease token
// that authorizes ack/nack/extend for exactly this delivery.
type Delivery struct {
	ID         uint64
	OriginID   uint64
	Priority   uint8
	EnqueuedAt int64
	Deliveries uint32 // including this one
	Body       []byte
	Token      string
}

// Queue holds one queue's in-memory state. Every mutation of the heaps,
// lease table, index or waiter list happens under mu. Log-derived mutations
// (enqueue/ack/requeue) are applied only by the broker's applier goroutine in
// log order; dequeue/extend are memory-only and never touch the log.
type Queue struct {
	id    uint32
	name  string
	cfg   Config
	b     *Broker
	dlqID uint32 // sibling dead-letter queue; 0 on DLQ queues themselves

	mu      sync.Mutex
	index   map[uint64]*message // every live message in any location
	ready   msgHeap
	delayed msgHeap
	leases  map[uint64]*lease
	leaseH  leaseHeap
	waiters []*waiter
	deleted bool

	enqTotal, ackTotal, requeueTotal, dlqTotal uint64

	timerWake    chan struct{}
	timerStop    chan struct{}
	timerStarted bool
	stopOnce     sync.Once
}

func newQueue(b *Broker, id uint32, name string, cfg Config) *Queue {
	q := &Queue{
		id:        id,
		name:      name,
		cfg:       cfg,
		b:         b,
		index:     make(map[uint64]*message),
		leases:    make(map[uint64]*lease),
		timerWake: make(chan struct{}, 1),
		timerStop: make(chan struct{}),
	}
	// The one comparator behind every advertised mode: aging-adjusted
	// priority first, then arrival order in the configured direction.
	lifo := cfg.Order == "lifo"
	q.ready.less = func(a, b *message) bool {
		if a.key != b.key {
			return a.key > b.key
		}
		if lifo {
			return a.id > b.id
		}
		return a.id < b.id
	}
	q.delayed.less = func(a, b *message) bool { return a.visibleAt < b.visibleAt }
	return q
}

func (q *Queue) Name() string { return q.name }
func (q *Queue) Cfg() Config  { return q.cfg }

// startTimer launches the per-queue timekeeper (delay migration + lease
// expiry). Not started during recovery; the broker starts all timers once
// recovery completes.
func (q *Queue) startTimer() {
	q.mu.Lock()
	if q.timerStarted || q.deleted {
		q.mu.Unlock()
		return
	}
	q.timerStarted = true
	q.mu.Unlock()
	go q.timerLoop()
}

func (q *Queue) stop() {
	q.stopOnce.Do(func() { close(q.timerStop) })
}

// ---- locked helpers ----

func (q *Queue) insertLocked(m *message, now int64) {
	q.index[m.id] = m
	if m.visibleAt > now {
		m.where = locDelayed
		q.delayed.push(m)
		q.pokeTimerLocked()
		return
	}
	m.where = locReady
	q.ready.push(m)
	q.notifyLocked(1)
}

// migrateDueLocked moves due delayed messages into the ready heap.
func (q *Queue) migrateDueLocked(nowMicros int64) {
	n := 0
	for {
		m := q.delayed.peek()
		if m == nil || m.visibleAt > nowMicros {
			break
		}
		q.delayed.pop()
		m.where = locReady
		q.ready.push(m)
		n++
	}
	if n > 0 {
		q.notifyLocked(n)
	}
}

// notifyLocked wakes up to n parked dequeuers, front of the line first. One
// wakeup per newly-ready message — waking everyone would just stampede the
// mutex.
func (q *Queue) notifyLocked(n int) {
	for i := 0; i < n && len(q.waiters) > 0; i++ {
		w := q.waiters[0]
		q.waiters = q.waiters[1:]
		select {
		case w.ch <- struct{}{}:
		default:
		}
	}
}

func (q *Queue) wakeAllLocked() {
	for _, w := range q.waiters {
		select {
		case w.ch <- struct{}{}:
		default:
		}
	}
	q.waiters = nil
}

func (q *Queue) pokeTimerLocked() {
	select {
	case q.timerWake <- struct{}{}:
	default:
	}
}

func (q *Queue) removeWaiter(w *waiter) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, x := range q.waiters {
		if x == w {
			q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
			return
		}
	}
}

// ---- apply operations (log-derived; run by the applier or recovery) ----

// applyEnqueue inserts committed messages. Handles both live traffic and
// recovery replay; a nil entry check makes replay idempotent.
func (q *Queue) applyEnqueue(msgs []*message) {
	now := time.Now().UnixMicro()
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.deleted {
		return
	}
	for _, m := range msgs {
		if _, dup := q.index[m.id]; dup {
			continue
		}
		q.enqTotal++
		q.insertLocked(m, now)
	}
}

// applyAck removes a message wherever it currently is. Live path: the acker
// already reserved it (locPending). Recovery path: it sits in ready/delayed.
func (q *Queue) applyAck(id uint64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	m := q.index[id]
	if m == nil {
		return
	}
	switch m.where {
	case locReady:
		q.ready.remove(m)
	case locDelayed:
		q.delayed.remove(m)
	case locLeased:
		if l := q.leases[id]; l != nil {
			q.leaseH.remove(l)
			delete(q.leases, id)
		}
	case locPending, locNone:
	}
	delete(q.index, id)
	m.where = locNone
	q.ackTotal++
}

// applyRequeue makes a message visible again (nack or lease expiry) with its
// updated delivery count. The aging key is deliberately NOT recomputed:
// seniority is retained, so anti-starvation covers total time in the system,
// and the key stays a pure function of the original ENQUEUE record.
func (q *Queue) applyRequeue(id uint64, visibleAt int64, deliveries uint32) {
	q.mu.Lock()
	defer q.mu.Unlock()
	m := q.index[id]
	if m == nil {
		return
	}
	switch m.where {
	case locReady:
		q.ready.remove(m)
	case locDelayed:
		q.delayed.remove(m)
	case locLeased:
		if l := q.leases[id]; l != nil {
			q.leaseH.remove(l)
			delete(q.leases, id)
		}
	case locPending, locNone:
	}
	m.deliveries = deliveries
	m.visibleAt = visibleAt
	q.requeueTotal++
	now := time.Now().UnixMicro()
	delete(q.index, id) // insertLocked re-adds
	q.insertLocked(m, now)
}

func (q *Queue) applyDelete() {
	q.mu.Lock()
	q.deleted = true
	q.index = make(map[uint64]*message)
	q.ready.items = nil
	q.delayed.items = nil
	q.leases = make(map[uint64]*lease)
	q.leaseH.items = nil
	q.wakeAllLocked()
	q.mu.Unlock()
	q.stop()
}

// ---- consumer operations ----

// Dequeue pops up to max ready messages under fresh leases, long-polling up
// to wait when the queue is empty. The waiting goroutine is the HTTP handler
// itself — nothing is spawned, so nothing can leak. Registration happens
// under the same mutex as insertion+notify, so wakeups cannot be lost.
func (q *Queue) Dequeue(ctx context.Context, max int, wait, leaseDur time.Duration) ([]Delivery, error) {
	if max <= 0 {
		max = 1
	}
	if leaseDur <= 0 {
		leaseDur = time.Duration(q.cfg.DefaultLeaseMS) * time.Millisecond
	}
	var timeout <-chan time.Time
	if wait > 0 {
		t := time.NewTimer(wait)
		defer t.Stop()
		timeout = t.C
	}
	retry := false
	for {
		q.mu.Lock()
		if q.deleted {
			q.mu.Unlock()
			return nil, ErrQueueNotFound
		}
		q.migrateDueLocked(time.Now().UnixMicro())
		var out []Delivery
		deadline := time.Now().Add(leaseDur)
		for len(out) < max && q.ready.len() > 0 {
			m := q.ready.pop()
			l := &lease{m: m, token: newToken(), deadline: deadline}
			m.where = locLeased
			q.leases[m.id] = l
			q.leaseH.push(l)
			out = append(out, Delivery{
				ID:         m.id,
				OriginID:   m.originID,
				Priority:   m.priority,
				EnqueuedAt: m.enqueuedAt,
				Deliveries: m.deliveries + 1,
				Body:       m.body,
				Token:      l.token,
			})
		}
		if len(out) > 0 {
			q.pokeTimerLocked()
			q.mu.Unlock()
			return out, nil
		}
		if wait <= 0 {
			q.mu.Unlock()
			return nil, nil
		}
		w := &waiter{ch: make(chan struct{}, 1)}
		if retry {
			// Woken but lost the race to another consumer: rejoin at the
			// front to keep arrival order roughly fair.
			q.waiters = append([]*waiter{w}, q.waiters...)
		} else {
			q.waiters = append(q.waiters, w)
		}
		q.mu.Unlock()

		select {
		case <-w.ch:
			retry = true
			continue
		case <-timeout:
			q.removeWaiter(w)
			return nil, nil
		case <-ctx.Done():
			q.removeWaiter(w)
			return nil, ctx.Err()
		}
	}
}

// requeueAction is a reserved lease transition, decided under the queue lock
// but logged outside it.
type requeueAction struct {
	m          *message
	deliveries uint32
	visibleAt  int64
	reason     byte
	toDLQ      bool
}

// reserveRequeueLocked removes the lease entry and marks the message pending.
// This removal under q.mu — BEFORE anything is logged — is the serialization
// point between ack, nack and sweeper expiry: whoever wins the mutex owns the
// transition, the loser finds no lease entry and backs off, and the log can
// never contain conflicting records for one delivery.
func (q *Queue) reserveRequeueLocked(l *lease, reason byte, delayMS int64) requeueAction {
	delete(q.leases, l.m.id)
	q.leaseH.remove(l)
	l.m.where = locPending
	count := l.m.deliveries + 1
	visibleAt := time.Now().Add(time.Duration(delayMS) * time.Millisecond).UnixMicro()
	return requeueAction{
		m:          l.m,
		deliveries: count,
		visibleAt:  visibleAt,
		reason:     reason,
		toDLQ:      q.cfg.MaxDeliveries > 0 && count >= q.cfg.MaxDeliveries,
	}
}

// Ack deletes a delivered message, durably. Stale tokens (expired lease,
// double ack, redelivered elsewhere) get ErrLeaseLost.
func (q *Queue) Ack(id uint64, token string) error {
	q.mu.Lock()
	l := q.leases[id]
	if l == nil || l.token != token {
		q.mu.Unlock()
		return ErrLeaseLost
	}
	delete(q.leases, id)
	q.leaseH.remove(l)
	l.m.where = locPending
	q.mu.Unlock()
	return q.b.submitAck(q, id)
}

// Nack returns a message early, optionally with a redelivery delay.
func (q *Queue) Nack(id uint64, token string, delayMS int64) error {
	q.mu.Lock()
	l := q.leases[id]
	if l == nil || l.token != token {
		q.mu.Unlock()
		return ErrLeaseLost
	}
	act := q.reserveRequeueLocked(l, wal.ReasonNack, delayMS)
	q.mu.Unlock()
	return q.b.submitRequeue(q, act)
}

// Extend pushes a lease deadline out (consumer heartbeat). Memory-only:
// leases are never persisted.
func (q *Queue) Extend(id uint64, token string, leaseMS int64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	l := q.leases[id]
	if l == nil || l.token != token {
		return ErrLeaseLost
	}
	l.deadline = time.Now().Add(time.Duration(leaseMS) * time.Millisecond)
	q.leaseH.fix(l)
	q.pokeTimerLocked()
	return nil
}

// ---- timekeeping ----

// timerLoop is the queue's single clock: it migrates due delayed messages
// and expires leases, sleeping exactly until the next deadline and getting
// poked whenever an earlier one appears.
func (q *Queue) timerLoop() {
	for {
		q.mu.Lock()
		if q.deleted {
			q.mu.Unlock()
			return
		}
		now := time.Now()
		q.migrateDueLocked(now.UnixMicro())
		var expired []requeueAction
		for {
			l := q.leaseH.peek()
			if l == nil || l.deadline.After(now) {
				break
			}
			expired = append(expired, q.reserveRequeueLocked(l, wal.ReasonLeaseExpired, 0))
		}
		sleep := q.nextWakeLocked(now)
		q.mu.Unlock()

		// Log the expiries outside the lock: Submit can block on a full
		// commit channel and needs no queue state.
		for _, act := range expired {
			if err := q.b.submitRequeue(q, act); err != nil {
				return // broker closing; message recovers as ready on restart
			}
		}

		var timer *time.Timer
		var fire <-chan time.Time
		if sleep > 0 {
			timer = time.NewTimer(sleep)
			fire = timer.C
		}
		select {
		case <-fire:
		case <-q.timerWake:
		case <-q.timerStop:
			if timer != nil {
				timer.Stop()
			}
			return
		}
		if timer != nil {
			timer.Stop()
		}
	}
}

// nextWakeLocked returns how long to sleep until the earliest deadline, or 0
// for "nothing scheduled, wait for a poke".
func (q *Queue) nextWakeLocked(now time.Time) time.Duration {
	var next time.Duration
	if m := q.delayed.peek(); m != nil {
		next = time.Duration(m.visibleAt-now.UnixMicro()) * time.Microsecond
	}
	if l := q.leaseH.peek(); l != nil {
		d := l.deadline.Sub(now)
		if next == 0 || d < next {
			next = d
		}
	}
	if next < 0 {
		next = time.Millisecond
	}
	return next
}

// ---- stats ----

type Stats struct {
	Name           string  `json:"name"`
	Order          string  `json:"order"`
	AgingRate      float64 `json:"aging_rate"`
	MaxDeliveries  uint32  `json:"max_deliveries"`
	DefaultLeaseMS int64   `json:"default_lease_ms"`
	Ready          int     `json:"ready"`
	Delayed        int     `json:"delayed"`
	InFlight       int     `json:"in_flight"`
	TotalEnqueued  uint64  `json:"total_enqueued"`
	TotalAcked     uint64  `json:"total_acked"`
	TotalRequeued  uint64  `json:"total_requeued"`
	TotalDLQ       uint64  `json:"total_dlq"`
	DLQDepth       int     `json:"dlq_depth"`
}

func (q *Queue) statsLocked() Stats {
	return Stats{
		Name:           q.name,
		Order:          q.cfg.Order,
		AgingRate:      q.cfg.AgingRate,
		MaxDeliveries:  q.cfg.MaxDeliveries,
		DefaultLeaseMS: q.cfg.DefaultLeaseMS,
		Ready:          q.ready.len(),
		Delayed:        q.delayed.len(),
		InFlight:       len(q.leases),
		TotalEnqueued:  q.enqTotal,
		TotalAcked:     q.ackTotal,
		TotalRequeued:  q.requeueTotal,
		TotalDLQ:       q.dlqTotal,
	}
}

func (q *Queue) cfgJSON() []byte {
	b, _ := json.Marshal(q.cfg)
	return b
}
