package core

import (
	"errors"
	"os"
	"time"

	"github.com/lgoyal6/franq/internal/wal"
)

var errReplayLimit = errors.New("replay limit reached")

// ReplayResult reports what a time-travel replay did. WindowStart is the
// oldest record still retained in the log - the honest replay horizon.
type ReplayResult struct {
	Replayed    int
	WindowStart time.Time
}

// Replay is the third layer of franq's replay story: because the WAL retains
// history until retention-driven compaction, previously ACKed messages can be
// re-enqueued as fresh copies (new id, OriginID pointing at the source).
// Kafka-style replay in a queue - SQS cannot do this.
//
// Scans sealed segments fully and the active segment up to its committed
// offset; segment deletion is blocked for the duration. Messages still live
// in the queue are skipped. Replay is not atomic with respect to concurrent
// traffic - it re-runs history, it doesn't freeze the present.
func (b *Broker) Replay(q *Queue, from, to time.Time, limit int) (ReplayResult, error) {
	if limit <= 0 {
		limit = 1000
	}
	fromMicros := int64(0)
	if !from.IsZero() {
		fromMicros = from.UnixMicro()
	}
	toMicros := time.Now().UnixMicro()
	if !to.IsZero() {
		toMicros = to.UnixMicro()
	}

	sealed, active, activeLen, release := b.log.View()
	defer release()

	var res ReplayResult
	var oldest int64
	var pending []EnqueueItem
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		if _, err := b.Enqueue(q, pending); err != nil {
			return err
		}
		res.Replayed += len(pending)
		pending = nil
		return nil
	}

	scanOne := func(payload []byte) error {
		rec, err := wal.Decode(payload)
		if err != nil {
			return err
		}
		e, ok := rec.(*wal.Enqueue)
		if !ok {
			return nil
		}
		if oldest == 0 || e.EnqueuedAt < oldest {
			oldest = e.EnqueuedAt
		}
		if e.QueueID != q.id || e.EnqueuedAt < fromMicros || e.EnqueuedAt >= toMicros {
			return nil
		}
		if res.Replayed+len(pending) >= limit {
			return errReplayLimit
		}
		q.mu.Lock()
		_, live := q.index[e.MsgID]
		q.mu.Unlock()
		if live {
			return nil
		}
		origin := e.MsgID
		if e.OriginID != 0 {
			origin = e.OriginID
		}
		pending = append(pending, EnqueueItem{
			Body:     e.Body,
			Priority: e.Priority,
			originID: origin,
		})
		if len(pending) >= 256 {
			return flush()
		}
		return nil
	}

	scanFile := func(path string, maxLen int64) error {
		buf, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if maxLen > 0 && int64(len(buf)) > maxLen {
			buf = buf[:maxLen]
		}
		const hdr = 16 // segment header
		if len(buf) <= hdr {
			return nil
		}
		_, err = wal.Scan(buf[hdr:], scanOne)
		return err
	}

	for _, path := range sealed {
		if err := scanFile(path, 0); err != nil {
			if errors.Is(err, errReplayLimit) {
				break
			}
			return res, err
		}
	}
	if res.Replayed+len(pending) < limit {
		if err := scanFile(active, activeLen); err != nil && !errors.Is(err, errReplayLimit) {
			return res, err
		}
	}
	if err := flush(); err != nil {
		return res, err
	}
	if oldest > 0 {
		res.WindowStart = time.UnixMicro(oldest)
	}
	return res, nil
}
