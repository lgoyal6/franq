package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	snapMagic   = "FRQSNP01"
	snapVersion = 1
	snapSuffix  = ".snap"
)

// SnapMsg is one message in a snapshot. Leased and reservation-pending
// messages are serialized exactly like ready ones: leases are deliberately
// not persisted, and any in-flight transition record lives in a segment after
// the snapshot cut, so replaying it on recovery converges to the right state.
type SnapMsg struct {
	ID         uint64
	OriginID   uint64
	Priority   uint8
	EnqueuedAt int64
	VisibleAt  int64
	Deliveries uint32
	Body       []byte
}

type SnapQueue struct {
	ID   uint32
	Name string
	Cfg  []byte
	Msgs []SnapMsg
}

// Snapshot is the state after applying everything through the end of segment
// LastSeg. NextMsgID/NextQueueID are persisted explicitly because a max-scan
// of surviving records would miss ids whose records were compacted away.
type Snapshot struct {
	LastSeg     uint64
	NextMsgID   uint64
	NextQueueID uint32
	Queues      []SnapQueue
}

func snapshotName(lastSeg uint64) string { return fmt.Sprintf("%012d%s", lastSeg, snapSuffix) }

func parseSnapshotName(name string) (uint64, bool) {
	base, ok := strings.CutSuffix(name, snapSuffix)
	if !ok || len(base) != 12 {
		return 0, false
	}
	id, err := strconv.ParseUint(base, 10, 64)
	if err != nil {
		return 0, false
	}
	return id, true
}

func EncodeSnapshot(s *Snapshot) []byte {
	b := make([]byte, 0, 4096)
	b = append(b, snapMagic...)
	b = binary.LittleEndian.AppendUint32(b, snapVersion)
	b = binary.LittleEndian.AppendUint64(b, s.LastSeg)
	b = binary.LittleEndian.AppendUint64(b, s.NextMsgID)
	b = binary.LittleEndian.AppendUint32(b, s.NextQueueID)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(s.Queues)))
	for _, q := range s.Queues {
		b = binary.LittleEndian.AppendUint32(b, q.ID)
		b = binary.LittleEndian.AppendUint16(b, uint16(len(q.Name)))
		b = append(b, q.Name...)
		b = binary.LittleEndian.AppendUint32(b, uint32(len(q.Cfg)))
		b = append(b, q.Cfg...)
		b = binary.LittleEndian.AppendUint32(b, uint32(len(q.Msgs)))
		for _, m := range q.Msgs {
			b = binary.LittleEndian.AppendUint64(b, m.ID)
			b = binary.LittleEndian.AppendUint64(b, m.OriginID)
			b = append(b, m.Priority)
			b = binary.LittleEndian.AppendUint64(b, uint64(m.EnqueuedAt))
			b = binary.LittleEndian.AppendUint64(b, uint64(m.VisibleAt))
			b = binary.LittleEndian.AppendUint32(b, m.Deliveries)
			b = binary.LittleEndian.AppendUint32(b, uint32(len(m.Body)))
			b = append(b, m.Body...)
		}
	}
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
}

func DecodeSnapshot(b []byte) (*Snapshot, error) {
	if len(b) < len(snapMagic)+4+4 {
		return nil, errors.New("wal: snapshot too short")
	}
	if string(b[:8]) != snapMagic {
		return nil, errors.New("wal: bad snapshot magic")
	}
	body, tail := b[:len(b)-4], b[len(b)-4:]
	if crc32.Checksum(body, castagnoli) != binary.LittleEndian.Uint32(tail) {
		return nil, errors.New("wal: snapshot crc mismatch")
	}
	r := &rd{b: body, off: 8}
	if v := r.u32(); v != snapVersion {
		return nil, fmt.Errorf("wal: unsupported snapshot version %d", v)
	}
	s := &Snapshot{
		LastSeg:     r.u64(),
		NextMsgID:   r.u64(),
		NextQueueID: r.u32(),
	}
	nq := int(r.u32())
	for range nq {
		if r.err != nil {
			break
		}
		q := SnapQueue{ID: r.u32()}
		q.Name = string(r.take(int(r.u16())))
		q.Cfg = r.take(int(r.u32()))
		nm := int(r.u32())
		for range nm {
			if r.err != nil {
				break
			}
			m := SnapMsg{ID: r.u64(), OriginID: r.u64(), Priority: r.u8()}
			m.EnqueuedAt = r.i64()
			m.VisibleAt = r.i64()
			m.Deliveries = r.u32()
			m.Body = r.take(int(r.u32()))
			q.Msgs = append(q.Msgs, m)
		}
		s.Queues = append(s.Queues, q)
	}
	if err := r.done(); err != nil {
		return nil, err
	}
	return s, nil
}

// WriteSnapshot durably writes s (tmp file → fsync → rename → dir fsync) and
// then prunes older snapshots. The previous snapshot is only removed after
// the new one is fully durable, so a crash mid-write always leaves a valid
// snapshot behind.
func WriteSnapshot(dir string, s *Snapshot) error {
	tmp := filepath.Join(dir, "snapshot.tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(EncodeSnapshot(s)); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	failpoint("mid-snapshot")
	final := filepath.Join(dir, snapshotName(s.LastSeg))
	if err := os.Rename(tmp, final); err != nil {
		return err
	}
	if err := fsyncDir(dir); err != nil {
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil // pruning is best-effort
	}
	for _, e := range entries {
		if id, ok := parseSnapshotName(e.Name()); ok && id < s.LastSeg {
			os.Remove(filepath.Join(dir, e.Name()))
		}
	}
	return nil
}

// LoadLatestSnapshot returns the newest snapshot that decodes and passes its
// CRC, falling back to older ones, or nil if none is usable.
func LoadLatestSnapshot(dir string) (*Snapshot, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var ids []uint64
	for _, e := range entries {
		if id, ok := parseSnapshotName(e.Name()); ok {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] })
	for _, id := range ids {
		buf, err := os.ReadFile(filepath.Join(dir, snapshotName(id)))
		if err != nil {
			continue
		}
		s, err := DecodeSnapshot(buf)
		if err != nil {
			fmt.Fprintf(os.Stderr, "franq: skipping unusable snapshot %s: %v\n", snapshotName(id), err)
			continue
		}
		return s, nil
	}
	return nil, nil
}
