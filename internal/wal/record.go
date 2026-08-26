// Package wal is franq's storage engine: an append-only, CRC-framed
// write-ahead log with group commit, segment rotation, snapshots and
// crash recovery. No database underneath - files and fsync only.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// Record types.
const (
	TypeEnqueue byte = 0x01
	TypeAck     byte = 0x02
	TypeRequeue byte = 0x03
	TypeQCreate byte = 0x04
	TypeQDelete byte = 0x05
)

// Requeue reasons.
const (
	ReasonNack         byte = 0
	ReasonLeaseExpired byte = 1
)

// MaxPayload bounds a single record payload; a larger length prefix in the
// log is treated as corruption, not as a record.
const MaxPayload = 16 << 20

// frameHeader is [u32 payloadLen][u32 crc32c(payload)]. All integers in the
// log are little-endian.
const frameHeader = 8

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Enqueue is the only record that carries a message body. VisibleAt <=
// EnqueuedAt means immediately visible (0 is the common encoding).
type Enqueue struct {
	QueueID    uint32
	MsgID      uint64
	OriginID   uint64 // 0 = original; else the message this was copied from (DLQ move, replay)
	Priority   uint8
	EnqueuedAt int64 // unix microseconds
	VisibleAt  int64 // unix microseconds
	Body       []byte
}

type Ack struct {
	QueueID uint32
	MsgID   uint64
}

type Requeue struct {
	QueueID    uint32
	MsgID      uint64
	VisibleAt  int64
	Deliveries uint32
	Reason     byte
}

type QCreate struct {
	QueueID uint32
	Name    string
	Cfg     []byte // JSON so the config can grow without a log-format bump
}

type QDelete struct {
	QueueID uint32
}

func appendFrame(dst, payload []byte) []byte {
	var hdr [frameHeader]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[4:8], crc32.Checksum(payload, castagnoli))
	dst = append(dst, hdr[:]...)
	return append(dst, payload...)
}

func AppendEnqueue(dst []byte, r *Enqueue) []byte {
	p := make([]byte, 0, 1+4+8+8+1+8+8+4+len(r.Body))
	p = append(p, TypeEnqueue)
	p = binary.LittleEndian.AppendUint32(p, r.QueueID)
	p = binary.LittleEndian.AppendUint64(p, r.MsgID)
	p = binary.LittleEndian.AppendUint64(p, r.OriginID)
	p = append(p, r.Priority)
	p = binary.LittleEndian.AppendUint64(p, uint64(r.EnqueuedAt))
	p = binary.LittleEndian.AppendUint64(p, uint64(r.VisibleAt))
	p = binary.LittleEndian.AppendUint32(p, uint32(len(r.Body)))
	p = append(p, r.Body...)
	return appendFrame(dst, p)
}

func AppendAck(dst []byte, r *Ack) []byte {
	p := make([]byte, 0, 1+4+8)
	p = append(p, TypeAck)
	p = binary.LittleEndian.AppendUint32(p, r.QueueID)
	p = binary.LittleEndian.AppendUint64(p, r.MsgID)
	return appendFrame(dst, p)
}

func AppendRequeue(dst []byte, r *Requeue) []byte {
	p := make([]byte, 0, 1+4+8+8+4+1)
	p = append(p, TypeRequeue)
	p = binary.LittleEndian.AppendUint32(p, r.QueueID)
	p = binary.LittleEndian.AppendUint64(p, r.MsgID)
	p = binary.LittleEndian.AppendUint64(p, uint64(r.VisibleAt))
	p = binary.LittleEndian.AppendUint32(p, r.Deliveries)
	p = append(p, r.Reason)
	return appendFrame(dst, p)
}

func AppendQCreate(dst []byte, r *QCreate) []byte {
	p := make([]byte, 0, 1+4+2+len(r.Name)+4+len(r.Cfg))
	p = append(p, TypeQCreate)
	p = binary.LittleEndian.AppendUint32(p, r.QueueID)
	p = binary.LittleEndian.AppendUint16(p, uint16(len(r.Name)))
	p = append(p, r.Name...)
	p = binary.LittleEndian.AppendUint32(p, uint32(len(r.Cfg)))
	p = append(p, r.Cfg...)
	return appendFrame(dst, p)
}

func AppendQDelete(dst []byte, r *QDelete) []byte {
	p := make([]byte, 0, 1+4)
	p = append(p, TypeQDelete)
	p = binary.LittleEndian.AppendUint32(p, r.QueueID)
	return appendFrame(dst, p)
}

// rd is a cursor over a record payload; the first decode error sticks.
type rd struct {
	b   []byte
	off int
	err error
}

func (r *rd) need(n int) bool {
	if r.err != nil {
		return false
	}
	if len(r.b)-r.off < n {
		r.err = errors.New("wal: short record")
		return false
	}
	return true
}

func (r *rd) u8() byte {
	if !r.need(1) {
		return 0
	}
	v := r.b[r.off]
	r.off++
	return v
}

func (r *rd) u16() uint16 {
	if !r.need(2) {
		return 0
	}
	v := binary.LittleEndian.Uint16(r.b[r.off:])
	r.off += 2
	return v
}

func (r *rd) u32() uint32 {
	if !r.need(4) {
		return 0
	}
	v := binary.LittleEndian.Uint32(r.b[r.off:])
	r.off += 4
	return v
}

func (r *rd) u64() uint64 {
	if !r.need(8) {
		return 0
	}
	v := binary.LittleEndian.Uint64(r.b[r.off:])
	r.off += 8
	return v
}

func (r *rd) i64() int64 { return int64(r.u64()) }

// take returns a copy so decoded records never pin a whole segment buffer.
func (r *rd) take(n int) []byte {
	if n < 0 || !r.need(n) {
		if r.err == nil {
			r.err = errors.New("wal: negative length")
		}
		return nil
	}
	v := append([]byte(nil), r.b[r.off:r.off+n]...)
	r.off += n
	return v
}

func (r *rd) done() error {
	if r.err != nil {
		return r.err
	}
	if r.off != len(r.b) {
		return fmt.Errorf("wal: %d trailing bytes in record", len(r.b)-r.off)
	}
	return nil
}

// Decode parses one record payload (as framed by the Append* functions) and
// returns one of *Enqueue, *Ack, *Requeue, *QCreate, *QDelete.
func Decode(p []byte) (any, error) {
	if len(p) == 0 {
		return nil, errors.New("wal: empty payload")
	}
	r := &rd{b: p, off: 1}
	switch p[0] {
	case TypeEnqueue:
		e := &Enqueue{
			QueueID:  r.u32(),
			MsgID:    r.u64(),
			OriginID: r.u64(),
			Priority: r.u8(),
		}
		e.EnqueuedAt = r.i64()
		e.VisibleAt = r.i64()
		e.Body = r.take(int(r.u32()))
		return e, r.done()
	case TypeAck:
		a := &Ack{QueueID: r.u32(), MsgID: r.u64()}
		return a, r.done()
	case TypeRequeue:
		q := &Requeue{QueueID: r.u32(), MsgID: r.u64()}
		q.VisibleAt = r.i64()
		q.Deliveries = r.u32()
		q.Reason = r.u8()
		return q, r.done()
	case TypeQCreate:
		c := &QCreate{QueueID: r.u32()}
		c.Name = string(r.take(int(r.u16())))
		c.Cfg = r.take(int(r.u32()))
		return c, r.done()
	case TypeQDelete:
		d := &QDelete{QueueID: r.u32()}
		return d, r.done()
	default:
		return nil, fmt.Errorf("wal: unknown record type 0x%02x", p[0])
	}
}

// Scan walks consecutive frames in buf, calling fn for each payload whose CRC
// verifies, and returns the number of bytes covered by valid frames. Scanning
// stops - without error - at a short tail, a zero length prefix (a zero-filled
// region from a crash must read as end-of-log, not as an endless stream of
// empty records), an oversized or over-long length, or a CRC mismatch. The
// caller compares the returned offset against the buffer length to classify
// clean end vs torn tail. An error from fn aborts the scan and is returned.
func Scan(buf []byte, fn func(payload []byte) error) (int64, error) {
	off := 0
	for {
		if len(buf)-off < frameHeader {
			return int64(off), nil
		}
		n := int(binary.LittleEndian.Uint32(buf[off:]))
		if n == 0 || n > MaxPayload || n > len(buf)-off-frameHeader {
			return int64(off), nil
		}
		crc := binary.LittleEndian.Uint32(buf[off+4:])
		p := buf[off+frameHeader : off+frameHeader+n]
		if crc32.Checksum(p, castagnoli) != crc {
			return int64(off), nil
		}
		if err := fn(p); err != nil {
			return int64(off), err
		}
		off += frameHeader + n
	}
}
