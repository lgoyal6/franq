package wal

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

func TestRecordRoundtrip(t *testing.T) {
	records := []any{
		&Enqueue{QueueID: 7, MsgID: 42, OriginID: 9, Priority: 200, EnqueuedAt: 1754000000000001, VisibleAt: 1754000000500000, Body: []byte("hello world")},
		&Enqueue{QueueID: 1, MsgID: 1, Body: nil},
		&Ack{QueueID: 3, MsgID: 99},
		&Requeue{QueueID: 3, MsgID: 99, VisibleAt: 1754000001000000, Deliveries: 4, Reason: ReasonLeaseExpired},
		&QCreate{QueueID: 12, Name: "orders", Cfg: []byte(`{"order":"lifo","aging_rate":2.5}`)},
		&QDelete{QueueID: 12},
	}
	var log []byte
	for _, r := range records {
		switch v := r.(type) {
		case *Enqueue:
			log = AppendEnqueue(log, v)
		case *Ack:
			log = AppendAck(log, v)
		case *Requeue:
			log = AppendRequeue(log, v)
		case *QCreate:
			log = AppendQCreate(log, v)
		case *QDelete:
			log = AppendQDelete(log, v)
		}
	}
	var got []any
	n, err := Scan(log, func(p []byte) error {
		rec, err := Decode(p)
		if err != nil {
			return err
		}
		got = append(got, rec)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(len(log)) {
		t.Fatalf("scan covered %d of %d bytes", n, len(log))
	}
	if len(got) != len(records) {
		t.Fatalf("decoded %d records, want %d", len(got), len(records))
	}
	for i := range records {
		if !reflect.DeepEqual(records[i], got[i]) {
			t.Errorf("record %d: got %+v want %+v", i, got[i], records[i])
		}
	}
}

func buildLog(t *testing.T, n int) []byte {
	t.Helper()
	var log []byte
	for i := range n {
		log = AppendEnqueue(log, &Enqueue{QueueID: 1, MsgID: uint64(i + 1), EnqueuedAt: int64(1000 + i), Body: bytes.Repeat([]byte{byte(i)}, 20+i)})
	}
	return log
}

func countRecords(buf []byte) (int64, int) {
	count := 0
	n, _ := Scan(buf, func([]byte) error { count++; return nil })
	return n, count
}

func TestScanTruncationAtEveryOffset(t *testing.T) {
	full := buildLog(t, 3)
	wholeLen, whole := countRecords(full)
	if whole != 3 || wholeLen != int64(len(full)) {
		t.Fatalf("baseline scan broken: %d records, %d bytes", whole, wholeLen)
	}
	// Find start of the last record by scanning the first two.
	var offsets []int64
	Scan(full, func(p []byte) error {
		return nil
	})
	// Recompute frame boundaries manually.
	boundaries := []int64{0}
	off := int64(0)
	for off < int64(len(full)) {
		n := int64(uint32(full[off]) | uint32(full[off+1])<<8 | uint32(full[off+2])<<16 | uint32(full[off+3])<<24)
		off += frameHeader + n
		boundaries = append(boundaries, off)
	}
	lastStart := boundaries[len(boundaries)-2]
	_ = offsets
	// Truncating anywhere inside the last record must recover exactly the
	// first two records, never an error, never a partial third.
	for cut := lastStart + 1; cut < int64(len(full)); cut++ {
		n, count := countRecords(full[:cut])
		if count != 2 || n != lastStart {
			t.Fatalf("cut at %d: got %d records to byte %d, want 2 records to byte %d", cut, count, n, lastStart)
		}
	}
}

func TestScanBitFlip(t *testing.T) {
	full := buildLog(t, 3)
	for _, i := range []int{len(full) - 1, len(full) - 10} {
		corrupt := append([]byte(nil), full...)
		corrupt[i] ^= 0x40
		_, count := countRecords(corrupt)
		if count != 2 {
			t.Fatalf("bit flip at %d: got %d records, want 2", i, count)
		}
	}
}

func TestScanZeroFilledTail(t *testing.T) {
	full := buildLog(t, 2)
	padded := append(append([]byte(nil), full...), make([]byte, 4096)...)
	n, count := countRecords(padded)
	if count != 2 || n != int64(len(full)) {
		t.Fatalf("zero tail: %d records to byte %d, want 2 records to byte %d", count, n, len(full))
	}
}

func TestScanGarbageTail(t *testing.T) {
	full := buildLog(t, 2)
	garbage := append(append([]byte(nil), full...), 0xDE, 0xAD, 0xBE, 0xEF, 0xFF, 0xFF, 0xFF, 0xFF, 1, 2, 3)
	n, count := countRecords(garbage)
	if count != 2 || n != int64(len(full)) {
		t.Fatalf("garbage tail: %d records to byte %d, want 2 records to byte %d", count, n, len(full))
	}
}

func TestScanOversizedLength(t *testing.T) {
	var buf []byte
	hdr := make([]byte, frameHeader)
	hdr[0] = 0xFF
	hdr[1] = 0xFF
	hdr[2] = 0xFF
	hdr[3] = 0x7F // ~2GiB length prefix
	buf = append(buf, hdr...)
	n, count := countRecords(buf)
	if count != 0 || n != 0 {
		t.Fatalf("oversized length parsed as valid: %d records, %d bytes", count, n)
	}
}

func TestScanFnErrorAborts(t *testing.T) {
	full := buildLog(t, 3)
	sentinel := errors.New("boom")
	calls := 0
	_, err := Scan(full, func([]byte) error {
		calls++
		if calls == 2 {
			return sentinel
		}
		return nil
	})
	if !errors.Is(err, sentinel) || calls != 2 {
		t.Fatalf("err=%v calls=%d, want sentinel at call 2", err, calls)
	}
}

func TestDecodeRejectsTrailingBytes(t *testing.T) {
	frame := AppendAck(nil, &Ack{QueueID: 1, MsgID: 2})
	payload := frame[frameHeader:]
	if _, err := Decode(append(payload, 0x00)); err == nil {
		t.Fatal("decode accepted trailing bytes")
	}
	if _, err := Decode([]byte{0x77}); err == nil {
		t.Fatal("decode accepted unknown type")
	}
	if _, err := Decode(nil); err == nil {
		t.Fatal("decode accepted empty payload")
	}
}
