package wal

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	segMagic     = "FRQWAL01"
	segVersion   = 1
	segHeaderLen = 16
	segSuffix    = ".seg"
)

func segmentName(id uint64) string { return fmt.Sprintf("%012d%s", id, segSuffix) }

func parseSegmentName(name string) (uint64, bool) {
	base, ok := strings.CutSuffix(name, segSuffix)
	if !ok || len(base) != 12 {
		return 0, false
	}
	id, err := strconv.ParseUint(base, 10, 64)
	if err != nil {
		return 0, false
	}
	return id, true
}

// listSegments returns segment ids in dir, ascending.
func listSegments(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var ids []uint64
	for _, e := range entries {
		if id, ok := parseSegmentName(e.Name()); ok {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

// createSegment creates a new segment with a synced header and syncs the
// directory so the file itself survives a crash.
func createSegment(dir string, id uint64) (*os.File, error) {
	path := filepath.Join(dir, segmentName(id))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	hdr := make([]byte, segHeaderLen)
	copy(hdr, segMagic)
	binary.LittleEndian.PutUint32(hdr[8:], segVersion)
	if _, err := f.Write(hdr); err != nil {
		f.Close()
		return nil, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return nil, err
	}
	if err := fsyncDir(dir); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

// readSegment reads a whole segment and returns its record region (after the
// validated header). Segments are bounded by SegmentBytes so this stays cheap.
func readSegment(path string) ([]byte, error) {
	buf, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(buf) < segHeaderLen {
		return nil, fmt.Errorf("wal: segment %s shorter than header", filepath.Base(path))
	}
	if string(buf[:8]) != segMagic {
		return nil, fmt.Errorf("wal: segment %s has bad magic", filepath.Base(path))
	}
	if v := binary.LittleEndian.Uint32(buf[8:]); v != segVersion {
		return nil, fmt.Errorf("wal: segment %s has unsupported version %d", filepath.Base(path), v)
	}
	return buf[segHeaderLen:], nil
}

func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
