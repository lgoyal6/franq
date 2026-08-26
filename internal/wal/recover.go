package wal

import (
	"fmt"
	"os"
	"path/filepath"
)

// RecoverResult reports what recovery found.
type RecoverResult struct {
	Snap      *Snapshot // nil if starting fresh
	LastSeg   uint64    // highest segment id present (or covered by the snapshot)
	Records   int       // tail records replayed
	TornTails int       // 1 if the last segment needed truncation
}

// Recover rebuilds state: newest valid snapshot first (via applySnap), then
// every segment after it in order (via applyRec, one decoded record at a
// time). A torn tail in the LAST segment is truncated - safe because nothing
// is externally observable before its record is fsynced, so a truncated tail
// only ever contains operations no client was told succeeded. Damage anywhere
// else is real corruption and refuses startup.
func Recover(walDir, snapDir string, applySnap func(*Snapshot) error, applyRec func(any) error) (*RecoverResult, error) {
	res := &RecoverResult{}

	snap, err := LoadLatestSnapshot(snapDir)
	if err != nil {
		return nil, err
	}
	if snap != nil {
		if err := applySnap(snap); err != nil {
			return nil, err
		}
		res.Snap = snap
		res.LastSeg = snap.LastSeg
	}

	ids, err := listSegments(walDir)
	if err != nil {
		return nil, err
	}
	var replay []uint64
	for _, id := range ids {
		if snap == nil || id > snap.LastSeg {
			replay = append(replay, id)
		}
	}
	for i, id := range replay {
		if i > 0 && id != replay[i-1]+1 {
			return nil, fmt.Errorf("wal: segment gap: %d then %d", replay[i-1], id)
		}
		path := filepath.Join(walDir, segmentName(id))
		content, err := readSegment(path)
		if err != nil {
			return nil, err
		}
		valid, err := Scan(content, func(p []byte) error {
			rec, err := Decode(p)
			if err != nil {
				return fmt.Errorf("wal: segment %d: %w", id, err)
			}
			res.Records++
			return applyRec(rec)
		})
		if err != nil {
			return nil, err
		}
		if valid < int64(len(content)) {
			last := i == len(replay)-1
			if !last {
				return nil, fmt.Errorf("wal: corruption inside sealed segment %d at byte %d", id, segHeaderLen+valid)
			}
			fmt.Fprintf(os.Stderr, "franq: truncating torn tail of segment %d at byte %d (%d bytes dropped)\n",
				id, segHeaderLen+valid, int64(len(content))-valid)
			if err := os.Truncate(path, segHeaderLen+valid); err != nil {
				return nil, err
			}
			res.TornTails++
		}
		if id > res.LastSeg {
			res.LastSeg = id
		}
	}
	return res, nil
}
