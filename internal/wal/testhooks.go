//go:build testhooks

// Exposes a test-only fsync hook for WAL integration tests.
// The HTTP disconnect test pauses committer.go before durability completes.
// Normal builds exclude this file; tests restore the hook after logs stop.
package wal

import "os"

// SetSegmentSyncForTest replaces fsync only in explicitly tagged test binaries.
// Callers must serialize use, set it before submitting work, and restore only
// after every owning log has stopped. It is absent from normal release builds.
func SetSegmentSyncForTest(fn func(*os.File) error) func() {
	previous := segmentSync
	segmentSync = fn
	return func() { segmentSync = previous }
}
