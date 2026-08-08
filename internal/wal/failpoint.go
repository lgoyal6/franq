package wal

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

// Failpoints let the crash-test harness kill the process at exact points in
// the storage engine. FRANQ_FAILPOINT=name:N exits with code 137 on the Nth
// hit of that named point. Zero overhead when unset beyond one string compare.
var (
	fpMu    sync.Mutex
	fpName  string
	fpAfter int
	fpCount int
)

func init() {
	v := os.Getenv("FRANQ_FAILPOINT")
	if v == "" {
		return
	}
	name, nStr, ok := strings.Cut(v, ":")
	n := 1
	if ok {
		if parsed, err := strconv.Atoi(nStr); err == nil && parsed > 0 {
			n = parsed
		}
	}
	fpName, fpAfter = name, n
}

func failpoint(name string) {
	if fpName == "" || fpName != name {
		return
	}
	fpMu.Lock()
	fpCount++
	hit := fpCount == fpAfter
	fpMu.Unlock()
	if hit {
		fmt.Fprintf(os.Stderr, "franq: failpoint %q hit, dying\n", name)
		os.Exit(137)
	}
}
