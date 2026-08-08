package core

// effKey is the ready-heap ordering key, and the reason franq has no
// FIFO/LIFO/priority/delay code paths — only this number and a seq tie-break.
//
// Linear priority aging says a message's effective priority at time t is
// p + λ·(t − start). Comparing two messages:
//
//	p1 + λ(t−s1) > p2 + λ(t−s2)  ⇔  p1 − λ·s1 > p2 − λ·s2
//
// t cancels, so the key p − λ·start is STATIC: exact continuous aging with
// zero re-heapification. start is when the message became visible (not when
// it was enqueued), so a delayed message doesn't arrive pre-aged and jump the
// line. With λ = 0 the key degenerates to plain priority; with all priorities
// equal it degenerates to pure FIFO/LIFO via the seq tie-break.
//
// The key must be stable across restarts (it's recomputed from persisted
// fields during recovery), so start is absolute unix time, never
// process-relative. float64 precision is ample: at unix-seconds magnitude
// (~1.7e9) the ulp is ~4e-7, far below both a priority step of 1 and any
// meaningful λ·Δt.
func effKey(priority uint8, agingRate float64, startMicros int64) float64 {
	return float64(priority) - agingRate*(float64(startMicros)/1e6)
}

// ageStart picks the aging clock origin: visibility time, falling back to
// enqueue time for immediately-visible messages (visibleAt == 0 encoding).
func ageStart(enqueuedAt, visibleAt int64) int64 {
	if visibleAt > enqueuedAt {
		return visibleAt
	}
	return enqueuedAt
}
