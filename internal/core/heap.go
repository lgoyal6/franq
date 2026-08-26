package core

// msgHeap is a binary heap of messages with positional removal via
// message.heapIdx. One instance is used max-first for the ready set and
// min-first for the delay set - the less function decides.
type msgHeap struct {
	items []*message
	less  func(a, b *message) bool
}

func (h *msgHeap) len() int { return len(h.items) }

func (h *msgHeap) peek() *message {
	if len(h.items) == 0 {
		return nil
	}
	return h.items[0]
}

func (h *msgHeap) push(m *message) {
	m.heapIdx = len(h.items)
	h.items = append(h.items, m)
	h.up(m.heapIdx)
}

func (h *msgHeap) pop() *message {
	m := h.items[0]
	h.swap(0, len(h.items)-1)
	h.items = h.items[:len(h.items)-1]
	if len(h.items) > 0 {
		h.down(0)
	}
	m.heapIdx = -1
	return m
}

func (h *msgHeap) remove(m *message) {
	i := m.heapIdx
	last := len(h.items) - 1
	h.swap(i, last)
	h.items = h.items[:last]
	if i < last {
		h.down(i)
		h.up(i)
	}
	m.heapIdx = -1
}

func (h *msgHeap) swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].heapIdx = i
	h.items[j].heapIdx = j
}

func (h *msgHeap) up(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if !h.less(h.items[i], h.items[parent]) {
			return
		}
		h.swap(i, parent)
		i = parent
	}
}

func (h *msgHeap) down(i int) {
	n := len(h.items)
	for {
		l, r := 2*i+1, 2*i+2
		smallest := i
		if l < n && h.less(h.items[l], h.items[smallest]) {
			smallest = l
		}
		if r < n && h.less(h.items[r], h.items[smallest]) {
			smallest = r
		}
		if smallest == i {
			return
		}
		h.swap(i, smallest)
		i = smallest
	}
}

// leaseHeap is a min-heap of leases by deadline, with positional removal and
// re-fix (for Extend). No lazy deletion: ack/nack/expiry all remove directly.
type leaseHeap struct {
	items []*lease
}

func (h *leaseHeap) len() int { return len(h.items) }

func (h *leaseHeap) peek() *lease {
	if len(h.items) == 0 {
		return nil
	}
	return h.items[0]
}

func (h *leaseHeap) push(l *lease) {
	l.heapIdx = len(h.items)
	h.items = append(h.items, l)
	h.up(l.heapIdx)
}

func (h *leaseHeap) remove(l *lease) {
	i := l.heapIdx
	last := len(h.items) - 1
	h.swap(i, last)
	h.items = h.items[:last]
	if i < last {
		h.down(i)
		h.up(i)
	}
	l.heapIdx = -1
}

// fix restores heap order after l's deadline changed in place.
func (h *leaseHeap) fix(l *lease) {
	h.down(l.heapIdx)
	h.up(l.heapIdx)
}

func (h *leaseHeap) swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].heapIdx = i
	h.items[j].heapIdx = j
}

func (h *leaseHeap) up(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if !h.items[i].deadline.Before(h.items[parent].deadline) {
			return
		}
		h.swap(i, parent)
		i = parent
	}
}

func (h *leaseHeap) down(i int) {
	n := len(h.items)
	for {
		l, r := 2*i+1, 2*i+2
		smallest := i
		if l < n && h.items[l].deadline.Before(h.items[smallest].deadline) {
			smallest = l
		}
		if r < n && h.items[r].deadline.Before(h.items[smallest].deadline) {
			smallest = r
		}
		if smallest == i {
			return
		}
		h.swap(i, smallest)
		i = smallest
	}
}
