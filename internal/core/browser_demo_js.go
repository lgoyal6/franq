//go:build js && wasm

package core

import "fmt"

// BrowserMessage is a synthetic scheduling input, not a durable enqueue.
type BrowserMessage struct {
	Label    string `json:"label"`
	Priority uint8  `json:"priority"`
	StartMS  int64  `json:"start_ms"`
	DelayMS  int64  `json:"delay_ms"`
}

// BrowserOrder uses the native heap comparator and aging key without a broker or WAL.
func BrowserOrder(messages []BrowserMessage, order string, aging float64, nowMS int64) ([]string, int, error) {
	if len(messages) > 24 || aging < 0 || aging > 20 || nowMS < 0 || nowMS > 60000 {
		return nil, 0, fmt.Errorf("Use at most 24 jobs, aging 0-20, and time 0-60000 ms")
	}
	cfg := Config{Order: order, AgingRate: aging}
	if err := cfg.normalize(); err != nil {
		return nil, 0, err
	}
	q := newQueue(nil, 1, "browser", cfg)
	waiting := 0
	for i, m := range messages {
		if len(m.Label) > 80 || m.StartMS < 0 || m.StartMS > 60000 || m.DelayMS < 0 || m.DelayMS > 60000 {
			return nil, 0, fmt.Errorf("Job label max 80 bytes; times 0-60000 ms")
		}
		visible := (m.StartMS + m.DelayMS) * 1000
		if visible > nowMS*1000 {
			waiting++
			continue
		}
		q.ready.push(&message{id: uint64(i + 1), body: []byte(m.Label), key: effKey(m.Priority, aging, ageStart(m.StartMS*1000, visible))})
	}
	result := []string{}
	for q.ready.len() > 0 {
		result = append(result, string(q.ready.pop().body))
	}
	return result, waiting, nil
}
