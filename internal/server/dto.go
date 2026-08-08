// Package server exposes the broker over HTTP+JSON (stdlib only).
//
// Conventions: message ids are decimal strings (uint64 overflows JS numbers);
// bodies are JSON strings (text payloads; base64 binary yourself); delays are
// relative (delay_ms) so client clocks are never trusted.
package server

import "github.com/lgoyal6/franq/internal/core"

type errorResp struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

type createQueueReq struct {
	Name           string  `json:"name"`
	Order          string  `json:"order,omitempty"`
	AgingRate      float64 `json:"aging_rate,omitempty"`
	MaxDeliveries  uint32  `json:"max_deliveries,omitempty"`
	DefaultLeaseMS int64   `json:"default_lease_ms,omitempty"`
}

type listQueuesResp struct {
	Queues []core.Stats `json:"queues"`
}

type enqueueMsg struct {
	Body     string `json:"body"`
	Priority uint8  `json:"priority,omitempty"`
	DelayMS  int64  `json:"delay_ms,omitempty"`
}

type enqueueReq struct {
	Messages []enqueueMsg `json:"messages"`
}

type enqueueResp struct {
	IDs []string `json:"ids"`
}

type dequeueReq struct {
	Max     int   `json:"max,omitempty"`
	WaitMS  int64 `json:"wait_ms,omitempty"`
	LeaseMS int64 `json:"lease_ms,omitempty"`
}

type msgResp struct {
	ID         string `json:"id"`
	OriginID   string `json:"origin_id,omitempty"`
	Body       string `json:"body"`
	Priority   uint8  `json:"priority"`
	Deliveries uint32 `json:"deliveries"`
	EnqueuedAt string `json:"enqueued_at"`
	Token      string `json:"token"`
}

type dequeueResp struct {
	Messages []msgResp `json:"messages"`
}

type ackItem struct {
	ID    string `json:"id"`
	Token string `json:"token"`
}

type ackReq struct {
	Acks []ackItem `json:"acks"`
}

type ackResult struct {
	ID    string `json:"id"`
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

type ackResp struct {
	Results []ackResult `json:"results"`
}

type nackReq struct {
	ID      string `json:"id"`
	Token   string `json:"token"`
	DelayMS int64  `json:"delay_ms,omitempty"`
}

type extendReq struct {
	ID      string `json:"id"`
	Token   string `json:"token"`
	LeaseMS int64  `json:"lease_ms"`
}

type replayReq struct {
	From  string `json:"from,omitempty"` // RFC3339; empty = beginning of retained history
	To    string `json:"to,omitempty"`   // RFC3339; empty = now
	Limit int    `json:"limit,omitempty"`
}

type replayResp struct {
	Replayed    int    `json:"replayed"`
	WindowStart string `json:"window_start,omitempty"`
}
