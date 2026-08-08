// Package client is a thin Go client for the franq HTTP API, shared by the
// igor CLI, the end-to-end tests and the crash harness.
package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type Client struct {
	base string
	hc   *http.Client
}

func New(base string) *Client {
	return &Client{
		base: strings.TrimRight(base, "/"),
		// Longer than the 60s long-poll cap so waits never die client-side.
		hc: &http.Client{Timeout: 90 * time.Second},
	}
}

// APIError is a non-2xx response, decoded.
type APIError struct {
	Status int
	Code   string
	Msg    string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("franq: %s (%s, http %d)", e.Msg, e.Code, e.Status)
}

func (c *Client) do(method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		buf, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(buf)
	}
	req, err := http.NewRequest(method, c.base+path, body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		var e struct {
			Error string `json:"error"`
			Code  string `json:"code"`
		}
		json.Unmarshal(raw, &e)
		if e.Error == "" {
			e.Error = strings.TrimSpace(string(raw))
		}
		return &APIError{Status: resp.StatusCode, Code: e.Code, Msg: e.Error}
	}
	if out != nil && len(raw) > 0 {
		return json.Unmarshal(raw, out)
	}
	return nil
}

// IsCode reports whether err is an APIError with the given code.
func IsCode(err error, code string) bool {
	if e, ok := err.(*APIError); ok {
		return e.Code == code
	}
	return false
}

// ---- types mirroring the server DTOs ----

type QueueConfig struct {
	Name           string  `json:"name"`
	Order          string  `json:"order,omitempty"`
	AgingRate      float64 `json:"aging_rate,omitempty"`
	MaxDeliveries  uint32  `json:"max_deliveries,omitempty"`
	DefaultLeaseMS int64   `json:"default_lease_ms,omitempty"`
}

type Stats struct {
	Name           string  `json:"name"`
	Order          string  `json:"order"`
	AgingRate      float64 `json:"aging_rate"`
	MaxDeliveries  uint32  `json:"max_deliveries"`
	DefaultLeaseMS int64   `json:"default_lease_ms"`
	Ready          int     `json:"ready"`
	Delayed        int     `json:"delayed"`
	InFlight       int     `json:"in_flight"`
	TotalEnqueued  uint64  `json:"total_enqueued"`
	TotalAcked     uint64  `json:"total_acked"`
	TotalRequeued  uint64  `json:"total_requeued"`
	TotalDLQ       uint64  `json:"total_dlq"`
	DLQDepth       int     `json:"dlq_depth"`
}

type EnqueueMsg struct {
	Body     string `json:"body"`
	Priority uint8  `json:"priority,omitempty"`
	DelayMS  int64  `json:"delay_ms,omitempty"`
}

type Msg struct {
	ID         string `json:"id"`
	OriginID   string `json:"origin_id,omitempty"`
	Body       string `json:"body"`
	Priority   uint8  `json:"priority"`
	Deliveries uint32 `json:"deliveries"`
	EnqueuedAt string `json:"enqueued_at"`
	Token      string `json:"token"`
}

type AckResult struct {
	ID    string `json:"id"`
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

type ReplayResult struct {
	Replayed    int    `json:"replayed"`
	WindowStart string `json:"window_start,omitempty"`
}

// ---- API surface ----

func (c *Client) Health() error {
	return c.do("GET", "/healthz", nil, nil)
}

func (c *Client) CreateQueue(cfg QueueConfig) error {
	return c.do("POST", "/v1/queues", cfg, nil)
}

func (c *Client) DeleteQueue(name string) error {
	return c.do("DELETE", "/v1/queues/"+name, nil, nil)
}

func (c *Client) Stats(name string) (Stats, error) {
	var s Stats
	err := c.do("GET", "/v1/queues/"+name, nil, &s)
	return s, err
}

func (c *Client) ListQueues() ([]Stats, error) {
	var resp struct {
		Queues []Stats `json:"queues"`
	}
	err := c.do("GET", "/v1/queues", nil, &resp)
	return resp.Queues, err
}

func (c *Client) Enqueue(queue string, msgs []EnqueueMsg) ([]string, error) {
	var resp struct {
		IDs []string `json:"ids"`
	}
	err := c.do("POST", "/v1/queues/"+queue+"/messages", map[string]any{"messages": msgs}, &resp)
	return resp.IDs, err
}

func (c *Client) Dequeue(queue string, max int, waitMS, leaseMS int64) ([]Msg, error) {
	var resp struct {
		Messages []Msg `json:"messages"`
	}
	err := c.do("POST", "/v1/queues/"+queue+"/dequeue",
		map[string]any{"max": max, "wait_ms": waitMS, "lease_ms": leaseMS}, &resp)
	return resp.Messages, err
}

func (c *Client) Ack(queue, id, token string) error {
	var resp struct {
		Results []AckResult `json:"results"`
	}
	err := c.do("POST", "/v1/queues/"+queue+"/ack",
		map[string]any{"acks": []map[string]string{{"id": id, "token": token}}}, &resp)
	if err != nil {
		if e, ok := err.(*APIError); ok && e.Status == 409 && len(resp.Results) > 0 {
			return err
		}
		return err
	}
	if len(resp.Results) == 1 && !resp.Results[0].OK {
		return fmt.Errorf("ack %s: %s", id, resp.Results[0].Error)
	}
	return nil
}

func (c *Client) Nack(queue, id, token string, delayMS int64) error {
	return c.do("POST", "/v1/queues/"+queue+"/nack",
		map[string]any{"id": id, "token": token, "delay_ms": delayMS}, nil)
}

func (c *Client) Extend(queue, id, token string, leaseMS int64) error {
	return c.do("POST", "/v1/queues/"+queue+"/extend",
		map[string]any{"id": id, "token": token, "lease_ms": leaseMS}, nil)
}

func (c *Client) Replay(queue, from, to string, limit int) (ReplayResult, error) {
	var res ReplayResult
	err := c.do("POST", "/v1/queues/"+queue+"/replay",
		map[string]any{"from": from, "to": to, "limit": limit}, &res)
	return res, err
}
