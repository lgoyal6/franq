package server

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/lgoyal6/franq/internal/core"
)

func (h *handler) createQueue(w http.ResponseWriter, r *http.Request) {
	var req createQueueReq
	if !decode(w, r, &req) {
		return
	}
	cfg := core.Config{
		Order:          req.Order,
		AgingRate:      req.AgingRate,
		MaxDeliveries:  req.MaxDeliveries,
		DefaultLeaseMS: req.DefaultLeaseMS,
	}
	if err := h.b.CreateQueue(req.Name, cfg); err != nil {
		writeCoreErr(w, err)
		return
	}
	q, _ := h.b.GetQueue(req.Name)
	writeJSON(w, http.StatusCreated, h.b.Stats(q))
}

func (h *handler) listQueues(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, listQueuesResp{Queues: h.b.ListStats()})
}

func (h *handler) getQueue(w http.ResponseWriter, r *http.Request) {
	q, ok := h.queue(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, h.b.Stats(q))
}

func (h *handler) deleteQueue(w http.ResponseWriter, r *http.Request) {
	if err := h.b.DeleteQueue(r.PathValue("q")); err != nil {
		writeCoreErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) enqueue(w http.ResponseWriter, r *http.Request) {
	q, ok := h.queue(w, r)
	if !ok {
		return
	}
	var req enqueueReq
	if !decode(w, r, &req) {
		return
	}
	if len(req.Messages) == 0 {
		writeErr(w, http.StatusBadRequest, "invalid", "messages must be non-empty")
		return
	}
	if len(req.Messages) > maxBatch {
		writeErr(w, http.StatusBadRequest, "invalid", "batch too large")
		return
	}
	items := make([]core.EnqueueItem, 0, len(req.Messages))
	for _, m := range req.Messages {
		if len(m.Body) > maxBodyBytes {
			writeErr(w, http.StatusBadRequest, "invalid", "body exceeds 1 MiB")
			return
		}
		if m.DelayMS < 0 || m.DelayMS > maxDelayMS {
			writeErr(w, http.StatusBadRequest, "invalid", "delay_ms out of range")
			return
		}
		items = append(items, core.EnqueueItem{Body: []byte(m.Body), Priority: m.Priority, DelayMS: m.DelayMS})
	}
	ids, err := h.b.Enqueue(q, items)
	if err != nil {
		writeCoreErr(w, err)
		return
	}
	resp := enqueueResp{IDs: make([]string, len(ids))}
	for i, id := range ids {
		resp.IDs[i] = strconv.FormatUint(id, 10)
	}
	writeJSON(w, http.StatusCreated, resp)
}

func (h *handler) dequeue(w http.ResponseWriter, r *http.Request) {
	q, ok := h.queue(w, r)
	if !ok {
		return
	}
	var req dequeueReq
	if !decode(w, r, &req) {
		return
	}
	if req.WaitMS < 0 || req.WaitMS > maxWaitMS {
		writeErr(w, http.StatusBadRequest, "invalid", "wait_ms out of range")
		return
	}
	ds, err := q.Dequeue(r.Context(),
		req.Max,
		time.Duration(req.WaitMS)*time.Millisecond,
		time.Duration(req.LeaseMS)*time.Millisecond)
	if err != nil {
		// Client went away or the server is shutting down: an empty
		// response is correct either way (nothing was leased).
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			writeJSON(w, http.StatusOK, dequeueResp{Messages: []msgResp{}})
			return
		}
		writeCoreErr(w, err)
		return
	}
	resp := dequeueResp{Messages: make([]msgResp, 0, len(ds))}
	for _, d := range ds {
		m := msgResp{
			ID:         strconv.FormatUint(d.ID, 10),
			Body:       string(d.Body),
			Priority:   d.Priority,
			Deliveries: d.Deliveries,
			EnqueuedAt: time.UnixMicro(d.EnqueuedAt).UTC().Format(time.RFC3339Nano),
			Token:      d.Token,
		}
		if d.OriginID != 0 {
			m.OriginID = strconv.FormatUint(d.OriginID, 10)
		}
		resp.Messages = append(resp.Messages, m)
	}
	writeJSON(w, http.StatusOK, resp)
}

func parseID(s string) (uint64, bool) {
	id, err := strconv.ParseUint(s, 10, 64)
	return id, err == nil && id != 0
}

func (h *handler) ack(w http.ResponseWriter, r *http.Request) {
	q, ok := h.queue(w, r)
	if !ok {
		return
	}
	var req ackReq
	if !decode(w, r, &req) {
		return
	}
	if len(req.Acks) == 0 || len(req.Acks) > maxBatch {
		writeErr(w, http.StatusBadRequest, "invalid", "acks must contain 1..500 items")
		return
	}
	ids := make([]uint64, len(req.Acks))
	tokens := make([]string, len(req.Acks))
	for i, a := range req.Acks {
		id, ok := parseID(a.ID)
		if !ok {
			writeErr(w, http.StatusBadRequest, "invalid", "bad message id "+a.ID)
			return
		}
		ids[i], tokens[i] = id, a.Token
	}
	errs := h.b.AckBatch(q, ids, tokens)
	resp := ackResp{Results: make([]ackResult, len(ids))}
	allOK := true
	for i, e := range errs {
		res := ackResult{ID: req.Acks[i].ID, OK: e == nil}
		if e != nil {
			allOK = false
			res.Error = e.Error()
		}
		resp.Results[i] = res
	}
	status := http.StatusOK
	if !allOK {
		status = http.StatusConflict
	}
	writeJSON(w, status, resp)
}

func (h *handler) nack(w http.ResponseWriter, r *http.Request) {
	q, ok := h.queue(w, r)
	if !ok {
		return
	}
	var req nackReq
	if !decode(w, r, &req) {
		return
	}
	id, okID := parseID(req.ID)
	if !okID {
		writeErr(w, http.StatusBadRequest, "invalid", "bad message id")
		return
	}
	if req.DelayMS < 0 || req.DelayMS > maxDelayMS {
		writeErr(w, http.StatusBadRequest, "invalid", "delay_ms out of range")
		return
	}
	if err := q.Nack(id, req.Token, req.DelayMS); err != nil {
		writeCoreErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) extend(w http.ResponseWriter, r *http.Request) {
	q, ok := h.queue(w, r)
	if !ok {
		return
	}
	var req extendReq
	if !decode(w, r, &req) {
		return
	}
	id, okID := parseID(req.ID)
	if !okID {
		writeErr(w, http.StatusBadRequest, "invalid", "bad message id")
		return
	}
	if req.LeaseMS < 100 || req.LeaseMS > 3_600_000 {
		writeErr(w, http.StatusBadRequest, "invalid", "lease_ms must be in [100, 3600000]")
		return
	}
	if err := q.Extend(id, req.Token, req.LeaseMS); err != nil {
		writeCoreErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) replay(w http.ResponseWriter, r *http.Request) {
	q, ok := h.queue(w, r)
	if !ok {
		return
	}
	var req replayReq
	if !decode(w, r, &req) {
		return
	}
	var from, to time.Time
	var err error
	if req.From != "" {
		if from, err = time.Parse(time.RFC3339, req.From); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid", "from: "+err.Error())
			return
		}
	}
	if req.To != "" {
		if to, err = time.Parse(time.RFC3339, req.To); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid", "to: "+err.Error())
			return
		}
	}
	res, err := h.b.Replay(q, from, to, req.Limit)
	if err != nil {
		writeCoreErr(w, err)
		return
	}
	resp := replayResp{Replayed: res.Replayed}
	if !res.WindowStart.IsZero() {
		resp.WindowStart = res.WindowStart.UTC().Format(time.RFC3339Nano)
	}
	writeJSON(w, http.StatusOK, resp)
}
