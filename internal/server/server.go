package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/lgoyal6/franq/internal/core"
)

// Request guardrails.
const (
	maxRequestBytes = 4 << 20
	maxBatch        = 500
	maxBodyBytes    = 1 << 20
	maxWaitMS       = 60_000
	maxDelayMS      = 7 * 24 * 3600 * 1000
)

type handler struct {
	b *core.Broker
}

// New returns the HTTP API for a broker.
func New(b *core.Broker) http.Handler {
	h := &handler{b: b}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/queues", h.createQueue)
	mux.HandleFunc("GET /v1/queues", h.listQueues)
	mux.HandleFunc("GET /v1/queues/{q}", h.getQueue)
	mux.HandleFunc("DELETE /v1/queues/{q}", h.deleteQueue)
	mux.HandleFunc("POST /v1/queues/{q}/messages", h.enqueue)
	mux.HandleFunc("POST /v1/queues/{q}/dequeue", h.dequeue)
	mux.HandleFunc("POST /v1/queues/{q}/ack", h.ack)
	mux.HandleFunc("POST /v1/queues/{q}/nack", h.nack)
	mux.HandleFunc("POST /v1/queues/{q}/extend", h.extend)
	mux.HandleFunc("POST /v1/queues/{q}/replay", h.replay)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok\n"))
	})
	return recoverPanics(mux)
}

func recoverPanics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if p := recover(); p != nil {
				fmt.Fprintf(os.Stderr, "franq: panic serving %s %s: %v\n", r.Method, r.URL.Path, p)
				writeErr(w, http.StatusInternalServerError, "internal", "internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, errorResp{Error: msg, Code: code})
}

// writeCoreErr maps broker errors onto HTTP statuses.
func writeCoreErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, core.ErrQueueNotFound):
		writeErr(w, http.StatusNotFound, "queue_not_found", err.Error())
	case errors.Is(err, core.ErrQueueExists):
		writeErr(w, http.StatusConflict, "queue_exists", err.Error())
	case errors.Is(err, core.ErrLeaseLost):
		writeErr(w, http.StatusConflict, "lease_lost", err.Error())
	case errors.Is(err, core.ErrNameReserved):
		writeErr(w, http.StatusBadRequest, "name_reserved", err.Error())
	case errors.Is(err, core.ErrInvalid):
		writeErr(w, http.StatusBadRequest, "invalid", err.Error())
	default:
		writeErr(w, http.StatusInternalServerError, "internal", err.Error())
	}
}

// decode parses a JSON body with a size cap. An empty body decodes into the
// zero value so POSTs with all-default fields can omit the body entirely.
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(v); err != nil {
		if errors.Is(err, io.EOF) {
			return true
		}
		writeErr(w, http.StatusBadRequest, "bad_json", "invalid JSON: "+err.Error())
		return false
	}
	return true
}

// queue resolves the {q} path parameter or writes a 404.
func (h *handler) queue(w http.ResponseWriter, r *http.Request) (*core.Queue, bool) {
	q, ok := h.b.GetQueue(r.PathValue("q"))
	if !ok {
		writeErr(w, http.StatusNotFound, "queue_not_found", "queue not found")
		return nil, false
	}
	return q, true
}
