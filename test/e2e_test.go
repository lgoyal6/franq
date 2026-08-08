// Package franqtest runs the full stack — broker, WAL, HTTP server, client —
// in-process against real sockets and real files.
package franqtest

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lgoyal6/franq/internal/client"
	"github.com/lgoyal6/franq/internal/core"
	"github.com/lgoyal6/franq/internal/server"
)

type stack struct {
	b    *core.Broker
	srv  *http.Server
	c    *client.Client
	addr string
}

func startStack(t *testing.T, dir string, opts core.Options) *stack {
	t.Helper()
	b, err := core.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	rootCtx, cancel := context.WithCancel(context.Background())
	srv := &http.Server{
		Handler:     server.New(b),
		BaseContext: func(net.Listener) context.Context { return rootCtx },
	}
	go srv.Serve(ln)
	s := &stack{b: b, srv: srv, c: client.New("http://" + ln.Addr().String()), addr: ln.Addr().String()}
	t.Cleanup(func() {
		cancel()
		s.stop(t)
	})
	_ = cancel
	return s
}

func (s *stack) stop(t *testing.T) {
	t.Helper()
	if s.srv != nil {
		ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
		s.srv.Shutdown(ctx)
		done()
		s.srv = nil
		s.b.Close()
	}
}

func TestE2EBasicFlow(t *testing.T) {
	s := startStack(t, t.TempDir(), core.Options{})
	if err := s.c.Health(); err != nil {
		t.Fatal(err)
	}
	if err := s.c.CreateQueue(client.QueueConfig{Name: "jobs", Order: "fifo"}); err != nil {
		t.Fatal(err)
	}
	if err := s.c.CreateQueue(client.QueueConfig{Name: "jobs"}); !client.IsCode(err, "queue_exists") {
		t.Fatalf("dup create: %v", err)
	}
	ids, err := s.c.Enqueue("jobs", []client.EnqueueMsg{
		{Body: "routine-1", Priority: 0},
		{Body: "urgent", Priority: 9},
		{Body: "routine-2", Priority: 0},
	})
	if err != nil || len(ids) != 3 {
		t.Fatalf("enqueue: %v %v", ids, err)
	}
	st, err := s.c.Stats("jobs")
	if err != nil || st.Ready != 3 {
		t.Fatalf("stats: %+v %v", st, err)
	}
	msgs, err := s.c.Dequeue("jobs", 10, 0, 60_000)
	if err != nil || len(msgs) != 3 {
		t.Fatalf("dequeue: %v %v", msgs, err)
	}
	if msgs[0].Body != "urgent" || msgs[1].Body != "routine-1" || msgs[2].Body != "routine-2" {
		t.Fatalf("priority-FIFO order wrong: %s, %s, %s", msgs[0].Body, msgs[1].Body, msgs[2].Body)
	}
	for _, m := range msgs {
		if err := s.c.Ack("jobs", m.ID, m.Token); err != nil {
			t.Fatal(err)
		}
	}
	// Double-ack must be rejected with a lease error.
	if err := s.c.Ack("jobs", msgs[0].ID, msgs[0].Token); err == nil {
		t.Fatal("double ack accepted")
	}
	st, _ = s.c.Stats("jobs")
	if st.Ready != 0 || st.InFlight != 0 || st.TotalAcked != 3 {
		t.Fatalf("post-ack stats: %+v", st)
	}
}

func TestE2ELongPollWakeup(t *testing.T) {
	s := startStack(t, t.TempDir(), core.Options{})
	if err := s.c.CreateQueue(client.QueueConfig{Name: "lp"}); err != nil {
		t.Fatal(err)
	}
	type result struct {
		msgs    []client.Msg
		elapsed time.Duration
		err     error
	}
	ch := make(chan result, 1)
	go func() {
		start := time.Now()
		msgs, err := s.c.Dequeue("lp", 1, 10_000, 60_000)
		ch <- result{msgs, time.Since(start), err}
	}()
	time.Sleep(300 * time.Millisecond)
	if _, err := s.c.Enqueue("lp", []client.EnqueueMsg{{Body: "wake up"}}); err != nil {
		t.Fatal(err)
	}
	r := <-ch
	if r.err != nil || len(r.msgs) != 1 {
		t.Fatalf("long poll: %v %v", r.msgs, r.err)
	}
	// The poller must be woken by the enqueue, not by its 10s timeout.
	if r.elapsed > 3*time.Second {
		t.Fatalf("long poll took %v — waiter never woke", r.elapsed)
	}
}

func TestE2ERestartDurability(t *testing.T) {
	dir := t.TempDir()
	s := startStack(t, dir, core.Options{})
	if err := s.c.CreateQueue(client.QueueConfig{Name: "durable", Order: "lifo", AgingRate: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.c.Enqueue("durable", []client.EnqueueMsg{{Body: "survivor"}, {Body: "victim"}}); err != nil {
		t.Fatal(err)
	}
	msgs, _ := s.c.Dequeue("durable", 1, 0, 60_000)
	if len(msgs) != 1 {
		t.Fatal("no delivery")
	}
	if err := s.c.Ack("durable", msgs[0].ID, msgs[0].Token); err != nil {
		t.Fatal(err)
	}
	acked := msgs[0].Body
	s.stop(t)

	s2 := startStack(t, dir, core.Options{})
	st, err := s2.c.Stats("durable")
	if err != nil {
		t.Fatal(err)
	}
	if st.Order != "lifo" || st.AgingRate != 1 {
		t.Fatalf("config lost across restart: %+v", st)
	}
	if st.Ready != 1 {
		t.Fatalf("expected 1 survivor, stats: %+v", st)
	}
	msgs, _ = s2.c.Dequeue("durable", 10, 0, 60_000)
	if len(msgs) != 1 || msgs[0].Body == acked {
		t.Fatalf("wrong survivor: %+v (acked %q)", msgs, acked)
	}
}

func TestE2EReplayEndpoint(t *testing.T) {
	s := startStack(t, t.TempDir(), core.Options{})
	if err := s.c.CreateQueue(client.QueueConfig{Name: "history"}); err != nil {
		t.Fatal(err)
	}
	s.c.Enqueue("history", []client.EnqueueMsg{{Body: "e1"}, {Body: "e2"}})
	msgs, _ := s.c.Dequeue("history", 10, 0, 60_000)
	for _, m := range msgs {
		s.c.Ack("history", m.ID, m.Token)
	}
	res, err := s.c.Replay("history", "", "", 100)
	if err != nil || res.Replayed != 2 {
		t.Fatalf("replay: %+v %v", res, err)
	}
	msgs, _ = s.c.Dequeue("history", 10, 0, 60_000)
	if len(msgs) != 2 || msgs[0].OriginID == "" {
		t.Fatalf("replayed messages wrong: %+v", msgs)
	}
}

func TestE2EValidation(t *testing.T) {
	s := startStack(t, t.TempDir(), core.Options{})
	if _, err := s.c.Stats("ghost"); !client.IsCode(err, "queue_not_found") {
		t.Fatalf("missing queue: %v", err)
	}
	if err := s.c.CreateQueue(client.QueueConfig{Name: "sneaky.dlq"}); !client.IsCode(err, "name_reserved") {
		t.Fatalf("reserved name: %v", err)
	}
	if err := s.c.CreateQueue(client.QueueConfig{Name: "q", Order: "sideways"}); !client.IsCode(err, "invalid") {
		t.Fatalf("bad order: %v", err)
	}
	// Raw bad JSON.
	resp, err := http.Post("http://"+s.addr+"/v1/queues", "application/json", strings.NewReader("{nope"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("bad JSON got %d", resp.StatusCode)
	}
}

// TestTornTailAtBrokerLevel corrupts the WAL tail on disk and verifies the
// whole stack recovers the exact valid prefix.
func TestTornTailAtBrokerLevel(t *testing.T) {
	dir := t.TempDir()
	s := startStack(t, dir, core.Options{})
	if err := s.c.CreateQueue(client.QueueConfig{Name: "torn"}); err != nil {
		t.Fatal(err)
	}
	for i := range 5 {
		if _, err := s.c.Enqueue("torn", []client.EnqueueMsg{{Body: fmt.Sprintf("m-%d", i)}}); err != nil {
			t.Fatal(err)
		}
	}
	s.stop(t)

	// Flip the last byte of the newest segment: the final enqueue's CRC dies.
	walDir := filepath.Join(dir, "wal")
	entries, err := os.ReadDir(walDir)
	if err != nil {
		t.Fatal(err)
	}
	last := entries[len(entries)-1].Name()
	path := filepath.Join(walDir, last)
	buf, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	buf[len(buf)-1] ^= 0xFF
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}

	s2 := startStack(t, dir, core.Options{})
	st, err := s2.c.Stats("torn")
	if err != nil {
		t.Fatal(err)
	}
	if st.Ready != 4 {
		t.Fatalf("expected exactly the 4-message prefix, got %d ready", st.Ready)
	}
	// And the truncated log must accept new writes cleanly.
	if _, err := s2.c.Enqueue("torn", []client.EnqueueMsg{{Body: "fresh"}}); err != nil {
		t.Fatal(err)
	}
	st, _ = s2.c.Stats("torn")
	if st.Ready != 5 {
		t.Fatalf("post-truncation enqueue: %+v", st)
	}
}
