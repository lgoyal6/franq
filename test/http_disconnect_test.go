//go:build testhooks

// Checks an accepted HTTP enqueue after the client resets its TCP connection.
// Uses server.New and the WAL hook to hold the request before fsync completes.
// Verifies later visibility and recovery without retrying the unknown result.
package franqtest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lgoyal6/franq/internal/core"
	"github.com/lgoyal6/franq/internal/server"
	"github.com/lgoyal6/franq/internal/wal"
)

func TestHTTPResetWhileAcceptedEnqueueWaitsForSync(t *testing.T) {
	dir := t.TempDir()
	broker, err := core.Open(dir, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err = broker.CreateQueue("reset", core.Config{}); err != nil {
		broker.Close()
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	restore := wal.SetSegmentSyncForTest(func(file *os.File) error {
		enteredOnce.Do(func() { close(entered) })
		<-release
		return file.Sync()
	})
	handlerDone := make(chan struct{})
	api := server.New(broker)
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		api.ServeHTTP(w, r)
		if r.Method == http.MethodPost {
			close(handlerDone)
		}
	}))
	closed := false
	defer func() {
		unblock()
		httpServer.Close()
		if !closed {
			broker.Close()
		}
		restore()
	}()

	connection, err := net.DialTimeout("tcp", strings.TrimPrefix(httpServer.URL, "http://"), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	const body = `{"messages":[{"body":"accepted-before-reset"}]}`
	if _, err = fmt.Fprintf(connection, "POST /v1/queues/reset/messages HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", len(body), body); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP request did not reach pending fsync")
	}
	queue, _ := broker.GetQueue("reset")
	if got := broker.Stats(queue).Ready; got != 0 {
		t.Fatalf("ready before fsync = %d, want zero", got)
	}
	tcp := connection.(*net.TCPConn)
	if err = tcp.SetLinger(0); err != nil {
		t.Fatal(err)
	}
	if err = tcp.Close(); err != nil {
		t.Fatal(err)
	}
	// No response was read. The caller has an unknown result and must not infer
	// failure or retry: this API does not provide enqueue idempotency keys.
	unblock()
	select {
	case <-handlerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("accepted handler did not finish after fsync release")
	}
	response, err := http.Get(httpServer.URL + "/v1/queues/reset")
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	var stats core.Stats
	if err = json.Unmarshal(data, &stats); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || stats.Ready != 1 {
		t.Fatalf("GET after reset = status %d, %s", response.StatusCode, data)
	}
	httpServer.Close()
	broker.Close()
	closed = true
	restore()
	reopened, err := core.Open(dir, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	queue, ok := reopened.GetQueue("reset")
	if !ok {
		t.Fatal("queue missing after reopen")
	}
	if got := reopened.Stats(queue).Ready; got != 1 {
		t.Fatalf("recovered ready = %d, want one", got)
	}
	messages, err := queue.Dequeue(context.Background(), 2, 0, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || string(messages[0].Body) != "accepted-before-reset" {
		t.Fatalf("recovered deliveries = %+v", messages)
	}
}
