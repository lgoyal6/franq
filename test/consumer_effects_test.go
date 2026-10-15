package franqtest

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/lgoyal6/franq/internal/consumer"
)

// Set FRANQ_TEST_POSTGRES_URL to a disposable local PostgreSQL instance to
// exercise the real transaction boundary. The test never creates cloud state.
func TestPostgresEffectIsIdempotentAcrossRedelivery(t *testing.T) {
	url := os.Getenv("FRANQ_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("FRANQ_TEST_POSTGRES_URL is unset")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	effects, err := consumer.ConnectPostgres(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer effects.Close()

	id := fmt.Sprintf("test-%d", time.Now().UnixNano())
	const body = "settle invoice"
	const attempts = 8
	results := make(chan bool, attempts)
	errs := make(chan error, attempts)
	var wg sync.WaitGroup
	for range attempts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			applied, err := effects.ApplyOnce(ctx, id, body)
			results <- applied
			errs <- err
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	appliedCount := 0
	for applied := range results {
		if applied {
			appliedCount++
		}
	}
	if appliedCount != 1 {
		t.Fatalf("concurrent duplicate delivery applied %d times, want exactly once", appliedCount)
	}

	applied, err := effects.ApplyOnce(ctx, id, body) // crash after transaction commit, before queue ack
	if err != nil || applied {
		t.Fatalf("redelivery after effect commit = applied %t, err %v", applied, err)
	}
	if _, err := effects.ApplyOnce(ctx, id, "different body"); err == nil {
		t.Fatal("conflicting message body was accepted")
	}
}
