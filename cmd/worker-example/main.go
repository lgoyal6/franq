// worker-example consumes Franq at least once and stores a durable local effect.
// The effect and deduplication key share one atomic ledger; a real downstream
// database must commit its effect and idempotency key in the same transaction.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/lgoyal6/franq/internal/client"
	"github.com/lgoyal6/franq/internal/consumer"
)

func main() {
	addr := flag.String("addr", "http://127.0.0.1:1818", "Franq API URL")
	queue := flag.String("queue", "", "queue name")
	state := flag.String("state", "worker-effects.json", "effect ledger path")
	postgresURL := flag.String("postgres-url", "", "PostgreSQL URL for a transactional consumer effect ledger")
	flag.Parse()
	if *queue == "" {
		log.Fatal("-queue is required")
	}
	c := client.New(*addr)
	if err := c.Health(); err != nil {
		log.Fatal(err)
	}
	var postgres *consumer.PostgresEffects
	if *postgresURL != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		var err error
		postgres, err = consumer.ConnectPostgres(ctx, *postgresURL)
		cancel()
		if err != nil {
			log.Fatal(err)
		}
		defer postgres.Close()
	}
	for {
		messages, err := c.Dequeue(*queue, 1, 1000, 30_000)
		if err != nil {
			log.Printf("dequeue: %v", err)
			time.Sleep(time.Second)
			continue
		}
		for _, message := range messages {
			var applied bool
			if postgres != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				applied, err = postgres.ApplyOnce(ctx, message.ID, message.Body)
				cancel()
			} else {
				applied, err = applyOnce(*state, message.ID, message.Body)
			}
			if err != nil {
				log.Printf("effect for message %s: %v", message.ID, err)
				_ = c.Nack(*queue, message.ID, message.Token, 1000)
				continue
			}
			if err := c.Ack(*queue, message.ID, message.Token); err != nil {
				log.Printf("ack message %s: %v", message.ID, err)
				continue
			}
			fmt.Fprintf(os.Stdout, "message=%s applied=%t\n", message.ID, applied)
		}
	}
}
