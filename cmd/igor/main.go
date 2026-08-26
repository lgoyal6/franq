// igor is franq's lab assistant: a CLI producer/consumer plus scripted,
// self-checking demos of every headline behavior.
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lgoyal6/franq/internal/client"
)

func usage() {
	fmt.Fprintf(os.Stderr, `igor - the franq lab assistant

usage: igor [-addr URL] <command> [flags]

commands:
  create   -q NAME [-order fifo|lifo] [-aging RATE] [-max-deliveries N] [-lease-ms MS]
  delete   -q NAME
  send     -q NAME -body TEXT [-priority N] [-delay-ms MS] [-n COUNT]
  stats    [-q NAME]                 (no -q: list all queues)
  work     -q NAME [-n WORKERS] [-fail-rate P]   run workers until Ctrl-C
  demo     [priority|delay|crash|starvation|timetravel|all]

igor talks to a running franqd (default http://127.0.0.1:1818).
`)
	os.Exit(2)
}

func main() {
	addr := flag.String("addr", "http://127.0.0.1:1818", "franqd base URL")
	flag.Usage = usage
	flag.Parse()
	if flag.NArg() < 1 {
		usage()
	}
	c := client.New(*addr)
	if err := c.Health(); err != nil {
		fmt.Fprintf(os.Stderr, "igor: cannot reach franqd at %s - is it running? (%v)\n", *addr, err)
		os.Exit(1)
	}

	cmd, args := flag.Arg(0), flag.Args()[1:]
	var err error
	switch cmd {
	case "create":
		err = cmdCreate(c, args)
	case "delete":
		err = cmdDelete(c, args)
	case "send":
		err = cmdSend(c, args)
	case "stats":
		err = cmdStats(c, args)
	case "work":
		err = cmdWork(c, args)
	case "demo":
		err = cmdDemo(c, args)
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "igor: %v\n", err)
		os.Exit(1)
	}
}

func cmdCreate(c *client.Client, args []string) error {
	fs := flag.NewFlagSet("create", flag.ExitOnError)
	q := fs.String("q", "", "queue name")
	order := fs.String("order", "fifo", "fifo or lifo")
	aging := fs.Float64("aging", 0, "aging rate (priority points per second waited)")
	maxDel := fs.Uint("max-deliveries", 0, "attempts before dead-lettering (0 = unlimited)")
	lease := fs.Int64("lease-ms", 30_000, "default lease")
	fs.Parse(args)
	if *q == "" {
		return fmt.Errorf("-q required")
	}
	if err := c.CreateQueue(client.QueueConfig{
		Name: *q, Order: *order, AgingRate: *aging,
		MaxDeliveries: uint32(*maxDel), DefaultLeaseMS: *lease,
	}); err != nil {
		return err
	}
	fmt.Printf("created %s (order=%s aging=%.2f max_deliveries=%d) - plus %s.dlq\n",
		*q, *order, *aging, *maxDel, *q)
	return nil
}

func cmdDelete(c *client.Client, args []string) error {
	fs := flag.NewFlagSet("delete", flag.ExitOnError)
	q := fs.String("q", "", "queue name")
	fs.Parse(args)
	if *q == "" {
		return fmt.Errorf("-q required")
	}
	if err := c.DeleteQueue(*q); err != nil {
		return err
	}
	fmt.Printf("deleted %s (and %s.dlq)\n", *q, *q)
	return nil
}

func cmdSend(c *client.Client, args []string) error {
	fs := flag.NewFlagSet("send", flag.ExitOnError)
	q := fs.String("q", "", "queue name")
	body := fs.String("body", "", "message body")
	pri := fs.Uint("priority", 0, "priority 0-255, higher first")
	delay := fs.Int64("delay-ms", 0, "visibility delay")
	n := fs.Int("n", 1, "copies to send")
	fs.Parse(args)
	if *q == "" || *body == "" {
		return fmt.Errorf("-q and -body required")
	}
	msgs := make([]client.EnqueueMsg, *n)
	for i := range msgs {
		b := *body
		if *n > 1 {
			b = fmt.Sprintf("%s [%d]", b, i+1)
		}
		msgs[i] = client.EnqueueMsg{Body: b, Priority: uint8(*pri), DelayMS: *delay}
	}
	ids, err := c.Enqueue(*q, msgs)
	if err != nil {
		return err
	}
	fmt.Printf("sent %d message(s), ids %v\n", len(ids), ids)
	return nil
}

func cmdStats(c *client.Client, args []string) error {
	fs := flag.NewFlagSet("stats", flag.ExitOnError)
	q := fs.String("q", "", "queue name (empty: all)")
	fs.Parse(args)
	var list []client.Stats
	if *q != "" {
		s, err := c.Stats(*q)
		if err != nil {
			return err
		}
		list = []client.Stats{s}
	} else {
		var err error
		if list, err = c.ListQueues(); err != nil {
			return err
		}
	}
	fmt.Printf("%-24s %-5s %6s %8s %9s %9s %8s %8s %6s\n",
		"QUEUE", "ORDER", "AGING", "READY", "DELAYED", "INFLIGHT", "ENQ", "ACK", "DLQ")
	for _, s := range list {
		fmt.Printf("%-24s %-5s %6.1f %8d %9d %9d %8d %8d %6d\n",
			s.Name, s.Order, s.AgingRate, s.Ready, s.Delayed, s.InFlight,
			s.TotalEnqueued, s.TotalAcked, s.DLQDepth)
	}
	return nil
}

func cmdWork(c *client.Client, args []string) error {
	fs := flag.NewFlagSet("work", flag.ExitOnError)
	q := fs.String("q", "", "queue name")
	n := fs.Int("n", 2, "concurrent workers")
	failRate := fs.Float64("fail-rate", 0, "probability a worker nacks a job")
	fs.Parse(args)
	if *q == "" {
		return fmt.Errorf("-q required")
	}
	fmt.Printf("igor: %d worker(s) on %q, fail rate %.0f%% - Ctrl-C to stop\n", *n, *q, *failRate*100)
	stop := make(chan struct{})
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		close(stop)
	}()
	var done atomic.Int64
	var wg sync.WaitGroup
	for w := 1; w <= *n; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w)))
			for {
				select {
				case <-stop:
					return
				default:
				}
				msgs, err := c.Dequeue(*q, 1, 2000, 30_000)
				if err != nil {
					fmt.Fprintf(os.Stderr, "worker-%d: %v\n", w, err)
					time.Sleep(time.Second)
					continue
				}
				for _, m := range msgs {
					if rng.Float64() < *failRate {
						fmt.Printf("worker-%d: 💥 botched %q (delivery #%d) - nacking\n", w, m.Body, m.Deliveries)
						c.Nack(*q, m.ID, m.Token, 500)
						continue
					}
					time.Sleep(time.Duration(50+rng.Intn(100)) * time.Millisecond)
					if err := c.Ack(*q, m.ID, m.Token); err != nil {
						fmt.Printf("worker-%d: lease lost on %q: %v\n", w, m.Body, err)
						continue
					}
					fmt.Printf("worker-%d: ✓ %q (priority %d, delivery #%d)\n", w, m.Body, m.Priority, m.Deliveries)
					done.Add(1)
				}
			}
		}(w)
	}
	wg.Wait()
	fmt.Printf("igor: stopped after %d completed jobs\n", done.Load())
	return nil
}
