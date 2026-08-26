package main

import (
	"fmt"
	"time"

	"github.com/lgoyal6/franq/internal/client"
)

// Each demo is a self-checking scenario: it narrates what it does, verifies
// the outcome, and reports PASS or FAIL. `igor demo all` runs the lot.

var demos = []struct {
	name string
	desc string
	fn   func(*client.Client) error
}{
	{"priority", "a VIP job jumps a backlog", demoPriority},
	{"delay", "delayed jobs surface on schedule", demoDelay},
	{"crash", "a worker dies mid-job; the job replays elsewhere", demoCrash},
	{"starvation", "LIFO+priority starves an old job - aging rescues it", demoStarvation},
	{"timetravel", "replay already-completed jobs from WAL history", demoTimeTravel},
}

func cmdDemo(c *client.Client, args []string) error {
	which := "all"
	if len(args) > 0 {
		which = args[0]
	}
	failed := 0
	ran := 0
	for _, d := range demos {
		if which != "all" && which != d.name {
			continue
		}
		ran++
		fmt.Printf("\n══ demo %s - %s ══\n", d.name, d.desc)
		if err := d.fn(c); err != nil {
			fmt.Printf("── FAIL: %v\n", err)
			failed++
		} else {
			fmt.Printf("── PASS\n")
		}
	}
	if ran == 0 {
		return fmt.Errorf("unknown demo %q (try: priority, delay, crash, starvation, timetravel, all)", which)
	}
	if failed > 0 {
		return fmt.Errorf("%d demo(s) failed", failed)
	}
	return nil
}

// freshQueue deletes any leftover queue from a previous run and recreates it.
func freshQueue(c *client.Client, cfg client.QueueConfig) error {
	if err := c.DeleteQueue(cfg.Name); err != nil && !client.IsCode(err, "queue_not_found") {
		return err
	}
	return c.CreateQueue(cfg)
}

func demoPriority(c *client.Client) error {
	const q = "demo.priority"
	if err := freshQueue(c, client.QueueConfig{Name: q, Order: "fifo"}); err != nil {
		return err
	}
	for i := 1; i <= 5; i++ {
		if _, err := c.Enqueue(q, []client.EnqueueMsg{{Body: fmt.Sprintf("routine-%d", i)}}); err != nil {
			return err
		}
	}
	fmt.Println("   queued 5 routine jobs (priority 0)")
	if _, err := c.Enqueue(q, []client.EnqueueMsg{{Body: "VIP", Priority: 9}}); err != nil {
		return err
	}
	fmt.Println("   queued 1 VIP job (priority 9) - last in line, timewise")
	msgs, err := c.Dequeue(q, 6, 0, 30_000)
	if err != nil {
		return err
	}
	var order []string
	for _, m := range msgs {
		order = append(order, m.Body)
		c.Ack(q, m.ID, m.Token)
	}
	fmt.Printf("   delivery order: %v\n", order)
	if len(order) != 6 || order[0] != "VIP" {
		return fmt.Errorf("VIP did not jump the line")
	}
	for i := 1; i < 6; i++ {
		if order[i] != fmt.Sprintf("routine-%d", i) {
			return fmt.Errorf("routine jobs out of FIFO order at %d: %v", i, order)
		}
	}
	fmt.Println("   VIP first, routines still FIFO among themselves")
	return nil
}

func demoDelay(c *client.Client) error {
	const q = "demo.delay"
	if err := freshQueue(c, client.QueueConfig{Name: q}); err != nil {
		return err
	}
	start := time.Now()
	if _, err := c.Enqueue(q, []client.EnqueueMsg{
		{Body: "espresso (now)"},
		{Body: "cold-brew (1.5s steep)", DelayMS: 1500},
	}); err != nil {
		return err
	}
	msgs, err := c.Dequeue(q, 10, 0, 30_000)
	if err != nil {
		return err
	}
	if len(msgs) != 1 || msgs[0].Body != "espresso (now)" {
		return fmt.Errorf("expected only the espresso immediately, got %d messages", len(msgs))
	}
	c.Ack(q, msgs[0].ID, msgs[0].Token)
	fmt.Println("   immediate dequeue: espresso only - cold-brew is invisible")
	msgs, err = c.Dequeue(q, 1, 5000, 30_000)
	if err != nil {
		return err
	}
	if len(msgs) != 1 {
		return fmt.Errorf("cold-brew never surfaced")
	}
	elapsed := time.Since(start).Round(10 * time.Millisecond)
	c.Ack(q, msgs[0].ID, msgs[0].Token)
	fmt.Printf("   long-poll delivered %q after %v\n", msgs[0].Body, elapsed)
	if elapsed < 1200*time.Millisecond {
		return fmt.Errorf("cold-brew served %v early", 1500*time.Millisecond-elapsed)
	}
	return nil
}

func demoCrash(c *client.Client) error {
	const q = "demo.crash"
	if err := freshQueue(c, client.QueueConfig{Name: q}); err != nil {
		return err
	}
	if _, err := c.Enqueue(q, []client.EnqueueMsg{{Body: "fragile-job"}}); err != nil {
		return err
	}
	msgs, err := c.Dequeue(q, 1, 0, 1000) // 1s lease
	if err != nil || len(msgs) != 1 {
		return fmt.Errorf("worker-1 got nothing: %v", err)
	}
	fmt.Printf("   worker-1 leased %q (1s lease)… and died 💀 (no ack)\n", msgs[0].Body)
	start := time.Now()
	redelivered, err := c.Dequeue(q, 1, 10_000, 30_000)
	if err != nil || len(redelivered) != 1 {
		return fmt.Errorf("worker-2 never got the redelivery: %v", err)
	}
	fmt.Printf("   worker-2 received %q after %v - delivery #%d\n",
		redelivered[0].Body, time.Since(start).Round(10*time.Millisecond), redelivered[0].Deliveries)
	if redelivered[0].ID != msgs[0].ID || redelivered[0].Deliveries != 2 {
		return fmt.Errorf("redelivery mismatch: %+v", redelivered[0])
	}
	if err := c.Ack(q, redelivered[0].ID, redelivered[0].Token); err != nil {
		return err
	}
	if err := c.Ack(q, msgs[0].ID, msgs[0].Token); err == nil {
		return fmt.Errorf("the dead worker's stale token was accepted")
	}
	fmt.Println("   worker-2 acked; the dead worker's stale token was rejected")
	return nil
}

// demoStarvation runs identical traffic through two queues - one plain, one
// with aging - and shows the difference: a priority-0 job under a stream of
// priority-5 work starves forever without aging, and climbs out with it.
func demoStarvation(c *client.Client) error {
	run := func(q string, aging float64) (served bool, waited time.Duration, err error) {
		if err := freshQueue(c, client.QueueConfig{Name: q, Order: "fifo", AgingRate: aging}); err != nil {
			return false, 0, err
		}
		if _, err := c.Enqueue(q, []client.EnqueueMsg{{Body: "neglected", Priority: 0}}); err != nil {
			return false, 0, err
		}
		start := time.Now()
		deadline := start.Add(3 * time.Second)
		hotshot := 0
		var stop bool
		for time.Now().Before(deadline) && !stop {
			hotshot++
			if _, err := c.Enqueue(q, []client.EnqueueMsg{
				{Body: fmt.Sprintf("hotshot-%d", hotshot), Priority: 5},
			}); err != nil {
				return false, 0, err
			}
			msgs, err := c.Dequeue(q, 1, 0, 30_000)
			if err != nil {
				return false, 0, err
			}
			for _, m := range msgs {
				c.Ack(q, m.ID, m.Token)
				if m.Body == "neglected" {
					served, waited = true, time.Since(start)
					stop = true
				}
			}
			time.Sleep(70 * time.Millisecond)
		}
		return served, waited, nil
	}

	fmt.Println("   scenario: a priority-0 job vs a nonstop stream of priority-5 jobs (3s)")
	served, _, err := run("demo.starve", 0)
	if err != nil {
		return err
	}
	fmt.Printf("   λ=0  (no aging): neglected served? %v - starved behind every hotshot\n", served)
	if served {
		return fmt.Errorf("without aging the pri-0 job should starve during the stream")
	}
	servedAged, waited, err := run("demo.aging", 10) // +10 pri/sec: outranks pri-5 after 0.5s
	if err != nil {
		return err
	}
	if !servedAged {
		return fmt.Errorf("with aging the pri-0 job should have been served")
	}
	fmt.Printf("   λ=10 (aging on): neglected served after %v - earned +%.1f effective priority and won\n",
		waited.Round(10*time.Millisecond), waited.Seconds()*10)
	return nil
}

func demoTimeTravel(c *client.Client) error {
	const q = "demo.timetravel"
	if err := freshQueue(c, client.QueueConfig{Name: q}); err != nil {
		return err
	}
	for i := 1; i <= 3; i++ {
		if _, err := c.Enqueue(q, []client.EnqueueMsg{{Body: fmt.Sprintf("order-%d", i)}}); err != nil {
			return err
		}
	}
	msgs, err := c.Dequeue(q, 10, 0, 30_000)
	if err != nil || len(msgs) != 3 {
		return fmt.Errorf("expected 3 deliveries, got %d (%v)", len(msgs), err)
	}
	for _, m := range msgs {
		if err := c.Ack(q, m.ID, m.Token); err != nil {
			return err
		}
	}
	st, _ := c.Stats(q)
	fmt.Printf("   processed & acked 3 orders - queue empty (ready=%d), history retained in the WAL\n", st.Ready)
	res, err := c.Replay(q, "", "", 100)
	if err != nil {
		return err
	}
	fmt.Printf("   POST /replay: re-enqueued %d messages from history (window starts %s)\n", res.Replayed, res.WindowStart)
	if res.Replayed != 3 {
		return fmt.Errorf("replayed %d, want 3", res.Replayed)
	}
	msgs, err = c.Dequeue(q, 10, 0, 30_000)
	if err != nil || len(msgs) != 3 {
		return fmt.Errorf("post-replay drain got %d", len(msgs))
	}
	for _, m := range msgs {
		fmt.Printf("   ↩︎ %q again (fresh id %s, origin %s)\n", m.Body, m.ID, m.OriginID)
		if m.OriginID == "" {
			return fmt.Errorf("replayed copy missing origin_id")
		}
		c.Ack(q, m.ID, m.Token)
	}
	fmt.Println("   same jobs, new identities - SQS cannot do this")
	return nil
}
