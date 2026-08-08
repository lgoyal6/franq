package core

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"
)

func openBroker(t *testing.T, dir string, opts Options) *Broker {
	t.Helper()
	b, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func mustCreate(t *testing.T, b *Broker, name string, cfg Config) *Queue {
	t.Helper()
	if err := b.CreateQueue(name, cfg); err != nil {
		t.Fatal(err)
	}
	q, ok := b.GetQueue(name)
	if !ok {
		t.Fatalf("queue %s vanished after create", name)
	}
	return q
}

func drain(t *testing.T, b *Broker, q *Queue, max int) []Delivery {
	t.Helper()
	var out []Delivery
	for {
		ds, err := q.Dequeue(context.Background(), 1, 0, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if len(ds) == 0 {
			return out
		}
		for _, d := range ds {
			if err := q.Ack(d.ID, d.Token); err != nil {
				t.Fatalf("ack %d: %v", d.ID, err)
			}
			out = append(out, d)
		}
		if len(out) > max {
			t.Fatalf("drained more than %d messages", max)
		}
	}
}

// TestOrderingModes drives every mode combination against a 20-line
// reference model: sort by key desc, then id in the configured direction.
func TestOrderingModes(t *testing.T) {
	for _, order := range []string{"fifo", "lifo"} {
		for _, withPriority := range []bool{false, true} {
			name := fmt.Sprintf("%s-pri=%v", order, withPriority)
			t.Run(name, func(t *testing.T) {
				b := openBroker(t, t.TempDir(), Options{})
				defer b.Close()
				q := mustCreate(t, b, "q", Config{Order: order})

				rng := rand.New(rand.NewSource(42))
				type ref struct {
					id  uint64
					pri uint8
				}
				var want []ref
				for i := range 60 {
					var pri uint8
					if withPriority {
						pri = uint8(rng.Intn(4))
					}
					ids, err := b.Enqueue(q, []EnqueueItem{{Body: []byte(fmt.Sprint(i)), Priority: pri}})
					if err != nil {
						t.Fatal(err)
					}
					want = append(want, ref{ids[0], pri})
				}
				sort.SliceStable(want, func(i, j int) bool {
					if want[i].pri != want[j].pri {
						return want[i].pri > want[j].pri
					}
					if order == "lifo" {
						return want[i].id > want[j].id
					}
					return want[i].id < want[j].id
				})
				got := drain(t, b, q, 100)
				if len(got) != len(want) {
					t.Fatalf("drained %d, want %d", len(got), len(want))
				}
				for i := range want {
					if got[i].ID != want[i].id {
						t.Fatalf("position %d: got id %d, want %d", i, got[i].ID, want[i].id)
					}
				}
			})
		}
	}
}

// TestAgingBeatsPriority: with a large aging rate, an old low-priority
// message must outrank a fresh high-priority one — the anti-starvation
// guarantee, delivered by a static key.
func TestAgingBeatsPriority(t *testing.T) {
	b := openBroker(t, t.TempDir(), Options{})
	defer b.Close()
	q := mustCreate(t, b, "q", Config{Order: "fifo", AgingRate: 1000})

	oldIDs, err := b.Enqueue(q, []EnqueueItem{{Body: []byte("old"), Priority: 0}})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond) // crossover for Δpri=5 at λ=1000 is 5ms
	if _, err := b.Enqueue(q, []EnqueueItem{{Body: []byte("vip"), Priority: 5}}); err != nil {
		t.Fatal(err)
	}
	got := drain(t, b, q, 5)
	if got[0].ID != oldIDs[0] {
		t.Fatalf("aged pri-0 message should beat fresh pri-5: got %s first", got[0].Body)
	}
}

// TestAgingOverridesLIFO documents the intended semantic: among equal
// priorities, any λ > 0 drains oldest-first even on a LIFO queue — LIFO
// starves old messages by design, aging exists to prevent exactly that, so
// when both are configured aging wins.
func TestAgingOverridesLIFO(t *testing.T) {
	b := openBroker(t, t.TempDir(), Options{})
	defer b.Close()
	q := mustCreate(t, b, "q", Config{Order: "lifo", AgingRate: 100})
	var ids []uint64
	for i := range 3 {
		got, err := b.Enqueue(q, []EnqueueItem{{Body: []byte(fmt.Sprint(i))}})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, got[0])
		time.Sleep(5 * time.Millisecond)
	}
	got := drain(t, b, q, 5)
	for i := range 3 {
		if got[i].ID != ids[i] {
			t.Fatalf("aging should drain oldest-first: position %d got %d want %d", i, got[i].ID, ids[i])
		}
	}
}

func TestDelayVisibility(t *testing.T) {
	b := openBroker(t, t.TempDir(), Options{})
	defer b.Close()
	q := mustCreate(t, b, "q", Config{})
	start := time.Now()
	if _, err := b.Enqueue(q, []EnqueueItem{{Body: []byte("later"), DelayMS: 300}}); err != nil {
		t.Fatal(err)
	}
	ds, err := q.Dequeue(context.Background(), 1, 0, time.Minute)
	if err != nil || len(ds) != 0 {
		t.Fatalf("delayed message visible immediately: %v %v", ds, err)
	}
	ds, err = q.Dequeue(context.Background(), 1, 5*time.Second, time.Minute)
	if err != nil || len(ds) != 1 {
		t.Fatalf("delayed message never arrived: %v %v", ds, err)
	}
	if el := time.Since(start); el < 250*time.Millisecond {
		t.Fatalf("message arrived after %v, before its 300ms delay", el)
	}
}

func TestLeaseExpiryRedelivery(t *testing.T) {
	b := openBroker(t, t.TempDir(), Options{})
	defer b.Close()
	q := mustCreate(t, b, "q", Config{})
	if _, err := b.Enqueue(q, []EnqueueItem{{Body: []byte("job")}}); err != nil {
		t.Fatal(err)
	}
	ds, _ := q.Dequeue(context.Background(), 1, 0, 150*time.Millisecond)
	if len(ds) != 1 || ds[0].Deliveries != 1 {
		t.Fatalf("first delivery wrong: %+v", ds)
	}
	// Crash the "worker": never ack. The sweeper must redeliver.
	ds2, err := q.Dequeue(context.Background(), 1, 5*time.Second, time.Minute)
	if err != nil || len(ds2) != 1 {
		t.Fatalf("no redelivery: %v %v", ds2, err)
	}
	if ds2[0].ID != ds[0].ID || ds2[0].Deliveries != 2 {
		t.Fatalf("redelivery wrong: %+v", ds2[0])
	}
	// The dead worker's stale token must be rejected.
	if err := q.Ack(ds[0].ID, ds[0].Token); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale ack: got %v, want ErrLeaseLost", err)
	}
	if err := q.Ack(ds2[0].ID, ds2[0].Token); err != nil {
		t.Fatal(err)
	}
}

func TestNackWithDelay(t *testing.T) {
	b := openBroker(t, t.TempDir(), Options{})
	defer b.Close()
	q := mustCreate(t, b, "q", Config{})
	b.Enqueue(q, []EnqueueItem{{Body: []byte("retry-me")}})
	ds, _ := q.Dequeue(context.Background(), 1, 0, time.Minute)
	start := time.Now()
	if err := q.Nack(ds[0].ID, ds[0].Token, 200); err != nil {
		t.Fatal(err)
	}
	ds2, err := q.Dequeue(context.Background(), 1, 5*time.Second, time.Minute)
	if err != nil || len(ds2) != 1 {
		t.Fatalf("nacked message never returned: %v %v", ds2, err)
	}
	if el := time.Since(start); el < 150*time.Millisecond {
		t.Fatalf("nack delay not honored: returned after %v", el)
	}
	if ds2[0].Deliveries != 2 {
		t.Fatalf("delivery count %d, want 2", ds2[0].Deliveries)
	}
}

func TestDLQAfterMaxDeliveries(t *testing.T) {
	b := openBroker(t, t.TempDir(), Options{})
	defer b.Close()
	q := mustCreate(t, b, "q", Config{MaxDeliveries: 2})
	ids, _ := b.Enqueue(q, []EnqueueItem{{Body: []byte("poison")}})
	for i := range 2 {
		ds, _ := q.Dequeue(context.Background(), 1, 2*time.Second, time.Minute)
		if len(ds) != 1 {
			t.Fatalf("attempt %d: no delivery", i+1)
		}
		if err := q.Nack(ds[0].ID, ds[0].Token, 0); err != nil {
			t.Fatal(err)
		}
	}
	// Second nack hit max_deliveries: the message must now be in q.dlq.
	ds, _ := q.Dequeue(context.Background(), 1, 0, time.Minute)
	if len(ds) != 0 {
		t.Fatalf("poison message still in main queue")
	}
	dlq, ok := b.GetQueue("q.dlq")
	if !ok {
		t.Fatal("dlq missing")
	}
	dd, err := dlq.Dequeue(context.Background(), 1, 2*time.Second, time.Minute)
	if err != nil || len(dd) != 1 {
		t.Fatalf("dlq empty: %v %v", dd, err)
	}
	if dd[0].OriginID != ids[0] || string(dd[0].Body) != "poison" {
		t.Fatalf("dlq copy wrong: %+v", dd[0])
	}
	if s := b.Stats(q); s.TotalDLQ != 1 {
		t.Fatalf("TotalDLQ = %d, want 1", s.TotalDLQ)
	}
}

// TestRestartDurability is the whole assignment in one test: acked messages
// stay gone, unacked leases come back, delayed messages stay delayed, all
// across a full close-and-reopen with segments and (possibly) snapshots.
func TestRestartDurability(t *testing.T) {
	dir := t.TempDir()
	b := openBroker(t, dir, Options{SegmentBytes: 2048}) // force rotations + snapshots
	q := mustCreate(t, b, "q", Config{Order: "fifo"})

	var kept []uint64
	for i := range 30 {
		ids, err := b.Enqueue(q, []EnqueueItem{{Body: []byte(fmt.Sprintf("m-%d", i))}})
		if err != nil {
			t.Fatal(err)
		}
		kept = append(kept, ids[0])
	}
	if _, err := b.Enqueue(q, []EnqueueItem{{Body: []byte("delayed"), DelayMS: 60_000}}); err != nil {
		t.Fatal(err)
	}
	// Ack 10, lease 5 without acking, leave 15 ready.
	acked := map[uint64]bool{}
	for range 10 {
		ds, _ := q.Dequeue(context.Background(), 1, 0, time.Minute)
		if err := q.Ack(ds[0].ID, ds[0].Token); err != nil {
			t.Fatal(err)
		}
		acked[ds[0].ID] = true
	}
	leased, _ := q.Dequeue(context.Background(), 5, 0, time.Hour)
	if len(leased) != 5 {
		t.Fatalf("leased %d, want 5", len(leased))
	}
	b.Close()

	b2 := openBroker(t, dir, Options{SegmentBytes: 2048})
	defer b2.Close()
	q2, ok := b2.GetQueue("q")
	if !ok {
		t.Fatal("queue lost across restart")
	}
	s := b2.Stats(q2)
	// Leases are not persisted: the 5 leased come back as ready → 20 ready.
	if s.Ready != 20 || s.Delayed != 1 || s.InFlight != 0 {
		t.Fatalf("post-restart stats: %+v", s)
	}
	got := drain(t, b2, q2, 30)
	if len(got) != 20 {
		t.Fatalf("drained %d, want 20", len(got))
	}
	seen := map[uint64]bool{}
	for _, d := range got {
		if acked[d.ID] {
			t.Fatalf("acked message %d rose from the dead", d.ID)
		}
		if seen[d.ID] {
			t.Fatalf("message %d delivered twice in one drain", d.ID)
		}
		seen[d.ID] = true
	}
}

func TestReplayTimeTravel(t *testing.T) {
	b := openBroker(t, t.TempDir(), Options{})
	defer b.Close()
	q := mustCreate(t, b, "q", Config{})
	var origIDs []uint64
	for i := range 3 {
		ids, _ := b.Enqueue(q, []EnqueueItem{{Body: []byte(fmt.Sprintf("job-%d", i))}})
		origIDs = append(origIDs, ids[0])
	}
	if got := drain(t, b, q, 5); len(got) != 3 {
		t.Fatalf("drained %d", len(got))
	}
	// Everything acked; the queue is empty — but history isn't.
	res, err := b.Replay(q, time.Time{}, time.Time{}, 100)
	if err != nil {
		t.Fatal(err)
	}
	if res.Replayed != 3 {
		t.Fatalf("replayed %d, want 3", res.Replayed)
	}
	got := drain(t, b, q, 5)
	if len(got) != 3 {
		t.Fatalf("post-replay drain %d, want 3", len(got))
	}
	origins := map[uint64]bool{}
	for _, d := range got {
		origins[d.OriginID] = true
	}
	for _, id := range origIDs {
		if !origins[id] {
			t.Fatalf("origin %d missing from replayed copies", id)
		}
	}
}

func TestQueueLifecycle(t *testing.T) {
	b := openBroker(t, t.TempDir(), Options{})
	defer b.Close()
	if err := b.CreateQueue("jobs", Config{}); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateQueue("jobs", Config{}); !errors.Is(err, ErrQueueExists) {
		t.Fatalf("dup create: %v", err)
	}
	if err := b.CreateQueue("evil.dlq", Config{}); !errors.Is(err, ErrNameReserved) {
		t.Fatalf("reserved name: %v", err)
	}
	if err := b.CreateQueue("bad name!", Config{}); err == nil {
		t.Fatal("bad name accepted")
	}
	if err := b.CreateQueue("x", Config{Order: "random"}); err == nil {
		t.Fatal("bad order accepted")
	}
	if _, ok := b.GetQueue("jobs.dlq"); !ok {
		t.Fatal("dlq not auto-created")
	}
	if err := b.DeleteQueue("jobs"); err != nil {
		t.Fatal(err)
	}
	if _, ok := b.GetQueue("jobs"); ok {
		t.Fatal("queue survived delete")
	}
	if _, ok := b.GetQueue("jobs.dlq"); ok {
		t.Fatal("dlq survived delete")
	}
	if err := b.CreateQueue("jobs", Config{Order: "lifo"}); err != nil {
		t.Fatalf("recreate after delete: %v", err)
	}
}

// TestConcurrentAccounting is the concurrency proof: N producers and M
// consumers race; every enqueued message must be delivered and acked exactly
// once. Run with -race.
func TestConcurrentAccounting(t *testing.T) {
	b := openBroker(t, t.TempDir(), Options{})
	defer b.Close()
	q := mustCreate(t, b, "q", Config{})

	const producers, perProducer, consumers = 4, 100, 4
	total := producers * perProducer

	var mu sync.Mutex
	ackedBodies := map[string]int{}

	var pwg sync.WaitGroup
	for p := range producers {
		pwg.Add(1)
		go func(p int) {
			defer pwg.Done()
			for i := range perProducer {
				body := fmt.Sprintf("p%d-%d", p, i)
				if _, err := b.Enqueue(q, []EnqueueItem{{Body: []byte(body), Priority: uint8(i % 3)}}); err != nil {
					t.Error(err)
					return
				}
			}
		}(p)
	}

	var cwg sync.WaitGroup
	done := make(chan struct{})
	for range consumers {
		cwg.Add(1)
		go func() {
			defer cwg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				ds, err := q.Dequeue(context.Background(), 5, 200*time.Millisecond, time.Minute)
				if err != nil {
					return
				}
				for _, d := range ds {
					if err := q.Ack(d.ID, d.Token); err != nil {
						t.Errorf("ack: %v", err)
						return
					}
					mu.Lock()
					ackedBodies[string(d.Body)]++
					mu.Unlock()
				}
			}
		}()
	}

	pwg.Wait()
	deadline := time.After(30 * time.Second)
	for {
		mu.Lock()
		n := len(ackedBodies)
		mu.Unlock()
		if n == total {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("only %d/%d messages consumed", n, total)
		case <-time.After(50 * time.Millisecond):
		}
	}
	close(done)
	cwg.Wait()

	for body, n := range ackedBodies {
		if n != 1 {
			t.Fatalf("message %s acked %d times", body, n)
		}
	}
	s := b.Stats(q)
	if s.Ready != 0 || s.InFlight != 0 || s.Delayed != 0 {
		t.Fatalf("queue not empty after consuming everything: %+v", s)
	}
}
