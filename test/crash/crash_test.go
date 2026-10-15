// Package crash is the durability torture chamber: it runs the REAL franqd
// binary, SIGKILLs it mid-traffic over and over, and then proves that
//
//	(1) every message whose enqueue was acknowledged is eventually delivered,
//	(2) no message whose ack was acknowledged is ever seen again,
//	(3) recovery always comes back healthy, never reporting sealed-segment
//	    corruption.
//
// Requests that were in flight at the moment of a kill are tracked as
// "maybe" - either outcome is legal for them, exactly like a real client
// whose TCP connection died mid-request.
package crash

import (
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lgoyal6/franq/internal/client"
)

var (
	buildOnce sync.Once
	binPath   string
	buildErr  error
)

func binary(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "franq-bin-*")
		if err != nil {
			buildErr = err
			return
		}
		binPath = filepath.Join(dir, "franqd")
		cmd := exec.Command("go", "build", "-o", binPath, "github.com/lgoyal6/franq/cmd/franqd")
		out, err := cmd.CombinedOutput()
		if err != nil {
			buildErr = fmt.Errorf("build: %v\n%s", err, out)
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return binPath
}

type proc struct {
	cmd    *exec.Cmd
	addr   string
	stderr *strings.Builder
}

func startServer(t *testing.T, dataDir string, extraEnv ...string) *proc {
	t.Helper()
	portFile := filepath.Join(t.TempDir(), fmt.Sprintf("port-%d", time.Now().UnixNano()))
	cmd := exec.Command(binary(t),
		"-addr", "127.0.0.1:0",
		"-data", dataDir,
		"-segment-bytes", "32768", // small segments: rotations + snapshots galore
		"-port-file", portFile,
	)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	cmd.Env = append(os.Environ(), extraEnv...)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &proc{cmd: cmd, stderr: &stderr}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if b, err := os.ReadFile(portFile); err == nil && len(b) > 0 {
			p.addr = string(b)
			if client.New("http://"+p.addr).Health() == nil {
				return p
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	p.kill()
	t.Fatalf("server not healthy within 5s; stderr:\n%s", stderr.String())
	return nil
}

func (p *proc) kill() {
	if p.cmd.Process != nil {
		p.cmd.Process.Kill() // SIGKILL: no cleanup, no flush, no mercy
	}
	p.cmd.Wait()
}

// accounting tracks ground truth across all rounds. Only observed HTTP
// responses mutate the definite sets; in-flight-at-kill goes to maybe sets.
type accounting struct {
	mu        sync.Mutex
	enqOK     map[string]bool      // body → enqueue 200 seen
	maybeEnq  map[string]bool      // enqueue request died mid-flight
	ackOK     map[string]time.Time // body → time its ack 200 was seen
	maybeAck  map[string]bool      // ack request died mid-flight
	delivered map[string]int
	nacked    map[string]bool
	zombies   []string
}

func newAccounting() *accounting {
	return &accounting{
		enqOK:     map[string]bool{},
		maybeEnq:  map[string]bool{},
		ackOK:     map[string]time.Time{},
		maybeAck:  map[string]bool{},
		delivered: map[string]int{},
		nacked:    map[string]bool{},
	}
}

// sawDelivery records a delivery and flags zombies: a delivery strictly
// after that body's successful ack is invariant (2) broken.
func (a *accounting) sawDelivery(body string, at time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.delivered[body]++
	if ackAt, ok := a.ackOK[body]; ok && at.After(ackAt) {
		a.zombies = append(a.zombies, body)
	}
}

const queueName = "crash"

func runTraffic(t *testing.T, addr string, acct *accounting, round int, stop <-chan struct{}) *sync.WaitGroup {
	t.Helper()
	c := client.New("http://" + addr)
	var wg sync.WaitGroup
	rng := rand.New(rand.NewSource(int64(round)))
	var rngMu sync.Mutex
	randF := func() float64 {
		rngMu.Lock()
		defer rngMu.Unlock()
		return rng.Float64()
	}

	for p := range 4 {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				n := 1 + int(randF()*3)
				msgs := make([]client.EnqueueMsg, n)
				bodies := make([]string, n)
				for j := range n {
					bodies[j] = fmt.Sprintf("r%d-p%d-%d-%d", round, p, i, j)
					msgs[j] = client.EnqueueMsg{
						Body:     bodies[j],
						Priority: uint8(int(randF() * 4)),
						DelayMS:  int64(randF() * 150),
					}
				}
				_, err := c.Enqueue(queueName, msgs)
				acct.mu.Lock()
				for _, b := range bodies {
					if err == nil {
						acct.enqOK[b] = true
					} else {
						acct.maybeEnq[b] = true
					}
				}
				acct.mu.Unlock()
				if err != nil {
					return // server is gone; round over
				}
			}
		}(p)
	}

	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				msgs, err := c.Dequeue(queueName, 3, 200, 10_000)
				if err != nil {
					return
				}
				for _, m := range msgs {
					acct.sawDelivery(m.Body, time.Now())
					if randF() < 0.10 {
						if err := c.Nack(queueName, m.ID, m.Token, 50); err == nil {
							acct.mu.Lock()
							acct.nacked[m.Body] = true
							acct.mu.Unlock()
						}
						continue
					}
					err := c.Ack(queueName, m.ID, m.Token)
					acct.mu.Lock()
					if err == nil {
						acct.ackOK[m.Body] = time.Now()
					} else if !client.IsCode(err, "lease_lost") {
						acct.maybeAck[m.Body] = true
					}
					acct.mu.Unlock()
				}
			}
		}()
	}
	return &wg
}

func TestCrashRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("crash harness skipped in -short mode")
	}
	dataDir := t.TempDir()
	acct := newAccounting()

	// Round 0 creates the queue; later rounds reuse it.
	{
		p := startServer(t, dataDir)
		c := client.New("http://" + p.addr)
		if err := c.CreateQueue(client.QueueConfig{Name: queueName, DefaultLeaseMS: 10_000}); err != nil {
			t.Fatal(err)
		}
		p.kill()
	}

	const rounds = 8
	for round := 1; round <= rounds; round++ {
		p := startServer(t, dataDir)
		stop := make(chan struct{})
		wg := runTraffic(t, p.addr, acct, round, stop)
		time.Sleep(time.Duration(100+rand.Intn(400)) * time.Millisecond)
		p.kill() // SIGKILL mid-traffic
		close(stop)
		wg.Wait()
		if s := p.stderr.String(); strings.Contains(s, "corruption inside sealed segment") {
			t.Fatalf("round %d: sealed segment corruption:\n%s", round, s)
		}
	}

	// Final round: no kill. Drain everything and settle.
	p := startServer(t, dataDir)
	defer p.kill()
	c := client.New("http://" + p.addr)
	empties := 0
	deadline := time.Now().Add(60 * time.Second)
	for empties < 5 {
		if time.Now().After(deadline) {
			st, _ := c.Stats(queueName)
			t.Fatalf("drain did not settle: %+v", st)
		}
		msgs, err := c.Dequeue(queueName, 10, 300, 10_000)
		if err != nil {
			t.Fatal(err)
		}
		if len(msgs) == 0 {
			st, err := c.Stats(queueName)
			if err != nil {
				t.Fatal(err)
			}
			if st.Ready == 0 && st.Delayed == 0 && st.InFlight == 0 {
				empties++
			}
			continue
		}
		empties = 0
		for _, m := range msgs {
			acct.sawDelivery(m.Body, time.Now())
			if err := c.Ack(queueName, m.ID, m.Token); err == nil {
				acct.mu.Lock()
				acct.ackOK[m.Body] = time.Now()
				acct.mu.Unlock()
			}
		}
	}

	// ---- The invariants ----
	acct.mu.Lock()
	defer acct.mu.Unlock()

	if len(acct.zombies) > 0 {
		t.Fatalf("INVARIANT 2 BROKEN - acked messages redelivered: %v", acct.zombies[:min(5, len(acct.zombies))])
	}
	var lost []string
	for body := range acct.enqOK {
		if _, acked := acct.ackOK[body]; acked {
			continue
		}
		if acct.maybeAck[body] {
			continue
		}
		if acct.delivered[body] == 0 {
			lost = append(lost, body)
		}
	}
	if len(lost) > 0 {
		t.Fatalf("INVARIANT 1 BROKEN - %d acknowledged enqueues lost, e.g. %v", len(lost), lost[:min(5, len(lost))])
	}
	t.Logf("crash matrix: %d confirmed enqueues, %d confirmed acks, %d maybe-enq, %d maybe-ack, %d nacks - no loss, no zombies",
		len(acct.enqOK), len(acct.ackOK), len(acct.maybeEnq), len(acct.maybeAck), len(acct.nacked))
}

// TestFailpoints kills the server at surgically chosen points inside the
// storage engine (mid-snapshot write, right after segment rotation) and
// verifies recovery: the previous snapshot is used, nothing is lost.
func TestFailpoints(t *testing.T) {
	if testing.Short() {
		t.Skip("failpoint tests skipped in -short mode")
	}
	for _, fp := range []string{"mid-snapshot:1", "after-rotate:1"} {
		t.Run(fp, func(t *testing.T) {
			dataDir := t.TempDir()
			p := startServer(t, dataDir, "FRANQ_FAILPOINT="+fp)
			c := client.New("http://" + p.addr)
			if err := c.CreateQueue(client.QueueConfig{Name: "fp"}); err != nil {
				t.Fatal(err)
			}
			// Pump until the failpoint fires (segment-bytes is 32 KiB, so a
			// few hundred records force rotation → snapshot → boom).
			var confirmed []string
			for i := range 5000 {
				body := fmt.Sprintf("fp-%d", i)
				if _, err := c.Enqueue("fp", []client.EnqueueMsg{{Body: body}}); err != nil {
					break // process died at the failpoint
				}
				confirmed = append(confirmed, body)
			}
			p.cmd.Wait()
			if len(confirmed) == 0 {
				t.Fatal("failpoint killed the server before any enqueue")
			}

			p2 := startServer(t, dataDir) // no failpoint: clean recovery
			defer p2.kill()
			c2 := client.New("http://" + p2.addr)
			seen := map[string]bool{}
			for {
				msgs, err := c2.Dequeue("fp", 10, 200, 60_000)
				if err != nil {
					t.Fatal(err)
				}
				if len(msgs) == 0 {
					break
				}
				for _, m := range msgs {
					seen[m.Body] = true
					c2.Ack("fp", m.ID, m.Token)
				}
			}
			for _, body := range confirmed {
				if !seen[body] {
					t.Fatalf("%s: confirmed enqueue %s lost across failpoint crash", fp, body)
				}
			}
			t.Logf("%s: all %d confirmed enqueues survived", fp, len(confirmed))
		})
	}
}

// TestSeededCrashSweep exercises deterministic crash points in the WAL commit,
// segment rotation, and snapshot publication path. Each seed preserves two
// acknowledged enqueues before the injected crash, then proves they recover.
func TestSeededCrashSweep(t *testing.T) {
	if testing.Short() {
		t.Skip("seeded crash sweep skipped in -short mode")
	}
	points := []string{"after-commit:4", "after-rotate:1", "mid-snapshot:1"}
	for seed := 0; seed < 100; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed-%03d", seed), func(t *testing.T) {
			dataDir := t.TempDir()
			fp := points[seed%len(points)]
			p := startServer(t, dataDir, "FRANQ_FAILPOINT="+fp)
			c := client.New("http://" + p.addr)
			queue := fmt.Sprintf("sweep-%03d", seed)
			if err := c.CreateQueue(client.QueueConfig{Name: queue}); err != nil {
				t.Fatal(err)
			}
			confirmed := []string{}
			for i := 0; i < 2; i++ {
				body := fmt.Sprintf("seed-%03d-confirmed-%d", seed, i)
				if _, err := c.Enqueue(queue, []client.EnqueueMsg{{Body: body}}); err != nil {
					t.Fatalf("seed %d pre-crash enqueue %d: %v", seed, i, err)
				}
				confirmed = append(confirmed, body)
			}
			rng := rand.New(rand.NewSource(int64(seed)))
			n := 300 + rng.Intn(100)
			batch := make([]client.EnqueueMsg, n)
			for i := range batch {
				batch[i] = client.EnqueueMsg{Body: fmt.Sprintf("seed-%03d-%03d-%016x-%s", seed, i, rng.Uint64(), strings.Repeat("x", 96))}
			}
			_, enqueueErr := c.Enqueue(queue, batch)
			if enqueueErr == nil {
				for _, msg := range batch {
					confirmed = append(confirmed, msg.Body)
				}
			}

			done := make(chan struct{})
			go func() { _ = p.cmd.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				_ = p.cmd.Process.Kill()
				<-done
				t.Fatalf("seed %d failpoint %s did not terminate the server; enqueue error: %v", seed, fp, enqueueErr)
			}
			if !strings.Contains(p.stderr.String(), "failpoint ") {
				t.Fatalf("seed %d exited without a failpoint hit: %s", seed, p.stderr.String())
			}

			p2 := startServer(t, dataDir)
			defer p2.kill()
			c2 := client.New("http://" + p2.addr)
			seen := map[string]bool{}
			for {
				msgs, err := c2.Dequeue(queue, 100, 100, 10_000)
				if err != nil {
					t.Fatal(err)
				}
				if len(msgs) == 0 {
					break
				}
				for _, msg := range msgs {
					seen[msg.Body] = true
					if err := c2.Ack(queue, msg.ID, msg.Token); err != nil && !client.IsCode(err, "lease_lost") {
						t.Fatalf("seed %d ack: %v", seed, err)
					}
				}
			}
			for _, body := range confirmed {
				if !seen[body] {
					t.Fatalf("seed %d failpoint %s lost confirmed enqueue %q", seed, fp, body)
				}
			}
			t.Logf("seed=%d failpoint=%s confirmed=%d batch=%d", seed, fp, len(confirmed), n)
		})
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
