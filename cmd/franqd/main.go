// franqd is the frankenqueue server: an HTTP queue where FIFO, LIFO,
// priority, delay and aging compose, backed by its own group-commit WAL.
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lgoyal6/franq/internal/core"
	"github.com/lgoyal6/franq/internal/server"
)

func main() {
	addr := flag.String("addr", ":1818", "listen address (1818: the year Frankenstein was published)")
	dataDir := flag.String("data", "./franq-data", "data directory (WAL segments + snapshots)")
	segmentBytes := flag.Int64("segment-bytes", 64<<20, "WAL segment size before rotation")
	retainBytes := flag.Int64("retain-bytes", 0, "sealed WAL bytes to keep for time-travel replay (0 = keep everything)")
	portFile := flag.String("port-file", "", "write the bound address to this file (for tests)")
	flag.Parse()

	start := time.Now()
	b, err := core.Open(*dataDir, core.Options{
		SegmentBytes: *segmentBytes,
		RetainBytes:  *retainBytes,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "franqd: recovery failed: %v\n", err)
		os.Exit(1)
	}
	rec := b.Recovery
	fmt.Printf("franqd: recovered %d queues, %d messages (snapshot=%v, tail records=%d, torn tails=%d) in %s\n",
		rec.Queues, rec.Messages, rec.HadSnapshot, rec.TailRecords, rec.TornTails,
		time.Since(start).Round(time.Millisecond))

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "franqd: listen: %v\n", err)
		os.Exit(1)
	}
	if *portFile != "" {
		if err := os.WriteFile(*portFile, []byte(ln.Addr().String()), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "franqd: port file: %v\n", err)
			os.Exit(1)
		}
	}

	rootCtx, cancel := context.WithCancel(context.Background())
	srv := &http.Server{
		Handler: server.New(b),
		// Long-polling dequeues park inside handlers; deriving every request
		// context from rootCtx lets shutdown release them all instantly.
		BaseContext: func(net.Listener) context.Context { return rootCtx },
	}

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		fmt.Println("franqd: shutting down")
		cancel()
		ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		srv.Shutdown(ctx)
	}()

	fmt.Printf("franqd: it's alive ⚡ listening on %s, data in %s\n", ln.Addr(), *dataDir)
	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		fmt.Fprintf(os.Stderr, "franqd: serve: %v\n", err)
		os.Exit(1)
	}
	b.Close()
	fmt.Println("franqd: state flushed, goodbye")
}
