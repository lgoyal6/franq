# Contributing to franq

Thanks for looking. franq is a queue server small enough to read end to end in a
sitting, and the constraints below are what keep it that way.

## The two rules that are not negotiable

**Zero dependencies.** `go.mod` has no `require` block and should stay that way.
The whole point of the storage engine is that it is files and `fsync`, not a
delegated database. A PR that adds a module dependency needs to argue why the
standard library genuinely cannot do it.

**Nothing is externally observable before its record is fsynced.** Enqueue 200s,
ack 200s and heap insertion all happen after the committer returns. This is the
invariant that lets recovery truncate a torn tail and know it dropped only
operations nobody was told succeeded. If your change moves work to the wrong side
of that boundary, the crash matrix will catch it, and it will not land.

## Getting oriented

| Path | What lives there |
|---|---|
| `internal/wal/` | Records, segments, group commit, snapshots, recovery, failpoints |
| `internal/core/` | Keys, heaps, queues, leases, the broker/applier, time-travel replay |
| `internal/server/` | HTTP handlers and DTOs |
| `internal/client/` | Go client, shared by the CLI and the tests |
| `cmd/franqd`, `cmd/igor` | Server binary and the client CLI |
| `test/crash/` | The `kill -9` matrix |

The README's "The write path" and "Recovery" diagrams are the shortest accurate
description of how those fit together. Read them first.

## Building and testing

```bash
make build        # bin/franqd, bin/igor
make test         # go test ./...
make race         # the suite under -race, which is how CI runs it
make vet
make crashtest    # the kill -9 matrix, ~5 minutes
make bench        # group-commit throughput
```

`make race` and `make crashtest` are the two that matter. Anything touching
`internal/wal/` or `internal/core/` should pass both locally before you open a PR.

## What makes a good PR here

- One concern per PR, with a test that fails before and passes after.
- Changes to the commit path, recovery, or snapshotting need a case in
  `test/crash/`, ideally driven through an existing failpoint in
  `internal/wal/failpoint.go` rather than a new bespoke harness.
- A single applier goroutine mutates queue state in exact log order. That is what
  makes snapshot cuts consistent without conflict resolution. Introducing a second
  writer is a redesign, not a patch, so open an issue first.
- Benchmark numbers in the README come from `make bench` on an M3 Pro. If your
  change moves them, include before and after output rather than editing the table.
- Leases are deliberately not persisted, and dequeue is deliberately memory-only.
  Persisting them is a real design discussion, not a bug fix.

## Good first areas

- Metrics: there is no `/metrics` endpoint, and the committer already has the
  batch-size and fsync-latency numbers a Prometheus exporter would want.
- The long-poll waiter path is worth a second reader for lost-wakeup edge cases;
  registration shares the mutex with insertion and notify, and that coupling is
  subtle.
- Retention policy: `-retain-bytes` is byte-based only; a time-based policy is a
  self-contained addition in `internal/wal/`.

## Conduct

Be decent. Disagree about the code, not about the person.
