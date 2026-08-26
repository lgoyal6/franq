#!/usr/bin/env bash
# Run by demo.tape inside a Hide block, so none of this reaches the screen.
#
# Starts franqd detached on 18180 rather than its default 1818, with a scratch
# data directory: the recording begins from an empty queue and cannot write
# into a franqd the reader already has running on the usual port.
set -euo pipefail
cd "$(dirname "$0")"

go build -o bin/franqd ./cmd/franqd
go build -o bin/igor ./cmd/igor

DATA="$(mktemp -d)"
nohup ./bin/franqd -addr 127.0.0.1:18180 -data "$DATA" >/tmp/franq-demo.log 2>&1 &
echo "$! $DATA" > /tmp/franq-demo.state
disown

# Wait for it to answer rather than sleeping a guessed amount.
for _ in $(seq 1 60); do
  curl -sf -o /dev/null "http://127.0.0.1:18180/v1/queues" && exit 0
  sleep 0.1
done
echo "franqd did not come up; see /tmp/franq-demo.log" >&2
exit 1
