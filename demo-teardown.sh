#!/usr/bin/env bash
# Stops the franqd that demo-setup.sh started and removes its scratch data.
set -uo pipefail
[ -f /tmp/franq-demo.state ] || exit 0
read -r PID DATA < /tmp/franq-demo.state
kill "$PID" 2>/dev/null || true
rm -rf "$DATA" /tmp/franq-demo.state
