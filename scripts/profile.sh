#!/bin/bash
set -e
echo "=== CPU Profile (perf) ==="
perf record -g ./target/release/fd-rdd --root /tmp/fd-rdd-profile &
PID=$!
sleep 30
kill $PID
perf report --stdio | head -50

echo "=== Memory Profile (dhat) ==="
# cargo dhat run --release -- --root /tmp/fd-rdd-profile
