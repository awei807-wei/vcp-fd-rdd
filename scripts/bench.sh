#!/bin/bash
set -e
FD_RDD_PID=""
TEST_DIR="/tmp/fd-rdd-bench-$(date +%s)"
mkdir -p "$TEST_DIR"

echo "=== Compile Time ==="
time cargo build --release 2>&1 | tail -3

echo "=== Startup Time (with index) ==="
./target/release/fd-rdd --root "$TEST_DIR" &
FD_RDD_PID=$!
sleep 5
curl -s http://localhost:7878/status | jq '.indexed_count'
kill $FD_RDD_PID 2>/dev/null || true
wait $FD_RDD_PID 2>/dev/null || true

echo "=== Memory (RSS) ==="
./target/release/fd-rdd --root "$TEST_DIR" &
FD_RDD_PID=$!
sleep 10
RSS=$(cat /proc/$FD_RDD_PID/status | grep VmRSS | awk '{print $2}')
echo "RSS: ${RSS} kB"
kill $FD_RDD_PID 2>/dev/null || true
wait $FD_RDD_PID 2>/dev/null || true

echo "=== Idle CPU ==="
./target/release/fd-rdd --root "$TEST_DIR" &
FD_RDD_PID=$!
sleep 30
cpu_idle=$(pidstat -u -p $FD_RDD_PID 1 5 | tail -1 | awk '{print 100 - $8}')
echo "Idle CPU: ${cpu_idle}%"
kill $FD_RDD_PID 2>/dev/null || true
wait $FD_RDD_PID 2>/dev/null || true

echo "=== Query Performance ==="
./target/release/fd-rdd --root "$TEST_DIR" &
FD_RDD_PID=$!
sleep 10
time curl -s "http://localhost:7878/search?q=*.rs&limit=1000"
kill $FD_RDD_PID 2>/dev/null || true
wait $FD_RDD_PID 2>/dev/null || true

echo "=== Event Storm ==="
./target/release/fd-rdd --root "$TEST_DIR" &
FD_RDD_PID=$!
sleep 5
mkdir -p "$TEST_DIR/storm"
for i in $(seq 1 10000); do touch "$TEST_DIR/storm/file_$i.txt"; done
echo "Created 10000 files"
sleep 5
curl -s "http://localhost:7878/search?q=file_*.txt&limit=10000" | jq '.total'
kill $FD_RDD_PID 2>/dev/null || true
wait $FD_RDD_PID 2>/dev/null || true

rm -rf "$TEST_DIR"
echo "=== Benchmark Complete ==="
