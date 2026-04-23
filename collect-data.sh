#!/bin/bash
# fd-rdd 数据收集脚本 - 后台长期运行
# 用法: nohup bash collect-data.sh &
# 然后可以关闭终端

set -e

FD_PID=$(pgrep -x fd-rdd | head -1)
if [ -z "$FD_PID" ]; then
    echo "ERROR: fd-rdd not running"
    exit 1
fi

echo "collector started at $(date -Iseconds), fd-rdd PID=$FD_PID"
echo "logs will be saved to /tmp/fd-rdd-*.log"

# 1. 收集 /proc 内存时序数据 (每 30 秒一次，共 120 次 = 60 分钟)
(
    for i in $(seq 1 120); do
        PID=$(pgrep -x fd-rdd | head -1)
        [ -z "$PID" ] && break

        echo "=== $(date -Iseconds) i=$i ===" >> /tmp/fd-rdd-proc.log
        cat /proc/$PID/status | grep -E 'VmRSS|VmSize|VmPeak|VmData|Threads|RssAnon|RssFile' >> /tmp/fd-rdd-proc.log
        cat /proc/$PID/smaps_rollup | grep -E 'Rss|Pss|Private_Dirty|Anonymous|LazyFree' >> /tmp/fd-rdd-proc.log
        echo "fd_count=$(ls -l /proc/$PID/fd 2>/dev/null | wc -l)" >> /tmp/fd-rdd-proc.log
        echo "---" >> /tmp/fd-rdd-proc.log

        sleep 30
    done
    echo "proc collector finished at $(date -Iseconds)" >> /tmp/fd-rdd-proc.log
) &

# 2. 定期提取 fd-rdd 日志中的 memory_report (每 60 秒一次)
(
    for i in $(seq 1 60); do
        grep "memory_report:" /tmp/fd-rdd-debug.log | tail -1 >> /tmp/fd-rdd-memreports.log
        sleep 60
    done
    echo "memreport collector finished at $(date -Iseconds)" >> /tmp/fd-rdd-memreports.log
) &

# 3. 查询可用性测试 (每 2 分钟一次)
(
    for i in $(seq 1 30); do
        echo "=== $(date -Iseconds) test=$i ===" >> /tmp/fd-rdd-query.log
        timeout 5 curl -s 'http://127.0.0.1:6060/search?q=README' >> /tmp/fd-rdd-query.log 2>&1 || echo "TIMEOUT_OR_FAIL" >> /tmp/fd-rdd-query.log
        echo "" >> /tmp/fd-rdd-query.log
        sleep 120
    done
    echo "query collector finished at $(date -Iseconds)" >> /tmp/fd-rdd-query.log
) &

# 4. 新增文件同步测试 (在 5 分钟、15 分钟、30 分钟时各测试一次)
(
    sleep 300
    TS=$(date +%s)
    TESTFILE="/home/shiyi/.fd-rdd-test-$TS.txt"
    echo "test-sync-$TS" > "$TESTFILE"
    echo "=== $(date -Iseconds) create_testfile=$TESTFILE ===" >> /tmp/fd-rdd-sync.log
    sleep 10
    timeout 5 curl -s "http://127.0.0.1:6060/search?q=fd-rdd-test-$TS" >> /tmp/fd-rdd-sync.log 2>&1 || echo "TIMEOUT" >> /tmp/fd-rdd-sync.log
    rm -f "$TESTFILE"

    sleep 600
    TS=$(date +%s)
    TESTFILE="/home/shiyi/.fd-rdd-test-$TS.txt"
    echo "test-sync-$TS" > "$TESTFILE"
    echo "=== $(date -Iseconds) create_testfile=$TESTFILE ===" >> /tmp/fd-rdd-sync.log
    sleep 10
    timeout 5 curl -s "http://127.0.0.1:6060/search?q=fd-rdd-test-$TS" >> /tmp/fd-rdd-sync.log 2>&1 || echo "TIMEOUT" >> /tmp/fd-rdd-sync.log
    rm -f "$TESTFILE"

    sleep 900
    TS=$(date +%s)
    TESTFILE="/home/shiyi/.fd-rdd-test-$TS.txt"
    echo "test-sync-$TS" > "$TESTFILE"
    echo "=== $(date -Iseconds) create_testfile=$TESTFILE ===" >> /tmp/fd-rdd-sync.log
    sleep 10
    timeout 5 curl -s "http://127.0.0.1:6060/search?q=fd-rdd-test-$TS" >> /tmp/fd-rdd-sync.log 2>&1 || echo "TIMEOUT" >> /tmp/fd-rdd-sync.log
    rm -f "$TESTFILE"

    echo "sync collector finished at $(date -Iseconds)" >> /tmp/fd-rdd-sync.log
) &

echo "All collectors started. PIDs:"
jobs -p
echo ""
echo "To check progress:"
echo "  tail -f /tmp/fd-rdd-debug.log         # fd-rdd 主日志"
echo "  tail -f /tmp/fd-rdd-proc.log          # /proc 内存数据"
echo "  tail -f /tmp/fd-rdd-memreports.log     # memory_report 摘要"
echo "  tail -f /tmp/fd-rdd-query.log         # 查询测试结果"
echo "  tail -f /tmp/fd-rdd-sync.log          # 新增文件同步测试"
echo ""
echo "To stop everything:"
echo "  kill $(pgrep -x fd-rdd)"
echo "  kill $(jobs -p)"
