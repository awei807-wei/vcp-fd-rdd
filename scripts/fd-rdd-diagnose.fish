#!/usr/bin/env fish
# fd-rdd 一键诊断脚本（fish）
# 用法:
#   ./scripts/fd-rdd-diagnose.fish
#   ./scripts/fd-rdd-diagnose.fish "30 min ago"

set since "2 hours ago"
if test (count $argv) -ge 1
    set since "$argv[1]"
end

set service "fd-rdd.service"
set status_url "http://127.0.0.1:6060/status"

echo "=== fd-rdd diagnose ==="
date "+%F %T %Z"
echo "since: $since"
echo

set pids (pidof fd-rdd 2>/dev/null | string split ' ')
if test (count $pids) -eq 0
    echo "[进程] 未找到 fd-rdd 进程"
else
    set pid $pids[1]
    echo "[进程] PID: $pid"
    ps -p $pid -o pid,etime,rss,cmd
    echo
    echo "[cmdline]"
    tr '\0' ' ' < /proc/$pid/cmdline
    echo
    echo
    echo "[smaps_rollup]"
    if test -r /proc/$pid/smaps_rollup
        if not cat /proc/$pid/smaps_rollup 2>/dev/null
            echo "读取 /proc/$pid/smaps_rollup 失败（可能是权限限制）"
        end
    else
        echo "无法读取 /proc/$pid/smaps_rollup"
    end
end

echo
echo "[HTTP /status]"
if not curl -sS --max-time 3 $status_url 2>/dev/null
    echo "请求 $status_url 失败（服务未监听或网络/权限限制）"
end
echo

echo
echo "[关键日志: overflow/rebuild/snapshot/compaction/version]"
set key_pat "Starting fd-rdd v|overflow|rebuild|Snapshot failed|Compaction"
if type -q rg
    journalctl --user -u $service -o cat --since "$since" | rg $key_pat
else
    journalctl --user -u $service -o cat --since "$since" | grep -E $key_pat
end

echo
echo "[最近 Memory Report 块]"
set tmp (mktemp)
journalctl --user -u $service -o cat --since "$since" | sed -n '/fd-rdd Memory Report/,+70p' | tail -n 120 > $tmp
if test -s $tmp
    cat $tmp
else
    echo "未捕获到 Memory Report（可能当前运行二进制未输出该报表，或日志窗口内无记录）"
end
rm -f $tmp

echo
echo "=== diagnose done ==="
