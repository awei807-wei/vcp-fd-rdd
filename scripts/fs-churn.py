#!/usr/bin/env python3
# fs-churn.py - 文件系统事件压力生成器（用于 watcher / EventPipeline / Flush/Compaction 回归）
#
# 设计目标：
# - 用“高频文件增删改名”在几分钟内模拟长时间运行的事件量
# - 可配：初始文件数、操作总量、各操作比例、最大文件数上限
# - 安全：仅在 root 内操作；清理/重置需要 marker 文件
#
# 用法示例：
#   python3 scripts/fs-churn.py --root /tmp/fd-rdd-churn --reset --populate 20000 --ops 200000
#   python3 scripts/fs-churn.py --root /tmp/fd-rdd-churn --ops 500000 --sleep-ms 1
#
# 长期不涨（plateau/soak）验证示例（需要提供 fd-rdd 的 PID）：
#   python3 scripts/fs-churn.py \
#     --root /tmp/fd-rdd-churn --reset --populate 20000 \
#     --ops 200000 --max-files 20000 \
#     --rounds 10 --settle-secs 20 \
#     --fd-pid 12345 --fd-metric pd --max-growth-mb 8
#   注：--fd-metric=pd 且可解析 MemoryReport 时，--max-growth-mb 实际比较的是
#       unaccounted=max(0, pdΔ-disk_tomb_estΔ)，用于避免把 LSM 的结构性增长误判为泄漏。
#
#   若无法读取 /proc/<pid>/smaps_rollup（权限限制），可：
#   - 把 --fd-metric 改为 rss（会 fallback 到 /proc/<pid>/statm）
#   - 或用 --spawn-fd 让脚本启动 fd-rdd（成为父进程），继续检查 pd/pc/pss

from __future__ import annotations

import argparse
import json
import os
import random
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path


MARKER_NAME = ".fd-rdd-churn-root"
MiB = 1024 * 1024
KiB = 1024
GiB = 1024 * 1024 * 1024

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
_HUMAN_BYTES_RE = re.compile(r"([0-9]+(?:\.[0-9]+)?)\s*(GB|MB|KB|B)")
_OVERFLOW_RE = re.compile(r"event channel overflow, total drops: (\d+)")
_REBUILD_TRIGGER_RE = re.compile(
    r"Event overflow delta reached threshold: delta=(\d+) threshold=(\d+), triggering rebuild"
)
_FAST_SYNC_TRIGGER_RE = re.compile(r"Event overflow recovery: triggering fast-sync")
_HEAP_SIGNAL_RE = re.compile(
    r"\[heap-signal\]\s+index_est_bytes=(\d+)\s+non_index_pd_bytes=(\d+)\s+suspected=(true|false)\s+rss_trend_mb_per_min=([+-]?[0-9]+(?:\.[0-9]+)?)"
)
_SMAPS_HEADER_RE = re.compile(r"^[0-9a-fA-F]+-[0-9a-fA-F]+\s")


def _strip_ansi(s: str) -> str:
    return _ANSI_RE.sub("", s)


def _parse_human_bytes(s: str) -> int | None:
    m = _HUMAN_BYTES_RE.search(s)
    if m is None:
        return None
    value_s, unit = m.group(1), m.group(2)
    try:
        value = float(value_s)
    except ValueError:
        return None
    scale = {"B": 1, "KB": KiB, "MB": MiB, "GB": GiB}.get(unit)
    if scale is None:
        return None
    return int(value * scale)


def _fmt_bytes(n: int | None) -> str:
    if n is None:
        return "N/A"
    if n >= GiB:
        return f"{n / GiB:.2f} GB"
    if n >= MiB:
        return f"{n / MiB:.2f} MB"
    if n >= KiB:
        return f"{n / KiB:.2f} KB"
    return f"{n} B"


def _disk_tomb_delta_bytes(
    baseline_rep: "ParsedReport | None", cur_rep: "ParsedReport | None"
) -> int | None:
    if baseline_rep is None or cur_rep is None:
        return None
    a = baseline_rep.disk_tomb_est_bytes
    b = cur_rep.disk_tomb_est_bytes
    if a is None or b is None:
        return None
    if b <= a:
        return 0
    return b - a


@dataclass
class HeapSignal:
    ts: float
    index_est_bytes: int
    non_index_pd_bytes: int
    suspected: bool
    rss_trend_mb_per_min: float


@dataclass
class ParsedReport:
    ts: float
    process_rss_bytes: int | None = None
    smaps_pd_bytes: int | None = None
    heap_idx_bytes: int | None = None
    heap_non_idx_pd_bytes: int | None = None
    heap_high_water_suspected: bool | None = None
    l2_total_bytes: int | None = None
    disk_tomb_est_bytes: int | None = None
    disk_tomb_max_bytes: int | None = None
    overlay_est_bytes: int | None = None
    rebuild_est_bytes: int | None = None
    rebuild_in_progress: bool | None = None
    event_overflow_drops: int | None = None
    event_rescan_signals: int | None = None
    event_raw_cap: int | None = None
    event_merged_cap: int | None = None
    event_records_cap: int | None = None


def _parse_memory_report(lines: list[str], ts: float) -> ParsedReport:
    r = ParsedReport(ts=ts)
    for raw in lines:
        line = raw.strip()

        if "Process RSS:" in line:
            r.process_rss_bytes = _parse_human_bytes(line)
            continue

        if line.startswith("║ Smaps:"):
            # 形如: rss=.. pss=.. pc=..
            matches = _HUMAN_BYTES_RE.findall(line)
            if len(matches) >= 3:
                # rss/pss/pc 不一定用于归因，先不存；只解析更关注的 pd 在下一行。
                pass
            continue

        if "pd=" in line and "Smaps:" not in line:
            r.smaps_pd_bytes = _parse_human_bytes(line)
            continue

        if "Heap:" in line:
            # 形如: Heap: idx=.. non-idx-pd=..
            matches = _HUMAN_BYTES_RE.findall(line)
            if len(matches) >= 2:
                r.heap_idx_bytes = _parse_human_bytes(" ".join(matches[0]))
                r.heap_non_idx_pd_bytes = _parse_human_bytes(" ".join(matches[1]))
            continue

        if "high-water-suspected=" in line:
            if "true" in line:
                r.heap_high_water_suspected = True
            elif "false" in line:
                r.heap_high_water_suspected = False
            continue

        if "L2 total:" in line:
            r.l2_total_bytes = _parse_human_bytes(line)
            continue

        if "tomb est:" in line:
            matches = _HUMAN_BYTES_RE.findall(line)
            if len(matches) >= 2:
                r.disk_tomb_est_bytes = _parse_human_bytes(" ".join(matches[0]))
                r.disk_tomb_max_bytes = _parse_human_bytes(" ".join(matches[1]))
            continue

        if "overlay est:" in line:
            r.overlay_est_bytes = _parse_human_bytes(line)
            continue

        if "rebuild est:" in line:
            r.rebuild_est_bytes = _parse_human_bytes(line)
            if "in_progress=true" in line:
                r.rebuild_in_progress = True
            elif "in_progress=false" in line:
                r.rebuild_in_progress = False
            continue

        m = re.search(r"overflow:\s+(\d+)\b", line)
        if m is not None:
            try:
                r.event_overflow_drops = int(m.group(1))
            except ValueError:
                pass
            continue

        m = re.search(r"rescan:\s+(\d+)\b", line)
        if m is not None:
            try:
                r.event_rescan_signals = int(m.group(1))
            except ValueError:
                pass
            continue

        m = re.search(r"raw cap:\s+(\d+)\b", line)
        if m is not None:
            try:
                r.event_raw_cap = int(m.group(1))
            except ValueError:
                pass
            continue

        m = re.search(r"merged cap:\s+(\d+)\b", line)
        if m is not None:
            try:
                r.event_merged_cap = int(m.group(1))
            except ValueError:
                pass
            continue

        m = re.search(r"records cap:\s+(\d+)\b", line)
        if m is not None:
            try:
                r.event_records_cap = int(m.group(1))
            except ValueError:
                pass
            continue

    return r


class FdLogState:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self.overflow_total: int = 0
        self.fast_sync_trigger_count: int = 0
        self.rebuild_trigger_count: int = 0
        self.last_rebuild_delta: int | None = None
        self.last_rebuild_threshold: int | None = None
        self.heap_signals: list[HeapSignal] = []
        self.reports: list[ParsedReport] = []

        self._in_report = False
        self._report_lines: list[str] = []

    def on_line(self, raw_line: str) -> None:
        ts = time.time()
        line = _strip_ansi(raw_line).rstrip("\n")

        with self._cond:
            m = _OVERFLOW_RE.search(line)
            if m is not None:
                try:
                    self.overflow_total = max(self.overflow_total, int(m.group(1)))
                except ValueError:
                    pass
                self._cond.notify_all()

            m = _REBUILD_TRIGGER_RE.search(line)
            if m is not None:
                self.rebuild_trigger_count += 1
                try:
                    self.last_rebuild_delta = int(m.group(1))
                    self.last_rebuild_threshold = int(m.group(2))
                except ValueError:
                    self.last_rebuild_delta = None
                    self.last_rebuild_threshold = None
                self._cond.notify_all()

            m = _FAST_SYNC_TRIGGER_RE.search(line)
            if m is not None:
                self.fast_sync_trigger_count += 1
                self._cond.notify_all()

            m = _HEAP_SIGNAL_RE.search(line)
            if m is not None:
                try:
                    index_est = int(m.group(1))
                    non_idx = int(m.group(2))
                    suspected = m.group(3) == "true"
                    trend = float(m.group(4))
                    self.heap_signals.append(
                        HeapSignal(
                            ts=ts,
                            index_est_bytes=index_est,
                            non_index_pd_bytes=non_idx,
                            suspected=suspected,
                            rss_trend_mb_per_min=trend,
                        )
                    )
                    # 限制内存：只保留最近 512 条
                    if len(self.heap_signals) > 512:
                        self.heap_signals = self.heap_signals[-512:]
                except ValueError:
                    pass
                self._cond.notify_all()

            # MemoryReport box capture（对日志前缀更鲁棒：不要求行首就是 box 字符）
            if not self._in_report and "╔" in line and "════" in line:
                self._in_report = True
                self._report_lines = [line]
                return
            if self._in_report:
                self._report_lines.append(line)
                if "╚" in line and "════" in line:
                    rep = _parse_memory_report(self._report_lines, ts=ts)
                    self.reports.append(rep)
                    if len(self.reports) > 256:
                        self.reports = self.reports[-256:]
                    self._in_report = False
                    self._report_lines = []
                    self._cond.notify_all()

    def latest_heap_signal(self) -> HeapSignal | None:
        with self._lock:
            return self.heap_signals[-1] if self.heap_signals else None

    def latest_report(self) -> ParsedReport | None:
        with self._lock:
            return self.reports[-1] if self.reports else None

    def wait_for_first_samples_after(
        self, after_ts: float, timeout_secs: float
    ) -> tuple[HeapSignal | None, ParsedReport | None]:
        deadline = time.time() + max(0.0, timeout_secs)
        with self._cond:
            while True:
                hs = None
                for x in reversed(self.heap_signals):
                    if x.ts >= after_ts:
                        hs = x
                        break
                rep = None
                for x in reversed(self.reports):
                    if x.ts >= after_ts:
                        rep = x
                        break
                if hs is not None or rep is not None:
                    return hs, rep
                remain = deadline - time.time()
                if remain <= 0:
                    return None, None
                self._cond.wait(timeout=remain)


def _start_fd_log_capture(proc: subprocess.Popen[str], echo: bool) -> tuple[FdLogState, threading.Thread]:
    state = FdLogState()

    def _reader() -> None:
        assert proc.stdout is not None
        for line in proc.stdout:
            state.on_line(line)
            if echo:
                # fd-rdd 输出可能很吵；默认在 --verdict 下关闭 echo，可用 --fd-echo 打开。
                sys.stdout.write(line)
                sys.stdout.flush()

    t = threading.Thread(target=_reader, name="fd-rdd-log-reader", daemon=True)
    t.start()
    return state, t


def _workspace_source_mtime() -> float:
    mt = 0.0
    for p in (Path("Cargo.toml"), Path("Cargo.lock")):
        try:
            mt = max(mt, p.stat().st_mtime)
        except OSError:
            pass

    for root in (Path("src"), Path("tests")):
        if not root.exists():
            continue
        for p in root.rglob("*.rs"):
            try:
                mt = max(mt, p.stat().st_mtime)
            except OSError:
                pass
    return mt


def _auto_detect_fd_bin(fd_bin: str) -> tuple[str | None, str | None]:
    if fd_bin:
        note = None
        resolved: Path | None = None
        p = Path(fd_bin)
        if p.exists():
            resolved = p
        else:
            w = shutil.which(fd_bin)
            if w:
                wp = Path(w)
                if wp.exists():
                    resolved = wp
        if resolved is not None:
            try:
                mt = resolved.stat().st_mtime
                src_mtime = _workspace_source_mtime()
                if mt < src_mtime:
                    note = (
                        "[warn] auto-spawn 检测到 --fd-bin 指定的二进制可能已过期："
                        f"binary_mtime={mt:.0f} < src_mtime={src_mtime:.0f}。"
                        "建议先运行 `cargo build --release`，或用 --fd-bin 指定最新产物。"
                    )
            except OSError:
                pass
        return fd_bin, note

    # 优先使用仓库内的构建产物（更可控），并尽量避免“release 存在但已过期”导致误判。
    release = Path("./target/release/fd-rdd")
    debug = Path("./target/debug/fd-rdd")

    candidates: list[tuple[Path, float]] = []
    for p in (release, debug):
        if not p.exists():
            continue
        try:
            candidates.append((p, p.stat().st_mtime))
        except OSError:
            continue

    if candidates:
        src_mtime = _workspace_source_mtime()
        fresh = [(p, mt) for p, mt in candidates if mt >= src_mtime]
        if fresh:
            # 两者都 fresh 时优先 release（更接近真实内存表现）。
            if any(p == release for p, _ in fresh):
                return str(release), None
            best_p, _ = max(fresh, key=lambda x: x[1])
            return (
                str(best_p),
                "[warn] auto-spawn 使用 debug 构建产物（release 不存在或不可用）；如需更贴近真实内存表现建议运行 `cargo build --release`。",
            )

        best_p, best_mt = max(candidates, key=lambda x: x[1])
        note = (
            "[warn] auto-spawn 检测到 fd-rdd 二进制可能已过期："
            f"binary_mtime={best_mt:.0f} < src_mtime={src_mtime:.0f}。"
            "建议先运行 `cargo build --release`，或用 --fd-bin 指定最新产物。"
        )
        return str(best_p), note

    return shutil.which("fd-rdd"), None


def _auto_spawn_fd_cmd(
    fd_bin: str,
    root: Path,
    report_interval_secs: int,
    trim_interval_secs: int,
    trim_pd_threshold_mb: int,
    event_channel_size: int,
    debounce_ms: int,
) -> list[str]:
    snapshot_path = root / "index.db"
    return [
        fd_bin,
        "--root",
        str(root),
        "--snapshot-path",
        str(snapshot_path),
        "--http-port",
        "0",
        "--no-build",
        "--no-snapshot",
        "--snapshot-interval-secs",
        "0",
        "--report-interval-secs",
        str(report_interval_secs),
        "--trim-interval-secs",
        str(trim_interval_secs),
        "--trim-pd-threshold-mb",
        str(trim_pd_threshold_mb),
        "--event-channel-size",
        str(event_channel_size),
        "--debounce-ms",
        str(debounce_ms),
        "--auto-flush-overlay-paths",
        "5000",
        "--auto-flush-overlay-bytes",
        "0",
    ]


def _int_arg(v: str) -> int:
    # 容错：复制粘贴时容易带上中文/英文右括号（例如 `20000）` / `20000)`）
    v = v.strip().rstrip(")）")
    return int(v)


def _float_arg(v: str) -> float:
    v = v.strip().rstrip(")）")
    return float(v)


def _write_small_file(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # 少量写入，确保产生 Create/Modify 类事件
    with open(path, "ab", buffering=0) as f:
        f.write(b"x")


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.utime(path, None)
    except FileNotFoundError:
        _write_small_file(path)


def _ensure_marker(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    marker = root / MARKER_NAME
    if not marker.exists():
        marker.write_text("fd-rdd fs-churn marker\n", encoding="utf-8")


def _require_marker(root: Path) -> None:
    marker = root / MARKER_NAME
    if not marker.exists():
        raise SystemExit(
            f"拒绝操作：{root} 下未发现 marker 文件 {MARKER_NAME}；"
            f"请先运行一次脚本创建，或手动创建该文件后再执行 --reset/--cleanup。"
        )


def _reset_root(root: Path) -> None:
    if root.exists():
        _require_marker(root)
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)
    _ensure_marker(root)


def _cleanup_root(root: Path) -> None:
    if not root.exists():
        return
    _require_marker(root)
    shutil.rmtree(root)


def _pick_dir(dir_fanout: int, idx: int) -> str:
    # 让目录分布更均匀，避免单目录过大导致 ext4 退化/notify 噪声
    return f"d{idx % dir_fanout:03d}"


def _file_path(root: Path, dir_fanout: int, file_id: int, suffix: str = ".txt") -> Path:
    return root / _pick_dir(dir_fanout, file_id) / f"f{file_id:08d}{suffix}"


def _human_mb(n: int) -> str:
    return f"{n / MiB:.2f} MB"


def _read_smaps_rollup_bytes(pid: int) -> dict[str, int] | None:
    p = Path(f"/proc/{pid}/smaps_rollup")
    try:
        s = p.read_text(encoding="utf-8")
    except OSError:
        return None

    out: dict[str, int] = {}
    for line in s.splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        key, val, unit = parts[0], parts[1], parts[2]
        if unit != "kB":
            continue
        try:
            kb = int(val)
        except ValueError:
            continue
        out[key.rstrip(":")] = kb * 1024
    return out


def _read_smaps_pd_breakdown(pid: int) -> tuple[dict[str, int], list[tuple[str, int]]] | None:
    """
    best-effort 读取 /proc/<pid>/smaps，按 mapping 类型统计 Private_Dirty，并返回 top mappings。

    目标：回答“pd 增长主要来自哪里（stack/heap/anon/file）”，避免归因停留在猜测层面。
    """
    p = Path(f"/proc/{pid}/smaps")
    try:
        f = p.open("r", encoding="utf-8", errors="replace")
    except OSError:
        return None

    def _cat(name: str) -> str:
        if name.startswith("[stack"):
            return "stack"
        if name == "[heap]":
            return "heap"
        if name.startswith("[") and name.endswith("]"):
            return "special"
        if not name:
            return "anon"
        return "file"

    cats: dict[str, int] = {"stack": 0, "heap": 0, "anon": 0, "file": 0, "special": 0}
    entries: list[tuple[int, str]] = []
    cur_name: str | None = None
    cur_pd: int = 0

    def _flush() -> None:
        nonlocal cur_name, cur_pd
        if cur_name is None:
            return
        c = _cat(cur_name)
        cats[c] = cats.get(c, 0) + cur_pd
        if cur_pd > 0:
            entries.append((cur_pd, cur_name))

    with f:
        for line in f:
            if _SMAPS_HEADER_RE.match(line):
                _flush()
                parts = line.split()
                # 形如：addr perms offset dev inode [pathname...]
                cur_name = " ".join(parts[5:]) if len(parts) >= 6 else ""
                cur_pd = 0
                continue
            if line.startswith("Private_Dirty:"):
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        kb = int(parts[1])
                    except ValueError:
                        kb = 0
                    cur_pd = kb * 1024
        _flush()

    entries.sort(reverse=True, key=lambda x: x[0])
    top = [(name, pd) for pd, name in entries[:8]]
    return cats, top


def _short_map_name(name: str) -> str:
    if not name:
        return "<anon>"
    if name.startswith("[") and name.endswith("]"):
        return name
    try:
        p = Path(name)
        if len(p.parts) >= 2:
            return "/".join(p.parts[-2:])
        return p.name or name
    except Exception:
        return name


def _git_head() -> str | None:
    try:
        out = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return None
    return out or None


def _write_report_json(path: Path, payload: dict) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    except OSError as e:
        print(f"[warn] 写入 report-json 失败：{path} ({e})")


def _read_statm_rss_bytes(pid: int) -> int | None:
    # /proc/<pid>/statm: size resident shared text lib data dt
    # resident 是 RSS pages
    p = Path(f"/proc/{pid}/statm")
    try:
        s = p.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    parts = s.split()
    if len(parts) < 2:
        return None
    try:
        resident_pages = int(parts[1])
    except ValueError:
        return None
    page_size = os.sysconf("SC_PAGE_SIZE")
    return resident_pages * page_size


def _fd_metric_bytes(pid: int, metric: str) -> int | None:
    smaps = _read_smaps_rollup_bytes(pid)
    key_map = {
        "rss": "Rss",
        "pss": "Pss",
        "pc": "Private_Clean",
        "pd": "Private_Dirty",
    }
    k = key_map.get(metric)
    if k is None:
        raise SystemExit(f"未知 metric：{metric}（可选：rss/pss/pc/pd）")
    if smaps is not None:
        return smaps.get(k, 0)

    # Fallback：在 smaps_rollup 权限受限时，仍可用 rss 做 plateau。
    if metric == "rss":
        return _read_statm_rss_bytes(pid)
    return None


def _print_fd_smaps(pid: int) -> None:
    smaps = _read_smaps_rollup_bytes(pid)
    if smaps is None:
        rss = _read_statm_rss_bytes(pid)
        if rss is None:
            print(f"[fd] pid={pid} smaps_rollup=unavailable statm=unavailable")
        else:
            print(f"[fd] pid={pid} rss={_human_mb(rss)} (from statm) smaps_rollup=unavailable")
        return
    rss = smaps.get("Rss", 0)
    pss = smaps.get("Pss", 0)
    pc = smaps.get("Private_Clean", 0)
    pd = smaps.get("Private_Dirty", 0)
    print(
        "[fd] pid={} rss={} pss={} pc={} pd={}".format(
            pid, _human_mb(rss), _human_mb(pss), _human_mb(pc), _human_mb(pd)
        )
    )


@dataclass
class Weights:
    create: int
    delete: int
    rename: int
    modify: int

    def validate(self) -> None:
        total = self.create + self.delete + self.rename + self.modify
        if total != 100:
            raise SystemExit(f"操作比例必须相加为 100（当前={total}）")


def _choose_op(rng: random.Random, w: Weights) -> str:
    r = rng.randrange(100)
    if r < w.create:
        return "create"
    r -= w.create
    if r < w.delete:
        return "delete"
    r -= w.delete
    if r < w.rename:
        return "rename"
    return "modify"


def _run_churn(
    rng: random.Random,
    w: Weights,
    root: Path,
    dir_fanout: int,
    files: list[Path],
    next_id: int,
    ops: int,
    max_files: int,
    sleep_s: float,
    log_every: int,
    label: str,
) -> int:
    start = time.time()
    for i in range(1, ops + 1):
        op = _choose_op(rng, w)

        # 为避免无限增长，超额时强制 delete
        if max_files and len(files) > max_files:
            op = "delete"

        if op == "create" or not files:
            p = _file_path(root, dir_fanout, next_id)
            next_id += 1
            _write_small_file(p)
            files.append(p)
        elif op == "delete":
            idx = rng.randrange(len(files))
            p = files.pop(idx)
            try:
                p.unlink()
            except FileNotFoundError:
                pass
        elif op == "rename":
            idx = rng.randrange(len(files))
            p = files[idx]
            new_p = _file_path(root, dir_fanout, next_id, suffix=".ren")
            next_id += 1
            new_p.parent.mkdir(parents=True, exist_ok=True)
            try:
                p.rename(new_p)
            except FileNotFoundError:
                # 目标已不存在：退化为 create
                _write_small_file(new_p)
            files[idx] = new_p
        else:  # modify
            p = files[rng.randrange(len(files))]
            _touch(p)

        if sleep_s:
            time.sleep(sleep_s)

        if log_every and i % log_every == 0:
            elapsed = time.time() - start
            rate = i / elapsed if elapsed > 0 else 0.0
            print(f"[{label}] ops={i}/{ops} alive={len(files)} rate={rate:.1f}/s")

    elapsed = time.time() - start
    rate = ops / elapsed if elapsed > 0 else 0.0
    print(f"[{label}-done] ops={ops} alive={len(files)} elapsed_s={elapsed:.2f} rate={rate:.1f}/s")
    return next_id


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description="fd-rdd watcher 压力文件操作生成器")
    ap.add_argument("--root", type=Path, default=Path("/tmp/fd-rdd-churn"), help="测试根目录")
    ap.add_argument("--reset", action="store_true", help="开始前清空 root（需 marker）")
    ap.add_argument("--cleanup", action="store_true", help="结束后删除 root（需 marker）")
    ap.add_argument("--seed", type=_int_arg, default=1, help="随机种子（复现用）")

    ap.add_argument(
        "--verdict",
        action="store_true",
        help="输出 PASS/FAIL 结论并设置退出码（0=pass, 3=fail）；会自动启用 multi-round 检查并建议使用 --spawn-fd/--auto-spawn-fd。",
    )
    ap.add_argument(
        "--report-json",
        type=Path,
        default=None,
        help="将本次运行的结论与关键指标写入 JSON（便于外部 review/归档）；示例：--report-json /tmp/fs-churn-report.json",
    )
    ap.add_argument("--fd-echo", action="store_true", help="输出 fd-rdd 子进程日志（默认：--verdict 下关闭以减少噪声）")

    ap.add_argument("--dir-fanout", type=_int_arg, default=256, help="子目录分桶数量")
    ap.add_argument("--populate", type=_int_arg, default=20_000, help="预先创建的文件数")
    ap.add_argument(
        "--ops",
        type=_int_arg,
        default=200_000,
        help="每轮操作数（rounds>1 时总操作数=ops*rounds；warmup_rounds 也按每轮 ops 计）",
    )
    ap.add_argument(
        "--max-files",
        type=_int_arg,
        default=20_000,
        help="最大文件数上限（超过会优先 delete，避免磁盘爆炸）",
    )
    ap.add_argument("--sleep-ms", type=_int_arg, default=0, help="每次操作后的 sleep（毫秒）")
    ap.add_argument("--log-every", type=_int_arg, default=10_000, help="每 N 次操作打印一次进度")

    # “长期不涨”验证（round-based soak）
    ap.add_argument("--rounds", type=_int_arg, default=1, help="轮数（>1 时按轮执行并可做 plateau 检查）")
    ap.add_argument(
        "--warmup-rounds",
        type=_int_arg,
        default=None,
        help="在 baseline 之前先跑 N 轮 churn+settle（不计入检查轮数），用于把“第一段台阶上升”从漂移判定里剔除；--verdict 默认=1。",
    )
    ap.add_argument(
        "--settle-secs",
        type=_int_arg,
        default=20,
        help="每轮结束后的等待时间（秒，用于让 fd-rdd 完成 flush/compaction）",
    )
    ap.add_argument(
        "--fd-pid",
        type=_int_arg,
        default=0,
        help="fd-rdd PID（提供后可读取 /proc/<pid>/smaps_rollup 做自动检查；rss 指标在权限受限时会 fallback 到 statm）",
    )
    ap.add_argument(
        "--fd-metric",
        choices=["rss", "pss", "pc", "pd"],
        default="pd",
        help="自动检查的指标：rss/pss/pc/pd（默认 pd=Private_Dirty）",
    )
    ap.add_argument(
        "--max-growth-mb",
        type=_float_arg,
        default=8.0,
        help="相对 baseline 的最大允许增长（MB；仅在 --fd-pid>0 时生效；当 --fd-metric=pd 且可解析 MemoryReport 时，判定使用 unaccounted=max(0, pdΔ-disk_tomb_estΔ)）",
    )

    ap.add_argument("--create-pct", type=_int_arg, default=35)
    ap.add_argument("--delete-pct", type=_int_arg, default=35)
    ap.add_argument("--rename-pct", type=_int_arg, default=20)
    ap.add_argument("--modify-pct", type=_int_arg, default=10)

    ap.add_argument(
        "--auto-spawn-fd",
        action="store_true",
        help="自动启动仓库内构建的 fd-rdd 并用其 PID 做检查（无需手工 --fd-pid/--spawn-fd）。仅用于回归/压测环境。",
    )
    ap.add_argument(
        "--fd-bin",
        default="",
        help="--auto-spawn-fd 使用的 fd-rdd 可执行文件路径（默认自动探测 target/release/fd-rdd、target/debug/fd-rdd 或 PATH）",
    )
    ap.add_argument(
        "--fd-report-interval-secs",
        type=_int_arg,
        default=-1,
        help="--auto-spawn-fd 时的 fd-rdd MemoryReport 间隔（秒）；默认：--verdict=5，其它=0（禁用）",
    )
    ap.add_argument(
        "--fd-trim-interval-secs",
        type=_int_arg,
        default=-1,
        help="--auto-spawn-fd 时的 fd-rdd trim 检查间隔（秒，-1=按 --verdict 默认策略，0=禁用）",
    )
    ap.add_argument(
        "--fd-trim-pd-threshold-mb",
        type=_int_arg,
        default=-1,
        help="--auto-spawn-fd 时的 fd-rdd trim Private_Dirty 阈值（MB，-1=按 --verdict 默认策略）",
    )
    ap.add_argument(
        "--fd-event-channel-size",
        type=_int_arg,
        default=4096,
        help="--auto-spawn-fd 时的 watcher 事件 channel 容量（越大越不容易 overflow，但会占用更多内存）",
    )
    ap.add_argument(
        "--fd-debounce-ms",
        type=_int_arg,
        default=100,
        help="--auto-spawn-fd 时的 watcher debounce 窗口（毫秒）",
    )

    # 让脚本成为 fd-rdd 的父进程：在 yama/ptrace_scope 限制下也能读到 smaps_rollup（pd/pc/pss）
    ap.add_argument(
        "--fd-env",
        action="append",
        default=[],
        help="为 --auto-spawn-fd/--spawn-fd 设置环境变量 KEY=VALUE（可重复）；例如：--fd-env MIMALLOC_PURGE_DELAY=0",
    )
    ap.add_argument(
        "--spawn-fd",
        nargs=argparse.REMAINDER,
        help="启动 fd-rdd 命令并自动使用其 PID 做检查；请放在参数最后，例如：--spawn-fd ./target/release/fd-rdd ...",
    )
    ap.add_argument("--spawn-fd-warmup-secs", type=_int_arg, default=2, help="启动 fd-rdd 后等待 N 秒再开始 populate/churn")
    ap.add_argument("--spawn-fd-keep", action="store_true", help="脚本结束后不终止 fd-rdd（默认 SIGINT）")
    args = ap.parse_args(argv)

    cleanup_requested = bool(args.cleanup)
    echo_fd_logs = bool(args.fd_echo) or (not args.verdict)
    fd_logs: FdLogState | None = None
    fd_logs_thread: threading.Thread | None = None

    w = Weights(args.create_pct, args.delete_pct, args.rename_pct, args.modify_pct)
    w.validate()

    rounds = max(1, args.rounds)
    warmup_rounds = args.warmup_rounds
    if warmup_rounds is None:
        warmup_rounds = 1 if args.verdict else 0
    warmup_rounds = max(0, warmup_rounds)

    if args.reset:
        _reset_root(args.root)
    else:
        _ensure_marker(args.root)

    spawn_env_overrides: dict[str, str] = {}
    for item in args.fd_env:
        if "=" not in item:
            print(f"[error] --fd-env 需要 KEY=VALUE 格式（收到：{item!r}）")
            return 2
        k, v = item.split("=", 1)
        k = k.strip()
        if not k:
            print(f"[error] --fd-env KEY 不能为空（收到：{item!r}）")
            return 2
        spawn_env_overrides[k] = v

    spawn_proc: subprocess.Popen[str] | None = None
    spawn_cmd: list[str] | None = None
    if args.auto_spawn_fd and (args.spawn_fd is not None or args.fd_pid > 0):
        print("[error] --auto-spawn-fd 不能与 --spawn-fd/--fd-pid 同时使用。")
        return 2
    if args.verdict and args.fd_pid <= 0 and args.spawn_fd is None and not args.auto_spawn_fd:
        args.auto_spawn_fd = True

    if args.auto_spawn_fd:
        fd_bin, fd_bin_note = _auto_detect_fd_bin(args.fd_bin)
        if fd_bin is None:
            print("[error] 未找到 fd-rdd 可执行文件：请先运行 `cargo build --release` 或通过 --fd-bin 指定路径。")
            return 2
        if fd_bin_note:
            print(fd_bin_note)
        report_interval_secs = args.fd_report_interval_secs
        if report_interval_secs < 0:
            report_interval_secs = 5 if args.verdict else 0

        # --verdict 模式下，默认开启 fd-rdd 的条件性 trim 循环（更贴近“过载后可恢复”的验收口径）。
        # 可用 --fd-trim-interval-secs=0 显式关闭。
        if args.fd_trim_interval_secs < 0:
            args.fd_trim_interval_secs = 5 if args.verdict else 0
        if args.fd_trim_pd_threshold_mb < 0:
            args.fd_trim_pd_threshold_mb = 32 if args.verdict else 128
        cmd = _auto_spawn_fd_cmd(
            fd_bin,
            args.root,
            report_interval_secs=report_interval_secs,
            trim_interval_secs=max(0, args.fd_trim_interval_secs),
            trim_pd_threshold_mb=max(0, args.fd_trim_pd_threshold_mb),
            event_channel_size=max(1, args.fd_event_channel_size),
            debounce_ms=max(0, args.fd_debounce_ms),
        )
        spawn_cmd = cmd
        try:
            env = os.environ.copy()
            env.update(spawn_env_overrides)
            spawn_proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                env=env,
            )
        except OSError as e:
            print(f"[error] 启动 fd-rdd 失败：{e}")
            return 2
        args.fd_pid = int(spawn_proc.pid)
        fd_logs, fd_logs_thread = _start_fd_log_capture(spawn_proc, echo=echo_fd_logs)
        print(f"[spawn:auto] fd-pid={args.fd_pid} cmd={' '.join(cmd)}")
        if args.spawn_fd_warmup_secs > 0:
            print(f"[warmup] sleep {args.spawn_fd_warmup_secs}s")
            time.sleep(args.spawn_fd_warmup_secs)

    if args.spawn_fd is not None:
        if args.fd_pid > 0:
            print("[error] --spawn-fd 与 --fd-pid 不能同时使用（避免检查对象混淆）。")
            return 2
        cmd = list(args.spawn_fd)
        if cmd[:1] == ["--"]:
            cmd = cmd[1:]
        if not cmd:
            print("[error] --spawn-fd 需要提供待启动命令，例如：--spawn-fd ./target/release/fd-rdd ...")
            return 2
        spawn_cmd = cmd
        try:
            env = os.environ.copy()
            env.update(spawn_env_overrides)
            spawn_proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                env=env,
            )
        except OSError as e:
            print(f"[error] 启动 fd-rdd 失败：{e}")
            return 2
        args.fd_pid = int(spawn_proc.pid)
        fd_logs, fd_logs_thread = _start_fd_log_capture(spawn_proc, echo=echo_fd_logs)
        print(f"[spawn] fd-pid={args.fd_pid} cmd={' '.join(cmd)}")
        if args.spawn_fd_warmup_secs > 0:
            print(f"[warmup] sleep {args.spawn_fd_warmup_secs}s")
            time.sleep(args.spawn_fd_warmup_secs)

    try:
        rng = random.Random(args.seed)

        files: list[Path] = []
        next_id = 0

        # 1) populate
        start = time.time()
        for _ in range(max(0, args.populate)):
            p = _file_path(args.root, args.dir_fanout, next_id)
            next_id += 1
            _write_small_file(p)
            files.append(p)
        populate_ms = int((time.time() - start) * 1000)
        print(f"[populate] files={len(files)} root={args.root} cost_ms={populate_ms}")

        # 2) churn / soak
        sleep_s = max(0, args.sleep_ms) / 1000.0
        ops = max(0, args.ops)
        max_files = max(0, args.max_files)

        baseline_bytes: int | None = None
        baseline_smaps: tuple[dict[str, int], list[tuple[str, int]]] | None = None
        printed_growth_mode = False
        if args.fd_pid > 0:
            if args.settle_secs > 0:
                print(f"[settle] after populate: sleep {args.settle_secs}s")
                time.sleep(args.settle_secs)
            _print_fd_smaps(args.fd_pid)
            probe_bytes = _fd_metric_bytes(args.fd_pid, args.fd_metric)
            if probe_bytes is None:
                if args.fd_metric == "rss":
                    print(f"[error] 无法读取 /proc/{args.fd_pid}/smaps_rollup 或 statm；请确认 pid 正确且有权限。")
                else:
                    print(
                        f"[error] 无法读取 /proc/{args.fd_pid}/smaps_rollup（{args.fd_metric} 需要 smaps_rollup 权限）。"
                        "建议：使用 --spawn-fd 让脚本启动 fd-rdd（成为父进程），或临时改用 --fd-metric rss。"
                    )
                return 2
            if warmup_rounds <= 0:
                baseline_bytes = probe_bytes
                if args.verdict and args.fd_metric == "pd":
                    baseline_smaps = _read_smaps_pd_breakdown(args.fd_pid)
                print(
                    "[baseline] metric={} value={} max_growth={:.2f} MB".format(
                        args.fd_metric, _human_mb(baseline_bytes), args.max_growth_mb
                    )
                )
                if args.verdict and args.fd_metric == "pd" and not printed_growth_mode:
                    print(
                        "[verdict] growth_mode=unaccounted(pdΔ-disk_tomb_estΔ) threshold={:.2f} MB (fallback: total pdΔ if MemoryReport missing)".format(
                            args.max_growth_mb
                        )
                    )
                    printed_growth_mode = True
            else:
                print(
                    "[probe] metric={} value={} (will warm up {} rounds before baseline)".format(
                        args.fd_metric, _human_mb(probe_bytes), warmup_rounds
                    )
                )

        # verdict 模式：默认启用多轮检查（否则单轮不足以判断“长期不涨”）
        if args.verdict and rounds <= 1:
            rounds = 6
            print(f"[verdict] override rounds -> {rounds} (use --rounds to customize)")

        should_check = baseline_bytes is not None and (args.verdict or rounds > 1)
        baseline_t: float | None = None
        samples: list[tuple[float, int]] = []
        verdict_growth_samples: list[tuple[float, int]] = []
        baseline_heap: HeapSignal | None = None
        baseline_rep: ParsedReport | None = None

        if args.fd_pid > 0 and baseline_bytes is not None:
            baseline_t = time.time()

        if warmup_rounds > 0:
            total_ops = ops * warmup_rounds
            print(
                f"[warmup-rounds] rounds={warmup_rounds} ops_per_round={ops} total_ops={total_ops} settle_secs={args.settle_secs}"
            )
            for r in range(1, warmup_rounds + 1):
                next_id = _run_churn(
                    rng=rng,
                    w=w,
                    root=args.root,
                    dir_fanout=args.dir_fanout,
                    files=files,
                    next_id=next_id,
                    ops=ops,
                    max_files=max_files,
                    sleep_s=sleep_s,
                    log_every=args.log_every,
                    label=f"w{r}",
                )
                if args.settle_secs > 0:
                    print(f"[settle] warmup round {r}: sleep {args.settle_secs}s")
                    time.sleep(args.settle_secs)
                if args.fd_pid > 0:
                    _print_fd_smaps(args.fd_pid)

            if args.fd_pid > 0:
                cur = _fd_metric_bytes(args.fd_pid, args.fd_metric)
                if cur is None:
                    print(f"[error] warmup: 无法读取 /proc/{args.fd_pid}/smaps_rollup")
                    return 2
                baseline_bytes = cur
                baseline_t = time.time()
                should_check = baseline_bytes is not None and (args.verdict or rounds > 1)
                if args.verdict and args.fd_metric == "pd":
                    baseline_smaps = _read_smaps_pd_breakdown(args.fd_pid)
                print(
                    "[baseline-after-warmup] metric={} value={} max_growth={:.2f} MB".format(
                        args.fd_metric, _human_mb(baseline_bytes), args.max_growth_mb
                    )
                )
                if args.verdict and args.fd_metric == "pd" and not printed_growth_mode:
                    print(
                        "[verdict] growth_mode=unaccounted(pdΔ-disk_tomb_estΔ) threshold={:.2f} MB (fallback: total pdΔ if MemoryReport missing)".format(
                            args.max_growth_mb
                        )
                    )
                    printed_growth_mode = True

        if baseline_t is not None and fd_logs is not None and args.verdict:
            # 尽量抓一份 baseline 的 MemoryReport/heap-signal，用于归因。
            hs, rep = fd_logs.wait_for_first_samples_after(baseline_t, timeout_secs=10)
            baseline_heap = hs
            baseline_rep = rep

        if args.verdict and baseline_bytes is None:
            print("[error] --verdict 需要可读取的 fd 指标：请使用 --auto-spawn-fd 或 --spawn-fd/--fd-pid。")
            return 2

        # 3) 进入检查轮
        if rounds == 1:
            next_id = _run_churn(
                rng=rng,
                w=w,
                root=args.root,
                dir_fanout=args.dir_fanout,
                files=files,
                next_id=next_id,
                ops=ops,
                max_files=max_files,
                sleep_s=sleep_s,
                log_every=args.log_every,
                label="churn",
            )
        else:
            total_ops = ops * rounds
            print(f"[soak] rounds={rounds} ops_per_round={ops} total_ops={total_ops} settle_secs={args.settle_secs}")
            for r in range(1, rounds + 1):
                next_id = _run_churn(
                    rng=rng,
                    w=w,
                    root=args.root,
                    dir_fanout=args.dir_fanout,
                    files=files,
                    next_id=next_id,
                    ops=ops,
                    max_files=max_files,
                    sleep_s=sleep_s,
                    log_every=args.log_every,
                    label=f"r{r}",
                )

                if args.settle_secs > 0:
                    print(f"[settle] round {r}: sleep {args.settle_secs}s")
                    time.sleep(args.settle_secs)

                if baseline_bytes is not None:
                    _print_fd_smaps(args.fd_pid)
                    cur = _fd_metric_bytes(args.fd_pid, args.fd_metric)
                    if cur is None:
                        print(f"[error] round {r}: 无法读取 /proc/{args.fd_pid}/smaps_rollup")
                        return 2
                    now = time.time()
                    growth_bytes = cur - baseline_bytes
                    growth_mb = growth_bytes / MiB
                    disk_tomb_delta_bytes: int | None = None
                    verdict_growth_bytes = growth_bytes
                    verdict_growth_mb = growth_mb
                    cur_rep_for_growth = fd_logs.latest_report() if fd_logs is not None else None
                    if args.fd_metric == "pd":
                        disk_tomb_delta_bytes = _disk_tomb_delta_bytes(baseline_rep, cur_rep_for_growth)
                        if disk_tomb_delta_bytes is not None:
                            verdict_growth_bytes = growth_bytes - disk_tomb_delta_bytes
                            if verdict_growth_bytes < 0:
                                verdict_growth_bytes = 0
                            verdict_growth_mb = verdict_growth_bytes / MiB

                    samples.append((now, cur))
                    verdict_growth_samples.append((now, verdict_growth_bytes))
                    if disk_tomb_delta_bytes is not None:
                        print(
                            "[check] round={} metric={} value={} growth={:+.2f} MB unaccounted={:+.2f} MB disk_tomb_delta={:+.2f} MB".format(
                                r,
                                args.fd_metric,
                                _human_mb(cur),
                                growth_mb,
                                verdict_growth_mb,
                                disk_tomb_delta_bytes / MiB,
                            )
                        )
                    else:
                        print(
                            "[check] round={} metric={} value={} growth={:+.2f} MB".format(
                                r, args.fd_metric, _human_mb(cur), growth_mb
                            )
                        )
                    if should_check and verdict_growth_mb > args.max_growth_mb:
                        cur_heap = fd_logs.latest_heap_signal() if fd_logs is not None else None
                        cur_rep = (
                            cur_rep_for_growth
                            if cur_rep_for_growth is not None
                            else (fd_logs.latest_report() if fd_logs is not None else None)
                        )
                        overflow_total = fd_logs.overflow_total if fd_logs is not None else 0
                        if cur_rep is not None and cur_rep.event_overflow_drops is not None:
                            overflow_total = max(overflow_total, cur_rep.event_overflow_drops)
                        rescan_total = (
                            cur_rep.event_rescan_signals
                            if cur_rep is not None and cur_rep.event_rescan_signals is not None
                            else 0
                        )
                        fast_sync_triggers = (
                            fd_logs.fast_sync_trigger_count if fd_logs is not None else 0
                        )
                        rebuild_triggers = (
                            fd_logs.rebuild_trigger_count if fd_logs is not None else 0
                        )

                        cur_smaps: tuple[dict[str, int], list[tuple[str, int]]] | None = None
                        if args.fd_metric == "pd":
                            cur_smaps = _read_smaps_pd_breakdown(args.fd_pid)

                        # 输出摘要，便于单次运行得到“失败原因 + 关键数值”
                        max_bytes = max(v for _, v in samples) if samples else cur
                        max_growth_mb = (max_bytes - baseline_bytes) / MiB
                        duration_min = (now - (baseline_t or now)) / 60.0
                        trend = (
                            ((cur - baseline_bytes) / MiB) / duration_min if duration_min > 0 else 0.0
                        )
                        if disk_tomb_delta_bytes is not None and args.fd_metric == "pd":
                            print(
                                "[fail] round={} unaccounted={:+.2f} MB > max_unaccounted={:.2f} MB (growth={:+.2f} MB disk_tomb_delta={:+.2f} MB max_growth={:+.2f} MB trend_mb_per_min={:+.2f})".format(
                                    r,
                                    verdict_growth_mb,
                                    args.max_growth_mb,
                                    growth_mb,
                                    disk_tomb_delta_bytes / MiB,
                                    max_growth_mb,
                                    trend,
                                )
                            )
                        else:
                            print(
                                "[fail] round={} growth={:+.2f} MB > max_growth={:.2f} MB (max_growth={:+.2f} MB trend_mb_per_min={:+.2f})".format(
                                    r, growth_mb, args.max_growth_mb, max_growth_mb, trend
                                )
                            )
                        print("[verdict] FAIL")
                        if args.verdict:
                            print("────")
                            print("[report] 归因摘要（best-effort）")
                            if disk_tomb_delta_bytes is not None and args.fd_metric == "pd":
                                print(
                                    "- Verdict: total_growth={:+.2f} MB disk_tomb_delta={:+.2f} MB unaccounted={:+.2f} MB threshold={:.2f} MB".format(
                                        growth_mb,
                                        disk_tomb_delta_bytes / MiB,
                                        verdict_growth_mb,
                                        args.max_growth_mb,
                                    )
                                )
                            if overflow_total > 0 or rescan_total > 0:
                                print(
                                    f"- 事件管道: overflow_drops={overflow_total} rescan_signals={rescan_total} fast_sync_triggers={fast_sync_triggers} rebuild_triggers={rebuild_triggers} (优先级高：这会让系统进入非常态路径)"
                                )
                            if cur_heap is not None:
                                idx_delta = (
                                    cur_heap.index_est_bytes - baseline_heap.index_est_bytes
                                    if baseline_heap is not None
                                    else None
                                )
                                non_idx_delta = (
                                    cur_heap.non_index_pd_bytes - baseline_heap.non_index_pd_bytes
                                    if baseline_heap is not None
                                    else None
                                )
                                print(
                                    "- Heap-signal: idx_est={} (Δ={}) non-idx-pd={} (Δ={}) suspected={} rss_trend_mb_per_min={:+.2f}".format(
                                        _fmt_bytes(cur_heap.index_est_bytes),
                                        _fmt_bytes(idx_delta) if idx_delta is not None else "N/A",
                                        _fmt_bytes(cur_heap.non_index_pd_bytes),
                                        _fmt_bytes(non_idx_delta) if non_idx_delta is not None else "N/A",
                                        cur_heap.suspected,
                                        cur_heap.rss_trend_mb_per_min,
                                    )
                                )
                            else:
                                print("- Heap-signal: N/A（未捕获 MemoryReport；请确保 fd-rdd 的 --report-interval-secs > 0 且运行时间 > 5s）")

                            if cur_rep is not None:
                                def _d(a: int | None, b: int | None) -> str:
                                    if a is None or b is None:
                                        return "N/A"
                                    return _fmt_bytes(b - a)

                                print(
                                    "- Index 分解: L2={} (Δ={}) overlay_est={} (Δ={}) disk_tomb_est={} (Δ={}) rebuild_est={} (Δ={})".format(
                                        _fmt_bytes(cur_rep.l2_total_bytes),
                                        _d(baseline_rep.l2_total_bytes if baseline_rep else None, cur_rep.l2_total_bytes),
                                        _fmt_bytes(cur_rep.overlay_est_bytes),
                                        _d(baseline_rep.overlay_est_bytes if baseline_rep else None, cur_rep.overlay_est_bytes),
                                        _fmt_bytes(cur_rep.disk_tomb_est_bytes),
                                        _d(baseline_rep.disk_tomb_est_bytes if baseline_rep else None, cur_rep.disk_tomb_est_bytes),
                                        _fmt_bytes(cur_rep.rebuild_est_bytes),
                                        _d(baseline_rep.rebuild_est_bytes if baseline_rep else None, cur_rep.rebuild_est_bytes),
                                    )
                                )
                                print(
                                    f"- Event buffers: raw_cap={cur_rep.event_raw_cap} merged_cap={cur_rep.event_merged_cap} records_cap={cur_rep.event_records_cap}"
                                )
                            else:
                                print("- MemoryReport: N/A（未捕获报告框）")

                            # smaps 细分：stack/heap/anon/file（帮助判断 non-idx-pd 是否主要来自线程栈等非 allocator 因素）
                            if cur_smaps is not None:
                                cur_cats, cur_top = cur_smaps
                                base_cats = baseline_smaps[0] if baseline_smaps is not None else None
                                parts: list[str] = []
                                for k in ("stack", "heap", "anon", "file", "special"):
                                    v = cur_cats.get(k, 0)
                                    if base_cats is not None:
                                        dv = v - base_cats.get(k, 0)
                                        parts.append(f"{k}={_fmt_bytes(v)} (Δ={_fmt_bytes(dv)})")
                                    else:
                                        parts.append(f"{k}={_fmt_bytes(v)}")
                                top_s = ", ".join(
                                    f"{_short_map_name(n)}:{_fmt_bytes(b)}" for n, b in cur_top[:3]
                                )
                                print(f"- Smaps(pd) 分解: {' '.join(parts)} top_pd=[{top_s}]")

                            # 粗略结论（按优先级）
                            if overflow_total > 0 or rescan_total > 0:
                                if fast_sync_triggers > 0 and rebuild_triggers == 0:
                                    recovery = "fast-sync"
                                elif rebuild_triggers > 0 and fast_sync_triggers == 0:
                                    recovery = "rebuild（疑似旧兜底路径/二进制未更新）"
                                elif fast_sync_triggers > 0 and rebuild_triggers > 0:
                                    recovery = "fast-sync + rebuild（混合）"
                                else:
                                    recovery = "兜底（未捕获触发日志）"
                                print(
                                    "- 推断: 主要瓶颈是事件链路溢出/Rescan 信号（try_send 丢事件或 inotify 队列 overflow）→ 触发 {}；pd 增长可能由非常态高水位引起。".format(
                                        recovery
                                    )
                                )
                            elif cur_heap is not None and cur_heap.suspected:
                                print("- 推断: 非索引 Private_Dirty 高水位占主导（allocator/临时缓冲常驻）。")
                            elif cur_heap is not None and baseline_heap is not None:
                                if (cur_heap.index_est_bytes - baseline_heap.index_est_bytes) > (
                                    cur_heap.non_index_pd_bytes - baseline_heap.non_index_pd_bytes
                                ):
                                    print("- 推断: 索引结构（idx_est）增长占主导；优先检查 L2/overlay/tombstones 是否持续累积。")
                                else:
                                    print("- 推断: 非索引部分增长占主导（即便未触发 suspected，也可能是缓冲高水位）。")
                            print("────")
                        if args.report_json is not None:
                            payload = {
                                "ts_utc": datetime.now(timezone.utc).isoformat(),
                                "git_head": _git_head(),
                                "args": {k: (str(v) if isinstance(v, Path) else v) for k, v in vars(args).items()},
                                "fd": {
                                    "pid": args.fd_pid,
                                    "spawn_cmd": spawn_cmd,
                                    "spawn_env_overrides": spawn_env_overrides,
                                },
                                "baseline": {
                                    "metric": args.fd_metric,
                                    "bytes": baseline_bytes,
                                    "heap_signal": asdict(baseline_heap) if baseline_heap is not None else None,
                                    "memory_report": asdict(baseline_rep) if baseline_rep is not None else None,
                                    "smaps_pd_breakdown": baseline_smaps[0] if baseline_smaps is not None else None,
                                },
                                "samples": [{"t": t, "bytes": b} for t, b in samples],
                                "verdict": {
                                    "status": "FAIL",
                                    "round": r,
                                    "metric": args.fd_metric,
                                    "value_bytes": cur,
                                    "growth_mb": growth_mb,
                                    "total_growth_mb": growth_mb,
                                    "verdict_growth_mb": verdict_growth_mb,
                                    "disk_tomb_delta_mb": (
                                        (disk_tomb_delta_bytes / MiB)
                                        if disk_tomb_delta_bytes is not None
                                        else None
                                    ),
                                    "growth_mode": (
                                        "unaccounted(pdΔ-disk_tomb_estΔ)"
                                        if (args.fd_metric == "pd" and disk_tomb_delta_bytes is not None)
                                        else "total"
                                    ),
                                    "max_growth_mb": args.max_growth_mb,
                                },
                                "attribution": {
                                    "overflow_drops_total": overflow_total,
                                    "rescan_signals_total": rescan_total,
                                    "fast_sync_triggers": fast_sync_triggers,
                                    "rebuild_triggers": rebuild_triggers,
                                    "heap_signal": asdict(cur_heap) if cur_heap is not None else None,
                                    "memory_report": asdict(cur_rep) if cur_rep is not None else None,
                                    "smaps_pd_breakdown": cur_smaps[0] if cur_smaps is not None else None,
                                    "smaps_pd_top": ([{"name": n, "pd_bytes": b} for n, b in cur_smaps[1]] if cur_smaps is not None else []),
                                },
                            }
                            _write_report_json(args.report_json, payload)
                        return 3

            if should_check:
                max_bytes = max(v for _, v in samples) if samples else baseline_bytes
                last_t, last_bytes = samples[-1] if samples else (time.time(), baseline_bytes)
                max_verdict_growth_bytes = (
                    max(v for _, v in verdict_growth_samples)
                    if verdict_growth_samples
                    else (max_bytes - baseline_bytes)
                )
                last_verdict_growth_bytes = (
                    verdict_growth_samples[-1][1]
                    if verdict_growth_samples
                    else (last_bytes - baseline_bytes)
                )
                max_growth_mb = (max_bytes - baseline_bytes) / MiB
                final_growth_mb = (last_bytes - baseline_bytes) / MiB
                max_verdict_growth_mb = max_verdict_growth_bytes / MiB
                final_verdict_growth_mb = last_verdict_growth_bytes / MiB
                duration_min = (last_t - (baseline_t or last_t)) / 60.0
                trend = (final_growth_mb / duration_min) if duration_min > 0 else 0.0
                final_disk_tomb_delta_mb: float | None = None
                if args.fd_metric == "pd" and fd_logs is not None:
                    final_rep = fd_logs.latest_report()
                    final_disk_tomb_delta_bytes = _disk_tomb_delta_bytes(baseline_rep, final_rep)
                    if final_disk_tomb_delta_bytes is not None:
                        final_disk_tomb_delta_mb = final_disk_tomb_delta_bytes / MiB

                if final_disk_tomb_delta_mb is not None:
                    print(
                        "[summary] metric={} baseline={} final={} total_growth={:+.2f} MB max_total_growth={:+.2f} MB unaccounted={:+.2f} MB max_unaccounted={:+.2f} MB disk_tomb_delta={:+.2f} MB threshold={:.2f} MB duration_min={:.2f} trend_mb_per_min={:+.2f}".format(
                            args.fd_metric,
                            _human_mb(baseline_bytes),
                            _human_mb(last_bytes),
                            final_growth_mb,
                            max_growth_mb,
                            final_verdict_growth_mb,
                            max_verdict_growth_mb,
                            final_disk_tomb_delta_mb,
                            args.max_growth_mb,
                            duration_min,
                            trend,
                        )
                    )
                else:
                    print(
                        "[summary] metric={} baseline={} final={} final_growth={:+.2f} MB max_growth={:+.2f} MB duration_min={:.2f} trend_mb_per_min={:+.2f}".format(
                            args.fd_metric,
                            _human_mb(baseline_bytes),
                            _human_mb(last_bytes),
                            final_growth_mb,
                            max_growth_mb,
                            duration_min,
                            trend,
                        )
                    )
                print("[ok] plateau check passed")
                if args.verdict:
                    print("[verdict] PASS")
                    if fd_logs is not None:
                        cur_heap = fd_logs.latest_heap_signal()
                        cur_rep = fd_logs.latest_report()
                        overflow_total = fd_logs.overflow_total
                        if cur_rep is not None and cur_rep.event_overflow_drops is not None:
                            overflow_total = max(overflow_total, cur_rep.event_overflow_drops)
                        rescan_total = (
                            cur_rep.event_rescan_signals
                            if cur_rep is not None and cur_rep.event_rescan_signals is not None
                            else 0
                        )
                        fast_sync_triggers = fd_logs.fast_sync_trigger_count
                        rebuild_triggers = fd_logs.rebuild_trigger_count
                        if (
                            overflow_total > 0
                            or rescan_total > 0
                            or cur_heap is not None
                            or cur_rep is not None
                        ):
                            print("────")
                            print("[report] 归因摘要（best-effort）")
                            if final_disk_tomb_delta_mb is not None and args.fd_metric == "pd":
                                print(
                                    "- Verdict: total_growth={:+.2f} MB disk_tomb_delta={:+.2f} MB unaccounted={:+.2f} MB threshold={:.2f} MB".format(
                                        final_growth_mb,
                                        final_disk_tomb_delta_mb,
                                        final_verdict_growth_mb,
                                        args.max_growth_mb,
                                    )
                                )
                            if overflow_total > 0 or rescan_total > 0:
                                print(
                                    f"- 事件管道: overflow_drops={overflow_total} rescan_signals={rescan_total} fast_sync_triggers={fast_sync_triggers} rebuild_triggers={rebuild_triggers}"
                                )
                            if cur_heap is not None:
                                print(
                                    "- Heap-signal: idx_est={} non-idx-pd={} suspected={} rss_trend_mb_per_min={:+.2f}".format(
                                        _fmt_bytes(cur_heap.index_est_bytes),
                                        _fmt_bytes(cur_heap.non_index_pd_bytes),
                                        cur_heap.suspected,
                                        cur_heap.rss_trend_mb_per_min,
                                    )
                                )
                            if cur_rep is not None:
                                print(
                                    "- Index 分解: L2={} overlay_est={} disk_tomb_est={} rebuild_est={}".format(
                                        _fmt_bytes(cur_rep.l2_total_bytes),
                                        _fmt_bytes(cur_rep.overlay_est_bytes),
                                        _fmt_bytes(cur_rep.disk_tomb_est_bytes),
                                        _fmt_bytes(cur_rep.rebuild_est_bytes),
                                    )
                                )
                            print("────")

                if args.report_json is not None:
                    cur_heap = fd_logs.latest_heap_signal() if fd_logs is not None else None
                    cur_rep = fd_logs.latest_report() if fd_logs is not None else None
                    overflow_total = fd_logs.overflow_total if fd_logs is not None else 0
                    if cur_rep is not None and cur_rep.event_overflow_drops is not None:
                        overflow_total = max(overflow_total, cur_rep.event_overflow_drops)
                    rescan_total = (
                        cur_rep.event_rescan_signals
                        if cur_rep is not None and cur_rep.event_rescan_signals is not None
                        else 0
                    )
                    fast_sync_triggers = fd_logs.fast_sync_trigger_count if fd_logs is not None else 0
                    rebuild_triggers = fd_logs.rebuild_trigger_count if fd_logs is not None else 0
                    cur_smaps = (
                        _read_smaps_pd_breakdown(args.fd_pid) if args.fd_metric == "pd" else None
                    )
                    payload = {
                        "ts_utc": datetime.now(timezone.utc).isoformat(),
                        "git_head": _git_head(),
                        "args": {
                            k: (str(v) if isinstance(v, Path) else v)
                            for k, v in vars(args).items()
                        },
                        "fd": {
                            "pid": args.fd_pid,
                            "spawn_cmd": spawn_cmd,
                            "spawn_env_overrides": spawn_env_overrides,
                        },
                        "baseline": {
                            "metric": args.fd_metric,
                            "bytes": baseline_bytes,
                            "heap_signal": asdict(baseline_heap)
                            if baseline_heap is not None
                            else None,
                            "memory_report": asdict(baseline_rep)
                            if baseline_rep is not None
                            else None,
                            "smaps_pd_breakdown": baseline_smaps[0]
                            if baseline_smaps is not None
                            else None,
                        },
                        "samples": [{"t": t, "bytes": b} for t, b in samples],
                        "verdict": {
                            "status": "PASS",
                            "metric": args.fd_metric,
                            "max_growth_mb": args.max_growth_mb,
                            "growth_mode": (
                                "unaccounted(pdΔ-disk_tomb_estΔ)"
                                if (final_disk_tomb_delta_mb is not None and args.fd_metric == "pd")
                                else "total"
                            ),
                            "final_total_growth_mb": final_growth_mb,
                            "max_total_growth_mb": max_growth_mb,
                            "final_verdict_growth_mb": final_verdict_growth_mb,
                            "max_verdict_growth_mb": max_verdict_growth_mb,
                            "final_disk_tomb_delta_mb": final_disk_tomb_delta_mb,
                        },
                        "attribution": {
                            "overflow_drops_total": overflow_total,
                            "rescan_signals_total": rescan_total,
                            "fast_sync_triggers": fast_sync_triggers,
                            "rebuild_triggers": rebuild_triggers,
                            "heap_signal": asdict(cur_heap) if cur_heap is not None else None,
                            "memory_report": asdict(cur_rep) if cur_rep is not None else None,
                            "smaps_pd_breakdown": cur_smaps[0]
                            if cur_smaps is not None
                            else None,
                            "smaps_pd_top": (
                                [{"name": n, "pd_bytes": b} for n, b in cur_smaps[1]]
                                if cur_smaps is not None
                                else []
                            ),
                        },
                    }
                    _write_report_json(args.report_json, payload)

        return 0
    finally:
        if spawn_proc is not None and not args.spawn_fd_keep:
            if spawn_proc.poll() is None:
                try:
                    spawn_proc.send_signal(signal.SIGINT)
                    spawn_proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    spawn_proc.terminate()
                    try:
                        spawn_proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        spawn_proc.kill()
                        spawn_proc.wait(timeout=5)

        if fd_logs_thread is not None:
            fd_logs_thread.join(timeout=2)

        # 清理 root 需要在 fd-rdd 停止之后，否则会导致其 final snapshot/WAL 写入失败。
        if cleanup_requested:
            if spawn_proc is not None and args.spawn_fd_keep:
                print("[cleanup] skipped: --spawn-fd-keep is set (avoid breaking a running fd-rdd)")
            else:
                _cleanup_root(args.root)
                print(f"[cleanup] removed {args.root}")


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
