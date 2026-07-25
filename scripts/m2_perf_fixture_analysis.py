"""M2 性能 fixture 的纯分析函数：扫描窗口检测、单周期成本、标定评估与报告渲染。"""

from __future__ import annotations

from typing import Any

# bd3b654 正式跑 block-01 A 腿的参考单价（9 个 7 秒轮转窗口的 A 侧口径）：
# read = 109,427 / 9；minor fault ≈ 42,059 / 9（B 侧窗口活动可忽略，net≈gross）；
# cpu = 18.42s / 9 加 B 侧窗口本底，取 2.05。首桶 5.5k~6k 仅作信息参考，不进标定。
DEFAULT_CALIBRATION_REFERENCE: dict[str, float] = {
    "read_syscalls_per_cycle": 12159.0,
    "minor_faults_per_cycle": 4673.0,
    "cpu_core_seconds_per_cycle": 2.05,
}
DEFAULT_CALIBRATION_TOLERANCE = 0.30
# 跨宿主标定只对 syscall/缺页这类计数指标把门：CPU 是时间量，随宿主硬件线性缩放
# （实测本机比正式 VM 快 ~2.2×）。同宿主基线（--baseline-json）则三项全部把门。
DEFAULT_GATED_METRICS = ("read_syscalls_per_cycle", "minor_faults_per_cycle")
DEFAULT_SPIKE_THRESHOLD = 1000.0
DEFAULT_TAIL_THRESHOLD = 50.0
DEFAULT_TAIL_GAP_SAMPLES = 4

_DELTA_KEYS = ("read_syscalls", "write_syscalls", "minor_faults", "cpu_ticks")


def sample_deltas(samples: list[dict[str, Any]]) -> list[dict[str, float]]:
    """把累计计数器样本转为相邻差分序列；elapsed 取区间末样本时刻。"""
    deltas: list[dict[str, float]] = []
    for prev, curr in zip(samples, samples[1:]):
        row = {"elapsed_secs": float(curr.get("elapsed_secs", 0.0) or 0.0)}
        for key in _DELTA_KEYS:
            row[key] = max(
                0.0, float(curr.get(key, 0) or 0) - float(prev.get(key, 0) or 0)
            )
        deltas.append(row)
    return deltas


def detect_scan_windows(
    deltas: list[dict[str, float]],
    *,
    spike_threshold: float = DEFAULT_SPIKE_THRESHOLD,
    tail_threshold: float = DEFAULT_TAIL_THRESHOLD,
    tail_gap_samples: int = DEFAULT_TAIL_GAP_SAMPLES,
) -> list[tuple[int, int]]:
    """按读差分尖峰识别递归扫描窗口，返回差分序列上的闭区间索引。

    onset 为单样本读差分 >= spike_threshold；随后活动尾（>= tail_threshold）
    延展窗口，最多容忍 tail_gap_samples 个静默样本的间隙。
    """
    windows: list[tuple[int, int]] = []
    index = 0
    while index < len(deltas):
        if deltas[index]["read_syscalls"] < spike_threshold:
            index += 1
            continue
        end = index
        gap = 0
        cursor = index + 1
        while cursor < len(deltas) and gap < tail_gap_samples:
            if deltas[cursor]["read_syscalls"] >= tail_threshold:
                end = cursor
                gap = 0
            else:
                gap += 1
            cursor += 1
        windows.append((index, end))
        index = end + 1 + tail_gap_samples
    return windows


def partition_windows(
    deltas: list[dict[str, float]],
    windows: list[tuple[int, int]],
    *,
    min_cycle_start_secs: float,
) -> tuple[list[tuple[int, int]], list[tuple[int, int]]]:
    """把 onset 早于 min_cycle_start_secs 的窗口归为启动扫描，其余归为轮转周期。"""
    startup: list[tuple[int, int]] = []
    cycles: list[tuple[int, int]] = []
    for window in windows:
        onset_elapsed = deltas[window[0]]["elapsed_secs"]
        (cycles if onset_elapsed >= min_cycle_start_secs else startup).append(window)
    return startup, cycles


def summarize_cycles(
    deltas: list[dict[str, float]],
    windows: list[tuple[int, int]],
    *,
    clk_tck: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for start, end in windows:
        span = deltas[start : end + 1]
        rows.append(
            {
                "start_elapsed_secs": round(span[0]["elapsed_secs"], 3),
                "end_elapsed_secs": round(span[-1]["elapsed_secs"], 3),
                "duration_secs": round(
                    span[-1]["elapsed_secs"] - span[0]["elapsed_secs"], 3
                ),
                "read_syscalls": int(sum(r["read_syscalls"] for r in span)),
                "write_syscalls": int(sum(r["write_syscalls"] for r in span)),
                "minor_faults": int(sum(r["minor_faults"] for r in span)),
                "cpu_core_seconds": round(
                    sum(r["cpu_ticks"] for r in span) / float(clk_tck), 6
                ),
                "first_bucket_read_syscalls": int(span[0]["read_syscalls"]),
            }
        )
    return rows


def _median(values: list[float]) -> float:
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return float(ordered[mid])
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def evaluate_calibration(
    cycles: list[dict[str, Any]],
    *,
    reference: dict[str, float] | None = None,
    tolerance: float = DEFAULT_CALIBRATION_TOLERANCE,
    gated_metrics: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    """以周期中位数对照参考单价；无周期时显式判失败，不静默通过。

    gated_metrics 为 None 时按参考来源取默认：显式 reference（同宿主基线）
    三项全部把门；内置 VM 参考只把门计数指标，CPU 仅报告。
    """
    if gated_metrics is None:
        gated_metrics = (
            tuple(DEFAULT_CALIBRATION_REFERENCE)
            if reference is not None
            else DEFAULT_GATED_METRICS
        )
    expected = dict(
        DEFAULT_CALIBRATION_REFERENCE if reference is None else reference
    )
    series = {
        "read_syscalls_per_cycle": [float(c["read_syscalls"]) for c in cycles],
        "minor_faults_per_cycle": [float(c["minor_faults"]) for c in cycles],
        "cpu_core_seconds_per_cycle": [float(c["cpu_core_seconds"]) for c in cycles],
    }
    result: dict[str, Any] = {
        "tolerance": tolerance,
        "cycle_count": len(cycles),
        "gated_metrics": list(gated_metrics),
        "metrics": {},
        "pass": bool(cycles),
    }
    for name, values in series.items():
        if not values:
            continue
        measured = _median(values)
        target = float(expected.get(name, 0.0))
        deviation = abs(measured - target) / target if target else 0.0
        gated = name in gated_metrics
        ok = deviation <= tolerance
        result["metrics"][name] = {
            "measured_median": round(measured, 6),
            "reference": target,
            "deviation_ratio": round(deviation, 6),
            "gated": gated,
            "pass": ok,
        }
        if gated:
            result["pass"] = bool(result["pass"] and ok)
    return result


def render_report(payload: dict[str, Any]) -> str:
    """渲染 FIXTURE-REPORT.md；周期数不足与启动窗口都显式声明，不做静默截断。"""
    cycles = payload.get("cycles", [])
    requested = int(payload.get("cycles_requested", 0))
    config = payload.get("config", {})
    lines = [
        f"# M2 perf fixture — {payload.get('run_label', '')}",
        "",
        f"- measured cycles: {len(cycles)}/{requested}"
        + ("（不足即视为覆盖缺口，不静默通过）" if len(cycles) < requested else ""),
        f"- rotating ttl/tick: {config.get('rotating_ttl_secs')}s/"
        f"{config.get('rotating_tick_secs')}s",
        f"- startup windows (excluded from cycles): "
        f"{len(payload.get('startup_windows', []))}",
        "",
        "| cycle | start (s) | duration (s) | reads | first bucket reads | writes | minor faults | cpu core-s |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for index, row in enumerate(cycles, start=1):
        lines.append(
            f"| {index} | {row['start_elapsed_secs']} | {row['duration_secs']} | "
            f"{row['read_syscalls']} | {row['first_bucket_read_syscalls']} | "
            f"{row['write_syscalls']} | {row['minor_faults']} | "
            f"{row['cpu_core_seconds']} |"
        )
    calibration = payload.get("calibration")
    lines += ["", "## calibration", ""]
    if calibration is None:
        lines.append("calibration disabled (--no-calibrate)")
    else:
        lines.append(
            f"calibration verdict: {'PASS' if calibration.get('pass') else 'FAIL'} "
            f"(tolerance {calibration.get('tolerance')}, cycles "
            f"{calibration.get('cycle_count')})"
        )
        lines += [
            "",
            "| metric | measured median | reference | deviation | gated | pass |",
            "|---|---:|---:|---:|---|---|",
        ]
        for name, row in calibration.get("metrics", {}).items():
            lines.append(
                f"| {name} | {row['measured_median']} | {row['reference']} | "
                f"{row['deviation_ratio']} | {row.get('gated', True)} | {row['pass']} |"
            )
    guard = payload.get("query_guard", {})
    watch = payload.get("watch_state")
    if watch is not None:
        lines += [
            "",
            "## watch state",
            "",
            f"- l3 dirs (last): {watch.get('l3_dirs_last', 0)}",
            f"- rotating active dirs max: {watch.get('rotating_active_dirs_max', 0)}",
            f"- rotating scan-only dirs (last): "
            f"{watch.get('rotating_scan_only_dirs_last', 0)}",
            f"- rotating budget blocked (last): "
            f"{watch.get('rotating_budget_blocked_last', 0)}",
        ]
    lines += [
        "",
        "## query guard",
        "",
        f"- hold count: {guard.get('query_guard_hold_count', 0)}",
        f"- hold total: {guard.get('query_guard_hold_total_ns', 0)} ns",
        "- hold p50/p95/p99: "
        f"{guard.get('query_guard_hold_p50_us', 0)}/"
        f"{guard.get('query_guard_hold_p95_us', 0)}/"
        f"{guard.get('query_guard_hold_p99_us', 0)} us",
    ]
    burst = payload.get("burst", {})
    lines += ["", "## burst", ""]
    if burst.get("enabled"):
        lines.append(
            f"- burst visible: {burst.get('visible')} "
            f"(latency {burst.get('latency_secs')} s)"
        )
    else:
        lines.append("- burst disabled")
    lines.append("")
    return "\n".join(lines)
