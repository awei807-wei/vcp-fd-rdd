"""M2 快速证伪 suite 的配对收益、成本与最终判定。"""

from __future__ import annotations

import math
import statistics
from datetime import datetime
from typing import Any

from m2_cold_window_falsification_validation import (
    block_protocol_invalid_reasons,
    leg_label,
    validate_a_correctness,
    validate_leg,
    validate_suite_audit,
)


CPU_RATIO_LIMIT = 1.10
READ_RATIO_LIMIT = 1.10
READ_SYSCALL_RATIO_LIMIT = 1.10
WRITE_RATIO_LIMIT = 1.25
WRITE_SYSCALL_RATIO_LIMIT = 1.25
RSS_RATIO_LIMIT = 1.10
RSS_DELTA_LIMIT_BYTES = 32 * 1024 * 1024
MINOR_FAULT_RATIO_LIMIT = 1.25
MINOR_FAULT_DELTA_LIMIT = 10_000
MAJOR_FAULT_DELTA_LIMIT = 8
QUERY_POLL_RATIO_LIMIT = 1.02
# 2026-07-25 方案包 202607251527_m2-cost-gate-recovery 任务6 冻结的量纲修订：
# 近零基线（B 组不做该功能）上的比值门必然失真，CPU 与读 syscall 与 RSS/
# minor-fault 同构地改为"比值+绝对增量"双门；正式跑之后不得调整。
CPU_DELTA_LIMIT_CORE_SECONDS = 5.0
READ_SYSCALL_DELTA_LIMIT = 15_000
# 物理读字节在近零基线上（两侧多为页缓存命中，绝对量 ~100KB 级）同样受比值失真：
# 观测到 ~57KB 页缓存噪声即可触发 1.5×。绝对阈值取 1 MiB（噪声的 ~60 倍），
# 真实内容读取回归仍会同时触发双门。
READ_BYTES_DELTA_LIMIT = 1024 * 1024
MIN_BENEFIT_BLOCKS = 3
MIN_SUCCESS_RATE_GAIN = 0.05
MIN_BENEFIT_BURSTS = 3
MIN_BENEFIT_WORKLOADS = 2
MAX_PAIRED_START_GAP_SECS = 1800.0


def _ratio(numerator: float, denominator: float) -> float:
    if denominator > 0:
        return numerator / denominator
    return 1.0 if numerator <= 0 else float("inf")


def _symmetric_ratio(left: float, right: float) -> float:
    return max(_ratio(left, right), _ratio(right, left))


def _query_time_ratio(a: dict[str, Any], b: dict[str, Any]) -> float | None:
    if int(a.get("metrics_sample_count", 0) or 0) <= 0:
        return None
    if int(b.get("metrics_sample_count", 0) or 0) <= 0:
        return None
    return _symmetric_ratio(
        int(a.get("queries_total_us_estimate", 0) or 0),
        int(b.get("queries_total_us_estimate", 0) or 0),
    )


def _query_work_pair_fields(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
    return {
        "a_query_count": int(a.get("queries_total", 0) or 0),
        "b_query_count": int(b.get("queries_total", 0) or 0),
        "a_query_avg_us": int(a.get("queries_avg_us", 0) or 0),
        "b_query_avg_us": int(b.get("queries_avg_us", 0) or 0),
        "a_query_total_seconds_estimate": round(
            int(a.get("queries_total_us_estimate", 0) or 0) / 1_000_000,
            6,
        ),
        "b_query_total_seconds_estimate": round(
            int(b.get("queries_total_us_estimate", 0) or 0) / 1_000_000,
            6,
        ),
        "query_time_estimate_ratio": _query_time_ratio(a, b),
        "a_query_guard_avg_us": int(a.get("query_guard_hold_avg_us", 0) or 0),
        "b_query_guard_avg_us": int(b.get("query_guard_hold_avg_us", 0) or 0),
        "a_query_guard_seconds_estimate": round(
            int(a.get("query_guard_hold_total_us_estimate", 0) or 0) / 1_000_000,
            6,
        ),
        "b_query_guard_seconds_estimate": round(
            int(b.get("query_guard_hold_total_us_estimate", 0) or 0) / 1_000_000,
            6,
        ),
        "a_query_guard_seconds_exact": round(
            int(a.get("query_guard_hold_total_ns", 0) or 0) / 1_000_000_000,
            6,
        ),
        "b_query_guard_seconds_exact": round(
            int(b.get("query_guard_hold_total_ns", 0) or 0) / 1_000_000_000,
            6,
        ),
        "a_query_guard_p95_us": int(a.get("query_guard_hold_p95_us", 0) or 0),
        "b_query_guard_p95_us": int(b.get("query_guard_hold_p95_us", 0) or 0),
        "a_query_guard_p99_us": int(a.get("query_guard_hold_p99_us", 0) or 0),
        "b_query_guard_p99_us": int(b.get("query_guard_hold_p99_us", 0) or 0),
    }


def _benefit_units(
    a: dict[str, Any], b: dict[str, Any]
) -> tuple[list[str], list[str]]:
    a_bursts = a["correctness"].get("positive_by_burst", {})
    b_bursts = b["correctness"].get("positive_by_burst", {})
    benefited: list[str] = []
    workloads: set[str] = set()
    for burst in sorted(set(a_bursts) & set(b_bursts), key=int):
        left = a_bursts[burst]
        right = b_bursts[burst]
        total = int(left.get("total", 0) or 0)
        recovered = int(left.get("ok", 0) or 0) - int(right.get("ok", 0) or 0)
        gain = float(left.get("success_rate", 0.0) or 0.0) - float(
            right.get("success_rate", 0.0) or 0.0
        )
        if total > 0 and recovered >= math.ceil(total * MIN_SUCCESS_RATE_GAIN) and gain >= MIN_SUCCESS_RATE_GAIN:
            benefited.append(burst)
            workloads.update(str(value) for value in left.get("workloads", []) if value)
    return benefited, sorted(workloads)


def _block_has_benefit(a: dict[str, Any], b: dict[str, Any]) -> bool:
    ac = a["correctness"]
    bc = b["correctness"]
    recovered = max(0, int(ac["positive_ok"]) - int(bc["positive_ok"]))
    positive_total = int(ac["positive_total"])
    required_recovered = math.ceil(positive_total * MIN_SUCCESS_RATE_GAIN)
    benefited_bursts, benefited_workloads = _benefit_units(a, b)
    return (
        recovered >= required_recovered
        and recovered > 0
        and ac["positive_success_rate"] - bc["positive_success_rate"]
        >= MIN_SUCCESS_RATE_GAIN
        and len(benefited_bursts) >= MIN_BENEFIT_BURSTS
        and len(benefited_workloads) >= MIN_BENEFIT_WORKLOADS
    )


def _resource_ratios(a: dict[str, Any], b: dict[str, Any]) -> dict[str, float]:
    fields = {
        "cpu_ratio": "cpu_core_seconds",
        "read_ratio": "read_bytes_delta",
        "read_syscalls_ratio": "read_syscalls_delta",
        "write_ratio": "write_bytes_delta",
        "write_syscalls_ratio": "write_syscalls_delta",
        "rss_p95_ratio": "rss_bytes_p95",
        "minor_faults_ratio": "minor_faults_delta",
        "major_faults_ratio": "major_faults_delta",
    }
    return {
        output: _ratio(a["resources"][source], b["resources"][source])
        for output, source in fields.items()
    }


def _per_recovered(incremental: float, recovered: int) -> float | None:
    return round(incremental / recovered, 6) if recovered > 0 else None


def _pair_row(block: int, a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
    ac = a["correctness"]
    bc = b["correctness"]
    ar = a["resources"]
    br = b["resources"]
    aq = a.get("query_work", {})
    bq = b.get("query_work", {})
    recovered = max(0, int(ac["positive_ok"]) - int(bc["positive_ok"]))
    cpu_delta = float(ar["cpu_core_seconds"]) - float(br["cpu_core_seconds"])
    read_delta = int(ar["read_bytes_delta"]) - int(br["read_bytes_delta"])
    write_delta = int(ar["write_bytes_delta"]) - int(br["write_bytes_delta"])
    benefited_bursts, benefited_workloads = _benefit_units(a, b)
    return {
        "block": block,
        "benefited": _block_has_benefit(a, b),
        "recovered_primary_paths": recovered,
        "positive_success_rate_gain": round(
            float(ac["positive_success_rate"])
            - float(bc["positive_success_rate"]),
            4,
        ),
        "benefited_bursts": benefited_bursts,
        "benefited_burst_count": len(benefited_bursts),
        "benefited_workloads": benefited_workloads,
        "benefited_workload_count": len(benefited_workloads),
        "visibility_success_rate_gain": round(
            float(ac["visibility_success_rate"])
            - float(bc["visibility_success_rate"]),
            4,
        ),
        "query_poll_load_ratio": _symmetric_ratio(
            int(a["protocol"]["visibility_poll_count"]),
            int(b["protocol"]["visibility_poll_count"]),
        ),
        **_query_work_pair_fields(aq, bq),
        "a_cpu_core_seconds": float(ar["cpu_core_seconds"]),
        "b_cpu_core_seconds": float(br["cpu_core_seconds"]),
        "incremental_cpu_core_seconds": round(cpu_delta, 6),
        "cpu_seconds_per_recovered_path": _per_recovered(cpu_delta, recovered),
        "a_read_bytes": int(ar["read_bytes_delta"]),
        "b_read_bytes": int(br["read_bytes_delta"]),
        "incremental_read_bytes": read_delta,
        "read_bytes_per_recovered_path": _per_recovered(read_delta, recovered),
        "a_write_bytes": int(ar["write_bytes_delta"]),
        "b_write_bytes": int(br["write_bytes_delta"]),
        "incremental_write_bytes": write_delta,
        "write_bytes_per_recovered_path": _per_recovered(write_delta, recovered),
        "rss_p95_delta_bytes": int(ar["rss_bytes_p95"])
        - int(br["rss_bytes_p95"]),
        "read_syscalls_delta_count": int(ar["read_syscalls_delta"])
        - int(br["read_syscalls_delta"]),
        "minor_faults_delta_count": int(ar["minor_faults_delta"])
        - int(br["minor_faults_delta"]),
        "major_faults_delta_count": int(ar["major_faults_delta"])
        - int(br["major_faults_delta"]),
        **_resource_ratios(a, b),
    }


def _evaluate_pairs(
    by_block: dict[int, dict[str, dict[str, Any]]],
    reasons: list[str],
) -> tuple[list[dict[str, Any]], int]:
    rows: list[dict[str, Any]] = []
    benefit_blocks = 0
    for block in range(1, 5):
        pair = by_block.get(block, {})
        if set(pair) != {"a", "b"}:
            reasons.append(f"block {block} 缺少完整 A/B 腿")
            continue
        a, b = pair["a"], pair["b"]
        validate_a_correctness(a, reasons)
        row = _pair_row(block, a, b)
        benefited = bool(row["benefited"])
        benefit_blocks += int(benefited)
        rows.append(row)
    return rows, benefit_blocks


def _median_ratios(rows: list[dict[str, Any]]) -> dict[str, float]:
    outputs = {
        "median_cpu_ratio": "cpu_ratio",
        "median_read_bytes_ratio": "read_ratio",
        "median_read_syscalls_ratio": "read_syscalls_ratio",
        "median_write_bytes_ratio": "write_ratio",
        "median_write_syscalls_ratio": "write_syscalls_ratio",
        "median_rss_p95_ratio": "rss_p95_ratio",
        "median_rss_p95_delta_bytes": "rss_p95_delta_bytes",
        "median_incremental_cpu_core_seconds": "incremental_cpu_core_seconds",
        "median_read_syscalls_delta_count": "read_syscalls_delta_count",
        "median_incremental_read_bytes": "incremental_read_bytes",
        "median_minor_faults_ratio": "minor_faults_ratio",
        "median_minor_faults_delta_count": "minor_faults_delta_count",
        "median_major_faults_ratio": "major_faults_ratio",
        "median_major_faults_delta_count": "major_faults_delta_count",
        "median_query_poll_load_ratio": "query_poll_load_ratio",
        "median_recovered_primary_paths": "recovered_primary_paths",
    }
    return {
        output: round(
            statistics.median(row[source] for row in rows)
            if rows
            else float("inf"),
            4,
        )
        for output, source in outputs.items()
    }


def _validate_cost_limits(medians: dict[str, float], reasons: list[str]) -> None:
    ratio_limits = (
        ("median_write_bytes_ratio", WRITE_RATIO_LIMIT, "write_bytes"),
        (
            "median_write_syscalls_ratio",
            WRITE_SYSCALL_RATIO_LIMIT,
            "write syscalls",
        ),
        (
            "median_query_poll_load_ratio",
            QUERY_POLL_RATIO_LIMIT,
            "visibility polling 负载",
        ),
    )
    for field, limit, label in ratio_limits:
        if medians[field] > limit:
            reasons.append(
                f"配对 {label} 中位比 {medians[field]:.3f} 超过 {limit:.2f}"
            )
    if (
        medians["median_read_bytes_ratio"] > READ_RATIO_LIMIT
        and medians["median_incremental_read_bytes"] > READ_BYTES_DELTA_LIMIT
    ):
        reasons.append(
            f"配对 read_bytes 同时超过 {READ_RATIO_LIMIT:.2f} 倍和 1 MiB 增量门槛"
        )
    if (
        medians["median_cpu_ratio"] > CPU_RATIO_LIMIT
        and medians["median_incremental_cpu_core_seconds"] > CPU_DELTA_LIMIT_CORE_SECONDS
    ):
        reasons.append(
            f"配对 CPU 同时超过 {CPU_RATIO_LIMIT:.2f} 倍和 "
            f"{CPU_DELTA_LIMIT_CORE_SECONDS:.0f} core-s 增量门槛"
        )
    if (
        medians["median_read_syscalls_ratio"] > READ_SYSCALL_RATIO_LIMIT
        and medians["median_read_syscalls_delta_count"] > READ_SYSCALL_DELTA_LIMIT
    ):
        reasons.append(
            f"配对 read syscalls 同时超过 {READ_SYSCALL_RATIO_LIMIT:.2f} 倍和 "
            f"{READ_SYSCALL_DELTA_LIMIT} 次增量门槛"
        )
    if (
        medians["median_rss_p95_ratio"] > RSS_RATIO_LIMIT
        and medians["median_rss_p95_delta_bytes"] > RSS_DELTA_LIMIT_BYTES
    ):
        reasons.append("配对 RSS p95 同时超过 1.10 倍和 32 MiB 增量门槛")
    if (
        medians["median_minor_faults_ratio"] > MINOR_FAULT_RATIO_LIMIT
        and medians["median_minor_faults_delta_count"] > MINOR_FAULT_DELTA_LIMIT
    ):
        reasons.append("配对 minor faults 同时超过 1.25 倍和 10000 次增量门槛")
    if medians["median_major_faults_delta_count"] > MAJOR_FAULT_DELTA_LIMIT:
        reasons.append(
            "配对 major faults 中位增量 "
            f"{medians['median_major_faults_delta_count']:.0f} 超过 8 次"
        )


def _validate_pair_cost_limits(rows: list[dict[str, Any]], reasons: list[str]) -> None:
    """Reject a single pathological block even when its median is hidden."""
    ratio_limits = (
        ("write_ratio", WRITE_RATIO_LIMIT, "write_bytes"),
        ("write_syscalls_ratio", WRITE_SYSCALL_RATIO_LIMIT, "write syscalls"),
        (
            "query_poll_load_ratio",
            QUERY_POLL_RATIO_LIMIT,
            "visibility polling 负载",
        ),
    )
    for row in rows:
        block = int(row["block"])
        for field, limit, label in ratio_limits:
            value = float(row[field])
            if value > limit:
                reasons.append(
                    f"block {block} 配对 {label} 比 {value:.3f} 超过 {limit:.2f}"
                )
        if (
            float(row["read_ratio"]) > READ_RATIO_LIMIT
            and int(row["incremental_read_bytes"]) > READ_BYTES_DELTA_LIMIT
        ):
            reasons.append(
                f"block {block} read_bytes 同时超过比例和绝对增量门槛"
            )
        if (
            float(row["cpu_ratio"]) > CPU_RATIO_LIMIT
            and float(row["incremental_cpu_core_seconds"]) > CPU_DELTA_LIMIT_CORE_SECONDS
        ):
            reasons.append(f"block {block} CPU 同时超过比例和绝对增量门槛")
        if (
            float(row["read_syscalls_ratio"]) > READ_SYSCALL_RATIO_LIMIT
            and int(row["read_syscalls_delta_count"]) > READ_SYSCALL_DELTA_LIMIT
        ):
            reasons.append(
                f"block {block} read syscalls 同时超过比例和绝对增量门槛"
            )
        if (
            float(row["rss_p95_ratio"]) > RSS_RATIO_LIMIT
            and int(row["rss_p95_delta_bytes"]) > RSS_DELTA_LIMIT_BYTES
        ):
            reasons.append(f"block {block} RSS p95 同时超过比例和绝对增量门槛")
        if (
            float(row["minor_faults_ratio"]) > MINOR_FAULT_RATIO_LIMIT
            and int(row["minor_faults_delta_count"]) > MINOR_FAULT_DELTA_LIMIT
        ):
            reasons.append(
                f"block {block} minor faults 同时超过比例和绝对增量门槛"
            )
        if int(row["major_faults_delta_count"]) > MAJOR_FAULT_DELTA_LIMIT:
            reasons.append(
                f"block {block} major faults 增量 "
                f"{int(row['major_faults_delta_count'])} 超过 8 次"
            )


def _validate_block_sequence(
    legs: list[dict[str, Any]],
    by_block: dict[int, dict[str, dict[str, Any]]],
    reasons: list[str],
) -> None:
    orders: list[str] = []
    expected_positions = {
        "ab": {"a": 1, "b": 2},
        "ba": {"b": 1, "a": 2},
    }
    for block in range(1, 5):
        pair = by_block.get(block, {})
        if set(pair) != {"a", "b"}:
            continue
        declared = {str(leg.get("order", "")) for leg in pair.values()}
        if len(declared) != 1:
            reasons.append(f"block {block} 两腿声明了不同 order")
            continue
        order = declared.pop()
        if order not in expected_positions:
            reasons.append(f"block {block} order 非 ab/ba：{order!r}")
            continue
        orders.append(order)
        for variant, expected in expected_positions[order].items():
            actual = int(pair[variant].get("position", 0) or 0)
            if actual != expected:
                reasons.append(
                    f"block {block} {variant.upper()} 组 position={actual}，"
                    f"与 order={order} 不一致"
                )
    if orders.count("ab") != 2 or orders.count("ba") != 2:
        reasons.append(
            "suite 顺序必须精确为 2×AB + 2×BA，"
            f"实际 AB={orders.count('ab')}、BA={orders.count('ba')}"
        )
    observed = [
        (
            int(leg.get("block", 0) or 0),
            int(leg.get("position", 0) or 0),
            str(leg.get("variant", "")),
        )
        for leg in legs
    ]
    expected = sorted(observed, key=lambda item: (item[0], item[1]))
    if observed != expected:
        reasons.append("suite 腿列表未按 block/position 的实际执行顺序保存")
    actual: list[tuple[float, int, int, str]] = []
    for leg in legs:
        audit = leg.get("audit", {})
        started = _timestamp(str(audit.get("daemon_started_at", "")))
        finished = _timestamp(str(audit.get("finished_at", "")))
        if started is None or finished is None or finished <= started:
            reasons.append(f"{leg_label(leg)}缺少有效实际 start/end 时间")
            continue
        actual.append(
            (
                started,
                int(leg.get("block", 0) or 0),
                int(leg.get("position", 0) or 0),
                str(leg.get("variant", "")),
            )
        )
    if len(actual) == len(legs):
        actual_order = [(block, position, variant) for _, block, position, variant in sorted(actual)]
        if actual_order != observed:
            reasons.append("suite 元数据顺序与各腿实际启动时间不一致")
        for block in range(1, 5):
            starts = sorted(start for start, value, _, _ in actual if value == block)
            if len(starts) == 2 and starts[1] - starts[0] > MAX_PAIRED_START_GAP_SECS:
                reasons.append(f"block {block} 两腿启动间隔超过 30 分钟，禁止跨会话拼接")


def _timestamp(value: str) -> float | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _strict_json_value(value: Any) -> Any:
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def evaluate_suite(legs: list[dict[str, Any]]) -> dict[str, Any]:
    reasons: list[str] = []
    if len(legs) != 8:
        reasons.append(f"suite 必须精确包含 8 腿，实际 {len(legs)}")
    validate_suite_audit(legs, reasons)
    by_block: dict[int, dict[str, dict[str, Any]]] = {}
    pairing_by_block: dict[int, dict[str, dict[str, Any]]] = {}
    invalid_protocol_blocks: list[int] = []
    seen_legs: set[tuple[int, str]] = set()
    for leg in legs:
        block = int(leg["block"])
        variant = str(leg["variant"])
        leg_key = (block, variant)
        if leg_key in seen_legs:
            reasons.append(f"block {block} {variant.upper()} 组出现重复腿")
        seen_legs.add(leg_key)
        if block not in range(1, 5) or variant not in {"a", "b"}:
            reasons.append(f"出现协议外腿：block={block}, variant={variant}")
        by_block.setdefault(block, {})[variant] = leg
        if not leg.get("valid"):
            reasons.append(f"{leg_label(leg)}运行无效")
        validate_leg(leg, reasons)
    for block, pair in by_block.items():
        protocol_reasons = block_protocol_invalid_reasons(list(pair.values()))
        if protocol_reasons:
            invalid_protocol_blocks.append(block)
            reasons.extend(
                reason for reason in protocol_reasons if reason not in reasons
            )
        else:
            pairing_by_block[block] = pair
    _validate_block_sequence(legs, by_block, reasons)
    pairs, benefit_blocks = _evaluate_pairs(pairing_by_block, reasons)
    medians = _median_ratios(pairs)
    if benefit_blocks < MIN_BENEFIT_BLOCKS:
        reasons.append(f"收益仅在 {benefit_blocks}/4 个配对块复现")
    _validate_pair_cost_limits(pairs, reasons)
    _validate_cost_limits(medians, reasons)
    strict_pairs = [
        {key: _strict_json_value(value) for key, value in row.items()}
        for row in pairs
    ]
    strict_medians = {
        key: _strict_json_value(value) for key, value in medians.items()
    }
    return {
        "decision": "pass" if not reasons else "fail",
        "reasons": reasons,
        "benefit_blocks": benefit_blocks,
        "invalid_protocol_blocks": sorted(invalid_protocol_blocks),
        "paired": strict_pairs,
        **strict_medians,
    }
