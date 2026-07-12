"""M2 快速证伪腿级协议、正确性与稳定性验证。"""

from __future__ import annotations

from pathlib import Path
from typing import Any


SOFT_DEGRADED_RATIO_LIMIT = 0.10
MAX_A_VISIBILITY_P95_SECS = 35.0
MIN_PROCESS_SAMPLE_COVERAGE = 0.95
MAX_PROCESS_SAMPLE_GAP_SECS = 3.0
EXPECTED_WORKLOADS = frozenset({"save100", "git_clone", "subtree_rename"})
EXPECTED_M2_ACTIONS = frozenset(
    {"ephemeral_watch", "fast_scan_lease", "scan_only"}
)
EXPECTED_ASSERTIONS_PER_BURST = {
    "save100": 200,
    "git_clone": 3,
    "subtree_rename": 200,
}
EXPECTED_POSITIVE_ASSERTIONS_PER_BURST = {
    "save100": 100,
    "git_clone": 3,
    "subtree_rename": 100,
}
EXPECTED_PROBES_PER_BURST = {
    "save100": 8,
    "git_clone": 3,
    "subtree_rename": 8,
}


def leg_label(leg: dict[str, Any]) -> str:
    return f"block {int(leg['block'])} {str(leg['variant']).upper()} 组"


def validate_a_correctness(leg: dict[str, Any], reasons: list[str]) -> None:
    label = leg_label(leg)
    correctness = leg["correctness"]
    if correctness["positive_success_rate"] != 1.0:
        reasons.append(f"{label}正向主断言未达到 100%")
    if correctness["visibility_success_rate"] != 1.0:
        reasons.append(f"{label} visibility 未达到 100%")
    if float(correctness.get("visibility_p95_secs", 0.0)) > MAX_A_VISIBILITY_P95_SECS:
        reasons.append(
            f"{label} visibility p95 超过 {MAX_A_VISIBILITY_P95_SECS:.0f} 秒 SLA"
        )
    by_workload = correctness.get("visibility_by_workload", {})
    if set(by_workload) != EXPECTED_WORKLOADS:
        reasons.append(f"{label} visibility 未覆盖全部三类 workload")
    for workload, row in by_workload.items():
        if float(row.get("success_rate", 0.0)) != 1.0:
            reasons.append(f"{label} {workload} visibility 未达到 100%")
        if float(row.get("p95_secs", 0.0)) > MAX_A_VISIBILITY_P95_SECS:
            reasons.append(
                f"{label} {workload} visibility p95 超过 "
                f"{MAX_A_VISIBILITY_P95_SECS:.0f} 秒 SLA"
            )


def _validate_common_correctness(leg: dict[str, Any], reasons: list[str]) -> None:
    label = leg_label(leg)
    correctness = leg["correctness"]
    if correctness.get("negative_success_rate") != 1.0:
        reasons.append(f"{label}负向主断言未达到 100%")
    if int(correctness.get("transport_failures", 0)) != 0:
        reasons.append(f"{label}主断言存在传输失败")
    if int(correctness.get("visibility_transport_failures", 0)) != 0:
        reasons.append(f"{label} visibility polling 存在传输失败")
    if correctness["duplicate_primary_paths"]:
        reasons.append(f"{label}存在重复主断言路径")


def _validate_mechanism(leg: dict[str, Any], reasons: list[str]) -> None:
    label = leg_label(leg)
    mechanism = leg.get("mechanism", {})
    activity_fields = (
        "rotating_active_dirs_max",
        "rotating_cycle_progress_pct_max",
        "rotating_promoted_last",
        "rotating_scan_only_last",
    )
    activity = sum(int(mechanism.get(field, 0) or 0) for field in activity_fields)
    if leg.get("variant") == "a" and activity <= 0:
        reasons.append(f"{label}未观测到 M2 活动")
    if leg.get("variant") == "b" and any(
        int(value or 0) != 0 for value in mechanism.values()
    ):
        reasons.append(f"{label}B 组出现 M2 活动")


def _validate_stability(leg: dict[str, Any], reasons: list[str]) -> None:
    label = leg_label(leg)
    stability = leg["stability"]
    if not stability.get("snapshot_ready"):
        reasons.append(f"{label} snapshot quiesce 未 ready")
    if stability["waterline_soft_degraded_last"]:
        reasons.append(f"{label}结束时 waterline 未恢复")
    if stability["waterline_soft_degraded_ratio"] > SOFT_DEGRADED_RATIO_LIMIT:
        reasons.append(f"{label} waterline soft 占比超过 10%")
    if stability.get("waterline_trigger_count", 0) > stability.get(
        "waterline_recover_count", 0
    ):
        reasons.append(f"{label} waterline soft 日志未配对恢复")
    if (
        stability.get("waterline_hard_degraded_samples", 0)
        or stability.get("waterline_hard_degraded_last")
    ):
        reasons.append(f"{label}出现 hard waterline degradation")
    if stability["dirty_queue_len_last"] != 0:
        reasons.append(f"{label}结束时 dirty queue 非零")
    if stability["log_error_count"]:
        reasons.append(f"{label} daemon 日志存在 ERROR")
    rebuild_count = int(stability.get("background_rebuild_count", 0) or 0)
    if rebuild_count and not stability.get("snapshot_rebuild_observed"):
        reasons.append(f"{label}出现未归因到 snapshot quiesce 的 background rebuild")
    if rebuild_count > 1:
        reasons.append(f"{label} background rebuild 超过一次")
    if stability.get("direct_v7_unsupported_count", 0) and not stability.get(
        "snapshot_rebuild_observed"
    ):
        reasons.append(f"{label} direct_v7_unsupported 未由安全 rebuild 收敛")


def _path_within(path: str, root: str) -> bool:
    candidate = Path(path)
    anchor = Path(root)
    return candidate == anchor or anchor in candidate.parents


def _validate_protocol(leg: dict[str, Any], reasons: list[str]) -> None:
    label = leg_label(leg)
    protocol = leg["protocol"]
    configured = protocol.get("configured_event_roots", [])
    cold_root_ok = (
        len(configured) == 1
        and Path(str(configured[0])).name == "cold-a"
        and all(
            _path_within(str(root), str(configured[0]))
            for root in protocol.get("event_roots", [])
        )
    )
    if not cold_root_ok:
        reasons.append(f"{label}事件未完全限制在 cold-a")
    if protocol.get("requested_tiers") != ["L3"]:
        reasons.append(f"{label}请求 tier 不是唯一 L3")
    tier_before = protocol.get("tier_before", {})
    physical_bursts = int(protocol.get("physical_bursts", 0) or 0)
    if (
        not tier_before
        or sum(tier_before.values()) != physical_bursts
        or int(tier_before.get("L3", 0)) != physical_bursts
    ):
        reasons.append(f"{label}未在 L3 注入全部 burst")
    if not protocol.get("fixed_root_schedule"):
        reasons.append(f"{label}未启用固定目录调度")
    if not protocol.get("deterministic_event_plan"):
        reasons.append(f"{label}未启用确定性事件计划")
    if int(protocol.get("visibility_poll_count", 0) or 0) <= 0:
        reasons.append(f"{label}缺少等负载 visibility polling 证据")
    target_debug_ok = int(protocol.get("target_m2_debug_ok_bursts", 0) or 0)
    target_seen = int(protocol.get("target_m2_seen_bursts", 0) or 0)
    target_active = int(protocol.get("target_m2_active_bursts", 0) or 0)
    target_unexpired = int(protocol.get("target_m2_unexpired_bursts", 0) or 0)
    target_causal = int(protocol.get("target_m2_causal_bursts", 0) or 0)
    target_actions = protocol.get("target_m2_actions", {})
    if target_debug_ok != physical_bursts:
        reasons.append(f"{label}缺少逐 burst 目标目录 M2 证据")
    if leg.get("variant") == "a" and target_seen != physical_bursts:
        reasons.append(f"{label}并非每个目标目录都已被 M2 轮转触达")
    if leg.get("variant") == "a":
        if target_active != physical_bursts or target_unexpired != physical_bursts:
            reasons.append(f"{label}并非每个目标目录在写入时都持有有效 M2 租约")
        if target_causal != physical_bursts:
            reasons.append(f"{label}并非每个 burst 都有写入后的 M2 扫描/事件证据")
        if not target_actions or set(target_actions) - EXPECTED_M2_ACTIONS:
            reasons.append(f"{label}目标目录 M2 action 非法或缺失")
    if leg.get("variant") == "b" and any(
        value
        for value in (
            target_seen,
            target_active,
            target_unexpired,
            target_causal,
            sum(int(count or 0) for count in target_actions.values()),
        )
    ):
        reasons.append(f"{label}B 组目标目录出现 M2 轮转证据")
    if (
        int(protocol.get("mutation_sequence_count", 0) or 0)
        != int(protocol.get("events_total", 0) or 0)
        or protocol.get("mutation_sequence_contiguous") is not True
    ):
        reasons.append(f"{label}物理 mutation 序列缺失或不连续")
    resources = leg.get("resources", {})
    if (
        resources.get("window_source") != "full_run"
        or int(resources.get("sample_count", 0)) < 2
        or int(resources.get("event_window_sample_count", 0)) < 2
    ):
        reasons.append(f"{label}缺少 full-run/event-window 资源窗口")
    if int(resources.get("read_syscalls_delta", 0) or 0) <= 0:
        reasons.append(f"{label}缺少可比较的 read syscall 计数")
    if int(resources.get("write_syscalls_delta", 0) or 0) <= 0:
        reasons.append(f"{label}缺少可比较的 write syscall 计数")
    if float(resources.get("sample_coverage_ratio", 0.0) or 0.0) < MIN_PROCESS_SAMPLE_COVERAGE:
        reasons.append(f"{label}process 采样覆盖率低于 95%")
    if float(resources.get("sample_max_gap_secs", 0.0) or 0.0) > MAX_PROCESS_SAMPLE_GAP_SECS:
        reasons.append(f"{label}process 采样最大间隔超过 3 秒")
    if int(resources.get("counter_regressions", 0) or 0) != 0:
        reasons.append(f"{label}process 单调计数器发生回退")
    _validate_burst_protocol(leg, reasons)


def _validate_burst_protocol(leg: dict[str, Any], reasons: list[str]) -> None:
    label = leg_label(leg)
    protocol = leg["protocol"]
    workloads = protocol.get("workloads", {})
    if set(workloads) != EXPECTED_WORKLOADS or any(
        int(workloads.get(kind, 0)) != 2 for kind in EXPECTED_WORKLOADS
    ):
        reasons.append(f"{label} workload 不是三类各 2 个 burst")
    physical_bursts = int(protocol.get("physical_bursts", 0))
    if physical_bursts != 6 or int(protocol.get("checks", 0)) != physical_bursts:
        reasons.append(f"{label}未完成 6 个 burst/check")
    if int(protocol.get("within_budget_bursts", 0)) != physical_bursts:
        reasons.append(f"{label}存在超过 1 秒预算的 burst")
    _validate_event_counts(leg, workloads, reasons)


def _validate_event_counts(
    leg: dict[str, Any],
    workloads: dict[str, Any],
    reasons: list[str],
) -> None:
    label = leg_label(leg)
    protocol = leg["protocol"]
    correctness = leg["correctness"]
    expected_assertions = sum(
        int(workloads.get(kind, 0)) * count
        for kind, count in EXPECTED_ASSERTIONS_PER_BURST.items()
    )
    if int(protocol.get("events_total", 0)) != expected_assertions:
        reasons.append(f"{label} burst 声明的 expected record 数量与固定协议不符")
    if int(correctness.get("primary_assertions", 0)) != expected_assertions:
        reasons.append(f"{label}主断言数量与固定协议不一致")
    if int(correctness.get("primary_unique_paths", 0)) != expected_assertions:
        reasons.append(f"{label}主断言没有保持唯一路径口径")
    expected_positive = sum(
        int(workloads.get(kind, 0)) * count
        for kind, count in EXPECTED_POSITIVE_ASSERTIONS_PER_BURST.items()
    )
    expected_negative = expected_assertions - expected_positive
    if (
        int(correctness.get("positive_total", 0)) != expected_positive
        or int(correctness.get("negative_total", 0)) != expected_negative
    ):
        reasons.append(f"{label}正负断言分区与固定协议不一致")
    expected_bursts = {
        str(index): (kind, EXPECTED_POSITIVE_ASSERTIONS_PER_BURST[kind])
        for index, kind in enumerate(
            ("save100", "git_clone", "subtree_rename") * 2,
            1,
        )
    }
    positive_by_burst = correctness.get("positive_by_burst", {})
    if set(positive_by_burst) != set(expected_bursts):
        reasons.append(f"{label}正向断言未按 6 个 burst 独立记账")
    else:
        for burst, (workload, total) in expected_bursts.items():
            row = positive_by_burst[burst]
            if (
                int(row.get("total", 0) or 0) != total
                or row.get("workloads") != [workload]
            ):
                reasons.append(f"{label}burst {burst} 正向断言口径不符")
    expected_probes = sum(
        int(workloads.get(kind, 0)) * count
        for kind, count in EXPECTED_PROBES_PER_BURST.items()
    )
    if int(correctness.get("visibility_total", 0)) != expected_probes:
        reasons.append(f"{label}可见性探针数量与固定协议不符")


def validate_suite_audit(legs: list[dict[str, Any]], reasons: list[str]) -> None:
    labels = {
        "git_sha": "Git SHA",
        "binary_sha256": "binary SHA256",
        "receipt_sha256": "构建回执 SHA256",
        "cargo_lock_sha256": "Cargo.lock SHA256",
        "validated_binary_sha256": "已验证 artifact SHA256",
        "initial_state_fingerprint": "初始状态指纹",
        "fixture_identity_sha256": "fixture 身份",
        "protocol_fingerprint": "协议参数指纹",
        "event_plan_sha256": "事件计划 SHA256",
    }
    for field, label in labels.items():
        values = {
            str(leg.get("audit", {}).get(field, ""))
            for leg in legs
            if str(leg.get("audit", {}).get(field, ""))
        }
        missing = any(not str(leg.get("audit", {}).get(field, "")) for leg in legs)
        if missing or len(values) != 1:
            reasons.append(f"跨腿 {label} 不一致或缺失")


def validate_leg(leg: dict[str, Any], reasons: list[str]) -> None:
    """执行不依赖配对另一腿的全部 fail-closed 验证。"""
    _validate_common_correctness(leg, reasons)
    _validate_mechanism(leg, reasons)
    _validate_protocol(leg, reasons)
    _validate_stability(leg, reasons)
