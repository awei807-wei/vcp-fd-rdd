"""M2 快速证伪腿级协议、正确性与稳定性验证。"""

from __future__ import annotations

from pathlib import Path
from datetime import datetime
from typing import Any

from m2_cold_window_ab_command import FALSIFICATION_EVENT_ROOT_NAMES


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
SWEEP_INTEGRATION_CYCLES = 1
SWEEP_INTEGRATION_TTL_SECS = 45
SWEEP_INTEGRATION_TICK_SECS = 15
SWEEP_INTEGRATION_PERIOD_SECS = 45
SWEEP_INTEGRATION_SETTLE_SECS = 150.0
SWEEP_INTEGRATION_REPAIR_DEADLINE_SECS = 75.0
SWEEP_INTEGRATION_COMPLETION_FENCE_TIMEOUT_SECS = (
    SWEEP_INTEGRATION_PERIOD_SECS
    + SWEEP_INTEGRATION_TTL_SECS
    + 2 * SWEEP_INTEGRATION_TICK_SECS
    + 30
)


def _timestamp(value: str) -> float | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def protocol_invalid_reasons(leg: dict[str, Any]) -> list[str]:
    """Return retryable protocol failures without mixing in product correctness."""
    label = leg_label(leg)
    protocol = leg.get("protocol", {})
    resources = leg.get("resources", {})
    reasons: list[str] = []
    physical_bursts = int(protocol.get("physical_bursts", 0) or 0)
    checks = int(protocol.get("checks", 0) or 0)
    if physical_bursts != 6 or checks != 6:
        reasons.append(
            f"{label}未完成 6 个 burst/check（{physical_bursts}/6, {checks}/6）"
        )
    if (
        resources.get("window_source") != "full_run"
        or int(resources.get("sample_count", 0) or 0) < 2
        or int(resources.get("event_window_sample_count", 0) or 0) < 2
    ):
        reasons.append(f"{label}资源采样窗口不完整")
    coverage = float(resources.get("sample_coverage_ratio", 0.0) or 0.0)
    if coverage < MIN_PROCESS_SAMPLE_COVERAGE:
        reasons.append(f"{label}process 采样覆盖率低于 95%（{coverage:.4f}）")
    max_gap = float(resources.get("sample_max_gap_secs", 0.0) or 0.0)
    if max_gap > MAX_PROCESS_SAMPLE_GAP_SECS:
        reasons.append(f"{label}process 采样最大间隔超过 3 秒（{max_gap:.3f}s）")
    audit = leg.get("audit", {})
    started = _timestamp(str(audit.get("daemon_started_at", "")))
    finished = _timestamp(str(audit.get("finished_at", "")))
    if started is None or finished is None or finished <= started:
        reasons.append(f"{label}缺少有效实际 start/end 时间")
    return reasons


def block_protocol_invalid_reasons(legs: list[dict[str, Any]]) -> list[str]:
    reasons = [reason for leg in legs for reason in protocol_invalid_reasons(leg)]
    if len(legs) != 2:
        reasons.append(f"block 协议必须包含完整 A/B 两腿，实际 {len(legs)}")
        return reasons
    starts = [
        _timestamp(str(leg.get("audit", {}).get("daemon_started_at", "")))
        for leg in legs
    ]
    if all(start is not None for start in starts):
        gap = abs(float(starts[1]) - float(starts[0]))
        if gap > 1800.0:
            reasons.append(f"block 两腿启动间隔超过 30 分钟（{gap:.3f}s）")
    return reasons


def sweep_integration_protocol_invalid_reasons(
    integration: dict[str, Any],
    *,
    expected_product_git_sha: str,
    expected_product_binary_sha256: str,
    expected_product_receipt_sha256: str,
    expected_protocol_fingerprint: str,
) -> list[str]:
    label = "§18.1 短 sweep 整合腿"
    protocol = integration.get("protocol", {})
    protocol = protocol if isinstance(protocol, dict) else {}
    audit = integration.get("audit", {})
    audit = audit if isinstance(audit, dict) else {}
    reasons: list[str] = []
    if integration.get("valid") is not True:
        reasons.append(f"{label}缺少完整成功的 fixture 终态")
    if audit.get("terminal_status") != "completed" or audit.get(
        "process_exit_code"
    ) != 0:
        reasons.append(f"{label}进程终态无效")
    if (
        expected_product_git_sha
        and audit.get("product_git_sha") != expected_product_git_sha
    ):
        reasons.append(f"{label} product Git SHA 不一致")
    harness_git_sha = str(audit.get("harness_git_sha", ""))
    if len(harness_git_sha) != 40 or any(
        character not in "0123456789abcdef" for character in harness_git_sha
    ):
        reasons.append(f"{label} harness Git SHA 无效")
    if audit.get("harness_worktree_dirty") is not False:
        reasons.append(f"{label} harness worktree 必须为 clean")
    if (
        expected_product_binary_sha256
        and audit.get("product_binary_sha256")
        != expected_product_binary_sha256
    ):
        reasons.append(f"{label} product binary SHA256 不一致")
    if (
        expected_product_receipt_sha256
        and audit.get("product_receipt_sha256")
        != expected_product_receipt_sha256
    ):
        reasons.append(f"{label}构建回执 SHA256 不一致")
    if (
        not expected_protocol_fingerprint
        or protocol.get("command_fingerprint") != expected_protocol_fingerprint
        or audit.get("command_fingerprint") != expected_protocol_fingerprint
    ):
        reasons.append(f"{label}协议参数指纹不一致")
    if int(protocol.get("cycles_requested", 0) or 0) != SWEEP_INTEGRATION_CYCLES:
        reasons.append(f"{label} cycles 配置不是 {SWEEP_INTEGRATION_CYCLES}")
    if int(protocol.get("cycles_observed", 0) or 0) < SWEEP_INTEGRATION_CYCLES:
        reasons.append(f"{label}未观测到完整 sweep cycle")
    expected = {
        "allow_cycle_shortfall": True,
        "burst_root_level": True,
        "sweep_completion_fence": True,
        "integration_repair_deadline_secs": (
            SWEEP_INTEGRATION_REPAIR_DEADLINE_SECS
        ),
        "rotating_cold_window": True,
        "fast_scan": True,
        "query_fast_scan_leases": False,
        "rotating_ttl_secs": SWEEP_INTEGRATION_TTL_SECS,
        "rotating_tick_secs": SWEEP_INTEGRATION_TICK_SECS,
        "rotating_full_sweep_period_secs": SWEEP_INTEGRATION_PERIOD_SECS,
        "settle_secs": SWEEP_INTEGRATION_SETTLE_SECS,
    }
    for key, value in expected.items():
        if protocol.get(key) != value:
            name = "full sweep period" if key == "rotating_full_sweep_period_secs" else key
            reasons.append(f"{label} {name} 配置不符（{protocol.get(key)!r} != {value!r}）")
    if int(protocol.get("watch_cycle_progress_pct_max", 0) or 0) < 100:
        reasons.append(f"{label}未从 watch-state 观测到 cycle progress 100%")
    for key in ("deep_modify", "sweep_only_modify"):
        probe = integration.get(key, {})
        probe = probe if isinstance(probe, dict) else {}
        if probe.get("skipped"):
            reasons.append(f"{label} {key} 被跳过：{probe.get('skipped')}")
    return list(dict.fromkeys(reasons))


def validate_sweep_integration(
    integration: dict[str, Any], reasons: list[str]
) -> None:
    label = "§18.1 短 sweep 整合腿"
    burst = integration.get("burst", {})
    burst = burst if isinstance(burst, dict) else {}
    if (
        burst.get("enabled") is not True
        or burst.get("visible") is not True
        or burst.get("root_level") is not True
    ):
        reasons.append(f"{label}同 daemon burst 可见性未通过")
    if burst.get("error"):
        reasons.append(f"{label} burst 存在错误：{burst.get('error')}")

    deep = integration.get("deep_modify", {})
    deep = deep if isinstance(deep, dict) else {}
    if deep.get("enabled") is not True:
        reasons.append(f"{label} deep modify 探针未启用")
    if deep.get("error"):
        reasons.append(f"{label} deep modify 存在错误：{deep.get('error')}")
    if deep.get("baseline_tier") not in {"ColdMmap", "FrozenManifestOnly"}:
        reasons.append(
            f"{label} deep modify 基线不是 ColdMmap/FrozenManifestOnly"
        )
    flagged = deep.get("flagged_secs")
    repaired = deep.get("repaired_secs")
    if not isinstance(flagged, (int, float)) or isinstance(flagged, bool):
        reasons.append(f"{label} verify 通道缺少 changed 打标证据")
    if deep.get("visible") is not True or not isinstance(
        repaired, (int, float)
    ) or isinstance(repaired, bool):
        reasons.append(f"{label} sweep 通道未完成索引修复")
    if deep.get("updated_tier") != "HotMemory":
        reasons.append(f"{label} deep modify 修复后未进入 HotMemory")
    if isinstance(flagged, (int, float)) and isinstance(repaired, (int, float)):
        if float(flagged) < 0 or float(flagged) > float(repaired):
            reasons.append(f"{label} verify/sweep 两相时间顺序无效")
        if float(repaired) > SWEEP_INTEGRATION_REPAIR_DEADLINE_SECS:
            reasons.append(
                f"{label}修复超过 {SWEEP_INTEGRATION_REPAIR_DEADLINE_SECS:.0f} 秒加速门"
            )

    sweep_only = integration.get("sweep_only_modify", {})
    sweep_only = sweep_only if isinstance(sweep_only, dict) else {}
    if sweep_only.get("enabled") is not True:
        reasons.append(f"{label} sweep-only 探针未启用")
    if sweep_only.get("error"):
        reasons.append(f"{label} sweep-only 存在错误：{sweep_only.get('error')}")
    if sweep_only.get("completion_fence") is not True:
        reasons.append(f"{label} sweep-only completion fence 未启用")
    if sweep_only.get("completion_fence_endpoint") != "/debug/tiered-watch":
        reasons.append(f"{label} sweep-only completion fence 端点不符")
    if (
        sweep_only.get("completion_fence_predicate")
        != "last_scan_seq > post_mutation_last_scan_seq and last_scan_cycle_id > post_mutation_cycle_id"
    ):
        reasons.append(f"{label} sweep-only completion fence 判定语义不符")
    timeout_secs = sweep_only.get("completion_fence_timeout_secs")
    if (
        sweep_only.get("completion_fence_timeout_formula")
        != "period+ttl+2*tick+30"
        or not isinstance(timeout_secs, (int, float))
        or isinstance(timeout_secs, bool)
        or float(timeout_secs) != SWEEP_INTEGRATION_COMPLETION_FENCE_TIMEOUT_SECS
    ):
        reasons.append(f"{label} sweep-only completion fence 超时配置不符")
    pre_seq = sweep_only.get("pre_mutation_last_scan_seq")
    pre_cycle = sweep_only.get("pre_mutation_cycle_id")
    post_seq = sweep_only.get("post_mutation_last_scan_seq")
    post_cycle = sweep_only.get("post_mutation_cycle_id")
    completion_seq = sweep_only.get("completion_last_scan_seq")
    completion_cycle = sweep_only.get("completion_last_scan_cycle_id")
    fence_numbers = (
        pre_seq,
        pre_cycle,
        post_seq,
        post_cycle,
        completion_seq,
        completion_cycle,
    )
    if any(
        not isinstance(value, int) or isinstance(value, bool)
        for value in fence_numbers
    ):
        reasons.append(f"{label} sweep-only completion fence 缺少序列证据")
    elif not (
        int(completion_seq) > int(post_seq)
        and int(completion_cycle) > int(post_cycle)
    ):
        reasons.append(f"{label} sweep-only completion fence 未跨越 post-mutation 水位")
    if sweep_only.get("completion_fence_observed") is not True:
        reasons.append(f"{label} sweep-only 未观测到 completion fence")
    waited = sweep_only.get("waited_secs")
    repair_deadline = sweep_only.get("repair_deadline_secs")
    if (
        not isinstance(repair_deadline, (int, float))
        or isinstance(repair_deadline, bool)
        or float(repair_deadline) != SWEEP_INTEGRATION_REPAIR_DEADLINE_SECS
    ):
        reasons.append(f"{label} sweep-only repair deadline 配置不符")
    if not isinstance(waited, (int, float)) or isinstance(waited, bool):
        reasons.append(f"{label} sweep-only 缺少 completion fence 等待时间")
    elif (
        float(waited) < 0
        or not isinstance(repair_deadline, (int, float))
        or isinstance(repair_deadline, bool)
        or float(waited) > float(repair_deadline)
    ):
        reasons.append(
            f"{label} sweep-only 后台修复超过 "
            f"{SWEEP_INTEGRATION_REPAIR_DEADLINE_SECS:.0f} 秒加速门"
        )
    searches_before = sweep_only.get("searches_before_fence")
    if not isinstance(searches_before, int) or isinstance(
        searches_before, bool
    ) or searches_before != 0:
        reasons.append(f"{label} sweep-only completion fence 前发生查询")
    first_query_count = sweep_only.get("first_query_count")
    if not isinstance(first_query_count, int) or isinstance(
        first_query_count, bool
    ) or first_query_count != 1:
        reasons.append(f"{label} sweep-only completion fence 后不是唯一首查")
    if (
        sweep_only.get("repaired_by_sweep") is not True
        or sweep_only.get("first_query_freshness") != "fresh"
        or sweep_only.get("first_query_tier") != "HotMemory"
    ):
        reasons.append(f"{label} sweep-only 首查未证明后台修复")


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
    if stability.get("watch_remove_failure_count", 0):
        reasons.append(f"{label} watcher remove 存在失败")
    if stability.get("dirty_queue_retry_drop_count", 0):
        reasons.append(f"{label} dirty queue repair 重试耗尽并丢弃")
    rebuild_count = int(stability.get("background_rebuild_count", 0) or 0)
    bootstrap_rebuild_count = int(
        stability.get("bootstrap_background_rebuild_count", 0) or 0
    )
    post_ready_rebuild_count = int(
        stability.get("post_ready_background_rebuild_count", rebuild_count) or 0
    )
    snapshot_quiesce_rebuild_count = int(
        stability.get("snapshot_quiesce_background_rebuild_count", 0) or 0
    )
    unattributed_post_ready_rebuild_count = int(
        stability.get(
            "unattributed_post_ready_background_rebuild_count",
            post_ready_rebuild_count,
        )
        or 0
    )
    if bootstrap_rebuild_count > 1:
        reasons.append(f"{label} bootstrap background rebuild 超过一次")
    if (
        "bootstrap_background_rebuild_count" in stability
        and "post_ready_background_rebuild_count" in stability
        and bootstrap_rebuild_count + post_ready_rebuild_count != rebuild_count
    ):
        reasons.append(f"{label} background rebuild 分账不一致")
    if post_ready_rebuild_count and not stability.get("snapshot_log_window_valid"):
        reasons.append(f"{label}缺少可验证的 snapshot quiesce 日志窗口")
    if (
        snapshot_quiesce_rebuild_count + unattributed_post_ready_rebuild_count
        != post_ready_rebuild_count
    ):
        reasons.append(f"{label} post-ready background rebuild 归因分账不一致")
    if unattributed_post_ready_rebuild_count:
        reasons.append(f"{label}出现未归因到 snapshot quiesce 的 background rebuild")
    if snapshot_quiesce_rebuild_count > 1:
        reasons.append(f"{label} snapshot quiesce background rebuild 超过一次")
    if snapshot_quiesce_rebuild_count and not stability.get(
        "snapshot_rebuild_observed"
    ):
        reasons.append(f"{label} snapshot rebuild 日志与 quiesce 回执不一致")
    if stability.get("snapshot_rebuild_observed") and not snapshot_quiesce_rebuild_count:
        reasons.append(f"{label} snapshot quiesce 回执缺少对应 rebuild 日志")
    if post_ready_rebuild_count > 1:
        reasons.append(f"{label} background rebuild 超过一次")
    if stability.get("direct_v7_unsupported_count", 0) and not (
        snapshot_quiesce_rebuild_count == 1
        and unattributed_post_ready_rebuild_count == 0
    ):
        reasons.append(f"{label} direct_v7_unsupported 未由安全 rebuild 收敛")


def _validate_protocol(leg: dict[str, Any], reasons: list[str]) -> None:
    label = leg_label(leg)
    protocol = leg["protocol"]
    configured = [str(root) for root in protocol.get("configured_event_roots", [])]
    event_roots = [str(root) for root in protocol.get("event_roots", [])]
    cold_root_ok = (
        len(configured) == len(FALSIFICATION_EVENT_ROOT_NAMES)
        and len(set(configured)) == len(FALSIFICATION_EVENT_ROOT_NAMES)
        and {Path(root).name for root in configured}
        == set(FALSIFICATION_EVENT_ROOT_NAMES)
        and set(event_roots) == set(configured)
    )
    if not cold_root_ok:
        reasons.append(f"{label}事件未精确使用固定六根池")
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
    target_entry_present = int(
        protocol.get("target_m2_entry_present_bursts", 0) or 0
    )
    target_seen = int(protocol.get("target_m2_seen_bursts", 0) or 0)
    target_active = int(protocol.get("target_m2_active_bursts", 0) or 0)
    target_unexpired = int(protocol.get("target_m2_unexpired_bursts", 0) or 0)
    target_causal = int(protocol.get("target_m2_causal_bursts", 0) or 0)
    target_actions = protocol.get("target_m2_actions", {})
    if target_debug_ok != physical_bursts:
        reasons.append(f"{label}缺少逐 burst 目标目录 M2 证据")
    if (
        leg.get("variant") == "a"
        and target_entry_present != physical_bursts
    ):
        reasons.append(f"{label}并非每个目标目录都有精确 M2 目标目录 entry")
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
        "planned_git_sha": "planned Git SHA",
        "executed_git_sha": "executed Git SHA",
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
    planned = {
        str(leg.get("audit", {}).get("planned_git_sha", "")) for leg in legs
    }
    executed = {
        str(leg.get("audit", {}).get("executed_git_sha", "")) for leg in legs
    }
    if planned != executed:
        reasons.append("planned Git SHA 与 executed Git SHA 不一致")
    query_modes = {
        bool(leg.get("protocol", {}).get("query_fast_scan_leases_enabled", True))
        for leg in legs
    }
    if len(query_modes) != 1:
        reasons.append("跨腿 Query Fast Scan lease 隔离变量不一致")


def validate_leg(leg: dict[str, Any], reasons: list[str]) -> None:
    """执行不依赖配对另一腿的全部 fail-closed 验证。"""
    _validate_common_correctness(leg, reasons)
    _validate_mechanism(leg, reasons)
    _validate_protocol(leg, reasons)
    _validate_stability(leg, reasons)
