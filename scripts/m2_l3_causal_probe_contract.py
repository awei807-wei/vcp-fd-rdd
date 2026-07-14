"""Frozen-run contract and provenance validation for the M2 L3 probe."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from m2_l3_causal_probe_config import (
    PROBE_ALLOWED_AB_GATE_REASONS,
    PROBE_DURATION_SECS,
    PROBE_FILE_COUNT,
    PROBE_MIN_LEASE_REMAINING_SECS,
    PROBE_POST_CLEANUP_AUDIT_SECS,
    PROBE_PRECONDITION_WAIT_SECS,
    PROBE_ROTATING_TICK_SECS,
    PROBE_ROTATING_TTL_SECS,
    PROBE_SETTLE_SECS,
    PROBE_START_DELAY_SECS,
    PROBE_SUBTREE_DEPTH,
)


FROZEN_RUNNER_ARGS: dict[str, Any] = {
    "duration_secs": PROBE_DURATION_SECS,
    "event_storm": True,
    "event_storm_kind": "subtree_rename",
    "event_storm_target_tier": "L3",
    "event_storm_ops": PROBE_FILE_COUNT,
    "event_storm_file_count": PROBE_FILE_COUNT,
    "event_storm_depth": PROBE_SUBTREE_DEPTH,
    "event_storm_start_delay_secs": float(PROBE_START_DELAY_SECS),
    "event_storm_precondition_wait_secs": float(PROBE_PRECONDITION_WAIT_SECS),
    "event_storm_min_lease_remaining_secs": float(PROBE_MIN_LEASE_REMAINING_SECS),
    "event_storm_settle_secs": float(PROBE_SETTLE_SECS),
    "event_storm_timeout_secs": 0.0,
    "event_storm_interval_secs": 300.0,
    "event_storm_max_bursts": 1,
    "event_storm_visibility_probes_per_burst": PROBE_FILE_COUNT,
    "event_storm_visibility_poll_interval_secs": 0.25,
    "event_storm_post_cleanup_audit_secs": float(PROBE_POST_CLEANUP_AUDIT_SECS),
    "event_storm_fixed_root_schedule": True,
    "event_storm_deterministic_plan": True,
    "event_storm_strict_protocol": True,
    "snapshot_interval_secs": 3600,
    "snapshot_path_disk": True,
    "rotating_cold_window": True,
    "rotating_budget": 1,
    "rotating_tick_secs": PROBE_ROTATING_TICK_SECS,
    "rotating_ttl_secs": PROBE_ROTATING_TTL_SECS,
    "rotating_max_dirs_per_tick": 1,
    "max_watch_dirs": 1,
    "l0_max_cost_per_root": 1,
    "workload_seed": 42,
}


def parse_utc_timestamp(value: Any) -> datetime | None:
    """Parse only timezone-aware ISO-8601 timestamps from evidence."""
    if not isinstance(value, str) or not value:
        return None
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def manifest_protocol_reasons(manifest: dict[str, Any]) -> list[str]:
    runner_args = manifest.get("runner_args")
    if not isinstance(runner_args, dict):
        return ["manifest.runner_args 缺失"]
    reasons = [
        f"runner_args.{key}={runner_args.get(key)!r}，期望 {expected!r}"
        for key, expected in FROZEN_RUNNER_ARGS.items()
        if runner_args.get(key) != expected
    ]
    roots = runner_args.get("root")
    if not isinstance(roots, list) or len(roots) != 1:
        reasons.append(f"runner_args.root 必须精确为单根，实际 {roots!r}")
    if runner_args.get("event_storm_root") != roots:
        reasons.append("event_storm_root 必须与唯一 indexed root 完全一致")
    raw_port = runner_args.get("port", 0)
    try:
        port = int(raw_port or 0)
    except (TypeError, ValueError):
        port = 0
    if not 1 <= port <= 65535:
        reasons.append(f"runner_args.port 非法：{raw_port!r}")
    return reasons


def benchmark_metadata(
    manifest: dict[str, Any], summary: dict[str, Any]
) -> dict[str, Any]:
    manifest_raw = manifest.get("ab_comparability_reasons", [])
    summary_raw = summary.get("ab_comparability_reasons", [])
    reasons_valid = all(
        isinstance(raw, list) and all(isinstance(item, str) for item in raw)
        for raw in (manifest_raw, summary_raw)
    )
    reasons = sorted({*manifest_raw, *summary_raw}) if reasons_valid else []
    manifest_exit = manifest.get("fd_rdd_exit_code")
    summary_exit = summary.get("fd_rdd_exit_code")
    manifest_fatal = str(manifest.get("fatal_error", "") or "")
    summary_fatal = str(summary.get("fatal_error", "") or "")
    lifecycle_consistent = (
        manifest_exit == summary_exit and manifest_fatal == summary_fatal
    )
    ab_consistent = (
        reasons_valid
        and manifest.get("ab_comparable") == summary.get("ab_comparable")
        and manifest_raw == summary_raw
    )
    return {
        "fd_rdd_exit_code": summary_exit if lifecycle_consistent else None,
        "fatal_error": summary_fatal or manifest_fatal,
        "manifest_fd_rdd_exit_code": manifest_exit,
        "summary_fd_rdd_exit_code": summary_exit,
        "manifest_fatal_error": manifest_fatal,
        "summary_fatal_error": summary_fatal,
        "lifecycle_consistent": lifecycle_consistent,
        "manifest_ab_comparable": manifest.get("ab_comparable"),
        "summary_ab_comparable": summary.get("ab_comparable"),
        "ab_comparability_reasons": reasons,
        "ab_comparability_reasons_valid": reasons_valid,
        "ab_comparability_consistent": ab_consistent,
    }


def benchmark_infrastructure_reasons(
    manifest: dict[str, Any], summary: dict[str, Any]
) -> list[str]:
    metadata = benchmark_metadata(manifest, summary)
    reasons: list[str] = []
    if metadata["lifecycle_consistent"] is not True:
        reasons.append("manifest/summary daemon 生命周期证据不一致")
    if (
        metadata["manifest_fd_rdd_exit_code"] != 0
        or metadata["summary_fd_rdd_exit_code"] != 0
        or metadata["manifest_fatal_error"]
        or metadata["summary_fatal_error"]
    ):
        reasons.append("manifest/summary 记录 daemon 非正常退出或 fatal_error")
    if metadata["ab_comparability_consistent"] is not True:
        reasons.append("manifest/summary A/B 可比性证据不一致或格式非法")
    comparable = metadata["manifest_ab_comparable"]
    ab_reasons = metadata["ab_comparability_reasons"]
    if comparable is True and ab_reasons:
        reasons.append("A/B comparable=true 但仍记录不可比原因")
    elif comparable is False:
        unknown = sorted(set(ab_reasons) - PROBE_ALLOWED_AB_GATE_REASONS)
        if not ab_reasons:
            reasons.append("A/B comparable=false 但缺少不可比原因")
        if unknown:
            reasons.append(f"A/B 不可比原因不在 focused 白名单：{unknown}")
    elif comparable is not True:
        reasons.append(f"manifest.ab_comparable 非布尔值：{comparable!r}")
    return reasons


def _manifest_root(manifest: dict[str, Any]) -> Path | None:
    runner_args = manifest.get("runner_args")
    roots = runner_args.get("root") if isinstance(runner_args, dict) else None
    if not isinstance(roots, list) or len(roots) != 1:
        return None
    value = roots[0]
    if not isinstance(value, str) or not value:
        return None
    return Path(value).resolve(strict=False)


def _path_with_anchor_parent(path: Path, anchor: str) -> Path | None:
    current = path
    while current != current.parent:
        if current.name == anchor:
            return current.parent
        current = current.parent
    return None


def root_binding_infrastructure_reasons(
    manifest: dict[str, Any], rows: list[dict[str, Any]]
) -> list[str]:
    expected_root = _manifest_root(manifest)
    if expected_root is None:
        return ["无法从 manifest 确定唯一根，不能验证证据路径"]
    reasons: list[str] = []
    rooted_kinds = {
        "burst_started",
        "burst_written",
        "burst_checked",
        "burst_cleanup",
        "post_cleanup_audit",
    }
    for row in rows:
        kind = str(row.get("event_kind", ""))
        if kind in rooted_kinds:
            raw_root = row.get("root")
            actual_root = (
                Path(raw_root).resolve(strict=False)
                if isinstance(raw_root, str) and raw_root
                else None
            )
            if actual_root != expected_root:
                reasons.append(f"{kind}.root 未绑定 manifest 唯一根")
        if kind in {"expected", "first_query", "visibility_probe"}:
            raw_path = row.get("path")
            actual_path = (
                Path(raw_path).resolve(strict=False)
                if isinstance(raw_path, str) and raw_path
                else None
            )
            if actual_path is None or not actual_path.is_relative_to(expected_root):
                reasons.append(f"{kind}.path 未位于 manifest 唯一根内")
        if kind in {"burst_cleanup", "post_cleanup_audit"}:
            raw_target = row.get("cleanup_target")
            target = (
                Path(raw_target).resolve(strict=False)
                if isinstance(raw_target, str) and raw_target
                else None
            )
            if (
                target is None
                or target == expected_root
                or not target.is_relative_to(expected_root)
            ):
                reasons.append(f"{kind}.cleanup_target 未绑定 manifest 唯一根子树")

    new_bursts = {
        burst
        for row in rows
        if row.get("event_kind") == "first_query"
        and row.get("operation") == "subtree_rename_new_visible_first_query"
        and isinstance(row.get("path"), str)
        for burst in [_path_with_anchor_parent(Path(row["path"]).resolve(), "dir_b")]
        if burst is not None
    }
    old_bursts = {
        burst
        for row in rows
        if row.get("event_kind") == "first_query"
        and row.get("operation") == "subtree_rename_old_hidden_first_query"
        and isinstance(row.get("path"), str)
        for burst in [_path_with_anchor_parent(Path(row["path"]).resolve(), "dir_a")]
        if burst is not None
    }
    if len(new_bursts) != 1 or len(old_bursts) != 1:
        reasons.append("subtree rename 新旧路径必须各自绑定唯一 burst")
    elif new_bursts != old_bursts:
        reasons.append("subtree rename 新旧路径未绑定同一个 burst")
    burst_root = next(iter(new_bursts or old_bursts), None)
    if burst_root is not None and burst_root.parent != expected_root:
        reasons.append("subtree rename burst 未直接位于 manifest 唯一根下")
    cleanup_targets = {
        Path(row["cleanup_target"]).resolve(strict=False)
        for row in rows
        if row.get("event_kind") in {"burst_cleanup", "post_cleanup_audit"}
        and isinstance(row.get("cleanup_target"), str)
    }
    if len(cleanup_targets) != 1:
        reasons.append("cleanup/audit 必须共享唯一 cleanup target")
    elif burst_root is not None and cleanup_targets != {burst_root}:
        reasons.append("cleanup/audit target 未绑定 subtree rename burst")
    return sorted(set(reasons))


def timeline_infrastructure_reasons(
    manifest: dict[str, Any], rows: list[dict[str, Any]]
) -> list[str]:
    created = parse_utc_timestamp(manifest.get("created_at"))
    finished = parse_utc_timestamp(manifest.get("finished_at"))
    reasons: list[str] = []
    if created is None or finished is None or created > finished:
        return ["manifest 运行时间边界缺失、非法或倒序"]
    timeline: list[tuple[str, datetime]] = []
    for kind in (
        "burst_started",
        "burst_written",
        "burst_checked",
        "burst_cleanup",
        "post_cleanup_audit",
    ):
        matching = [row for row in rows if row.get("event_kind") == kind]
        if len(matching) != 1:
            reasons.append(f"{kind} 时间证据必须精确为 1，实际 {len(matching)}")
            continue
        event_time = parse_utc_timestamp(matching[0].get("ts"))
        if event_time is None:
            reasons.append(f"{kind}.ts 时间格式非法或缺失")
            continue
        if not created <= event_time <= finished:
            reasons.append(f"{kind}.ts 未落在本轮 manifest 时间边界内")
        timeline.append((kind, event_time))
    if any(left[1] > right[1] for left, right in zip(timeline, timeline[1:])):
        reasons.append("burst/audit 关键事件时间顺序倒挂")
    return reasons
