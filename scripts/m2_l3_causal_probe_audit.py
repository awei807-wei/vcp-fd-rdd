"""Watcher and snapshot evidence schema checks for the M2 L3 probe."""

from __future__ import annotations

from typing import Any

from m2_l3_causal_probe_config import (
    PROBE_ROTATING_TICK_SECS,
    PROBE_ROTATING_TTL_SECS,
)
from m2_l3_causal_probe_contract import parse_utc_timestamp


def evidence_schema_infrastructure_reasons(
    rows: list[dict[str, Any]],
) -> list[str]:
    required_numeric: dict[str, tuple[str, ...]] = {
        "burst_started": (
            "target_m2_cycle_id",
            "target_m2_scan_seq",
            "target_m2_scan_cycle_id",
            "target_m2_event_seq",
            "target_m2_event_cycle_id",
            "target_m2_observed_unix_secs",
            "target_m2_expires_unix_secs",
        ),
        "burst_written": (
            "target_m2_cycle_id",
            "target_m2_scan_seq",
            "target_m2_event_seq",
            "target_m2_fence_cycle_id",
            "target_m2_fence_scan_seq",
            "target_m2_fence_scan_cycle_id",
            "target_m2_fence_event_seq",
            "target_m2_fence_event_cycle_id",
            "target_m2_fence_expires_unix_secs",
        ),
        "burst_checked": (
            "target_m2_after_cycle_id",
            "target_m2_after_scan_seq",
            "target_m2_after_scan_cycle_id",
            "target_m2_after_event_seq",
            "target_m2_after_event_cycle_id",
        ),
    }
    required_boolean: dict[str, tuple[str, ...]] = {
        "burst_started": (
            "target_m2_debug_ok",
            "target_m2_entry_present",
            "target_m2_active",
        ),
        "burst_written": (
            "target_m2_debug_ok",
            "target_m2_entry_present",
            "target_m2_active",
            "target_m2_fence_debug_ok",
            "target_m2_fence_entry_present",
            "target_m2_fence_active",
        ),
        "burst_checked": (
            "target_m2_after_debug_ok",
            "target_m2_after_entry_present",
            "target_m2_after_active",
        ),
    }
    reasons: list[str] = []
    for kind in required_numeric:
        matching = [row for row in rows if row.get("event_kind") == kind]
        if len(matching) != 1:
            continue
        row = matching[0]
        for field in required_numeric[kind]:
            if type(row.get(field)) is not int:
                reasons.append(f"{kind}.{field} 缺失或不是 int")
        for field in required_boolean[kind]:
            if type(row.get(field)) is not bool:
                reasons.append(f"{kind}.{field} 缺失或不是 bool")
    return reasons


def audit_infrastructure_reasons(rows: list[dict[str, Any]]) -> list[str]:
    audits = [row for row in rows if row.get("event_kind") == "post_cleanup_audit"]
    if len(audits) != 1:
        return [f"post-cleanup audit 必须精确为 1，实际 {len(audits)}"]
    audit = audits[0]
    reasons: list[str] = []
    for field in (
        "debug_ok",
        "cleanup_removed",
        "cleanup_target_exists",
        "target_entry_present",
        "target_ephemeral_watch",
        "target_rotating_active",
        "watcher_ledger_cleared",
        "audit_after_lease_expiry",
        "audit_before_next_rotation",
        "audit_window_valid",
        "ok",
    ):
        if type(audit.get(field)) is not bool:
            reasons.append(f"post-cleanup audit 字段 {field} 缺失或不是 bool")
    if type(audit.get("target_rotating_action")) is not str:
        reasons.append("post-cleanup audit 字段 target_rotating_action 缺失或不是 string")
    for field in (
        "cleanup_target_entries",
        "cleanup_target_ephemeral_watch_dirs",
        "cleanup_target_rotating_active_dirs",
    ):
        value = audit.get(field)
        if type(value) is not int or value < 0:
            reasons.append(f"post-cleanup audit 字段 {field} 缺失或不是非负 int")
    if audit.get("debug_ok") is not True:
        reasons.append("post-cleanup audit debug 请求失败")
    raw_fields = {
        name: audit.get(name)
        for name in (
            "audit_unix_secs",
            "lease_expires_unix_secs",
            "next_rotation_estimate_unix_secs",
        )
    }
    if any(type(value) is not int or value <= 0 for value in raw_fields.values()):
        reasons.append("post-cleanup audit 原始时间戳缺失或非法")
        return reasons
    audited = raw_fields["audit_unix_secs"]
    lease_expires = raw_fields["lease_expires_unix_secs"]
    next_rotation = raw_fields["next_rotation_estimate_unix_secs"]
    expected_next_rotation = (
        lease_expires - PROBE_ROTATING_TTL_SECS + PROBE_ROTATING_TICK_SECS
    )
    after_expiry = audited >= lease_expires
    before_rotation = audited < next_rotation
    window_valid = after_expiry and before_rotation
    audit_ts = parse_utc_timestamp(audit.get("ts"))
    if audit_ts is None or abs(int(audit_ts.timestamp()) - audited) > 1:
        reasons.append("post-cleanup audit.ts 与 audit_unix_secs 不一致")
    writes = [row for row in rows if row.get("event_kind") == "burst_written"]
    fence_expires = (
        writes[0].get("target_m2_fence_expires_unix_secs")
        if len(writes) == 1
        else None
    )
    if type(fence_expires) is not int or fence_expires != lease_expires:
        reasons.append("post-cleanup audit lease 到期时间与 write fence 不一致")
    if next_rotation != expected_next_rotation:
        reasons.append("post-cleanup audit 下一 rotation 时间与冻结 tick/TTL 不一致")
    if audit.get("audit_after_lease_expiry") is not after_expiry:
        reasons.append("post-cleanup audit_after_lease_expiry 与原始时间戳不一致")
    if audit.get("audit_before_next_rotation") is not before_rotation:
        reasons.append("post-cleanup audit_before_next_rotation 与原始时间戳不一致")
    if audit.get("audit_window_valid") is not window_valid:
        reasons.append("post-cleanup audit_window_valid 与原始时间戳不一致")
    if not window_valid:
        reasons.append("post-cleanup audit 未落在 lease 到期与下一 rotation 之间的时间窗")
    return reasons


def snapshot_schema_infrastructure_reasons(summary: dict[str, Any]) -> list[str]:
    quiesce = summary.get("shutdown_snapshot_quiesce")
    if not isinstance(quiesce, dict):
        return ["shutdown_snapshot_quiesce 缺失或不是 object"]
    reasons: list[str] = []
    for field in ("count", "ok", "failures"):
        value = quiesce.get(field)
        if type(value) is not int or value < 0:
            reasons.append(f"snapshot quiesce 字段 {field} 缺失或不是非负 int")
    for field in ("ready", "written", "rebuild_observed", "daemon_log_window_valid"):
        if type(quiesce.get(field)) is not bool:
            reasons.append(f"snapshot quiesce 字段 {field} 缺失或不是 bool")
    for field in ("last_daemon_error", "error"):
        if not isinstance(quiesce.get(field), str):
            reasons.append(f"snapshot quiesce 字段 {field} 缺失或不是 string")
    return reasons
