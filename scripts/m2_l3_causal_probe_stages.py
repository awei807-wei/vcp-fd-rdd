"""Online-independent stage evaluators for the M2 L3 causal probe."""

from __future__ import annotations

import re
from collections import Counter
from pathlib import Path
from typing import Any

from m2_causal_evidence import evaluate_target_m2_causality
from m2_l3_causal_probe_config import (
    PROBE_FILE_COUNT,
    PROBE_MIN_LEASE_REMAINING_SECS,
    PROBE_SETTLE_SECS,
    PROBE_SUBTREE_DEPTH,
    PROBE_VISIBILITY_TOLERANCE_SECS,
)
from m2_l3_causal_probe_snapshot import post_mutation_log_lines


WATCH_REMOVE_RE = re.compile(
    r"tiered ephemeral watcher remove failed for (?P<path>.+?): "
)


def _stage(reasons: list[str], **details: Any) -> dict[str, Any]:
    return {"status": "fail" if reasons else "pass", "reasons": reasons, **details}


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError, OverflowError):
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError, OverflowError):
        return default


def protocol_stage(rows: list[dict[str, Any]]) -> dict[str, Any]:
    starts = [row for row in rows if row.get("event_kind") == "burst_started"]
    reasons: list[str] = []
    if len(starts) != 1:
        reasons.append(f"必须精确记录 1 个 burst_started，实际 {len(starts)}")
        return _stage(reasons, burst_count=len(starts))
    row = starts[0]
    requested = str(row.get("requested_tier", "")).upper()
    observed = str(row.get("tier_before", "")).upper()
    action = str(row.get("target_m2_action", ""))
    observed_at = _safe_int(row.get("target_m2_observed_unix_secs"))
    expires_at = _safe_int(row.get("target_m2_expires_unix_secs"))
    if requested != "L3" or observed != "L3":
        reasons.append(f"写入前必须为 L3，requested={requested} observed={observed}")
    if row.get("target_m2_debug_ok") is not True:
        reasons.append("写入前 target M2 debug 失败")
    if row.get("target_m2_entry_present") is not True:
        reasons.append("写入前缺少 exact M2 entry")
    if row.get("target_m2_active") is not True:
        reasons.append("写入前 M2 lease 未激活")
    if action not in {"ephemeral_watch", "fast_scan_lease", "scan_only"}:
        reasons.append(f"写入前 M2 action 非法：{action or 'missing'}")
    if observed_at <= 0 or expires_at <= observed_at:
        reasons.append("写入前 M2 lease 已过期或缺少到期时间")
    elif expires_at - observed_at < PROBE_MIN_LEASE_REMAINING_SECS:
        reasons.append(f"写入前 M2 lease 剩余时间不足 {PROBE_MIN_LEASE_REMAINING_SECS} 秒")
    return _stage(
        reasons,
        burst_count=1,
        requested_tier=requested,
        observed_tier=observed,
        action=action,
        lease_remaining_secs=max(0, expires_at - observed_at),
    )


def causal_stage(rows: list[dict[str, Any]]) -> dict[str, Any]:
    writes = [row for row in rows if row.get("event_kind") == "burst_written"]
    checks = [row for row in rows if row.get("event_kind") == "burst_checked"]
    reasons: list[str] = []
    if len(writes) != 1 or len(checks) != 1:
        reasons.append(
            f"必须各有 1 个 burst_written/burst_checked，实际 {len(writes)}/{len(checks)}"
        )
        return _stage(reasons, writes=len(writes), checks=len(checks))
    evidence = evaluate_target_m2_causality(writes[0], checks[0])
    reasons.extend(str(reason) for reason in evidence.pop("reasons"))
    return _stage(reasons, **evidence)


def _query_ok(row: dict[str, Any], should_exist: bool) -> bool:
    return (
        row.get("transport_ok") is True
        and row.get("first_query_exists") is should_exist
        and row.get("correct") is True
    )


def visibility_stage(rows: list[dict[str, Any]]) -> dict[str, Any]:
    first = [
        row
        for row in rows
        if row.get("event_kind") == "first_query"
        and row.get("workload") == "subtree_rename"
    ]
    new_rows = [row for row in first if row.get("should_exist") is True]
    old_rows = [row for row in first if row.get("should_exist") is False]
    probes = [
        row
        for row in rows
        if row.get("event_kind") == "visibility_probe"
        and row.get("workload") == "subtree_rename"
    ]
    new_ok = sum(_query_ok(row, True) for row in new_rows)
    old_ok = sum(_query_ok(row, False) for row in old_rows)
    probe_ok = sum(
        row.get("visible") is True and row.get("timeout") is not True for row in probes
    )
    ages = [_safe_float(row.get("event_age_secs")) for row in first]
    latencies = [_safe_float(row.get("latency_secs")) for row in probes]
    observed_max = max([*ages, *latencies], default=0.0)
    effective_sla = PROBE_SETTLE_SECS + PROBE_VISIBILITY_TOLERANCE_SECS
    reasons: list[str] = []
    new_paths = [str(row.get("path", "")) for row in new_rows]
    old_paths = [str(row.get("path", "")) for row in old_rows]
    probe_paths = [str(row.get("path", "")) for row in probes]
    if len(set(new_paths)) != PROBE_FILE_COUNT:
        reasons.append("新路径必须为 10 个唯一值")
    if len(set(old_paths)) != PROBE_FILE_COUNT:
        reasons.append("旧路径必须为 10 个唯一值")

    def suffixes(paths: list[str], anchor: str) -> set[tuple[str, ...]]:
        values: set[tuple[str, ...]] = set()
        for value in paths:
            parts = Path(value).parts
            if anchor not in parts:
                continue
            index = parts.index(anchor)
            suffix = tuple(parts[index + 1 :])
            if len(suffix) == PROBE_SUBTREE_DEPTH + 1:
                values.add(suffix)
        return values

    new_suffixes = suffixes(new_paths, "dir_b")
    old_suffixes = suffixes(old_paths, "dir_a")
    if len(new_suffixes) != PROBE_FILE_COUNT:
        reasons.append("新路径未形成三层深的 10 个唯一 suffix")
    if len(old_suffixes) != PROBE_FILE_COUNT:
        reasons.append("旧路径未形成三层深的 10 个唯一 suffix")
    if new_suffixes != old_suffixes:
        reasons.append("dir_a/dir_b 新旧路径 suffix 未一一配对")
    if set(probe_paths) != set(new_paths):
        reasons.append("连续可见性探针未覆盖精确的新路径集合")
    if len(new_rows) != PROBE_FILE_COUNT or new_ok != PROBE_FILE_COUNT:
        reasons.append(f"新路径必须 10/10 可见，实际 {new_ok}/{len(new_rows)}")
    if len(old_rows) != PROBE_FILE_COUNT or old_ok != PROBE_FILE_COUNT:
        reasons.append(f"旧路径必须 10/10 隐藏，实际 {old_ok}/{len(old_rows)}")
    if len(probes) != PROBE_FILE_COUNT or probe_ok != PROBE_FILE_COUNT:
        reasons.append(f"连续可见性探针必须 10/10，实际 {probe_ok}/{len(probes)}")
    if observed_max > effective_sla:
        reasons.append(f"可见性超过 5 秒 SLA 加 1 秒调度容差：{observed_max:.3f}s")
    return _stage(
        reasons,
        new_total=len(new_rows),
        new_ok=new_ok,
        old_total=len(old_rows),
        old_ok=old_ok,
        visibility_total=len(probes),
        visibility_ok=probe_ok,
        observed_max_secs=round(observed_max, 3),
        effective_sla_secs=effective_sla,
    )


def _watch_remove_failures(log_text: str) -> Counter[str]:
    failures: Counter[str] = Counter()
    for line in log_text.splitlines():
        match = WATCH_REMOVE_RE.search(line)
        if match:
            failures[match.group("path").strip()] += 1
    return failures


def watcher_stage(rows: list[dict[str, Any]], log_text: str) -> dict[str, Any]:
    cleanups = [row for row in rows if row.get("event_kind") == "burst_cleanup"]
    audits = [row for row in rows if row.get("event_kind") == "post_cleanup_audit"]
    failures = _watch_remove_failures("\n".join(post_mutation_log_lines(rows, log_text)))
    repeat_max = max(failures.values(), default=0)
    reasons: list[str] = []
    cleanup_ok = (
        len(cleanups) == 1
        and cleanups[0].get("ok") is True
        and cleanups[0].get("removed") is True
    )
    if not cleanup_ok:
        reasons.append("workload cleanup 缺失或未删除干净")
    audit = audits[0] if len(audits) == 1 else {}
    if len(audits) != 1:
        reasons.append(f"post-cleanup watcher audit 必须精确为 1，实际 {len(audits)}")
    else:
        audit_reason_count = len(reasons)
        if audit.get("debug_ok") is not True:
            reasons.append("post-cleanup watcher debug 请求失败")
        if audit.get("cleanup_target_exists") is True:
            reasons.append("cleanup target 在审计时仍存在")
        if audit.get("target_ephemeral_watch") is True:
            reasons.append("cleanup 后 exact root 仍持有 ephemeral watch")
        if audit.get("target_rotating_active") is True:
            reasons.append("cleanup 后 exact root 仍持有 rotating lease")
        if int(audit.get("cleanup_target_ephemeral_watch_dirs", 0) or 0) > 0:
            reasons.append("已删除 subtree 下仍有 ephemeral watch")
        if int(audit.get("cleanup_target_rotating_active_dirs", 0) or 0) > 0:
            reasons.append("已删除 subtree 下仍有 rotating lease")
        if audit.get("watcher_ledger_cleared") is not True:
            reasons.append("post-cleanup watcher 账本未清除")
        if audit.get("ok") is not True and len(reasons) == audit_reason_count:
            reasons.append("post-cleanup watcher audit 返回 ok=false")
    if repeat_max > 1:
        reasons.append(f"同一路径 watcher remove 失败重复出现 {repeat_max} 次")
    return _stage(
        reasons,
        cleanup_count=len(cleanups),
        audit_count=len(audits),
        audit_debug_ok=audit.get("debug_ok") is True,
        cleanup_target_exists=audit.get("cleanup_target_exists") is True,
        target_entry_present=audit.get("target_entry_present") is True,
        target_ephemeral_watch=audit.get("target_ephemeral_watch") is True,
        target_rotating_active=audit.get("target_rotating_active") is True,
        target_rotating_action=str(audit.get("target_rotating_action", "")),
        cleanup_target_ephemeral_watch_dirs=_safe_int(
            audit.get("cleanup_target_ephemeral_watch_dirs")
        ),
        cleanup_target_rotating_active_dirs=_safe_int(
            audit.get("cleanup_target_rotating_active_dirs")
        ),
        watcher_ledger_cleared=audit.get("watcher_ledger_cleared") is True,
        remove_failure_count=sum(failures.values()),
        remove_failure_repeat_max=repeat_max,
        repeated_paths=sorted(path for path, count in failures.items() if count > 1),
    )
