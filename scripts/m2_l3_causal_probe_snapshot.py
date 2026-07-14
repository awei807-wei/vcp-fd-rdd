"""Snapshot stage evaluation for the focused M2 L3 probe."""

from __future__ import annotations

from typing import Any

from m2_l3_causal_probe_contract import parse_utc_timestamp


SNAPSHOT_FAILURE_MARKERS = (
    "snapshot_upsert_unresolved",
    "direct_v7_unsupported",
    "Snapshot failed:",
    "Final snapshot failed:",
)


def post_mutation_log_lines(
    rows: list[dict[str, Any]], log_text: str
) -> list[str]:
    """Return log lines at or after the mutation fence."""
    writes = [row for row in rows if row.get("event_kind") == "burst_written"]
    mutation_ts = (
        parse_utc_timestamp(writes[0].get("ts")) if len(writes) == 1 else None
    )
    selected: list[str] = []
    for line in log_text.splitlines():
        token = line.split(" ", 1)[0] if " " in line else ""
        line_ts = parse_utc_timestamp(token)
        if mutation_ts is None or line_ts is None or line_ts >= mutation_ts:
            selected.append(line)
    return selected


def snapshot_stage(
    summary: dict[str, Any],
    rows: list[dict[str, Any]],
    log_text: str,
) -> dict[str, Any]:
    """Evaluate post-mutation snapshot persistence evidence."""
    quiesce = summary.get("shutdown_snapshot_quiesce")
    quiesce = quiesce if isinstance(quiesce, dict) else {}
    log_lines = post_mutation_log_lines(rows, log_text)
    marker_counts = {
        marker: sum(marker in line for line in log_lines)
        for marker in SNAPSHOT_FAILURE_MARKERS
    }
    rebuild_count = sum("Starting background rebuild:" in line for line in log_lines)
    reasons: list[str] = []
    if summary.get("fd_rdd_exit_code") != 0 or summary.get("fatal_error"):
        reasons.append("fd-rdd 未正常退出或 runner 记录 fatal_error")
    quiesce_ok = (
        quiesce.get("count") == 1
        and quiesce.get("ok") == 1
        and quiesce.get("failures") == 0
        and quiesce.get("ready") is True
        and quiesce.get("written") is True
    )
    if not quiesce_ok:
        reasons.append("cleanup 后 snapshot quiesce 未持久完成")
    if (
        quiesce.get("rebuild_observed") is True
        or quiesce.get("last_daemon_error")
        or quiesce.get("error")
        or quiesce.get("daemon_log_window_valid") is not True
    ):
        reasons.append("cleanup 后 snapshot quiesce 进入 rebuild 或返回 daemon error")
    if any(marker_counts.values()):
        reasons.append("mutation 后日志出现 snapshot 失败标记")
    if rebuild_count:
        reasons.append(f"mutation 后触发 background rebuild {rebuild_count} 次")
    return {
        "status": "fail" if reasons else "pass",
        "reasons": reasons,
        "quiesce_ready": quiesce.get("ready") is True,
        "quiesce_written": quiesce.get("written") is True,
        "quiesce_rebuild_observed": quiesce.get("rebuild_observed") is True,
        "snapshot_failure_markers": marker_counts,
        "background_rebuild_count": rebuild_count,
    }
