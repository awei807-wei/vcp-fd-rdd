"""Markdown rendering for the focused M2 L3 causal probe."""

from __future__ import annotations

from typing import Any

from m2_l3_causal_probe_config import PROBE_POST_CLEANUP_AUDIT_SECS


def _render_stage_evidence(stages: dict[str, Any]) -> str:
    protocol = stages.get("protocol", {})
    causal = stages.get("causal_progress", {})
    rename = stages.get("rename_visibility", {})
    watcher = stages.get("watcher_cleanup", {})
    snapshot = stages.get("snapshot_persistence", {})
    markers = snapshot.get("snapshot_failure_markers", {})
    marker_total = sum(int(value or 0) for value in markers.values())
    return "\n".join(
        (
            "- 协议："
            f"requested/observed=`{protocol.get('requested_tier', '')}/"
            f"{protocol.get('observed_tier', '')}`，"
            f"action=`{protocol.get('action', '')}`，"
            f"lease 剩余 `{protocol.get('lease_remaining_secs', 0)}s`",
            "- 因果："
            f"scan_seq `{causal.get('fence_scan_seq', 0)} → "
            f"{causal.get('after_scan_seq', 0)}`，"
            f"event_seq `{causal.get('fence_event_seq', 0)} → "
            f"{causal.get('after_event_seq', 0)}`",
            "- 重命名："
            f"新路径 `{rename.get('new_ok', 0)}/{rename.get('new_total', 0)}`，"
            f"旧路径 `{rename.get('old_ok', 0)}/{rename.get('old_total', 0)}`，"
            f"连续探针 `{rename.get('visibility_ok', 0)}/"
            f"{rename.get('visibility_total', 0)}`，"
            f"最慢 `{rename.get('observed_max_secs', 0)}s`",
            "- Watcher："
            f"cleanup/audit=`{watcher.get('cleanup_count', 0)}/"
            f"{watcher.get('audit_count', 0)}`，"
            f"ephemeral_watch=`{watcher.get('target_ephemeral_watch', False)}`，"
            f"rotating_active=`{watcher.get('target_rotating_active', False)}`，"
            f"ledger_cleared=`{watcher.get('watcher_ledger_cleared', False)}`",
            "- 快照："
            f"ready=`{snapshot.get('quiesce_ready', False)}`，"
            f"written=`{snapshot.get('quiesce_written', False)}`，"
            f"rebuild=`{snapshot.get('quiesce_rebuild_observed', False)}`，"
            f"失败标记=`{marker_total}`",
        )
    )


def render_report(result: dict[str, Any]) -> str:
    """Render one standalone focused-probe report."""
    rows = []
    for name, stage in result.get("stages", {}).items():
        reasons = "; ".join(stage.get("reasons", [])) or "—"
        rows.append(f"| {name} | {stage.get('status', 'missing')} | {reasons} |")
    reason_lines = "\n".join(
        f"- {reason}" for reason in result.get("gate_reasons", [])
    ) or "- 无"
    return f"""# M2 单 L3 根因果探针报告

## 判定

`{result.get('decision', 'infrastructure_error')}`

| 阶段 | 状态 | 原因 |
|---|---|---|
{chr(10).join(rows)}

## 门禁原因

{reason_lines}

## 关键证据

{_render_stage_evidence(result.get('stages', {}))}

## 固定协议

- 单个 L3 根目录
- 一次 `subtree_rename`
- 三层深、10 个文件
- 写后 5 秒查询，允许 1 秒调度容差
- cleanup 后等待 {PROBE_POST_CLEANUP_AUDIT_SECS} 秒审计 watcher 账本
- 最后执行 snapshot quiesce

## 产物

- benchmark run: `{result.get('benchmark_run_dir', '')}`
- runner log: `{result.get('runner_log', '')}`
"""
