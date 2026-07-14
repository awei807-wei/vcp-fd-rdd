"""Offline orchestration for the focused M2 L3 causal probe."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from m2_l3_causal_probe_audit import (
    audit_infrastructure_reasons,
    evidence_schema_infrastructure_reasons,
    snapshot_schema_infrastructure_reasons,
)
from m2_l3_causal_probe_contract import (
    benchmark_infrastructure_reasons,
    benchmark_metadata,
    manifest_protocol_reasons,
    root_binding_infrastructure_reasons,
    timeline_infrastructure_reasons,
)
from m2_l3_causal_probe_snapshot import snapshot_stage
from m2_l3_causal_probe_stages import (
    causal_stage,
    protocol_stage,
    visibility_stage,
    watcher_stage,
)


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain one JSON object")
    return value


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        value = json.loads(line)
        if not isinstance(value, dict):
            raise ValueError(f"{path}:{number} must contain one JSON object")
        rows.append(value)
    return rows


def _unreadable_result(run_dir: Path, exc: Exception) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "decision": "infrastructure_error",
        "benchmark_run_dir": str(run_dir),
        "gate_reasons": [f"probe 产物不可读：{exc}"],
        "stages": {},
    }


def analyze_run(run_dir: Path) -> dict[str, Any]:
    """Recompute a verdict while treating all on-disk evidence as untrusted."""
    try:
        manifest = _read_json(run_dir / "manifest.json")
        summary = _read_json(run_dir / "summary.json")
        rows = _read_jsonl(run_dir / "event-storm-samples.jsonl")
        log_text = (run_dir / "fd-rdd.log").read_text(encoding="utf-8")
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return _unreadable_result(run_dir, exc)

    infrastructure_reasons: list[str] = []
    infrastructure_reasons.extend(manifest_protocol_reasons(manifest))
    if manifest.get("run_state") != "completed":
        infrastructure_reasons.append(
            f"benchmark run_state={manifest.get('run_state', 'missing')}"
        )
    if manifest.get("completion_reason") != "duration_elapsed":
        infrastructure_reasons.append(
            f"benchmark completion_reason={manifest.get('completion_reason', 'missing')}"
        )
    infrastructure_reasons.extend(benchmark_infrastructure_reasons(manifest, summary))
    infrastructure_reasons.extend(root_binding_infrastructure_reasons(manifest, rows))
    infrastructure_reasons.extend(timeline_infrastructure_reasons(manifest, rows))
    infrastructure_reasons.extend(evidence_schema_infrastructure_reasons(rows))
    infrastructure_reasons.extend(audit_infrastructure_reasons(rows))
    infrastructure_reasons.extend(snapshot_schema_infrastructure_reasons(summary))

    try:
        stages = {
            "protocol": protocol_stage(rows),
            "causal_progress": causal_stage(rows),
            "rename_visibility": visibility_stage(rows),
            "watcher_cleanup": watcher_stage(rows, log_text),
            "snapshot_persistence": snapshot_stage(summary, rows, log_text),
        }
    except (TypeError, ValueError, OverflowError, KeyError, AttributeError) as exc:
        infrastructure_reasons.append(
            f"probe 证据字段类型或结构非法：{type(exc).__name__}: {exc}"
        )
        stages = {}

    gate_reasons = list(infrastructure_reasons)
    for name, stage in stages.items():
        gate_reasons.extend(f"{name}: {reason}" for reason in stage["reasons"])
    decision = (
        "infrastructure_error"
        if infrastructure_reasons
        else "fail" if gate_reasons else "pass"
    )
    return {
        "schema_version": 1,
        "decision": decision,
        "benchmark_run_dir": str(run_dir),
        "benchmark": benchmark_metadata(manifest, summary),
        "gate_reasons": gate_reasons,
        "stages": stages,
    }
