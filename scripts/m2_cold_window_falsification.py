"""M2 快速证伪 suite 的腿级采集与分析。"""

from __future__ import annotations

import hashlib
import json
import signal
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any


PROTOCOL_TREATMENT_KEYS = frozenset({"rotating_cold_window"})


@dataclass(frozen=True)
class LegSpec:
    block: int
    position: int
    variant: str
    order: str
    base_dir: Path


def balanced_block_orders(seed: int) -> list[tuple[str, str]]:
    orders = [("a", "b"), ("a", "b"), ("b", "a"), ("b", "a")]
    random.Random(seed).shuffle(orders)
    return orders


def build_leg_specs(suite_dir: Path, seed: int) -> list[LegSpec]:
    specs: list[LegSpec] = []
    for block, variants in enumerate(balanced_block_orders(seed), 1):
        order = "".join(variants)
        for position, variant in enumerate(variants, 1):
            base = suite_dir / f"block-{block:02d}-{order}" / f"leg-{position}-{variant}"
            specs.append(LegSpec(block, position, variant, order, base))
    return specs


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    try:
        values = [
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line
        ]
    except (OSError, json.JSONDecodeError):
        return []
    return [value for value in values if isinstance(value, dict)]


def _mapping(container: dict[str, Any], key: str) -> dict[str, Any]:
    value = container.get(key)
    return value if isinstance(value, dict) else {}


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * p / 100)))
    return round(ordered[index], 3)


def _first_query_correct(row: dict[str, Any]) -> bool:
    if row.get("transport_ok") is False or bool(row.get("error")):
        return False
    if "correct" in row:
        return bool(row.get("correct"))
    return bool(row.get("first_query_exists")) == bool(row.get("should_exist"))


def _count_values(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    result: dict[str, int] = {}
    for row in rows:
        value = str(row.get(key, ""))
        if value:
            result[value] = result.get(value, 0) + 1
    return result


def _group_correctness(
    rows: list[dict[str, Any]],
    key: str,
    correct: Any,
) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        value = str(row.get(key, ""))
        if value:
            grouped.setdefault(value, []).append(row)
    result: dict[str, dict[str, Any]] = {}
    for value, selected in grouped.items():
        ok = sum(1 for row in selected if correct(row))
        workloads = sorted(
            {str(row.get("workload", "")) for row in selected if row.get("workload")}
        )
        result[value] = {
            "total": len(selected),
            "ok": ok,
            "success_rate": round(ok / len(selected), 4),
            "workloads": workloads,
        }
    return result


def _protocol_fingerprint(manifest: dict[str, Any]) -> str:
    inputs = _mapping(manifest, "ab_parameter_fingerprint_inputs")
    runner_args = _mapping(inputs, "runner_args")
    if not runner_args:
        return ""
    payload = {
        key: value
        for key, value in sorted(runner_args.items())
        if key not in PROTOCOL_TREATMENT_KEYS
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _analyze_correctness(rows: list[dict[str, Any]]) -> dict[str, Any]:
    primary = [
        row
        for row in rows
        if row.get("event_kind") == "first_query"
        and row.get("query_phase") != "immediate"
    ]
    positive = [row for row in primary if bool(row.get("should_exist"))]
    negative = [row for row in primary if not bool(row.get("should_exist"))]
    positive_ok = sum(1 for row in positive if _first_query_correct(row))
    negative_ok = sum(1 for row in negative if _first_query_correct(row))
    transport_failures = sum(
        1
        for row in primary
        if row.get("transport_ok") is False or bool(row.get("error"))
    )
    primary_paths = [str(row.get("path", "")) for row in primary if row.get("path")]
    visibility_rows = [row for row in rows if row.get("event_kind") == "visibility_probe"]
    visibility_by_path = {
        str(row.get("path", "")): row for row in visibility_rows if row.get("path")
    }
    visible = [row for row in visibility_by_path.values() if bool(row.get("visible"))]
    visibility_transport_failures = sum(
        int(row.get("transport_failures", 0) or 0)
        for row in visibility_by_path.values()
    )
    unique_primary = len(set(primary_paths))
    positive_by_burst = _group_correctness(positive, "cycle", _first_query_correct)
    positive_by_workload = _group_correctness(
        positive, "workload", _first_query_correct
    )
    visibility_by_workload = _group_correctness(
        list(visibility_by_path.values()),
        "workload",
        lambda row: bool(row.get("visible")),
    )
    for workload, grouped in visibility_by_workload.items():
        selected = [
            row
            for row in visibility_by_path.values()
            if str(row.get("workload", "")) == workload and bool(row.get("visible"))
        ]
        grouped["p95_secs"] = _percentile(
            [float(row.get("latency_secs", 0.0) or 0.0) for row in selected],
            95,
        )
    return {
        "primary_assertions": len(primary),
        "primary_unique_paths": unique_primary,
        "duplicate_primary_paths": len(primary_paths) - unique_primary,
        "positive_total": len(positive),
        "positive_ok": positive_ok,
        "positive_success_rate": round(positive_ok / len(positive), 4) if positive else 0.0,
        "positive_by_burst": positive_by_burst,
        "positive_by_workload": positive_by_workload,
        "negative_total": len(negative),
        "negative_ok": negative_ok,
        "negative_success_rate": round(negative_ok / len(negative), 4) if negative else 0.0,
        "transport_failures": transport_failures,
        "visibility_total": len(visibility_by_path),
        "visibility_visible": len(visible),
        "visibility_transport_failures": visibility_transport_failures,
        "visibility_success_rate": (
            round(len(visible) / len(visibility_by_path), 4)
            if visibility_by_path
            else 0.0
        ),
        "visibility_p95_secs": _percentile(
            [float(row.get("latency_secs", 0.0) or 0.0) for row in visible],
            95,
        ),
        "visibility_by_workload": visibility_by_workload,
    }


def _target_m2_causal(burst: dict[str, Any], check: dict[str, Any]) -> bool:
    if not burst.get("target_m2_active"):
        return False
    action = str(burst.get("target_m2_action", ""))
    if action not in {"ephemeral_watch", "fast_scan_lease", "scan_only"}:
        return False
    observed = int(burst.get("target_m2_observed_unix_secs", 0) or 0)
    expires = int(burst.get("target_m2_expires_unix_secs", 0) or 0)
    mutation_completed = int(burst.get("mutation_completed_unix_secs", 0) or 0)
    if (
        observed <= 0
        or mutation_completed <= 0
        or expires < max(observed, mutation_completed)
    ):
        return False
    before_scan = int(burst.get("target_m2_last_scan_unix_secs", 0) or 0)
    before_event = int(burst.get("target_m2_last_event_unix_secs", 0) or 0)
    after_scan = int(check.get("target_m2_after_last_scan_unix_secs", 0) or 0)
    after_event = int(check.get("target_m2_after_last_event_unix_secs", 0) or 0)
    scan_advanced = after_scan > before_scan and after_scan >= mutation_completed
    event_advanced = after_event > before_event and after_event >= mutation_completed
    return scan_advanced or (action == "ephemeral_watch" and event_advanced)


def _protocol_summary(
    manifest: dict[str, Any],
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    bursts = [row for row in events if row.get("event_kind") == "burst_written"]
    runner_args = _mapping(manifest, "runner_args")
    configured_roots = runner_args.get("event_storm_root", [])
    if not isinstance(configured_roots, list):
        configured_roots = [configured_roots] if configured_roots else []
    checks = [row for row in events if row.get("event_kind") == "burst_checked"]
    delayed_checks = {
        int(row.get("cycle", 0) or 0): row
        for row in checks
        if row.get("query_phase") == "delayed"
    }
    causal_bursts = [
        row
        for row in bursts
        if _target_m2_causal(
            row,
            delayed_checks.get(int(row.get("cycle", 0) or 0), {}),
        )
    ]
    plan_rows = [
        (
            int(row.get("cycle", 0) or 0),
            str(row.get("workload", "")),
            str(row.get("operation", "")),
            str(row.get("path", "")),
            str(row.get("query", "")),
            bool(row.get("should_exist")),
            int(row.get("mutation_seq", 0) or 0),
        )
        for row in events
        if row.get("event_kind") == "first_query"
        and row.get("query_phase") == "delayed"
    ]
    plan_sha256 = hashlib.sha256(
        json.dumps(plan_rows, ensure_ascii=False, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()
    mutation_sequence = [row[-1] for row in plan_rows]
    return {
        "physical_bursts": len(bursts),
        "checks": len(checks),
        "requested_tiers": sorted(_count_values(bursts, "requested_tier")),
        "tier_before": _count_values(bursts, "tier_before"),
        "configured_event_roots": sorted(str(root) for root in configured_roots),
        "event_roots": sorted(
            {str(row.get("root", "")) for row in bursts if row.get("root")}
        ),
        "workloads": _count_values(bursts, "selected_kind"),
        "events_total": sum(int(row.get("events_total", 0) or 0) for row in bursts),
        "within_budget_bursts": sum(
            1 for row in bursts if row.get("within_budget") is True
        ),
        "target_m2_debug_ok_bursts": sum(
            1 for row in bursts if row.get("target_m2_debug_ok") is True
        ),
        "target_m2_seen_bursts": sum(
            1 for row in bursts if row.get("target_m2_seen") is True
        ),
        "target_m2_active_bursts": sum(
            1 for row in bursts if row.get("target_m2_active") is True
        ),
        "target_m2_unexpired_bursts": sum(
            1
            for row in bursts
            if row.get("target_m2_active") is True
            and int(row.get("target_m2_expires_unix_secs", 0) or 0)
            >= max(
                int(row.get("target_m2_observed_unix_secs", 0) or 0),
                int(row.get("mutation_completed_unix_secs", 0) or 0),
            )
            > 0
        ),
        "target_m2_causal_bursts": len(causal_bursts),
        "target_m2_causal_actions": _count_values(
            causal_bursts, "target_m2_action"
        ),
        "target_m2_actions": _count_values(bursts, "target_m2_action"),
        "mutation_sequence_count": len(mutation_sequence),
        "mutation_sequence_contiguous": mutation_sequence
        == list(range(1, len(mutation_sequence) + 1)),
        "visibility_poll_count": sum(
            int(row.get("visibility_poll_count", 0) or 0) for row in checks
        ),
        "fixed_root_schedule": bool(
            runner_args.get("event_storm_fixed_root_schedule")
        ),
        "deterministic_event_plan": bool(
            runner_args.get("event_storm_deterministic_plan")
        ),
        "event_plan_sha256": plan_sha256,
    }


def _resource_summary(summary: dict[str, Any]) -> dict[str, Any]:
    process = _mapping(summary, "process")
    event_process = _mapping(summary, "process_after_event_storm_start")
    fields = {
        "read_bytes_delta": "read_bytes_delta",
        "write_bytes_delta": "write_bytes_delta",
        "read_syscalls_delta": "read_syscalls_delta",
        "write_syscalls_delta": "write_syscalls_delta",
        "minor_faults_delta": "minor_faults_delta",
        "major_faults_delta": "major_faults_delta",
        "rss_bytes_p95": "rss_bytes_p95",
    }
    result = {
        output: int(process.get(source, 0) or 0)
        for output, source in fields.items()
    }
    result["sample_count"] = int(process.get("sample_count", 0) or 0)
    result["window_source"] = "full_run" if result["sample_count"] > 0 else "missing"
    result["sample_first_elapsed_secs"] = float(
        process.get("sample_first_elapsed_secs", 0.0) or 0.0
    )
    result["sample_last_elapsed_secs"] = float(
        process.get("sample_last_elapsed_secs", 0.0) or 0.0
    )
    result["sample_coverage_secs"] = float(
        process.get("sample_coverage_secs", 0.0) or 0.0
    )
    result["sample_coverage_ratio"] = float(
        process.get("sample_coverage_ratio", 0.0) or 0.0
    )
    result["sample_max_gap_secs"] = float(
        process.get("sample_max_gap_secs", 0.0) or 0.0
    )
    result["counter_regressions"] = int(
        process.get("counter_regressions", 0) or 0
    )
    result["cpu_core_seconds"] = float(process.get("cpu_core_seconds", 0.0) or 0.0)
    result["event_window_sample_count"] = int(
        event_process.get("sample_count", 0) or 0
    )
    result["event_window_cpu_core_seconds"] = float(
        event_process.get("cpu_core_seconds", 0.0) or 0.0
    )
    for key in (
        "read_bytes_delta",
        "write_bytes_delta",
        "read_syscalls_delta",
        "write_syscalls_delta",
        "minor_faults_delta",
        "major_faults_delta",
        "rss_bytes_p95",
    ):
        result[f"event_window_{key}"] = int(event_process.get(key, 0) or 0)
    return result


def _mechanism_summary(watch: dict[str, Any]) -> dict[str, int]:
    fields = {
        "rotating_active_dirs_max": "rotating_cold_window_active_dirs_max",
        "rotating_cycle_progress_pct_max": "rotating_cold_window_cycle_progress_pct_max",
        "rotating_promoted_last": "rotating_cold_window_promoted_to_ephemeral_last",
        "rotating_scan_only_last": "rotating_cold_window_scan_only_dirs_last",
        "rotating_budget_blocked_last": "rotating_cold_window_budget_blocked_last",
    }
    return {output: int(watch.get(source, 0) or 0) for output, source in fields.items()}


def _stability_summary(
    watch: dict[str, Any],
    snapshot: dict[str, Any],
    log_text: str,
) -> dict[str, Any]:
    return {
        "snapshot_rebuild_observed": bool(snapshot.get("rebuild_observed")),
        "snapshot_ready": bool(snapshot.get("ready")),
        "snapshot_last_daemon_error": str(snapshot.get("last_daemon_error", "")),
        "waterline_soft_degraded_ratio": float(
            watch.get("waterline_soft_degraded_ratio", 0.0) or 0.0
        ),
        "waterline_soft_degraded_last": bool(
            watch.get("waterline_soft_degraded_last")
        ),
        "waterline_hard_degraded_samples": int(
            watch.get("waterline_hard_degraded_samples", 0) or 0
        ),
        "waterline_hard_degraded_last": bool(
            watch.get("waterline_hard_degraded_last")
        ),
        "dirty_queue_len_last": int(watch.get("dirty_queue_len_last", 0) or 0),
        "log_error_count": log_text.count("ERROR"),
        "direct_v7_unsupported_count": log_text.count("direct_v7_unsupported"),
        "background_rebuild_count": log_text.count("Starting background rebuild"),
        "waterline_trigger_count": log_text.count(
            "waterline alarm: soft degradation triggered"
        ),
        "waterline_recover_count": log_text.count(
            "waterline alarm: soft degradation recovered"
        ),
    }


def _run_valid(manifest: dict[str, Any], summary: dict[str, Any]) -> bool:
    return bool(
        manifest.get("run_state") == "completed"
        and manifest.get("completion_reason") == "duration_elapsed"
        and manifest.get("ab_comparable") is True
        and summary.get("fd_rdd_exit_code") == 0
        and summary.get("ab_comparable") is True
    )


def analyze_leg(spec: LegSpec, run_dir: Path) -> dict[str, Any]:
    manifest = _read_json(run_dir / "manifest.json")
    summary = _read_json(run_dir / "summary.json")
    events = _read_jsonl(run_dir / "event-storm-samples.jsonl")
    log_path = run_dir / "fd-rdd.log"
    log_text = (
        log_path.read_text(encoding="utf-8", errors="replace")
        if log_path.exists()
        else ""
    )
    fixture = _mapping(manifest, "fixture")
    provenance = _mapping(manifest, "artifact_provenance")
    watch = _mapping(summary, "watch_state")
    protocol = _protocol_summary(manifest, events)
    return {
        "block": spec.block,
        "position": spec.position,
        "order": spec.order,
        "variant": spec.variant,
        "run_dir": str(run_dir),
        "valid": _run_valid(manifest, summary),
        "correctness": _analyze_correctness(events),
        "protocol": protocol,
        "resources": _resource_summary(summary),
        "mechanism": _mechanism_summary(watch),
        "stability": _stability_summary(
            watch,
            _mapping(summary, "shutdown_snapshot_quiesce"),
            log_text,
        ),
        "audit": {
            "git_sha": str(manifest.get("git_sha", "")),
            "binary_sha256": str(manifest.get("binary_sha256", "")),
            "receipt_sha256": str(provenance.get("receipt_sha256", "")),
            "cargo_lock_sha256": str(
                provenance.get("cargo_lock_sha256", "")
            ),
            "validated_binary_sha256": str(
                provenance.get("validated_binary_sha256", "")
            ),
            "initial_state_fingerprint": str(
                manifest.get("initial_state_fingerprint", "")
            ),
            "fixture_identity_sha256": str(fixture.get("identity_sha256", "")),
            "protocol_fingerprint": _protocol_fingerprint(manifest),
            "event_plan_sha256": str(protocol.get("event_plan_sha256", "")),
            "daemon_started_at": str(manifest.get("daemon_started_at", "")),
            "finished_at": str(manifest.get("finished_at", "")),
            "actual_duration_secs": float(
                manifest.get("actual_duration_secs", 0.0) or 0.0
            ),
        },
    }
