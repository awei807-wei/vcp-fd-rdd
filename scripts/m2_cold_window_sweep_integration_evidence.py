#!/usr/bin/env python3
"""Independent raw-evidence reconstruction for the short-sweep proof."""

from __future__ import annotations

import json
import math
import tarfile
from pathlib import Path
from typing import Any

from m2_cold_window_build_receipt import (
    load_build_receipt,
    sha256_file,
    validate_build_receipt,
)
from m2_cold_window_falsification import analyze_sweep_integration
from m2_cold_window_falsification_validation import (
    sweep_integration_protocol_invalid_reasons,
    validate_sweep_integration,
)
from m2_cold_window_sweep_integration import (
    BUILD_RECEIPT_NAME,
    CHECKSUMS_NAME,
    EVIDENCE_MANIFEST_NAME,
    IntegrationEvidenceError,
    MANIFEST_NAME,
    PRODUCT_BINARY_RELATIVE,
    PROBE_EVENTS_NAME,
    RAW_ATTEMPT_FILES,
    READY_MARKERS,
    REPO_ROOT,
    RESULT_NAME,
    SUMMARY_NAME,
    _atomic_json,
    _read_json,
    integration_spec,
    protocol_fingerprint,
)

def product_identity(source_suite: Path) -> dict[str, Any]:
    receipt_path = source_suite / BUILD_RECEIPT_NAME
    binary = source_suite / PRODUCT_BINARY_RELATIVE
    receipt = load_build_receipt(receipt_path)
    product_git_sha = str(receipt.get("source_git_sha", ""))
    planned_git_sha = str(receipt.get("planned_git_sha", ""))
    errors = validate_build_receipt(
        receipt_path,
        REPO_ROOT,
        binary,
        product_git_sha,
        planned_git_sha,
    )
    if errors:
        raise IntegrationEvidenceError(
            "source r9 build identity invalid: " + ", ".join(errors)
        )
    return {
        "source_suite": str(source_suite),
        "receipt_path": str(receipt_path),
        "binary_path": str(binary),
        "product_git_sha": product_git_sha,
        "product_binary_sha256": sha256_file(binary),
        "product_receipt_sha256": sha256_file(receipt_path),
    }


def _strict_json(path: Path, description: str) -> dict[str, Any]:
    value = _read_json(path)
    if not value:
        raise IntegrationEvidenceError(f"{description} missing or invalid: {path}")
    return value


def _strict_jsonl(path: Path, description: str) -> list[dict[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise IntegrationEvidenceError(f"{description} missing: {path}") from exc
    if not lines:
        raise IntegrationEvidenceError(f"{description} empty: {path}")
    rows: list[dict[str, Any]] = []
    for line in lines:
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise IntegrationEvidenceError(
                f"{description} contains invalid JSONL: {path}"
            ) from exc
        if not isinstance(value, dict):
            raise IntegrationEvidenceError(
                f"{description} contains non-object JSONL row: {path}"
            )
        rows.append(value)
    return rows


def _raw_watch_summary(metrics_path: Path) -> dict[str, Any]:
    """Recompute cycle completion directly from immutable metrics samples."""
    rows = _strict_jsonl(metrics_path, "metrics samples")
    cycle_rows: list[tuple[int, int]] = []
    for row in rows:
        watch_state = row.get("watch_state")
        if not isinstance(watch_state, dict):
            continue
        cycle_id = watch_state.get("rotating_cold_window_cycle_id")
        progress = watch_state.get("rotating_cold_window_cycle_progress_pct")
        if isinstance(cycle_id, bool) or isinstance(progress, bool):
            continue
        try:
            cycle_rows.append((int(cycle_id), int(progress)))
        except (TypeError, ValueError):
            continue
    cycle_ids = [cycle_id for cycle_id, _ in cycle_rows]
    completed_ids = sorted(
        {
            cycle_id
            for cycle_id, progress in cycle_rows
            if cycle_id > 0 and progress >= 100
        }
    )
    return {
        "rotating_cycle_id_min": min(cycle_ids) if cycle_ids else None,
        "rotating_cycle_id_max": max(cycle_ids) if cycle_ids else None,
        "rotating_cycle_progress_pct_max": max(
            (progress for _, progress in cycle_rows), default=None
        ),
        "rotating_completed_cycle_ids": completed_ids,
        "rotating_completed_cycles_observed": len(completed_ids),
    }


def _watch_summary_mismatch_reasons(
    report_watch: dict[str, Any], raw_watch: dict[str, Any]
) -> list[str]:
    label = "§18.1 短 sweep 整合腿"
    return [
        f"{label} fixture report watch-state 与原始 metrics 不一致: {key}"
        for key, raw_value in raw_watch.items()
        if report_watch.get(key) != raw_value
    ]


def _event_monotonic(row: dict[str, Any]) -> float | None:
    value = row.get("monotonic_secs")
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return None
    return float(value)


def _event_int(row: dict[str, Any], key: str) -> int | None:
    value = row.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        return None
    return value


def _raw_probe_summary(events_path: Path) -> tuple[dict[str, Any], list[str]]:
    """Recompute the completion fence and unique first search from raw JSONL."""
    label = "§18.1 短 sweep 整合腿"
    rows = _strict_jsonl(events_path, "sweep probe events")
    reasons: list[str] = []
    times = [_event_monotonic(row) for row in rows]
    if any(value is None for value in times) or any(
        float(current) < float(previous)
        for previous, current in zip(times, times[1:])
        if previous is not None and current is not None
    ):
        reasons.append(f"{label}原始 probe 事件单调时间无效")

    def phase_rows(phase: str) -> list[tuple[int, dict[str, Any]]]:
        return [
            (index, row)
            for index, row in enumerate(rows)
            if row.get("phase") == phase
        ]

    pre_rows = phase_rows("pre_mutation_debug")
    mutation_rows = phase_rows("mutation_written")
    post_rows = phase_rows("post_mutation_debug")
    poll_rows = phase_rows("poll_debug")
    search_rows = phase_rows("search")
    if len(pre_rows) != 1 or pre_rows[0][1].get("success") is not True:
        reasons.append(f"{label}原始 probe 缺少唯一成功 pre-mutation 观测")
    if len(mutation_rows) != 1 or mutation_rows[0][1].get("success") is not True:
        reasons.append(f"{label}原始 probe 缺少唯一 mutation 写入事件")
    if len(post_rows) != 1 or post_rows[0][1].get("success") is not True:
        reasons.append(f"{label}原始 probe 缺少唯一成功 post-mutation 观测")
    if pre_rows and mutation_rows and post_rows and not (
        pre_rows[0][0] < mutation_rows[0][0] < post_rows[0][0]
    ):
        reasons.append(f"{label}原始 probe 的 pre/write/post 顺序无效")
    if not poll_rows:
        reasons.append(f"{label}原始 probe 缺少 completion fence 轮询")

    pre = pre_rows[0][1] if len(pre_rows) == 1 else {}
    post_index, post = post_rows[0] if len(post_rows) == 1 else (-1, {})
    deadline = pre.get("repair_deadline_secs")
    if not isinstance(deadline, (int, float)) or isinstance(deadline, bool):
        deadline = None
        reasons.append(f"{label}原始 probe 缺少 repair deadline")
    post_seq = _event_int(post, "last_scan_seq")
    post_cycle = _event_int(post, "cycle_id")
    if post_seq is None or post_cycle is None:
        reasons.append(f"{label}原始 probe post-mutation 水位无效")

    fence_index = -1
    fence: dict[str, Any] = {}
    if post_seq is not None and post_cycle is not None:
        for index, row in poll_rows:
            scan_seq = _event_int(row, "last_scan_seq")
            scan_cycle = _event_int(row, "last_scan_cycle_id")
            if (
                index > post_index
                and row.get("success") is True
                and scan_seq is not None
                and scan_cycle is not None
                and scan_seq > post_seq
                and scan_cycle > post_cycle
            ):
                fence_index = index
                fence = row
                break
    fence_observed = fence_index >= 0
    if not fence_observed:
        reasons.append(f"{label}原始 probe 未观测到 post-mutation completion fence")

    searches_before = sum(
        1 for index, _ in search_rows if not fence_observed or index < fence_index
    )
    searches_after = [
        row for index, row in search_rows if fence_observed and index > fence_index
    ]
    if searches_before != 0:
        reasons.append(f"{label}原始 probe completion fence 前发生查询")
    if len(search_rows) != 1 or len(searches_after) != 1:
        reasons.append(f"{label}原始 probe completion fence 后不是唯一查询")
    search = searches_after[0] if len(searches_after) == 1 else {}
    mutation = mutation_rows[0][1] if len(mutation_rows) == 1 else {}
    mutation_path = mutation.get("path")
    if (
        not isinstance(mutation_path, str)
        or not mutation_path
        or search.get("path") != mutation_path
        or search.get("query") != Path(mutation_path).name
    ):
        reasons.append(f"{label}原始 probe 唯一查询目标与 mutation 路径不一致")

    mutation_at = _event_monotonic(mutation)
    fence_at = _event_monotonic(fence)
    waited = (
        fence_at - mutation_at
        if mutation_at is not None and fence_at is not None
        else None
    )
    if waited is None or waited < 0:
        reasons.append(f"{label}原始 probe completion fence 等待时间无效")
    elif deadline is not None and waited > float(deadline):
        reasons.append(f"{label}原始 probe 后台修复超过 {float(deadline):g} 秒加速门")

    raw = {
        "enabled": True,
        "path": mutation_path,
        "completion_fence": True,
        "completion_fence_observed": fence_observed,
        "repair_deadline_secs": float(deadline) if deadline is not None else None,
        "pre_mutation_last_scan_seq": _event_int(pre, "last_scan_seq"),
        "pre_mutation_last_scan_cycle_id": _event_int(
            pre, "last_scan_cycle_id"
        ),
        "pre_mutation_cycle_id": _event_int(pre, "cycle_id"),
        "post_mutation_last_scan_seq": post_seq,
        "post_mutation_last_scan_cycle_id": _event_int(
            post, "last_scan_cycle_id"
        ),
        "post_mutation_cycle_id": post_cycle,
        "completion_last_scan_seq": _event_int(fence, "last_scan_seq"),
        "completion_last_scan_cycle_id": _event_int(
            fence, "last_scan_cycle_id"
        ),
        "searches_before_fence": searches_before,
        "first_query_count": len(searches_after),
        "waited_secs": waited,
        "first_query_freshness": search.get("freshness"),
        "first_query_tier": search.get("index_tier"),
        "repaired_by_sweep": (
            search.get("success") is True
            and search.get("freshness") == "fresh"
            and search.get("index_tier") == "HotMemory"
        ),
    }
    return raw, reasons


def _probe_summary_mismatch_reasons(
    report: dict[str, Any], raw: dict[str, Any]
) -> list[str]:
    label = "§18.1 短 sweep 整合腿"
    keys = (
        "path",
        "completion_fence_observed",
        "repair_deadline_secs",
        "pre_mutation_last_scan_seq",
        "pre_mutation_last_scan_cycle_id",
        "pre_mutation_cycle_id",
        "post_mutation_last_scan_seq",
        "post_mutation_last_scan_cycle_id",
        "post_mutation_cycle_id",
        "completion_last_scan_seq",
        "completion_last_scan_cycle_id",
        "searches_before_fence",
        "first_query_count",
        "waited_secs",
        "first_query_freshness",
        "first_query_tier",
        "repaired_by_sweep",
    )
    def values_match(key: str) -> bool:
        report_value = report.get(key)
        raw_value = raw.get(key)
        if key in {"repair_deadline_secs", "waited_secs"}:
            if (
                isinstance(report_value, (int, float))
                and not isinstance(report_value, bool)
                and isinstance(raw_value, (int, float))
                and not isinstance(raw_value, bool)
            ):
                return math.isclose(
                    float(report_value), float(raw_value), rel_tol=0.0, abs_tol=1e-6
                )
        return report_value == raw_value

    return [
        f"{label} fixture report sweep probe 与原始 JSONL 不一致: {key}"
        for key in keys
        if not values_match(key)
    ]


def validate_raw_attempt(attempt: Path) -> None:
    _strict_json(attempt / RESULT_NAME, "terminal result")
    _strict_json(attempt / "fixture-report.json", "fixture report")
    _strict_jsonl(attempt / "process-samples.jsonl", "process samples")
    _strict_jsonl(attempt / "metrics-samples.jsonl", "metrics samples")
    _strict_jsonl(attempt / PROBE_EVENTS_NAME, "sweep probe events")
    for relative in ("FIXTURE-REPORT.md", "fd-rdd.log", "config-home/fd-rdd/config.toml"):
        path = attempt / relative
        if not path.is_file() or path.stat().st_size <= 0:
            raise IntegrationEvidenceError(f"raw fixture artifact missing: {path}")
    log_text = (attempt / "fd-rdd.log").read_text(
        encoding="utf-8", errors="replace"
    )
    if not any(marker in log_text for marker in READY_MARKERS):
        raise IntegrationEvidenceError("fixture log lacks ready marker")
    runner_log = attempt.parent / f"{attempt.name}.runner.log"
    if not runner_log.is_file() or runner_log.stat().st_size <= 0:
        raise IntegrationEvidenceError("runner log missing")


def recompute_summary(
    output_dir: Path,
    source_suite: Path,
    attempt: Path,
) -> dict[str, Any]:
    """Rebuild protocol and correctness only from raw files and product bytes."""
    validate_raw_attempt(attempt)
    identity = product_identity(source_suite)
    spec = integration_spec(output_dir)
    fingerprint = protocol_fingerprint(spec)
    integration = analyze_sweep_integration(
        attempt,
        result_name=RESULT_NAME,
        settle_secs=spec.settle_secs,
    )
    report = _strict_json(attempt / "fixture-report.json", "fixture report")
    report_watch = report.get("watch_state")
    report_watch = report_watch if isinstance(report_watch, dict) else {}
    raw_watch = _raw_watch_summary(attempt / "metrics-samples.jsonl")
    report_probe = report.get("sweep_only_modify")
    report_probe = report_probe if isinstance(report_probe, dict) else {}
    raw_probe, raw_probe_reasons = _raw_probe_summary(
        attempt / PROBE_EVENTS_NAME
    )
    protocol = integration.get("protocol")
    protocol = protocol if isinstance(protocol, dict) else {}
    raw_cycles = int(raw_watch["rotating_completed_cycles_observed"])
    protocol.update(
        {
            "cycles_observed": max(
                int(protocol.get("syscall_cycles_observed", 0) or 0), raw_cycles
            ),
            "watch_cycles_observed": raw_cycles,
            "watch_cycle_id_min": raw_watch["rotating_cycle_id_min"],
            "watch_cycle_id_max": raw_watch["rotating_cycle_id_max"],
            "watch_cycle_progress_pct_max": raw_watch[
                "rotating_cycle_progress_pct_max"
            ],
            "watch_completed_cycle_ids": raw_watch[
                "rotating_completed_cycle_ids"
            ],
        }
    )
    protocol_reasons = sweep_integration_protocol_invalid_reasons(
        integration,
        expected_product_git_sha=identity["product_git_sha"],
        expected_product_binary_sha256=identity["product_binary_sha256"],
        expected_product_receipt_sha256=identity["product_receipt_sha256"],
        expected_protocol_fingerprint=fingerprint,
    )
    protocol_reasons.extend(_watch_summary_mismatch_reasons(report_watch, raw_watch))
    protocol_reasons.extend(
        _probe_summary_mismatch_reasons(report_probe, raw_probe)
    )
    protocol_reasons.extend(raw_probe_reasons)
    if raw_cycles <= 0:
        protocol_reasons.append(
            "§18.1 短 sweep 整合腿原始 metrics 未观测到完成 cycle"
        )
    audit = integration.get("audit", {})
    audit = audit if isinstance(audit, dict) else {}
    if Path(str(audit.get("source_suite", ""))).absolute() != source_suite.absolute():
        protocol_reasons.append("§18.1 短 sweep 整合腿 source suite 不一致")
    if Path(str(audit.get("terminal_run_dir", ""))).absolute() != attempt.absolute():
        protocol_reasons.append("§18.1 短 sweep 整合腿 terminal run_dir 不一致")
    correctness_reasons: list[str] = []
    integration["sweep_only_modify"].update(raw_probe)
    validate_sweep_integration(integration, correctness_reasons)
    integration["protocol_invalid_reasons"] = protocol_reasons
    integration["correctness_reasons"] = correctness_reasons
    integration["protocol_valid"] = not protocol_reasons
    integration["correctness_passed"] = not correctness_reasons
    reasons = [*protocol_reasons, *correctness_reasons]
    return {
        "schema": 1,
        "kind": "m2-sweep-integration-only",
        "output_dir": str(output_dir),
        "source_suite": str(source_suite),
        "source_suite_immutable": True,
        "product": identity,
        "harness": {
            "git_sha": integration.get("audit", {}).get("harness_git_sha", ""),
            "worktree_dirty": integration.get("audit", {}).get(
                "harness_worktree_dirty"
            ),
        },
        "protocol_fingerprint": fingerprint,
        "integration": integration,
        "decision": "pass" if not reasons else "fail",
        "reasons": reasons,
    }


def _bundle_files(
    output_dir: Path,
    source_suite: Path,
    attempt: Path,
) -> list[tuple[Path, Path]]:
    files = [
        (output_dir / SUMMARY_NAME, Path(SUMMARY_NAME)),
        (output_dir / MANIFEST_NAME, Path(MANIFEST_NAME)),
        (
            source_suite / BUILD_RECEIPT_NAME,
            Path("product") / BUILD_RECEIPT_NAME,
        ),
        (
            source_suite / PRODUCT_BINARY_RELATIVE,
            Path("product") / "fd-rdd",
        ),
    ]
    for relative in RAW_ATTEMPT_FILES:
        files.append((attempt / relative, Path("attempt") / relative))
    files.append(
        (
            attempt.parent / f"{attempt.name}.runner.log",
            Path("attempt") / f"{attempt.name}.runner.log",
        )
    )
    return files


def write_evidence_bundle(
    output_dir: Path,
    source_suite: Path,
    attempt: Path,
) -> Path:
    """Recompute before packaging; never trust a persisted pass boolean."""
    summary = recompute_summary(output_dir, source_suite, attempt)
    _atomic_json(output_dir / SUMMARY_NAME, summary)
    files = _bundle_files(output_dir, source_suite, attempt)
    members: list[dict[str, Any]] = []
    for source, archive_path in files:
        if not source.is_file():
            raise IntegrationEvidenceError(f"evidence member missing: {source}")
        members.append(
            {
                "path": str(archive_path),
                "sha256": sha256_file(source),
                "size": source.stat().st_size,
            }
        )
    evidence_manifest = {
        "schema": 1,
        "kind": "m2-sweep-integration-only",
        "decision": summary["decision"],
        "members": members,
    }
    _atomic_json(output_dir / EVIDENCE_MANIFEST_NAME, evidence_manifest)
    checksum_lines = [
        f"{member['sha256']}  {member['path']}" for member in members
    ]
    checksum_lines.append(
        f"{sha256_file(output_dir / EVIDENCE_MANIFEST_NAME)}  {EVIDENCE_MANIFEST_NAME}"
    )
    (output_dir / CHECKSUMS_NAME).write_text(
        "\n".join(checksum_lines) + "\n", encoding="utf-8"
    )
    bundle_path = output_dir.with_name(output_dir.name + "-evidence.tar.gz")
    with tarfile.open(bundle_path, "w:gz") as archive:
        for source, archive_path in files:
            archive.add(source, arcname=str(archive_path), recursive=False)
        archive.add(
            output_dir / EVIDENCE_MANIFEST_NAME,
            arcname=EVIDENCE_MANIFEST_NAME,
            recursive=False,
        )
        archive.add(
            output_dir / CHECKSUMS_NAME,
            arcname=CHECKSUMS_NAME,
            recursive=False,
        )
    return bundle_path
