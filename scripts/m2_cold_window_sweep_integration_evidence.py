#!/usr/bin/env python3
"""Independent raw-evidence reconstruction for the short-sweep proof."""

from __future__ import annotations

import json
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


def _strict_jsonl(path: Path, description: str) -> None:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise IntegrationEvidenceError(f"{description} missing: {path}") from exc
    if not lines:
        raise IntegrationEvidenceError(f"{description} empty: {path}")
    for line in lines:
        try:
            json.loads(line)
        except json.JSONDecodeError as exc:
            raise IntegrationEvidenceError(
                f"{description} contains invalid JSONL: {path}"
            ) from exc


def validate_raw_attempt(attempt: Path) -> None:
    _strict_json(attempt / RESULT_NAME, "terminal result")
    _strict_json(attempt / "fixture-report.json", "fixture report")
    _strict_jsonl(attempt / "process-samples.jsonl", "process samples")
    _strict_jsonl(attempt / "metrics-samples.jsonl", "metrics samples")
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
    protocol_reasons = sweep_integration_protocol_invalid_reasons(
        integration,
        expected_product_git_sha=identity["product_git_sha"],
        expected_product_binary_sha256=identity["product_binary_sha256"],
        expected_product_receipt_sha256=identity["product_receipt_sha256"],
        expected_protocol_fingerprint=fingerprint,
    )
    audit = integration.get("audit", {})
    audit = audit if isinstance(audit, dict) else {}
    if Path(str(audit.get("source_suite", ""))).absolute() != source_suite.absolute():
        protocol_reasons.append("§18.1 短 sweep 整合腿 source suite 不一致")
    if Path(str(audit.get("terminal_run_dir", ""))).absolute() != attempt.absolute():
        protocol_reasons.append("§18.1 短 sweep 整合腿 terminal run_dir 不一致")
    correctness_reasons: list[str] = []
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
