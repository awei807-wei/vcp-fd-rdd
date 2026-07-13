"""从已验证的 M2 suite 收集有限且可审计的证据成员。"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from m2_cold_window_build_receipt import sha256_file
from m2_cold_window_evidence_validation import (
    BUILD_RECEIPT_NAME,
    EVIDENCE_ATTEMPT_FILES,
    EVIDENCE_REQUIRED_ATTEMPT_FILES,
    EVIDENCE_ROOT_FILES,
    EVIDENCE_RUNNER_LOG_RE,
    EvidenceBundleError,
    ValidatedAttempt,
    path_contains_symlink,
    referenced_attempts,
    required_file,
    strict_json,
    validate_success_attempt,
    validate_success_suite_documents,
)


def _runtime_config_bytes(manifest: dict[str, Any], relative: Path) -> bytes:
    runner_args = manifest.get("runner_args")
    if not isinstance(runner_args, dict):
        raise EvidenceBundleError(f"manifest 缺少 runner_args: {relative}")
    payload = {
        "schema": 1,
        "source": "manifest.json",
        "run_dir": str(relative),
        "runner_args": runner_args,
        "command": manifest.get("command", []),
        "config": manifest.get("config", ""),
        "ab_parameter_fingerprint": manifest.get("ab_parameter_fingerprint", ""),
        "execution_fingerprint": manifest.get("execution_fingerprint", ""),
    }
    rendered = json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False)
    return (rendered + "\n").encode("utf-8")


def _source_record(
    suite_dir: Path,
    relative: Path,
    *,
    required: bool,
    category: str,
) -> dict[str, Any] | None:
    path = suite_dir / relative
    if not path.exists():
        if required:
            required_file(path, str(relative))
        return None
    if path_contains_symlink(suite_dir.absolute(), path.absolute()):
        if required:
            raise EvidenceBundleError(f"必需证据含符号链接: {relative}")
        return None
    if not path.is_file():
        if required:
            raise EvidenceBundleError(f"必需证据不是普通文件: {relative}")
        return None
    try:
        size = path.stat().st_size
        digest = sha256_file(path)
    except OSError as exc:
        raise EvidenceBundleError(f"证据文件无法读取: {relative}") from exc
    return {
        "path": path,
        "relative": relative,
        "bytes": None,
        "sha256": digest,
        "size": size,
        "required": required,
        "category": category,
    }


def _generated_record(
    relative: Path, content: bytes, *, required: bool, category: str
) -> dict[str, Any]:
    return {
        "path": None,
        "relative": relative,
        "bytes": content,
        "sha256": hashlib.sha256(content).hexdigest(),
        "size": len(content),
        "required": required,
        "category": category,
    }


def _insert_record(
    records: dict[Path, dict[str, Any]], record: dict[str, Any] | None
) -> None:
    if record is None:
        return
    relative = record["relative"]
    if relative in records:
        raise EvidenceBundleError(f"证据成员重复: {relative}")
    records[relative] = record


def _attempt_metrics(attempt: Path) -> tuple[Path, ...]:
    metrics_dir = attempt / "reports" / "metrics"
    if not metrics_dir.is_dir() or metrics_dir.is_symlink():
        return ()
    return tuple(
        path
        for path in sorted(metrics_dir.rglob("*"))
        if path.suffix in {".json", ".jsonl"}
        and path.is_file()
        and not path.is_symlink()
    )


def _collect_declared_files(
    suite_dir: Path,
    relative: Path,
    successful: bool,
    records: dict[Path, dict[str, Any]],
) -> None:
    for name in sorted(EVIDENCE_ATTEMPT_FILES):
        _insert_record(
            records,
            _source_record(
                suite_dir,
                relative / name,
                required=successful and name in EVIDENCE_REQUIRED_ATTEMPT_FILES,
                category="leg",
            ),
        )
    config = relative / "config-home" / "fd-rdd" / "config.toml"
    _insert_record(
        records,
        _source_record(suite_dir, config, required=successful, category="config"),
    )


def _collect_metrics(
    suite_dir: Path,
    attempt: Path,
    successful: bool,
    validated: ValidatedAttempt | None,
    records: dict[Path, dict[str, Any]],
) -> None:
    metric_paths = validated.metric_paths if validated else _attempt_metrics(attempt)
    for metric in metric_paths:
        _insert_record(
            records,
            _source_record(
                suite_dir,
                metric.relative_to(suite_dir),
                required=successful,
                category="metrics",
            ),
        )


def _collect_generated_config(
    attempt: Path,
    relative: Path,
    successful: bool,
    validated: ValidatedAttempt | None,
    records: dict[Path, dict[str, Any]],
) -> None:
    manifest = validated.manifest if validated else None
    if manifest is None and (attempt / "manifest.json").is_file():
        try:
            manifest = strict_json(attempt / "manifest.json", "leg manifest")
        except EvidenceBundleError:
            manifest = None
    if manifest is None:
        return
    _insert_record(
        records,
        _generated_record(
            relative / "runtime-config.json",
            _runtime_config_bytes(manifest, relative),
            required=successful,
            category="runtime-config",
        ),
    )


def _collect_attempt_records(
    suite_dir: Path,
    attempt: Path,
    relative: Path,
    successful: bool,
    validated: ValidatedAttempt | None,
    records: dict[Path, dict[str, Any]],
) -> None:
    _collect_declared_files(suite_dir, relative, successful, records)
    _collect_metrics(suite_dir, attempt, successful, validated, records)
    _collect_generated_config(attempt, relative, successful, validated, records)
    runner_log = relative.parent / f"{relative.name}.runner.log"
    if EVIDENCE_RUNNER_LOG_RE.fullmatch(runner_log.name):
        _insert_record(
            records,
            _source_record(
                suite_dir,
                runner_log,
                required=successful,
                category="runner-log",
            ),
        )


def collect_evidence_records(
    suite_dir: Path, summary: dict[str, Any]
) -> tuple[list[dict[str, Any]], list[str]]:
    gate = summary.get("gate", {})
    successful = isinstance(gate, dict) and gate.get("decision") == "pass"
    references = referenced_attempts(suite_dir, summary)
    validated: dict[Path, ValidatedAttempt] = {}
    if successful:
        validate_success_suite_documents(suite_dir, summary)
        validated = {
            reference.relative: validate_success_attempt(reference)
            for reference in references
        }
    records: dict[Path, dict[str, Any]] = {}
    for name in sorted(EVIDENCE_ROOT_FILES):
        required = name in {"summary.json", "REPORT.md", "manifest.json"} or (
            successful and name == BUILD_RECEIPT_NAME
        )
        _insert_record(
            records,
            _source_record(
                suite_dir, Path(name), required=required, category="suite"
            ),
        )
    for reference in references:
        _collect_attempt_records(
            suite_dir,
            reference.path,
            reference.relative,
            successful,
            validated.get(reference.relative),
            records,
        )
    return [records[path] for path in sorted(records)], [
        str(reference.relative) for reference in references
    ]
