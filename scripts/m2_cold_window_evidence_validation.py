"""M2 证伪证据输入的 fail-closed 语义验证。"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from m2_cold_window_ab_result import (
    WRAPPER_RESULT_NAME,
    WRAPPER_RESULT_SCHEMA,
)
from m2_cold_window_build_receipt import SCHEMA as BUILD_RECEIPT_SCHEMA
from m2_cold_window_build_receipt import sha256_file
from m2_cold_window_falsification import LegSpec, analyze_leg


BUILD_RECEIPT_NAME = "build-provenance.json"
EVIDENCE_REQUIRED_ROOT_FILES = frozenset(
    {"summary.json", "REPORT.md", "manifest.json", BUILD_RECEIPT_NAME}
)
EVIDENCE_OPTIONAL_ROOT_FILES = frozenset({"progress.json"})
EVIDENCE_ROOT_FILES = EVIDENCE_REQUIRED_ROOT_FILES | EVIDENCE_OPTIONAL_ROOT_FILES
EVIDENCE_REQUIRED_ATTEMPT_FILES = frozenset(
    {
        "manifest.json",
        "summary.json",
        WRAPPER_RESULT_NAME,
        "event-storm-samples.jsonl",
        "fd-rdd.log",
    }
)
EVIDENCE_OPTIONAL_ATTEMPT_FILES = frozenset(
    {
        "REPORT.md",
        "events.jsonl",
        "shutdown-samples.jsonl",
        "process-samples.jsonl",
        "endpoint-samples.jsonl",
        "canary-samples.jsonl",
        "hot-churn-samples.jsonl",
    }
)
EVIDENCE_ATTEMPT_FILES = (
    EVIDENCE_REQUIRED_ATTEMPT_FILES | EVIDENCE_OPTIONAL_ATTEMPT_FILES
)
EVIDENCE_BLOCK_RE = re.compile(r"block-(0[1-4])-(ab|ba)\Z")
EVIDENCE_LEG_RE = re.compile(r"leg-([12])-([ab])\Z")
EVIDENCE_ATTEMPT_RE = re.compile(r"attempt-(0*[1-9][0-9]*)\Z")
EVIDENCE_RUNNER_LOG_RE = re.compile(r"attempt-(0*[1-9][0-9]*)\.runner\.log\Z")
READY_MARKERS = ("fd-rdd ready.", "HTTP Query Server listening")


class EvidenceBundleError(RuntimeError):
    """最终 suite 证据不完整或内部不一致。"""


@dataclass(frozen=True)
class AttemptReference:
    leg: dict[str, Any]
    path: Path
    relative: Path


@dataclass(frozen=True)
class ValidatedAttempt:
    reference: AttemptReference
    manifest: dict[str, Any]
    metric_paths: tuple[Path, ...]


def path_contains_symlink(root: Path, path: Path) -> bool:
    try:
        relative = path.relative_to(root)
    except ValueError:
        return True
    current = root
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            return True
    return False


def required_file(path: Path, description: str, *, nonempty: bool = False) -> Path:
    if path.is_symlink() or not path.is_file():
        raise EvidenceBundleError(f"缺少必需证据 {description}: {path}")
    if nonempty and path.stat().st_size <= 0:
        raise EvidenceBundleError(f"必需证据为空 {description}: {path}")
    return path


def strict_json(path: Path, description: str) -> dict[str, Any]:
    required_file(path, description, nonempty=True)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EvidenceBundleError(f"必需证据无法解析 {description}: {path}") from exc
    if not isinstance(value, dict):
        raise EvidenceBundleError(f"必需证据不是 JSON object {description}: {path}")
    return value


def strict_jsonl(path: Path, description: str) -> list[dict[str, Any]]:
    required_file(path, description, nonempty=True)
    rows: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
        for line_number, line in enumerate(lines, 1):
            if not line.strip():
                continue
            value = json.loads(line)
            if not isinstance(value, dict):
                raise EvidenceBundleError(
                    f"必需 JSONL 行不是 object {description}:{line_number}"
                )
            rows.append(value)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EvidenceBundleError(f"必需证据无法解析 {description}: {path}") from exc
    if not rows:
        raise EvidenceBundleError(f"必需 JSONL 没有记录 {description}: {path}")
    return rows


def _resolve_attempt_path(
    suite_dir: Path, raw_leg: dict[str, Any], index: int
) -> tuple[Path, Path]:
    suite_root = suite_dir.absolute()
    raw_run_dir = raw_leg.get("run_dir")
    if not isinstance(raw_run_dir, str) or not raw_run_dir:
        raise EvidenceBundleError(f"summary.legs[{index}] 缺少 run_dir")
    candidate = Path(raw_run_dir).expanduser()
    candidate = (candidate if candidate.is_absolute() else suite_root / candidate).absolute()
    try:
        relative = candidate.relative_to(suite_root)
    except ValueError as exc:
        raise EvidenceBundleError(
            f"summary.legs[{index}].run_dir 位于 suite 外: {candidate}"
        ) from exc
    if len(relative.parts) != 3 or path_contains_symlink(suite_root, candidate):
        raise EvidenceBundleError(
            f"summary.legs[{index}].run_dir 不是协议普通目录: {relative}"
        )
    try:
        resolved = candidate.resolve(strict=True)
    except OSError as exc:
        raise EvidenceBundleError(
            f"summary.legs[{index}].run_dir 不存在: {candidate}"
        ) from exc
    expected = suite_dir.resolve().joinpath(*relative.parts)
    if resolved != expected or not resolved.is_dir():
        raise EvidenceBundleError(f"run_dir 不是普通 attempt 目录: {candidate}")
    return candidate, relative


def _attempt_fields(relative: Path, index: int) -> dict[str, Any]:
    block = EVIDENCE_BLOCK_RE.fullmatch(relative.parts[0])
    leg = EVIDENCE_LEG_RE.fullmatch(relative.parts[1])
    attempt = EVIDENCE_ATTEMPT_RE.fullmatch(relative.parts[2])
    if block is None or leg is None or attempt is None:
        raise EvidenceBundleError(
            f"summary.legs[{index}].run_dir 不符合协议结构: {relative}"
        )
    fields = {
        "block": int(block.group(1)),
        "position": int(leg.group(1)),
        "order": block.group(2),
        "variant": leg.group(2),
    }
    if fields["variant"] != fields["order"][fields["position"] - 1]:
        raise EvidenceBundleError(f"run_dir 腿顺序不一致: {relative}")
    return fields


def _validate_success_shape(references: list[AttemptReference]) -> None:
    if len(references) != 8:
        raise EvidenceBundleError(
            f"成功 suite 必须精确引用 8 个 run_dir，实际为 {len(references)}"
        )
    if any(reference.leg.get("valid") is not True for reference in references):
        raise EvidenceBundleError("成功 suite 引用了无效腿")
    counts = {
        order: sum(reference.leg.get("order") == order for reference in references)
        for order in ("ab", "ba")
    }
    if counts != {"ab": 4, "ba": 4}:
        raise EvidenceBundleError(f"成功 suite 的 AB/BA 腿数无效: {counts}")
    for block in range(1, 5):
        selected = [ref for ref in references if ref.leg.get("block") == block]
        if (
            len(selected) != 2
            or {ref.leg.get("position") for ref in selected} != {1, 2}
            or len({ref.leg.get("order") for ref in selected}) != 1
        ):
            raise EvidenceBundleError(f"成功 suite 的 block {block} 引用不完整")


def referenced_attempts(
    suite_dir: Path, summary: dict[str, Any]
) -> list[AttemptReference]:
    legs = summary.get("legs", [])
    if not isinstance(legs, list):
        raise EvidenceBundleError("summary.legs 不是数组")
    references: list[AttemptReference] = []
    seen_paths: set[Path] = set()
    seen_legs: set[tuple[int, int, str]] = set()
    for index, raw_leg in enumerate(legs, 1):
        if not isinstance(raw_leg, dict):
            raise EvidenceBundleError(f"summary.legs[{index}] 不是 object")
        candidate, relative = _resolve_attempt_path(suite_dir, raw_leg, index)
        fields = _attempt_fields(relative, index)
        if any(raw_leg.get(key) != value for key, value in fields.items()):
            raise EvidenceBundleError(f"run_dir 与腿元数据不一致: {relative}")
        key = (fields["block"], fields["position"], fields["variant"])
        if candidate in seen_paths or key in seen_legs:
            raise EvidenceBundleError(f"summary 重复引用 attempt: {relative}")
        seen_paths.add(candidate)
        seen_legs.add(key)
        references.append(AttemptReference(raw_leg, candidate, relative))
    gate = summary.get("gate", {})
    if isinstance(gate, dict) and gate.get("decision") == "pass":
        _validate_success_shape(references)
    return references


def _validate_metric(path: Path) -> None:
    description = f"metrics {path.name}"
    if path.suffix == ".jsonl":
        strict_jsonl(path, description)
        return
    try:
        strict_json(path, description)
    except EvidenceBundleError:
        # fd-rdd 的按小时 metrics 文件使用 .json 后缀，但内容是逐行 JSON。
        strict_jsonl(path, description)


def _metric_paths(attempt: Path) -> tuple[Path, ...]:
    metrics_dir = attempt / "reports" / "metrics"
    if metrics_dir.is_symlink() or not metrics_dir.is_dir():
        raise EvidenceBundleError(f"缺少必需证据 metrics: {metrics_dir}")
    paths = tuple(
        path
        for path in sorted(metrics_dir.rglob("*"))
        if path.suffix in {".json", ".jsonl"} and path.is_file() and not path.is_symlink()
    )
    if not paths:
        raise EvidenceBundleError(f"缺少必需证据 metrics JSON/JSONL: {metrics_dir}")
    for path in paths:
        _validate_metric(path)
    return paths


def _first_difference(expected: Any, actual: Any, path: str) -> str:
    if type(expected) is not type(actual):
        return path
    if isinstance(expected, dict):
        if set(expected) != set(actual):
            missing = sorted(set(expected) ^ set(actual))
            return f"{path}.{missing[0]}"
        for key in sorted(expected):
            difference = _first_difference(expected[key], actual[key], f"{path}.{key}")
            if difference:
                return difference
        return ""
    if isinstance(expected, list):
        if len(expected) != len(actual):
            return f"{path}.length"
        for index, (left, right) in enumerate(zip(expected, actual)):
            difference = _first_difference(left, right, f"{path}[{index}]")
            if difference:
                return difference
        return ""
    return "" if expected == actual else path


def _validate_reanalysis(reference: AttemptReference) -> None:
    leg = reference.leg
    spec = LegSpec(
        int(leg["block"]),
        int(leg["position"]),
        str(leg["variant"]),
        str(leg["order"]),
        reference.path.parent,
    )
    actual = analyze_leg(spec, reference.path)
    for field in (
        "run_dir",
        "valid",
        "protocol",
        "correctness",
        "resources",
        "mechanism",
        "stability",
        "audit",
    ):
        difference = _first_difference(leg.get(field), actual.get(field), field)
        if difference:
            raise EvidenceBundleError(
                f"腿级重新分析不一致 {reference.relative}: {difference}"
            )


def _expected_receipt_sha256(summary: dict[str, Any]) -> str:
    legs = summary.get("legs")
    if not isinstance(legs, list) or len(legs) != 8:
        raise EvidenceBundleError("构建回执摘要必须由精确 8 腿共同证明")
    digests: list[str] = []
    for index, leg in enumerate(legs, 1):
        audit = leg.get("audit") if isinstance(leg, dict) else None
        digest = audit.get("receipt_sha256") if isinstance(audit, dict) else None
        if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            raise EvidenceBundleError(
                f"summary.legs[{index}].audit.receipt_sha256 无效"
            )
        digests.append(digest)
    unique = set(digests)
    if len(unique) != 1:
        raise EvidenceBundleError("八腿构建回执 SHA-256 不一致")
    return digests[0]


def validate_build_receipt(path: Path, summary: dict[str, Any]) -> None:
    receipt = strict_json(path, BUILD_RECEIPT_NAME)
    if (
        receipt.get("schema") not in {1, 3, BUILD_RECEIPT_SCHEMA}
        or receipt.get("build_succeeded") is not True
    ):
        raise EvidenceBundleError(f"构建回执终态无效: {path}")
    expected_digest = _expected_receipt_sha256(summary)
    try:
        actual_digest = sha256_file(path)
    except OSError as exc:
        raise EvidenceBundleError(f"构建回执无法计算 SHA-256: {path}") from exc
    if actual_digest != expected_digest:
        raise EvidenceBundleError(
            "根级 build-provenance.json SHA-256 与八腿审计记录不一致"
        )


def validate_success_suite_documents(
    suite_dir: Path, expected_summary: dict[str, Any]
) -> None:
    persisted = strict_json(suite_dir / "summary.json", "suite summary")
    if persisted != expected_summary:
        raise EvidenceBundleError("suite summary 与本轮分析结果不一致")
    manifest = strict_json(suite_dir / "manifest.json", "suite manifest")
    if not (
        manifest.get("run_state") == "completed"
        and manifest.get("decision") == "pass"
        and manifest.get("completed_legs") == 8
        and manifest.get("observed_legs") == 8
    ):
        raise EvidenceBundleError("suite manifest 终态无效")
    required_file(suite_dir / "REPORT.md", "suite report", nonempty=True)
    validate_build_receipt(suite_dir / BUILD_RECEIPT_NAME, expected_summary)


def validate_success_attempt(reference: AttemptReference) -> ValidatedAttempt:
    attempt = reference.path
    manifest = strict_json(attempt / "manifest.json", "leg manifest")
    summary = strict_json(attempt / "summary.json", "leg summary")
    wrapper = strict_json(attempt / WRAPPER_RESULT_NAME, "wrapper result")
    if not (
        manifest.get("run_state") == "completed"
        and manifest.get("completion_reason") == "duration_elapsed"
        and manifest.get("ab_comparable") is True
    ):
        raise EvidenceBundleError(f"leg manifest 终态无效: {reference.relative}")
    manifest_run_dir = manifest.get("run_dir")
    if manifest_run_dir and Path(str(manifest_run_dir)).absolute() != attempt.absolute():
        raise EvidenceBundleError(f"leg manifest run_dir 不匹配: {reference.relative}")
    if summary.get("fd_rdd_exit_code") != 0 or summary.get("ab_comparable") is not True:
        raise EvidenceBundleError(f"leg summary 终态无效: {reference.relative}")
    if not (
        wrapper.get("schema") == WRAPPER_RESULT_SCHEMA
        and wrapper.get("status") == "passed"
        and wrapper.get("wrapper_exit_code") == 0
        and wrapper.get("variant") == reference.leg.get("variant")
        and wrapper.get("profile") == "falsification"
    ):
        raise EvidenceBundleError(f"wrapper 终态或腿身份无效: {reference.relative}")
    wrapper_run_dir = wrapper.get("run_dir")
    # Wrapper schema 1 通过文件位置绑定 run_dir；新版本可额外自描述 run_dir。
    if wrapper_run_dir and Path(str(wrapper_run_dir)).absolute() != attempt.absolute():
        raise EvidenceBundleError(f"wrapper run_dir 不匹配: {reference.relative}")

    events = strict_jsonl(
        attempt / "event-storm-samples.jsonl", "event-storm JSONL"
    )
    written = sum(row.get("event_kind") == "burst_written" for row in events)
    checked = sum(row.get("event_kind") == "burst_checked" for row in events)
    if written <= 0:
        raise EvidenceBundleError(f"event-storm 缺少 burst_written: {reference.relative}")
    if checked <= 0 or checked < written:
        raise EvidenceBundleError(f"event-storm 缺少 burst_checked: {reference.relative}")

    log_path = required_file(attempt / "fd-rdd.log", "fd-rdd.log", nonempty=True)
    log_text = log_path.read_text(encoding="utf-8", errors="replace")
    if not any(marker in log_text for marker in READY_MARKERS):
        raise EvidenceBundleError(f"fd-rdd.log 缺少 ready 标记: {reference.relative}")
    required_file(
        attempt / "config-home" / "fd-rdd" / "config.toml",
        "config.toml",
        nonempty=True,
    )
    runner_log = attempt.parent / f"{attempt.name}.runner.log"
    required_file(runner_log, "runner.log", nonempty=True)
    metric_paths = _metric_paths(attempt)
    _validate_reanalysis(reference)
    return ValidatedAttempt(reference, manifest, metric_paths)
