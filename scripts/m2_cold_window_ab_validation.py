"""M2 A/B wrapper 的运行中预检与终态门禁。"""

from __future__ import annotations

import json
import signal
import subprocess
import time
from pathlib import Path
from typing import Any, Iterable

from m2_cold_window_ab_command import VARIANTS


METRICS_START_TIMEOUT_SECS = 900.0
M2_PREFLIGHT_TIMEOUT_SECS = 300.0
PREFLIGHT_POLL_SECS = 2.0

REQUIRED_ARTIFACTS = (
    "summary.json",
    "event-storm-samples.jsonl",
    "process-samples.jsonl",
    "endpoint-samples.jsonl",
    "REPORT.md",
    "manifest.json",
)

M2_ACTIVITY_FIELDS = (
    "rotating_cold_window_active_dirs",
    "rotating_cold_window_cycle_id",
    "rotating_cold_window_cycle_progress_pct",
    "rotating_cold_window_promoted_to_ephemeral",
    "rotating_cold_window_fast_scan_lease_dirs",
    "rotating_cold_window_scan_only_dirs",
)


class GateError(RuntimeError):
    """表示一次 benchmark 不能作为有效 M2 A/B 样本。"""

    def __init__(self, reasons: Iterable[str]):
        self.reasons = list(reasons)
        super().__init__("；".join(self.reasons))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        for line_number, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(), 1
        ):
            if not line.strip():
                continue
            value = json.loads(line)
            if not isinstance(value, dict):
                raise ValueError(f"第 {line_number} 行不是 JSON 对象")
            rows.append(value)
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        raise GateError([f"无法读取 {path}：{exc}"]) from exc
    return rows


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise GateError([f"无法读取 {path}：{exc}"]) from exc
    if not isinstance(value, dict):
        raise GateError([f"{path} 不是 JSON 对象"])
    return value


def load_builtin_metrics(run_dir: Path) -> list[dict[str, Any]]:
    metric_paths = sorted((run_dir / "reports" / "metrics").glob("metrics_*.json"))
    if not metric_paths:
        raise GateError(["未找到 fd-rdd 内建 metrics：reports/metrics/metrics_*.json"])
    rows = [row for path in metric_paths for row in _read_jsonl(path)]
    if not rows:
        raise GateError(["fd-rdd 内建 metrics 文件为空"])
    return rows


def _watch_sample(row: dict[str, Any]) -> dict[str, Any]:
    for key in ("watch_state", "tiered_watch"):
        nested = row.get(key)
        if isinstance(nested, dict):
            return nested
    return row


def _number(sample: dict[str, Any], key: str) -> float:
    value = sample.get(key, 0)
    if isinstance(value, bool):
        return float(value)
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _metric_state(
    rows: list[dict[str, Any]], variant: str
) -> tuple[list[str], bool, bool]:
    samples = [_watch_sample(row) for row in rows]
    expected_enabled = VARIANTS[variant][2]
    enabled_values = [
        sample["rotating_cold_window_enabled"]
        for sample in samples
        if "rotating_cold_window_enabled" in sample
    ]
    hard_reasons: list[str] = []
    if not enabled_values:
        hard_reasons.append("内建 metrics 未采样 rotating_cold_window_enabled")
    elif any(
        not isinstance(value, bool) or value is not expected_enabled
        for value in enabled_values
    ):
        expected_text = "true" if expected_enabled else "false"
        hard_reasons.append(
            f"rotating_cold_window_enabled 与 {variant.upper()} 组不一致，"
            f"期望始终为 {expected_text}"
        )

    has_cold_tier = any(
        _number(sample, "l2_dirs") > 0 or _number(sample, "l3_dirs") > 0
        for sample in samples
    )
    active_fields = {
        field
        for sample in samples
        for field in M2_ACTIVITY_FIELDS
        if _number(sample, field) > 0
    }
    has_activity = bool(active_fields)
    if variant == "b" and has_activity:
        hard_reasons.append(
            "B 组出现了本应为 0 的 M2 活动：" + ", ".join(sorted(active_fields))
        )
    return hard_reasons, has_cold_tier, has_activity


def validate_metrics(rows: list[dict[str, Any]], variant: str) -> list[str]:
    reasons, has_cold_tier, has_activity = _metric_state(rows, variant)
    if not has_cold_tier:
        reasons.append("整个运行期间未形成 L2 或 L3 冷目录，样本不能验证 M2")
    if variant == "a" and not has_activity:
        reasons.append(
            "A 组未观测到任何 M2 活动（active_dirs/cycle/progress/"
            "promoted/lease/scan-only 均为 0）"
        )
    return reasons


def _initial_manifest_gate(run_dir: Path) -> list[str]:
    path = run_dir / "manifest.json"
    if not path.is_file():
        return []
    manifest = _read_json_object(path)
    state = str(manifest.get("run_state", ""))
    if state == "failed":
        return [
            "runner 在运行中预检前失败："
            + str(manifest.get("fatal_error") or manifest.get("completion_reason"))
        ]
    if state not in {"starting", "running"} or manifest.get("ab_comparable") is not False:
        return []
    raw = manifest.get("ab_comparability_reasons", []) or []
    details = raw if isinstance(raw, list) else [raw]
    return [
        "runner 初始状态不可做 A/B 比较："
        + ", ".join(str(item) for item in details)
    ]


def wait_for_preflight(
    process: subprocess.Popen[Any], run_dir: Path, variant: str
) -> None:
    """在长跑前验证 treatment、冷层候选和 M2 执行前提。"""
    metrics_deadline = time.monotonic() + METRICS_START_TIMEOUT_SECS
    activity_deadline: float | None = None
    last_pending = "尚未采到内建 metrics"
    while process.poll() is None:
        manifest_reasons = _initial_manifest_gate(run_dir)
        if manifest_reasons:
            raise GateError(["运行中预检失败", *manifest_reasons])
        try:
            rows = load_builtin_metrics(run_dir)
        except GateError as exc:
            last_pending = "；".join(exc.reasons)
        else:
            hard_reasons, has_cold_tier, has_activity = _metric_state(rows, variant)
            if hard_reasons:
                raise GateError(["运行中预检失败", *hard_reasons])
            if activity_deadline is None:
                activity_deadline = time.monotonic() + M2_PREFLIGHT_TIMEOUT_SECS
            if has_cold_tier and (variant == "b" or has_activity):
                print("运行中预检通过：treatment 正确，冷层已形成，M2 活动符合组别")
                return
            last_pending = f"冷层已形成={has_cold_tier}，M2 活动已出现={has_activity}"

        now = time.monotonic()
        deadline = activity_deadline or metrics_deadline
        if now >= deadline:
            raise GateError([f"运行中预检超时：{last_pending}"])
        time.sleep(PREFLIGHT_POLL_SECS)

    raise GateError([f"底层 benchmark 在预检完成前退出，退出码 {process.returncode}"])


def _validate_run_metadata(run_dir: Path, reasons: list[str]) -> None:
    summary_path = run_dir / "summary.json"
    if summary_path.is_file() and summary_path.stat().st_size > 0:
        try:
            summary = _read_json_object(summary_path)
            if summary.get("fd_rdd_exit_code") != 0:
                reasons.append(
                    "summary 记录 fd-rdd 退出码异常："
                    f"{summary.get('fd_rdd_exit_code')!r}"
                )
            if summary.get("ab_comparable") is not True:
                raw_detail = summary.get("ab_comparability_reasons", []) or []
                detail_items = raw_detail if isinstance(raw_detail, list) else [raw_detail]
                detail = ", ".join(str(item) for item in detail_items)
                reasons.append(
                    f"runner 判定本轮不可做 A/B 比较：{detail or '原因缺失'}"
                )
        except GateError as exc:
            reasons.extend(exc.reasons)

    manifest_path = run_dir / "manifest.json"
    if manifest_path.is_file() and manifest_path.stat().st_size > 0:
        try:
            manifest = _read_json_object(manifest_path)
            if manifest.get("run_state") != "completed":
                reasons.append(
                    f"manifest run_state 非 completed：{manifest.get('run_state')!r}"
                )
            if manifest.get("completion_reason") != "duration_elapsed":
                reasons.append(
                    "manifest completion_reason 非 duration_elapsed："
                    f"{manifest.get('completion_reason')!r}"
                )
            if manifest.get("ab_comparable") is not True:
                raw_detail = manifest.get("ab_comparability_reasons", []) or []
                detail_items = raw_detail if isinstance(raw_detail, list) else [raw_detail]
                detail = ", ".join(str(item) for item in detail_items)
                reasons.append(
                    "manifest 判定本轮不可做 A/B 比较："
                    f"{detail or '原因缺失'}"
                )
        except GateError as exc:
            reasons.extend(exc.reasons)


def _validate_run_samples(run_dir: Path, reasons: list[str]) -> None:
    for sample_name in ("process-samples.jsonl", "endpoint-samples.jsonl"):
        sample_path = run_dir / sample_name
        if sample_path.is_file() and sample_path.stat().st_size > 0:
            try:
                if not _read_jsonl(sample_path):
                    reasons.append(f"采样产物为空：{sample_name}")
            except GateError as exc:
                reasons.extend(exc.reasons)

    event_path = run_dir / "event-storm-samples.jsonl"
    if event_path.is_file() and event_path.stat().st_size > 0:
        try:
            event_rows = _read_jsonl(event_path)
            burst_rows = [
                row
                for row in event_rows
                if row.get("event_kind") == "burst_written"
                and _number(row, "events_total") > 0
            ]
            if not burst_rows:
                reasons.append("事件风暴未实际产生包含事件的 burst")
        except GateError as exc:
            reasons.extend(exc.reasons)


def validate_run(run_dir: Path, variant: str) -> None:
    reasons = [
        f"缺少或为空的产物：{name}"
        for name in REQUIRED_ARTIFACTS
        if not (run_dir / name).is_file() or (run_dir / name).stat().st_size == 0
    ]
    try:
        reasons.extend(validate_metrics(load_builtin_metrics(run_dir), variant))
    except GateError as exc:
        reasons.extend(exc.reasons)
    _validate_run_metadata(run_dir, reasons)
    _validate_run_samples(run_dir, reasons)
    if reasons:
        raise GateError(reasons)
