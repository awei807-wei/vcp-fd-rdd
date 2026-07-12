"""M2 一键 A/B 驱动的有界失败现场汇总。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, TextIO


DIAGNOSTIC_LOG_LINES = 20
DIAGNOSTIC_EVENT_LINES = 5
DIAGNOSTIC_SCAN_LINES = 400
DIAGNOSTIC_LINE_CHARS = 2000
TAIL_READ_CHUNK_BYTES = 64 * 1024

LOG_ERROR_MARKERS = (
    "error",
    "failed",
    "failure",
    "fatal",
    "panic",
    "traceback",
    "exception",
    "not found",
    "exited",
    "snapshot_upsert_unresolved",
    "no space left",
    "permission denied",
    "timed out",
)


def runner_log_path(run_dir: Path) -> Path:
    """The wrapper log is a sibling so creating it cannot taint run_dir provenance."""

    return run_dir.parent / f"{run_dir.name}.runner.log"


def _bounded_text(value: Any) -> str:
    if isinstance(value, (dict, list)):
        text = json.dumps(value, ensure_ascii=False, sort_keys=True)
    else:
        text = str(value)
    if len(text) <= DIAGNOSTIC_LINE_CHARS:
        return text
    return text[: DIAGNOSTIC_LINE_CHARS - 1] + "…"


def _read_json_object(path: Path) -> tuple[dict[str, Any] | None, str]:
    if not path.is_file():
        return None, ""
    try:
        value = json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except (OSError, json.JSONDecodeError) as exc:
        return None, f"{path.name} 无法解析：{exc}"
    if not isinstance(value, dict):
        return None, f"{path.name} 顶层不是 JSON 对象"
    return value, ""


def _present_fields(payload: dict[str, Any], keys: tuple[str, ...]) -> str:
    fields = [
        f"{key}={_bounded_text(payload[key])}"
        for key in keys
        if key in payload and payload[key] not in (None, "", [], {})
    ]
    return ", ".join(fields)


def _manifest_diagnostics(run_dir: Path) -> list[str]:
    manifest, error = _read_json_object(run_dir / "manifest.json")
    if error:
        return [error]
    if manifest is None:
        return []

    diagnostics: list[str] = []
    state = _present_fields(
        manifest,
        (
            "run_state",
            "failure_stage",
            "completion_reason",
            "fd_rdd_exit_code",
            "fatal_error",
            "process_sampler_error",
            "cleanup_errors",
            "ab_comparability_reasons",
            "snapshot_path",
        ),
    )
    if state:
        diagnostics.append(f"manifest：{state}")

    execution = manifest.get("execution")
    if isinstance(execution, dict):
        execution_state = _present_fields(
            execution,
            (
                "completion_reason",
                "exit_code",
                "fatal_error",
                "process_sampler_error",
                "cleanup_errors",
                "final_snapshot_failed",
                "event_storm_write_failures",
                "event_storm_cleanup_failures",
                "unsupported_workloads",
                "endpoint_sample_failures",
            ),
        )
        if execution_state:
            diagnostics.append(f"manifest.execution：{execution_state}")
    return diagnostics


def _summary_diagnostics(run_dir: Path) -> list[str]:
    summary, error = _read_json_object(run_dir / "summary.json")
    if error:
        return [error]
    if summary is None:
        return []

    state = _present_fields(
        summary,
        (
            "fd_rdd_exit_code",
            "fatal_error",
            "ab_comparable",
            "ab_comparability_reasons",
            "built_in_metrics_dir",
        ),
    )
    return [f"summary：{state}"] if state else []


def _tail_lines(path: Path, limit: int) -> list[str]:
    if limit <= 0 or not path.is_file():
        return []
    try:
        with path.open("rb") as handle:
            handle.seek(0, 2)
            position = handle.tell()
            chunks: list[bytes] = []
            newline_count = 0
            while position > 0 and newline_count <= limit:
                chunk_size = min(TAIL_READ_CHUNK_BYTES, position)
                position -= chunk_size
                handle.seek(position)
                chunk = handle.read(chunk_size)
                chunks.append(chunk)
                newline_count += chunk.count(b"\n")
    except OSError as exc:
        return [f"无法读取 {path.name}：{exc}"]
    data = b"".join(reversed(chunks)).decode("utf-8", errors="replace")
    return data.splitlines()[-limit:]


def _event_diagnostics(run_dir: Path) -> list[str]:
    path = run_dir / "events.jsonl"
    rows: list[dict[str, Any]] = []
    parse_errors: list[str] = []
    for line in _tail_lines(path, DIAGNOSTIC_SCAN_LINES):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            parse_errors.append(f"events.jsonl 尾部无法解析：{exc}")
            continue
        if isinstance(value, dict):
            rows.append(value)

    interesting = [
        row
        for row in rows
        if row.get("error")
        or any(
            marker in str(row.get("event", "")).lower()
            for marker in ("fatal", "error", "failed", "shutdown", "interrupt")
        )
    ]
    selected = (interesting or rows)[-DIAGNOSTIC_EVENT_LINES:]
    diagnostics = [
        "events.jsonl 最近关键事件：\n"
        + "\n".join(f"  {_bounded_text(row)}" for row in selected)
    ] if selected else []
    diagnostics.extend(parse_errors[-1:])
    return diagnostics


def _log_diagnostics(run_dir: Path) -> list[str]:
    path = run_dir / "fd-rdd.log"
    lines = _tail_lines(path, DIAGNOSTIC_SCAN_LINES)
    if not lines:
        return []
    interesting = [
        line for line in lines if any(marker in line.lower() for marker in LOG_ERROR_MARKERS)
    ]
    selected = (interesting or lines)[-DIAGNOSTIC_LOG_LINES:]
    return [
        "fd-rdd.log 关键尾部：\n"
        + "\n".join(f"  {_bounded_text(line)}" for line in selected)
    ]


def _runner_log_diagnostics(run_dir: Path) -> list[str]:
    path = runner_log_path(run_dir)
    lines = _tail_lines(path, DIAGNOSTIC_SCAN_LINES)
    if not lines:
        return []
    interesting = [
        line for line in lines if any(marker in line.lower() for marker in LOG_ERROR_MARKERS)
    ]
    selected = (interesting or lines)[-DIAGNOSTIC_LOG_LINES:]
    return [
        "runner.log 关键尾部：\n"
        + "\n".join(f"  {_bounded_text(line)}" for line in selected)
    ]


def _artifact_status(run_dir: Path) -> str:
    paths = (
        ("manifest", run_dir / "manifest.json"),
        ("summary", run_dir / "summary.json"),
        ("events", run_dir / "events.jsonl"),
        ("daemon_log", run_dir / "fd-rdd.log"),
        ("runner_log", runner_log_path(run_dir)),
    )
    states: list[str] = []
    for name, path in paths:
        try:
            if not path.is_file():
                state = "missing"
            else:
                size = path.stat().st_size
                state = "empty" if size == 0 else f"present({size}B)"
        except OSError as exc:
            state = f"unreadable({_bounded_text(exc)})"
        states.append(f"{name}={state}")
    return "现场产物状态：" + ", ".join(states)


def collect_failure_diagnostics(run_dir: Path) -> list[str]:
    """Return bounded, copyable evidence for a failed benchmark leg."""

    diagnostics = [f"失败现场目录：{run_dir}", _artifact_status(run_dir)]
    diagnostics.extend(_manifest_diagnostics(run_dir))
    diagnostics.extend(_summary_diagnostics(run_dir))
    diagnostics.extend(_event_diagnostics(run_dir))
    diagnostics.extend(_runner_log_diagnostics(run_dir))
    diagnostics.extend(_log_diagnostics(run_dir))
    diagnostics.append(
        "产物路径："
        f"log={run_dir / 'fd-rdd.log'}；"
        f"runner_log={runner_log_path(run_dir)}；"
        f"manifest={run_dir / 'manifest.json'}；"
        f"summary={run_dir / 'summary.json'}；"
        f"events={run_dir / 'events.jsonl'}"
    )
    return diagnostics


def extend_failure_reasons(
    reasons: Iterable[str], run_dir: Path
) -> list[str]:
    """Append bounded on-disk evidence without duplicating an existing reason."""

    merged = [str(reason) for reason in reasons]
    for diagnostic in collect_failure_diagnostics(run_dir):
        if diagnostic not in merged:
            merged.append(diagnostic)
    return merged


def exception_reasons(error: Exception) -> list[str]:
    """Preserve structured gate reasons and name unexpected exception types."""

    reasons = getattr(error, "reasons", None)
    if isinstance(reasons, list):
        return [str(reason) for reason in reasons]
    return [f"{type(error).__name__}: {error}"]


def render_failure_report(reasons: Iterable[str], stream: TextIO) -> None:
    """Render each failure and its multiline evidence as a copyable list."""

    print("M2 A/B 门禁失败：", file=stream)
    for reason in reasons:
        lines = str(reason).splitlines() or [""]
        print(f"- {lines[0]}", file=stream)
        for line in lines[1:]:
            print(f"  {line}", file=stream)
