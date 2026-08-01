#!/usr/bin/env python3
"""Process-group lifecycle for the standalone short-sweep fixture."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from m2_cold_window_ab_command import REPO_ROOT


RESULT_NAME = "sweep-integration-result.json"
PROCESS_GROUP_TERM_TIMEOUT_SECS = 90.0
PROCESS_GROUP_KILL_TIMEOUT_SECS = 10.0


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _group_alive(process_group: int) -> bool:
    try:
        os.killpg(process_group, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _wait_group_gone(process_group: int, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while _group_alive(process_group) and time.monotonic() < deadline:
        time.sleep(0.05)
    return not _group_alive(process_group)


def ensure_process_group_gone(
    process_group: int,
    *,
    term_timeout: float = PROCESS_GROUP_TERM_TIMEOUT_SECS,
    kill_timeout: float = PROCESS_GROUP_KILL_TIMEOUT_SECS,
) -> None:
    """On every exit, reap all descendants with TERM then KILL fallback."""
    if not _group_alive(process_group):
        return
    try:
        os.killpg(process_group, signal.SIGTERM)
    except ProcessLookupError:
        return
    if _wait_group_gone(process_group, term_timeout):
        return
    try:
        os.killpg(process_group, signal.SIGKILL)
    except ProcessLookupError:
        return
    if not _wait_group_gone(process_group, kill_timeout):
        raise RuntimeError(f"process group {process_group} survived SIGKILL")


def terminate_and_reap_process_group(process: subprocess.Popen[str]) -> None:
    """Terminate the leader and descendants, reap the leader, then verify the PGID."""
    process_group = process.pid
    if _group_alive(process_group):
        try:
            os.killpg(process_group, signal.SIGTERM)
        except ProcessLookupError:
            pass
    try:
        process.wait(timeout=PROCESS_GROUP_TERM_TIMEOUT_SECS)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process_group, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=PROCESS_GROUP_KILL_TIMEOUT_SECS)
    ensure_process_group_gone(process_group)


def _running_result(
    attempt: Path,
    *,
    product_git_sha: str,
    product_binary_sha256: str,
    product_receipt_sha256: str,
    harness_git_sha: str,
    harness_worktree_dirty: bool,
    source_suite: Path,
    expected_protocol_fingerprint: str,
) -> dict[str, Any]:
    return {
        "schema": 1,
        "status": "running",
        "exit_code": None,
        "run_dir": str(attempt),
        "source_suite": str(source_suite),
        "product_git_sha": product_git_sha,
        "product_binary_sha256": product_binary_sha256,
        "product_receipt_sha256": product_receipt_sha256,
        "harness_git_sha": harness_git_sha,
        "harness_worktree_dirty": harness_worktree_dirty,
        "protocol_fingerprint": expected_protocol_fingerprint,
        "process_group_id": 0,
        "started_at": _utc_iso(),
        "finished_at": "",
    }


def run_fixture_process(
    command: list[str],
    attempt: Path,
    *,
    product_git_sha: str,
    product_binary_sha256: str,
    product_receipt_sha256: str,
    harness_git_sha: str,
    harness_worktree_dirty: bool,
    source_suite: Path,
    expected_protocol_fingerprint: str,
) -> int:
    attempt.mkdir(parents=True, exist_ok=True)
    result_path = attempt / RESULT_NAME
    audit = _running_result(
        attempt,
        product_git_sha=product_git_sha,
        product_binary_sha256=product_binary_sha256,
        product_receipt_sha256=product_receipt_sha256,
        harness_git_sha=harness_git_sha,
        harness_worktree_dirty=harness_worktree_dirty,
        source_suite=source_suite,
        expected_protocol_fingerprint=expected_protocol_fingerprint,
    )
    _atomic_json(result_path, audit)
    log_path = attempt.parent / f"{attempt.name}.runner.log"
    with log_path.open("w", encoding="utf-8") as log:
        process = subprocess.Popen(
            command,
            cwd=REPO_ROOT,
            start_new_session=True,
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
        )
        audit["process_group_id"] = process.pid
        _atomic_json(result_path, audit)
        try:
            return_code = process.wait()
        except BaseException as process_error:
            try:
                terminate_and_reap_process_group(process)
            except Exception as cleanup_error:
                raise cleanup_error from process_error
            raise
        ensure_process_group_gone(process.pid)
    _atomic_json(
        result_path,
        {
            **audit,
            "status": "completed",
            "exit_code": return_code,
            "finished_at": _utc_iso(),
        },
    )
    return return_code
