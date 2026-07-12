"""M2 A/B wrapper 的可恢复终态记录。"""

from __future__ import annotations

import json
import fcntl
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator


WRAPPER_RESULT_NAME = "ab-wrapper-result.json"
WRAPPER_RESULT_SCHEMA = 1


def running_wrapper_result_path(run_dir: Path) -> Path:
    """运行中证据放在同级，不能提前创建 benchmark 的新 run_dir。"""
    return run_dir.parent / f".{run_dir.name}.ab-wrapper-running.json"


@contextmanager
def exclusive_wrapper_run_lock(run_dir: Path) -> Iterator[Path]:
    """Prevent two wrappers from racing on the same benchmark run directory."""
    run_dir.parent.mkdir(parents=True, exist_ok=True)
    path = run_dir.parent / f".{run_dir.name}.ab-wrapper.lock"
    with path.open("a+", encoding="utf-8") as stream:
        try:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(f"run 目录正被另一 wrapper 占用：{run_dir}") from exc
        try:
            yield path
        finally:
            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def write_wrapper_result(
    run_dir: Path,
    *,
    status: str,
    exit_code: int,
    variant: str,
    profile: str,
    reasons: Iterable[str] = (),
    benchmark_process_group: int = 0,
) -> Path:
    """原子写入 wrapper 终态，供 suite 断点续跑做 fail-closed 判定。"""
    if status == "running":
        run_dir.parent.mkdir(parents=True, exist_ok=True)
        path = running_wrapper_result_path(run_dir)
    else:
        run_dir.mkdir(parents=True, exist_ok=True)
        path = run_dir / WRAPPER_RESULT_NAME
    payload: dict[str, Any] = {
        "schema": WRAPPER_RESULT_SCHEMA,
        "recorded_at": datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z"),
        "status": status,
        "wrapper_exit_code": exit_code,
        "variant": variant,
        "profile": profile,
        "reasons": list(reasons),
    }
    if benchmark_process_group > 0:
        payload["benchmark_process_group"] = benchmark_process_group
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)
    if status != "running":
        try:
            running_wrapper_result_path(run_dir).unlink()
        except FileNotFoundError:
            pass
    return path


def try_write_wrapper_result(
    run_dir: Path,
    *,
    status: str,
    exit_code: int,
    variant: str,
    profile: str,
    reasons: Iterable[str] = (),
    benchmark_process_group: int = 0,
) -> str:
    """写入终态并把 I/O 失败转换为可并入门禁报告的原因。"""
    try:
        write_wrapper_result(
            run_dir,
            status=status,
            exit_code=exit_code,
            variant=variant,
            profile=profile,
            reasons=reasons,
            benchmark_process_group=benchmark_process_group,
        )
    except OSError as exc:
        return f"wrapper 终态写入失败（{status}）：{exc}"
    return ""
