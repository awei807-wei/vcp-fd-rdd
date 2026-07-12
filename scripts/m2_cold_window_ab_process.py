"""Process lifecycle helpers for the M2 one-click A/B driver."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Sequence, TextIO


class TerminationRequested(BaseException):
    """Convert an external SIGTERM into a cleanup-aware control-flow signal."""

    def __init__(self, signum: int):
        self.signum = signum
        super().__init__(f"received signal {signum}")

    @property
    def exit_code(self) -> int:
        return 128 + self.signum


@contextmanager
def sigterm_as_exception() -> Iterator[None]:
    """Ensure SIGTERM unwinds Python scopes instead of bypassing cleanup."""
    previous = signal.getsignal(signal.SIGTERM)

    def handle(signum: int, _frame: Any) -> None:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        raise TerminationRequested(signum)

    signal.signal(signal.SIGTERM, handle)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, previous)


@contextmanager
def cleanup_signal_shield() -> Iterator[None]:
    """Prevent a repeated terminal signal from interrupting child cleanup."""
    previous_sigint = signal.getsignal(signal.SIGINT)
    previous_sigterm = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    try:
        yield
    finally:
        signal.signal(signal.SIGINT, previous_sigint)
        signal.signal(signal.SIGTERM, previous_sigterm)


@dataclass
class RunnerOutputCapture:
    """Tee runner output to the terminal and a persistent sibling log."""

    log_path: Path
    errors: list[str] = field(default_factory=list)
    thread: threading.Thread | None = None

    def join(self, timeout: float | None = None) -> list[str]:
        if self.thread is not None:
            try:
                self.thread.join(timeout)
            except Exception as exc:  # noqa: BLE001 - preserve the gate failure
                self.errors.append(f"等待 runner 输出采集线程失败：{exc}")
                return list(self.errors)
            if self.thread.is_alive():
                message = f"runner 输出采集线程未在 {timeout} 秒内结束"
                if message not in self.errors:
                    self.errors.append(message)
        return list(self.errors)


def _pump_output(
    stream: TextIO,
    log_file: TextIO,
    capture: RunnerOutputCapture,
) -> None:
    log_enabled = True
    terminal_enabled = True
    try:
        for line in stream:
            if log_enabled:
                try:
                    log_file.write(line)
                    log_file.flush()
                except (OSError, ValueError) as exc:
                    capture.errors.append(f"runner 日志写入失败：{exc}")
                    log_enabled = False
            if terminal_enabled:
                try:
                    sys.stdout.write(line)
                    sys.stdout.flush()
                except (OSError, ValueError) as exc:
                    capture.errors.append(f"runner 终端输出失败：{exc}")
                    terminal_enabled = False
    except (OSError, ValueError) as exc:
        capture.errors.append(f"runner 输出读取失败：{exc}")
    finally:
        try:
            stream.close()
        except OSError as exc:
            capture.errors.append(f"runner 输出管道关闭失败：{exc}")
        try:
            log_file.close()
        except OSError as exc:
            capture.errors.append(f"runner 日志关闭失败：{exc}")


def start_runner_process(
    command: Sequence[str], cwd: Path, log_path: Path
) -> tuple[subprocess.Popen[str], RunnerOutputCapture]:
    """Start the runner and persist its merged stdout/stderr without hiding it."""

    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_file = log_path.open("w", encoding="utf-8", buffering=1)
    try:
        process = subprocess.Popen(
            list(command),
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            start_new_session=True,
        )
    except BaseException:
        log_file.close()
        raise
    if process.stdout is None:
        log_file.close()
        process.terminate()
        raise RuntimeError("runner stdout pipe was not created")

    capture = RunnerOutputCapture(log_path=log_path)
    capture.thread = threading.Thread(
        target=_pump_output,
        args=(process.stdout, log_file, capture),
        name="m2-ab-runner-output",
        daemon=True,
    )
    capture.thread.start()
    return process, capture


def _is_running(process: subprocess.Popen[Any], errors: list[str]) -> bool:
    try:
        return process.poll() is None
    except Exception as exc:  # noqa: BLE001 - preserve the original gate failure
        errors.append(f"检查底层 benchmark 状态失败：{exc}")
        return True


def _group_alive(process_group: int, errors: list[str]) -> bool:
    try:
        os.killpg(process_group, 0)
        return True
    except ProcessLookupError:
        return False
    except Exception as exc:  # noqa: BLE001 - preserve cleanup evidence
        errors.append(f"检查 benchmark 进程组失败：{exc}")
        return True


def _signal_group(process_group: int, sig: signal.Signals, errors: list[str]) -> None:
    try:
        os.killpg(process_group, sig)
    except ProcessLookupError:
        return
    except Exception as exc:  # noqa: BLE001 - continue through escalation
        errors.append(f"向 benchmark 进程组发送 {sig.name} 失败：{exc}")


def _wait_group_exit(process_group: int, timeout_secs: float, errors: list[str]) -> bool:
    deadline = time.monotonic() + timeout_secs
    while time.monotonic() < deadline:
        if not _group_alive(process_group, errors):
            return True
        time.sleep(0.05)
    return not _group_alive(process_group, errors)


def stop_benchmark(
    process: subprocess.Popen[Any],
    graceful_timeout_secs: float = 330.0,
    fallback_timeout_secs: float = 10.0,
) -> list[str]:
    """Best-effort stop that never hides the gate failure which triggered it."""

    errors: list[str] = []
    process_group = int(process.pid)
    if not _is_running(process, errors) and not _group_alive(process_group, errors):
        return errors

    _signal_group(process_group, signal.SIGINT, errors)
    if _is_running(process, errors):
        try:
            process.wait(timeout=graceful_timeout_secs)
        except subprocess.TimeoutExpired:
            errors.append(
                f"底层 benchmark 在 {graceful_timeout_secs} 秒内未优雅停止"
            )
        except Exception as exc:  # noqa: BLE001 - continue with fallback
            errors.append(f"等待底层 benchmark 优雅停止失败：{exc}")
    if not _group_alive(process_group, errors):
        return errors

    _signal_group(process_group, signal.SIGTERM, errors)
    if _is_running(process, errors):
        try:
            process.wait(timeout=fallback_timeout_secs)
        except subprocess.TimeoutExpired:
            errors.append(
                f"底层 benchmark 在 SIGTERM 后 {fallback_timeout_secs} 秒内未停止"
            )
        except Exception as exc:  # noqa: BLE001 - continue with kill fallback
            errors.append(f"等待底层 benchmark SIGTERM 失败：{exc}")
    if _wait_group_exit(process_group, fallback_timeout_secs, errors):
        return errors

    _signal_group(process_group, signal.SIGKILL, errors)
    if _is_running(process, errors):
        try:
            process.wait(timeout=fallback_timeout_secs)
        except Exception as exc:  # noqa: BLE001 - diagnostics must still be rendered
            errors.append(f"等待底层 benchmark SIGKILL 失败：{exc}")
    if not _wait_group_exit(process_group, fallback_timeout_secs, errors):
        errors.append("benchmark 进程组在 SIGKILL 后仍未退出")
    return errors


def cleanup_benchmark(
    process: subprocess.Popen[Any] | None,
    capture: RunnerOutputCapture | None,
) -> list[str]:
    """在 fixture 锁仍持有时停止 runner，并汇总所有清理错误。"""
    errors: list[str] = []
    if process is not None:
        errors.extend(stop_benchmark(process))
    if capture is not None:
        errors.extend(capture.join(timeout=10))
    return errors
