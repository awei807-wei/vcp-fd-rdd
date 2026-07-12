"""Process lifecycle helpers for the M2 one-click A/B driver."""

from __future__ import annotations

import signal
import subprocess
import sys
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Sequence, TextIO


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


def stop_benchmark(
    process: subprocess.Popen[Any],
    graceful_timeout_secs: float = 330.0,
    fallback_timeout_secs: float = 10.0,
) -> list[str]:
    """Best-effort stop that never hides the gate failure which triggered it."""

    errors: list[str] = []
    if not _is_running(process, errors):
        return errors

    try:
        process.send_signal(signal.SIGINT)
    except Exception as exc:  # noqa: BLE001 - continue with terminate fallback
        errors.append(f"向底层 benchmark 发送 SIGINT 失败：{exc}")
    else:
        try:
            process.wait(timeout=graceful_timeout_secs)
            return errors
        except subprocess.TimeoutExpired:
            errors.append(
                f"底层 benchmark 在 {graceful_timeout_secs} 秒内未优雅停止"
            )
        except Exception as exc:  # noqa: BLE001 - continue with fallback
            errors.append(f"等待底层 benchmark 优雅停止失败：{exc}")

    if not _is_running(process, errors):
        return errors
    try:
        process.terminate()
        process.wait(timeout=fallback_timeout_secs)
        return errors
    except subprocess.TimeoutExpired:
        errors.append(
            f"底层 benchmark 在 terminate 后 {fallback_timeout_secs} 秒内未停止"
        )
    except Exception as exc:  # noqa: BLE001 - continue with kill fallback
        errors.append(f"终止底层 benchmark 失败：{exc}")

    if not _is_running(process, errors):
        return errors
    try:
        process.kill()
        process.wait(timeout=fallback_timeout_secs)
    except Exception as exc:  # noqa: BLE001 - diagnostics must still be rendered
        errors.append(f"强制终止底层 benchmark 失败：{exc}")
    return errors
