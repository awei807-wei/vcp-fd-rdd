#!/usr/bin/env python3
"""M2 冷层轮转一键 A/B 驱动。"""

from __future__ import annotations

import argparse
import shlex
import signal
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from m2_cold_window_ab_command import (
    BINARY,
    COLD_DIR_COUNT,
    FIXTURE_DIR_NAME,
    REPO_ROOT,
    RUN_ROOT,
    PROFILES,
    VARIANTS,
    build_command,
)
from m2_cold_window_ab_fixture import (
    assert_safe_fixture_root,
    exclusive_fixture_lock,
    fixture_root,
    rebuild_fixture,
)
from m2_cold_window_ab_result import (
    exclusive_wrapper_run_lock,
    try_write_wrapper_result,
)
from m2_cold_window_ab_validation import (
    REQUIRED_ARTIFACTS,
    GateError,
    validate_metrics,
    validate_run,
    wait_for_preflight,
)
from m2_cold_window_ab_diagnostics import (
    exception_reasons,
    extend_failure_reasons,
    render_failure_report,
    runner_log_path,
)
from m2_cold_window_ab_process import (
    RunnerOutputCapture,
    TerminationRequested,
    cleanup_signal_shield,
    cleanup_benchmark,
    sigterm_as_exception,
    start_runner_process,
    stop_benchmark,
)

def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="fd-rdd M2 冷层轮转一键 A/B 驱动")
    parser.add_argument("variant", choices=sorted(VARIANTS), help="a=开启轮转，b=关闭轮转")
    parser.add_argument("--profile", choices=sorted(PROFILES), default="standard")
    parser.add_argument("--run-dir", type=Path, default=None)
    parser.add_argument("--binary", type=Path, default=BINARY)
    parser.add_argument("--artifact-provenance-receipt", type=Path, default=None)
    parser.add_argument("--dry-run", action="store_true", help="仅打印固定路径和完整命令")
    return parser.parse_args(argv)


def _profile_precondition_error(args: argparse.Namespace) -> str:
    if args.profile != "falsification":
        return ""
    if args.artifact_provenance_receipt is None:
        return "falsification profile 必须由 suite 提供 --artifact-provenance-receipt"
    receipt = args.artifact_provenance_receipt.expanduser().resolve()
    if not receipt.is_file():
        return f"falsification build receipt 不存在：{receipt}"
    return ""


def _write_failed_result(
    args: argparse.Namespace,
    run_dir: Path,
    reasons: list[str],
) -> int:
    result_error = try_write_wrapper_result(
        run_dir,
        status="failed",
        exit_code=1,
        variant=args.variant,
        profile=args.profile,
        reasons=reasons,
    )
    if result_error:
        reasons.append(result_error)
    render_failure_report(extend_failure_reasons(reasons, run_dir), sys.stderr)
    return 1


def _write_interrupted_result(
    args: argparse.Namespace,
    run_dir: Path,
    interruption: KeyboardInterrupt | TerminationRequested,
    cleanup_reasons: list[str],
) -> int:
    exit_code = (
        interruption.exit_code
        if isinstance(interruption, TerminationRequested)
        else 130
    )
    reason = (
        f"M2 A/B 测试收到 SIGTERM({interruption.signum})"
        if isinstance(interruption, TerminationRequested)
        else "M2 A/B 测试已由用户中断"
    )
    reasons = [reason, *cleanup_reasons]
    result_error = try_write_wrapper_result(
        run_dir,
        status="interrupted",
        exit_code=exit_code,
        variant=args.variant,
        profile=args.profile,
        reasons=reasons,
    )
    if result_error:
        reasons.append(result_error)
    render_failure_report(reasons, sys.stderr)
    return exit_code


def _cleanup_safely(
    process: subprocess.Popen[Any] | None,
    capture: RunnerOutputCapture | None,
) -> list[str]:
    with cleanup_signal_shield():
        return cleanup_benchmark(process, capture)


def _run_locked(
    args: argparse.Namespace,
    run_dir: Path,
    binary: Path,
) -> int:
    process: subprocess.Popen[Any] | None = None
    capture: RunnerOutputCapture | None = None
    try:
        roots = rebuild_fixture()
        RUN_ROOT.mkdir(parents=True, exist_ok=True)
        command = build_command(
            args.variant,
            run_dir,
            roots,
            profile=args.profile,
            artifact_provenance_receipt=args.artifact_provenance_receipt,
            binary=binary,
        )
        process, capture = start_runner_process(
            command, REPO_ROOT, runner_log_path(run_dir)
        )
        running_result_error = try_write_wrapper_result(
            run_dir,
            status="running",
            exit_code=-1,
            variant=args.variant,
            profile=args.profile,
            benchmark_process_group=process.pid,
        )
        if running_result_error:
            raise GateError([running_result_error])
        wait_for_preflight(process, run_dir, args.variant)
        return_code = process.wait()
        capture_errors = capture.join(timeout=10)
        if capture_errors:
            raise GateError(["runner 输出采集失败", *capture_errors])
        if return_code != 0:
            raise GateError([f"底层 benchmark 失败，退出码 {return_code}"])
        validate_run(run_dir, args.variant)
    except (KeyboardInterrupt, TerminationRequested) as exc:
        return _write_interrupted_result(
            args,
            run_dir,
            exc,
            _cleanup_safely(process, capture),
        )
    except Exception as exc:  # noqa: BLE001 - persist every gate failure
        cleanup_reasons = _cleanup_safely(process, capture)
        reasons = exception_reasons(exc)
        reasons.extend(reason for reason in cleanup_reasons if reason not in reasons)
        return _write_failed_result(args, run_dir, reasons)

    result_error = try_write_wrapper_result(
        run_dir,
        status="passed",
        exit_code=0,
        variant=args.variant,
        profile=args.profile,
    )
    if result_error:
        return _write_failed_result(args, run_dir, [result_error])
    print(f"M2 {args.variant.upper()} 组测试及门禁通过：{run_dir}")
    return 0


def _execute(args: argparse.Namespace, run_dir: Path, binary: Path) -> int:
    precondition_error = _profile_precondition_error(args)
    if precondition_error:
        return _write_failed_result(args, run_dir, [precondition_error])

    initial_result_error = try_write_wrapper_result(
        run_dir,
        status="running",
        exit_code=-1,
        variant=args.variant,
        profile=args.profile,
    )
    if initial_result_error:
        render_failure_report([initial_result_error], sys.stderr)
        return 1
    try:
        with exclusive_fixture_lock():
            return _run_locked(args, run_dir, binary)
    except (KeyboardInterrupt, TerminationRequested) as exc:
        return _write_interrupted_result(args, run_dir, exc, [])
    except Exception as exc:  # noqa: BLE001 - lock/preparation failures need evidence
        return _write_failed_result(args, run_dir, exception_reasons(exc))


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    label, _, _ = VARIANTS[args.variant]
    default_label = label if args.profile == "standard" else f"{label}_{args.profile}"
    run_dir = (
        args.run_dir.expanduser().resolve()
        if args.run_dir is not None
        else RUN_ROOT / f"{utc_stamp()}_{default_label}"
    )
    binary = args.binary.expanduser().resolve()
    roots = {name: fixture_root() / name for name in ("cold-a", "cold-b", "hot")}
    command = build_command(
        args.variant,
        run_dir,
        roots,
        profile=args.profile,
        artifact_provenance_receipt=args.artifact_provenance_receipt,
        binary=binary,
    )

    print(f"variant: {args.variant} ({label})")
    print(f"profile: {args.profile}")
    print(f"fixture_root: {fixture_root()}")
    print(f"run_dir: {run_dir}")
    print(f"repo: {REPO_ROOT}")
    print(f"binary: {binary}")
    print("command:")
    print(shlex.join(command))
    if args.dry_run:
        return 0
    try:
        with sigterm_as_exception():
            try:
                with exclusive_wrapper_run_lock(run_dir):
                    return _execute(args, run_dir, binary)
            except RuntimeError as exc:
                render_failure_report([str(exc)], sys.stderr)
                return 1
    except (KeyboardInterrupt, TerminationRequested) as exc:
        return _write_interrupted_result(args, run_dir, exc, [])


if __name__ == "__main__":
    raise SystemExit(main())
