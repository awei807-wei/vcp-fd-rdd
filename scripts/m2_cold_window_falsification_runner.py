"""M2 快速证伪 suite 的编排、恢复与报告。"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import shlex
import signal
import subprocess
import sys
import tarfile  # 保留给既有故障注入测试的兼容导出
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from m2_cold_window_ab_command import REPO_ROOT, RUN_ROOT
from m2_cold_window_build_receipt import (
    cargo_args_for_target,
    compiler_artifact_path,
    create_build_receipt,
    git_head_sha as _git_head_sha,
    git_worktree_dirty as _git_worktree_dirty,
    sha256_file,
    validate_build_receipt,
)
from m2_cold_window_ab_result import (
    WRAPPER_RESULT_NAME,
    WRAPPER_RESULT_SCHEMA,
    running_wrapper_result_path,
)
from m2_cold_window_ab_process import (
    TerminationRequested,
    cleanup_signal_shield,
    sigterm_as_exception,
)
from m2_cold_window_falsification import (
    LegSpec,
    analyze_leg,
    balanced_block_orders,
    build_leg_specs,
)
from m2_cold_window_falsification_gate import evaluate_suite
from m2_cold_window_falsification_report import render_report
from m2_cold_window_evidence_bundle import (
    BUILD_RECEIPT_NAME,
    EVIDENCE_ATTEMPT_FILES,
    EVIDENCE_CHECKSUMS_NAME,
    EVIDENCE_MANIFEST_NAME,
    EVIDENCE_OPTIONAL_ATTEMPT_FILES,
    EVIDENCE_OPTIONAL_ROOT_FILES,
    EVIDENCE_REQUIRED_ATTEMPT_FILES,
    EVIDENCE_REQUIRED_ROOT_FILES,
    EVIDENCE_ROOT_FILES,
    EvidenceBundleError,
    evidence_bundle_path as _evidence_bundle_path,
    invalidate_published_evidence as _invalidate_published_evidence,
    remove_published_documents as _remove_published_documents,
    write_evidence_bundle as _write_evidence_bundle,
)


AB_DRIVER = REPO_ROOT / "scripts" / "m2-cold-window-ab.py"
BUILD_TARGET_NAME = "build-target"
WRAPPER_TERMINATION_TIMEOUT_SECS = 420.0
ORPHAN_TERMINATION_TIMEOUT_SECS = 10.0
MAX_REUSABLE_BLOCK_START_GAP_SECS = 1800.0


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _attempt_dirs(base_dir: Path) -> list[Path]:
    attempts = [path for path in base_dir.glob("attempt-*") if path.is_dir()]
    return sorted(attempts, key=_attempt_number)


def _attempt_number(path: Path) -> int:
    try:
        return int(path.name.removeprefix("attempt-"))
    except ValueError:
        return -1


def completed_attempt(
    base_dir: Path,
    expected_variant: str | None = None,
    expected_receipt_sha256: str = "",
    expected_binary_sha256: str = "",
) -> Path | None:
    """返回最新的完整有效腿；失败 attempt 保留但不会被复用。"""
    for path in reversed(_attempt_dirs(base_dir)):
        manifest = _read_json(path / "manifest.json")
        summary = _read_json(path / "summary.json")
        wrapper_result = _read_json(path / WRAPPER_RESULT_NAME)
        run_audit = summary.get("run_audit", {})
        provenance = (
            run_audit.get("artifact_provenance", {})
            if isinstance(run_audit, dict)
            else {}
        )
        provenance = provenance if isinstance(provenance, dict) else {}
        if (
            wrapper_result.get("schema") == WRAPPER_RESULT_SCHEMA
            and wrapper_result.get("status") == "passed"
            and wrapper_result.get("wrapper_exit_code") == 0
            and wrapper_result.get("profile") == "falsification"
            and (
                expected_variant is None
                or wrapper_result.get("variant") == expected_variant
            )
            and (
                not expected_receipt_sha256
                or provenance.get("receipt_sha256") == expected_receipt_sha256
            )
            and (
                not expected_binary_sha256
                or provenance.get("validated_binary_sha256")
                == expected_binary_sha256
            )
            and manifest.get("run_state") == "completed"
            and manifest.get("completion_reason") == "duration_elapsed"
            and manifest.get("ab_comparable") is True
            and summary.get("fd_rdd_exit_code") == 0
            and summary.get("ab_comparable") is True
        ):
            return path
    return None


def next_attempt(base_dir: Path) -> Path:
    number = max((_attempt_number(path) for path in _attempt_dirs(base_dir)), default=0)
    return base_dir / f"attempt-{number + 1:02d}"


def _manifest_start_epoch(attempt: Path) -> float | None:
    raw = str(_read_json(attempt / "manifest.json").get("daemon_started_at", ""))
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _reusable_block_attempts(
    specs: list[LegSpec],
    receipt_sha256: str,
    binary_sha256: str,
) -> dict[Path, Path]:
    """Reuse only an entire, temporally adjacent block; never splice sessions."""
    attempts: list[tuple[LegSpec, Path, float]] = []
    for spec in specs:
        attempt = completed_attempt(
            spec.base_dir,
            spec.variant,
            receipt_sha256,
            binary_sha256,
        )
        if attempt is None:
            return {}
        started = _manifest_start_epoch(attempt)
        if started is None:
            return {}
        attempts.append((spec, attempt, started))
    if len(attempts) != 2:
        return {}
    if attempts[1][2] <= attempts[0][2]:
        return {}
    if attempts[1][2] - attempts[0][2] > MAX_REUSABLE_BLOCK_START_GAP_SECS:
        return {}
    return {spec.base_dir: attempt for spec, attempt, _ in attempts}


@contextmanager
def exclusive_suite_lock(suite_dir: Path) -> Iterator[Path]:
    """保护 suite manifest、attempt 分配和 progress，拒绝同目录并发编排。"""
    path = suite_dir / ".suite.lock"
    with path.open("a+", encoding="utf-8") as stream:
        try:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(f"suite 目录正被另一进程占用：{suite_dir}") from exc
        try:
            yield path
        finally:
            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def suite_binary(suite_dir: Path) -> Path:
    return suite_dir / BUILD_TARGET_NAME / "release" / "fd-rdd"


def leg_command(
    spec: LegSpec,
    run_dir: Path,
    receipt_path: Path,
    binary: Path,
) -> list[str]:
    return [
        sys.executable,
        str(AB_DRIVER),
        spec.variant,
        "--profile",
        "falsification",
        "--run-dir",
        str(run_dir),
        "--binary",
        str(binary),
        "--artifact-provenance-receipt",
        str(receipt_path),
    ]


def run_wrapper_process(command: list[str]) -> int:
    """运行一腿；外层中断时先让 wrapper 清理其独立 benchmark 进程组。"""
    process = subprocess.Popen(command, cwd=REPO_ROOT, start_new_session=True)
    try:
        return process.wait()
    except BaseException:
        with cleanup_signal_shield():
            if process.poll() is None:
                try:
                    process.send_signal(signal.SIGTERM)
                    process.wait(timeout=WRAPPER_TERMINATION_TIMEOUT_SECS)
                except subprocess.TimeoutExpired:
                    _terminate_recorded_benchmark(command)
                    _kill_process_group(process.pid)
                    try:
                        process.wait(timeout=ORPHAN_TERMINATION_TIMEOUT_SECS)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=ORPHAN_TERMINATION_TIMEOUT_SECS)
                except ProcessLookupError:
                    pass
        raise


def _command_option(command: list[str], option: str) -> str:
    try:
        return command[command.index(option) + 1]
    except (ValueError, IndexError):
        return ""


def _group_alive(process_group: int) -> bool:
    try:
        os.killpg(process_group, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _kill_process_group(process_group: int) -> None:
    try:
        os.killpg(process_group, signal.SIGKILL)
    except ProcessLookupError:
        pass


def _verified_benchmark_group(run_dir: Path) -> int:
    running = _read_json(running_wrapper_result_path(run_dir))
    try:
        process_group = int(running.get("benchmark_process_group", 0) or 0)
    except (TypeError, ValueError):
        process_group = 0
    if process_group <= 1:
        manifest = _read_json(run_dir / "manifest.json")
        try:
            daemon_pid = int(manifest.get("daemon_pid", 0) or 0)
            process_group = os.getpgid(daemon_pid) if daemon_pid > 1 else 0
        except (OSError, TypeError, ValueError):
            return 0
    try:
        if os.getpgid(process_group) != process_group:
            return 0
        cmdline = Path(f"/proc/{process_group}/cmdline").read_bytes().split(b"\0")
    except (OSError, ProcessLookupError):
        return 0
    if not any(
        Path(os.fsdecode(arg)).name == "m2-cold-window-vm-bench.py"
        for arg in cmdline
        if arg
    ):
        return 0
    return process_group


def _terminate_recorded_benchmark(command: list[str]) -> None:
    """Last-resort cleanup for a wrapper that could not finish its own teardown."""
    raw_run_dir = _command_option(command, "--run-dir")
    if not raw_run_dir:
        return
    process_group = _verified_benchmark_group(Path(raw_run_dir))
    if process_group <= 1:
        return
    try:
        os.killpg(process_group, signal.SIGTERM)
    except ProcessLookupError:
        return
    deadline = time.monotonic() + ORPHAN_TERMINATION_TIMEOUT_SECS
    while time.monotonic() < deadline and _group_alive(process_group):
        time.sleep(0.05)
    if _group_alive(process_group):
        _kill_process_group(process_group)


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    temp.replace(path)


def _resume_error(suite_dir: Path, seed: int) -> str:
    manifest_path = suite_dir / "manifest.json"
    if not manifest_path.exists():
        return ""
    manifest = _read_json(manifest_path)
    if not manifest:
        return "已有 suite manifest 无法解析，拒绝覆盖续跑"
    previous_seed = manifest.get("sequence_seed")
    try:
        parsed_seed = int(previous_seed) if previous_seed is not None else None
    except (TypeError, ValueError):
        return f"已有 suite 的 sequence_seed 无效：{previous_seed!r}"
    if parsed_seed is not None and parsed_seed != seed:
        return f"已有 suite 的 sequence_seed={previous_seed}，不能改为 {seed} 续跑"
    return ""


def _write_running_manifest(suite_dir: Path, seed: int) -> None:
    _atomic_json(
        suite_dir / "manifest.json",
        {
            "schema": 1,
            "run_state": "running",
            "decision": "pending",
            "suite_dir": str(suite_dir),
            "sequence_seed": seed,
            "completed_legs": 0,
        },
    )


def _progress_legs(suite_dir: Path) -> list[dict[str, Any]]:
    progress = _read_json(suite_dir / "progress.json")
    legs = progress.get("legs", [])
    if not isinstance(legs, list):
        return []
    return [
        leg
        for leg in legs
        if isinstance(leg, dict) and leg.get("valid") is True
    ]


def _recover_completed_legs(
    specs: list[LegSpec],
    suite_dir: Path,
    receipt_path: Path,
) -> list[dict[str, Any]]:
    """Reconstruct passed legs so an interrupt cannot lose a just-finished attempt."""
    binary = suite_binary(suite_dir)
    if not receipt_path.is_file() or not binary.is_file():
        return _progress_legs(suite_dir)
    try:
        receipt_sha256 = sha256_file(receipt_path)
        binary_sha256 = sha256_file(binary)
    except OSError:
        return _progress_legs(suite_dir)
    legs: list[dict[str, Any]] = []
    for block in range(1, 5):
        block_specs = [spec for spec in specs if spec.block == block]
        reusable = _reusable_block_attempts(
            block_specs, receipt_sha256, binary_sha256
        )
        if len(reusable) != 2:
            break
        legs.extend(
            analyze_leg(spec, reusable[spec.base_dir]) for spec in block_specs
        )
    return legs


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="fd-rdd M2 四块配对快速证伪 suite")
    parser.add_argument("--run-dir", type=Path, default=None)
    parser.add_argument("--sequence-seed", type=int, default=42)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def _print_plan(specs: list[LegSpec], skip_build: bool, receipt_path: Path) -> None:
    binary = suite_binary(receipt_path.parent)
    build_command = cargo_args_for_target(binary.parent.parent)
    print("build: skipped" if skip_build else f"build: {shlex.join(build_command)}")
    print(f"build_receipt: {receipt_path}")
    for spec in specs:
        command = shlex.join(
            leg_command(spec, spec.base_dir / "attempt-01", receipt_path, binary)
        )
        print(
            f"block={spec.block} order={spec.order} position={spec.position} {command}"
        )


def _run_legs(
    specs: list[LegSpec],
    suite_dir: Path,
    receipt_path: Path,
) -> tuple[list[dict[str, Any]], str]:
    legs: list[dict[str, Any]] = []
    binary = suite_binary(suite_dir)
    receipt_sha256 = sha256_file(receipt_path)
    binary_sha256 = sha256_file(binary)
    for block in range(1, 5):
        block_specs = [spec for spec in specs if spec.block == block]
        reusable = _reusable_block_attempts(
            block_specs, receipt_sha256, binary_sha256
        )
        block_legs: list[dict[str, Any]] = []
        for spec in block_specs:
            attempt = reusable.get(spec.base_dir)
            if attempt is None:
                attempt = next_attempt(spec.base_dir)
                attempt.parent.mkdir(parents=True, exist_ok=True)
                return_code = run_wrapper_process(
                    leg_command(spec, attempt, receipt_path, binary)
                )
                if return_code != 0:
                    _atomic_json(suite_dir / "progress.json", {"legs": legs})
                    return [*legs, analyze_leg(spec, attempt)], (
                        f"block {spec.block} {spec.variant.upper()} 组退出码 "
                        f"{return_code}"
                    )
                if completed_attempt(
                    spec.base_dir,
                    spec.variant,
                    receipt_sha256,
                    binary_sha256,
                ) != attempt:
                    _atomic_json(suite_dir / "progress.json", {"legs": legs})
                    return [*legs, analyze_leg(spec, attempt)], (
                        f"block {spec.block} {spec.variant.upper()} wrapper 返回 0，"
                        "但终态不可恢复"
                    )
            block_legs.append(analyze_leg(spec, attempt))
        legs.extend(block_legs)
        _atomic_json(suite_dir / "progress.json", {"legs": legs})
    return legs, ""


def _suite_summary(
    suite_dir: Path,
    seed: int,
    legs: list[dict[str, Any]],
    infrastructure_error: str,
) -> dict[str, Any]:
    gate = evaluate_suite(legs)
    if infrastructure_error:
        gate["decision"] = "fail"
        gate["reasons"].append(infrastructure_error)
    return {
        "schema": 1,
        "suite_dir": str(suite_dir),
        "sequence_seed": seed,
        "orders": ["".join(order) for order in balanced_block_orders(seed)],
        "legs": legs,
        "infrastructure_error": infrastructure_error,
        "gate": gate,
    }


def _write_output_documents(suite_dir: Path, summary: dict[str, Any]) -> None:
    _atomic_json(suite_dir / "summary.json", summary)
    (suite_dir / "REPORT.md").write_text(
        render_report(summary),
        encoding="utf-8",
    )
    _atomic_json(
        suite_dir / "manifest.json",
        {
            "schema": 1,
            "run_state": (
                "completed"
                if len(summary["legs"]) == 8
                and not summary.get("infrastructure_error")
                else "failed"
            ),
            "decision": summary["gate"]["decision"],
            "suite_dir": str(suite_dir),
            "sequence_seed": summary["sequence_seed"],
            "completed_legs": sum(
                1 for leg in summary["legs"] if leg.get("valid") is True
            ),
            "observed_legs": len(summary["legs"]),
            "reasons": summary["gate"]["reasons"],
        },
    )


def _record_evidence_bundle_failure(
    summary: dict[str, Any],
    error: Exception,
) -> None:
    reason = f"证据包生成失败：{type(error).__name__}: {error}"
    gate = summary.get("gate", {})
    gate = dict(gate) if isinstance(gate, dict) else {}
    reasons = gate.get("reasons", [])
    reasons = list(reasons) if isinstance(reasons, list) else []
    gate["decision"] = "fail"
    if reason not in reasons:
        reasons.append(reason)
    gate["reasons"] = reasons
    summary["gate"] = gate
    previous = str(summary.get("infrastructure_error", ""))
    summary["infrastructure_error"] = f"{previous}; {reason}" if previous else reason


def _resume_rejection_documents(
    suite_dir: Path,
    requested_seed: int,
    previous_manifest: dict[str, Any],
    previous_summary: dict[str, Any],
    reason: str,
) -> tuple[dict[str, Any], dict[str, Any], str]:
    gate = previous_summary.get("gate", {})
    gate = dict(gate) if isinstance(gate, dict) else {}
    reasons = gate.get("reasons", [])
    reasons = list(reasons) if isinstance(reasons, list) else []
    if reason not in reasons:
        reasons.append(reason)
    gate.update({"decision": "fail", "reasons": reasons})
    persisted_summary = dict(previous_summary)
    persisted_summary.update(
        {
            "schema": (
                previous_summary.get("schema")
                if isinstance(previous_summary.get("schema"), int)
                else 1
            ),
            "suite_dir": str(suite_dir),
            "sequence_seed": previous_manifest.get(
                "sequence_seed",
                previous_summary.get("sequence_seed", requested_seed),
            ),
            "requested_sequence_seed": requested_seed,
            "infrastructure_error": reason,
            "gate": gate,
        }
    )
    persisted_manifest = dict(previous_manifest)
    manifest_reasons = persisted_manifest.get("reasons", [])
    manifest_reasons = (
        list(manifest_reasons) if isinstance(manifest_reasons, list) else []
    )
    if reason not in manifest_reasons:
        manifest_reasons.append(reason)
    persisted_manifest.update(
        {
            "schema": (
                previous_manifest.get("schema")
                if isinstance(previous_manifest.get("schema"), int)
                else 1
            ),
            "run_state": "failed",
            "decision": "fail",
            "suite_dir": str(suite_dir),
            "requested_sequence_seed": requested_seed,
            "reasons": manifest_reasons,
        }
    )
    report = (
        "# M2 快速证伪续跑被拒绝\n\n"
        f"- 判定：`fail`\n- 原因：{reason}\n"
    )
    return persisted_summary, persisted_manifest, report


def _persist_resume_rejection(
    suite_dir: Path,
    requested_seed: int,
    previous_manifest: dict[str, Any],
    previous_summary: dict[str, Any],
    reason: str,
) -> None:
    """Invalidate a prior pass before recording a rejected resume request."""
    _invalidate_published_evidence(suite_dir, remove_documents=True)
    summary, manifest, report = _resume_rejection_documents(
        suite_dir,
        requested_seed,
        previous_manifest,
        previous_summary,
        reason,
    )
    try:
        _atomic_json(suite_dir / "summary.json", summary)
        (suite_dir / "REPORT.md").write_text(report, encoding="utf-8")
        _atomic_json(suite_dir / "manifest.json", manifest)
    except Exception:  # noqa: BLE001 - absence is safer than a stale pass
        _invalidate_published_evidence(suite_dir, remove_documents=True)


def _write_outputs(suite_dir: Path, summary: dict[str, Any]) -> Path | None:
    suite_dir.mkdir(parents=True, exist_ok=True)
    with cleanup_signal_shield():
        _invalidate_published_evidence(suite_dir)
        try:
            _write_output_documents(suite_dir, summary)
            return _write_evidence_bundle(suite_dir, summary)
        except Exception as exc:  # noqa: BLE001 - persist explicit infrastructure failure
            _record_evidence_bundle_failure(summary, exc)
            _invalidate_published_evidence(suite_dir, remove_documents=True)
            try:
                _write_output_documents(suite_dir, summary)
            except Exception as persist_exc:  # noqa: BLE001 - fail closed on disk errors
                _record_evidence_bundle_failure(summary, persist_exc)
                _invalidate_published_evidence(suite_dir, remove_documents=True)
            return None


def _validate_build_receipt(receipt_path: Path, binary: Path) -> str:
    dirty = _git_worktree_dirty(REPO_ROOT)
    if dirty is not False:
        state = "不干净" if dirty else "状态不可用"
        return f"当前 Git 工作区{state}，不能复用 {BUILD_RECEIPT_NAME}"
    errors = validate_build_receipt(
        receipt_path,
        REPO_ROOT,
        binary,
        _git_head_sha(REPO_ROOT),
    )
    if not errors:
        return ""
    return f"{BUILD_RECEIPT_NAME} 无效：{', '.join(errors)}"


def _build_release(suite_dir: Path) -> str:
    pre_build_git_sha = _git_head_sha(REPO_ROOT)
    pre_build_dirty = _git_worktree_dirty(REPO_ROOT)
    if not pre_build_git_sha:
        return "release binary 构建前无法读取 Git HEAD"
    if pre_build_dirty is not False:
        state = "不干净" if pre_build_dirty else "状态不可用"
        return f"release binary 构建前 Git 工作区{state}"
    binary = suite_binary(suite_dir)
    cargo_args = cargo_args_for_target(binary.parent.parent)
    result = subprocess.run(
        cargo_args,
        cwd=REPO_ROOT,
        check=False,
        stdout=subprocess.PIPE,
        text=True,
    )
    if result.returncode != 0:
        return "release binary 构建失败"
    artifact = compiler_artifact_path(result.stdout or "")
    if artifact != binary.resolve() or not binary.is_file():
        return "Cargo 未报告本次 suite 私有 fd-rdd compiler artifact"
    post_build_git_sha = _git_head_sha(REPO_ROOT)
    post_build_dirty = _git_worktree_dirty(REPO_ROOT)
    if post_build_git_sha != pre_build_git_sha:
        return "release binary 构建期间 Git HEAD 发生变化"
    if post_build_dirty is not False:
        state = "不干净" if post_build_dirty else "状态不可用"
        return f"release binary 构建后 Git 工作区{state}"
    suite_dir.mkdir(parents=True, exist_ok=True)
    receipt_path = suite_dir / BUILD_RECEIPT_NAME
    _atomic_json(
        receipt_path,
        create_build_receipt(
            REPO_ROOT,
            binary,
            pre_build_git_sha,
            post_build_git_sha,
        ),
    )
    return _validate_build_receipt(receipt_path, binary)


def _prepare_build(suite_dir: Path, skip_build: bool) -> str:
    receipt_path = suite_dir / BUILD_RECEIPT_NAME
    binary = suite_binary(suite_dir)
    if receipt_path.exists():
        return _validate_build_receipt(receipt_path, binary)
    if any(suite_dir.glob("block-*/**/attempt-*")):
        return f"已有 attempt 但缺少 {BUILD_RECEIPT_NAME}，拒绝混用二进制续跑"
    if skip_build:
        return f"--skip-build 需要已有且有效的 {BUILD_RECEIPT_NAME}"
    return _build_release(suite_dir)


def _prepare_suite_resume(suite_dir: Path, sequence_seed: int) -> str:
    previous_manifest = _read_json(suite_dir / "manifest.json")
    previous_summary = _read_json(suite_dir / "summary.json")
    error = _resume_error(suite_dir, sequence_seed)
    if error:
        _persist_resume_rejection(
            suite_dir,
            sequence_seed,
            previous_manifest,
            previous_summary,
            error,
        )
        return error
    _invalidate_published_evidence(suite_dir, remove_documents=True)
    return ""


def _run_suite(
    args: argparse.Namespace,
    suite_dir: Path,
    specs: list[LegSpec],
    receipt_path: Path,
) -> int:
    suite_dir.mkdir(parents=True, exist_ok=True)
    interrupted_exit_code = 0
    infrastructure_error = _prepare_suite_resume(suite_dir, args.sequence_seed)
    if infrastructure_error:
        print(f"M2 快速证伪拒绝续跑：{infrastructure_error}", file=sys.stderr)
        return 1
    legs: list[dict[str, Any]] = []
    try:
        _write_running_manifest(suite_dir, args.sequence_seed)
        infrastructure_error = _prepare_build(suite_dir, args.skip_build)
        if not infrastructure_error:
            legs, infrastructure_error = _run_legs(
                specs,
                suite_dir,
                receipt_path,
            )
    except (KeyboardInterrupt, TerminationRequested) as exc:
        legs = _recover_completed_legs(specs, suite_dir, receipt_path)
        interrupted_exit_code = (
            exc.exit_code if isinstance(exc, TerminationRequested) else 130
        )
        source = "SIGTERM" if isinstance(exc, TerminationRequested) else "用户"
        infrastructure_error = (
            f"suite 被{source}中断，可用同一 --run-dir 续跑"
        )
    except Exception as exc:  # noqa: BLE001 - persist the exact infrastructure failure
        legs = _recover_completed_legs(specs, suite_dir, receipt_path)
        infrastructure_error = (
            f"suite 编排异常：{type(exc).__name__}: {exc}；可用同一 --run-dir 续跑"
        )
    summary = _suite_summary(
        suite_dir,
        args.sequence_seed,
        legs,
        infrastructure_error,
    )
    evidence_bundle = _write_outputs(suite_dir, summary)
    print(f"suite_dir: {suite_dir}")
    print(f"summary_path: {suite_dir / 'summary.json'}")
    print(f"report_path: {suite_dir / 'REPORT.md'}")
    print(f"evidence_bundle: {evidence_bundle or 'unavailable'}")
    print(f"M2 快速证伪判定: {summary['gate']['decision']}")
    for reason in summary["gate"]["reasons"]:
        print(f"gate_reason: {reason}")
    if interrupted_exit_code:
        return interrupted_exit_code
    if evidence_bundle is None:
        return 1
    if infrastructure_error:
        return 1
    return 0 if summary["gate"]["decision"] == "pass" else 2


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    suite_dir = (
        args.run_dir.expanduser().resolve()
        if args.run_dir is not None
        else RUN_ROOT / f"{utc_stamp()}_m2_falsification"
    )
    specs = build_leg_specs(suite_dir, args.sequence_seed)
    receipt_path = suite_dir / BUILD_RECEIPT_NAME
    if args.dry_run:
        _print_plan(specs, args.skip_build, receipt_path)
        return 0
    suite_dir.mkdir(parents=True, exist_ok=True)
    try:
        with sigterm_as_exception():
            with exclusive_suite_lock(suite_dir):
                return _run_suite(args, suite_dir, specs, receipt_path)
    except (KeyboardInterrupt, TerminationRequested) as exc:
        legs = _recover_completed_legs(specs, suite_dir, receipt_path)
        exit_code = exc.exit_code if isinstance(exc, TerminationRequested) else 130
        source = "SIGTERM" if isinstance(exc, TerminationRequested) else "用户"
        summary = _suite_summary(
            suite_dir,
            args.sequence_seed,
            legs,
            f"suite 被{source}中断，可用同一 --run-dir 续跑",
        )
        _write_outputs(suite_dir, summary)
        return exit_code
    except RuntimeError as exc:
        print(f"M2 快速证伪拒绝并发运行：{exc}", file=sys.stderr)
        return 1
