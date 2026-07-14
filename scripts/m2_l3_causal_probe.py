"""One-click focused M2 L3 causal probe orchestration."""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from m2_l3_causal_probe_analysis import analyze_run
from m2_l3_causal_probe_command import build_benchmark_command, prepare_fixture
from m2_l3_causal_probe_config import (
    DEFAULT_RUN_ROOT,
    PROBE_ALLOWED_AB_GATE_REASONS,
    REPO_ROOT,
)
from m2_l3_causal_probe_report import render_report


def _default_suite_dir() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    nonce = uuid.uuid4().hex[:8]
    return DEFAULT_RUN_ROOT / (
        f"{stamp}_{os.getpid()}_{nonce}_m2_l3_causal_probe"
    )


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run one focused M2 L3 subtree-rename causal probe."
    )
    parser.add_argument("--repo", default=str(REPO_ROOT))
    parser.add_argument("--binary", default="")
    parser.add_argument("--build", choices=("always", "never"), default="never")
    parser.add_argument("--run-dir", default="")
    parser.add_argument("--fixture-root", default="")
    parser.add_argument(
        "--port",
        type=int,
        default=0,
        help="HTTP endpoint port; 0 selects a currently free loopback port",
    )
    parser.add_argument("--analyze-only", default="")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def find_free_tcp_port() -> int:
    """Return a currently free loopback TCP port for the benchmark daemon."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _resolve_port(requested: int) -> int:
    if requested == 0:
        return find_free_tcp_port()
    if not 1 <= requested <= 65535:
        raise ValueError(f"--port must be 0 or between 1 and 65535: {requested}")
    return requested


def _resolved_layout(args: argparse.Namespace) -> tuple[Path, Path, Path, Path, Path]:
    repo_root = Path(args.repo).expanduser().resolve()
    suite_dir = (
        Path(args.run_dir).expanduser().resolve()
        if args.run_dir
        else _default_suite_dir()
    )
    fixture_root = (
        Path(args.fixture_root).expanduser().resolve()
        if args.fixture_root
        else Path.home()
        / "fd-rdd-m2-l3-causal-probe"
        / suite_dir.name
        / "root"
    )
    binary = (
        Path(args.binary).expanduser().resolve()
        if args.binary
        else repo_root / "target" / "release" / "fd-rdd"
    )
    return repo_root, suite_dir, suite_dir / "benchmark", fixture_root, binary


def _write_json(path: Path, value: dict[str, Any]) -> None:
    next_path = path.with_name(path.name + ".next")
    next_path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    next_path.replace(path)


def _run_command(command: list[str], log_path: Path) -> int:
    """Run the benchmark while mirroring stdout to the terminal and runner log."""
    with log_path.open("w", encoding="utf-8") as log_handle:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        assert process.stdout is not None
        for line in process.stdout:
            print(line, end="", flush=True)
            log_handle.write(line)
            log_handle.flush()
        return process.wait()


def _decision_exit_code(decision: str) -> int:
    if decision == "pass":
        return 0
    return 2 if decision == "infrastructure_error" else 1


def _runner_exit_is_allowed_ab_gate(
    runner_exit_code: int, result: dict[str, Any]
) -> bool:
    benchmark = result.get("benchmark")
    if runner_exit_code != 1 or not isinstance(benchmark, dict):
        return False
    reasons = benchmark.get("ab_comparability_reasons")
    return (
        result.get("decision") != "infrastructure_error"
        and benchmark.get("fd_rdd_exit_code") == 0
        and not benchmark.get("fatal_error")
        and benchmark.get("manifest_ab_comparable") is False
        and benchmark.get("summary_ab_comparable") is False
        and benchmark.get("ab_comparability_reasons_valid") is True
        and isinstance(reasons, list)
        and bool(reasons)
        and set(reasons).issubset(PROBE_ALLOWED_AB_GATE_REASONS)
    )


def _apply_runner_exit_gate(
    runner_exit_code: int, result: dict[str, Any]
) -> None:
    accepted = _runner_exit_is_allowed_ab_gate(runner_exit_code, result)
    result["runner_exit_accepted_as_ab_gate"] = accepted
    benchmark = result.get("benchmark")
    if runner_exit_code == 0:
        comparable = (
            benchmark.get("manifest_ab_comparable") is True
            and benchmark.get("summary_ab_comparable") is True
            and benchmark.get("ab_comparability_reasons") == []
            and benchmark.get("ab_comparability_consistent") is True
        ) if isinstance(benchmark, dict) else False
        if comparable:
            return
        result["decision"] = "infrastructure_error"
        result.setdefault("gate_reasons", []).insert(
            0,
            "benchmark runner 退出 0 但 benchmark A/B 可比性证据不一致",
        )
        return
    if accepted:
        return
    reasons = (
        benchmark.get("ab_comparability_reasons", [])
        if isinstance(benchmark, dict)
        else []
    )
    detail = f"，A/B reasons={reasons!r}" if reasons else ""
    result["decision"] = "infrastructure_error"
    result.setdefault("gate_reasons", []).insert(
        0,
        f"benchmark runner 非零退出 {runner_exit_code}，不属于允许的 A/B 门禁{detail}",
    )


def _execute_probe(
    args: argparse.Namespace,
    suite_dir: Path,
    benchmark_dir: Path,
    fixture_root: Path,
    binary: Path,
    command: list[str],
) -> tuple[int, dict[str, Any]]:
    if args.build == "never" and not binary.is_file():
        raise FileNotFoundError(f"release binary not found: {binary}")
    fixture = prepare_fixture(fixture_root)
    _write_json(suite_dir / "command.json", {"argv": command})
    runner_log = suite_dir / "runner.log"
    runner_exit_code = _run_command(command, runner_log)
    result = analyze_run(benchmark_dir)
    result.update(
        {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "suite_dir": str(suite_dir),
            "fixture_root": str(fixture_root),
            "fixture": fixture,
            "runner_log": str(runner_log),
            "runner_exit_code": runner_exit_code,
        }
    )
    _apply_runner_exit_gate(runner_exit_code, result)
    _write_json(suite_dir / "summary.json", result)
    (suite_dir / "REPORT.md").write_text(render_report(result), encoding="utf-8")
    return _decision_exit_code(result["decision"]), result


def _analyze_only(run_dir: Path) -> tuple[int, dict[str, Any]]:
    result = analyze_run(run_dir)
    result["generated_at"] = datetime.now(timezone.utc).isoformat()
    result["analysis_mode"] = "analyze-only"
    result["runner_log"] = str(run_dir / "fd-rdd.log")
    _write_json(run_dir / "l3-causal-probe-summary.json", result)
    (run_dir / "L3-CAUSAL-PROBE-REPORT.md").write_text(
        render_report(result), encoding="utf-8"
    )
    return _decision_exit_code(result["decision"]), result


def _persist_infrastructure_failure(
    suite_dir: Path,
    fixture_root: Path,
    result: dict[str, Any],
) -> None:
    """Persist startup/runtime probe failures when this invocation owns the suite."""
    result.update(
        {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "suite_dir": str(suite_dir),
            "fixture_root": str(fixture_root),
            "runner_log": str(suite_dir / "runner.log"),
        }
    )
    _write_json(suite_dir / "summary.json", result)
    (suite_dir / "REPORT.md").write_text(render_report(result), encoding="utf-8")


def _print_result(
    result: dict[str, Any],
    output_dir: Path,
    analyze_only: bool,
    *,
    artifacts_persisted: bool | None = None,
) -> None:
    summary_name = "l3-causal-probe-summary.json" if analyze_only else "summary.json"
    report_name = "L3-CAUSAL-PROBE-REPORT.md" if analyze_only else "REPORT.md"
    summary_path = output_dir / summary_name
    report_path = output_dir / report_name
    print(
        json.dumps(
            {
                "decision": result["decision"],
                "output_dir": str(output_dir),
                "summary_path": str(summary_path),
                "report_path": str(report_path),
                "artifacts_persisted": (
                    summary_path.is_file() and report_path.is_file()
                    if artifacts_persisted is None
                    else artifacts_persisted
                ),
                "gate_reasons": result.get("gate_reasons", []),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def _dry_run_payload(
    suite_dir: Path,
    benchmark_dir: Path,
    fixture_root: Path,
    command: list[str],
) -> dict[str, Any]:
    return {
        "mode": "dry-run",
        "suite_dir": str(suite_dir),
        "benchmark_run_dir": str(benchmark_dir),
        "fixture_root": str(fixture_root),
        "command": command,
    }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.analyze_only:
        run_dir = Path(args.analyze_only).expanduser().resolve()
        try:
            exit_code, result = _analyze_only(run_dir)
        except (OSError, ValueError) as exc:
            result = {
                "decision": "infrastructure_error",
                "gate_reasons": [f"probe 离线分析失败：{exc}"],
                "stages": {},
            }
            exit_code = 2
        _print_result(result, run_dir, True)
        return exit_code

    repo_root, suite_dir, benchmark_dir, fixture_root, binary = _resolved_layout(args)
    suite_owned = False
    try:
        port = _resolve_port(args.port)
        command = build_benchmark_command(
            repo_root=repo_root,
            binary=binary,
            run_dir=benchmark_dir,
            fixture_root=fixture_root,
            build=args.build,
            port=port,
        )
        if args.dry_run:
            print(
                json.dumps(
                    _dry_run_payload(
                        suite_dir, benchmark_dir, fixture_root, command
                    ),
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return 0
        suite_dir.mkdir(parents=True, exist_ok=False)
        suite_owned = True
        exit_code, result = _execute_probe(
            args, suite_dir, benchmark_dir, fixture_root, binary, command
        )
    except (OSError, ValueError) as exc:
        result = {
            "decision": "infrastructure_error",
            "gate_reasons": [f"probe 启动失败：{exc}"],
            "stages": {},
        }
        if suite_owned:
            try:
                _persist_infrastructure_failure(suite_dir, fixture_root, result)
            except OSError as persist_exc:
                result["gate_reasons"].append(
                    f"probe 失败现场无法落盘：{persist_exc}"
                )
        _print_result(
            result,
            suite_dir,
            False,
            artifacts_persisted=(
                suite_owned
                and (suite_dir / "summary.json").is_file()
                and (suite_dir / "REPORT.md").is_file()
            ),
        )
        return 2
    _print_result(result, suite_dir, False)
    return exit_code
