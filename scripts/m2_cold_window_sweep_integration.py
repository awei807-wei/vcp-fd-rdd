#!/usr/bin/env python3
"""Run the accelerated sweep proof without mutating the immutable r9 suite."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from m2_cold_window_ab_command import REPO_ROOT, RUN_ROOT
from m2_cold_window_build_receipt import (
    git_head_sha,
    git_worktree_dirty,
)
from m2_cold_window_falsification import (
    SweepIntegrationSpec,
)
from m2_cold_window_sweep_integration_process import run_fixture_process


PERF_FIXTURE = REPO_ROOT / "scripts" / "m2_perf_fixture.py"
BUILD_RECEIPT_NAME = "build-provenance.json"
PRODUCT_BINARY_RELATIVE = Path("build-target/release/fd-rdd")
RESULT_NAME = "sweep-integration-result.json"
SUMMARY_NAME = "integration-summary.json"
MANIFEST_NAME = "manifest.json"
EVIDENCE_MANIFEST_NAME = "evidence-manifest.json"
CHECKSUMS_NAME = "SHA256SUMS"
PROBE_EVENTS_NAME = "sweep-probe-events.jsonl"
RAW_ATTEMPT_FILES = (
    RESULT_NAME,
    "fixture-report.json",
    "FIXTURE-REPORT.md",
    "fd-rdd.log",
    "process-samples.jsonl",
    "metrics-samples.jsonl",
    PROBE_EVENTS_NAME,
    "config-home/fd-rdd/config.toml",
)
READY_MARKERS = ("fd-rdd ready.", "HTTP Query Server listening")


class IntegrationEvidenceError(RuntimeError):
    """Raised when independent evidence reconstruction fails closed."""


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def integration_spec(output_dir: Path) -> SweepIntegrationSpec:
    return SweepIntegrationSpec(output_dir / "attempts")


def protocol_fingerprint(spec: SweepIntegrationSpec) -> str:
    payload = {
        "schema": 1,
        "cycles": spec.cycles,
        "port": spec.port,
        "rotating_ttl_secs": spec.rotating_ttl_secs,
        "rotating_tick_secs": spec.rotating_tick_secs,
        "rotating_full_sweep_period_secs": (
            spec.rotating_full_sweep_period_secs
        ),
        "settle_secs": spec.settle_secs,
        "repair_deadline_secs": spec.repair_deadline_secs,
        "calibrate": False,
        "allow_cycle_shortfall": True,
        "burst": True,
        "burst_root_level": True,
        "deep_modify_probe": True,
        "sweep_completion_fence": {
            "enabled": True,
            "endpoint": "/debug/tiered-watch",
            "predicate": (
                "last_scan_seq>post_mutation_last_scan_seq && "
                "last_scan_cycle_id>post_mutation_cycle_id"
            ),
            "timeout_formula": "period+ttl+2*tick+30",
            "first_search": "once_after_fence",
        },
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return hashlib.sha256(encoded).hexdigest()


def fixture_command(
    spec: SweepIntegrationSpec,
    attempt: Path,
    binary: Path,
) -> list[str]:
    return [
        sys.executable,
        str(PERF_FIXTURE),
        "--repo",
        str(REPO_ROOT),
        "--binary",
        str(binary),
        "--build",
        "never",
        "--run-dir",
        str(attempt),
        "--port",
        str(spec.port),
        "--cycles",
        str(spec.cycles),
        "--allow-cycle-shortfall",
        "--rotating-ttl-secs",
        str(spec.rotating_ttl_secs),
        "--rotating-tick-secs",
        str(spec.rotating_tick_secs),
        "--rotating-full-sweep-period-secs",
        str(spec.rotating_full_sweep_period_secs),
        "--settle-secs",
        f"{spec.settle_secs:g}",
        "--no-calibrate",
        "--burst",
        "--burst-root-level",
        "--deep-modify-probe",
        "--sweep-completion-fence",
        "--integration-repair-deadline-secs",
        f"{spec.repair_deadline_secs:g}",
    ]


def _attempt_number(path: Path) -> int:
    try:
        return int(path.name.removeprefix("attempt-"))
    except ValueError:
        return -1


def attempt_dirs(base_dir: Path) -> list[Path]:
    return sorted(
        (path for path in base_dir.glob("attempt-*") if path.is_dir()),
        key=_attempt_number,
    )


def completed_terminal_attempt(base_dir: Path) -> Path | None:
    """Any completed process is immutable terminal evidence, even when it failed."""
    for attempt in reversed(attempt_dirs(base_dir)):
        result = _read_json(attempt / RESULT_NAME)
        if result.get("schema") == 1 and result.get("status") == "completed":
            return attempt
    return None


def next_attempt(base_dir: Path) -> Path:
    number = max((_attempt_number(path) for path in attempt_dirs(base_dir)), default=0)
    return base_dir / f"attempt-{number + 1:02d}"


def _write_running_manifest(
    output_dir: Path,
    source_suite: Path,
    identity: dict[str, Any],
    harness_sha: str,
    harness_dirty: bool,
    fingerprint: str,
) -> None:
    _atomic_json(
        output_dir / MANIFEST_NAME,
        {
            "schema": 1,
            "kind": "m2-sweep-integration-only",
            "run_state": "running",
            "source_suite": str(source_suite),
            "source_suite_immutable": True,
            "product_git_sha": identity["product_git_sha"],
            "product_binary_sha256": identity["product_binary_sha256"],
            "product_receipt_sha256": identity["product_receipt_sha256"],
            "harness_git_sha": harness_sha,
            "harness_worktree_dirty": harness_dirty,
            "protocol_fingerprint": fingerprint,
        },
    )


def run_integration_only(output_dir: Path, source_suite: Path) -> int:
    from m2_cold_window_sweep_integration_evidence import (
        product_identity,
        recompute_summary,
        write_evidence_bundle,
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    source_suite = source_suite.expanduser().resolve()
    identity = product_identity(source_suite)
    spec = integration_spec(output_dir)
    fingerprint = protocol_fingerprint(spec)
    harness_sha = git_head_sha(REPO_ROOT)
    harness_dirty = git_worktree_dirty(REPO_ROOT)
    if not harness_sha or harness_dirty is None:
        raise IntegrationEvidenceError("harness Git identity unavailable")
    _write_running_manifest(
        output_dir,
        source_suite,
        identity,
        harness_sha,
        harness_dirty,
        fingerprint,
    )
    attempt = completed_terminal_attempt(spec.base_dir)
    if attempt is None:
        attempt = next_attempt(spec.base_dir)
        run_fixture_process(
            fixture_command(spec, attempt, Path(identity["binary_path"])),
            attempt,
            product_git_sha=identity["product_git_sha"],
            product_binary_sha256=identity["product_binary_sha256"],
            product_receipt_sha256=identity["product_receipt_sha256"],
            harness_git_sha=harness_sha,
            harness_worktree_dirty=harness_dirty,
            source_suite=source_suite,
            expected_protocol_fingerprint=fingerprint,
        )
    summary = recompute_summary(output_dir, source_suite, attempt)
    _atomic_json(output_dir / SUMMARY_NAME, summary)
    bundle = output_dir.with_name(output_dir.name + "-evidence.tar.gz")
    manifest = _read_json(output_dir / MANIFEST_NAME)
    manifest.update(
        {
            "run_state": "completed",
            "decision": summary["decision"],
            "attempt": str(attempt),
            "evidence_bundle": str(bundle),
            "reasons": summary["reasons"],
        }
    )
    _atomic_json(output_dir / MANIFEST_NAME, manifest)
    try:
        bundle = write_evidence_bundle(output_dir, source_suite, attempt)
    except Exception as exc:
        bundle.unlink(missing_ok=True)
        manifest.update(
            {
                "run_state": "failed",
                "decision": "fail",
                "evidence_bundle": "",
                "reasons": [*summary["reasons"], f"evidence bundle failed: {exc}"],
            }
        )
        _atomic_json(output_dir / MANIFEST_NAME, manifest)
        raise
    return 0 if summary["decision"] == "pass" else 2


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the independent M2 short-sweep integration proof"
    )
    parser.add_argument("--source-suite", type=Path, required=True)
    parser.add_argument("--run-dir", type=Path, default=None)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    output_dir = (
        args.run_dir.expanduser().resolve()
        if args.run_dir is not None
        else RUN_ROOT / f"{_utc_stamp()}_m2_sweep_integration"
    )
    source_suite = args.source_suite.expanduser().resolve()
    if output_dir == source_suite or source_suite in output_dir.parents:
        print("integration output must not modify the source r9 suite", file=sys.stderr)
        return 1
    if args.dry_run:
        spec = integration_spec(output_dir)
        print("source_suite:", source_suite)
        print("output_dir:", output_dir)
        print("command:", " ".join(fixture_command(spec, next_attempt(spec.base_dir), source_suite / PRODUCT_BINARY_RELATIVE)))
        return 0
    try:
        return run_integration_only(output_dir, source_suite)
    except IntegrationEvidenceError as exc:
        print(f"M2 sweep integration failed closed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
