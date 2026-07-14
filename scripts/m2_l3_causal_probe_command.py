"""Fixture preparation and frozen benchmark command for the M2 L3 probe."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from typing import Any

from m2_l3_causal_probe_config import (
    FIXTURE_ANCHOR_DIRS,
    PROBE_DURATION_SECS,
    PROBE_FILE_COUNT,
    PROBE_MIN_LEASE_REMAINING_SECS,
    PROBE_POST_CLEANUP_AUDIT_SECS,
    PROBE_PRECONDITION_WAIT_SECS,
    PROBE_ROTATING_TICK_SECS,
    PROBE_ROTATING_TTL_SECS,
    PROBE_SETTLE_SECS,
    PROBE_START_DELAY_SECS,
    PROBE_SUBTREE_DEPTH,
)


def _run_label(run_dir: Path) -> str:
    digest = hashlib.sha256(str(run_dir.resolve()).encode("utf-8")).hexdigest()[:12]
    return f"m2-l3-causal-{digest}"


def prepare_fixture(root: Path) -> dict[str, Any]:
    """Create a unique stable root whose watch cost cannot collapse to one entry."""
    if root.exists():
        raise FileExistsError(f"probe fixture already exists: {root}")
    root.mkdir(parents=True)
    digest = hashlib.sha256()
    for index in range(FIXTURE_ANCHOR_DIRS):
        relative = Path(f"anchor-{index:02d}") / "anchor.txt"
        content = f"fd-rdd M2 L3 causal probe anchor {index:02d}\n"
        path = root / relative
        path.parent.mkdir()
        path.write_text(content, encoding="utf-8")
        digest.update(relative.as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(content.encode("utf-8"))
    identity = {
        "schema_version": 1,
        "completed": True,
        "actual_file_count": FIXTURE_ANCHOR_DIRS + 1,
        "seed": 42,
        "layout_version": "m2-l3-causal-probe-v1",
        "anchor_dirs": FIXTURE_ANCHOR_DIRS,
        "anchor_files": FIXTURE_ANCHOR_DIRS,
        "content_sha256": digest.hexdigest(),
    }
    (root / ".fd-rdd-m2-fixture.json").write_text(
        json.dumps(identity, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return identity


def build_benchmark_command(
    *,
    repo_root: Path,
    binary: Path,
    run_dir: Path,
    fixture_root: Path,
    build: str,
    port: int,
) -> list[str]:
    """Build the frozen one-root command used by the causal probe."""
    if not 1 <= port <= 65535:
        raise ValueError(f"probe port must be between 1 and 65535: {port}")
    runner = repo_root / "scripts" / "m2-cold-window-vm-bench.py"
    return [
        sys.executable,
        str(runner),
        "--repo",
        str(repo_root),
        "--binary",
        str(binary),
        "--build",
        build,
        "--run-label",
        _run_label(run_dir),
        "--port",
        str(port),
        "--run-dir",
        str(run_dir),
        "--root",
        str(fixture_root),
        "--cold-roots",
        str(fixture_root),
        "--watch-mode",
        "tiered",
        "--runtime-profile",
        "default",
        "--tiered-profile",
        "balanced",
        "--duration-secs",
        str(PROBE_DURATION_SECS),
        "--sample-interval-secs",
        "2",
        "--process-sample-interval-secs",
        "0.5",
        "--snapshot-interval-secs",
        "3600",
        "--rotating-cold-window",
        "--rotating-budget",
        "1",
        "--rotating-tick-secs",
        str(PROBE_ROTATING_TICK_SECS),
        "--rotating-ttl-secs",
        str(PROBE_ROTATING_TTL_SECS),
        "--rotating-max-cost-per-root",
        "64",
        "--rotating-max-dirs-per-tick",
        "1",
        "--max-watch-dirs",
        "1",
        "--l0-max-cost-per-root",
        "1",
        "--l1-scan-interval-secs",
        "1",
        "--l2-scan-interval-secs",
        "2",
        "--l3-scan-interval-secs",
        "21600",
        "--l1-empty-scans-to-l2",
        "1",
        "--l2-empty-scans-to-l3",
        "1",
        "--fast-scan",
        "--proc-sampler",
        "--event-storm",
        "--event-storm-root",
        str(fixture_root),
        "--event-storm-kind",
        "subtree_rename",
        "--event-storm-target-tier",
        "L3",
        "--event-storm-ops",
        str(PROBE_FILE_COUNT),
        "--event-storm-file-count",
        str(PROBE_FILE_COUNT),
        "--event-storm-depth",
        str(PROBE_SUBTREE_DEPTH),
        "--event-storm-duration-budget-secs",
        "1",
        "--event-storm-start-delay-secs",
        str(PROBE_START_DELAY_SECS),
        "--event-storm-precondition-wait-secs",
        str(PROBE_PRECONDITION_WAIT_SECS),
        "--event-storm-min-lease-remaining-secs",
        str(PROBE_MIN_LEASE_REMAINING_SECS),
        "--event-storm-interval-secs",
        "300",
        "--event-storm-settle-secs",
        str(PROBE_SETTLE_SECS),
        "--event-storm-timeout-secs",
        "0",
        "--event-storm-max-bursts",
        "1",
        "--event-storm-visibility-probes-per-burst",
        str(PROBE_FILE_COUNT),
        "--event-storm-visibility-poll-interval-secs",
        "0.25",
        "--event-storm-post-cleanup-audit-secs",
        str(PROBE_POST_CLEANUP_AUDIT_SECS),
        "--event-storm-fixed-root-schedule",
        "--event-storm-deterministic-plan",
        "--event-storm-strict-protocol",
        "--snapshot-path-disk",
        "--workload-seed",
        "42",
        "--shutdown-timeout-secs",
        "120",
    ]
