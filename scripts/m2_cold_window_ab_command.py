"""M2 一键 A/B 驱动的固定路径与命令定义。"""

from __future__ import annotations

import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
BENCH_RUNNER = REPO_ROOT / "scripts" / "m2-cold-window-vm-bench.py"
BINARY = REPO_ROOT / "target" / "release" / "fd-rdd"
RUN_ROOT = Path("/tmp/fd-rdd-m2-runs")
FIXTURE_DIR_NAME = "fd-rdd-m2-roots"
COLD_DIR_COUNT = 300
WORKLOAD_SEED = 42

VARIANTS = {
    "a": ("a_rotating", "--rotating-cold-window", True),
    "b": ("b_baseline", "--no-rotating-cold-window", False),
}


def treatment_args(variant: str) -> list[str]:
    return [VARIANTS[variant][1]]


def build_command(
    variant: str, run_dir: Path, roots: dict[str, Path]
) -> list[str]:
    cold_roots = f"{roots['cold-a']},{roots['cold-b']}"
    command = [
        sys.executable,
        str(BENCH_RUNNER),
        "--repo", str(REPO_ROOT),
        "--binary", str(BINARY),
        "--build", "always",
        "--run-label", run_dir.name,
        "--run-dir", str(run_dir),
        "--root", str(roots["cold-a"]),
        "--root", str(roots["cold-b"]),
        "--root", str(roots["hot"]),
        "--hot-roots", str(roots["hot"]),
        "--cold-roots", cold_roots,
        "--watch-mode", "tiered",
        "--runtime-profile", "default",
        "--tiered-profile", "balanced",
        "--duration-secs", "3600",
        "--sample-interval-secs", "10",
        "--process-sample-interval-secs", "0.5",
        "--snapshot-interval-secs", "300",
        "--rotating-budget", "128",
        "--rotating-tick-secs", "30",
        "--rotating-ttl-secs", "180",
        "--rotating-max-cost-per-root", "64",
        "--rotating-max-dirs-per-tick", "8",
        "--max-watch-dirs", "8",
        "--l0-max-cost-per-root", "1",
        "--l1-scan-interval-secs", "5",
        "--l2-scan-interval-secs", "60",
        "--l3-scan-interval-secs", "21600",
        "--l1-empty-scans-to-l2", "1",
        "--l2-empty-scans-to-l3", "2",
        "--fast-scan",
        "--proc-sampler",
        "--canary-root", str(roots["hot"]),
        "--canary-interval-secs", "120",
        "--canary-timeout-secs", "90",
        "--passive-canary-root", str(roots["cold-a"]),
        "--passive-canary-start-delay-secs", "180",
        "--passive-canary-interval-secs", "180",
        "--passive-canary-settle-secs", "120",
        "--passive-canary-timeout-secs", "0",
        "--event-storm",
        "--event-storm-root", str(roots["cold-a"]),
        "--event-storm-root", str(roots["cold-b"]),
        "--event-storm-root", str(roots["hot"]),
        "--event-storm-kind",
        "rw100,save100,git_clone,npm_install,subtree_rename,mount_storm,inode_reuse,time_skew",
        "--event-storm-target-tier", "L0,L1,L2,L3",
        "--event-storm-ops", "100",
        "--event-storm-duration-budget-secs", "1",
        "--event-storm-start-delay-secs", "180",
        "--event-storm-interval-secs", "180",
        "--event-storm-settle-secs", "120",
        "--event-storm-immediate-query",
        "--immediate-query-settle-secs", "5",
        "--snapshot-path-disk",
        "--workload-seed", str(WORKLOAD_SEED),
        "--shutdown-timeout-secs", "300",
    ]
    command.extend(treatment_args(variant))
    return command
