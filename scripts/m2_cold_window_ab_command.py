"""M2 一键 A/B 驱动的固定路径与命令定义。"""

from __future__ import annotations

import hashlib
import sys
from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
BENCH_RUNNER = REPO_ROOT / "scripts" / "m2-cold-window-vm-bench.py"
BINARY = REPO_ROOT / "target" / "release" / "fd-rdd"
RUN_ROOT = Path("/tmp/fd-rdd-m2-runs")
FIXTURE_DIR_NAME = "fd-rdd-m2-roots"
COLD_DIR_COUNT = 300
WORKLOAD_SEED = 42
BASE_ROOT_NAMES = ("cold-a", "cold-b", "hot")
FALSIFICATION_EVENT_ROOT_NAMES = tuple(
    f"cold-storm-{index:02d}" for index in range(1, 7)
)
FIXTURE_ROOT_NAMES = (*BASE_ROOT_NAMES, *FALSIFICATION_EVENT_ROOT_NAMES)

VARIANTS = {
    "a": ("a_rotating", "--rotating-cold-window", True),
    "b": ("b_baseline", "--no-rotating-cold-window", False),
}


@dataclass(frozen=True)
class BenchmarkProfile:
    """单腿 benchmark 中除 treatment 外必须冻结的协议参数。"""

    build: str
    duration_secs: int
    event_root_names: tuple[str, ...]
    passive_root_name: str
    event_kinds: str
    target_tiers: str
    event_start_delay_secs: int
    event_interval_secs: int
    event_settle_secs: int
    immediate_query: bool
    active_canary: bool
    fixed_root_schedule: bool
    deterministic_event_plan: bool
    strict_protocol: bool
    max_bursts: int = 0
    visibility_probes_per_burst: int = 0
    visibility_poll_interval_secs: float = 1.0


PROFILES = {
    "standard": BenchmarkProfile(
        build="always",
        duration_secs=3600,
        event_root_names=("cold-a", "cold-b", "hot"),
        passive_root_name="cold-a",
        event_kinds=(
            "rw100,save100,git_clone,npm_install,subtree_rename,"
            "mount_storm,inode_reuse,time_skew"
        ),
        target_tiers="L0,L1,L2,L3",
        event_start_delay_secs=180,
        event_interval_secs=10,
        event_settle_secs=60,
        immediate_query=True,
        active_canary=True,
        fixed_root_schedule=False,
        deterministic_event_plan=False,
        strict_protocol=False,
    ),
    "falsification": BenchmarkProfile(
        build="never",
        duration_secs=1200,
        event_root_names=FALSIFICATION_EVENT_ROOT_NAMES,
        passive_root_name="cold-b",
        event_kinds="save100,git_clone,subtree_rename",
        target_tiers="L3",
        event_start_delay_secs=240,
        # Each burst uses a distinct registered root, so strict L3 preflight
        # never depends on a previously mutated root demoting back from L2.
        event_interval_secs=30,
        event_settle_secs=120,
        immediate_query=False,
        active_canary=False,
        fixed_root_schedule=True,
        deterministic_event_plan=True,
        strict_protocol=True,
        max_bursts=6,
        visibility_probes_per_burst=8,
        visibility_poll_interval_secs=1.0,
    ),
}


def treatment_args(variant: str) -> list[str]:
    return [VARIANTS[variant][1]]


def _run_label(run_dir: Path) -> str:
    digest = hashlib.sha256(str(run_dir.resolve()).encode("utf-8")).hexdigest()[:12]
    return f"{run_dir.name[:48]}-{digest}"


def _base_args(
    run_dir: Path,
    roots: dict[str, Path],
    profile: BenchmarkProfile,
    binary: Path,
) -> list[str]:
    daemon_root_names = tuple(
        dict.fromkeys((*BASE_ROOT_NAMES, *profile.event_root_names))
    )
    cold_root_names = tuple(
        dict.fromkeys(
            (
                "cold-a",
                "cold-b",
                *(name for name in profile.event_root_names if name != "hot"),
            )
        )
    )
    args = [
        sys.executable,
        str(BENCH_RUNNER),
        "--repo",
        str(REPO_ROOT),
        "--binary",
        str(binary),
        "--build",
        profile.build,
        "--run-label",
        _run_label(run_dir),
        "--run-dir",
        str(run_dir),
    ]
    for root_name in daemon_root_names:
        args.extend(("--root", str(roots[root_name])))
    args.extend(
        [
            "--hot-roots",
            str(roots["hot"]),
            "--cold-roots",
            ",".join(str(roots[name]) for name in cold_root_names),
            "--watch-mode",
            "tiered",
            "--runtime-profile",
            "default",
            "--tiered-profile",
            "balanced",
            "--duration-secs",
            str(profile.duration_secs),
            "--sample-interval-secs",
            "10",
            "--process-sample-interval-secs",
            "0.5",
            "--snapshot-interval-secs",
            "300",
        ]
    )
    return args


def _tiered_args() -> list[str]:
    return [
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
    ]


def _canary_args(
    roots: dict[str, Path],
    profile: BenchmarkProfile,
) -> list[str]:
    args: list[str] = []
    if profile.active_canary:
        args.extend(
            (
                "--canary-root",
                str(roots["hot"]),
                "--canary-interval-secs",
                "120",
                "--canary-timeout-secs",
                "90",
            )
        )
    args.extend(
        (
            "--passive-canary-root",
            str(roots[profile.passive_root_name]),
            "--passive-canary-start-delay-secs",
            "180",
            "--passive-canary-interval-secs",
            "180",
            "--passive-canary-settle-secs",
            "120",
            "--passive-canary-timeout-secs",
            "0",
        )
    )
    return args


def _event_storm_args(
    roots: dict[str, Path],
    profile: BenchmarkProfile,
) -> list[str]:
    args = ["--event-storm"]
    for root_name in profile.event_root_names:
        args.extend(("--event-storm-root", str(roots[root_name])))
    args.extend(
        [
            "--event-storm-kind", profile.event_kinds,
            "--event-storm-target-tier", profile.target_tiers,
            "--event-storm-ops", "100",
            "--event-storm-duration-budget-secs", "1",
            "--event-storm-start-delay-secs", str(profile.event_start_delay_secs),
            "--event-storm-interval-secs", str(profile.event_interval_secs),
            "--event-storm-settle-secs", str(profile.event_settle_secs),
        ]
    )
    if profile.immediate_query:
        args.extend(("--event-storm-immediate-query", "--immediate-query-settle-secs", "5"))
    if profile.max_bursts > 0:
        args.extend(("--event-storm-max-bursts", str(profile.max_bursts)))
    if profile.visibility_probes_per_burst > 0:
        args.extend(
            (
                "--event-storm-visibility-probes-per-burst",
                str(profile.visibility_probes_per_burst),
                "--event-storm-visibility-poll-interval-secs",
                f"{profile.visibility_poll_interval_secs:g}",
            )
        )
    if profile.fixed_root_schedule:
        args.append("--event-storm-fixed-root-schedule")
    if profile.deterministic_event_plan:
        args.append("--event-storm-deterministic-plan")
    if profile.strict_protocol:
        args.append("--event-storm-strict-protocol")
    return args


def build_command(
    variant: str,
    run_dir: Path,
    roots: dict[str, Path],
    profile: str = "standard",
    artifact_provenance_receipt: Path | None = None,
    binary: Path = BINARY,
) -> list[str]:
    selected = PROFILES[profile]
    command = _base_args(run_dir, roots, selected, binary)
    if artifact_provenance_receipt is not None:
        command.extend(
            (
                "--artifact-provenance-receipt",
                str(artifact_provenance_receipt),
            )
        )
    command.extend(_tiered_args())
    command.extend(_canary_args(roots, selected))
    command.extend(_event_storm_args(roots, selected))
    command.extend(
        (
            "--snapshot-path-disk",
            "--workload-seed",
            str(WORKLOAD_SEED),
            "--shutdown-timeout-secs",
            "300",
        )
    )
    command.extend(treatment_args(variant))
    return command
