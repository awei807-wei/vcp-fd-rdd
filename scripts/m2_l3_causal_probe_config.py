"""Frozen protocol constants for the focused M2 L3 causal probe."""

from pathlib import Path


PROBE_DURATION_SECS = 80
PROBE_START_DELAY_SECS = 0
PROBE_PRECONDITION_WAIT_SECS = 60
PROBE_MIN_LEASE_REMAINING_SECS = 18
PROBE_SETTLE_SECS = 5
PROBE_ROTATING_TICK_SECS = 45
PROBE_ROTATING_TTL_SECS = 20
PROBE_POST_CLEANUP_AUDIT_SECS = 22
PROBE_FILE_COUNT = 10
PROBE_SUBTREE_DEPTH = 3
PROBE_VISIBILITY_TOLERANCE_SECS = 1.0
FIXTURE_ANCHOR_DIRS = 16
PROBE_ALLOWED_AB_GATE_REASONS = frozenset(
    {"git_worktree_dirty", "artifact_provenance_unverified"}
)
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RUN_ROOT = Path("/tmp/fd-rdd-m2-runs")
