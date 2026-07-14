#!/usr/bin/env python3
"""
Start fd-rdd in an isolated VM benchmark run directory and collect runtime metrics.

This script is intentionally orchestration-only:
- fd-rdd still writes its built-in JSONL metrics under <run-dir>/reports/metrics/.
- The script adds endpoint snapshots, /proc process samples, optional search canaries,
  and a single-run summary/report so A/B runs can be compared later.
"""

from __future__ import annotations

import argparse
import bisect
import errno
import hashlib
import json
import os
import random
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from m2_cold_window_build_receipt import (
    CARGO_ARGS as BUILD_CARGO_ARGS,
    compiler_artifact_path,
    load_build_receipt,
    validate_build_receipt,
)


ENDPOINTS = ["/health", "/status", "/metrics", "/memory", "/watch-state"]

AB_PARAMETER_FINGERPRINT_SCHEMA = 2
INITIAL_STATE_FINGERPRINT_SCHEMA = 1
EXECUTION_FINGERPRINT_SCHEMA = 3
PASSIVE_SHUTDOWN_RECONCILE_TIMEOUT_SECS = 30.0
PASSIVE_SHUTDOWN_RECONCILE_MAX_ATTEMPTS = 3
PASSIVE_SHUTDOWN_RECONCILE_RETRY_INTERVAL_SECS = 0.1
SHUTDOWN_SNAPSHOT_QUIESCE_RETRY_INTERVAL_SECS = 0.25
SHUTDOWN_SNAPSHOT_READY_CONFIRMATIONS = 2
AB_PARAMETER_FINGERPRINT_IGNORED_ARGS = frozenset(
    {
        # A/B legs intentionally differ in checkout/artifact identity and output
        # location. These remain fully recorded in runner_args, but do not make
        # otherwise identical workloads compare unequal.
        "repo",
        "binary",
        "run_label",
        "run_dir",
        "port",
        "sweep_config",
        "artifact_provenance_receipt",
    }
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def json_line(path: Path, record: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False, separators=(",", ":"))
        f.write("\n")


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    """Durably replace a JSON document without exposing a truncated manifest."""
    path.parent.mkdir(parents=True, exist_ok=True)
    next_path = path.with_name(path.name + ".next")
    try:
        with next_path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(next_path, path)
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        dir_fd = os.open(path.parent, flags)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    finally:
        next_path.unlink(missing_ok=True)


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def toml_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = int(round((len(ordered) - 1) * pct / 100.0))
    return float(ordered[max(0, min(idx, len(ordered) - 1))])


def first_query_correct(record: dict[str, Any]) -> bool:
    """Return semantic correctness only for completed first-query requests."""
    if not first_query_transport_ok(record):
        return False
    if "first_query_exists" in record and "should_exist" in record:
        return bool(record.get("first_query_exists")) == bool(record.get("should_exist"))
    return bool(record.get("ok"))


def first_query_transport_ok(record: dict[str, Any]) -> bool:
    """Return whether a first-query request completed without a transport error."""
    if "transport_ok" in record:
        return bool(record.get("transport_ok"))
    return not bool(record.get("error"))


def short_uds_socket_path(run_dir: Path) -> Path:
    """Build a stable AF_UNIX path that stays well below Linux SUN_LEN."""
    digest = hashlib.sha256(os.fsencode(run_dir.resolve())).hexdigest()[:16]
    return Path("/tmp") / f"fd-rdd-{digest}.sock"


def cleanup_uds_socket(socket_path: Path) -> None:
    """Remove a stale or stopped daemon socket without masking run teardown."""
    socket_path.unlink(missing_ok=True)


def _json_manifest_value(value: Any) -> Any:
    """Convert argparse values to deterministic JSON without dropping fields."""
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {
            str(key): _json_manifest_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_json_manifest_value(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def manifest_runner_args(args: argparse.Namespace) -> dict[str, Any]:
    """Return every effective argparse field in a stable, JSON-safe mapping."""
    return {
        key: _json_manifest_value(value)
        for key, value in sorted(vars(args).items())
    }


def ab_parameter_fingerprint_inputs(runner_args: dict[str, Any]) -> dict[str, Any]:
    """Build the auditable payload used to decide whether two legs are comparable."""
    comparable_args = {
        key: value
        for key, value in sorted(runner_args.items())
        if key not in AB_PARAMETER_FINGERPRINT_IGNORED_ARGS
    }
    return {
        "schema": AB_PARAMETER_FINGERPRINT_SCHEMA,
        "runner_args": comparable_args,
    }


def ab_parameter_fingerprint(runner_args: dict[str, Any]) -> str:
    """Hash all workload-affecting parameters while excluding A/B leg identity."""
    payload = ab_parameter_fingerprint_inputs(runner_args)
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def git_head_sha(repo: Path) -> str:
    """Return the exact commit under test, or an empty string with no false guess."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo,
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    sha = result.stdout.strip().lower()
    if result.returncode == 0 and len(sha) == 40 and all(ch in "0123456789abcdef" for ch in sha):
        return sha
    return ""


def git_worktree_dirty(repo: Path) -> bool | None:
    """Return whether tracked or untracked worktree content differs from HEAD."""
    try:
        result = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=normal"],
            cwd=repo,
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    return bool(result.stdout.strip())


def _deduplicated_fixture_roots(roots: list[Path]) -> list[Path]:
    resolved = sorted(
        {root.expanduser().resolve() for root in roots},
        key=lambda path: (len(path.parts), str(path)),
    )
    selected: list[Path] = []
    for candidate in resolved:
        if any(candidate == parent or candidate.is_relative_to(parent) for parent in selected):
            continue
        selected.append(candidate)
    return selected


def _fixture_json_declaration(root: Path) -> dict[str, Any] | None:
    path = root / ".fd-rdd-m2-fixture.json"
    if not path.exists():
        return None
    try:
        raw = path.read_bytes()
        data = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        return {"trusted": False, "error": f"{path}: {exc!r}"}
    if not isinstance(data, dict):
        return {"trusted": False, "error": f"{path}: fixture manifest must be an object"}
    count = next(
        (
            data.get(key)
            for key in ("actual_file_count", "actual_files", "file_count", "total_files")
            if data.get(key) is not None
        ),
        None,
    )
    if data.get("completed") is not True:
        return {"trusted": False, "error": f"{path}: fixture manifest is not completed"}
    try:
        file_count = int(count)
    except (TypeError, ValueError):
        return {"trusted": False, "error": f"{path}: missing valid file count"}
    if file_count < 0:
        return {"trusted": False, "error": f"{path}: negative file count"}
    return {
        "trusted": True,
        "verified": True,
        "source": "fixture_manifest",
        "file_count": file_count,
        "seed": data.get("seed"),
        "layout_version": data.get("layout_version", data.get("schema_version", "")),
        "content_sha256": data.get("content_sha256", ""),
        "declaration_sha256": hashlib.sha256(raw).hexdigest(),
        "path": str(path),
    }


def _fixture_text_declaration(root: Path) -> dict[str, Any] | None:
    path = root / ".fd-rdd-m2-fixture"
    if not path.exists():
        return None
    try:
        raw = path.read_bytes()
        text = raw.decode("utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        return {"trusted": False, "error": f"{path}: {exc!r}"}
    lines = text.splitlines()
    if not lines or lines[0].strip() != "fd-rdd m2 realistic fixture":
        return {"trusted": False, "error": f"{path}: unrecognized fixture marker"}
    fields: dict[str, str] = {}
    for line in lines[1:]:
        key, separator, value = line.partition("=")
        if separator:
            fields[key.strip()] = value.strip()
    try:
        file_count = int(fields["total_files"])
        seed = int(fields["seed"])
    except (KeyError, ValueError):
        return {"trusted": False, "error": f"{path}: invalid seed/total_files declaration"}
    if file_count <= 0:
        return {"trusted": False, "error": f"{path}: non-positive total_files"}
    return {
        "trusted": True,
        # The legacy builder writes this declaration before generation. It is a
        # trusted workload declaration, not an exhaustive post-build verification.
        "verified": False,
        "source": "fixture_marker_declared",
        "file_count": file_count,
        "seed": seed,
        "layout_version": "legacy-marker-v1",
        "declaration_sha256": hashlib.sha256(raw).hexdigest(),
        "path": str(path),
    }


def load_fixture_identity(roots: list[Path]) -> dict[str, Any]:
    """Read small fixture declarations only; never traverse the indexed tree."""
    declarations: list[dict[str, Any]] = []
    errors: list[str] = []
    for root in _deduplicated_fixture_roots(roots):
        declaration = _fixture_json_declaration(root)
        if declaration is None:
            declaration = _fixture_text_declaration(root)
        if declaration is None:
            errors.append(f"{root}: fixture declaration not found")
            continue
        if not declaration.get("trusted"):
            errors.append(str(declaration.get("error", f"{root}: untrusted declaration")))
            continue
        declarations.append({"root": str(root), **declaration})

    trusted = bool(declarations) and not errors
    sources = {str(item["source"]) for item in declarations}
    source = next(iter(sources)) if len(sources) == 1 else "mixed_fixture_declarations"
    file_count = (
        sum(int(item["file_count"]) for item in declarations) if trusted else None
    )
    seeds = {item.get("seed") for item in declarations}
    seed = next(iter(seeds)) if len(seeds) == 1 else None
    identity_payload = [
        {
            "root": item["root"],
            "source": item["source"],
            "file_count": item["file_count"],
            "seed": item.get("seed"),
            "layout_version": item.get("layout_version", ""),
            "content_sha256": item.get("content_sha256", ""),
            "declaration_sha256": item["declaration_sha256"],
        }
        for item in declarations
    ]
    identity_sha256 = hashlib.sha256(
        json.dumps(identity_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return {
        "file_count": file_count,
        "count_source": source if trusted else "unverified",
        "count_verified": trusted and all(bool(item.get("verified")) for item in declarations),
        "trusted": trusted,
        "seed": seed,
        "identity_sha256": identity_sha256,
        "roots": declarations,
        "errors": errors,
    }


def sha256_file(path: Path) -> str:
    """Hash an artifact in bounded chunks for an auditable binary identity."""
    digest = hashlib.sha256()
    try:
        with path.open("rb") as f:
            while chunk := f.read(1024 * 1024):
                digest.update(chunk)
    except FileNotFoundError:
        return ""
    return digest.hexdigest()


def snapshot_initial_state(snapshot_path: Path) -> dict[str, Any]:
    """Inspect fixed recovery paths without reading snapshot contents or walking dirs."""
    state_dir = snapshot_path.with_suffix(".d")
    candidates = {
        "primary": snapshot_path,
        "legacy_v7": snapshot_path.with_suffix(".v7"),
        "stable": state_dir / "stable.v7",
        "stable_prev": state_dir / "stable.prev.v7",
        "stable_next": state_dir / "stable.next.v7",
        "runtime_state": state_dir / "runtime-state.json",
        "events_wal": state_dir / "events.wal",
    }
    artifacts: dict[str, dict[str, Any]] = {}
    errors: list[str] = []
    try:
        parent_device = snapshot_path.parent.stat().st_dev
    except OSError as exc:
        parent_device = None
        errors.append(f"{snapshot_path.parent}: {exc!r}")
    for label, path in candidates.items():
        try:
            stat = path.stat()
            artifacts[label] = {"exists": True, "size_bytes": stat.st_size}
        except FileNotFoundError:
            artifacts[label] = {"exists": False, "size_bytes": 0}
        except OSError as exc:
            artifacts[label] = {"exists": False, "size_bytes": 0}
            errors.append(f"{path}: {exc!r}")
    try:
        state_dir_exists = state_dir.exists()
    except OSError as exc:
        state_dir_exists = False
        errors.append(f"{state_dir}: {exc!r}")
    fresh = not state_dir_exists and not any(
        bool(item["exists"]) for item in artifacts.values()
    )
    return {
        "fresh": fresh,
        "state_dir_exists": state_dir_exists,
        "parent_device": parent_device,
        "artifacts": artifacts,
        "errors": errors,
    }


def build_initial_state(
    snapshot_path: Path,
    fixture: dict[str, Any],
    *,
    rust_log: str,
    binary: Path,
    git_sha: str,
    git_dirty: bool | None,
    artifact_provenance: dict[str, Any],
    run_dir_preexisting: bool = False,
) -> dict[str, Any]:
    snapshot = snapshot_initial_state(snapshot_path)
    collection_errors = [
        *[str(error) for error in fixture.get("errors", [])],
        *[str(error) for error in snapshot.get("errors", [])],
    ]
    return {
        "fixture": fixture,
        "snapshot": snapshot,
        "rust_log": rust_log,
        "binary_sha256": sha256_file(binary),
        "git_sha": git_sha,
        "git_dirty": git_dirty,
        "artifact_provenance": artifact_provenance,
        "run_dir_preexisting": run_dir_preexisting,
        "collection_errors": collection_errors,
    }


def _fingerprint(payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def initial_state_fingerprint(initial_state: dict[str, Any]) -> str:
    # Code identity is audited separately because A/B legs intentionally run
    # different binaries. This fingerprint covers the state that must match.
    return _fingerprint(
        {
            "schema": INITIAL_STATE_FINGERPRINT_SCHEMA,
            "fixture": initial_state.get("fixture"),
            "snapshot": initial_state.get("snapshot"),
            "rust_log": initial_state.get("rust_log"),
            "run_dir_preexisting": initial_state.get("run_dir_preexisting", False),
            "collection_errors": initial_state.get("collection_errors", []),
        }
    )


def _log_contains(path: Path, needle: str) -> bool:
    try:
        with path.open("r", encoding="utf-8", errors="replace") as f:
            return any(needle in line for line in f)
    except FileNotFoundError:
        return False


def build_execution_state(
    run_dir: Path,
    *,
    requested_duration_secs: int,
    actual_duration_secs: float,
    exit_code: int | None,
    fatal_error: str,
    process_sampler_error: str,
    completion_reason: str,
    cleanup_errors: list[str],
    shutdown_signal_elapsed_secs: float | None = None,
    event_storm_enabled: bool = False,
    mixed_workload_enabled: bool = False,
    passive_canary_enabled: bool = False,
    shutdown_snapshot_quiesce_required: bool = False,
) -> dict[str, Any]:
    """Summarize realized workload and harness health after one benchmark leg."""
    event_rows = read_jsonl(run_dir / "event-storm-samples.jsonl")
    hot_rows = read_jsonl(run_dir / "hot-churn-samples.jsonl")
    endpoint_rows = read_jsonl(run_dir / "endpoint-samples.jsonl")
    process_rows = read_jsonl(run_dir / "process-samples.jsonl")
    canary_rows = read_jsonl(run_dir / "canary-samples.jsonl")
    shutdown_rows = read_jsonl(run_dir / "shutdown-samples.jsonl")
    passive_shutdown_reconcile_rows = [
        row
        for row in canary_rows
        if row.get("operation") == "passive_shutdown_reconcile"
    ]
    passive_shutdown_reconcile_ok = sum(
        1
        for row in passive_shutdown_reconcile_rows
        if row.get("ok") is True and row.get("stable") is True
    )
    passive_shutdown_reconcile_failures = (
        len(passive_shutdown_reconcile_rows) - passive_shutdown_reconcile_ok
    )
    snapshot_quiesce_rows = [
        row
        for row in shutdown_rows
        if row.get("operation") == "shutdown_snapshot_quiesce"
    ]
    snapshot_quiesce_ok = sum(
        1
        for row in snapshot_quiesce_rows
        if row.get("ok") is True and row.get("ready") is True
    )
    snapshot_quiesce_failures = len(snapshot_quiesce_rows) - snapshot_quiesce_ok
    burst_rows = [row for row in event_rows if row.get("event_kind") == "burst_written"]
    burst_write_failures = sum(
        1 for row in event_rows if row.get("event_kind") == "burst_write_failed"
    )
    cleanup_rows = [
        row for row in event_rows if row.get("event_kind") == "burst_cleanup"
    ]
    cleanup_failures = sum(1 for row in cleanup_rows if row.get("ok") is not True)
    unsupported_workloads = sum(
        1 for row in event_rows if row.get("event_kind") == "unsupported_workload"
    )
    hot_churn_errors = sum(
        1 for row in hot_rows if row.get("event_kind") == "hot_churn_error"
    )
    final_snapshot_failed = _log_contains(
        run_dir / "fd-rdd.log", "Final snapshot failed:"
    )
    bursts_by_kind: dict[str, int] = {}
    events_by_kind: dict[str, int] = {}
    for row in burst_rows:
        kind = str(row.get("selected_kind", "unknown"))
        bursts_by_kind[kind] = bursts_by_kind.get(kind, 0) + 1
        events_by_kind[kind] = events_by_kind.get(kind, 0) + int(
            row.get("events_total", 0) or 0
        )
    payload: dict[str, Any] = {
        "schema": EXECUTION_FINGERPRINT_SCHEMA,
        "requested_duration_secs": int(requested_duration_secs),
        "actual_duration_secs": round(float(actual_duration_secs), 3),
        "duration_completed": (
            requested_duration_secs > 0
            and completion_reason == "duration_elapsed"
            and actual_duration_secs >= requested_duration_secs
        ),
        "completion_reason": completion_reason,
        "exit_code": exit_code,
        "fatal_error": fatal_error,
        "process_sampler_error": process_sampler_error,
        "cleanup_errors": list(cleanup_errors),
        "shutdown_signal_elapsed_secs": shutdown_signal_elapsed_secs,
        "passive_canary_enabled": passive_canary_enabled,
        "passive_shutdown_reconcile_count": len(
            passive_shutdown_reconcile_rows
        ),
        "passive_shutdown_reconcile_ok": passive_shutdown_reconcile_ok,
        "passive_shutdown_reconcile_failures": (
            passive_shutdown_reconcile_failures
        ),
        "passive_shutdown_reconcile_missing": (
            passive_canary_enabled and not passive_shutdown_reconcile_rows
        ),
        "passive_shutdown_reconcile_failed": (
            passive_canary_enabled
            and bool(passive_shutdown_reconcile_rows)
            and passive_shutdown_reconcile_failures > 0
        ),
        "shutdown_snapshot_quiesce_required": (
            shutdown_snapshot_quiesce_required
        ),
        "shutdown_snapshot_quiesce_count": len(snapshot_quiesce_rows),
        "shutdown_snapshot_quiesce_ok": snapshot_quiesce_ok,
        "shutdown_snapshot_quiesce_failures": snapshot_quiesce_failures,
        "shutdown_snapshot_quiesce_missing": (
            shutdown_snapshot_quiesce_required and not snapshot_quiesce_rows
        ),
        "shutdown_snapshot_quiesce_failed": (
            shutdown_snapshot_quiesce_required
            and bool(snapshot_quiesce_rows)
            and snapshot_quiesce_failures > 0
        ),
        "event_storm_bursts": len(burst_rows),
        "event_storm_enabled": event_storm_enabled,
        "event_storm_events_written": sum(
            int(row.get("events_total", 0) or 0) for row in burst_rows
        ),
        "event_storm_bursts_by_kind": bursts_by_kind,
        "event_storm_events_by_kind": events_by_kind,
        "event_storm_first_queries": sum(
            1 for row in event_rows if row.get("event_kind") == "first_query"
        ),
        "event_storm_write_failures": burst_write_failures,
        "event_storm_cleanups": len(cleanup_rows),
        "event_storm_cleanup_failures": cleanup_failures,
        "event_storm_cleanup_entries_estimated": sum(
            int(row.get("entries_estimated", 0) or 0)
            for row in cleanup_rows
            if isinstance(row.get("entries_estimated"), (int, float))
        ),
        "event_storm_cleanup_duration_secs": round(
            sum(
                float(row.get("duration_secs", 0.0) or 0.0)
                for row in cleanup_rows
                if isinstance(row.get("duration_secs"), (int, float))
            ),
            3,
        ),
        "unsupported_workloads": unsupported_workloads,
        "hot_churn_batches": sum(
            1 for row in hot_rows if row.get("event_kind") == "hot_churn_batch"
        ),
        "mixed_workload_enabled": mixed_workload_enabled,
        "hot_churn_files_created": sum(
            int(row.get("files_created", 0) or 0)
            for row in hot_rows
            if row.get("event_kind") == "hot_churn_batch"
        ),
        "hot_churn_errors": hot_churn_errors,
        "endpoint_sample_failures": sum(
            1 for row in endpoint_rows if not bool(row.get("ok"))
        ),
        "process_sample_count": len(process_rows),
        "memory_endpoint_sample_count": sum(
            1
            for row in endpoint_rows
            if row.get("endpoint") == "/memory" and bool(row.get("ok"))
        ),
        "final_snapshot_failed": final_snapshot_failed,
        "phase_attribution": {
            "final_snapshot_window_bounded": shutdown_signal_elapsed_secs is not None,
            "periodic_snapshot_lifecycle_available": False,
            "limitation": "periodic snapshot intervals cannot be distinguished without daemon lifecycle telemetry",
        },
    }
    payload["fingerprint"] = _fingerprint(payload)
    return payload


def evaluate_ab_comparability(
    initial_state: dict[str, Any],
    execution_state: dict[str, Any] | None = None,
) -> tuple[bool, list[str]]:
    """Fail closed when a run lacks reproducible initial state or harness health."""
    reasons: list[str] = []
    fixture = initial_state.get("fixture")
    snapshot = initial_state.get("snapshot")
    if not isinstance(fixture, dict) or not fixture.get("trusted"):
        reasons.append("fixture_count_unverified")
    elif fixture.get("count_verified") is not True:
        reasons.append("fixture_count_not_verified")
    if not isinstance(snapshot, dict) or not snapshot.get("fresh"):
        reasons.append("snapshot_preexisting")
    if not initial_state.get("binary_sha256"):
        reasons.append("binary_identity_unavailable")
    if not initial_state.get("git_sha"):
        reasons.append("git_sha_unavailable")
    if initial_state.get("git_dirty") is True:
        reasons.append("git_worktree_dirty")
    elif initial_state.get("git_dirty") is None:
        reasons.append("git_worktree_state_unavailable")
    provenance = initial_state.get("artifact_provenance")
    if not isinstance(provenance, dict) or not provenance.get("verified"):
        reasons.append("artifact_provenance_unverified")
    elif (
        provenance.get("validated_binary_sha256")
        != initial_state.get("binary_sha256")
        or provenance.get("execution_binary_sha256")
        != initial_state.get("binary_sha256")
    ):
        reasons.append("artifact_binary_identity_changed")
    if initial_state.get("collection_errors"):
        reasons.append("initial_state_collection_failed")
    if initial_state.get("run_dir_preexisting"):
        reasons.append("run_dir_preexisting")

    if execution_state is not None:
        completion_reason = str(execution_state.get("completion_reason", ""))
        if int(execution_state.get("requested_duration_secs", 0) or 0) <= 0:
            reasons.append("duration_not_fixed")
        if completion_reason == "interrupted":
            reasons.append("run_interrupted")
        elif not execution_state.get("duration_completed"):
            reasons.append("duration_not_completed")
        if execution_state.get("exit_code") != 0:
            reasons.append("daemon_exit_failed")
        if execution_state.get("fatal_error"):
            reasons.append("runner_fatal_error")
        if execution_state.get("process_sampler_error"):
            reasons.append("process_sampler_failed")
        if execution_state.get("cleanup_errors"):
            reasons.append("cleanup_failed")
        if execution_state.get("passive_canary_enabled"):
            reconcile_count = int(
                execution_state.get("passive_shutdown_reconcile_count", 0) or 0
            )
            reconcile_ok = int(
                execution_state.get("passive_shutdown_reconcile_ok", 0) or 0
            )
            reconcile_failures = int(
                execution_state.get("passive_shutdown_reconcile_failures", 0)
                or 0
            )
            if reconcile_count <= 0:
                reasons.append("passive_shutdown_reconcile_missing")
            elif reconcile_ok != reconcile_count or reconcile_failures > 0:
                reasons.append("passive_shutdown_reconcile_failed")
        if execution_state.get("shutdown_snapshot_quiesce_required"):
            quiesce_count = int(
                execution_state.get("shutdown_snapshot_quiesce_count", 0) or 0
            )
            quiesce_ok = int(
                execution_state.get("shutdown_snapshot_quiesce_ok", 0) or 0
            )
            quiesce_failures = int(
                execution_state.get("shutdown_snapshot_quiesce_failures", 0) or 0
            )
            if quiesce_count <= 0:
                reasons.append("shutdown_snapshot_quiesce_missing")
            elif quiesce_ok != quiesce_count or quiesce_failures > 0:
                reasons.append("shutdown_snapshot_quiesce_failed")
        if int(execution_state.get("event_storm_write_failures", 0) or 0) > 0:
            reasons.append("event_storm_write_failed")
        if int(execution_state.get("event_storm_cleanup_failures", 0) or 0) > 0:
            reasons.append("event_storm_cleanup_failed")
        if (
            execution_state.get("event_storm_enabled")
            and int(execution_state.get("event_storm_bursts", 0) or 0) <= 0
        ):
            reasons.append("event_storm_not_exercised")
        if int(execution_state.get("unsupported_workloads", 0) or 0) > 0:
            reasons.append("unsupported_workload")
        if int(execution_state.get("hot_churn_errors", 0) or 0) > 0:
            reasons.append("hot_churn_failed")
        if (
            execution_state.get("mixed_workload_enabled")
            and int(execution_state.get("hot_churn_batches", 0) or 0) <= 0
        ):
            reasons.append("mixed_workload_not_exercised")
        if int(execution_state.get("endpoint_sample_failures", 0) or 0) > 0:
            reasons.append("endpoint_sampling_failed")
        if int(execution_state.get("process_sample_count", 0) or 0) <= 0:
            reasons.append("process_samples_missing")
        if int(execution_state.get("memory_endpoint_sample_count", 0) or 0) <= 0:
            reasons.append("memory_endpoint_samples_missing")
        if execution_state.get("final_snapshot_failed"):
            reasons.append("final_snapshot_failed")
        if execution_state.get("shutdown_signal_elapsed_secs") is None:
            reasons.append("shutdown_boundary_missing")
    return not reasons, reasons


def http_json(base_url: str, path: str, params: dict[str, str] | None = None, timeout: float = 2.0) -> Any:
    url = base_url + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    if not raw:
        return None
    return json.loads(raw.decode("utf-8"))


def post_json(
    base_url: str,
    path: str,
    payload: dict[str, Any],
    timeout: float = 2.0,
) -> Any:
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode(
        "utf-8"
    )
    req = urllib.request.Request(
        base_url + path,
        data=encoded,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    if not raw:
        return None
    return json.loads(raw.decode("utf-8"))


def bounded_unique_paths(paths: list[Path], limit: int = 10) -> list[Path]:
    """Keep first-seen paths in request order, capped at the /scan API limit."""
    unique: list[Path] = []
    seen: set[str] = set()
    for path in paths:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        unique.append(path)
        if len(unique) >= limit:
            break
    return unique


def scan_response_metrics(
    response: Any,
) -> tuple[int, int, int, int | float, bool]:
    """Validate the fields needed to audit a successful synchronous /scan."""
    if not isinstance(response, dict):
        raise ValueError(f"POST /scan returned non-object JSON: {response!r}")
    scanned = response.get("scanned")
    changed = response.get("changed")
    deleted = response.get("deleted")
    daemon_elapsed_ms = response.get("elapsed_ms")
    stable = response.get("stable")
    for field, value in (
        ("scanned", scanned),
        ("changed", changed),
        ("deleted", deleted),
    ):
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError(f"POST /scan returned invalid {field}: {value!r}")
    if (
        not isinstance(daemon_elapsed_ms, (int, float))
        or isinstance(daemon_elapsed_ms, bool)
    ):
        raise ValueError(
            f"POST /scan returned invalid elapsed_ms: {daemon_elapsed_ms!r}"
        )
    if not isinstance(stable, bool):
        raise ValueError(f"POST /scan returned invalid stable: {stable!r}")
    return scanned, changed, deleted, daemon_elapsed_ms, stable


def stable_scan_audit(
    base_url: str,
    paths: list[str],
    timeout_secs: float,
) -> dict[str, Any]:
    """Retry synchronous /scan until the daemon reports a stable pass."""
    started_at = time.monotonic()
    deadline = started_at + max(0.001, timeout_secs)
    http_latency_secs = 0.0
    audit: dict[str, Any] = {
        "ok": False,
        "attempts": 0,
        "scanned": None,
        "changed": None,
        "deleted": None,
        "daemon_elapsed_ms": None,
        "stable": None,
    }
    try:
        for attempt in range(1, PASSIVE_SHUTDOWN_RECONCILE_MAX_ATTEMPTS + 1):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"POST /scan timed out before attempt {attempt}")
            audit["attempts"] = attempt
            request_started_at = time.monotonic()
            try:
                response = post_json(
                    base_url,
                    "/scan",
                    {"paths": paths},
                    timeout=remaining,
                )
            finally:
                http_latency_secs += time.monotonic() - request_started_at
            scanned, changed, deleted, elapsed_ms, stable = scan_response_metrics(
                response
            )
            audit["scanned"] = int(audit["scanned"] or 0) + scanned
            audit["changed"] = int(audit["changed"] or 0) + changed
            audit["deleted"] = int(audit["deleted"] or 0) + deleted
            audit["daemon_elapsed_ms"] = (
                float(audit["daemon_elapsed_ms"] or 0) + elapsed_ms
            )
            audit["stable"] = stable
            if stable:
                audit["ok"] = True
                break
            if attempt >= PASSIVE_SHUTDOWN_RECONCILE_MAX_ATTEMPTS:
                raise RuntimeError(f"POST /scan remained unstable after {attempt} attempts")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"POST /scan timed out after {attempt} attempts")
            time.sleep(
                min(PASSIVE_SHUTDOWN_RECONCILE_RETRY_INTERVAL_SECS, remaining)
            )
    except Exception as exc:  # noqa: BLE001 - preserve exact shutdown evidence
        audit["error"] = repr(exc)
    audit["http_latency_secs"] = round(http_latency_secs, 3)
    audit["latency_secs"] = round(time.monotonic() - started_at, 3)
    return audit


def snapshot_response_metrics(response: Any) -> tuple[bool, bool, bool, str | None]:
    """Validate the durable-quiescence fields returned by POST /snapshot."""
    if not isinstance(response, dict):
        raise ValueError(f"POST /snapshot returned non-object JSON: {response!r}")
    ready = response.get("ready")
    written = response.get("written")
    is_rebuilding = response.get("is_rebuilding")
    error = response.get("error")
    for field, value in (
        ("ready", ready),
        ("written", written),
        ("is_rebuilding", is_rebuilding),
    ):
        if not isinstance(value, bool):
            raise ValueError(f"POST /snapshot returned invalid {field}: {value!r}")
    if error is not None and not isinstance(error, str):
        raise ValueError(f"POST /snapshot returned invalid error: {error!r}")
    return ready, written, is_rebuilding, error


def stable_snapshot_audit(
    base_url: str,
    timeout_secs: float,
) -> dict[str, Any]:
    """Retry POST /snapshot until direct persistence or rebuild fully converges."""
    started_at = time.monotonic()
    deadline = started_at + max(0.001, timeout_secs)
    http_latency_secs = 0.0
    audit: dict[str, Any] = {
        "ok": False,
        "ready": False,
        "written": False,
        "attempts": 0,
        "not_ready_responses": 0,
        "ready_confirmations": 0,
        "is_rebuilding_last": False,
        "rebuild_observed": False,
        "last_daemon_error": "",
    }
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("POST /snapshot quiescence deadline expired")
            audit["attempts"] = int(audit["attempts"]) + 1
            request_started_at = time.monotonic()
            try:
                response = post_json(
                    base_url,
                    "/snapshot",
                    {},
                    timeout=remaining,
                )
            finally:
                http_latency_secs += time.monotonic() - request_started_at
            ready, written, is_rebuilding, daemon_error = snapshot_response_metrics(
                response
            )
            audit["ready"] = ready
            audit["written"] = bool(audit["written"]) or written
            audit["is_rebuilding_last"] = is_rebuilding
            audit["rebuild_observed"] = (
                bool(audit["rebuild_observed"])
                or is_rebuilding
                or bool(daemon_error and "rebuild" in daemon_error.lower())
            )
            if daemon_error:
                audit["last_daemon_error"] = daemon_error
            if ready:
                audit["ready_confirmations"] = int(audit["ready_confirmations"]) + 1
                if (
                    int(audit["ready_confirmations"])
                    >= SHUTDOWN_SNAPSHOT_READY_CONFIRMATIONS
                ):
                    audit["ok"] = True
                    break
            else:
                audit["ready_confirmations"] = 0
                audit["not_ready_responses"] = (
                    int(audit["not_ready_responses"]) + 1
                )
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("POST /snapshot remained unready until deadline")
            time.sleep(
                min(SHUTDOWN_SNAPSHOT_QUIESCE_RETRY_INTERVAL_SECS, remaining)
            )
    except Exception as exc:  # noqa: BLE001 - preserve exact durability evidence
        audit["error"] = repr(exc)
    audit["http_latency_secs"] = round(http_latency_secs, 3)
    audit["latency_secs"] = round(time.monotonic() - started_at, 3)
    return audit


def record_shutdown_snapshot_quiesce(
    base_url: str,
    out_path: Path,
    *,
    started_at: float,
    timeout_secs: float,
    daemon_log_path: Path | None = None,
) -> dict[str, Any]:
    """Persist and audit the final non-shutdown snapshot/rebuild barrier."""
    record_started_at = time.monotonic()
    log_offset_start = -1
    if daemon_log_path is not None:
        try:
            log_offset_start = daemon_log_path.stat().st_size
        except OSError:
            log_offset_start = -1
    record: dict[str, Any] = {
        "operation": "shutdown_snapshot_quiesce",
        "timeout_secs": timeout_secs,
        "started_elapsed_secs": round(record_started_at - started_at, 3),
        "daemon_log_offset_start": log_offset_start,
    }
    record.update(stable_snapshot_audit(base_url, timeout_secs))
    log_offset_end = -1
    if daemon_log_path is not None:
        try:
            log_offset_end = daemon_log_path.stat().st_size
        except OSError:
            log_offset_end = -1
    record["daemon_log_offset_end"] = log_offset_end
    record["daemon_log_window_valid"] = (
        log_offset_start >= 0 and log_offset_end >= log_offset_start
    )
    record["ts"] = utc_now()
    record["elapsed_secs"] = round(time.monotonic() - started_at, 3)
    json_line(out_path, record)
    return record


def daemon_http_startup_error(log_text: str) -> str:
    """Return the first log line proving that the HTTP query server failed."""
    error_markers = (
        "query server error",
        "failed to bind",
        "bind failed",
        "binding failed",
        "bind error",
        "failure binding",
        "error binding",
        "unable to bind",
        "cannot bind",
        "address already in use",
        "query server disabled",
    )
    for raw_line in log_text.splitlines():
        line = raw_line.strip()
        lowered = line.lower()
        if "uds query server error" in lowered:
            continue
        if line and any(marker in lowered for marker in error_markers):
            return line
    return ""


def _read_daemon_startup_log(daemon_log_path: Path) -> str:
    try:
        return daemon_log_path.read_text(encoding="utf-8", errors="replace")
    except FileNotFoundError:
        return ""
    except OSError as exc:
        raise RuntimeError(
            f"failed to read current fd-rdd startup log {daemon_log_path}: {exc}"
        ) from exc


def _raise_if_daemon_exited(
    process: subprocess.Popen[bytes],
    expected_log: str,
) -> None:
    exit_code = process.poll()
    if exit_code is not None:
        raise RuntimeError(
            "fd-rdd exited before HTTP startup completed "
            f"(exit code {exit_code}); expected log confirmation: {expected_log}"
        )


def wait_for_http(
    base_url: str,
    timeout_secs: float,
    *,
    process: subprocess.Popen[bytes],
    daemon_log_path: Path,
    expected_port: int,
) -> None:
    """Wait for HTTP health and prove that this daemon owns the selected port."""
    expected_log = f"HTTP Query Server listening on port {expected_port}"
    timeout_secs = max(0.0, timeout_secs)
    deadline = time.monotonic() + timeout_secs
    last_http_error: Exception | None = None

    while True:
        log_text = _read_daemon_startup_log(daemon_log_path)
        startup_error = daemon_http_startup_error(log_text)
        if startup_error:
            raise RuntimeError(
                f"fd-rdd HTTP query server startup failed: {startup_error}"
            )
        _raise_if_daemon_exited(process, expected_log)

        log_confirmed = expected_log in log_text
        if log_confirmed:
            try:
                http_json(base_url, "/health", timeout=1.0)
            except Exception as exc:  # noqa: BLE001 - preserve exact HTTP failure
                last_http_error = exc
            else:
                _raise_if_daemon_exited(process, expected_log)
                return

        _raise_if_daemon_exited(process, expected_log)
        now = time.monotonic()
        if now >= deadline:
            details: list[str] = []
            if log_confirmed:
                details.append(f"health endpoint not ready: {last_http_error!r}")
            else:
                details.append(f"missing current-run log confirmation: {expected_log}")
            raise RuntimeError(
                f"fd-rdd HTTP startup timed out after {timeout_secs:.3f}s: "
                + "; ".join(details)
            )
        time.sleep(min(0.25, deadline - now))


def port_is_free(port: int) -> bool:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.5)
            return sock.connect_ex(("127.0.0.1", port)) != 0
    except PermissionError:
        # Some restricted CI/sandbox profiles deny raw socket creation.
        # In a normal VM this check works; here we let fd-rdd be the final arbiter.
        return True


def split_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def read_proc_status(pid: int) -> dict[str, int]:
    out: dict[str, int] = {}
    try:
        text = Path(f"/proc/{pid}/status").read_text(encoding="utf-8")
    except FileNotFoundError:
        return out
    for line in text.splitlines():
        key, _, rest = line.partition(":")
        if key in {"VmRSS", "VmSwap", "VmSize"}:
            value = rest.strip().split()[0]
            out[key.lower() + "_bytes"] = int(value) * 1024
        elif key == "Threads":
            out["threads"] = int(rest.strip())
    return out


def read_proc_stat(pid: int) -> dict[str, int]:
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    except FileNotFoundError:
        return {}
    rparen = stat.rfind(")")
    if rparen < 0:
        return {}
    parts = stat[rparen + 2 :].split()
    if len(parts) < 15:
        return {}
    # parts[0] is procfs field 3 (state).
    return {
        "minor_faults": int(parts[7]),
        "major_faults": int(parts[9]),
        "ticks": int(parts[11]) + int(parts[12]),
    }


def read_proc_ticks(pid: int) -> int | None:
    return read_proc_stat(pid).get("ticks")


def read_proc_io(pid: int) -> dict[str, int]:
    try:
        text = Path(f"/proc/{pid}/io").read_text(encoding="utf-8")
    except FileNotFoundError:
        return {}
    aliases = {
        "read_bytes": "read_bytes",
        "write_bytes": "write_bytes",
        "syscr": "read_syscalls",
        "syscw": "write_syscalls",
    }
    result: dict[str, int] = {}
    for line in text.splitlines():
        key, separator, value = line.partition(":")
        output_key = aliases.get(key)
        if separator and output_key:
            result[output_key] = int(value.strip())
    return result


def read_fd_count(pid: int) -> int:
    try:
        return len(list(Path(f"/proc/{pid}/fd").iterdir()))
    except FileNotFoundError:
        return 0


def process_sampler(pid: int) -> Any:
    ticks_per_sec = os.sysconf(os.sysconf_names.get("SC_CLK_TCK", "SC_CLK_TCK"))
    previous_ticks: int | None = None
    previous_at: float | None = None
    while True:
        now = time.monotonic()
        proc_stat = read_proc_stat(pid)
        ticks = proc_stat.get("ticks")
        cpu_pct = 0.0
        if (
            ticks is not None
            and previous_ticks is not None
            and previous_at is not None
            and now > previous_at
        ):
            cpu_pct = ((ticks - previous_ticks) / ticks_per_sec) / (now - previous_at) * 100.0
        previous_ticks = ticks
        previous_at = now

        status = read_proc_status(pid)
        status.update(read_proc_io(pid))
        if ticks is not None:
            status["cpu_ticks"] = ticks
        status.update(
            {
                key: proc_stat[key]
                for key in ("minor_faults", "major_faults")
                if key in proc_stat
            }
        )
        status["fd_count"] = read_fd_count(pid)
        status["cpu_pct"] = round(cpu_pct, 3)
        yield status


class ProcessSampleRunner:
    """Sample procfs independently so slow HTTP endpoints cannot hide RSS peaks."""

    def __init__(
        self,
        pid: int,
        out_path: Path,
        started_at: float,
        interval_secs: float,
        process_running: Callable[[], bool] | None = None,
    ) -> None:
        self.pid = pid
        self.out_path = out_path
        self.started_at = started_at
        self.interval_secs = max(0.05, interval_secs)
        self.process_running = process_running
        self.error = ""
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._loop,
            daemon=True,
            name="proc-sampler",
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None

    def _loop(self) -> None:
        sampler = process_sampler(self.pid)
        try:
            while not self._stop.is_set():
                sample = next(sampler)
                if not sample and not self._daemon_is_running():
                    return
                record = {
                    "ts": utc_now(),
                    "elapsed_secs": round(time.monotonic() - self.started_at, 3),
                    **sample,
                }
                json_line(self.out_path, record)
                if self._stop.wait(self.interval_secs):
                    return
        except Exception as exc:  # noqa: BLE001 - surfaced in the run summary
            if self._is_procfs_exit_race(exc) and not self._daemon_is_running():
                return
            self.error = repr(exc)

    def _daemon_is_running(self) -> bool:
        if self.process_running is not None:
            try:
                return bool(self.process_running())
            except Exception:  # noqa: BLE001 - do not hide a real sampler error
                return True
        return Path(f"/proc/{self.pid}").exists()

    @staticmethod
    def _is_procfs_exit_race(exc: Exception) -> bool:
        return isinstance(exc, OSError) and exc.errno in {
            errno.ENOENT,
            errno.EACCES,
            errno.ESRCH,
        }


def search_results(base_url: str, query: str, limit: int = 50) -> list[dict[str, Any]]:
    data = http_json(
        base_url,
        "/search",
        {"q": query, "limit": str(limit)},
        timeout=5.0,
    )
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict) and isinstance(data.get("results"), list):
        return [item for item in data["results"] if isinstance(item, dict)]
    return []


def result_has_path(results: list[dict[str, Any]], path: Path) -> bool:
    target = str(path)
    return any(str(item.get("path", "")) == target for item in results)


def wait_search_state(
    base_url: str,
    query: str,
    path: Path,
    should_exist: bool,
    timeout_secs: float,
) -> tuple[bool, float, int]:
    start = time.monotonic()
    polls = 0
    while time.monotonic() - start <= timeout_secs:
        polls += 1
        try:
            exists = result_has_path(search_results(base_url, query), path)
            if exists == should_exist:
                return True, time.monotonic() - start, polls
        except Exception:
            pass
        time.sleep(0.25)
    return False, time.monotonic() - start, polls


def check_search_state_once(
    base_url: str,
    query: str,
    path: Path,
    should_exist: bool,
) -> tuple[bool, bool, float, str]:
    start = time.monotonic()
    try:
        exists = result_has_path(search_results(base_url, query), path)
        return exists == should_exist, exists, time.monotonic() - start, ""
    except Exception as exc:  # noqa: BLE001 - benchmark evidence should keep exact error
        return False, False, time.monotonic() - start, repr(exc)


def debug_tiered_watch(base_url: str, root: Path | None = None) -> dict[str, Any]:
    params = {"root": str(root)} if root else None
    data = http_json(base_url, "/debug/tiered-watch", params=params, timeout=5.0)
    return data if isinstance(data, dict) else {}


def compute_tier_distribution(watch_sample: dict[str, Any] | None) -> dict[str, int]:
    """Count directories at each watch tier (L0/L1/L2/L3) from a /watch-state sample."""
    counts: dict[str, int] = {"L0": 0, "L1": 0, "L2": 0, "L3": 0, "unknown": 0}
    if not watch_sample:
        return counts
    # The public /watch-state endpoint exposes aggregate counters. A per-dir
    # `dirs` array belongs to /debug/tiered-watch, and is retained only as a
    # compatibility fallback for callers that intentionally pass that dump.
    aggregate_keys = {tier: f"{tier.lower()}_dirs" for tier in ("L0", "L1", "L2", "L3")}
    if any(key in watch_sample for key in aggregate_keys.values()):
        for tier, key in aggregate_keys.items():
            counts[tier] = int(watch_sample.get(key, 0) or 0)
        return counts
    dirs = watch_sample.get("dirs")
    if isinstance(dirs, list):
        for d in dirs:
            if not isinstance(d, dict):
                continue
            tier = str(d.get("watch_tier", "")).upper()
            if tier in counts:
                counts[tier] += 1
            else:
                counts["unknown"] += 1
    return counts


def safe_name(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "-" for ch in value).strip("-")


EVENT_STORM_DIR_PREFIX = "fd-rdd-m2-event-storm-"


EVENT_STORM_KIND_ALIASES = {
    "rw100": "rw100",
    "save100": "save100",
    "git_clone": "git_clone",
    "gitclone": "git_clone",
    "npm_install": "npm_install",
    "npminstall": "npm_install",
    "subtree_rename": "subtree_rename",
    "subtree_rename_avalanche": "subtree_rename",
    "dir_rename": "subtree_rename",
    "mount_storm": "mount_storm",
    "mount_point_storm": "mount_storm",
    "inode_reuse": "inode_reuse",
    "ghost_inode_reuse": "inode_reuse",
    "ghost_reuse": "inode_reuse",
    "inode_reuse_stress": "inode_reuse_stress",
    "inode_stress": "inode_reuse_stress",
    "time_skew": "time_skew",
    "clock_skew": "time_skew",
}


def normalize_event_storm_kinds(raw: list[str]) -> list[str]:
    normalized: list[str] = []
    for item in raw:
        key = item.strip().lower().replace("-", "_")
        if not key:
            continue
        normalized.append(EVENT_STORM_KIND_ALIASES.get(key, key))
    return normalized


def supported_event_storm_kinds() -> list[str]:
    return sorted(set(EVENT_STORM_KIND_ALIASES.values()))


def run_canary_cycle(base_url: str, canary_root: Path, timeout_secs: float) -> list[dict[str, Any]]:
    canary_root.mkdir(parents=True, exist_ok=True)
    marker = f"fd_rdd_m2_canary_{int(time.time() * 1000)}"
    created = canary_root / f"{marker}_create.txt"
    renamed = canary_root / f"{marker}_rename.txt"
    records: list[dict[str, Any]] = []

    created.write_text(f"{utc_now()} create\n", encoding="utf-8")
    ok, latency, polls = wait_search_state(base_url, created.name, created, True, timeout_secs)
    records.append(
        {
            "operation": "create_visible",
            "canary_kind": "active",
            "path": str(created),
            "ok": ok,
            "latency_secs": round(latency, 3),
            "polls": polls,
        }
    )

    created.rename(renamed)
    ok_new, latency_new, polls_new = wait_search_state(
        base_url, renamed.name, renamed, True, timeout_secs
    )
    ok_old, latency_old, polls_old = wait_search_state(
        base_url, created.name, created, False, timeout_secs
    )
    records.append(
        {
            "operation": "rename_new_visible",
            "canary_kind": "active",
            "path": str(renamed),
            "ok": ok_new,
            "latency_secs": round(latency_new, 3),
            "polls": polls_new,
        }
    )
    records.append(
        {
            "operation": "rename_old_hidden",
            "canary_kind": "active",
            "path": str(created),
            "ok": ok_old,
            "latency_secs": round(latency_old, 3),
            "polls": polls_old,
        }
    )

    renamed.unlink(missing_ok=True)
    ok, latency, polls = wait_search_state(base_url, renamed.name, renamed, False, timeout_secs)
    records.append(
        {
            "operation": "delete_hidden",
            "canary_kind": "active",
            "path": str(renamed),
            "ok": ok,
            "latency_secs": round(latency, 3),
            "polls": polls,
        }
    )
    return records


class PassiveCanaryRunner:
    """Creates canary files first and queries later to avoid measuring query-triggered repair."""

    def __init__(
        self,
        base_url: str,
        root: Path,
        out_path: Path,
        started_at: float,
        interval_secs: float,
        settle_secs: float,
        timeout_secs: float,
        start_delay_secs: float,
    ) -> None:
        self.base_url = base_url
        self.root = root
        self.out_path = out_path
        self.started_at = started_at
        self.interval_secs = max(1.0, interval_secs)
        self.settle_secs = max(0.0, settle_secs)
        self.timeout_secs = max(0.0, timeout_secs)
        self.next_start_at = time.monotonic() + max(0.0, start_delay_secs)
        self.active: dict[str, Any] | None = None
        self.cycle = 0

    def tick(self, now: float) -> None:
        if self.active is None:
            if now >= self.next_start_at:
                self.start_cycle(now)
            return
        if now < float(self.active["due_at"]):
            return
        self.process_due(now)

    def start_cycle(self, now: float) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        self.cycle += 1
        marker = f"fd_rdd_m2_passive_{int(time.time() * 1000)}_{self.cycle}"
        created = self.root / f"{marker}_create.txt"
        renamed = self.root / f"{marker}_rename.txt"
        created.write_text(f"{utc_now()} passive create\n", encoding="utf-8")
        self.active = {
            "cycle": self.cycle,
            "stage": "check_create",
            "created": created,
            "renamed": renamed,
            "stage_started_at": now,
            "due_at": now + self.settle_secs,
        }
        self.emit(
            {
                "operation": "passive_create_written",
                "path": str(created),
                "ok": True,
                "passive_wait_secs": 0.0,
            }
        )

    def process_due(self, now: float) -> None:
        assert self.active is not None
        stage = str(self.active["stage"])
        if stage == "check_create":
            created = Path(self.active["created"])
            self.record_first_query(
                "passive_create_first_query",
                created.name,
                created,
                True,
                now,
            )
            self.record_after_query_if_needed(
                "passive_create_after_query",
                created.name,
                created,
                True,
            )
            renamed = Path(self.active["renamed"])
            try:
                created.rename(renamed)
                self.emit(
                    {
                        "operation": "passive_rename_applied",
                        "old_path": str(created),
                        "new_path": str(renamed),
                        "ok": True,
                    }
                )
                self.active["stage"] = "check_rename"
                self.active["stage_started_at"] = time.monotonic()
                self.active["due_at"] = time.monotonic() + self.settle_secs
            except Exception as exc:  # noqa: BLE001 - keep exact failure evidence
                self.emit(
                    {
                        "operation": "passive_rename_prepare",
                        "path": str(created),
                        "ok": False,
                        "error": repr(exc),
                    }
                )
                self.finish_cycle()
        elif stage == "check_rename":
            created = Path(self.active["created"])
            renamed = Path(self.active["renamed"])
            self.record_first_query(
                "passive_rename_new_first_query",
                renamed.name,
                renamed,
                True,
                now,
            )
            self.record_after_query_if_needed(
                "passive_rename_new_after_query",
                renamed.name,
                renamed,
                True,
            )
            self.record_first_query(
                "passive_rename_old_first_query",
                created.name,
                created,
                False,
                time.monotonic(),
            )
            try:
                renamed.unlink(missing_ok=True)
                self.emit(
                    {
                        "operation": "passive_delete_applied",
                        "path": str(renamed),
                        "ok": True,
                    }
                )
                self.active["stage"] = "check_delete"
                self.active["stage_started_at"] = time.monotonic()
                self.active["due_at"] = time.monotonic() + self.settle_secs
            except Exception as exc:  # noqa: BLE001
                self.emit(
                    {
                        "operation": "passive_delete_prepare",
                        "path": str(renamed),
                        "ok": False,
                        "error": repr(exc),
                    }
                )
                self.finish_cycle()
        elif stage == "check_delete":
            renamed = Path(self.active["renamed"])
            self.record_first_query(
                "passive_delete_first_query",
                renamed.name,
                renamed,
                False,
                now,
            )
            self.record_after_query_if_needed(
                "passive_delete_after_query",
                renamed.name,
                renamed,
                False,
            )
            self.finish_cycle()

    def record_first_query(
        self,
        operation: str,
        query: str,
        path: Path,
        should_exist: bool,
        now: float,
    ) -> bool:
        assert self.active is not None
        ok, exists, latency, error = check_search_state_once(
            self.base_url,
            query,
            path,
            should_exist,
        )
        self.emit(
            {
                "operation": operation,
                "path": str(path),
                "query": query,
                "ok": ok,
                "correct": exists == should_exist,
                "transport_ok": not bool(error),
                "first_query_exists": exists,
                "should_exist": should_exist,
                "latency_secs": round(latency, 3),
                "passive_wait_secs": round(now - float(self.active["stage_started_at"]), 3),
                **({"error": error} if error else {}),
            }
        )
        return ok

    def record_after_query_if_needed(
        self,
        operation: str,
        query: str,
        path: Path,
        should_exist: bool,
    ) -> None:
        if self.timeout_secs <= 0:
            return
        ok, latency, polls = wait_search_state(
            self.base_url,
            query,
            path,
            should_exist,
            self.timeout_secs,
        )
        self.emit(
            {
                "operation": operation,
                "path": str(path),
                "query": query,
                "ok": ok,
                "should_exist": should_exist,
                "latency_secs": round(latency, 3),
                "polls": polls,
            }
        )

    def reconcile_shutdown(self, paths: list[Path] | None = None) -> dict[str, Any]:
        """Synchronously reconcile passive canary state before daemon shutdown."""
        active_cycle = (
            int(self.active["cycle"])
            if self.active is not None and "cycle" in self.active
            else None
        )
        active_stage = (
            str(self.active["stage"])
            if self.active is not None and "stage" in self.active
            else None
        )
        timeout_secs = max(
            PASSIVE_SHUTDOWN_RECONCILE_TIMEOUT_SECS,
            self.timeout_secs,
        )
        reconcile_paths = bounded_unique_paths([self.root, *(paths or [])])
        record: dict[str, Any] = {
            "operation": "passive_shutdown_reconcile",
            "root": str(self.root),
            "paths": [str(path) for path in reconcile_paths],
            "active_cycle": active_cycle,
            "active_stage": active_stage,
            "timeout_secs": timeout_secs,
        }
        record.update(
            stable_scan_audit(
                self.base_url,
                record["paths"],
                timeout_secs,
            )
        )
        self.emit_shutdown_record(record)
        return record

    def emit_shutdown_record(self, record: dict[str, Any]) -> None:
        record["canary_kind"] = "passive"
        record["ts"] = utc_now()
        record["elapsed_secs"] = round(time.monotonic() - self.started_at, 3)
        json_line(self.out_path, record)

    def emit(self, record: dict[str, Any]) -> None:
        assert self.active is not None
        record.setdefault("ok", False)
        record["canary_kind"] = "passive"
        record["cycle"] = int(self.active["cycle"])
        record["ts"] = utc_now()
        record["elapsed_secs"] = round(time.monotonic() - self.started_at, 3)
        json_line(self.out_path, record)

    def finish_cycle(self) -> None:
        self.active = None
        self.next_start_at = time.monotonic() + self.interval_secs


class EventStormRunner:
    """Injects short filesystem bursts and measures eventual search visibility."""

    def __init__(
        self,
        base_url: str,
        roots: list[Path],
        out_path: Path,
        started_at: float,
        start_delay_secs: float,
        interval_secs: float,
        settle_secs: float,
        timeout_secs: float,
        ops_per_burst: int,
        duration_budget_secs: float,
        time_skew_secs: float,
        kinds: list[str],
        target_tiers: list[str],
        file_count: int = 0,
        subtree_depth: int = 2,
        inode_stress_iterations: int = 0,
        inode_stress_tmpfs_inodes: int = 200,
        immediate_query_enabled: bool = False,
        immediate_query_settle_secs: float = 5.0,
        max_bursts: int = 0,
        visibility_probes_per_burst: int = 0,
        visibility_poll_interval_secs: float = 1.0,
        fixed_root_schedule: bool = False,
        deterministic_plan_seed: int | None = None,
        rotating_tick_secs: float = 0.0,
        rotating_ttl_secs: float = 0.0,
        rotating_dirs_per_tick: int = 0,
        strict_protocol: bool = False,
        treatment_enabled: bool = False,
        post_cleanup_audit_secs: float = 0.0,
        precondition_wait_secs: float = 0.0,
        min_lease_remaining_secs: float = 0.0,
    ) -> None:
        self.base_url = base_url
        self.roots = roots
        self.out_path = out_path
        self.started_at = started_at
        self.start_delay_secs = max(0.0, start_delay_secs)
        self.interval_secs = max(1.0, interval_secs)
        self.settle_secs = max(0.0, settle_secs)
        self.timeout_secs = max(0.0, timeout_secs)
        self.ops_per_burst = max(1, ops_per_burst)
        self.duration_budget_secs = max(0.1, duration_budget_secs)
        self.time_skew_secs = max(1.0, time_skew_secs)
        # Task 5: explicit file-count and tree-depth knobs. file_count<=0 means
        # "use each workload's existing ops-based default" (preserves old behavior).
        self.file_count = max(0, file_count)
        self.subtree_depth = max(1, subtree_depth)
        # Task 2: inode_reuse_stress tmpfs tuning.
        self.inode_stress_iterations = max(0, inode_stress_iterations)
        self.inode_stress_tmpfs_inodes = max(16, inode_stress_tmpfs_inodes)
        # Task 2: immediate-query two-pass mode. When enabled, after each burst
        # we do an "immediate" query pass at immediate_query_settle_secs (e.g. 5s,
        # simulating a user searching right after downloading) and then a "delayed"
        # pass at the normal settle_secs. Both are reported separately.
        self.immediate_query_enabled = bool(immediate_query_enabled)
        self.immediate_query_settle_secs = max(0.0, immediate_query_settle_secs)
        self.max_bursts = max(0, max_bursts)
        self.visibility_probes_per_burst = max(0, visibility_probes_per_burst)
        self.visibility_poll_interval_secs = max(0.2, visibility_poll_interval_secs)
        self.fixed_root_schedule = bool(fixed_root_schedule)
        self.rotating_tick_secs = max(0.0, rotating_tick_secs)
        self.rotating_ttl_secs = max(0.0, rotating_ttl_secs)
        self.rotating_dirs_per_tick = max(0, rotating_dirs_per_tick)
        self.strict_protocol = bool(strict_protocol)
        self.treatment_enabled = bool(treatment_enabled)
        self.post_cleanup_audit_secs = max(0.0, post_cleanup_audit_secs)
        self.pending_cleanup_audits: list[dict[str, Any]] = []
        self.precondition_wait_secs = max(0.0, precondition_wait_secs)
        self.min_lease_remaining_secs = max(0.0, min_lease_remaining_secs)
        self.precondition_wait_started_at: float | None = None
        self.protocol_error = ""
        self.kinds = normalize_event_storm_kinds(kinds)
        self.target_tiers = [tier.upper() for tier in target_tiers]
        tiers = self.target_tiers or [""]
        workloads = self.kinds or ["rw100"]
        self.work_items = [(tier, kind) for tier in tiers for kind in workloads]
        # Run-unique id (timestamp + pid) folded into burst paths so concurrent or
        # repeated runs (A vs B legs, re-runs on shared storm roots) never collide
        # on fixture directories -- the root cause of OSError(39, 'Directory not empty').
        self.run_id = (
            f"seed-{deterministic_plan_seed}"
            if deterministic_plan_seed is not None
            else f"{time.time_ns()}-{os.getpid()}"
        )
        self.query_token = hashlib.sha256(self.run_id.encode("utf-8")).hexdigest()[:8]
        self.next_start_at = time.monotonic() + self.start_delay_secs
        self.active: dict[str, Any] | None = None
        self.current_burst_started_at = 0.0
        self.mutation_seq = 0
        self.cycle = 0

    def tick(self, now: float) -> None:
        self.poll_post_cleanup_audits(now)
        if self.active is None:
            if now >= self.next_start_at:
                if self.max_bursts > 0 and self.cycle >= self.max_bursts:
                    return
                self.start_cycle(now)
            return
        self.poll_visibility(now)
        if now < float(self.active["due_at"]):
            return
        self.process_due(now)

    def start_cycle(self, now: float) -> None:
        if not self.roots:
            return
        if self.max_bursts > 0 and self.cycle >= self.max_bursts:
            return
        previous_cycle = self.cycle
        self.cycle += 1
        requested_tier, selected_kind = self.work_items[(self.cycle - 1) % len(self.work_items)]
        root = self.select_root(requested_tier)
        if self.strict_protocol and not root.is_dir():
            self.reject_protocol_precondition(
                root,
                requested_tier,
                selected_kind,
                "",
                f"selected fixture root does not exist: {root}",
                {},
            )
            return
        root.mkdir(parents=True, exist_ok=True)
        burst_root = self.burst_root(root, self.burst_directory_kind(selected_kind))
        tier_before = self.tier_for_root(root)
        target_m2_evidence = self.m2_evidence_for_root(root)
        protocol_error = self.protocol_precondition_error(
            requested_tier,
            tier_before,
            target_m2_evidence,
        )
        if protocol_error:
            if self.defer_protocol_precondition(
                now,
                root,
                requested_tier,
                selected_kind,
                tier_before,
                protocol_error,
                target_m2_evidence,
            ):
                self.cycle = previous_cycle
                return
            self.precondition_wait_started_at = None
            self.reject_protocol_precondition(
                root,
                requested_tier,
                selected_kind,
                tier_before,
                protocol_error,
                target_m2_evidence,
            )
            return
        self.precondition_wait_started_at = None
        # Capture the distribution only once the strict precondition is proven;
        # retries while the root is demoting must not masquerade as bursts.
        if self.cycle == 1:
            self._emit_tier_distribution("storm_start")
        events: list[dict[str, Any]] = []
        cycle_started = time.monotonic()
        self.current_burst_started_at = cycle_started
        self.emit(
            {
                "event_kind": "burst_started",
                "operation": "burst_started",
                "root": str(root),
                "requested_tier": requested_tier,
                "selected_kind": selected_kind,
                "tier_before": tier_before,
                "fixed_root_schedule": self.fixed_root_schedule,
                **target_m2_evidence,
            }
        )
        try:
            if selected_kind == "rw100":
                events.extend(self.write_rw100(root, tier_before))
            elif selected_kind == "save100":
                events.extend(self.write_save100(root, tier_before))
            elif selected_kind == "git_clone":
                events.extend(self.write_git_clone_fixture(root, tier_before))
            elif selected_kind == "npm_install":
                events.extend(self.write_npm_install_fixture(root, tier_before))
            elif selected_kind == "subtree_rename":
                events.extend(self.write_subtree_rename_avalanche(root, tier_before))
            elif selected_kind == "mount_storm":
                events.extend(self.write_mount_storm_fixture(root, tier_before))
            elif selected_kind == "inode_reuse":
                events.extend(self.write_inode_reuse_fixture(root, tier_before))
            elif selected_kind == "inode_reuse_stress":
                events.extend(self.write_inode_reuse_stress(root, tier_before))
            elif selected_kind == "time_skew":
                events.extend(self.write_time_skew_fixture(root, tier_before))
            else:
                self.emit(
                    {
                        "event_kind": "unsupported_workload",
                        "operation": "unsupported_workload",
                        "selected_kind": selected_kind,
                        "supported_kinds": supported_event_storm_kinds(),
                        "ok": False,
                    }
                )
        except Exception as exc:  # noqa: BLE001 - keep going; record evidence
            # Fix C: isolate each workload so one fixture failure (e.g. a stray
            # ENOTEMPTY) cannot kill the entire run. Skip the settle/check phase
            # for this cycle and schedule the next burst.
            self.emit(
                {
                    "event_kind": "burst_write_failed",
                    "operation": "burst_write_failed",
                    "root": str(root),
                    "requested_tier": requested_tier,
                    "selected_kind": selected_kind,
                    "tier_before": tier_before,
                    "ok": False,
                    "error": repr(exc),
                }
            )
            self.cleanup_burst_root(
                burst_root,
                root,
                selected_kind,
                phase="burst_write_failed",
            )
            self.active = None
            self.next_start_at = time.monotonic() + self.interval_secs
            return
        generation_secs = time.monotonic() - cycle_started
        mutation_completed_unix_secs = int(time.time())
        target_m2_fence = {
            key.replace("target_m2_", "target_m2_fence_", 1): value
            for key, value in self.m2_evidence_for_root(root).items()
        }
        # Task 2: when immediate-query mode is enabled, the first due point is
        # the immediate settle (e.g. 5s); after that pass we reschedule to the
        # normal settle_secs for the delayed pass. Otherwise a single pass at
        # settle_secs (backwards compatible).
        use_immediate = (
            self.immediate_query_enabled
            and self.immediate_query_settle_secs < self.settle_secs
        )
        immediate_due = time.monotonic() + self.immediate_query_settle_secs
        delayed_due = time.monotonic() + self.settle_secs
        due_at = immediate_due if use_immediate else delayed_due
        self.active = {
            "cycle": self.cycle,
            "root": root,
            "burst_root": burst_root,
            "requested_tier": requested_tier,
            "selected_kind": selected_kind,
            "tier_before": tier_before,
            "events": events,
            "started_at": cycle_started,
            "due_at": due_at,
            "stage": "immediate_query" if use_immediate else "delayed_query",
            "delayed_due_at": delayed_due,
            "immediate_done": False,
            "visibility_probes": self.select_visibility_probes(events),
            "visibility_next_poll_at": time.monotonic(),
            "target_m2_before": target_m2_evidence,
            "target_m2_fence": target_m2_fence,
            "mutation_completed_unix_secs": mutation_completed_unix_secs,
        }
        self.emit(
            {
                "event_kind": "burst_written",
                "operation": "burst_written",
                "root": str(root),
                "requested_tier": requested_tier,
                "selected_kind": selected_kind,
                "tier_before": tier_before,
                "events_total": len(events),
                "duration_secs": round(generation_secs, 3),
                "duration_budget_secs": self.duration_budget_secs,
                "within_budget": generation_secs <= self.duration_budget_secs,
                "mutation_completed_unix_secs": mutation_completed_unix_secs,
                "kinds": self.kinds,
                **target_m2_evidence,
                **target_m2_fence,
            }
        )
        for event in events:
            self.emit(event)

    def defer_protocol_precondition(
        self,
        now: float,
        root: Path,
        requested_tier: str,
        selected_kind: str,
        tier_before: str,
        error: str,
        evidence: dict[str, Any],
    ) -> bool:
        """Wait for a strict L3/fresh-lease precondition without consuming a cycle."""
        if self.precondition_wait_secs <= 0:
            return False
        if self.precondition_wait_started_at is None:
            self.precondition_wait_started_at = now
        elapsed = max(0.0, now - self.precondition_wait_started_at)
        if elapsed >= self.precondition_wait_secs:
            return False
        self.emit(
            {
                "event_kind": "protocol_precondition_wait",
                "operation": "protocol_precondition_wait",
                "root": str(root),
                "requested_tier": requested_tier,
                "selected_kind": selected_kind,
                "tier_before": tier_before,
                "error": error,
                "wait_elapsed_secs": round(elapsed, 3),
                "wait_remaining_secs": round(
                    self.precondition_wait_secs - elapsed, 3
                ),
                "ok": False,
                **evidence,
            }
        )
        self.next_start_at = now + min(
            0.5,
            self.precondition_wait_secs - elapsed,
        )
        return True

    def select_visibility_probes(
        self,
        events: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        positives = [event for event in events if bool(event.get("should_exist"))]
        count = min(self.visibility_probes_per_burst, len(positives))
        if count <= 0:
            return []
        if count == 1:
            selected = [positives[0]]
        elif count == len(positives):
            selected = positives
        else:
            indexes = {
                round(index * (len(positives) - 1) / (count - 1))
                for index in range(count)
            }
            selected = [positives[index] for index in sorted(indexes)]
        return [
            {
                "event": event,
                "polls": 0,
                "transport_failures": 0,
                "last_error": "",
                "visible_at": None,
                "visible_query_latency": 0.0,
            }
            for event in selected
        ]

    def poll_visibility(self, now: float) -> None:
        assert self.active is not None
        probes = list(self.active.get("visibility_probes", []))
        if not probes or now < float(self.active.get("visibility_next_poll_at", 0.0)):
            return
        self.active["visibility_next_poll_at"] = now + self.visibility_poll_interval_secs
        for probe in probes:
            event = probe["event"]
            ok, exists, query_latency, error = check_search_state_once(
                self.base_url,
                str(event["query"]),
                Path(event["path"]),
                True,
            )
            completed_at = time.monotonic()
            probe["polls"] = int(probe.get("polls", 0)) + 1
            if error:
                probe["transport_failures"] = int(
                    probe.get("transport_failures", 0)
                ) + 1
                probe["last_error"] = error
            if ok and exists and probe.get("visible_at") is None:
                probe["visible_at"] = completed_at
                probe["visible_query_latency"] = query_latency

    def emit_visibility_result(
        self,
        probe: dict[str, Any],
        now: float,
        *,
        visible: bool,
        timeout: bool,
        query_latency: float = 0.0,
    ) -> None:
        assert self.active is not None
        event = probe["event"]
        event_age = max(
            0.0,
            now
            - float(self.active["started_at"])
            - float(event.get("burst_elapsed_secs", 0.0)),
        )
        tier_after = self.tier_for_root(Path(self.active["root"]))
        self.emit(
            {
                "event_kind": "visibility_probe",
                "operation": str(event.get("operation", "")) + "_visibility",
                "workload": event.get("workload", ""),
                "path": event.get("path", ""),
                "query": event.get("query", ""),
                "visible": visible,
                "timeout": timeout,
                "ok": visible,
                "latency_secs": round(event_age, 3),
                "query_latency_secs": round(query_latency, 3),
                "polls": int(probe.get("polls", 0)),
                "transport_failures": int(probe.get("transport_failures", 0)),
                "requested_tier": self.active.get("requested_tier", ""),
                "tier_before": event.get("tier_before", ""),
                "tier_after": tier_after,
                **(
                    {"last_error": str(probe.get("last_error", ""))}
                    if probe.get("last_error")
                    else {}
                ),
            }
        )
    def finalize_visibility_probes(self, now: float) -> None:
        assert self.active is not None
        for probe in list(self.active.get("visibility_probes", [])):
            visible_at = probe.get("visible_at")
            self.emit_visibility_result(
                probe,
                float(visible_at) if visible_at is not None else now,
                visible=visible_at is not None,
                timeout=visible_at is None,
                query_latency=float(probe.get("visible_query_latency", 0.0) or 0.0),
            )

    def write_rw100(self, root: Path, tier_before: str) -> list[dict[str, Any]]:
        burst_root = self.burst_root(root, "rw100")
        burst_root.mkdir(parents=True, exist_ok=True)
        records: list[dict[str, Any]] = []
        deadline = time.monotonic() + self.duration_budget_secs
        count = self.file_count if self.file_count > 0 else self.ops_per_burst
        for i in range(count):
            path = burst_root / f"rw_{i:04d}.txt"
            marker = f"fd_rdd_m2_storm_rw_{self.cycle}_{i:04d}"
            path.write_text(f"{utc_now()} {marker}\n", encoding="utf-8")
            _ = path.read_text(encoding="utf-8")
            with path.open("a", encoding="utf-8") as f:
                f.write("append\n")
            records.append(self.expected_record("rw100", "create_modify", path, path.name, True, tier_before))
            if time.monotonic() > deadline:
                break
        return records

    def write_save100(self, root: Path, tier_before: str) -> list[dict[str, Any]]:
        burst_root = self.burst_root(root, "save100")
        burst_root.mkdir(parents=True, exist_ok=True)
        records: list[dict[str, Any]] = []
        deadline = time.monotonic() + self.duration_budget_secs
        count = self.file_count if self.file_count > 0 else self.ops_per_burst
        for i in range(count):
            name = f"m2v_{self.query_token}_{self.cycle:03d}_save_{i:04d}"
            final = burst_root / f"{name}.txt"
            tmp = burst_root / f".{name}.tmp"
            marker = f"fd_rdd_m2_storm_save_{self.cycle}_{i:04d}"
            tmp.write_text(f"{utc_now()} {marker}\n", encoding="utf-8")
            tmp.rename(final)
            records.append(self.expected_record("save100", "atomic_save_final", final, final.name, True, tier_before))
            records.append(self.expected_record("save100", "atomic_save_tmp_hidden", tmp, tmp.name, False, tier_before))
            if time.monotonic() > deadline:
                break
        return records

    def write_git_clone_fixture(self, root: Path, tier_before: str) -> list[dict[str, Any]]:
        repo_root = self.burst_root(root, "git-clone") / "repo"
        records: list[dict[str, Any]] = []
        dirs = [
            repo_root / ".git" / "objects" / "pack",
            repo_root / ".git" / "refs" / "heads",
            repo_root / "src",
            repo_root / "tests",
        ]
        for d in dirs:
            d.mkdir(parents=True, exist_ok=True)
        files = [
            (
                repo_root / f"m2v_{self.query_token}_{self.cycle:03d}_README.md",
                "fd_rdd_m2_storm_git_readme",
            ),
            (
                repo_root / "src" / f"m2v_{self.query_token}_{self.cycle:03d}_main.rs",
                "fd_rdd_m2_storm_git_main",
            ),
            (
                repo_root / "tests" / f"m2v_{self.query_token}_{self.cycle:03d}_smoke.rs",
                "fd_rdd_m2_storm_git_smoke",
            ),
        ]
        for path, marker in files:
            path.write_text(f"{utc_now()} {marker}\n", encoding="utf-8")
            records.append(self.expected_record("git_clone", "clone_file_visible", path, path.name, True, tier_before))
        hidden_files = [
            (repo_root / ".git" / "HEAD", "ref: refs/heads/main"),
            (repo_root / ".git" / "refs" / "heads" / "main", "0000000000000000000000000000000000000000"),
            (repo_root / ".git" / "objects" / "pack" / "pack-test.idx", "fd_rdd_m2_storm_git_pack_idx"),
            (repo_root / ".git" / "objects" / "pack" / "pack-test.pack", "fd_rdd_m2_storm_git_pack"),
        ]
        for path, marker in hidden_files:
            path.write_text(f"{utc_now()} {marker}\n", encoding="utf-8")
        return records

    def write_npm_install_fixture(self, root: Path, tier_before: str) -> list[dict[str, Any]]:
        pkg_root = self.burst_root(root, "npm-install") / "app"
        node_modules = pkg_root / "node_modules"
        records: list[dict[str, Any]] = []
        packages = max(1, (self.file_count if self.file_count > 0 else self.ops_per_burst) // 10)
        for i in range(packages):
            pkg = node_modules / f"pkg_{i:03d}"
            pkg.mkdir(parents=True, exist_ok=True)
            files = [
                (pkg / f"package_{i:03d}.json", f"fd_rdd_m2_storm_npm_pkg_{self.cycle}_{i:03d}"),
                (pkg / f"index_{i:03d}.js", f"fd_rdd_m2_storm_npm_index_{self.cycle}_{i:03d}"),
                (pkg / f"README_{i:03d}.md", f"fd_rdd_m2_storm_npm_readme_{self.cycle}_{i:03d}"),
            ]
            for path, marker in files:
                path.write_text(f"{utc_now()} {marker}\n", encoding="utf-8")
                records.append(
                    self.expected_record(
                        "npm_install",
                        "npm_node_modules_hidden",
                        path,
                        path.name,
                        False,
                        tier_before,
                    )
                )
        package_manifest = pkg_root / f"fd_rdd_m2_npm_root_{self.cycle:03d}.probe"
        package_manifest.write_text(
            f"{utc_now()} fd_rdd_m2_storm_npm_package_root_{self.cycle}\n",
            encoding="utf-8",
        )
        records.append(
            self.expected_record(
                "npm_install",
                "npm_package_root_visible",
                package_manifest,
                package_manifest.name,
                True,
                tier_before,
            )
        )
        lock = pkg_root / "package-lock.json"
        lock.parent.mkdir(parents=True, exist_ok=True)
        lock.write_text(f"{utc_now()} fd_rdd_m2_storm_npm_lock_{self.cycle}\n", encoding="utf-8")
        records.append(self.expected_record("npm_install", "npm_lock_visible", lock, lock.name, True, tier_before))
        return records

    def write_subtree_rename_avalanche(self, root: Path, tier_before: str) -> list[dict[str, Any]]:
        burst_root = self.burst_root(root, "subtree-rename")
        source = burst_root / "dir_a"
        destination = burst_root / "dir_b"
        records: list[dict[str, Any]] = []
        created: list[tuple[Path, Path]] = []
        deadline = time.monotonic() + self.duration_budget_secs
        # Task 5: --event-storm-file-count overrides the ops-based width; depth
        # comes from --event-storm-depth (default 2, preserving the old layout).
        width = self.file_count if self.file_count > 0 else max(1, min(self.ops_per_burst, 200))
        depth = self.subtree_depth
        for i in range(width):
            parent = source
            for level in range(depth):
                parent = parent / f"level{level + 1}_{(i // (10 ** level)) % 10:02d}"
            parent.mkdir(parents=True, exist_ok=True)
            old_path = parent / (
                f"m2v_{self.query_token}_{self.cycle:03d}_deep_{i:04d}.txt"
            )
            old_path.write_text(
                f"{utc_now()} fd_rdd_m2_storm_subtree_rename_{self.cycle}_{i:04d}\n",
                encoding="utf-8",
            )
            new_path = destination / old_path.relative_to(source)
            created.append((old_path, new_path))
            if time.monotonic() > deadline:
                break
        destination.parent.mkdir(parents=True, exist_ok=True)
        # Fix D: clear any stale destination before the directory rename so
        # rename(2) can never hit ENOTEMPTY on a leftover populated dir_b.
        shutil.rmtree(destination, ignore_errors=True)
        source.rename(destination)
        for old_path, new_path in created:
            records.append(
                self.expected_record(
                    "subtree_rename",
                    "subtree_rename_new_visible",
                    new_path,
                    new_path.name,
                    True,
                    tier_before,
                    {
                        "old_path": str(old_path),
                        "renamed_subtree_from": str(source),
                        "renamed_subtree_to": str(destination),
                    },
                )
            )
            records.append(
                self.expected_record(
                    "subtree_rename",
                    "subtree_rename_old_hidden",
                    old_path,
                    old_path.name,
                    False,
                    tier_before,
                    {
                        "new_path": str(new_path),
                        "renamed_subtree_from": str(source),
                        "renamed_subtree_to": str(destination),
                    },
                )
            )
        return records

    def write_mount_storm_fixture(self, root: Path, tier_before: str) -> list[dict[str, Any]]:
        burst_root = self.burst_root(root, "mount-storm")
        mount_point = burst_root / "mountpoint"
        detached = burst_root / ".detached_mountpoint"
        records: list[dict[str, Any]] = []
        created: list[Path] = []
        deadline = time.monotonic() + self.duration_budget_secs
        width = self.file_count if self.file_count > 0 else max(1, min(self.ops_per_burst, 200))
        for i in range(width):
            parent = mount_point / f"tree_{i % 20:02d}"
            parent.mkdir(parents=True, exist_ok=True)
            path = parent / f"offline_{self.cycle:03d}_{i:04d}.txt"
            path.write_text(
                f"{utc_now()} fd_rdd_m2_storm_mount_storm_{self.cycle}_{i:04d}\n",
                encoding="utf-8",
            )
            created.append(path)
            if time.monotonic() > deadline:
                break
        # Fix D: clear any stale detached dir before the directory rename so
        # rename(2) can never hit ENOTEMPTY on a leftover .detached_mountpoint.
        shutil.rmtree(detached, ignore_errors=True)
        mount_point.rename(detached)
        sample_limit = min(len(created), max(1, min(32, self.ops_per_burst)))
        for old_path in created[:sample_limit]:
            records.append(
                self.expected_record(
                    "mount_storm",
                    "mount_point_offline_old_hidden",
                    old_path,
                    old_path.name,
                    False,
                    tier_before,
                    {
                        "simulated": True,
                        "simulation": "rename fixture mountpoint to a hidden detached directory",
                        "offline_root": str(mount_point),
                        "detached_root": str(detached),
                    },
                )
            )
        return records

    def write_inode_reuse_fixture(self, root: Path, tier_before: str) -> list[dict[str, Any]]:
        burst_root = self.burst_root(root, "inode-reuse")
        burst_root.mkdir(parents=True, exist_ok=True)
        records: list[dict[str, Any]] = []
        deadline = time.monotonic() + self.duration_budget_secs
        width = self.file_count if self.file_count > 0 else max(1, min(self.ops_per_burst, 200))
        for i in range(width):
            old_path = burst_root / f"ghost_old_{self.cycle:03d}_{i:04d}.txt"
            new_path = burst_root / f"ghost_new_{self.cycle:03d}_{i:04d}.txt"
            old_path.write_text(
                f"{utc_now()} fd_rdd_m2_storm_inode_old_{self.cycle}_{i:04d}\n",
                encoding="utf-8",
            )
            old_stat = old_path.stat()
            old_path.unlink()
            new_path.write_text(
                f"{utc_now()} fd_rdd_m2_storm_inode_new_{self.cycle}_{i:04d}\n",
                encoding="utf-8",
            )
            new_stat = new_path.stat()
            inode_reused = (
                old_stat.st_dev == new_stat.st_dev and old_stat.st_ino == new_stat.st_ino
            )
            metadata = {
                "old_dev": old_stat.st_dev,
                "old_inode": old_stat.st_ino,
                "new_dev": new_stat.st_dev,
                "new_inode": new_stat.st_ino,
                "inode_reused": inode_reused,
            }
            records.append(
                self.expected_record(
                    "inode_reuse",
                    "inode_reuse_old_hidden",
                    old_path,
                    old_path.name,
                    False,
                    tier_before,
                    metadata,
                )
            )
            records.append(
                self.expected_record(
                    "inode_reuse",
                    "inode_reuse_new_visible",
                    new_path,
                    new_path.name,
                    True,
                    tier_before,
                    metadata,
                )
            )
            if time.monotonic() > deadline:
                break
        return records

    def write_inode_reuse_stress(self, root: Path, tier_before: str) -> list[dict[str, Any]]:
        """Task 2: dedicated inode-reuse stress test on a small tmpfs.

        Creates a tmpfs with a limited inode pool so delete+recreate in a tight
        loop is very likely to recycle the just-freed inode. When reuse is
        observed we emit expected_records (old path hidden / new path visible)
        carrying old/new dev+inode metadata so the deferred search check verifies
        the generation/filekey ghost-revival defense. Falls back to a plain
        directory (with a warning) when mounting is not permitted.
        """
        burst_root = self.burst_root(root, "inode-reuse-stress")
        burst_root.mkdir(parents=True, exist_ok=True)
        mount_point = burst_root / "stress-tmpfs"
        mount_point.mkdir(parents=True, exist_ok=True)
        records: list[dict[str, Any]] = []
        iterations = self.inode_stress_iterations if self.inode_stress_iterations > 0 else 100
        nr_inodes = self.inode_stress_tmpfs_inodes

        mounted = False
        try:
            subprocess.run(
                ["mount", "-t", "tmpfs", "-o", f"nr_inodes={nr_inodes},size=10m", "tmpfs", str(mount_point)],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            mounted = True
        except Exception:
            # No privileges (or no mount binary): fall back to a regular dir.
            # Inode reuse is less likely here, but the workload still exercises
            # the create/delete/recreate event path and remains correct.
            mounted = False
        work_dir = mount_point
        self.emit(
            {
                "event_kind": "inode_reuse_stress_setup",
                "operation": "inode_reuse_stress_setup",
                "workload": "inode_reuse_stress",
                "root": str(root),
                "mount_point": str(mount_point),
                "tmpfs_mounted": mounted,
                "tmpfs_nr_inodes": nr_inodes if mounted else 0,
                "iterations": iterations,
                "ok": True,
            }
        )

        attempts = 0
        observed = 0
        deadline = time.monotonic() + max(self.duration_budget_secs, 5.0)
        for i in range(iterations):
            old_path = work_dir / f"old_{self.cycle:03d}_{i:05d}.txt"
            new_path = work_dir / f"new_{self.cycle:03d}_{i:05d}.txt"
            try:
                old_path.write_text(
                    f"{utc_now()} fd_rdd_m2_storm_inode_stress_old_{self.cycle}_{i:05d}\n",
                    encoding="utf-8",
                )
                old_stat = old_path.stat()
                old_path.unlink()
                # Tight loop: immediately create a new file in the same directory
                # to maximize the chance the freed inode is recycled.
                new_path.write_text(
                    f"{utc_now()} fd_rdd_m2_storm_inode_stress_new_{self.cycle}_{i:05d}\n",
                    encoding="utf-8",
                )
                new_stat = new_path.stat()
            except OSError:
                # tmpfs inode exhaustion or other FS error: stop early.
                break
            attempts += 1
            reused = old_stat.st_dev == new_stat.st_dev and old_stat.st_ino == new_stat.st_ino
            if not reused:
                if time.monotonic() > deadline:
                    break
                continue
            observed += 1
            metadata = {
                "old_dev": old_stat.st_dev,
                "old_inode": old_stat.st_ino,
                "new_dev": new_stat.st_dev,
                "new_inode": new_stat.st_ino,
                "inode_reused": True,
                "ghost_revival_defense": "old_hidden+new_visible search checks verify generation/filekey defense",
            }
            records.append(
                self.expected_record(
                    "inode_reuse",
                    "inode_reuse_old_hidden",
                    old_path,
                    old_path.name,
                    False,
                    tier_before,
                    metadata,
                )
            )
            records.append(
                self.expected_record(
                    "inode_reuse",
                    "inode_reuse_new_visible",
                    new_path,
                    new_path.name,
                    True,
                    tier_before,
                    metadata,
                )
            )
            if time.monotonic() > deadline:
                break

        # Cleanup: unmount tmpfs if we mounted it, then remove the mountpoint.
        if mounted:
            try:
                subprocess.run(
                    ["umount", str(mount_point)],
                    check=False,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            except Exception:
                pass
        shutil.rmtree(mount_point, ignore_errors=True)

        self.emit(
            {
                "event_kind": "inode_reuse_stress_summary",
                "operation": "inode_reuse_stress_summary",
                "workload": "inode_reuse_stress",
                "root": str(root),
                "inode_reuse_attempts": attempts,
                "inode_reuse_observed": observed,
                "tmpfs_mounted": mounted,
                "ok": True,
            }
        )
        return records

    def write_time_skew_fixture(self, root: Path, tier_before: str) -> list[dict[str, Any]]:
        burst_root = self.burst_root(root, "time-skew")
        burst_root.mkdir(parents=True, exist_ok=True)
        records: list[dict[str, Any]] = []
        deadline = time.monotonic() + self.duration_budget_secs
        width = self.file_count if self.file_count > 0 else max(1, min(self.ops_per_burst, 200))
        skewed_mtime = max(0.0, time.time() - self.time_skew_secs)
        for i in range(width):
            path = burst_root / f"time_skew_{self.cycle:03d}_{i:04d}.txt"
            marker = f"fd_rdd_m2_storm_time_skew_{self.cycle}_{i:04d}"
            path.write_text(f"{utc_now()} {marker}\n", encoding="utf-8")
            os.utime(path, (skewed_mtime, skewed_mtime))
            records.append(
                self.expected_record(
                    "time_skew",
                    "backdated_file_visible",
                    path,
                    path.name,
                    True,
                    tier_before,
                    {
                        "simulated": True,
                        "simulation": "backdate fixture mtime instead of changing system clock",
                        "mtime_skew_secs": self.time_skew_secs,
                    },
                )
            )
            if time.monotonic() > deadline:
                break
        return records

    def burst_root(self, root: Path, kind: str) -> Path:
        return root / f"{EVENT_STORM_DIR_PREFIX}{safe_name(kind)}-{self.run_id}-{self.cycle:03d}"

    @staticmethod
    def burst_directory_kind(kind: str) -> str:
        return {
            "git_clone": "git-clone",
            "npm_install": "npm-install",
            "subtree_rename": "subtree-rename",
            "mount_storm": "mount-storm",
            "inode_reuse": "inode-reuse",
            "inode_reuse_stress": "inode-reuse-stress",
            "time_skew": "time-skew",
        }.get(kind, kind)

    def select_root(self, requested_tier: str) -> Path:
        if self.fixed_root_schedule:
            candidates = [
                root
                for root in sorted(self.roots)
                if root.is_dir()
                and not root.is_symlink()
                and self._stable_fixture_anchor(root) == root.resolve()
            ]
            if not candidates:
                raise RuntimeError("fixed event-storm root schedule has no directory")
            return candidates[self.fixed_root_schedule_index(len(candidates))]
        if requested_tier:
            try:
                dump = debug_tiered_watch(self.base_url)
                dirs = dump.get("dirs")
                if isinstance(dirs, list):
                    candidates_by_path: dict[str, Path] = {}
                    requested_tier = requested_tier.upper()
                    for item in dirs:
                        if not isinstance(item, dict):
                            continue
                        if str(item.get("watch_tier", "")).upper() != requested_tier:
                            continue
                        raw_path = item.get("path")
                        if not isinstance(raw_path, str) or not raw_path:
                            continue
                        anchor = self._stable_fixture_anchor(Path(raw_path))
                        if anchor is not None:
                            candidates_by_path[str(anchor)] = anchor
                    candidates = [
                        candidates_by_path[path]
                        for path in sorted(candidates_by_path)
                    ]
                    if candidates:
                        return candidates[(self.cycle - 1) % len(candidates)]
            except Exception:
                pass
        return self.roots[(self.cycle - 1) % len(self.roots)]

    def fixed_root_schedule_index(self, candidate_count: int) -> int:
        """Choose the same non-repeating root order in both A/B legs.

        Strict protocol preflight, rather than a wall-clock prediction, proves
        the selected root's actual tier and treatment lease before mutation.
        """
        if candidate_count <= 0:
            raise ValueError("candidate_count must be positive")
        return (self.cycle - 1) % candidate_count

    @staticmethod
    def _has_event_storm_component(relative_path: Path) -> bool:
        return any(
            part.startswith(EVENT_STORM_DIR_PREFIX)
            for part in relative_path.parts
        )

    def _stable_fixture_anchor(self, path: Path) -> Path | None:
        """Return a canonical, existing fixture directory safe for storm writes."""

        lexical_path = Path(os.path.abspath(os.fspath(path)))
        try:
            resolved_path = path.resolve(strict=True)
        except (OSError, RuntimeError):
            return None
        if not resolved_path.is_dir():
            return None

        within_configured_root = False
        for root in self.roots:
            try:
                lexical_root = Path(os.path.abspath(os.fspath(root)))
                resolved_root = root.resolve(strict=True)
                resolved_relative = resolved_path.relative_to(resolved_root)
            except (OSError, RuntimeError, ValueError):
                continue

            lexical_relative: Path | None = None
            for candidate_root in (lexical_root, resolved_root):
                try:
                    lexical_relative = lexical_path.relative_to(candidate_root)
                    break
                except ValueError:
                    continue

            if (
                lexical_relative is not None
                and self._has_event_storm_component(lexical_relative)
            ):
                return None
            if self._has_event_storm_component(resolved_relative):
                return None
            within_configured_root = True
        return resolved_path if within_configured_root else None

    def within_configured_roots(self, path: Path) -> bool:
        return self._stable_fixture_anchor(path) is not None

    def expected_record(
        self,
        workload: str,
        operation: str,
        path: Path,
        query: str,
        should_exist: bool,
        tier_before: str,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.mutation_seq += 1
        record = {
            "event_kind": "expected",
            "operation": operation,
            "workload": workload,
            "path": str(path),
            "query": query,
            "should_exist": should_exist,
            "tier_before": tier_before,
            "write_elapsed_secs": round(time.monotonic() - self.started_at, 3),
            "burst_elapsed_secs": round(time.monotonic() - self.current_burst_started_at, 3),
            "mutation_seq": self.mutation_seq,
        }
        if extra:
            record.update(extra)
        return record

    def process_due(self, now: float) -> None:
        assert self.active is not None
        stage = str(self.active.get("stage", "delayed_query"))
        # Task 2: immediate-query two-pass mode. The immediate pass runs at
        # immediate_query_settle_secs (e.g. 5s) and only does a single-shot
        # query (no after_query retry) so it does not block the main loop. After
        # it, we reschedule to the delayed_due_at and return without finishing
        # the cycle. The delayed pass is the full existing behavior.
        if stage == "immediate_query":
            self.run_query_pass(now, phase="immediate")
            self.active["stage"] = "delayed_query"
            self.active["due_at"] = float(self.active["delayed_due_at"])
            self.active["immediate_done"] = True
            return
        # delayed_query (default, backwards compatible)
        self.run_query_pass(now, phase="delayed")
        self.finalize_visibility_probes(time.monotonic())
        self.cleanup_burst_root(
            Path(self.active["burst_root"]),
            Path(self.active["root"]),
            str(self.active.get("selected_kind", "unknown")),
            phase="delayed_query",
        )
        self.active = None
        self.next_start_at = time.monotonic() + self.interval_secs

    @staticmethod
    def estimate_tree_entries(root: Path) -> tuple[int | None, str]:
        if not root.exists():
            return 0, ""
        count = 0
        pending = [root]
        try:
            while pending:
                current = pending.pop()
                with os.scandir(current) as entries:
                    for entry in entries:
                        count += 1
                        if entry.is_dir(follow_symlinks=False):
                            pending.append(Path(entry.path))
        except OSError as exc:
            return None, repr(exc)
        return count, ""

    def cleanup_burst_root(
        self,
        burst_root: Path,
        selected_root: Path,
        workload: str,
        phase: str,
    ) -> None:
        started = time.monotonic()
        entries_estimated, estimate_error = self.estimate_tree_entries(burst_root)
        error = ""
        removed = False
        try:
            if burst_root.parent != selected_root:
                raise ValueError("event-storm cleanup target escaped selected root")
            if not burst_root.name.startswith(EVENT_STORM_DIR_PREFIX):
                raise ValueError("event-storm cleanup target lacks fixture prefix")
            if self.run_id not in burst_root.name:
                raise ValueError("event-storm cleanup target belongs to another run")
            if burst_root.is_symlink():
                raise ValueError("event-storm cleanup target is a symlink")
            if burst_root.exists():
                shutil.rmtree(burst_root)
            removed = not burst_root.exists()
            if not removed:
                raise OSError("event-storm cleanup target still exists")
        except Exception as exc:  # noqa: BLE001 - cleanup failure is benchmark evidence
            error = repr(exc)
        self.emit(
            {
                "event_kind": "burst_cleanup",
                "operation": "burst_cleanup",
                "workload": workload,
                "root": str(selected_root),
                "cleanup_target": str(burst_root),
                "cleanup_phase": phase,
                "entries_estimated": entries_estimated,
                "duration_secs": round(time.monotonic() - started, 3),
                "removed": removed,
                "ok": not error,
                **({"estimate_error": estimate_error} if estimate_error else {}),
                **({"error": error} if error else {}),
            }
        )
        if self.post_cleanup_audit_secs > 0:
            active = self.active if isinstance(self.active, dict) else {}
            fence = active.get("target_m2_fence")
            fence = fence if isinstance(fence, dict) else {}
            lease_expires = int(
                fence.get("target_m2_fence_expires_unix_secs", 0) or 0
            )
            next_rotation = (
                lease_expires - self.rotating_ttl_secs + self.rotating_tick_secs
                if lease_expires > 0
                else 0
            )
            self.pending_cleanup_audits.append(
                {
                    "due_at": time.monotonic() + self.post_cleanup_audit_secs,
                    "selected_root": selected_root,
                    "cleanup_target": burst_root,
                    "workload": workload,
                    "cleanup_removed": removed,
                    "lease_expires_unix_secs": lease_expires,
                    "next_rotation_estimate_unix_secs": next_rotation,
                }
            )

    def poll_post_cleanup_audits(self, now: float) -> None:
        """Record whether expired rotating/ephemeral watcher state has converged."""
        pending: list[dict[str, Any]] = []
        for audit in self.pending_cleanup_audits:
            if now < float(audit["due_at"]):
                pending.append(audit)
                continue
            self.emit_post_cleanup_audit(audit)
        self.pending_cleanup_audits = pending

    def emit_post_cleanup_audit(self, audit: dict[str, Any]) -> None:
        selected_root = Path(audit["selected_root"])
        cleanup_target = Path(audit["cleanup_target"])
        debug_ok = False
        error = ""
        dirs: list[dict[str, Any]] = []
        try:
            dump = debug_tiered_watch(self.base_url, selected_root)
            raw_dirs = dump.get("dirs")
            if not isinstance(raw_dirs, list):
                raise RuntimeError("debug tiered-watch response has no dirs array")
            dirs = [item for item in raw_dirs if isinstance(item, dict)]
            debug_ok = True
        except Exception as exc:  # noqa: BLE001 - audit failure is probe evidence
            error = repr(exc)

        root_str = str(selected_root)
        target_entry = next(
            (item for item in dirs if str(item.get("path", "")) == root_str),
            None,
        )
        cleanup_entries: list[dict[str, Any]] = []
        for item in dirs:
            value = str(item.get("path", ""))
            if not value:
                continue
            candidate = Path(value)
            if candidate == cleanup_target or cleanup_target in candidate.parents:
                cleanup_entries.append(item)

        target_ephemeral = bool(
            target_entry is not None and target_entry.get("ephemeral_watch")
        )
        target_rotating = bool(
            target_entry is not None
            and target_entry.get("rotating_cold_window")
        )
        target_action = (
            str(target_entry.get("rotating_cold_window_action", ""))
            if target_entry is not None
            else ""
        )
        cleanup_ephemeral = sum(
            1 for item in cleanup_entries if bool(item.get("ephemeral_watch"))
        )
        cleanup_rotating = sum(
            1
            for item in cleanup_entries
            if bool(item.get("rotating_cold_window"))
        )
        ledger_cleared = (
            target_entry is not None
            and not target_ephemeral
            and not target_rotating
            and not target_action
            and cleanup_ephemeral == 0
            and cleanup_rotating == 0
        )
        cleanup_target_exists = cleanup_target.exists()
        audited_unix_secs = int(time.time())
        lease_expires = int(audit.get("lease_expires_unix_secs", 0) or 0)
        next_rotation = int(
            audit.get("next_rotation_estimate_unix_secs", 0) or 0
        )
        after_lease_expiry = (
            lease_expires > 0 and audited_unix_secs >= lease_expires
        )
        before_next_rotation = (
            next_rotation > 0 and audited_unix_secs < next_rotation
        )
        self.emit(
            {
                "event_kind": "post_cleanup_audit",
                "operation": "post_cleanup_audit",
                "workload": str(audit.get("workload", "")),
                "root": root_str,
                "cleanup_target": str(cleanup_target),
                "audit_delay_secs": self.post_cleanup_audit_secs,
                "cleanup_removed": bool(audit.get("cleanup_removed")),
                "cleanup_target_exists": cleanup_target_exists,
                "audit_unix_secs": audited_unix_secs,
                "lease_expires_unix_secs": lease_expires,
                "next_rotation_estimate_unix_secs": next_rotation,
                "audit_after_lease_expiry": after_lease_expiry,
                "audit_before_next_rotation": before_next_rotation,
                "audit_window_valid": (
                    after_lease_expiry and before_next_rotation
                ),
                "debug_ok": debug_ok,
                "target_entry_present": target_entry is not None,
                "target_ephemeral_watch": target_ephemeral,
                "target_rotating_active": target_rotating,
                "target_rotating_action": target_action,
                "cleanup_target_entries": len(cleanup_entries),
                "cleanup_target_ephemeral_watch_dirs": cleanup_ephemeral,
                "cleanup_target_rotating_active_dirs": cleanup_rotating,
                "watcher_ledger_cleared": ledger_cleared,
                "ok": (
                    debug_ok
                    and bool(audit.get("cleanup_removed"))
                    and not cleanup_target_exists
                    and ledger_cleared
                ),
                **({"error": error} if error else {}),
            }
        )

    def run_query_pass(self, now: float, phase: str) -> None:
        """Run a single query pass over the active burst's expected events.

        phase is "immediate" or "delayed". The immediate pass skips the
        after_query retry loop (it would block) and is tagged separately so the
        summary can report immediate vs delayed success rates.
        """
        assert self.active is not None
        events = list(self.active["events"])
        root = Path(self.active["root"])
        tier_after = self.tier_for_root(root)
        ok_count = 0
        positive_total = 0
        positive_ok = 0
        latencies: list[float] = []
        do_retry = phase == "delayed" and self.timeout_secs > 0
        for event in events:
            event_details = self.event_details(event)
            ok, exists, latency, error = check_search_state_once(
                self.base_url,
                str(event["query"]),
                Path(event["path"]),
                bool(event["should_exist"]),
            )
            query_completed_at = time.monotonic()
            if ok:
                ok_count += 1
                latencies.append(latency)
            if bool(event["should_exist"]):
                positive_total += 1
                if ok:
                    positive_ok += 1
            self.emit(
                {
                    "event_kind": "first_query",
                    "operation": str(event["operation"]) + "_first_query",
                    "workload": event["workload"],
                    "path": event["path"],
                    "query": event["query"],
                    "should_exist": event["should_exist"],
                    "ok": ok,
                    "correct": exists == bool(event["should_exist"]),
                    "transport_ok": not bool(error),
                    "first_query_exists": exists,
                    "latency_secs": round(latency, 3),
                    "query_phase": phase,
                    "settle_secs": round(
                        query_completed_at - float(self.active["started_at"]), 3
                    ),
                    "event_age_secs": round(
                        query_completed_at
                        - float(self.active["started_at"])
                        - float(event.get("burst_elapsed_secs", 0.0)),
                        3,
                    ),
                    "burst_elapsed_secs": float(event.get("burst_elapsed_secs", 0.0)),
                    "write_elapsed_secs": float(event.get("write_elapsed_secs", 0.0)),
                    "requested_tier": self.active.get("requested_tier", ""),
                    "tier_before": event.get("tier_before", ""),
                    "tier_after": tier_after,
                    **event_details,
                    **({"error": error} if error else {}),
                }
            )
            if not ok and do_retry:
                after_ok, after_latency, after_polls = wait_search_state(
                    self.base_url,
                    str(event["query"]),
                    Path(event["path"]),
                    bool(event["should_exist"]),
                    self.timeout_secs,
                )
                after_completed_at = time.monotonic()
                self.emit(
                    {
                        "event_kind": "after_query",
                        "operation": str(event["operation"]) + "_after_query",
                        "workload": event["workload"],
                        "path": event["path"],
                        "query": event["query"],
                        "should_exist": event["should_exist"],
                        "ok": after_ok,
                        "latency_secs": round(after_latency, 3),
                        "polls": after_polls,
                        "query_phase": phase,
                        "event_age_secs": round(
                            after_completed_at
                            - float(self.active["started_at"])
                            - float(event.get("burst_elapsed_secs", 0.0)),
                            3,
                        ),
                        "burst_elapsed_secs": float(event.get("burst_elapsed_secs", 0.0)),
                        "write_elapsed_secs": float(event.get("write_elapsed_secs", 0.0)),
                        "requested_tier": self.active.get("requested_tier", ""),
                        "tier_before": event.get("tier_before", ""),
                        "tier_after": tier_after,
                        **event_details,
                    }
                )
        target_m2_after = {
            key.replace("target_m2_", "target_m2_after_", 1): value
            for key, value in self.m2_evidence_for_root(root).items()
        }
        total = len(events)
        self.emit(
            {
                "event_kind": "burst_checked",
                "operation": "burst_checked",
                "root": str(root),
                "events_total": total,
                "ok": ok_count,
                "missed": total - ok_count,
                "success_rate": round(ok_count / total, 4) if total else 0.0,
                "positive_total": positive_total,
                "positive_ok": positive_ok,
                "positive_success_rate": (
                    round(positive_ok / positive_total, 4) if positive_total else 0.0
                ),
                "first_query_p50_secs": round(percentile(latencies, 50), 3),
                "first_query_p95_secs": round(percentile(latencies, 95), 3),
                "query_phase": phase,
                "requested_tier": self.active.get("requested_tier", ""),
                "tier_before": self.active.get("tier_before", ""),
                "tier_after": tier_after,
                "mutation_completed_unix_secs": int(
                    self.active.get("mutation_completed_unix_secs", 0) or 0
                ),
                **target_m2_after,
                "visibility_poll_count": sum(
                    int(probe.get("polls", 0) or 0)
                    for probe in self.active.get("visibility_probes", [])
                ),
            }
        )

    def event_details(self, event: dict[str, Any]) -> dict[str, Any]:
        core_keys = {
            "event_kind",
            "operation",
            "workload",
            "path",
            "query",
            "should_exist",
            "tier_before",
            "write_elapsed_secs",
            "burst_elapsed_secs",
        }
        return {key: value for key, value in event.items() if key not in core_keys}

    def _emit_tier_distribution(self, phase: str) -> None:
        """Task 3: capture directory tier counts and emit them to the JSONL log.

        Called at the start of the first event-storm burst (phase='storm_start')
        so the summary can report how many directories demoted during the settle
        phase. Also prints to stdout for live progress.
        """
        try:
            dump = debug_tiered_watch(self.base_url)
            counts = compute_tier_distribution(dump)
            total = sum(counts.values())
            self.emit(
                {
                    "event_kind": "tier_distribution",
                    "operation": "tier_distribution",
                    "phase": phase,
                    "tier_counts": counts,
                    "total_dirs": total,
                }
            )
            print(
                f"Tier distribution ({phase}): {counts} (total={total})",
                flush=True,
            )
        except Exception:
            pass

    def tier_for_root(self, root: Path) -> str:
        try:
            root_path = Path(root)
            configured_ancestors = [
                Path(candidate)
                for candidate in self.roots
                if Path(candidate) == root_path or Path(candidate) in root_path.parents
            ]
            debug_anchor = (
                max(configured_ancestors, key=lambda path: len(path.parts))
                if configured_ancestors
                else root_path
            )
            dump = debug_tiered_watch(self.base_url, debug_anchor)
            dirs = dump.get("dirs")
            if not isinstance(dirs, list):
                return ""
            ancestors: list[tuple[int, dict[str, Any]]] = []
            for item in dirs:
                if not isinstance(item, dict) or not item.get("path"):
                    continue
                candidate_path = Path(str(item["path"]))
                if candidate_path == root_path or candidate_path in root_path.parents:
                    ancestors.append((len(candidate_path.parts), item))
            if ancestors:
                nearest = max(ancestors, key=lambda row: row[0])[1]
                return str(nearest.get("watch_tier", ""))
            return ""
        except Exception:
            return ""

    def m2_evidence_for_root(self, root: Path) -> dict[str, Any]:
        """Capture one target-specific M2 lease and progress snapshot."""
        observed_unix_secs = int(time.time())
        empty_evidence = {
            "target_m2_seen": False,
            "target_m2_active": False,
            "target_m2_action": "",
            "target_m2_cycle_id": 0,
            "target_m2_expires_unix_secs": 0,
            "target_m2_last_scan_unix_secs": 0,
            "target_m2_last_event_unix_secs": 0,
            "target_m2_scan_seq": 0,
            "target_m2_scan_cycle_id": 0,
            "target_m2_event_seq": 0,
            "target_m2_event_cycle_id": 0,
            "target_m2_observed_unix_secs": observed_unix_secs,
        }
        try:
            dump = debug_tiered_watch(self.base_url, root)
            dirs = dump.get("dirs")
            if not isinstance(dirs, list):
                raise RuntimeError("debug tiered-watch response has no dirs array")
            root_str = str(root)
            item = next(
                (
                    candidate
                    for candidate in dirs
                    if isinstance(candidate, dict)
                    and candidate.get("path") == root_str
                ),
                None,
            )
            if item is None:
                return {
                    "target_m2_debug_ok": True,
                    "target_m2_entry_present": False,
                    **empty_evidence,
                }
            return {
                "target_m2_debug_ok": True,
                "target_m2_entry_present": True,
                "target_m2_seen": bool(item.get("rotating_cold_window_seen")),
                "target_m2_active": bool(item.get("rotating_cold_window")),
                "target_m2_action": str(
                    item.get("rotating_cold_window_action", "")
                ),
                "target_m2_cycle_id": int(
                    item.get("rotating_cold_window_cycle_id", 0) or 0
                ),
                "target_m2_expires_unix_secs": int(
                    item.get("rotating_cold_window_expires_unix_secs", 0) or 0
                ),
                "target_m2_last_scan_unix_secs": int(item.get("last_scan", 0) or 0),
                "target_m2_last_event_unix_secs": int(item.get("last_event", 0) or 0),
                "target_m2_scan_seq": int(
                    item.get("rotating_cold_window_last_scan_seq", 0) or 0
                ),
                "target_m2_scan_cycle_id": int(
                    item.get("rotating_cold_window_last_scan_cycle_id", 0) or 0
                ),
                "target_m2_event_seq": int(
                    item.get("rotating_cold_window_last_event_seq", 0) or 0
                ),
                "target_m2_event_cycle_id": int(
                    item.get("rotating_cold_window_last_event_cycle_id", 0) or 0
                ),
                "target_m2_observed_unix_secs": observed_unix_secs,
            }
        except Exception as exc:  # noqa: BLE001 - evidence failure must gate the leg
            return {
                "target_m2_debug_ok": False,
                "target_m2_entry_present": False,
                **empty_evidence,
                "target_m2_debug_error": repr(exc),
            }

    def reject_protocol_precondition(
        self,
        root: Path,
        requested_tier: str,
        selected_kind: str,
        tier_before: str,
        error: str,
        evidence: dict[str, Any],
    ) -> None:
        self.protocol_error = error
        self.emit(
            {
                "event_kind": "protocol_precondition_failed",
                "operation": "protocol_precondition_failed",
                "root": str(root),
                "requested_tier": requested_tier,
                "selected_kind": selected_kind,
                "tier_before": tier_before,
                "fixed_root_schedule": self.fixed_root_schedule,
                "error": error,
                "ok": False,
                **evidence,
            }
        )

    def protocol_precondition_error(
        self,
        requested_tier: str,
        observed_tier: str,
        evidence: dict[str, Any],
    ) -> str:
        """Return a strict-protocol error before the workload mutates the fixture."""
        if not self.strict_protocol:
            return ""
        normalized_requested = requested_tier.upper()
        normalized_observed = observed_tier.upper()
        if normalized_requested and normalized_requested != normalized_observed:
            return (
                f"requested {normalized_requested} but observed "
                f"{normalized_observed or 'unknown'}"
            )
        if evidence.get("target_m2_debug_ok") is not True:
            return "target M2 debug request failed"
        if self.treatment_enabled:
            if evidence.get("target_m2_entry_present") is not True:
                return "treatment target has no exact M2 entry"
            if not evidence.get("target_m2_active"):
                return "treatment target has no active M2 lease"
            action = str(evidence.get("target_m2_action", ""))
            if action not in {"ephemeral_watch", "fast_scan_lease", "scan_only"}:
                return f"treatment target has invalid M2 action: {action or 'missing'}"
            observed = int(evidence.get("target_m2_observed_unix_secs", 0) or 0)
            expires = int(evidence.get("target_m2_expires_unix_secs", 0) or 0)
            if observed <= 0 or expires <= observed:
                return "treatment target M2 lease is expired"
            remaining = expires - observed
            if remaining < self.min_lease_remaining_secs:
                return (
                    f"treatment target M2 lease remaining {remaining}s is below "
                    f"minimum {self.min_lease_remaining_secs:g}s"
                )
            return ""
        baseline_activity = any(
            (
                bool(evidence.get("target_m2_seen")),
                bool(evidence.get("target_m2_active")),
                bool(evidence.get("target_m2_action")),
                int(evidence.get("target_m2_cycle_id", 0) or 0) != 0,
                int(evidence.get("target_m2_expires_unix_secs", 0) or 0) != 0,
                int(evidence.get("target_m2_scan_seq", 0) or 0) != 0,
                int(evidence.get("target_m2_scan_cycle_id", 0) or 0) != 0,
                int(evidence.get("target_m2_event_seq", 0) or 0) != 0,
                int(evidence.get("target_m2_event_cycle_id", 0) or 0) != 0,
            )
        )
        return "baseline target unexpectedly has M2 activity" if baseline_activity else ""

    def emit(self, record: dict[str, Any]) -> None:
        record.setdefault("ok", True)
        record["cycle"] = self.cycle
        record["ts"] = utc_now()
        record["elapsed_secs"] = round(time.monotonic() - self.started_at, 3)
        json_line(self.out_path, record)


class HotChurnRunner:
    """Task 3: background thread that continuously churns files in hot roots.

    Simulates real L0 hot-layer pressure (IDE saves, git operations, build
    artifacts) while cold rotation is trying to work. Every 2-5 seconds it
    creates/modifies/deletes 10-50 files in hot roots, then periodically issues
    a search query against a recently-created file to measure hot-layer query
    latency. Records are written to hot-churn-samples.jsonl.
    """

    def __init__(
        self,
        base_url: str,
        roots: list[Path],
        out_path: Path,
        started_at: float,
        interval_secs: float = 3.0,
        start_delay_secs: float = 30.0,
        workload_seed: int = 42,
    ) -> None:
        self.base_url = base_url
        self.roots = [r for r in roots if r]
        self.out_path = out_path
        self.started_at = started_at
        self.interval_secs = max(0.5, interval_secs)
        self.start_delay_secs = max(0.0, start_delay_secs)
        self.random = random.Random(workload_seed)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._run_id = f"{time.time_ns()}-{os.getpid()}"

    def start(self) -> None:
        if not self.roots:
            return
        self._thread = threading.Thread(target=self._loop, daemon=True, name="hot-churn")
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None

    def _loop(self) -> None:
        # Wait for the start delay, then churn continuously.
        if self._stop.wait(self.start_delay_secs):
            return
        batch = 0
        while not self._stop.is_set():
            batch += 1
            try:
                self._churn_batch(batch)
            except Exception as exc:  # noqa: BLE001 - keep churning on errors
                self._emit({
                    "event_kind": "hot_churn_error",
                    "operation": "hot_churn_error",
                    "batch": batch,
                    "ok": False,
                    "error": repr(exc),
                })
            # Jittered sleep 2-5 seconds.
            jitter = self.random.uniform(
                max(1.0, self.interval_secs - 1.0), self.interval_secs + 2.0
            )
            if self._stop.wait(jitter):
                return

    def _churn_batch(self, batch: int) -> None:
        root = self.roots[(batch - 1) % len(self.roots)]
        churn_dir = root / f"fd-rdd-m2-hot-churn-{self._run_id}-{batch:05d}"
        churn_dir.mkdir(parents=True, exist_ok=True)
        n_files = self.random.randint(10, 50)
        created: list[Path] = []
        churn_started = time.monotonic()
        for i in range(n_files):
            kind = self.random.choice(["ide_save", "git_op", "build_artifact"])
            path = churn_dir / f"{kind}_{i:03d}.txt"
            marker = f"fd_rdd_m2_hot_churn_{batch}_{i:03d}"
            path.write_text(f"{utc_now()} {marker}\n", encoding="utf-8")
            created.append(path)
        # Modify a few (simulate IDE re-saves).
        for path in created[: max(1, n_files // 4)]:
            with path.open("a", encoding="utf-8") as f:
                f.write("resave\n")
        # Delete a few (simulate build cleanup).
        for path in created[: max(1, n_files // 5)]:
            path.unlink(missing_ok=True)
        write_secs = time.monotonic() - churn_started
        self._emit({
            "event_kind": "hot_churn_batch",
            "operation": "hot_churn_batch",
            "batch": batch,
            "root": str(root),
            "files_created": n_files,
            "write_secs": round(write_secs, 3),
            "ok": True,
        })
        # Hot-layer query latency: query one of the just-created files.
        query_target = created[len(created) // 2] if created else None
        if query_target is not None and query_target.exists():
            ok, exists, latency, error = check_search_state_once(
                self.base_url,
                query_target.name,
                query_target,
                True,
            )
            self._emit({
                "event_kind": "hot_layer_query",
                "operation": "hot_layer_query",
                "batch": batch,
                "path": str(query_target),
                "query": query_target.name,
                "ok": ok,
                "correct": exists,
                "transport_ok": not bool(error),
                "first_query_exists": exists,
                "should_exist": True,
                "latency_secs": round(latency, 3),
                **({"error": error} if error else {}),
            })
        # Best-effort cleanup so churn dirs don't grow unbounded.
        try:
            shutil.rmtree(churn_dir, ignore_errors=True)
        except Exception:
            pass

    def _emit(self, record: dict[str, Any]) -> None:
        record.setdefault("ok", False)
        record["ts"] = utc_now()
        record["elapsed_secs"] = round(time.monotonic() - self.started_at, 3)
        json_line(self.out_path, record)


def benchmark_shutdown_reconcile_paths(
    passive_root: Path,
    active_canary_root: Path | None,
    event_storm: EventStormRunner | None,
    mixed_workload_enabled: bool,
    hot_roots: list[Path],
) -> list[Path]:
    """Collect benchmark-owned parents that may still need negative facts."""
    candidates = [passive_root]
    if active_canary_root is not None:
        candidates.append(active_canary_root)
    if event_storm is not None:
        active = event_storm.active
        if isinstance(active, dict) and active.get("burst_root") is not None:
            candidates.append(Path(active["burst_root"]))
        candidates.extend(event_storm.roots)
    if mixed_workload_enabled:
        candidates.extend(hot_roots)
    return bounded_unique_paths(candidates)


def resolve_root_paths(csv_value: str) -> list[Path]:
    """Resolve a comma-separated list of root paths, skipping empties."""
    return [Path(p).expanduser().resolve() for p in split_csv(csv_value)]


def validate_realistic_fixture(args: argparse.Namespace) -> list[Path]:
    """Task 1/6: validate that the realistic fixture roots exist.

    Returns the list of all configured roots (hot + warm + cold). Prints a
    helpful error and raises SystemExit if the fixture is missing.
    """
    hot = resolve_root_paths(args.hot_roots)
    warm = resolve_root_paths(args.warm_roots)
    cold = resolve_root_paths(args.cold_roots)
    all_paths = hot + warm + cold
    missing = [p for p in all_paths if not p.exists()]
    if missing:
        print(
            "ERROR: realistic-mode fixture roots not found:\n"
            + "\n".join(f"  - {p}" for p in missing)
            + "\n\nThe realistic 1M-file fixture has not been created yet.\n"
            "Ask the fixture-builder teammate to generate it, or point "
            "--hot-roots/--warm-roots/--cold-roots at an existing fixture.",
            file=sys.stderr,
        )
        raise SystemExit(2)
    return all_paths


def pick_fixture_canary_dirs(cold_roots: list[Path], hot_roots: list[Path]) -> tuple[Path | None, Path | None]:
    """Task 6: pick a writable subdirectory inside a cold root (passive canary)
    and a hot root (active canary). Returns (passive_dir, active_dir)."""
    passive_dir: Path | None = None
    active_dir: Path | None = None
    for root in cold_roots:
        candidate = root / ".m2-fixture-canary"
        try:
            candidate.mkdir(parents=True, exist_ok=True)
            passive_dir = candidate
            break
        except OSError:
            continue
    for root in hot_roots:
        candidate = root / ".m2-fixture-canary"
        try:
            candidate.mkdir(parents=True, exist_ok=True)
            active_dir = candidate
            break
        except OSError:
            continue
    return passive_dir, active_dir


def write_config(args: argparse.Namespace, config_home: Path) -> Path:
    cfg_dir = config_home / "fd-rdd"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    cfg_path = cfg_dir / "config.toml"
    roots = ", ".join(toml_string(str(Path(root).expanduser().resolve())) for root in args.root)
    lines = [
        f"roots = [{roots}]",
        f"http_port = {args.port}",
        f"snapshot_interval_secs = {args.snapshot_interval_secs}",
        f"include_hidden = {str(args.include_hidden).lower()}",
        "watch_enabled = true",
        f"watch_mode = {toml_string(args.watch_mode)}",
        f"runtime_profile = {toml_string(args.runtime_profile.replace('-', '_'))}",
        "",
        "[tiered_watch]",
        f"profile = {toml_string(args.tiered_profile)}",
        f"rotating_cold_window_enabled = {str(args.rotating_cold_window).lower()}",
        f"rotating_cold_window_budget = {args.rotating_budget}",
        f"rotating_cold_window_tick_secs = {args.rotating_tick_secs}",
        f"rotating_cold_window_ttl_secs = {args.rotating_ttl_secs}",
        f"rotating_cold_window_max_cost_per_root = {args.rotating_max_cost_per_root}",
        f"rotating_cold_window_max_dirs_per_tick = {args.rotating_max_dirs_per_tick}",
        f"max_watch_dirs = {args.max_watch_dirs}",
        f"l0_max_cost_per_root = {args.l0_max_cost_per_root}",
        f"l1_scan_interval_secs = {args.l1_scan_interval_secs}",
        f"l2_scan_interval_secs = {args.l2_scan_interval_secs}",
        f"l3_scan_interval_secs = {args.l3_scan_interval_secs}",
        f"l1_empty_scans_to_l2 = {args.l1_empty_scans_to_l2}",
        f"l2_empty_scans_to_l3 = {args.l2_empty_scans_to_l3}",
        f"l1_l2_fast_scan_enabled = {str(args.fast_scan).lower()}",
        "",
        "[proc_sampler]",
        f"enabled = {str(args.proc_sampler).lower()}",
        "",
    ]
    cfg_path.write_text("\n".join(lines), encoding="utf-8")
    return cfg_path


def build_if_needed(
    repo: Path,
    binary: Path,
    build: str,
    source_git_sha: str,
    artifact_provenance_receipt: Path | None = None,
) -> dict[str, Any]:
    cargo_lock = repo / "Cargo.lock"
    provenance: dict[str, Any] = {
        "verified": False,
        "build_mode": build,
        "built_this_run": False,
        "source_git_sha": source_git_sha,
        "cargo_lock_sha256": sha256_file(cargo_lock),
        "cargo_args": list(BUILD_CARGO_ARGS),
    }
    if build == "never" or (build == "auto" and binary.exists()):
        if artifact_provenance_receipt is not None:
            receipt_path = artifact_provenance_receipt.expanduser().resolve()
            errors = validate_build_receipt(
                receipt_path,
                repo,
                binary,
                source_git_sha,
            )
            current_dirty = git_worktree_dirty(repo)
            if current_dirty is not False:
                errors.append(
                    "current_worktree_dirty"
                    if current_dirty
                    else "current_worktree_state_unavailable"
                )
            receipt = load_build_receipt(receipt_path)
            provenance.update(
                {
                    "verified": not errors,
                    "receipt_path": str(receipt_path),
                    "receipt_sha256": sha256_file(receipt_path),
                    "receipt_validation_errors": errors,
                    "current_worktree_dirty": current_dirty,
                    "validated_binary_sha256": str(
                        receipt.get("binary_sha256", "")
                    )
                    if not errors
                    else "",
                    "compiler_artifact": str(
                        receipt.get("compiler_artifact", "")
                    ),
                    "cargo_args": list(receipt.get("cargo_args", []) or []),
                }
            )
        return provenance
    pre_build_dirty = git_worktree_dirty(repo)
    pre_build_lock_sha = sha256_file(cargo_lock)
    result = subprocess.run(
        provenance["cargo_args"],
        cwd=repo,
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    artifact = compiler_artifact_path(result.stdout or "")
    post_build_sha = git_head_sha(repo)
    post_build_dirty = git_worktree_dirty(repo)
    post_build_lock_sha = sha256_file(cargo_lock)
    binary_sha256 = sha256_file(binary)
    provenance["built_this_run"] = True
    provenance["pre_build_git_dirty"] = pre_build_dirty
    provenance["post_build_git_sha"] = post_build_sha
    provenance["post_build_git_dirty"] = post_build_dirty
    provenance["post_build_cargo_lock_sha256"] = post_build_lock_sha
    provenance["compiler_artifact"] = str(artifact or "")
    provenance["validated_binary_sha256"] = binary_sha256
    provenance["verified"] = bool(
        source_git_sha
        and post_build_sha == source_git_sha
        and pre_build_dirty is False
        and post_build_dirty is False
        and pre_build_lock_sha
        and post_build_lock_sha == pre_build_lock_sha
        and artifact == binary.resolve()
        and binary_sha256
    )
    return provenance


def stage_execution_binary(
    source_binary: Path,
    run_dir: Path,
    expected_sha256: str,
) -> tuple[Path, str]:
    """把已验证 artifact 固定为腿私有只读副本，隔离并发 Cargo 替换。"""
    source_binary = source_binary.resolve()
    source_before = sha256_file(source_binary)
    if not source_before or (expected_sha256 and source_before != expected_sha256):
        raise RuntimeError("artifact_binary_identity_changed_before_staging")
    artifact_dir = run_dir / "artifact"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    staged = artifact_dir / "fd-rdd"
    temporary = artifact_dir / ".fd-rdd.next"
    shutil.copyfile(source_binary, temporary)
    temporary.chmod(0o500)
    temporary.replace(staged)
    source_after = sha256_file(source_binary)
    staged_sha256 = sha256_file(staged)
    if source_after != source_before or staged_sha256 != source_before:
        raise RuntimeError("artifact_binary_identity_changed_during_staging")
    return staged, staged_sha256


def collect_endpoint_samples(base_url: str, out: Path, started_at: float) -> None:
    for endpoint in ENDPOINTS:
        request_started_elapsed_secs = round(time.monotonic() - started_at, 3)
        record = {
            "request_started_elapsed_secs": request_started_elapsed_secs,
            "endpoint": endpoint,
        }
        try:
            record["ok"] = True
            record["data"] = http_json(base_url, endpoint, timeout=4.0)
        except Exception as exc:  # noqa: BLE001 - written as benchmark evidence
            record["ok"] = False
            record["error"] = repr(exc)
        record["ts"] = utc_now()
        record["elapsed_secs"] = round(time.monotonic() - started_at, 3)
        json_line(out, record)


L2_TIMELINE_COMPONENTS = (
    "estimated_bytes",
    "arena_bytes",
    "filekey_to_docid_bytes",
    "trigram_bytes",
    "parent_index_bytes",
    "parent_path_lookup_bytes",
)


def classify_memory_phase(data: dict[str, Any]) -> str:
    """Classify a /memory sample by index lifecycle state."""
    rebuild = data.get("rebuild") if isinstance(data.get("rebuild"), dict) else {}
    base = data.get("base") if isinstance(data.get("base"), dict) else {}
    if bool(rebuild.get("in_progress")):
        return "rebuild"
    if str(rebuild.get("owned_snapshot_state", "none")) in {"pending", "writing"}:
        return "initial_build_publish"
    if int(base.get("hot_memory_entries", 0) or 0) > 0:
        return "hot_base_snapshot"
    if (
        int(base.get("manifest_only_entries", 0) or 0) > 0
        or int(base.get("cold_segment_count", 0) or 0) > 0
    ):
        return "cold_steady"
    return "unclassified"


def build_memory_timeline(
    endpoint_samples: list[dict[str, Any]],
    process_samples: list[dict[str, Any]],
    shutdown_signal_elapsed_secs: float | None = None,
) -> dict[str, Any]:
    """Summarize lifecycle RSS peaks and the requested L2 component time series."""
    memory_points: list[dict[str, Any]] = []
    for item in endpoint_samples:
        data = item.get("data")
        if not (
            item.get("ok")
            and item.get("endpoint") == "/memory"
            and isinstance(data, dict)
        ):
            continue
        base = data.get("base") if isinstance(data.get("base"), dict) else {}
        l2 = data.get("l2") if isinstance(data.get("l2"), dict) else {}
        rebuild = data.get("rebuild") if isinstance(data.get("rebuild"), dict) else {}
        memory_points.append(
            {
                "elapsed_secs": float(item.get("elapsed_secs", 0.0) or 0.0),
                "phase": classify_memory_phase(data),
                "owned_snapshot_state": str(
                    rebuild.get("owned_snapshot_state", "none")
                ),
                "endpoint_rss_bytes": int(data.get("process_rss_bytes", 0) or 0),
                "base_hot_memory_entries": int(base.get("hot_memory_entries", 0) or 0),
                "base_manifest_only_entries": int(
                    base.get("manifest_only_entries", 0) or 0
                ),
                "base_cold_mmap_bytes": int(base.get("cold_mmap_bytes", 0) or 0),
                "non_index_private_dirty_bytes": int(
                    data.get("non_index_private_dirty_bytes", 0) or 0
                ),
                **{key: int(l2.get(key, 0) or 0) for key in L2_TIMELINE_COMPONENTS},
            }
        )
    memory_points.sort(key=lambda point: point["elapsed_secs"])
    reached_cold_steady = False
    for point in memory_points:
        if point["phase"] == "cold_steady":
            reached_cold_steady = True
        elif point["phase"] == "hot_base_snapshot" and not reached_cold_steady:
            point["phase"] = "initial_build_publish"

    elapsed_points = [float(point["elapsed_secs"]) for point in memory_points]
    def nearest_phase(elapsed_secs: float) -> str:
        if (
            shutdown_signal_elapsed_secs is not None
            and elapsed_secs >= shutdown_signal_elapsed_secs
        ):
            return "final_snapshot_window"
        if not memory_points:
            return "unclassified"
        idx = bisect.bisect_left(elapsed_points, elapsed_secs)
        if idx <= 0:
            return str(memory_points[0]["phase"])
        if idx >= len(memory_points):
            return str(memory_points[-1]["phase"])
        before = memory_points[idx - 1]
        after = memory_points[idx]
        if elapsed_secs - float(before["elapsed_secs"]) <= float(after["elapsed_secs"]) - elapsed_secs:
            return str(before["phase"])
        return str(after["phase"])

    process_by_phase: dict[str, list[tuple[float, int]]] = {}
    for item in process_samples:
        elapsed = float(item.get("elapsed_secs", 0.0) or 0.0)
        phase = nearest_phase(elapsed)
        process_by_phase.setdefault(phase, []).append(
            (elapsed, int(item.get("vmrss_bytes", 0) or 0))
        )

    phase_peaks: dict[str, dict[str, Any]] = {}
    for phase in (
        "rebuild",
        "initial_build_publish",
        "cold_steady",
        "hot_base_snapshot",
        "final_snapshot_window",
    ):
        endpoint_rows = [point for point in memory_points if point["phase"] == phase]
        process_rows = process_by_phase.get(phase, [])
        endpoint_rss = [int(point["endpoint_rss_bytes"]) for point in endpoint_rows]
        process_rss = [rss for _elapsed, rss in process_rows]
        process_peak = max(process_rows, key=lambda row: row[1]) if process_rows else (0.0, 0)
        endpoint_peak = (
            max(endpoint_rows, key=lambda point: int(point["endpoint_rss_bytes"]))
            if endpoint_rows
            else None
        )
        phase_peaks[phase] = {
            "endpoint_sample_count": len(endpoint_rows),
            "process_sample_count": len(process_rows),
            "endpoint_rss_bytes_p95": int(percentile(endpoint_rss, 95)),
            "endpoint_rss_bytes_max": max(endpoint_rss) if endpoint_rss else 0,
            "endpoint_peak_elapsed_secs": (
                round(float(endpoint_peak["elapsed_secs"]), 3) if endpoint_peak else 0.0
            ),
            "process_rss_bytes_p95": int(percentile(process_rss, 95)),
            "process_rss_bytes_max": process_peak[1],
            "process_peak_elapsed_secs": round(process_peak[0], 3),
        }

    component_summaries: dict[str, dict[str, Any]] = {}
    for key in L2_TIMELINE_COMPONENTS:
        values = [int(point[key]) for point in memory_points]
        max_point = max(memory_points, key=lambda point: int(point[key])) if memory_points else None
        component_summaries[key] = {
            "first": values[0] if values else 0,
            "last": values[-1] if values else 0,
            "p95": int(percentile(values, 95)),
            "max": max(values) if values else 0,
            "max_elapsed_secs": round(float(max_point["elapsed_secs"]), 3) if max_point else 0.0,
        }

    series_cap = 256
    series = memory_points
    if len(series) > series_cap:
        required = {0, len(series) - 1}
        for phase in {str(point["phase"]) for point in series}:
            phase_indices = [
                idx for idx, point in enumerate(series) if point["phase"] == phase
            ]
            required.add(phase_indices[0])
            required.add(phase_indices[-1])
            required.add(
                max(
                    phase_indices,
                    key=lambda idx: int(series[idx]["endpoint_rss_bytes"]),
                )
            )
        for idx in range(1, len(series)):
            if series[idx - 1]["phase"] != series[idx]["phase"]:
                required.add(idx - 1)
                required.add(idx)
        for key in L2_TIMELINE_COMPONENTS:
            required.add(max(range(len(series)), key=lambda idx: int(series[idx][key])))

        remaining = max(0, series_cap - len(required))
        if remaining > 0:
            denominator = max(1, remaining - 1)
            for idx in range(remaining):
                required.add(round(idx * (len(series) - 1) / denominator))
        series = [series[idx] for idx in sorted(required)[:series_cap]]

    return {
        "phase_peaks": phase_peaks,
        "limitations": {
            "final_snapshot_window_bounded": shutdown_signal_elapsed_secs is not None,
            "periodic_snapshot_lifecycle_available": False,
            "periodic_snapshot_phase_attribution": (
                "unavailable_without_daemon_snapshot_lifecycle_telemetry"
            ),
            "owned_snapshot_l2_semantics": (
                "pending/writing L2 components preserve the captured generation "
                "high-water mark; after ownership consumption they are conservative, "
                "not the writer's real-time external-sort working set"
            ),
            "formal_ab_fixture_reset": (
                "restore the same pristine read-only VM/disk snapshot before each leg; "
                "a verified manifest does not prove the current tree or page-cache state"
            ),
            "formal_ab_sweep_reuse": (
                "sequential sweep variants are exploratory only and must not replace "
                "per-leg VM snapshot restoration for formal memory A/B"
            ),
        },
        "l2": {
            "sample_count": len(memory_points),
            "components": component_summaries,
            "series": series,
        },
    }


def summarize_visibility_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    unique: dict[str, dict[str, Any]] = {}
    for index, row in enumerate(rows):
        path = str(row.get("path", ""))
        unique.setdefault(path or f"<missing-path-{index}>", row)
    selected = list(unique.values())
    visible = [row for row in selected if bool(row.get("visible"))]
    latencies = [float(row.get("latency_secs", 0.0) or 0.0) for row in visible]
    by_workload: dict[str, dict[str, Any]] = {}
    for workload in sorted({str(row.get("workload", "")) for row in selected}):
        if not workload:
            continue
        workload_rows = [row for row in selected if row.get("workload") == workload]
        workload_visible = sum(1 for row in workload_rows if bool(row.get("visible")))
        by_workload[workload] = {
            "total": len(workload_rows),
            "visible": workload_visible,
            "success_rate": round(workload_visible / len(workload_rows), 4),
        }
    return {
        "total": len(selected),
        "visible": len(visible),
        "timeouts": len(selected) - len(visible),
        "success_rate": round(len(visible) / len(selected), 4) if selected else 0.0,
        "latency_p50_secs": round(percentile(latencies, 50), 3),
        "latency_p95_secs": round(percentile(latencies, 95), 3),
        "latency_max_secs": round(max(latencies) if latencies else 0.0, 3),
        "transport_failures": sum(
            int(row.get("transport_failures", 0) or 0) for row in selected
        ),
        "by_workload": by_workload,
    }


def process_counter_delta(samples: list[dict[str, Any]], key: str) -> int:
    values = [int(sample[key]) for sample in samples if key in sample]
    return max(0, values[-1] - values[0]) if len(values) >= 2 else 0


PROCESS_MONOTONIC_COUNTERS = (
    "cpu_ticks",
    "read_bytes",
    "write_bytes",
    "read_syscalls",
    "write_syscalls",
    "minor_faults",
    "major_faults",
)


def process_sampling_diagnostics(
    samples: list[dict[str, Any]],
    requested_duration_secs: float,
) -> dict[str, Any]:
    elapsed = [
        float(sample.get("elapsed_secs", 0.0) or 0.0)
        for sample in samples
        if "elapsed_secs" in sample
    ]
    gaps = [
        current - previous
        for previous, current in zip(elapsed, elapsed[1:])
        if current >= previous
    ]
    regressions = 0
    for key in PROCESS_MONOTONIC_COUNTERS:
        values = [int(sample[key]) for sample in samples if key in sample]
        regressions += sum(
            1 for previous, current in zip(values, values[1:]) if current < previous
        )
    coverage_secs = max(0.0, elapsed[-1] - elapsed[0]) if len(elapsed) >= 2 else 0.0
    requested = max(0.0, float(requested_duration_secs))
    return {
        "sample_first_elapsed_secs": round(elapsed[0], 3) if elapsed else 0.0,
        "sample_last_elapsed_secs": round(elapsed[-1], 3) if elapsed else 0.0,
        "sample_coverage_secs": round(coverage_secs, 3),
        "sample_coverage_ratio": round(
            min(1.0, coverage_secs / requested) if requested > 0 else 0.0,
            4,
        ),
        "sample_max_gap_secs": round(max(gaps) if gaps else 0.0, 3),
        "counter_regressions": regressions,
    }


def integrate_cpu_core_seconds(samples: list[dict[str, Any]]) -> float:
    ticks = [int(sample["cpu_ticks"]) for sample in samples if "cpu_ticks" in sample]
    if len(ticks) >= 2:
        try:
            ticks_per_sec = float(
                os.sysconf(os.sysconf_names.get("SC_CLK_TCK", "SC_CLK_TCK"))
            )
        except (OSError, TypeError, ValueError):
            ticks_per_sec = 0.0
        if ticks_per_sec > 0:
            return round(max(0, ticks[-1] - ticks[0]) / ticks_per_sec, 3)

    total = 0.0
    previous_elapsed: float | None = None
    for sample in samples:
        elapsed = float(sample.get("elapsed_secs", 0.0) or 0.0)
        if previous_elapsed is not None and elapsed >= previous_elapsed:
            total += (
                float(sample.get("cpu_pct", 0.0) or 0.0)
                / 100.0
                * (elapsed - previous_elapsed)
            )
        previous_elapsed = elapsed
    return round(total, 3)


def summarize_process_after(
    samples: list[dict[str, Any]],
    start_elapsed_secs: float | None,
) -> dict[str, Any]:
    selected: list[dict[str, Any]] = []
    if start_elapsed_secs is not None:
        first_index = next(
            (
                index
                for index, sample in enumerate(samples)
                if float(sample.get("elapsed_secs", 0.0) or 0.0)
                >= start_elapsed_secs
            ),
            len(samples),
        )
        selected = samples[max(0, first_index - 1) :]
    rss = [int(sample.get("vmrss_bytes", 0) or 0) for sample in selected]
    result: dict[str, Any] = {
        "sample_count": len(selected),
        "start_elapsed_secs": round(start_elapsed_secs or 0.0, 3),
        "end_elapsed_secs": round(
            float(selected[-1].get("elapsed_secs", 0.0) or 0.0), 3
        )
        if selected
        else 0.0,
        "cpu_core_seconds": integrate_cpu_core_seconds(selected),
        "rss_bytes_p95": int(percentile(rss, 95)),
        "rss_bytes_max": max(rss) if rss else 0,
    }
    for key in (
        "read_bytes",
        "write_bytes",
        "read_syscalls",
        "write_syscalls",
        "minor_faults",
        "major_faults",
    ):
        result[f"{key}_delta"] = process_counter_delta(selected, key)
    return result


def summarize(run_dir: Path, label: str, exit_code: int | None) -> dict[str, Any]:
    manifest: dict[str, Any] = {}
    manifest_path = run_dir / "manifest.json"
    if manifest_path.exists():
        try:
            loaded_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            if isinstance(loaded_manifest, dict):
                manifest = loaded_manifest
        except (OSError, json.JSONDecodeError):
            manifest = {}
    process_samples = read_jsonl(run_dir / "process-samples.jsonl")
    endpoint_samples = read_jsonl(run_dir / "endpoint-samples.jsonl")
    canary_samples = read_jsonl(run_dir / "canary-samples.jsonl")
    shutdown_samples = read_jsonl(run_dir / "shutdown-samples.jsonl")
    event_storm_samples = read_jsonl(run_dir / "event-storm-samples.jsonl")
    hot_churn_samples = read_jsonl(run_dir / "hot-churn-samples.jsonl")

    cpu = [float(item.get("cpu_pct", 0.0)) for item in process_samples]
    rss = [int(item.get("vmrss_bytes", 0)) for item in process_samples]
    fds = [int(item.get("fd_count", 0)) for item in process_samples]

    watch_samples = [
        item["data"]
        for item in endpoint_samples
        if item.get("ok") and item.get("endpoint") == "/watch-state" and isinstance(item.get("data"), dict)
    ]
    status_samples = [
        item["data"]
        for item in endpoint_samples
        if item.get("ok") and item.get("endpoint") == "/status" and isinstance(item.get("data"), dict)
    ]
    memory_samples = [
        item["data"]
        for item in endpoint_samples
        if item.get("ok") and item.get("endpoint") == "/memory" and isinstance(item.get("data"), dict)
    ]
    health_samples = [
        item["data"]
        for item in endpoint_samples
        if item.get("ok") and item.get("endpoint") == "/health" and isinstance(item.get("data"), dict)
    ]

    def nums(samples: list[dict[str, Any]], key: str) -> list[float]:
        return [float(s.get(key, 0) or 0) for s in samples]

    def summarize_canary_group(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        by_op: dict[str, dict[str, Any]] = {}
        for op in sorted({str(item.get("operation", "")) for item in rows}):
            if not op:
                continue
            op_rows = [item for item in rows if item.get("operation") == op]
            semantic_rows = [
                item
                for item in op_rows
                if first_query_correct(item)
            ]
            latencies = [float(item.get("latency_secs", 0.0)) for item in semantic_rows]
            passive_waits = [
                float(item.get("passive_wait_secs", 0.0))
                for item in op_rows
                if "passive_wait_secs" in item
            ]
            summary = {
                "count": len(op_rows),
                "ok": len(semantic_rows),
                "timeouts": len(op_rows) - len(semantic_rows),
                "transport_ok": sum(1 for item in op_rows if first_query_transport_ok(item)),
                "transport_failures": sum(
                    1 for item in op_rows if not first_query_transport_ok(item)
                ),
                "p50_secs": round(percentile(latencies, 50), 3),
                "p95_secs": round(percentile(latencies, 95), 3),
                "p99_secs": round(percentile(latencies, 99), 3),
                "max_secs": round(max(latencies) if latencies else 0.0, 3),
            }
            if passive_waits:
                summary["passive_wait_p50_secs"] = round(percentile(passive_waits, 50), 3)
                summary["passive_wait_p95_secs"] = round(percentile(passive_waits, 95), 3)
                summary["passive_wait_max_secs"] = round(max(passive_waits), 3)
            by_op[op] = summary
        return by_op

    canary_by_op = summarize_canary_group(canary_samples)
    active_canary_by_op = summarize_canary_group(
        [item for item in canary_samples if item.get("canary_kind") in ("", "active", None)]
    )
    passive_canary_by_op = summarize_canary_group(
        [item for item in canary_samples if item.get("canary_kind") == "passive"]
    )
    active_canary_count = sum(
        1 for item in canary_samples if item.get("canary_kind") in ("", "active", None)
    )
    passive_canary_count = sum(
        1 for item in canary_samples if item.get("canary_kind") == "passive"
    )
    passive_shutdown_reconcile_rows = [
        item
        for item in canary_samples
        if item.get("operation") == "passive_shutdown_reconcile"
    ]
    passive_shutdown_reconcile_ok = sum(
        1
        for item in passive_shutdown_reconcile_rows
        if item.get("ok") is True and item.get("stable") is True
    )
    passive_shutdown_reconcile_failures = (
        len(passive_shutdown_reconcile_rows) - passive_shutdown_reconcile_ok
    )
    snapshot_quiesce_rows = [
        item
        for item in shutdown_samples
        if item.get("operation") == "shutdown_snapshot_quiesce"
    ]
    snapshot_quiesce_ok = sum(
        1
        for item in snapshot_quiesce_rows
        if item.get("ok") is True and item.get("ready") is True
    )
    snapshot_quiesce_failures = len(snapshot_quiesce_rows) - snapshot_quiesce_ok
    snapshot_log_window_valid = len(snapshot_quiesce_rows) == 1 and all(
        item.get("daemon_log_window_valid") is True
        and isinstance(item.get("daemon_log_offset_start"), int)
        and not isinstance(item.get("daemon_log_offset_start"), bool)
        and isinstance(item.get("daemon_log_offset_end"), int)
        and not isinstance(item.get("daemon_log_offset_end"), bool)
        and int(item["daemon_log_offset_start"]) >= 0
        and int(item["daemon_log_offset_end"])
        >= int(item["daemon_log_offset_start"])
        for item in snapshot_quiesce_rows
    )
    quiesce_start = min(
        (
            float(item.get("started_elapsed_secs", 0.0) or 0.0)
            for item in snapshot_quiesce_rows
        ),
        default=0.0,
    )
    quiesce_end = max(
        (
            float(item.get("elapsed_secs", 0.0) or 0.0)
            for item in snapshot_quiesce_rows
        ),
        default=0.0,
    )
    quiesce_process_samples = [
        item
        for item in process_samples
        if snapshot_quiesce_rows
        and quiesce_start <= float(item.get("elapsed_secs", -1.0) or -1.0) <= quiesce_end
    ]
    quiesce_cpu = [
        float(item.get("cpu_pct", 0.0) or 0.0)
        for item in quiesce_process_samples
    ]
    quiesce_rss = [
        int(item.get("vmrss_bytes", 0) or 0)
        for item in quiesce_process_samples
    ]
    passive_first_query_ops = [
        "passive_create_first_query",
        "passive_rename_new_first_query",
        "passive_rename_old_first_query",
        "passive_delete_first_query",
    ]
    passive_positive_first_query_ops = [
        "passive_create_first_query",
        "passive_rename_new_first_query",
    ]
    passive_first_query = {
        op: passive_canary_by_op[op]
        for op in passive_first_query_ops
        if op in passive_canary_by_op
    }
    passive_positive_first_query = {
        op: passive_canary_by_op[op]
        for op in passive_positive_first_query_ops
        if op in passive_canary_by_op
    }
    passive_first_query_total = sum(item["count"] for item in passive_first_query.values())
    passive_first_query_ok = sum(item["ok"] for item in passive_first_query.values())
    passive_first_query_success_rate = (
        round(passive_first_query_ok / passive_first_query_total, 4)
        if passive_first_query_total
        else 0.0
    )
    passive_positive_first_query_total = sum(
        item["count"] for item in passive_positive_first_query.values()
    )
    passive_positive_first_query_ok = sum(
        item["ok"] for item in passive_positive_first_query.values()
    )
    passive_positive_first_query_success_rate = (
        round(passive_positive_first_query_ok / passive_positive_first_query_total, 4)
        if passive_positive_first_query_total
        else 0.0
    )

    event_first_queries = [
        item for item in event_storm_samples if item.get("event_kind") == "first_query"
    ]
    event_after_queries = [
        item for item in event_storm_samples if item.get("event_kind") == "after_query"
    ]
    event_bursts = [
        item for item in event_storm_samples if item.get("event_kind") == "burst_checked"
    ]
    event_written = [
        item for item in event_storm_samples if item.get("event_kind") == "burst_written"
    ]
    event_started = [
        item for item in event_storm_samples if item.get("event_kind") == "burst_started"
    ]
    event_cleanups = [
        item for item in event_storm_samples if item.get("event_kind") == "burst_cleanup"
    ]
    event_post_cleanup_audits = [
        item
        for item in event_storm_samples
        if item.get("event_kind") == "post_cleanup_audit"
    ]
    event_visibility = [
        item for item in event_storm_samples if item.get("event_kind") == "visibility_probe"
    ]
    first_burst_elapsed = min(
        (
            float(item.get("elapsed_secs", 0.0) or 0.0)
            for item in event_written
            if "elapsed_secs" in item
        ),
        default=None,
    )
    process_after_first_burst = summarize_process_after(
        process_samples,
        first_burst_elapsed,
    )
    first_event_storm_elapsed = min(
        (
            float(item.get("elapsed_secs", 0.0) or 0.0)
            for item in event_started
            if "elapsed_secs" in item
        ),
        default=None,
    )
    process_after_event_storm_start = summarize_process_after(
        process_samples,
        first_event_storm_elapsed,
    )
    # Task 2: split first-query rows by query_phase (immediate vs delayed).
    event_immediate_queries = [
        item for item in event_first_queries if item.get("query_phase") == "immediate"
    ]
    event_delayed_queries = [
        item for item in event_first_queries if item.get("query_phase") != "immediate"
    ]
    # Task 3: hot-layer query rows from the mixed-workload churn thread.
    hot_layer_queries = [
        item for item in hot_churn_samples if item.get("event_kind") == "hot_layer_query"
    ]

    def summarize_event_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
        total = len(rows)
        ok = sum(1 for item in rows if first_query_correct(item))
        transport_ok = sum(1 for item in rows if first_query_transport_ok(item))
        positive = [item for item in rows if item.get("should_exist")]
        positive_ok = sum(1 for item in positive if first_query_correct(item))
        latencies = [
            float(item.get("latency_secs", 0.0))
            for item in rows
            if first_query_correct(item)
        ]
        settles = [float(item.get("settle_secs", 0.0)) for item in rows if "settle_secs" in item]
        ages = [float(item.get("event_age_secs", 0.0)) for item in rows if "event_age_secs" in item]
        return {
            "total": total,
            "ok": ok,
            "missed": total - ok,
            "success_rate": round(ok / total, 4) if total else 0.0,
            "transport_ok": transport_ok,
            "transport_failures": total - transport_ok,
            "transport_success_rate": round(transport_ok / total, 4) if total else 0.0,
            "positive_total": len(positive),
            "positive_ok": positive_ok,
            "positive_success_rate": round(positive_ok / len(positive), 4) if positive else 0.0,
            "first_query_p50_secs": round(percentile(latencies, 50), 3),
            "first_query_p95_secs": round(percentile(latencies, 95), 3),
            "first_query_max_secs": round(max(latencies) if latencies else 0.0, 3),
            "settle_p50_secs": round(percentile(settles, 50), 3),
            "settle_p95_secs": round(percentile(settles, 95), 3),
            "settle_max_secs": round(max(settles) if settles else 0.0, 3),
            "event_age_p50_secs": round(percentile(ages, 50), 3),
            "event_age_p95_secs": round(percentile(ages, 95), 3),
            "event_age_max_secs": round(max(ages) if ages else 0.0, 3),
        }

    event_by_workload = {
        workload: summarize_event_rows(
            [item for item in event_first_queries if item.get("workload") == workload]
        )
        for workload in sorted({str(item.get("workload", "")) for item in event_first_queries})
        if workload
    }
    event_by_tier = {
        tier: summarize_event_rows(
            [item for item in event_first_queries if str(item.get("tier_before", "")) == tier]
        )
        for tier in sorted({str(item.get("tier_before", "")) for item in event_first_queries})
        if tier
    }
    cleanup_durations = [
        float(item.get("duration_secs", 0.0))
        for item in event_cleanups
        if isinstance(item.get("duration_secs"), (int, float))
    ]
    cleanup_entries = [
        int(item["entries_estimated"])
        for item in event_cleanups
        if isinstance(item.get("entries_estimated"), (int, float))
    ]
    event_cleanup_summary = {
        "count": len(event_cleanups),
        "ok": sum(1 for item in event_cleanups if item.get("ok") is True),
        "failures": sum(1 for item in event_cleanups if item.get("ok") is not True),
        "entries_estimated_total": sum(cleanup_entries),
        "duration_p95_secs": round(percentile(cleanup_durations, 95), 3),
        "duration_max_secs": round(max(cleanup_durations) if cleanup_durations else 0.0, 3),
    }
    event_post_cleanup_summary = {
        "count": len(event_post_cleanup_audits),
        "ok": sum(
            1 for item in event_post_cleanup_audits if item.get("ok") is True
        ),
        "failures": sum(
            1 for item in event_post_cleanup_audits if item.get("ok") is not True
        ),
        "watcher_ledger_cleared": sum(
            1
            for item in event_post_cleanup_audits
            if item.get("watcher_ledger_cleared") is True
        ),
        "cleanup_target_entries_max": max(
            (
                int(item.get("cleanup_target_entries", 0) or 0)
                for item in event_post_cleanup_audits
            ),
            default=0,
        ),
        "cleanup_target_ephemeral_watch_dirs_max": max(
            (
                int(item.get("cleanup_target_ephemeral_watch_dirs", 0) or 0)
                for item in event_post_cleanup_audits
            ),
            default=0,
        ),
        "cleanup_target_rotating_active_dirs_max": max(
            (
                int(item.get("cleanup_target_rotating_active_dirs", 0) or 0)
                for item in event_post_cleanup_audits
            ),
            default=0,
        ),
        "target_ephemeral_watch_seen": any(
            item.get("target_ephemeral_watch") is True
            for item in event_post_cleanup_audits
        ),
        "target_rotating_active_seen": any(
            item.get("target_rotating_active") is True
            for item in event_post_cleanup_audits
        ),
        "audit_after_lease_expiry": sum(
            1
            for item in event_post_cleanup_audits
            if item.get("audit_after_lease_expiry") is True
        ),
        "audit_before_next_rotation": sum(
            1
            for item in event_post_cleanup_audits
            if item.get("audit_before_next_rotation") is True
        ),
        "audit_window_valid": sum(
            1
            for item in event_post_cleanup_audits
            if item.get("audit_window_valid") is True
        ),
    }
    burst_durations = [float(item.get("duration_secs", 0.0)) for item in event_written]

    def count_event_op(operation: str) -> int:
        return sum(1 for item in event_first_queries if item.get("operation") == operation)

    def count_event_op_ok(operation: str) -> int:
        return sum(
            1
            for item in event_first_queries
            if item.get("operation") == operation and first_query_correct(item)
        )

    inode_reuse_new_rows = [
        item
        for item in event_first_queries
        if item.get("operation") == "inode_reuse_new_visible_first_query"
    ]

    # Task 3: cold-freshness spike metrics, computed from the /watch-state
    # sample series (paired with their endpoint elapsed_secs for time-aware
    # rates). spike_count = samples whose p95 exceeds 2x the running median;
    # slope_max = max |dp95/dt| between consecutive samples (secs per second).
    watch_p95_series: list[tuple[float, float]] = []
    for item in endpoint_samples:
        if (
            item.get("ok")
            and item.get("endpoint") == "/watch-state"
            and isinstance(item.get("data"), dict)
        ):
            try:
                p95 = float(item["data"].get("cold_freshness_age_p95_secs") or 0)
                elapsed = float(item.get("elapsed_secs", 0.0) or 0.0)
                watch_p95_series.append((elapsed, p95))
            except (TypeError, ValueError):
                continue
    cold_freshness_spike_count = 0
    cold_freshness_slope_max = 0.0
    if len(watch_p95_series) >= 2:
        running_values: list[float] = []
        for _elapsed, p95 in watch_p95_series:
            running_values.append(p95)
            if len(running_values) >= 3:
                median = percentile(running_values, 50)
                if median > 0 and p95 > 2.0 * median:
                    cold_freshness_spike_count += 1
        for (t0, v0), (t1, v1) in zip(watch_p95_series, watch_p95_series[1:]):
            dt = t1 - t0
            if dt > 0:
                cold_freshness_slope_max = max(cold_freshness_slope_max, abs(v1 - v0) / dt)

    # Task 2: if an inode_reuse_stress run emitted a stress summary, use its
    # attempt/observed counters (which reflect the full tight-loop iteration
    # count) instead of the first-query-derived counts that undercount attempts.
    inode_reuse_stress_rows = [
        item
        for item in event_storm_samples
        if item.get("event_kind") == "inode_reuse_stress_summary"
    ]

    event_special = {
        "subtree_rename_pairs_checked": count_event_op(
            "subtree_rename_new_visible_first_query"
        ),
        "subtree_rename_new_visible_ok": count_event_op_ok(
            "subtree_rename_new_visible_first_query"
        ),
        "subtree_rename_old_hidden_ok": count_event_op_ok(
            "subtree_rename_old_hidden_first_query"
        ),
        "mount_storm_old_hidden_checked": count_event_op(
            "mount_point_offline_old_hidden_first_query"
        ),
        "mount_storm_old_hidden_ok": count_event_op_ok(
            "mount_point_offline_old_hidden_first_query"
        ),
        "inode_reuse_attempts": len(inode_reuse_new_rows),
        "inode_reuse_observed": sum(
            1 for item in inode_reuse_new_rows if item.get("inode_reused")
        ),
        "inode_reuse_new_visible_ok": count_event_op_ok(
            "inode_reuse_new_visible_first_query"
        ),
        "inode_reuse_old_hidden_ok": count_event_op_ok(
            "inode_reuse_old_hidden_first_query"
        ),
        "time_skew_backdated_checked": count_event_op("backdated_file_visible_first_query"),
        "time_skew_backdated_visible_ok": count_event_op_ok(
            "backdated_file_visible_first_query"
        ),
    }
    if inode_reuse_stress_rows:
        # Stress test reports the true iteration count; aggregate across cycles.
        event_special["inode_reuse_attempts"] = sum(
            int(item.get("inode_reuse_attempts", 0) or 0) for item in inode_reuse_stress_rows
        )
        event_special["inode_reuse_observed"] = sum(
            int(item.get("inode_reuse_observed", 0) or 0) for item in inode_reuse_stress_rows
        )
        event_special["inode_reuse_stress_tmpfs_mounted"] = any(
            bool(item.get("tmpfs_mounted")) for item in inode_reuse_stress_rows
        )
    if int(event_special["inode_reuse_attempts"]) <= 0:
        event_special["inode_reuse_status"] = "not_run"
    elif int(event_special["inode_reuse_observed"]) <= 0:
        event_special["inode_reuse_status"] = "inconclusive"
    else:
        event_special["inode_reuse_status"] = "exercised"

    # Task 4: scale-aware metrics.
    # index_total_files / index_total_dirs from the last /status sample.
    index_total_files = 0
    index_total_dirs = 0
    if status_samples:
        last_status = status_samples[-1]
        index_total_files = int(
            last_status.get("indexed_count", 0)
            or last_status.get("total_files", 0)
            or last_status.get("indexed_files", 0)
            or 0
        )
        index_total_dirs = int(last_status.get("total_dirs", 0) or last_status.get("indexed_dirs", 0) or 0)
    # Current /watch-state reports aggregate l0_dirs/l1_dirs/l2_dirs/l3_dirs.
    # Older debug dumps with a dirs array remain supported by the helper.
    cold_dir_count = 0
    hot_dir_count = 0
    if watch_samples:
        last_tiers = compute_tier_distribution(watch_samples[-1])
        cold_dir_count = last_tiers["L2"] + last_tiers["L3"]
        hot_dir_count = last_tiers["L0"] + last_tiers["L1"]
    # rotation_cycle_estimate_secs = (cold_dir_count / max_dirs_per_tick) * tick_secs.
    # Uses the last watch-state sample's rotating params when available; falls
    # back to 0 when the rotating window is disabled or no data.
    rotation_cycle_estimate_secs = 0.0
    if watch_samples and cold_dir_count > 0:
        last_watch = watch_samples[-1]
        runner_args = (
            manifest.get("runner_args")
            if isinstance(manifest.get("runner_args"), dict)
            else {}
        )
        rotating_enabled = bool(
            last_watch.get(
                "rotating_cold_window_enabled",
                runner_args.get("rotating_cold_window", False),
            )
        )
        max_dirs_per_tick = int(
            last_watch.get("rotating_cold_window_max_dirs_per_tick", 0)
            or runner_args.get("rotating_max_dirs_per_tick", 0)
            or 0
        )
        tick_secs = float(
            last_watch.get("rotating_cold_window_tick_secs", 0)
            or runner_args.get("rotating_tick_secs", 0)
            or 0
        )
        if rotating_enabled and max_dirs_per_tick > 0 and tick_secs > 0:
            rotation_cycle_estimate_secs = (cold_dir_count / max_dirs_per_tick) * tick_secs
    # memory_per_file_bytes = RSS max / total files (efficiency metric).
    rss_max_bytes = max(rss) if rss else 0
    memory_per_file_bytes = round(rss_max_bytes / index_total_files, 3) if index_total_files > 0 else 0.0
    # cold_freshness_age_trend: time series of cold_freshness_age_p95 samples.
    # Task 5: cap to avoid unbounded growth on long-duration (7200s+) runs.
    cold_freshness_age_trend = [
        {"elapsed_secs": round(elapsed, 1), "cold_freshness_age_p95_secs": int(p95)}
        for elapsed, p95 in watch_p95_series
    ]
    _trend_cap = 2000
    if len(cold_freshness_age_trend) > _trend_cap:
        # Evenly downsample to the cap.
        step = len(cold_freshness_age_trend) / _trend_cap
        cold_freshness_age_trend = [
            cold_freshness_age_trend[int(i * step)] for i in range(_trend_cap)
        ]

    # Task 3: tier distribution at three points: start of run, start of event
    # storm (after settle delay), and end of run. The storm_start snapshot is
    # emitted by EventStormRunner._emit_tier_distribution into the JSONL log.
    tier_dist_start = compute_tier_distribution(watch_samples[0] if watch_samples else None)
    tier_dist_end = compute_tier_distribution(watch_samples[-1] if watch_samples else None)
    tier_dist_storm_start: dict[str, int] | None = None
    for item in event_storm_samples:
        if (
            item.get("event_kind") == "tier_distribution"
            and item.get("phase") == "storm_start"
        ):
            tier_dist_storm_start = item.get("tier_counts")
            break

    event_first_query_summary = summarize_event_rows(event_first_queries)
    raw_shutdown_elapsed = manifest.get("shutdown_signal_elapsed_secs")
    shutdown_signal_elapsed_secs = (
        float(raw_shutdown_elapsed) if raw_shutdown_elapsed is not None else None
    )
    memory_timeline = build_memory_timeline(
        endpoint_samples,
        process_samples,
        shutdown_signal_elapsed_secs=shutdown_signal_elapsed_secs,
    )

    summary = {
        "label": label,
        "generated_at": utc_now(),
        "fd_rdd_exit_code": exit_code,
        "ab_comparable": bool(manifest.get("ab_comparable")),
        "ab_comparability_reasons": list(
            manifest.get("ab_comparability_reasons", []) or []
        ),
        "run_audit": {
            "git_sha": str(manifest.get("git_sha", "")),
            "git_dirty": manifest.get("git_dirty"),
            "binary_sha256": str(manifest.get("binary_sha256", "")),
            "artifact_provenance": manifest.get("artifact_provenance", {}),
            "run_state": str(manifest.get("run_state", "")),
            "completion_reason": str(manifest.get("completion_reason", "")),
            "ab_parameter_fingerprint": str(
                manifest.get("ab_parameter_fingerprint", "")
            ),
            "initial_state_fingerprint": str(
                manifest.get("initial_state_fingerprint", "")
            ),
            "execution_fingerprint": str(
                manifest.get("execution_fingerprint", "")
            ),
            "ab_comparable": bool(manifest.get("ab_comparable")),
            "ab_comparability_reasons": list(
                manifest.get("ab_comparability_reasons", []) or []
            ),
            "fixture_initial_file_count": manifest.get("fixture_initial_file_count"),
            "fixture_initial_file_count_source": str(
                manifest.get("fixture_initial_file_count_source", "unverified")
            ),
            "fixture_initial_file_count_verified": bool(
                (manifest.get("fixture") or {}).get("count_verified", False)
                if isinstance(manifest.get("fixture"), dict)
                else False
            ),
            "requested_duration_secs": int(
                manifest.get("duration_secs", 0) or 0
            ),
            "actual_duration_secs": float(
                manifest.get("actual_duration_secs", 0.0) or 0.0
            ),
        },
        "sample_counts": {
            "process": len(process_samples),
            "endpoint": len(endpoint_samples),
            "watch_state": len(watch_samples),
            "memory": len(memory_samples),
            "health": len(health_samples),
            "canary": len(canary_samples),
            "canary_active": active_canary_count,
            "canary_passive": passive_canary_count,
            "passive_shutdown_reconcile": len(
                passive_shutdown_reconcile_rows
            ),
            "shutdown_snapshot_quiesce": len(snapshot_quiesce_rows),
            "event_storm": len(event_storm_samples),
            "event_storm_first_query": len(event_first_queries),
            "event_storm_visibility": len(event_visibility),
        },
        "process": {
            "sample_count": len(process_samples),
            **process_sampling_diagnostics(
                process_samples,
                float(manifest.get("duration_secs", 0) or 0),
            ),
            "cpu_pct_p50": round(percentile(cpu, 50), 3),
            "cpu_pct_p95": round(percentile(cpu, 95), 3),
            "cpu_pct_max": round(max(cpu) if cpu else 0.0, 3),
            "cpu_core_seconds": integrate_cpu_core_seconds(process_samples),
            "rss_bytes_p95": int(percentile(rss, 95)),
            "rss_bytes_max": max(rss) if rss else 0,
            "fd_count_max": max(fds) if fds else 0,
            "read_bytes_delta": process_counter_delta(process_samples, "read_bytes"),
            "write_bytes_delta": process_counter_delta(process_samples, "write_bytes"),
            "read_syscalls_delta": process_counter_delta(
                process_samples, "read_syscalls"
            ),
            "write_syscalls_delta": process_counter_delta(
                process_samples, "write_syscalls"
            ),
            "minor_faults_delta": process_counter_delta(
                process_samples, "minor_faults"
            ),
            "major_faults_delta": process_counter_delta(
                process_samples, "major_faults"
            ),
        },
        "process_after_first_burst": process_after_first_burst,
        "process_after_event_storm_start": process_after_event_storm_start,
        "watch_state": {
            "dirty_queue_len_max": int(max(nums(watch_samples, "dirty_queue_len") or [0])),
            "dirty_queue_len_last": int(
                nums(watch_samples[-1:], "dirty_queue_len")[0]
                if watch_samples
                else 0
            ),
            "waterline_soft_degraded_samples": sum(
                1 for sample in watch_samples if bool(sample.get("waterline_soft_degraded"))
            ),
            "waterline_soft_degraded_ratio": round(
                sum(
                    1
                    for sample in watch_samples
                    if bool(sample.get("waterline_soft_degraded"))
                )
                / len(watch_samples),
                4,
            )
            if watch_samples
            else 0.0,
            "waterline_soft_degraded_last": bool(
                watch_samples[-1].get("waterline_soft_degraded", False)
            )
            if watch_samples
            else False,
            "waterline_hard_degraded_samples": sum(
                1 for sample in watch_samples if bool(sample.get("waterline_hard_degraded"))
            ),
            "waterline_hard_degraded_last": bool(
                watch_samples[-1].get("waterline_hard_degraded", False)
            )
            if watch_samples
            else False,
            "waterline_effective_rotating_budget_last": int(
                nums(watch_samples[-1:], "waterline_effective_rotating_budget")[0]
                if watch_samples
                else 0
            ),
            "fast_scan_coverage_lag_p99_ms_max": int(
                max(nums(watch_samples, "fast_scan_coverage_lag_p99_ms") or [0])
            ),
            "cold_freshness_age_p95_secs_first": int(
                nums(watch_samples[:1], "cold_freshness_age_p95_secs")[0]
                if watch_samples
                else 0
            ),
            "cold_freshness_age_p95_secs_last": int(
                nums(watch_samples[-1:], "cold_freshness_age_p95_secs")[0]
                if watch_samples
                else 0
            ),
            "cold_freshness_age_p95_secs_max": int(
                max(nums(watch_samples, "cold_freshness_age_p95_secs") or [0])
            ),
            "cold_freshness_age_p99_secs_max": int(
                max(nums(watch_samples, "cold_freshness_age_p99_secs") or [0])
            ),
            "cold_freshness_age_spike_count": cold_freshness_spike_count,
            "cold_freshness_age_slope_max": round(cold_freshness_slope_max, 3),
            "cold_freshness_age_p95_secs_delta": int(
                (
                    nums(watch_samples[-1:], "cold_freshness_age_p95_secs")[0]
                    - nums(watch_samples[:1], "cold_freshness_age_p95_secs")[0]
                )
                if watch_samples
                else 0
            ),
            "rotating_cold_window_budget_blocked_last": int(
                nums(watch_samples[-1:], "rotating_cold_window_budget_blocked")[0]
                if watch_samples
                else 0
            ),
            "rotating_cold_window_active_dirs_max": int(
                max(nums(watch_samples, "rotating_cold_window_active_dirs") or [0])
            ),
            "rotating_cold_window_cycle_progress_pct_max": int(
                max(nums(watch_samples, "rotating_cold_window_cycle_progress_pct") or [0])
            ),
            "rotating_cold_window_promoted_to_ephemeral_last": int(
                nums(watch_samples[-1:], "rotating_cold_window_promoted_to_ephemeral")[0]
                if watch_samples
                else 0
            ),
            "rotating_cold_window_fast_scan_lease_dirs_last": int(
                nums(watch_samples[-1:], "rotating_cold_window_fast_scan_lease_dirs")[0]
                if watch_samples
                else 0
            ),
            "rotating_cold_window_scan_only_dirs_last": int(
                nums(watch_samples[-1:], "rotating_cold_window_scan_only_dirs")[0]
                if watch_samples
                else 0
            ),
            "ephemeral_watch_budget_blocked_last": int(
                nums(watch_samples[-1:], "ephemeral_watch_budget_blocked")[0]
                if watch_samples
                else 0
            ),
            "proc_sampler_triggered_watches_last": int(
                nums(watch_samples[-1:], "proc_sampler_triggered_watches")[0]
                if watch_samples
                else 0
            ),
        },
        "memory_endpoint": {
            "process_rss_bytes_max": int(
                max(nums(memory_samples, "process_rss_bytes") or [0])
            ),
            "process_swap_bytes_max": int(
                max(nums(memory_samples, "process_swap_bytes") or [0])
            ),
        },
        "memory_timeline": memory_timeline,
        "health": {
            "index_health_last": health_samples[-1].get("index_health") if health_samples else "",
            "watcher_degraded_seen": any(bool(item.get("watcher_degraded")) for item in health_samples),
            "tiered_degraded_seen": any(bool(item.get("tiered_degraded")) for item in health_samples),
        },
        "canary": canary_by_op,
        "canary_active": active_canary_by_op,
        "canary_passive": passive_canary_by_op,
        "passive_shutdown_reconcile": {
            "count": len(passive_shutdown_reconcile_rows),
            "ok": passive_shutdown_reconcile_ok,
            "failures": passive_shutdown_reconcile_failures,
            "stable": bool(passive_shutdown_reconcile_rows)
            and passive_shutdown_reconcile_failures == 0,
            "attempts_max": max(
                (
                    int(item.get("attempts", 0) or 0)
                    for item in passive_shutdown_reconcile_rows
                ),
                default=0,
            ),
            "paths_max": max(
                (
                    len(item.get("paths", []))
                    for item in passive_shutdown_reconcile_rows
                    if isinstance(item.get("paths"), list)
                ),
                default=0,
            ),
            "scanned_total": sum(
                int(item.get("scanned", 0) or 0)
                for item in passive_shutdown_reconcile_rows
                if isinstance(item.get("scanned"), (int, float))
                and not isinstance(item.get("scanned"), bool)
            ),
            "changed_total": sum(
                int(item.get("changed", 0) or 0)
                for item in passive_shutdown_reconcile_rows
                if isinstance(item.get("changed"), (int, float))
                and not isinstance(item.get("changed"), bool)
            ),
            "deleted_total": sum(
                int(item.get("deleted", 0) or 0)
                for item in passive_shutdown_reconcile_rows
                if isinstance(item.get("deleted"), (int, float))
                and not isinstance(item.get("deleted"), bool)
            ),
            "latency_max_secs": round(
                max(
                    (
                        float(item.get("latency_secs", 0.0) or 0.0)
                        for item in passive_shutdown_reconcile_rows
                        if isinstance(item.get("latency_secs"), (int, float))
                    ),
                    default=0.0,
                ),
                3,
            ),
        },
        "shutdown_snapshot_quiesce": {
            "count": len(snapshot_quiesce_rows),
            "ok": snapshot_quiesce_ok,
            "failures": snapshot_quiesce_failures,
            "ready": bool(snapshot_quiesce_rows)
            and snapshot_quiesce_failures == 0,
            "written": any(
                item.get("written") is True for item in snapshot_quiesce_rows
            ),
            "rebuild_observed": any(
                item.get("rebuild_observed") is True
                for item in snapshot_quiesce_rows
            ),
            "daemon_log_window_valid": snapshot_log_window_valid,
            "daemon_log_offset_start": min(
                (
                    int(item.get("daemon_log_offset_start", -1))
                    for item in snapshot_quiesce_rows
                    if item.get("daemon_log_window_valid") is True
                ),
                default=-1,
            ),
            "daemon_log_offset_end": max(
                (
                    int(item.get("daemon_log_offset_end", -1))
                    for item in snapshot_quiesce_rows
                    if item.get("daemon_log_window_valid") is True
                ),
                default=-1,
            ),
            "attempts_max": max(
                (
                    int(item.get("attempts", 0) or 0)
                    for item in snapshot_quiesce_rows
                ),
                default=0,
            ),
            "ready_confirmations_max": max(
                (
                    int(item.get("ready_confirmations", 0) or 0)
                    for item in snapshot_quiesce_rows
                ),
                default=0,
            ),
            "not_ready_responses_total": sum(
                int(item.get("not_ready_responses", 0) or 0)
                for item in snapshot_quiesce_rows
            ),
            "last_daemon_error": next(
                (
                    str(item.get("last_daemon_error", ""))
                    for item in reversed(snapshot_quiesce_rows)
                    if item.get("last_daemon_error")
                ),
                "",
            ),
            "error": next(
                (
                    str(item.get("error", ""))
                    for item in reversed(snapshot_quiesce_rows)
                    if item.get("error")
                ),
                "",
            ),
            "latency_max_secs": round(
                max(
                    (
                        float(item.get("latency_secs", 0.0) or 0.0)
                        for item in snapshot_quiesce_rows
                    ),
                    default=0.0,
                ),
                3,
            ),
            "process_sample_count": len(quiesce_process_samples),
            "cpu_pct_p95": round(percentile(quiesce_cpu, 95), 3),
            "cpu_pct_max": round(max(quiesce_cpu) if quiesce_cpu else 0.0, 3),
            "rss_bytes_max": max(quiesce_rss) if quiesce_rss else 0,
        },
        "passive_first_query": {
            "total": passive_first_query_total,
            "ok": passive_first_query_ok,
            "success_rate": passive_first_query_success_rate,
            "operations": passive_first_query,
        },
        "passive_positive_first_query": {
            "total": passive_positive_first_query_total,
            "ok": passive_positive_first_query_ok,
            "success_rate": passive_positive_first_query_success_rate,
            "operations": passive_positive_first_query,
        },
        "event_storm": {
            "bursts": len(event_written),
            "checks": len(event_bursts),
            "events_total": event_first_query_summary["total"],
            "ok": event_first_query_summary["ok"],
            "missed": event_first_query_summary["missed"],
            "success_rate": event_first_query_summary["success_rate"],
            "transport_ok": event_first_query_summary["transport_ok"],
            "transport_failures": event_first_query_summary["transport_failures"],
            "transport_success_rate": event_first_query_summary["transport_success_rate"],
            "positive_total": event_first_query_summary["positive_total"],
            "positive_ok": event_first_query_summary["positive_ok"],
            "positive_success_rate": event_first_query_summary["positive_success_rate"],
            "burst_duration_p50_secs": round(percentile(burst_durations, 50), 3),
            "burst_duration_p95_secs": round(percentile(burst_durations, 95), 3),
            "burst_duration_max_secs": round(max(burst_durations) if burst_durations else 0.0, 3),
            "first_query": event_first_query_summary,
            "after_query": summarize_event_rows(event_after_queries),
            # Task 2: immediate vs delayed query-phase breakdown.
            "immediate_query": summarize_event_rows(event_immediate_queries),
            "delayed_query": summarize_event_rows(event_delayed_queries),
            "visibility": summarize_visibility_rows(event_visibility),
            "cleanup": event_cleanup_summary,
            "post_cleanup_audit": event_post_cleanup_summary,
            "by_workload": event_by_workload,
            "by_tier_before": event_by_tier,
            "special": event_special,
        },
        # Task 3: hot-layer query latency from the mixed-workload churn thread.
        "hot_layer_query": summarize_event_rows(hot_layer_queries),
        # Task 4: scale-aware metrics.
        "scale_aware": {
            "index_total_files": index_total_files,
            "index_total_dirs": index_total_dirs,
            "cold_dir_count": cold_dir_count,
            "hot_dir_count": hot_dir_count,
            "rotation_cycle_estimate_secs": round(rotation_cycle_estimate_secs, 1),
            "memory_per_file_bytes": memory_per_file_bytes,
            "cold_freshness_age_trend": cold_freshness_age_trend,
        },
        # Task 3: tier distribution showing how many directories are at each
        # tier (L0/L1/L2/L3) at three points during the run.
        "tier_distribution": {
            "start_of_run": tier_dist_start,
            "storm_start": tier_dist_storm_start if tier_dist_storm_start is not None else {},
            "end_of_run": tier_dist_end,
        },
        "built_in_metrics_dir": str(run_dir / "reports" / "metrics"),
    }
    (run_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return summary


def write_report(run_dir: Path, summary: dict[str, Any]) -> None:
    memory_timeline = summary["memory_timeline"]
    memory_report = {
        "phase_peaks": memory_timeline["phase_peaks"],
        "limitations": memory_timeline["limitations"],
        "l2": {
            "sample_count": memory_timeline["l2"]["sample_count"],
            "components": memory_timeline["l2"]["components"],
        },
    }
    report = f"""# fd-rdd M2 VM Benchmark Report

## Run

- Label: `{summary["label"]}`
- Generated: `{summary["generated_at"]}`
- fd-rdd exit code: `{summary["fd_rdd_exit_code"]}`
- Fatal error: `{summary.get("fatal_error", "")}`
- Run state/reason: `{summary["run_audit"]["run_state"]}` / `{summary["run_audit"]["completion_reason"]}`
- Git SHA: `{summary["run_audit"]["git_sha"]}`
- Git dirty: `{summary["run_audit"]["git_dirty"]}`
- Binary SHA256: `{summary["run_audit"]["binary_sha256"]}`
- Artifact provenance: `{summary["run_audit"]["artifact_provenance"]}`
- A/B parameter fingerprint: `{summary["run_audit"]["ab_parameter_fingerprint"]}`
- Initial-state fingerprint: `{summary["run_audit"]["initial_state_fingerprint"]}`
- Execution fingerprint: `{summary["run_audit"]["execution_fingerprint"]}`
- A/B comparable: `{summary["run_audit"]["ab_comparable"]}` — `{summary["run_audit"]["ab_comparability_reasons"]}`
- Fixture initial file count/source/verified: `{summary["run_audit"]["fixture_initial_file_count"]}` / `{summary["run_audit"]["fixture_initial_file_count_source"]}` / `{summary["run_audit"]["fixture_initial_file_count_verified"]}`
- Requested/actual duration seconds: `{summary["run_audit"]["requested_duration_secs"]}` / `{summary["run_audit"]["actual_duration_secs"]}`
- Built-in metrics: `{summary["built_in_metrics_dir"]}`

## Key single-run metrics

| Metric | Value |
|---|---:|
| process CPU p95 | {summary["process"]["cpu_pct_p95"]}% |
| process CPU max | {summary["process"]["cpu_pct_max"]}% |
| process CPU core seconds | {summary["process"]["cpu_core_seconds"]} |
| process sample coverage ratio | {summary["process"]["sample_coverage_ratio"]} |
| process sample max gap seconds | {summary["process"]["sample_max_gap_secs"]} |
| process counter regressions | {summary["process"]["counter_regressions"]} |
| process read bytes delta | {summary["process"]["read_bytes_delta"]} |
| process write bytes delta | {summary["process"]["write_bytes_delta"]} |
| process read syscalls delta | {summary["process"]["read_syscalls_delta"]} |
| process write syscalls delta | {summary["process"]["write_syscalls_delta"]} |
| process minor faults delta | {summary["process"]["minor_faults_delta"]} |
| process major faults delta | {summary["process"]["major_faults_delta"]} |
| event-storm-window samples | {summary["process_after_event_storm_start"]["sample_count"]} |
| event-storm-window CPU core seconds | {summary["process_after_event_storm_start"]["cpu_core_seconds"]} |
| event-storm-window read bytes delta | {summary["process_after_event_storm_start"]["read_bytes_delta"]} |
| event-storm-window write bytes delta | {summary["process_after_event_storm_start"]["write_bytes_delta"]} |
| event-storm-window read syscalls delta | {summary["process_after_event_storm_start"]["read_syscalls_delta"]} |
| event-storm-window write syscalls delta | {summary["process_after_event_storm_start"]["write_syscalls_delta"]} |
| event-storm-window minor faults delta | {summary["process_after_event_storm_start"]["minor_faults_delta"]} |
| event-storm-window major faults delta | {summary["process_after_event_storm_start"]["major_faults_delta"]} |
| event-storm-window RSS p95 | {summary["process_after_event_storm_start"]["rss_bytes_p95"]} |
| after-first-burst samples | {summary["process_after_first_burst"]["sample_count"]} |
| after-first-burst CPU core seconds | {summary["process_after_first_burst"]["cpu_core_seconds"]} |
| after-first-burst read bytes delta | {summary["process_after_first_burst"]["read_bytes_delta"]} |
| after-first-burst write bytes delta | {summary["process_after_first_burst"]["write_bytes_delta"]} |
| after-first-burst minor faults delta | {summary["process_after_first_burst"]["minor_faults_delta"]} |
| after-first-burst major faults delta | {summary["process_after_first_burst"]["major_faults_delta"]} |
| after-first-burst RSS p95 | {summary["process_after_first_burst"]["rss_bytes_p95"]} |
| process RSS p95 | {summary["process"]["rss_bytes_p95"]} |
| process RSS max | {summary["process"]["rss_bytes_max"]} |
| rebuild RSS max | {summary["memory_timeline"]["phase_peaks"]["rebuild"]["process_rss_bytes_max"]} |
| initial build publish RSS max | {summary["memory_timeline"]["phase_peaks"]["initial_build_publish"]["process_rss_bytes_max"]} |
| cold steady RSS max | {summary["memory_timeline"]["phase_peaks"]["cold_steady"]["process_rss_bytes_max"]} |
| hot-base snapshot RSS max | {summary["memory_timeline"]["phase_peaks"]["hot_base_snapshot"]["process_rss_bytes_max"]} |
| final snapshot window RSS max | {summary["memory_timeline"]["phase_peaks"]["final_snapshot_window"]["process_rss_bytes_max"]} |
| fd count max | {summary["process"]["fd_count_max"]} |
| dirty queue max | {summary["watch_state"]["dirty_queue_len_max"]} |
| dirty queue last | {summary["watch_state"]["dirty_queue_len_last"]} |
| waterline soft degraded samples | {summary["watch_state"]["waterline_soft_degraded_samples"]} |
| waterline soft degraded ratio | {summary["watch_state"]["waterline_soft_degraded_ratio"]} |
| waterline soft degraded last | {summary["watch_state"]["waterline_soft_degraded_last"]} |
| waterline hard degraded samples | {summary["watch_state"]["waterline_hard_degraded_samples"]} |
| waterline hard degraded last | {summary["watch_state"]["waterline_hard_degraded_last"]} |
| waterline effective rotating budget last | {summary["watch_state"]["waterline_effective_rotating_budget_last"]} |
| fast scan lag p99 max ms | {summary["watch_state"]["fast_scan_coverage_lag_p99_ms_max"]} |
| cold freshness age p95 first s | {summary["watch_state"]["cold_freshness_age_p95_secs_first"]} |
| cold freshness age p95 last s | {summary["watch_state"]["cold_freshness_age_p95_secs_last"]} |
| cold freshness age p99 max s | {summary["watch_state"]["cold_freshness_age_p99_secs_max"]} |
| cold freshness age spike count | {summary["watch_state"]["cold_freshness_age_spike_count"]} |
| cold freshness age slope max s/s | {summary["watch_state"]["cold_freshness_age_slope_max"]} |
| cold freshness age p95 delta s | {summary["watch_state"]["cold_freshness_age_p95_secs_delta"]} |
| rotating budget blocked last | {summary["watch_state"]["rotating_cold_window_budget_blocked_last"]} |
| rotating active dirs max | {summary["watch_state"]["rotating_cold_window_active_dirs_max"]} |
| rotating cycle progress max % | {summary["watch_state"]["rotating_cold_window_cycle_progress_pct_max"]} |
| rotating scan-only dirs last | {summary["watch_state"]["rotating_cold_window_scan_only_dirs_last"]} |
| proc sampler triggered watches last | {summary["watch_state"]["proc_sampler_triggered_watches_last"]} |
| passive first query success rate | {summary["passive_first_query"]["success_rate"]} |
| passive positive first query success rate | {summary["passive_positive_first_query"]["success_rate"]} |
| shutdown reconcile stable | {summary["passive_shutdown_reconcile"]["stable"]} |
| shutdown reconcile attempts max | {summary["passive_shutdown_reconcile"]["attempts_max"]} |
| shutdown reconcile paths max | {summary["passive_shutdown_reconcile"]["paths_max"]} |
| shutdown reconcile changed total | {summary["passive_shutdown_reconcile"]["changed_total"]} |
| shutdown reconcile deleted total | {summary["passive_shutdown_reconcile"]["deleted_total"]} |
| shutdown reconcile max s | {summary["passive_shutdown_reconcile"]["latency_max_secs"]} |
| snapshot quiesce ready | {summary["shutdown_snapshot_quiesce"]["ready"]} |
| snapshot quiesce rebuild observed | {summary["shutdown_snapshot_quiesce"]["rebuild_observed"]} |
| snapshot quiesce attempts max | {summary["shutdown_snapshot_quiesce"]["attempts_max"]} |
| snapshot quiesce max s | {summary["shutdown_snapshot_quiesce"]["latency_max_secs"]} |
| snapshot quiesce CPU p95 | {summary["shutdown_snapshot_quiesce"]["cpu_pct_p95"]}% |
| snapshot quiesce CPU max | {summary["shutdown_snapshot_quiesce"]["cpu_pct_max"]}% |
| snapshot quiesce RSS max | {summary["shutdown_snapshot_quiesce"]["rss_bytes_max"]} |
| event storm success rate | {summary["event_storm"]["success_rate"]} |
| event storm transport success rate | {summary["event_storm"]["transport_success_rate"]} |
| event storm positive success rate | {summary["event_storm"]["positive_success_rate"]} |
| event storm first-query p95 s | {summary["event_storm"]["first_query"]["first_query_p95_secs"]} |
| event storm first-query age p95 s | {summary["event_storm"]["first_query"]["event_age_p95_secs"]} |
| event storm after-query p95 s | {summary["event_storm"]["after_query"]["first_query_p95_secs"]} |
| event storm burst duration max s | {summary["event_storm"]["burst_duration_max_secs"]} |
| event storm immediate-query success rate | {summary["event_storm"]["immediate_query"]["success_rate"]} |
| event storm immediate-query p95 s | {summary["event_storm"]["immediate_query"]["first_query_p95_secs"]} |
| event storm delayed-query success rate | {summary["event_storm"]["delayed_query"]["success_rate"]} |
| event storm visibility success rate | {summary["event_storm"]["visibility"]["success_rate"]} |
| event storm visibility p95 s | {summary["event_storm"]["visibility"]["latency_p95_secs"]} |
| event storm visibility timeouts | {summary["event_storm"]["visibility"]["timeouts"]} |
| event storm cleanup count | {summary["event_storm"]["cleanup"]["count"]} |
| event storm cleanup failures | {summary["event_storm"]["cleanup"]["failures"]} |
| event storm cleanup entries estimated | {summary["event_storm"]["cleanup"]["entries_estimated_total"]} |
| event storm cleanup p95 s | {summary["event_storm"]["cleanup"]["duration_p95_secs"]} |
| post-cleanup audit count | {summary["event_storm"]["post_cleanup_audit"]["count"]} |
| post-cleanup audit failures | {summary["event_storm"]["post_cleanup_audit"]["failures"]} |
| post-cleanup watcher ledger cleared | {summary["event_storm"]["post_cleanup_audit"]["watcher_ledger_cleared"]} |
| post-cleanup audit window valid | {summary["event_storm"]["post_cleanup_audit"]["audit_window_valid"]} |
| hot layer query success rate | {summary["hot_layer_query"]["success_rate"]} |
| hot layer query p95 s | {summary["hot_layer_query"]["first_query_p95_secs"]} |
| inode reuse status | {summary["event_storm"]["special"]["inode_reuse_status"]} |
| index total files | {summary["scale_aware"]["index_total_files"]} |
| index total dirs | {summary["scale_aware"]["index_total_dirs"]} |
| cold dir count (L2+L3) | {summary["scale_aware"]["cold_dir_count"]} |
| hot dir count (L0+L1) | {summary["scale_aware"]["hot_dir_count"]} |
| rotation cycle estimate s | {summary["scale_aware"]["rotation_cycle_estimate_secs"]} |
| memory per file bytes | {summary["scale_aware"]["memory_per_file_bytes"]} |
| tier dist start (L0/L1/L2/L3) | {summary["tier_distribution"]["start_of_run"]} |
| tier dist storm start (L0/L1/L2/L3) | {summary["tier_distribution"]["storm_start"]} |
| tier dist end (L0/L1/L2/L3) | {summary["tier_distribution"]["end_of_run"]} |
| index health last | {summary["health"]["index_health_last"]} |

## Memory lifecycle and L2 timeline

`rebuild` covers samples where `/memory.rebuild.in_progress` is true.
`initial_build_publish` covers either the legacy first hot-base publication or
the owned rebuild generation while it is pending/writing its first cold remount.
`cold_steady` covers cold-base samples with no hot entries. `hot_base_snapshot`
covers later full-base materialization only when that state is visible through
`/memory`. `final_snapshot_window` begins at the
runner's exact SIGTERM boundary. Periodic direct-streaming snapshot intervals
cannot be distinguished until daemon lifecycle telemetry is available; affected
peaks may remain under `cold_steady`, as recorded in the limitations field.

Formal memory A/B legs must each start by restoring the same pristine read-only
VM/disk snapshot. A completed verified fixture manifest proves generation-time
completeness only; it does not prove the current tree or page-cache state.
Sequential `--sweep-config` variants therefore remain exploratory and are not a
substitute for per-leg VM snapshot restoration.

```json
{json.dumps(memory_report, ensure_ascii=False, indent=2)}
```

## Canary

Active canary numbers include immediate query polling and can measure query-triggered repair.
Passive canary numbers use create-first/query-later probes and are better for background freshness.

### Active canary

```json
{json.dumps(summary["canary_active"], ensure_ascii=False, indent=2)}
```

### Passive first-query canary

```json
{json.dumps(summary["passive_first_query"], ensure_ascii=False, indent=2)}
```

### Passive positive first-query canary

```json
{json.dumps(summary["passive_positive_first_query"], ensure_ascii=False, indent=2)}
```

### Passive all records

```json
{json.dumps(summary["canary_passive"], ensure_ascii=False, indent=2)}
```

## Event storm

Event storm records are synthetic fixture bursts. They include rw100, save100, git-clone-like, npm-install-like, subtree-rename, mount-offline simulation, inode-reuse, and mtime-skew writes, then measure first-query visibility after the configured settle window.

```json
{json.dumps(summary["event_storm"], ensure_ascii=False, indent=2)}
```

### Event storm by workload

```json
{json.dumps(summary["event_storm"]["by_workload"], ensure_ascii=False, indent=2)}
```

### Event storm by tier

```json
{json.dumps(summary["event_storm"]["by_tier_before"], ensure_ascii=False, indent=2)}
```

### Event storm special checks

```json
{json.dumps(summary["event_storm"]["special"], ensure_ascii=False, indent=2)}
```

## Hot-layer query (mixed workload)

Hot-layer query latency from the background hot-churn thread (--mixed-workload). Measures how quickly newly-created files in hot roots become searchable while cold rotation is active.

```json
{json.dumps(summary["hot_layer_query"], ensure_ascii=False, indent=2)}
```

## Scale-aware metrics

Scale-aware metrics for realistic 1M-file testing: total indexed files/dirs, cold/hot directory counts, estimated full rotation cycle time, memory efficiency, and the cold freshness age trend over time.

```json
{json.dumps(summary["scale_aware"], ensure_ascii=False, indent=2)}
```

## Files

- `config-home/fd-rdd/config.toml`: isolated fd-rdd config for this run.
- `fd-rdd.log`: daemon stdout/stderr.
- `endpoint-samples.jsonl`: periodic `/health`, `/status`, `/metrics`, `/memory`, `/watch-state`.
- `process-samples.jsonl`: `/proc/<pid>` CPU/RSS/FD/thread samples.
- `canary-samples.jsonl`: optional active and passive create/rename/delete evidence.
- `event-storm-samples.jsonl`: optional synthetic event burst writes and first-query evidence.
- `hot-churn-samples.jsonl`: optional mixed-workload hot-root churn and hot-layer query evidence.
- `reports/metrics/*.json`: fd-rdd built-in JSONL metrics, reusable for jq/offline analysis.
"""
    (run_dir / "REPORT.md").write_text(report, encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="fd-rdd M2 cold-window VM benchmark runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Realistic 1M-file mode recommendations:\n"
            "  --runtime-profile memory_light      lower overlay flush thresholds (50K vs 250K paths), reduces peak RSS\n"
            "  --l1-empty-scans-to-l2 1            demote after 1 empty scan instead of 5 (default)\n"
            "  --l1-scan-interval-secs 5           scan every 5s instead of 30s for faster demotion\n"
            "  --event-storm-start-delay-secs 120  settle phase: 120s allows tier demotion before events arrive\n"
            "  --snapshot-path-disk               put snapshot on disk instead of tmpfs (mmap pages count toward RSS)\n"
            "\n"
            "Example realistic-mode command:\n"
            "  python3 scripts/m2-cold-window-vm-bench.py --root /path/to/fixture \\\n"
            "    --realistic-mode --hot-roots ... --cold-roots ... \\\n"
            "    --runtime-profile memory_light --l1-empty-scans-to-l2 1 \\\n"
            "    --l1-scan-interval-secs 5 --event-storm-start-delay-secs 120 \\\n"
            "    --snapshot-path-disk --event-storm --duration-secs 3600\n"
        ),
    )
    parser.add_argument("--root", action="append", required=True, help="indexed root; repeatable")
    parser.add_argument("--repo", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--binary", default="target/release/fd-rdd")
    parser.add_argument("--build", choices=["auto", "always", "never"], default="auto")
    parser.add_argument(
        "--artifact-provenance-receipt",
        default="",
        help="JSON receipt proving that a --build never artifact came from this checkout",
    )
    parser.add_argument("--run-label", default="rotating")
    parser.add_argument("--run-dir", default="")
    parser.add_argument("--port", type=int, default=6060)
    parser.add_argument("--duration-secs", type=int, default=3600, help="0 means until Ctrl-C")
    parser.add_argument("--sample-interval-secs", type=float, default=10.0)
    parser.add_argument(
        "--process-sample-interval-secs",
        type=float,
        default=0.5,
        help="independent procfs RSS sampling interval; unaffected by endpoint latency",
    )
    parser.add_argument("--snapshot-interval-secs", type=int, default=300)
    parser.add_argument("--watch-mode", choices=["tiered", "recursive", "off"], default="tiered")
    parser.add_argument(
        "--runtime-profile",
        choices=["default", "memory_light", "memory-light"],
        default="default",
        help="runtime profile. 'memory_light' is recommended for 1M-file tests: it "
             "uses lower overlay flush thresholds (50K paths vs 250K), reducing peak RSS.",
    )
    parser.add_argument("--tiered-profile", choices=["balanced", "strict", "low_power"], default="balanced")
    parser.add_argument("--include-hidden", action="store_true")
    parser.add_argument("--rotating-cold-window", dest="rotating_cold_window", action="store_true", default=True)
    parser.add_argument("--no-rotating-cold-window", dest="rotating_cold_window", action="store_false")
    parser.add_argument("--rotating-budget", type=int, default=128)
    parser.add_argument("--rotating-tick-secs", type=int, default=30)
    parser.add_argument("--rotating-ttl-secs", type=int, default=180)
    parser.add_argument("--rotating-max-cost-per-root", type=int, default=64)
    parser.add_argument("--rotating-max-dirs-per-tick", type=int, default=8)
    parser.add_argument("--max-watch-dirs", type=int, default=131072)
    parser.add_argument("--l0-max-cost-per-root", type=int, default=8192)
    parser.add_argument(
        "--l1-scan-interval-secs",
        type=int,
        default=30,
        help="L1 directory scan interval in seconds. For realistic 1M-file mode, use 5 "
             "with --l1-empty-scans-to-l2 1 for rapid tier demotion.",
    )
    parser.add_argument("--l2-scan-interval-secs", type=int, default=300)
    parser.add_argument("--l3-scan-interval-secs", type=int, default=21600)
    parser.add_argument(
        "--l1-empty-scans-to-l2",
        type=int,
        default=5,
        help="consecutive empty L1 scans required to demote a directory to L2. "
             "For realistic 1M-file mode, use 1 for rapid demotion. Default 5.",
    )
    parser.add_argument(
        "--l2-empty-scans-to-l3",
        type=int,
        default=3,
        help="consecutive empty L2 scans required to demote a directory to L3. Default 3.",
    )
    parser.add_argument("--fast-scan", dest="fast_scan", action="store_true", default=True)
    parser.add_argument("--no-fast-scan", dest="fast_scan", action="store_false")
    parser.add_argument("--proc-sampler", dest="proc_sampler", action="store_true", default=True)
    parser.add_argument("--no-proc-sampler", dest="proc_sampler", action="store_false")
    parser.add_argument("--canary-root", default="")
    parser.add_argument("--canary-interval-secs", type=float, default=60.0)
    parser.add_argument("--canary-timeout-secs", type=float, default=30.0)
    parser.add_argument("--passive-canary-root", default="")
    parser.add_argument("--passive-canary-interval-secs", type=float, default=180.0)
    parser.add_argument("--passive-canary-settle-secs", type=float, default=90.0)
    parser.add_argument("--passive-canary-timeout-secs", type=float, default=0.0)
    parser.add_argument("--passive-canary-start-delay-secs", type=float, default=60.0)
    parser.add_argument(
        "--event-storm",
        action="store_true",
        help="inject synthetic filesystem bursts and summarize first-query catch-up",
    )
    parser.add_argument(
        "--event-storm-root",
        action="append",
        default=[],
        help="storm root; repeatable. Defaults to indexed roots.",
    )
    parser.add_argument(
        "--event-storm-kind",
        default="rw100,save100,git_clone,npm_install",
        help=(
            "comma-separated: rw100,save100,git_clone,npm_install,subtree_rename,"
            "mount_storm,inode_reuse,inode_reuse_stress,time_skew"
        ),
    )
    parser.add_argument(
        "--event-storm-start-delay-secs",
        type=float,
        default=120.0,
        help="seconds to wait after fd-rdd starts before beginning the event storm. "
             "This serves as a settle phase that lets tiered-watch demote directories "
             "from L1 to L2/L3 before events arrive. For realistic 1M-file mode, use 120+ "
             "with --l1-empty-scans-to-l2 1 --l1-scan-interval-secs 5. Default 120.",
    )
    parser.add_argument("--event-storm-interval-secs", type=float, default=300.0)
    parser.add_argument("--event-storm-settle-secs", type=float, default=120.0)
    parser.add_argument("--event-storm-timeout-secs", type=float, default=0.0)
    parser.add_argument(
        "--event-storm-max-bursts",
        type=int,
        default=0,
        help="stop scheduling new bursts after this count; 0 keeps running until shutdown",
    )
    parser.add_argument("--event-storm-ops", type=int, default=100,
                        help="ops per burst. Default 100; 500-1000 recommended for stress testing.")
    parser.add_argument("--event-storm-duration-budget-secs", type=float, default=1.0)
    parser.add_argument(
        "--event-storm-time-skew-secs",
        type=float,
        default=3600.0,
        help="mtime backdating used by the time_skew fixture; does not change system clock",
    )
    parser.add_argument(
        "--event-storm-target-tier",
        default="L0,L1,L2,L3",
        help="comma-separated preferred tiers for successive bursts; falls back to roots",
    )
    parser.add_argument(
        "--event-storm-file-count",
        type=int,
        default=0,
        help="explicit number of files each file-producing workload creates. "
             "0 (default) keeps the per-workload ops-based defaults (backwards compatible).",
    )
    parser.add_argument(
        "--event-storm-depth",
        type=int,
        default=2,
        help="directory tree depth for the subtree_rename avalanche workload (default 2).",
    )
    parser.add_argument(
        "--event-storm-inode-stress-iterations",
        type=int,
        default=0,
        help="iterations for the inode_reuse_stress workload (default 0 => 100).",
    )
    parser.add_argument(
        "--event-storm-inode-stress-tmpfs-inodes",
        type=int,
        default=200,
        help="nr_inodes for the inode_reuse_stress tmpfs mount (default 200).",
    )
    parser.add_argument(
        "--realistic-mode",
        action="store_true",
        help="enable realistic-scale benchmark mode (1M-file fixture with tiered "
             "directory layout). Requires --hot-roots/--cold-roots/--warm-roots "
             "to point at an existing realistic fixture.",
    )
    parser.add_argument(
        "--hot-roots",
        default="",
        help="comma-separated paths that should be L0 hot (e.g. Projects/). "
             "Used by --mixed-workload and --canary-in-fixture.",
    )
    parser.add_argument(
        "--cold-roots",
        default="",
        help="comma-separated L2/L3 cold paths (e.g. Downloads,Pictures,Music). "
             "Used by --canary-in-fixture and cold dir counting.",
    )
    parser.add_argument(
        "--warm-roots",
        default="",
        help="comma-separated L1 warm paths (e.g. Documents,.config).",
    )
    parser.add_argument(
        "--immediate-query-settle-secs",
        type=float,
        default=5.0,
        help="for immediate-query event storm mode, settle time before the first "
             "query pass (default 5s). Simulates a user searching right after "
             "downloading a file.",
    )
    parser.add_argument(
        "--event-storm-immediate-query",
        action="store_true",
        help="after each burst, do TWO query passes: an immediate pass at "
             "--immediate-query-settle-secs and a delayed pass at "
             "--event-storm-settle-secs. Reports both in summary.",
    )
    parser.add_argument(
        "--event-storm-visibility-probes-per-burst",
        type=int,
        default=0,
        help="number of deterministic positive paths continuously polled per burst; 0 disables",
    )
    parser.add_argument(
        "--event-storm-visibility-poll-interval-secs",
        type=float,
        default=1.0,
        help="poll interval for non-blocking write-to-visible probes (default 1s)",
    )
    parser.add_argument(
        "--event-storm-post-cleanup-audit-secs",
        type=float,
        default=0.0,
        help=(
            "after each burst cleanup, wait this many seconds and record exact "
            "rotating/ephemeral watcher ledger state; 0 disables"
        ),
    )
    parser.add_argument(
        "--event-storm-precondition-wait-secs",
        type=float,
        default=0.0,
        help=(
            "wait up to this many seconds for strict tier/lease preconditions "
            "instead of failing the burst immediately; 0 disables"
        ),
    )
    parser.add_argument(
        "--event-storm-min-lease-remaining-secs",
        type=float,
        default=0.0,
        help=(
            "strict treatment bursts require at least this many seconds of "
            "remaining M2 lease; 0 keeps the legacy expiry-only check"
        ),
    )
    parser.add_argument(
        "--event-storm-fixed-root-schedule",
        action="store_true",
        help="select deterministic fixture child directories instead of post-treatment tiers",
    )
    parser.add_argument(
        "--event-storm-strict-protocol",
        action="store_true",
        help=(
            "fail the run before the first fixture mutation when the requested tier "
            "or treatment-specific M2 evidence cannot be proven"
        ),
    )
    parser.add_argument(
        "--event-storm-deterministic-plan",
        action="store_true",
        help="derive event-storm paths from --workload-seed for paired A/B identity",
    )
    parser.add_argument(
        "--mixed-workload",
        action="store_true",
        help="run a background thread that continuously churns files in hot roots "
             "(IDE saves, git ops, build artifacts) while cold rotation runs. "
             "Tracks hot-layer query latency separately.",
    )
    parser.add_argument(
        "--mixed-workload-interval-secs",
        type=float,
        default=3.0,
        help="approximate interval between hot churn batches (default 3s, "
             "jittered 2-5s).",
    )
    parser.add_argument(
        "--workload-seed",
        type=int,
        default=42,
        help="deterministic seed for mixed-workload file counts, kinds, and jitter",
    )
    parser.add_argument(
        "--canary-in-fixture",
        action="store_true",
        help="place canary files inside the realistic fixture's cold/hot "
             "directories instead of separate canary roots. Passive canary in "
             "cold dirs, active canary in hot dirs.",
    )
    parser.add_argument(
        "--sweep-config",
        default="",
        help="path to a JSON sweep config file. When set, runs one benchmark per "
             "variant (overriding base args) and prints a comparison table. "
             "Format: {\"base_args\": {...}, \"variants\": [{\"label\": ..., ...}]}.",
    )
    parser.add_argument(
        "--snapshot-path-disk",
        action="store_true",
        default=False,
        help="put the fd-rdd snapshot (index.db) on a known disk path "
             "($HOME/.fd-rdd-bench-snapshots/<run-label>/index.db) instead of the run "
             "directory (which may be on tmpfs if /tmp is tmpfs). Important because "
             "mmap'd snapshot pages on tmpfs count toward RSS.",
    )
    parser.add_argument("--startup-timeout-secs", type=float, default=60.0)
    parser.add_argument(
        "--shutdown-timeout-secs",
        type=float,
        default=300.0,
        help="maximum seconds for the pre-SIGTERM snapshot/rebuild barrier, "
        "and separately for the final snapshot after SIGTERM",
    )
    return parser.parse_args()


def _update_manifest(
    manifest_path: Path,
    manifest: dict[str, Any],
    **updates: Any,
) -> None:
    manifest.update(updates)
    atomic_write_json(manifest_path, manifest)


def _start_daemon_process(
    cmd: list[str],
    *,
    port: int,
    run_dir: Path,
    env: dict[str, str],
    process_sample_interval_secs: float,
) -> tuple[subprocess.Popen[bytes], Any, ProcessSampleRunner, float]:
    """Start daemon and sampler, cleaning partial resources if startup fails."""
    log_file = (run_dir / "fd-rdd.log").open("wb")
    proc: subprocess.Popen[bytes] | None = None
    samples: ProcessSampleRunner | None = None
    try:
        if not port_is_free(port):
            raise SystemExit(f"127.0.0.1:{port} is already in use")
        proc = subprocess.Popen(
            cmd,
            cwd=run_dir,
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
        started_at = time.monotonic()
        samples = ProcessSampleRunner(
            proc.pid,
            run_dir / "process-samples.jsonl",
            started_at,
            process_sample_interval_secs,
            process_running=lambda: proc.poll() is None,
        )
        samples.start()
        return proc, log_file, samples, started_at
    except BaseException:
        if samples is not None:
            samples.stop()
        if proc is not None and proc.poll() is None:
            try:
                proc.terminate()
                proc.wait(timeout=3)
            except Exception:  # noqa: BLE001 - startup failure must still clean up
                try:
                    proc.kill()
                    proc.wait(timeout=3)
                except Exception:
                    pass
        log_file.close()
        raise


def run_single(args: argparse.Namespace) -> dict[str, Any]:
    """Run one benchmark with an atomic, fail-closed lifecycle manifest."""
    repo = Path(args.repo).resolve()
    run_dir = (
        Path(args.run_dir)
        if args.run_dir
        else repo
        / "reports"
        / "m2-cold-window-vm"
        / f"{utc_stamp()}_{args.run_label}"
    ).resolve()
    args.run_dir = str(run_dir)
    run_dir_preexisting = run_dir.exists()
    checkout_git_sha = git_head_sha(repo)
    checkout_git_dirty = git_worktree_dirty(repo)
    run_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = run_dir / "manifest.json"
    attempt_started_at = time.monotonic()
    manifest: dict[str, Any] = {
        "label": args.run_label,
        "created_at": utc_now(),
        "repo": str(repo),
        "git_sha": checkout_git_sha,
        "git_dirty": checkout_git_dirty,
        "run_dir": str(run_dir),
        "run_dir_preexisting": run_dir_preexisting,
        "runner_args": manifest_runner_args(args),
        "run_state": "preparing",
        "failure_stage": "preflight",
        "completion_reason": "",
        "ab_comparable": False,
        "ab_comparability_reasons": ["run_not_completed"],
    }
    atomic_write_json(manifest_path, manifest)
    try:
        return _run_single_prepared(
            args,
            repo=repo,
            run_dir=run_dir,
            manifest_path=manifest_path,
            manifest=manifest,
        )
    except BaseException as exc:
        stage = str(manifest.get("failure_stage", "preflight"))
        startup_stages = {
            "preflight",
            "build",
            "fixture_validation",
            "configuring",
            "starting_daemon",
        }
        failure_reasons = list(manifest.get("ab_comparability_reasons", []) or [])
        if "run_failed_before_completion" not in failure_reasons:
            failure_reasons.append("run_failed_before_completion")
        _update_manifest(
            manifest_path,
            manifest,
            run_state="failed",
            failure_stage=stage,
            completion_reason=(
                "startup_failed" if stage in startup_stages else "runner_exception"
            ),
            fatal_error=repr(exc),
            finished_at=utc_now(),
            attempt_duration_secs=round(time.monotonic() - attempt_started_at, 3),
            ab_comparable=False,
            ab_comparability_reasons=failure_reasons,
        )
        raise


def _run_single_prepared(
    args: argparse.Namespace,
    *,
    repo: Path,
    run_dir: Path,
    manifest_path: Path,
    manifest: dict[str, Any],
) -> dict[str, Any]:
    _update_manifest(manifest_path, manifest, failure_stage="build")
    source_binary = Path(args.binary)
    if not source_binary.is_absolute():
        source_binary = repo / source_binary
    source_binary = source_binary.resolve()
    artifact_provenance = build_if_needed(
        repo,
        source_binary,
        args.build,
        str(manifest.get("git_sha", "")),
        (
            Path(args.artifact_provenance_receipt)
            if args.artifact_provenance_receipt
            else None
        ),
    )
    if not source_binary.exists():
        raise SystemExit(f"binary not found: {source_binary}")
    if (
        artifact_provenance.get("verified") is not True
        and (args.artifact_provenance_receipt or args.build == "always")
    ):
        errors = artifact_provenance.get("receipt_validation_errors", [])
        detail = ",".join(str(error) for error in errors) or "unverified build"
        raise RuntimeError(f"artifact_provenance_unverified: {detail}")
    binary, execution_binary_sha256 = stage_execution_binary(
        source_binary,
        run_dir,
        str(artifact_provenance.get("validated_binary_sha256", "")),
    )
    artifact_provenance["source_binary"] = str(source_binary)
    artifact_provenance["execution_binary"] = str(binary)
    artifact_provenance["execution_binary_sha256"] = execution_binary_sha256
    _update_manifest(manifest_path, manifest, failure_stage="fixture_validation")
    hot_roots = resolve_root_paths(args.hot_roots)
    cold_roots = resolve_root_paths(args.cold_roots)
    if args.realistic_mode:
        validate_realistic_fixture(args)
    if args.canary_in_fixture:
        passive_dir, active_dir = pick_fixture_canary_dirs(cold_roots, hot_roots)
        if passive_dir is not None:
            args.passive_canary_root = str(passive_dir)
        if active_dir is not None:
            args.canary_root = str(active_dir)

    fixture_roots = [Path(root).expanduser().resolve() for root in args.root]
    fixture_identity = load_fixture_identity(fixture_roots)
    print(
        "Fixture declared file count: "
        f"{fixture_identity.get('file_count')} "
        f"(source: {fixture_identity.get('count_source')})",
        flush=True,
    )

    _update_manifest(manifest_path, manifest, failure_stage="configuring")
    config_home = run_dir / "config-home"
    runtime_dir = run_dir / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    cfg_path = write_config(args, config_home)
    env = os.environ.copy()
    env["XDG_CONFIG_HOME"] = str(config_home)
    env["XDG_RUNTIME_DIR"] = str(runtime_dir)
    env.setdefault("RUST_LOG", "info")
    base_url = f"http://127.0.0.1:{args.port}"
    if args.snapshot_path_disk:
        home = Path(os.environ.get("HOME", str(Path.home())))
        snapshot_dir = home / ".fd-rdd-bench-snapshots" / safe_name(args.run_label)
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        snapshot_path = snapshot_dir / "index.db"
        print(f"Snapshot path (on disk): {snapshot_path}", flush=True)
    else:
        snapshot_path = run_dir / "index.db"
    uds_socket = short_uds_socket_path(run_dir)
    cleanup_uds_socket(uds_socket)
    cmd = [
        str(binary),
        "--http-port",
        str(args.port),
        "--snapshot-path",
        str(snapshot_path),
        "--uds-socket",
        str(uds_socket),
        "--watch-mode",
        args.watch_mode,
        "--runtime-profile",
        args.runtime_profile,
        "--snapshot-interval-secs",
        str(args.snapshot_interval_secs),
    ]
    for root in args.root:
        cmd.extend(["--root", str(Path(root).expanduser().resolve())])

    effective_runner_args = manifest_runner_args(args)
    parameter_inputs = ab_parameter_fingerprint_inputs(effective_runner_args)
    git_sha = str(manifest.get("git_sha", ""))
    initial_state = build_initial_state(
        snapshot_path,
        fixture_identity,
        rust_log=str(env["RUST_LOG"]),
        binary=binary,
        git_sha=git_sha,
        git_dirty=manifest.get("git_dirty"),
        artifact_provenance=artifact_provenance,
        run_dir_preexisting=bool(manifest.get("run_dir_preexisting")),
    )
    initially_comparable, initial_reasons = evaluate_ab_comparability(initial_state)
    _update_manifest(
        manifest_path,
        manifest,
        run_state="starting",
        failure_stage="starting_daemon",
        git_sha=git_sha,
        git_dirty=initial_state["git_dirty"],
        binary=str(binary),
        binary_sha256=initial_state["binary_sha256"],
        artifact_provenance=artifact_provenance,
        config=str(cfg_path),
        base_url=base_url,
        uds_socket=str(uds_socket),
        command=cmd,
        roots=[str(root) for root in fixture_roots],
        runner_args=effective_runner_args,
        duration_secs=args.duration_secs,
        sample_interval_secs=args.sample_interval_secs,
        process_sample_interval_secs=args.process_sample_interval_secs,
        event_storm_ops=args.event_storm_ops,
        mixed_workload=args.mixed_workload,
        workload_seed=args.workload_seed,
        fixture=fixture_identity,
        fixture_initial_file_count=fixture_identity.get("file_count"),
        fixture_initial_file_count_source=fixture_identity.get("count_source"),
        fixture_initial_file_count_errors=fixture_identity.get("errors", []),
        initial_state=initial_state,
        initial_state_fingerprint_schema=INITIAL_STATE_FINGERPRINT_SCHEMA,
        initial_state_fingerprint=initial_state_fingerprint(initial_state),
        ab_parameter_fingerprint_schema=AB_PARAMETER_FINGERPRINT_SCHEMA,
        ab_parameter_fingerprint=ab_parameter_fingerprint(effective_runner_args),
        ab_parameter_fingerprint_inputs=parameter_inputs,
        ab_comparable=initially_comparable,
        ab_comparability_reasons=initial_reasons,
        rotating_cold_window=args.rotating_cold_window,
        active_canary_root=(
            str(Path(args.canary_root).expanduser().resolve())
            if args.canary_root
            else ""
        ),
        passive_canary_root=(
            str(Path(args.passive_canary_root).expanduser().resolve())
            if args.passive_canary_root
            else ""
        ),
        passive_canary_settle_secs=args.passive_canary_settle_secs,
        event_storm=args.event_storm,
        event_storm_roots=[
            str(Path(root).expanduser().resolve())
            for root in (args.event_storm_root or args.root)
        ],
        event_storm_kind=normalize_event_storm_kinds(
            split_csv(args.event_storm_kind)
        ),
        event_storm_target_tier=split_csv(args.event_storm_target_tier),
        event_storm_time_skew_secs=args.event_storm_time_skew_secs,
        event_storm_file_count=args.event_storm_file_count,
        event_storm_depth=args.event_storm_depth,
        event_storm_inode_stress_iterations=args.event_storm_inode_stress_iterations,
        event_storm_inode_stress_tmpfs_inodes=(
            args.event_storm_inode_stress_tmpfs_inodes
        ),
        snapshot_path_disk=args.snapshot_path_disk,
        snapshot_path=str(snapshot_path),
        snapshot_initial_state=initial_state["snapshot"],
        rust_log=str(env["RUST_LOG"]),
        shutdown_signal_elapsed_secs=None,
    )

    if sha256_file(binary) != initial_state["binary_sha256"]:
        raise RuntimeError("artifact_binary_identity_changed_before_exec")
    proc, log_file, process_samples, started_at = _start_daemon_process(
        cmd,
        port=args.port,
        run_dir=run_dir,
        env=env,
        process_sample_interval_secs=args.process_sample_interval_secs,
    )
    exit_code: int | None = None
    fatal_error = ""
    completion_reason = "runtime_error"
    cleanup_errors: list[str] = []
    shutdown_signal_elapsed_secs: float | None = None
    hot_churn: HotChurnRunner | None = None
    event_storm: EventStormRunner | None = None
    passive_canary: PassiveCanaryRunner | None = None
    canary_root = (
        Path(args.canary_root).expanduser().resolve()
        if args.canary_root
        else None
    )
    try:
        passive_canary = (
            PassiveCanaryRunner(
                base_url=base_url,
                root=Path(args.passive_canary_root).expanduser().resolve(),
                out_path=run_dir / "canary-samples.jsonl",
                started_at=started_at,
                interval_secs=args.passive_canary_interval_secs,
                settle_secs=args.passive_canary_settle_secs,
                timeout_secs=args.passive_canary_timeout_secs,
                start_delay_secs=args.passive_canary_start_delay_secs,
            )
            if args.passive_canary_root
            else None
        )
        _update_manifest(
            manifest_path,
            manifest,
            run_state="running",
            failure_stage="runtime",
            daemon_pid=proc.pid,
            daemon_started_at=utc_now(),
        )
        wait_for_http(
            base_url,
            args.startup_timeout_secs,
            process=proc,
            daemon_log_path=run_dir / "fd-rdd.log",
            expected_port=args.port,
        )
        json_line(
            run_dir / "events.jsonl",
            {"ts": utc_now(), "event": "http_ready", "pid": proc.pid},
        )
        next_endpoint_sample = time.monotonic()
        next_canary = time.monotonic() + args.canary_interval_secs
        deadline = (
            None if args.duration_secs == 0 else time.monotonic() + args.duration_secs
        )
        event_storm = (
            EventStormRunner(
                base_url=base_url,
                roots=[
                    Path(root).expanduser().resolve()
                    for root in (args.event_storm_root or args.root)
                ],
                out_path=run_dir / "event-storm-samples.jsonl",
                started_at=started_at,
                start_delay_secs=args.event_storm_start_delay_secs,
                interval_secs=args.event_storm_interval_secs,
                settle_secs=args.event_storm_settle_secs,
                timeout_secs=args.event_storm_timeout_secs,
                ops_per_burst=args.event_storm_ops,
                duration_budget_secs=args.event_storm_duration_budget_secs,
                time_skew_secs=args.event_storm_time_skew_secs,
                kinds=normalize_event_storm_kinds(split_csv(args.event_storm_kind)),
                target_tiers=split_csv(args.event_storm_target_tier),
                file_count=args.event_storm_file_count,
                subtree_depth=args.event_storm_depth,
                inode_stress_iterations=args.event_storm_inode_stress_iterations,
                inode_stress_tmpfs_inodes=args.event_storm_inode_stress_tmpfs_inodes,
                immediate_query_enabled=args.event_storm_immediate_query,
                immediate_query_settle_secs=args.immediate_query_settle_secs,
                max_bursts=args.event_storm_max_bursts,
                visibility_probes_per_burst=(
                    args.event_storm_visibility_probes_per_burst
                ),
                visibility_poll_interval_secs=(
                    args.event_storm_visibility_poll_interval_secs
                ),
                fixed_root_schedule=args.event_storm_fixed_root_schedule,
                deterministic_plan_seed=(
                    args.workload_seed
                    if args.event_storm_deterministic_plan
                    else None
                ),
                rotating_tick_secs=args.rotating_tick_secs,
                rotating_ttl_secs=args.rotating_ttl_secs,
                rotating_dirs_per_tick=args.rotating_max_dirs_per_tick,
                strict_protocol=args.event_storm_strict_protocol,
                treatment_enabled=args.rotating_cold_window,
                post_cleanup_audit_secs=(
                    args.event_storm_post_cleanup_audit_secs
                ),
                precondition_wait_secs=(
                    args.event_storm_precondition_wait_secs
                ),
                min_lease_remaining_secs=(
                    args.event_storm_min_lease_remaining_secs
                ),
            )
            if args.event_storm
            else None
        )
        hot_churn = (
            HotChurnRunner(
                base_url=base_url,
                roots=hot_roots,
                out_path=run_dir / "hot-churn-samples.jsonl",
                started_at=started_at,
                interval_secs=args.mixed_workload_interval_secs,
                workload_seed=args.workload_seed,
            )
            if args.mixed_workload and hot_roots
            else None
        )
        if hot_churn is not None:
            hot_churn.start()
        if event_storm is not None and args.event_storm_start_delay_secs > 0:
            print(
                f"Settle phase: waiting {args.event_storm_start_delay_secs:.0f} seconds "
                "for tier demotion before event storm starts...",
                flush=True,
            )

        while True:
            if proc.poll() is not None:
                exit_code = proc.returncode
                completion_reason = "daemon_exit"
                break
            now = time.monotonic()
            if deadline is not None and now >= deadline:
                completion_reason = "duration_elapsed"
                break
            if now >= next_endpoint_sample:
                collect_endpoint_samples(
                    base_url, run_dir / "endpoint-samples.jsonl", started_at
                )
                next_endpoint_sample = time.monotonic() + args.sample_interval_secs
                now = time.monotonic()
                if deadline is not None and now >= deadline:
                    completion_reason = "duration_elapsed"
                    break
            if canary_root and now >= next_canary:
                for record in run_canary_cycle(
                    base_url, canary_root, args.canary_timeout_secs
                ):
                    record["ts"] = utc_now()
                    record["elapsed_secs"] = round(
                        time.monotonic() - started_at, 3
                    )
                    json_line(run_dir / "canary-samples.jsonl", record)
                next_canary = time.monotonic() + args.canary_interval_secs
                now = time.monotonic()
                if deadline is not None and now >= deadline:
                    completion_reason = "duration_elapsed"
                    break
            if passive_canary:
                passive_canary.tick(now)
                now = time.monotonic()
                if deadline is not None and now >= deadline:
                    completion_reason = "duration_elapsed"
                    break
            if event_storm:
                event_storm.tick(now)
                if event_storm.protocol_error:
                    completion_reason = "protocol_failed"
                    fatal_error = (
                        "event_storm_protocol_failed: "
                        f"{event_storm.protocol_error}"
                    )
                    json_line(
                        run_dir / "events.jsonl",
                        {
                            "ts": utc_now(),
                            "event": "fatal_error",
                            "error": fatal_error,
                        },
                    )
                    break
            time.sleep(0.2)
    except KeyboardInterrupt:
        completion_reason = "interrupted"
        json_line(run_dir / "events.jsonl", {"ts": utc_now(), "event": "interrupted"})
    except Exception as exc:  # noqa: BLE001 - preserve partial benchmark evidence
        completion_reason = "runtime_error"
        fatal_error = repr(exc)
        json_line(
            run_dir / "events.jsonl",
            {"ts": utc_now(), "event": "fatal_error", "error": fatal_error},
        )
    finally:
        try:
            _update_manifest(
                manifest_path,
                manifest,
                run_state="stopping",
                failure_stage="cleanup",
                completion_reason=completion_reason,
            )
        except Exception as exc:  # noqa: BLE001 - cleanup must still stop daemon
            cleanup_errors.append(f"stopping_manifest: {exc!r}")
        if hot_churn is not None:
            try:
                hot_churn.stop()
            except Exception as exc:  # noqa: BLE001 - continue daemon cleanup
                cleanup_errors.append(f"hot_churn_stop: {exc!r}")
        if proc.poll() is None:
            if passive_canary is not None:
                try:
                    reconcile_paths = benchmark_shutdown_reconcile_paths(
                        passive_canary.root,
                        canary_root,
                        event_storm,
                        args.mixed_workload,
                        hot_roots,
                    )
                    passive_canary.reconcile_shutdown(reconcile_paths)
                except BaseException as exc:  # keep SIGTERM reachable on every failure
                    cleanup_errors.append(
                        f"passive_shutdown_reconcile_record: {exc!r}"
                    )
            try:
                record_shutdown_snapshot_quiesce(
                    base_url,
                    run_dir / "shutdown-samples.jsonl",
                    started_at=started_at,
                    timeout_secs=max(1.0, float(args.shutdown_timeout_secs)),
                    daemon_log_path=run_dir / "fd-rdd.log",
                )
            except BaseException as exc:  # keep SIGTERM reachable on every failure
                cleanup_errors.append(
                    f"shutdown_snapshot_quiesce_record: {exc!r}"
                )
            signal_elapsed = round(time.monotonic() - started_at, 6)
            try:
                proc.send_signal(signal.SIGTERM)
            except Exception as exc:  # noqa: BLE001 - continue sampler/log cleanup
                cleanup_errors.append(f"daemon_signal: {exc!r}")
                if proc.poll() is None:
                    try:
                        proc.kill()
                        proc.wait(timeout=10)
                    except Exception as kill_exc:  # noqa: BLE001
                        cleanup_errors.append(f"daemon_signal_kill: {kill_exc!r}")
            else:
                shutdown_signal_elapsed_secs = signal_elapsed
                try:
                    _update_manifest(
                        manifest_path,
                        manifest,
                        shutdown_signal_elapsed_secs=shutdown_signal_elapsed_secs,
                    )
                except Exception as exc:  # noqa: BLE001
                    cleanup_errors.append(f"shutdown_manifest: {exc!r}")
                try:
                    json_line(
                        run_dir / "events.jsonl",
                        {
                            "ts": utc_now(),
                            "event": "shutdown_signal",
                            "signal": "SIGTERM",
                            "elapsed_secs": shutdown_signal_elapsed_secs,
                        },
                    )
                except OSError as exc:
                    cleanup_errors.append(f"shutdown_event: {exc!r}")
                try:
                    proc.wait(timeout=max(1.0, args.shutdown_timeout_secs))
                except subprocess.TimeoutExpired:
                    cleanup_errors.append("daemon_shutdown_timeout")
                    try:
                        proc.kill()
                        proc.wait(timeout=10)
                    except Exception as exc:  # noqa: BLE001
                        cleanup_errors.append(f"daemon_kill: {exc!r}")
                except Exception as exc:  # noqa: BLE001
                    cleanup_errors.append(f"daemon_wait: {exc!r}")
        try:
            process_samples.stop()
        except Exception as exc:  # noqa: BLE001
            cleanup_errors.append(f"process_sampler_stop: {exc!r}")
        if process_samples.error:
            try:
                json_line(
                    run_dir / "events.jsonl",
                    {
                        "ts": utc_now(),
                        "event": "process_sampler_error",
                        "error": process_samples.error,
                    },
                )
            except OSError as exc:
                cleanup_errors.append(f"sampler_error_record: {exc!r}")
            if not fatal_error:
                fatal_error = f"process sampler failed: {process_samples.error}"
        if exit_code is None:
            exit_code = proc.poll()
        try:
            log_file.close()
        except OSError as exc:
            cleanup_errors.append(f"log_close: {exc!r}")
        try:
            cleanup_uds_socket(uds_socket)
        except OSError as exc:
            cleanup_errors.append(f"uds_cleanup: {exc!r}")

    actual_duration_secs = round(time.monotonic() - started_at, 3)
    execution_state = build_execution_state(
        run_dir,
        requested_duration_secs=args.duration_secs,
        actual_duration_secs=actual_duration_secs,
        exit_code=exit_code,
        fatal_error=fatal_error,
        process_sampler_error=process_samples.error,
        completion_reason=completion_reason,
        cleanup_errors=cleanup_errors,
        shutdown_signal_elapsed_secs=shutdown_signal_elapsed_secs,
        event_storm_enabled=args.event_storm,
        mixed_workload_enabled=args.mixed_workload,
        passive_canary_enabled=bool(args.passive_canary_root),
        shutdown_snapshot_quiesce_required=True,
    )
    comparable, reasons = evaluate_ab_comparability(initial_state, execution_state)
    terminal_failed = bool(
        fatal_error
        or cleanup_errors
        or exit_code != 0
        or completion_reason == "runtime_error"
        or not execution_state["duration_completed"]
    )
    terminal_state = "failed" if terminal_failed else "completed"
    terminal_failure_stage = (
        "cleanup"
        if cleanup_errors
        else "runtime"
        if terminal_failed
        else ""
    )
    _update_manifest(
        manifest_path,
        manifest,
        run_state=terminal_state,
        failure_stage=terminal_failure_stage,
        completion_reason=completion_reason,
        finished_at=utc_now(),
        actual_duration_secs=actual_duration_secs,
        fd_rdd_exit_code=exit_code,
        process_sampler_error=process_samples.error,
        fatal_error=fatal_error,
        cleanup_errors=cleanup_errors,
        shutdown_signal_elapsed_secs=shutdown_signal_elapsed_secs,
        passive_shutdown_reconcile_count=execution_state[
            "passive_shutdown_reconcile_count"
        ],
        passive_shutdown_reconcile_ok=execution_state[
            "passive_shutdown_reconcile_ok"
        ],
        passive_shutdown_reconcile_failures=execution_state[
            "passive_shutdown_reconcile_failures"
        ],
        shutdown_snapshot_quiesce_count=execution_state[
            "shutdown_snapshot_quiesce_count"
        ],
        shutdown_snapshot_quiesce_ok=execution_state[
            "shutdown_snapshot_quiesce_ok"
        ],
        shutdown_snapshot_quiesce_failures=execution_state[
            "shutdown_snapshot_quiesce_failures"
        ],
        execution=execution_state,
        execution_fingerprint_schema=EXECUTION_FINGERPRINT_SCHEMA,
        execution_fingerprint=execution_state["fingerprint"],
        ab_comparable=comparable,
        ab_comparability_reasons=reasons,
    )

    try:
        summary = summarize(run_dir, args.run_label, exit_code)
        if fatal_error:
            summary["fatal_error"] = fatal_error
            atomic_write_json(run_dir / "summary.json", summary)
        write_report(run_dir, summary)
    except BaseException:
        _update_manifest(manifest_path, manifest, failure_stage="reporting")
        raise
    print(
        json.dumps(
            {"run_dir": str(run_dir), "summary": summary},
            ensure_ascii=False,
            indent=2,
        )
    )
    return summary


def _apply_overrides(args: argparse.Namespace, overrides: dict[str, Any]) -> argparse.Namespace:
    """Return a copy of `args` with the given CLI-style overrides applied.

    Keys use the CLI flag names with dashes (e.g. "rotating-budget"); values are
    coerced to the type of the existing attribute when possible.
    """
    import copy as _copy

    new_args = _copy.copy(args)
    attr_map = {
        key.replace("-", "_"): key for key in [
            "rotating-budget", "rotating-tick-secs", "rotating-ttl-secs",
            "rotating-max-cost-per-root", "rotating-max-dirs-per-tick",
            "rotating-cold-window", "no-rotating-cold-window",
            "duration-secs", "sample-interval-secs", "process-sample-interval-secs",
            "snapshot-interval-secs", "shutdown-timeout-secs",
            "tiered-profile", "watch-mode", "max-watch-dirs",
            "l0-max-cost-per-root", "l1-scan-interval-secs", "l2-scan-interval-secs",
            "l3-scan-interval-secs", "l1-empty-scans-to-l2", "l2-empty-scans-to-l3",
            "fast-scan", "no-fast-scan", "event-storm-ops", "event-storm-file-count",
            "event-storm-depth", "event-storm-interval-secs", "event-storm-settle-secs",
            "realistic-mode", "mixed-workload", "event-storm-immediate-query",
            "canary-in-fixture", "immediate-query-settle-secs",
            "mixed-workload-interval-secs", "workload-seed",
            "snapshot-path-disk", "event-storm-start-delay-secs",
            "event-storm-strict-protocol",
        ]
    }
    for raw_key, value in overrides.items():
        attr = raw_key.replace("-", "_")
        if attr == "label":
            new_args.run_label = str(value)
            continue
        if attr == "no_rotating_cold_window" and value:
            new_args.rotating_cold_window = False
            continue
        if attr == "rotating_cold_window":
            new_args.rotating_cold_window = bool(value)
            continue
        current = getattr(new_args, attr, None)
        if isinstance(current, bool):
            setattr(new_args, attr, bool(value))
        elif isinstance(current, int) and not isinstance(current, bool):
            setattr(new_args, attr, int(value))
        elif isinstance(current, float):
            setattr(new_args, attr, float(value))
        else:
            setattr(new_args, attr, value)
    return new_args


def run_sweep(args: argparse.Namespace) -> int:
    """Task 4: run one benchmark per sweep variant and print a comparison table."""
    import copy as _copy

    sweep_path = Path(args.sweep_config).expanduser().resolve()
    if not sweep_path.exists():
        raise SystemExit(f"sweep config not found: {sweep_path}")
    config = json.loads(sweep_path.read_text(encoding="utf-8"))
    base_overrides = config.get("base_args", {}) or {}
    variants = config.get("variants", []) or []
    if not variants:
        raise SystemExit("sweep config has no variants")

    results: list[dict[str, Any]] = []
    base_port = args.port
    for idx, variant in enumerate(variants):
        label = str(variant.get("label", f"variant-{idx:02d}"))
        variant_args = _copy.copy(args)
        variant_args.sweep_config = ""  # avoid recursion
        variant_args = _apply_overrides(variant_args, base_overrides)
        variant_args = _apply_overrides(variant_args, {k: v for k, v in variant.items() if k != "label"})
        variant_args.run_label = label
        variant_args.run_dir = ""  # auto-generate per variant
        variant_args.port = base_port + idx  # unique port per variant
        print(f"\n=== sweep [{idx + 1}/{len(variants)}] {label} ===", flush=True)
        try:
            summary = run_single(variant_args)
        except Exception as exc:  # noqa: BLE001 - keep sweep going
            print(f"sweep variant {label} failed: {repr(exc)}", flush=True)
            summary = {"label": label, "fatal_error": repr(exc)}
        summary["sweep_label"] = label
        results.append(summary)

    # Comparison table
    cols = [
        ("label", lambda s: s.get("label", s.get("sweep_label", ""))),
        ("bursts", lambda s: s.get("event_storm", {}).get("bursts", "")),
        ("es_success", lambda s: s.get("event_storm", {}).get("success_rate", "")),
        ("cf_p95_max", lambda s: s.get("watch_state", {}).get("cold_freshness_age_p95_secs_max", "")),
        ("cf_spike", lambda s: s.get("watch_state", {}).get("cold_freshness_age_spike_count", "")),
        ("cf_slope", lambda s: s.get("watch_state", {}).get("cold_freshness_age_slope_max", "")),
        ("dirty_q_max", lambda s: s.get("watch_state", {}).get("dirty_queue_len_max", "")),
        ("rss_max", lambda s: s.get("process", {}).get("rss_bytes_max", "")),
        ("cold_dirs", lambda s: s.get("scale_aware", {}).get("cold_dir_count", "")),
        ("tier_end", lambda s: s.get("tier_distribution", {}).get("end_of_run", "")),
        ("passive_sr", lambda s: s.get("passive_first_query", {}).get("success_rate", "")),
        ("inode_reuse_obs", lambda s: s.get("event_storm", {}).get("special", {}).get("inode_reuse_observed", "")),
        ("fatal", lambda s: s.get("fatal_error", "")),
    ]
    header = " | ".join(name for name, _ in cols)
    sep = "-+-".join("-" * len(name) for name, _ in cols)
    print("\n=== sweep comparison ===")
    print(header)
    print(sep)
    for s in results:
        row = []
        for _, getter in cols:
            val = getter(s)
            row.append(str(val) if val != "" else "-")
        print(" | ".join(row))

    sweep_report = {
        "generated_at": utc_now(),
        "sweep_config": str(sweep_path),
        "variants": results,
    }
    repo = Path(args.repo).resolve()
    out_path = repo / "reports" / "m2-cold-window-vm" / f"{utc_stamp()}_sweep_comparison.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(sweep_report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"\nsweep comparison written to: {out_path}")
    return (
        0
        if all(
            not summary.get("fatal_error")
            and summary.get("fd_rdd_exit_code") == 0
            and summary.get("ab_comparable") is True
            for summary in results
        )
        else 1
    )


def main() -> int:
    args = parse_args()
    if args.sweep_config:
        return run_sweep(args)
    summary = run_single(args)
    exit_code = summary.get("fd_rdd_exit_code")
    fatal = summary.get("fatal_error", "")
    return (
        0
        if not fatal
        and exit_code == 0
        and summary.get("ab_comparable") is True
        else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())
