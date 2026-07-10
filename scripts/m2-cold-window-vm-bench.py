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
from typing import Any


ENDPOINTS = ["/health", "/status", "/metrics", "/memory", "/watch-state"]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def json_line(path: Path, record: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False, separators=(",", ":"))
        f.write("\n")


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


def wait_for_http(base_url: str, timeout_secs: float) -> None:
    deadline = time.monotonic() + timeout_secs
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            http_json(base_url, "/health", timeout=1.0)
            return
        except Exception as exc:  # noqa: BLE001 - diagnostics include exact error
            last_error = exc
            time.sleep(0.25)
    raise RuntimeError(f"fd-rdd HTTP endpoint did not become ready: {last_error}")


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


def read_proc_ticks(pid: int) -> int | None:
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    rparen = stat.rfind(")")
    if rparen < 0:
        return None
    parts = stat[rparen + 2 :].split()
    if len(parts) < 15:
        return None
    # After state, utime/stime are fields 14/15 in procfs, indices 11/12 here.
    return int(parts[11]) + int(parts[12])


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
        ticks = read_proc_ticks(pid)
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
    ) -> None:
        self.pid = pid
        self.out_path = out_path
        self.started_at = started_at
        self.interval_secs = max(0.05, interval_secs)
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
                record = {
                    "ts": utc_now(),
                    "elapsed_secs": round(time.monotonic() - self.started_at, 3),
                    **next(sampler),
                }
                json_line(self.out_path, record)
                if self._stop.wait(self.interval_secs):
                    return
        except Exception as exc:  # noqa: BLE001 - surfaced in the run summary
            self.error = repr(exc)


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
        self.kinds = normalize_event_storm_kinds(kinds)
        self.target_tiers = [tier.upper() for tier in target_tiers]
        tiers = self.target_tiers or [""]
        workloads = self.kinds or ["rw100"]
        self.work_items = [(tier, kind) for tier in tiers for kind in workloads]
        # Run-unique id (timestamp + pid) folded into burst paths so concurrent or
        # repeated runs (A vs B legs, re-runs on shared storm roots) never collide
        # on fixture directories -- the root cause of OSError(39, 'Directory not empty').
        self.run_id = f"{time.time_ns()}-{os.getpid()}"
        self.next_start_at = time.monotonic() + self.start_delay_secs
        self.active: dict[str, Any] | None = None
        self.current_burst_started_at = 0.0
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
        if not self.roots:
            return
        self.cycle += 1
        # Task 3: on the very first burst, capture and report tier distribution
        # so the summary can show how many dirs demoted during the settle phase.
        if self.cycle == 1:
            self._emit_tier_distribution("storm_start")
        requested_tier, selected_kind = self.work_items[(self.cycle - 1) % len(self.work_items)]
        root = self.select_root(requested_tier)
        root.mkdir(parents=True, exist_ok=True)
        tier_before = self.tier_for_root(root)
        events: list[dict[str, Any]] = []
        cycle_started = time.monotonic()
        self.current_burst_started_at = cycle_started
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
            self.active = None
            self.next_start_at = time.monotonic() + self.interval_secs
            return
        generation_secs = time.monotonic() - cycle_started
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
            "requested_tier": requested_tier,
            "selected_kind": selected_kind,
            "tier_before": tier_before,
            "events": events,
            "started_at": cycle_started,
            "due_at": due_at,
            "stage": "immediate_query" if use_immediate else "delayed_query",
            "delayed_due_at": delayed_due,
            "immediate_done": False,
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
                "kinds": self.kinds,
            }
        )
        for event in events:
            self.emit(event)

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
            final = burst_root / f"save_{i:04d}.txt"
            tmp = burst_root / f".save_{i:04d}.tmp"
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
            (repo_root / "README.md", "fd_rdd_m2_storm_git_readme"),
            (repo_root / "src" / "main.rs", "fd_rdd_m2_storm_git_main"),
            (repo_root / "tests" / "smoke.rs", "fd_rdd_m2_storm_git_smoke"),
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
                records.append(self.expected_record("npm_install", "npm_file_visible", path, path.name, True, tier_before))
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
            old_path = parent / f"deep_{self.cycle:03d}_{i:04d}.txt"
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
        return root / f"fd-rdd-m2-event-storm-{safe_name(kind)}-{self.run_id}-{self.cycle:03d}"

    def select_root(self, requested_tier: str) -> Path:
        if requested_tier:
            try:
                dump = debug_tiered_watch(self.base_url)
                dirs = dump.get("dirs")
                if isinstance(dirs, list):
                    candidates = [
                        Path(str(item.get("path")))
                        for item in dirs
                        if isinstance(item, dict)
                        and str(item.get("watch_tier", "")).upper() == requested_tier
                        and self.within_configured_roots(Path(str(item.get("path"))))
                    ]
                    candidates = sorted(set(candidates))
                    if candidates:
                        return candidates[(self.cycle - 1) % len(candidates)]
            except Exception:
                pass
        return self.roots[(self.cycle - 1) % len(self.roots)]

    def within_configured_roots(self, path: Path) -> bool:
        try:
            resolved = path.resolve()
        except OSError:
            resolved = path
        for root in self.roots:
            try:
                resolved.relative_to(root)
                return True
            except ValueError:
                if resolved == root:
                    return True
        return False

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
        # Fix E: best-effort teardown of the just-checked storm fixtures so the
        # storm root does not grow unbounded and cannot poison a later re-run.
        # Only removes per-burst subdirs (prefixed fd-rdd-m2-event-storm-),
        # never the indexed root itself. ignore_errors so cleanup never crashes.
        try:
            for child in list(Path(self.active["root"]).iterdir()):
                if child.name.startswith("fd-rdd-m2-event-storm-"):
                    shutil.rmtree(child, ignore_errors=True)
        except Exception:
            pass
        self.active = None
        self.next_start_at = time.monotonic() + self.interval_secs

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
                    "settle_secs": round(now - float(self.active["started_at"]), 3),
                    "event_age_secs": round(
                        now
                        - float(self.active["started_at"])
                        - float(event.get("burst_elapsed_secs", 0.0))
                        + latency,
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
                            now
                            - float(self.active["started_at"])
                            - float(event.get("burst_elapsed_secs", 0.0))
                            + after_latency,
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
            dump = debug_tiered_watch(self.base_url, root)
            dirs = dump.get("dirs")
            if not isinstance(dirs, list):
                return ""
            root_str = str(root)
            exact = [item for item in dirs if isinstance(item, dict) and item.get("path") == root_str]
            if exact:
                return str(exact[0].get("watch_tier", ""))
            prefix = root_str.rstrip("/") + "/"
            candidates = [
                item for item in dirs
                if isinstance(item, dict) and str(item.get("path", "")).startswith(prefix)
            ]
            return str(candidates[0].get("watch_tier", "")) if candidates else ""
        except Exception:
            return ""

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
    ) -> None:
        self.base_url = base_url
        self.roots = [r for r in roots if r]
        self.out_path = out_path
        self.started_at = started_at
        self.interval_secs = max(0.5, interval_secs)
        self.start_delay_secs = max(0.0, start_delay_secs)
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
            jitter = random.uniform(max(1.0, self.interval_secs - 1.0), self.interval_secs + 2.0)
            if self._stop.wait(jitter):
                return

    def _churn_batch(self, batch: int) -> None:
        root = self.roots[(batch - 1) % len(self.roots)]
        churn_dir = root / f"fd-rdd-m2-hot-churn-{self._run_id}-{batch:05d}"
        churn_dir.mkdir(parents=True, exist_ok=True)
        n_files = random.randint(10, 50)
        created: list[Path] = []
        churn_started = time.monotonic()
        for i in range(n_files):
            kind = random.choice(["ide_save", "git_op", "build_artifact"])
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


def build_if_needed(repo: Path, binary: Path, build: str) -> None:
    if build == "never":
        return
    if build == "auto" and binary.exists():
        return
    subprocess.run(["cargo", "build", "--release"], cwd=repo, check=True)


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
        memory_points.append(
            {
                "elapsed_secs": float(item.get("elapsed_secs", 0.0) or 0.0),
                "phase": classify_memory_phase(data),
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
        "l2": {
            "sample_count": len(memory_points),
            "components": component_summaries,
            "series": series,
        },
    }


def summarize(run_dir: Path, label: str, exit_code: int | None) -> dict[str, Any]:
    process_samples = read_jsonl(run_dir / "process-samples.jsonl")
    endpoint_samples = read_jsonl(run_dir / "endpoint-samples.jsonl")
    canary_samples = read_jsonl(run_dir / "canary-samples.jsonl")
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
        index_total_files = int(last_status.get("total_files", 0) or last_status.get("indexed_files", 0) or 0)
        index_total_dirs = int(last_status.get("total_dirs", 0) or last_status.get("indexed_dirs", 0) or 0)
    # cold_dir_count / hot_dir_count from the last /watch-state sample's dirs list.
    cold_dir_count = 0
    hot_dir_count = 0
    if watch_samples:
        last_watch = watch_samples[-1]
        dirs = last_watch.get("dirs")
        if isinstance(dirs, list):
            for d in dirs:
                if not isinstance(d, dict):
                    continue
                tier = str(d.get("watch_tier", "")).upper()
                if tier in ("L2", "L3"):
                    cold_dir_count += 1
                elif tier in ("L0", "L1"):
                    hot_dir_count += 1
    # rotation_cycle_estimate_secs = (cold_dir_count / max_dirs_per_tick) * tick_secs.
    # Uses the last watch-state sample's rotating params when available; falls
    # back to 0 when the rotating window is disabled or no data.
    rotation_cycle_estimate_secs = 0.0
    if watch_samples and cold_dir_count > 0:
        last_watch = watch_samples[-1]
        max_dirs_per_tick = int(last_watch.get("rotating_cold_window_max_dirs_per_tick", 0) or 0)
        tick_secs = float(last_watch.get("rotating_cold_window_tick_secs", 0) or 0)
        if max_dirs_per_tick > 0 and tick_secs > 0:
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
    memory_timeline = build_memory_timeline(endpoint_samples, process_samples)

    summary = {
        "label": label,
        "generated_at": utc_now(),
        "fd_rdd_exit_code": exit_code,
        "sample_counts": {
            "process": len(process_samples),
            "endpoint": len(endpoint_samples),
            "watch_state": len(watch_samples),
            "memory": len(memory_samples),
            "health": len(health_samples),
            "canary": len(canary_samples),
            "canary_active": active_canary_count,
            "canary_passive": passive_canary_count,
            "event_storm": len(event_storm_samples),
            "event_storm_first_query": len(event_first_queries),
        },
        "process": {
            "cpu_pct_p50": round(percentile(cpu, 50), 3),
            "cpu_pct_p95": round(percentile(cpu, 95), 3),
            "cpu_pct_max": round(max(cpu) if cpu else 0.0, 3),
            "rss_bytes_p95": int(percentile(rss, 95)),
            "rss_bytes_max": max(rss) if rss else 0,
            "fd_count_max": max(fds) if fds else 0,
        },
        "watch_state": {
            "dirty_queue_len_max": int(max(nums(watch_samples, "dirty_queue_len") or [0])),
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
            "bursts": len(event_bursts),
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
- Built-in metrics: `{summary["built_in_metrics_dir"]}`

## Key single-run metrics

| Metric | Value |
|---|---:|
| process CPU p95 | {summary["process"]["cpu_pct_p95"]}% |
| process CPU max | {summary["process"]["cpu_pct_max"]}% |
| process RSS p95 | {summary["process"]["rss_bytes_p95"]} |
| process RSS max | {summary["process"]["rss_bytes_max"]} |
| rebuild RSS max | {summary["memory_timeline"]["phase_peaks"]["rebuild"]["process_rss_bytes_max"]} |
| initial build publish RSS max | {summary["memory_timeline"]["phase_peaks"]["initial_build_publish"]["process_rss_bytes_max"]} |
| cold steady RSS max | {summary["memory_timeline"]["phase_peaks"]["cold_steady"]["process_rss_bytes_max"]} |
| hot-base snapshot RSS max | {summary["memory_timeline"]["phase_peaks"]["hot_base_snapshot"]["process_rss_bytes_max"]} |
| fd count max | {summary["process"]["fd_count_max"]} |
| dirty queue max | {summary["watch_state"]["dirty_queue_len_max"]} |
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
`initial_build_publish` covers the first complete hot-base publication before the
first mmap/manifest cold remount. `cold_steady` covers cold-base samples with no
hot entries. `hot_base_snapshot` covers later periodic full-base materialization.

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
            "mount_storm,inode_reuse,time_skew"
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
    return parser.parse_args()


def run_single(args: argparse.Namespace) -> dict[str, Any]:
    """Run a single benchmark and return its summary dict."""
    repo = Path(args.repo).resolve()
    binary = Path(args.binary)
    if not binary.is_absolute():
        binary = repo / binary
    build_if_needed(repo, binary, args.build)
    if not binary.exists():
        raise SystemExit(f"binary not found: {binary}")
    if not port_is_free(args.port):
        raise SystemExit(f"127.0.0.1:{args.port} is already in use")

    # Task 1: realistic-mode fixture validation. When --realistic-mode is set we
    # verify the hot/warm/cold fixture roots exist before starting fd-rdd so the
    # user gets a helpful error instead of a confusing empty-index run.
    hot_roots = resolve_root_paths(args.hot_roots)
    warm_roots = resolve_root_paths(args.warm_roots)
    cold_roots = resolve_root_paths(args.cold_roots)
    if args.realistic_mode:
        validate_realistic_fixture(args)
    # Task 6: fixture-aware canary. Override the separate canary roots with
    # subdirectories inside the realistic fixture's cold (passive) and hot
    # (active) directories.
    if args.canary_in_fixture:
        passive_dir, active_dir = pick_fixture_canary_dirs(cold_roots, hot_roots)
        if passive_dir is not None:
            args.passive_canary_root = str(passive_dir)
        if active_dir is not None:
            args.canary_root = str(active_dir)

    run_dir = Path(args.run_dir) if args.run_dir else repo / "reports" / "m2-cold-window-vm" / f"{utc_stamp()}_{args.run_label}"
    run_dir = run_dir.resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    config_home = run_dir / "config-home"
    runtime_dir = run_dir / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    cfg_path = write_config(args, config_home)

    env = os.environ.copy()
    env["XDG_CONFIG_HOME"] = str(config_home)
    env["XDG_RUNTIME_DIR"] = str(runtime_dir)
    env.setdefault("RUST_LOG", "info")
    base_url = f"http://127.0.0.1:{args.port}"
    # Task 5: when --snapshot-path-disk is set, put the snapshot on a known disk
    # path instead of the run directory (which may be on tmpfs if /tmp is tmpfs).
    # mmap'd snapshot pages on tmpfs count toward RSS, skewing memory benchmarks.
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

    manifest = {
        "label": args.run_label,
        "created_at": utc_now(),
        "repo": str(repo),
        "binary": str(binary),
        "run_dir": str(run_dir),
        "config": str(cfg_path),
        "base_url": base_url,
        "uds_socket": str(uds_socket),
        "command": cmd,
        "roots": [str(Path(root).expanduser().resolve()) for root in args.root],
        "rotating_cold_window": args.rotating_cold_window,
        "active_canary_root": str(Path(args.canary_root).expanduser().resolve()) if args.canary_root else "",
        "passive_canary_root": (
            str(Path(args.passive_canary_root).expanduser().resolve())
            if args.passive_canary_root
            else ""
        ),
        "passive_canary_settle_secs": args.passive_canary_settle_secs,
        "event_storm": args.event_storm,
        "event_storm_roots": [
            str(Path(root).expanduser().resolve())
            for root in (args.event_storm_root or args.root)
        ],
        "event_storm_kind": normalize_event_storm_kinds(split_csv(args.event_storm_kind)),
        "event_storm_target_tier": split_csv(args.event_storm_target_tier),
        "event_storm_time_skew_secs": args.event_storm_time_skew_secs,
        "event_storm_file_count": args.event_storm_file_count,
        "event_storm_depth": args.event_storm_depth,
        "event_storm_inode_stress_iterations": args.event_storm_inode_stress_iterations,
        "event_storm_inode_stress_tmpfs_inodes": args.event_storm_inode_stress_tmpfs_inodes,
        "snapshot_path_disk": args.snapshot_path_disk,
        "snapshot_path": str(snapshot_path),
        "process_sample_interval_secs": args.process_sample_interval_secs,
    }
    (run_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    log_file = (run_dir / "fd-rdd.log").open("wb")
    proc = subprocess.Popen(cmd, cwd=run_dir, env=env, stdout=log_file, stderr=subprocess.STDOUT)
    started_at = time.monotonic()
    process_samples = ProcessSampleRunner(
        proc.pid,
        run_dir / "process-samples.jsonl",
        started_at,
        args.process_sample_interval_secs,
    )
    process_samples.start()
    exit_code: int | None = None
    fatal_error = ""
    hot_churn: HotChurnRunner | None = None

    try:
        wait_for_http(base_url, args.startup_timeout_secs)
        json_line(
            run_dir / "events.jsonl",
            {"ts": utc_now(), "event": "http_ready", "pid": proc.pid},
        )

        next_endpoint_sample = time.monotonic()
        next_canary = time.monotonic() + args.canary_interval_secs
        deadline = None if args.duration_secs == 0 else time.monotonic() + args.duration_secs
        canary_root = Path(args.canary_root).expanduser().resolve() if args.canary_root else None
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
            )
            if args.event_storm
            else None
        )
        # Task 3: mixed-workload hot churn runs in a background thread,
        # creating real L0 pressure while cold rotation works.
        hot_churn = (
            HotChurnRunner(
                base_url=base_url,
                roots=hot_roots,
                out_path=run_dir / "hot-churn-samples.jsonl",
                started_at=started_at,
                interval_secs=args.mixed_workload_interval_secs,
            )
            if args.mixed_workload and hot_roots
            else None
        )
        if hot_churn is not None:
            hot_churn.start()

        # Task 1: settle phase progress. If the event storm has a start delay,
        # inform the user that we're waiting for tier demotion before events begin.
        if event_storm is not None and args.event_storm_start_delay_secs > 0:
            print(
                f"Settle phase: waiting {args.event_storm_start_delay_secs:.0f} seconds "
                f"for tier demotion before event storm starts...",
                flush=True,
            )

        while True:
            if proc.poll() is not None:
                exit_code = proc.returncode
                break
            now = time.monotonic()
            if deadline is not None and now >= deadline:
                break
            if now >= next_endpoint_sample:
                collect_endpoint_samples(base_url, run_dir / "endpoint-samples.jsonl", started_at)
                next_endpoint_sample = time.monotonic() + args.sample_interval_secs
            if canary_root and now >= next_canary:
                for record in run_canary_cycle(base_url, canary_root, args.canary_timeout_secs):
                    record["ts"] = utc_now()
                    record["elapsed_secs"] = round(time.monotonic() - started_at, 3)
                    json_line(run_dir / "canary-samples.jsonl", record)
                next_canary = time.monotonic() + args.canary_interval_secs
            if passive_canary:
                passive_canary.tick(now)
            if event_storm:
                event_storm.tick(now)
            time.sleep(0.2)
    except KeyboardInterrupt:
        json_line(run_dir / "events.jsonl", {"ts": utc_now(), "event": "interrupted"})
    except Exception as exc:  # noqa: BLE001 - keep partial evidence on startup/runtime failure
        fatal_error = repr(exc)
        json_line(
            run_dir / "events.jsonl",
            {"ts": utc_now(), "event": "fatal_error", "error": fatal_error},
        )
    finally:
        if hot_churn is not None:
            hot_churn.stop()
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=10)
        process_samples.stop()
        if process_samples.error:
            json_line(
                run_dir / "events.jsonl",
                {
                    "ts": utc_now(),
                    "event": "process_sampler_error",
                    "error": process_samples.error,
                },
            )
            if not fatal_error:
                fatal_error = f"process sampler failed: {process_samples.error}"
        exit_code = proc.returncode if exit_code is None else exit_code
        log_file.close()
        cleanup_uds_socket(uds_socket)

    summary = summarize(run_dir, args.run_label, exit_code)
    if fatal_error:
        summary["fatal_error"] = fatal_error
        (run_dir / "summary.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
    write_report(run_dir, summary)
    print(json.dumps({"run_dir": str(run_dir), "summary": summary}, ensure_ascii=False, indent=2))
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
            "snapshot-interval-secs",
            "tiered-profile", "watch-mode", "max-watch-dirs",
            "l0-max-cost-per-root", "l1-scan-interval-secs", "l2-scan-interval-secs",
            "l3-scan-interval-secs", "l1-empty-scans-to-l2", "l2-empty-scans-to-l3",
            "fast-scan", "no-fast-scan", "event-storm-ops", "event-storm-file-count",
            "event-storm-depth", "event-storm-interval-secs", "event-storm-settle-secs",
            "realistic-mode", "mixed-workload", "event-storm-immediate-query",
            "canary-in-fixture", "immediate-query-settle-secs",
            "mixed-workload-interval-secs",
            "snapshot-path-disk", "event-storm-start-delay-secs",
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
    return 0


def main() -> int:
    args = parse_args()
    if args.sweep_config:
        return run_sweep(args)
    summary = run_single(args)
    exit_code = summary.get("fd_rdd_exit_code")
    fatal = summary.get("fatal_error", "")
    return 0 if not fatal and exit_code in (0, -signal.SIGTERM) else 1


if __name__ == "__main__":
    raise SystemExit(main())
