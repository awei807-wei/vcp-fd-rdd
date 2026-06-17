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
import json
import os
import signal
import socket
import subprocess
import sys
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
        self.kinds = normalize_event_storm_kinds(kinds)
        self.target_tiers = [tier.upper() for tier in target_tiers]
        tiers = self.target_tiers or [""]
        workloads = self.kinds or ["rw100"]
        self.work_items = [(tier, kind) for tier in tiers for kind in workloads]
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
        requested_tier, selected_kind = self.work_items[(self.cycle - 1) % len(self.work_items)]
        root = self.select_root(requested_tier)
        root.mkdir(parents=True, exist_ok=True)
        tier_before = self.tier_for_root(root)
        events: list[dict[str, Any]] = []
        cycle_started = time.monotonic()
        self.current_burst_started_at = cycle_started
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
        generation_secs = time.monotonic() - cycle_started
        due_at = time.monotonic() + self.settle_secs
        self.active = {
            "cycle": self.cycle,
            "root": root,
            "requested_tier": requested_tier,
            "selected_kind": selected_kind,
            "tier_before": tier_before,
            "events": events,
            "started_at": cycle_started,
            "due_at": due_at,
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
        for i in range(self.ops_per_burst):
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
        for i in range(self.ops_per_burst):
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
        packages = max(1, self.ops_per_burst // 10)
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
        width = max(1, min(self.ops_per_burst, 200))
        for i in range(width):
            parent = source / f"level1_{i % 10:02d}" / f"level2_{i % 25:02d}"
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
        width = max(1, min(self.ops_per_burst, 200))
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
        width = max(1, min(self.ops_per_burst, 200))
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

    def write_time_skew_fixture(self, root: Path, tier_before: str) -> list[dict[str, Any]]:
        burst_root = self.burst_root(root, "time-skew")
        burst_root.mkdir(parents=True, exist_ok=True)
        records: list[dict[str, Any]] = []
        deadline = time.monotonic() + self.duration_budget_secs
        width = max(1, min(self.ops_per_burst, 200))
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
        return root / f"fd-rdd-m2-event-storm-{safe_name(kind)}-{self.cycle:03d}"

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
        events = list(self.active["events"])
        root = Path(self.active["root"])
        tier_after = self.tier_for_root(root)
        ok_count = 0
        positive_total = 0
        positive_ok = 0
        latencies: list[float] = []
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
                    "first_query_exists": exists,
                    "latency_secs": round(latency, 3),
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
            if not ok and self.timeout_secs > 0:
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
                "requested_tier": self.active.get("requested_tier", ""),
                "tier_before": self.active.get("tier_before", ""),
                "tier_after": tier_after,
            }
        )
        self.active = None
        self.next_start_at = time.monotonic() + self.interval_secs

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
        record = {
            "ts": utc_now(),
            "elapsed_secs": round(time.monotonic() - started_at, 3),
            "endpoint": endpoint,
        }
        try:
            record["ok"] = True
            record["data"] = http_json(base_url, endpoint, timeout=4.0)
        except Exception as exc:  # noqa: BLE001 - written as benchmark evidence
            record["ok"] = False
            record["error"] = repr(exc)
        json_line(out, record)


def summarize(run_dir: Path, label: str, exit_code: int | None) -> dict[str, Any]:
    process_samples = read_jsonl(run_dir / "process-samples.jsonl")
    endpoint_samples = read_jsonl(run_dir / "endpoint-samples.jsonl")
    canary_samples = read_jsonl(run_dir / "canary-samples.jsonl")
    event_storm_samples = read_jsonl(run_dir / "event-storm-samples.jsonl")

    cpu = [float(item.get("cpu_pct", 0.0)) for item in process_samples]
    rss = [int(item.get("vmrss_bytes", 0)) for item in process_samples]
    fds = [int(item.get("fd_count", 0)) for item in process_samples]

    watch_samples = [
        item["data"]
        for item in endpoint_samples
        if item.get("ok") and item.get("endpoint") == "/watch-state" and isinstance(item.get("data"), dict)
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
            latencies = [float(item.get("latency_secs", 0.0)) for item in op_rows if item.get("ok")]
            passive_waits = [
                float(item.get("passive_wait_secs", 0.0))
                for item in op_rows
                if "passive_wait_secs" in item
            ]
            summary = {
                "count": len(op_rows),
                "ok": sum(1 for item in op_rows if item.get("ok")),
                "timeouts": sum(1 for item in op_rows if not item.get("ok")),
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

    def summarize_event_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
        total = len(rows)
        ok = sum(1 for item in rows if item.get("ok"))
        positive = [item for item in rows if item.get("should_exist")]
        positive_ok = sum(1 for item in positive if item.get("ok"))
        latencies = [float(item.get("latency_secs", 0.0)) for item in rows if item.get("ok")]
        settles = [float(item.get("settle_secs", 0.0)) for item in rows if "settle_secs" in item]
        ages = [float(item.get("event_age_secs", 0.0)) for item in rows if "event_age_secs" in item]
        return {
            "total": total,
            "ok": ok,
            "missed": total - ok,
            "success_rate": round(ok / total, 4) if total else 0.0,
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
            if item.get("operation") == operation and item.get("ok")
        )

    inode_reuse_new_rows = [
        item
        for item in event_first_queries
        if item.get("operation") == "inode_reuse_new_visible_first_query"
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
            "events_total": sum(int(item.get("events_total", 0) or 0) for item in event_bursts),
            "ok": sum(int(item.get("ok", 0) or 0) for item in event_bursts),
            "missed": sum(int(item.get("missed", 0) or 0) for item in event_bursts),
            "success_rate": (
                round(
                    sum(int(item.get("ok", 0) or 0) for item in event_bursts)
                    / sum(int(item.get("events_total", 0) or 0) for item in event_bursts),
                    4,
                )
                if sum(int(item.get("events_total", 0) or 0) for item in event_bursts)
                else 0.0
            ),
            "positive_total": sum(int(item.get("positive_total", 0) or 0) for item in event_bursts),
            "positive_ok": sum(int(item.get("positive_ok", 0) or 0) for item in event_bursts),
            "positive_success_rate": (
                round(
                    sum(int(item.get("positive_ok", 0) or 0) for item in event_bursts)
                    / sum(int(item.get("positive_total", 0) or 0) for item in event_bursts),
                    4,
                )
                if sum(int(item.get("positive_total", 0) or 0) for item in event_bursts)
                else 0.0
            ),
            "burst_duration_p50_secs": round(percentile(burst_durations, 50), 3),
            "burst_duration_p95_secs": round(percentile(burst_durations, 95), 3),
            "burst_duration_max_secs": round(max(burst_durations) if burst_durations else 0.0, 3),
            "first_query": summarize_event_rows(event_first_queries),
            "after_query": summarize_event_rows(event_after_queries),
            "by_workload": event_by_workload,
            "by_tier_before": event_by_tier,
            "special": event_special,
        },
        "built_in_metrics_dir": str(run_dir / "reports" / "metrics"),
    }
    (run_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return summary


def write_report(run_dir: Path, summary: dict[str, Any]) -> None:
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
| process RSS max | {summary["process"]["rss_bytes_max"]} |
| fd count max | {summary["process"]["fd_count_max"]} |
| dirty queue max | {summary["watch_state"]["dirty_queue_len_max"]} |
| fast scan lag p99 max ms | {summary["watch_state"]["fast_scan_coverage_lag_p99_ms_max"]} |
| cold freshness age p95 first s | {summary["watch_state"]["cold_freshness_age_p95_secs_first"]} |
| cold freshness age p95 last s | {summary["watch_state"]["cold_freshness_age_p95_secs_last"]} |
| cold freshness age p95 delta s | {summary["watch_state"]["cold_freshness_age_p95_secs_delta"]} |
| cold freshness age p99 max s | {summary["watch_state"]["cold_freshness_age_p99_secs_max"]} |
| rotating budget blocked last | {summary["watch_state"]["rotating_cold_window_budget_blocked_last"]} |
| rotating active dirs max | {summary["watch_state"]["rotating_cold_window_active_dirs_max"]} |
| rotating cycle progress max % | {summary["watch_state"]["rotating_cold_window_cycle_progress_pct_max"]} |
| rotating scan-only dirs last | {summary["watch_state"]["rotating_cold_window_scan_only_dirs_last"]} |
| proc sampler triggered watches last | {summary["watch_state"]["proc_sampler_triggered_watches_last"]} |
| passive first query success rate | {summary["passive_first_query"]["success_rate"]} |
| passive positive first query success rate | {summary["passive_positive_first_query"]["success_rate"]} |
| event storm success rate | {summary["event_storm"]["success_rate"]} |
| event storm positive success rate | {summary["event_storm"]["positive_success_rate"]} |
| event storm first-query p95 s | {summary["event_storm"]["first_query"]["first_query_p95_secs"]} |
| event storm first-query age p95 s | {summary["event_storm"]["first_query"]["event_age_p95_secs"]} |
| event storm after-query p95 s | {summary["event_storm"]["after_query"]["first_query_p95_secs"]} |
| event storm burst duration max s | {summary["event_storm"]["burst_duration_max_secs"]} |
| index health last | {summary["health"]["index_health_last"]} |

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

## Files

- `config-home/fd-rdd/config.toml`: isolated fd-rdd config for this run.
- `fd-rdd.log`: daemon stdout/stderr.
- `endpoint-samples.jsonl`: periodic `/health`, `/status`, `/metrics`, `/memory`, `/watch-state`.
- `process-samples.jsonl`: `/proc/<pid>` CPU/RSS/FD/thread samples.
- `canary-samples.jsonl`: optional active and passive create/rename/delete evidence.
- `event-storm-samples.jsonl`: optional synthetic event burst writes and first-query evidence.
- `reports/metrics/*.json`: fd-rdd built-in JSONL metrics, reusable for jq/offline analysis.
"""
    (run_dir / "REPORT.md").write_text(report, encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="fd-rdd M2 cold-window VM benchmark runner")
    parser.add_argument("--root", action="append", required=True, help="indexed root; repeatable")
    parser.add_argument("--repo", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--binary", default="target/release/fd-rdd")
    parser.add_argument("--build", choices=["auto", "always", "never"], default="auto")
    parser.add_argument("--run-label", default="rotating")
    parser.add_argument("--run-dir", default="")
    parser.add_argument("--port", type=int, default=6060)
    parser.add_argument("--duration-secs", type=int, default=3600, help="0 means until Ctrl-C")
    parser.add_argument("--sample-interval-secs", type=float, default=10.0)
    parser.add_argument("--snapshot-interval-secs", type=int, default=300)
    parser.add_argument("--watch-mode", choices=["tiered", "recursive", "off"], default="tiered")
    parser.add_argument("--runtime-profile", choices=["default", "memory_light", "memory-light"], default="default")
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
    parser.add_argument("--l1-scan-interval-secs", type=int, default=30)
    parser.add_argument("--l2-scan-interval-secs", type=int, default=300)
    parser.add_argument("--l3-scan-interval-secs", type=int, default=21600)
    parser.add_argument("--l1-empty-scans-to-l2", type=int, default=5)
    parser.add_argument("--l2-empty-scans-to-l3", type=int, default=3)
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
    parser.add_argument("--event-storm-start-delay-secs", type=float, default=120.0)
    parser.add_argument("--event-storm-interval-secs", type=float, default=300.0)
    parser.add_argument("--event-storm-settle-secs", type=float, default=120.0)
    parser.add_argument("--event-storm-timeout-secs", type=float, default=0.0)
    parser.add_argument("--event-storm-ops", type=int, default=100)
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
    parser.add_argument("--startup-timeout-secs", type=float, default=60.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo = Path(args.repo).resolve()
    binary = Path(args.binary)
    if not binary.is_absolute():
        binary = repo / binary
    build_if_needed(repo, binary, args.build)
    if not binary.exists():
        raise SystemExit(f"binary not found: {binary}")
    if not port_is_free(args.port):
        raise SystemExit(f"127.0.0.1:{args.port} is already in use")

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
    snapshot_path = run_dir / "index.db"
    uds_socket = runtime_dir / "fd-rdd.sock"
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
    }
    (run_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    log_file = (run_dir / "fd-rdd.log").open("wb")
    proc = subprocess.Popen(cmd, cwd=run_dir, env=env, stdout=log_file, stderr=subprocess.STDOUT)
    sampler = process_sampler(proc.pid)
    started_at = time.monotonic()
    exit_code: int | None = None
    fatal_error = ""

    try:
        wait_for_http(base_url, args.startup_timeout_secs)
        json_line(
            run_dir / "events.jsonl",
            {"ts": utc_now(), "event": "http_ready", "pid": proc.pid},
        )

        next_sample = time.monotonic()
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
            )
            if args.event_storm
            else None
        )

        while True:
            if proc.poll() is not None:
                exit_code = proc.returncode
                break
            now = time.monotonic()
            if deadline is not None and now >= deadline:
                break
            if now >= next_sample:
                collect_endpoint_samples(base_url, run_dir / "endpoint-samples.jsonl", started_at)
                proc_record = {
                    "ts": utc_now(),
                    "elapsed_secs": round(time.monotonic() - started_at, 3),
                    **next(sampler),
                }
                json_line(run_dir / "process-samples.jsonl", proc_record)
                next_sample = now + args.sample_interval_secs
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
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=10)
        exit_code = proc.returncode if exit_code is None else exit_code
        log_file.close()

    summary = summarize(run_dir, args.run_label, exit_code)
    if fatal_error:
        summary["fatal_error"] = fatal_error
        (run_dir / "summary.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
    write_report(run_dir, summary)
    print(json.dumps({"run_dir": str(run_dir), "summary": summary}, ensure_ascii=False, indent=2))
    return 0 if not fatal_error and exit_code in (0, -signal.SIGTERM) else 1


if __name__ == "__main__":
    raise SystemExit(main())
