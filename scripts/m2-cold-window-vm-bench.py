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
            "path": str(renamed),
            "ok": ok_new,
            "latency_secs": round(latency_new, 3),
            "polls": polls_new,
        }
    )
    records.append(
        {
            "operation": "rename_old_hidden",
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
            "path": str(renamed),
            "ok": ok,
            "latency_secs": round(latency, 3),
            "polls": polls,
        }
    )
    return records


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
    process_samples = []
    endpoint_samples = []
    canary_samples = []
    for path, target in [
        (run_dir / "process-samples.jsonl", process_samples),
        (run_dir / "endpoint-samples.jsonl", endpoint_samples),
        (run_dir / "canary-samples.jsonl", canary_samples),
    ]:
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                target.append(json.loads(line))

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

    canary_by_op: dict[str, dict[str, Any]] = {}
    for op in sorted({str(item.get("operation", "")) for item in canary_samples}):
        if not op:
            continue
        rows = [item for item in canary_samples if item.get("operation") == op]
        latencies = [float(item.get("latency_secs", 0.0)) for item in rows if item.get("ok")]
        canary_by_op[op] = {
            "count": len(rows),
            "ok": sum(1 for item in rows if item.get("ok")),
            "timeouts": sum(1 for item in rows if not item.get("ok")),
            "p50_secs": round(percentile(latencies, 50), 3),
            "p95_secs": round(percentile(latencies, 95), 3),
            "p99_secs": round(percentile(latencies, 99), 3),
            "max_secs": round(max(latencies) if latencies else 0.0, 3),
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
            "rotating_cold_window_budget_blocked_last": int(
                nums(watch_samples[-1:], "rotating_cold_window_budget_blocked")[0]
                if watch_samples
                else 0
            ),
            "rotating_cold_window_active_dirs_max": int(
                max(nums(watch_samples, "rotating_cold_window_active_dirs") or [0])
            ),
            "ephemeral_watch_budget_blocked_last": int(
                nums(watch_samples[-1:], "ephemeral_watch_budget_blocked")[0]
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
| cold freshness age p99 max s | {summary["watch_state"]["cold_freshness_age_p99_secs_max"]} |
| rotating budget blocked last | {summary["watch_state"]["rotating_cold_window_budget_blocked_last"]} |
| rotating active dirs max | {summary["watch_state"]["rotating_cold_window_active_dirs_max"]} |
| index health last | {summary["health"]["index_health_last"]} |

## Canary

Use canary numbers only when `--canary-root` was set.

```json
{json.dumps(summary["canary"], ensure_ascii=False, indent=2)}
```

## Files

- `config-home/fd-rdd/config.toml`: isolated fd-rdd config for this run.
- `fd-rdd.log`: daemon stdout/stderr.
- `endpoint-samples.jsonl`: periodic `/health`, `/status`, `/metrics`, `/memory`, `/watch-state`.
- `process-samples.jsonl`: `/proc/<pid>` CPU/RSS/FD/thread samples.
- `canary-samples.jsonl`: optional create/rename/delete search visibility latency.
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
    parser.add_argument("--fast-scan", dest="fast_scan", action="store_true", default=True)
    parser.add_argument("--no-fast-scan", dest="fast_scan", action="store_false")
    parser.add_argument("--proc-sampler", dest="proc_sampler", action="store_true", default=True)
    parser.add_argument("--no-proc-sampler", dest="proc_sampler", action="store_false")
    parser.add_argument("--canary-root", default="")
    parser.add_argument("--canary-interval-secs", type=float, default=60.0)
    parser.add_argument("--canary-timeout-secs", type=float, default=30.0)
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
