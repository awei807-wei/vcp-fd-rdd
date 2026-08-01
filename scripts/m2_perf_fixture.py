"""M2 冷轮转分钟级性能 fixture：no-change 周期单价测量与正式跑标定。

复用 m2-cold-window-vm-bench 的配置、daemon 生命周期与 0.5s 进程采样，
在 2×300 子目录冷根上以缩短的轮转节奏测量每个递归周期的
读/写 syscall、CPU、次缺页与首桶尖峰，并对照 bd3b654 正式跑单价做标定。
机制参数（预算、成本上限、fast-scan、query lease 关闭）与正式腿一致，
仅缩短调度节奏；节奏不影响单周期扫描工作量，这正是标定要验证的假设。
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import signal
import sys
import threading
import time
from argparse import Namespace
from pathlib import Path
from typing import Any

import m2_perf_fixture_analysis as analysis
from m2_cold_window_ab_fixture import exclusive_fixture_lock, rebuild_fixture

_BENCH_PATH = Path(__file__).resolve().parent / "m2-cold-window-vm-bench.py"


def _load_bench() -> Any:
    existing = sys.modules.get("m2_cold_window_vm_bench")
    if existing is not None:
        return existing
    spec = importlib.util.spec_from_file_location(
        "m2_cold_window_vm_bench", _BENCH_PATH
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


BENCH = _load_bench()

# 与正式腿 _tiered_args 完全一致的机制参数；fixture 只允许覆盖调度节奏。
_FORMAL_MECHANISM_DEFAULTS: dict[str, Any] = {
    "snapshot_interval_secs": 300,
    "include_hidden": False,
    "watch_mode": "tiered",
    "runtime_profile": "default",
    "tiered_profile": "balanced",
    "rotating_cold_window": True,
    "rotating_budget": 128,
    "rotating_tick_secs": 30,
    "rotating_ttl_secs": 180,
    "rotating_max_cost_per_root": 64,
    "rotating_max_dirs_per_tick": 8,
    # 正式默认 1800s;fixture 运行时按 --rotating-full-sweep-period-secs 覆盖
    # （默认 0 = 每周期 sweep，保持单价测量语义）。
    "rotating_full_sweep_period_secs": 1800,
    "max_watch_dirs": 8,
    "l0_max_cost_per_root": 1,
    "l1_scan_interval_secs": 5,
    "l2_scan_interval_secs": 60,
    "l3_scan_interval_secs": 21600,
    "l1_empty_scans_to_l2": 1,
    "l2_empty_scans_to_l3": 2,
    "fast_scan": True,
    "query_fast_scan_leases": False,
    # 正式腿开启 proc_sampler，但它是全系统 /proc 扫描器：在极简 VM 上开销可忽略，
    # 在多进程宿主机上会制造 ~5k 读/秒的地板，完全淹没轮转窗口信号（实测确认）。
    # fixture 测的是 M2 扫描单价，与该观测组件无关，因此默认关闭。
    "proc_sampler": False,
}


def build_daemon_namespace(**overrides: Any) -> Namespace:
    values = dict(_FORMAL_MECHANISM_DEFAULTS)
    values.update(overrides)
    return Namespace(**values)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--binary", default="target/release/fd-rdd")
    parser.add_argument("--build", choices=["auto", "always", "never"], default="auto")
    parser.add_argument("--run-dir", default="")
    parser.add_argument("--port", type=int, default=6065)
    parser.add_argument("--cycles", type=int, default=5)
    parser.add_argument(
        "--allow-cycle-shortfall",
        action="store_true",
        help="integration-only: do not fail when syscall spike windows miss a real rotating cycle",
    )
    parser.add_argument("--rotating-ttl-secs", type=int, default=45)
    parser.add_argument("--rotating-tick-secs", type=int, default=15)
    parser.add_argument(
        "--rotating-full-sweep-period-secs",
        type=int,
        default=0,
        help="0 = 每周期 sweep（单价测量）；设为正式值可验证续租周期的静默性",
    )
    # L1/L2 扫描节奏必须保持正式值：FastScanLease 目录的覆盖维护重扫跟随该节奏，
    # 压缩它会把维护扫描放大几十倍，破坏 per-cycle 标定（实测确认）。
    parser.add_argument("--l1-scan-interval-secs", type=int, default=5)
    parser.add_argument("--l2-scan-interval-secs", type=int, default=60)
    parser.add_argument("--settle-secs", type=float, default=150.0)
    parser.add_argument("--process-sample-interval-secs", type=float, default=0.5)
    parser.add_argument("--metrics-interval-secs", type=float, default=2.0)
    parser.add_argument("--query-qps", type=float, default=4.0)
    parser.add_argument("--burst", dest="burst", action="store_true", default=True)
    parser.add_argument("--no-burst", dest="burst", action="store_false")
    parser.add_argument(
        "--burst-root-level",
        action="store_true",
        help="integration-only: create the burst probe directly under the cold root",
    )
    parser.add_argument(
        "--burst-timeout-secs",
        type=float,
        default=0.0,
        help="0 表示自动取 rotating ttl + tick + 10：FastScanLease 目录的深层新建要等下一轮递归",
    )
    parser.add_argument("--spike-threshold", type=float, default=1000.0)
    parser.add_argument(
        "--calibrate", dest="calibrate", action="store_true", default=True
    )
    parser.add_argument("--no-calibrate", dest="calibrate", action="store_false")
    parser.add_argument(
        "--deep-modify-probe",
        dest="deep_modify_probe",
        action="store_true",
        default=True,
        help="§18.1 探针：深层已有文件原地改写须在 sweep 周期内可见",
    )
    parser.add_argument(
        "--no-deep-modify-probe", dest="deep_modify_probe", action="store_false"
    )
    parser.add_argument(
        "--baseline-json",
        default="",
        help="用某次 fixture 报告的周期中位数替代内置 bd3b654 参考单价",
    )
    parser.add_argument("--http-timeout-secs", type=float, default=60.0)
    return parser.parse_args(argv)


def _poll_metrics(
    base_url: str, out_path: Path, interval: float, stop: threading.Event
) -> None:
    while not stop.is_set():
        record: dict[str, Any] = {"ts": time.time()}
        try:
            record["metrics"] = BENCH.http_json(base_url, "/metrics", timeout=2.0)
        except Exception as exc:  # noqa: BLE001 - 采样失败记录后继续
            record["error"] = repr(exc)
        try:
            record["watch_state"] = BENCH.http_json(
                base_url, "/watch-state", timeout=2.0
            )
        except Exception as exc:  # noqa: BLE001 - 采样失败记录后继续
            record["watch_state_error"] = repr(exc)
        BENCH.json_line(out_path, record)
        stop.wait(interval)


def _query_load(
    base_url: str, term: str, qps: float, stop: threading.Event
) -> None:
    interval = 1.0 / max(0.1, qps)
    while not stop.is_set():
        try:
            BENCH.search_results(base_url, term, limit=10)
        except Exception:  # noqa: BLE001 - 查询负载失败不终止测量
            pass
        stop.wait(interval)


def _run_burst_probe(
    base_url: str,
    cold_root: Path,
    timeout_secs: float,
    *,
    root_level: bool = False,
) -> dict[str, Any]:
    parent = cold_root if root_level else cold_root / "d000"
    probe = parent / "burst_probe_fixture.txt"
    probe.write_text("fd-rdd perf fixture burst probe\n", encoding="utf-8")
    started = time.monotonic()
    deadline = started + timeout_secs
    visible = False
    while time.monotonic() < deadline:
        try:
            results = BENCH.search_results(base_url, probe.name, limit=10)
        except Exception:  # noqa: BLE001 - 传输失败按不可见继续轮询
            results = []
        if BENCH.result_has_path(results, probe):
            visible = True
            break
        time.sleep(0.25)
    return {
        "enabled": True,
        "visible": visible,
        "latency_secs": round(time.monotonic() - started, 3),
        "path": str(probe),
        "root_level": root_level,
    }


def _search_entry(base_url: str, path: Path) -> dict[str, Any] | None:
    try:
        results = BENCH.search_results(base_url, path.name, limit=20)
    except Exception:  # noqa: BLE001 - 传输失败按未见处理
        return None
    for item in results:
        if str(item.get("path", "")) == str(path):
            return item
    return None


def _run_deep_modify_probe(
    base_url: str, cold_root: Path, timeout_secs: float
) -> dict[str, Any]:
    """§18.1 两相探针：改写已有深层文件内容（所有目录 mtime 不变）。

    相位一（flagged）：查询端冷结果验真在下一次查询就应把 freshness 标为
    changed——用户可见的陈旧标记即时生效，与 sweep 无关。
    相位二（repaired）：完整递归 sweep 检出 mtime 变化并 upsert，条目翻到
    overlay（tier 变化或 freshness 回 fresh）——这是索引修复的 SLA 通道。
    """
    target = cold_root / "d100" / "file_100.txt"
    baseline = _search_entry(base_url, target)
    if baseline is None:
        return {"enabled": True, "visible": False, "error": "baseline entry missing"}
    baseline_tier = baseline.get("index_tier")
    target.write_text("deep modify probe payload\n", encoding="utf-8")
    started = time.monotonic()
    deadline = started + timeout_secs
    flagged_secs: float | None = None
    last_tier = baseline_tier
    while time.monotonic() < deadline:
        entry = _search_entry(base_url, target)
        if entry is not None:
            freshness = str(entry.get("freshness", ""))
            last_tier = entry.get("index_tier")
            if flagged_secs is None and (
                freshness == "changed" or entry.get("validated") is False
            ):
                flagged_secs = round(time.monotonic() - started, 3)
            if last_tier != baseline_tier or freshness == "fresh":
                return {
                    "enabled": True,
                    "visible": True,
                    "flagged_secs": flagged_secs,
                    "repaired_secs": round(time.monotonic() - started, 3),
                    "baseline_tier": baseline_tier,
                    "updated_tier": last_tier,
                }
        time.sleep(0.5)
    return {
        "enabled": True,
        "visible": False,
        "flagged_secs": flagged_secs,
        "repaired_secs": None,
        "baseline_tier": baseline_tier,
        "updated_tier": last_tier,
    }


def _run_sweep_only_modify_probe(
    base_url: str, cold_root: Path, wait_secs: float
) -> dict[str, Any]:
    """§18.1 纯 sweep 通道：改写一个不被任何查询触碰的深层文件，静默等待
    一个轮转周期后单次查询——若 sweep 检出并修复，首查即 fresh/overlay；
    若首查报 changed，说明修复来自本次查询而非 sweep（§18.1 违约信号）。"""
    target = cold_root / "d150" / "file_150.txt"
    target.write_text("sweep-only deep modify payload\n", encoding="utf-8")
    time.sleep(wait_secs)
    entry = _search_entry(base_url, target)
    if entry is None:
        return {"enabled": True, "repaired_by_sweep": False, "error": "entry missing"}
    freshness = str(entry.get("freshness", ""))
    return {
        "enabled": True,
        "waited_secs": wait_secs,
        "first_query_freshness": freshness,
        "first_query_tier": entry.get("index_tier"),
        "repaired_by_sweep": freshness != "changed"
        and entry.get("index_tier") == "HotMemory",
    }


def _load_reference(args: argparse.Namespace) -> dict[str, float] | None:
    if not args.baseline_json:
        return None
    payload = json.loads(Path(args.baseline_json).read_text(encoding="utf-8"))
    metrics = payload.get("calibration", {}).get("metrics", {})
    return {
        name: float(row["measured_median"])
        for name, row in metrics.items()
        if "measured_median" in row
    }


def _watch_state_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    last = rows[-1] if rows else {}
    cycle_rows: list[tuple[int, int]] = []
    for row in rows:
        try:
            cycle_rows.append(
                (
                    int(row.get("rotating_cold_window_cycle_id", 0) or 0),
                    int(
                        row.get(
                            "rotating_cold_window_cycle_progress_pct", 0
                        )
                        or 0
                    ),
                )
            )
        except (TypeError, ValueError):
            continue
    cycle_ids = [cycle_id for cycle_id, _ in cycle_rows]
    completed_ids = sorted(
        {
            cycle_id
            for cycle_id, progress in cycle_rows
            if cycle_id > 0 and progress >= 100
        }
    )
    return {
        "l3_dirs_last": int(last.get("l3_dirs", 0) or 0),
        "rotating_active_dirs_max": int(
            last.get("rotating_cold_window_active_dirs_max", 0) or 0
        ),
        "rotating_scan_only_dirs_last": int(
            last.get("rotating_cold_window_scan_only_dirs_last", 0) or 0
        ),
        "rotating_budget_blocked_last": int(
            last.get("rotating_cold_window_budget_blocked_last", 0) or 0
        ),
        "rotating_cycle_id_min": min(cycle_ids) if cycle_ids else None,
        "rotating_cycle_id_max": max(cycle_ids) if cycle_ids else None,
        "rotating_cycle_progress_pct_max": max(
            (progress for _, progress in cycle_rows), default=None
        ),
        "rotating_completed_cycle_ids": completed_ids,
        "rotating_completed_cycles_observed": len(completed_ids),
    }


def run(args: argparse.Namespace) -> int:
    repo = Path(args.repo).resolve()
    run_dir = (
        Path(args.run_dir).resolve()
        if args.run_dir
        else Path.home() / "fd-rdd-perf-fixture" / BENCH.utc_stamp()
    )
    run_dir.mkdir(parents=True, exist_ok=True)
    with exclusive_fixture_lock():
        roots = rebuild_fixture()
        cold_roots = [roots["cold-a"], roots["cold-b"]]

        binary = Path(args.binary)
        if not binary.is_absolute():
            binary = repo / binary
        BENCH.build_if_needed(repo, binary, args.build, BENCH.git_head_sha(repo))
        if not binary.exists():
            raise SystemExit(f"binary not found: {binary}")

        daemon_ns = build_daemon_namespace(
            root=[str(path) for path in cold_roots],
            port=args.port,
            rotating_ttl_secs=args.rotating_ttl_secs,
            rotating_tick_secs=args.rotating_tick_secs,
            rotating_full_sweep_period_secs=args.rotating_full_sweep_period_secs,
            l1_scan_interval_secs=args.l1_scan_interval_secs,
            l2_scan_interval_secs=args.l2_scan_interval_secs,
        )
        config_home = run_dir / "config-home"
        runtime_dir = run_dir / "runtime"
        runtime_dir.mkdir(parents=True, exist_ok=True)
        BENCH.write_config(daemon_ns, config_home)
        env = os.environ.copy()
        env["XDG_CONFIG_HOME"] = str(config_home)
        env["XDG_RUNTIME_DIR"] = str(runtime_dir)
        env.setdefault("RUST_LOG", "info")
        base_url = f"http://127.0.0.1:{args.port}"
        uds_socket = BENCH.short_uds_socket_path(run_dir)
        BENCH.cleanup_uds_socket(uds_socket)
        cmd = [
            str(binary),
            "--http-port",
            str(args.port),
            "--snapshot-path",
            str(run_dir / "index.db"),
            "--uds-socket",
            str(uds_socket),
            "--watch-mode",
            daemon_ns.watch_mode,
            "--runtime-profile",
            daemon_ns.runtime_profile,
            "--snapshot-interval-secs",
            str(daemon_ns.snapshot_interval_secs),
        ]
        for root in cold_roots:
            cmd.extend(["--root", str(root)])

        proc, log_file, samples, _started_at = BENCH._start_daemon_process(
            cmd,
            port=args.port,
            run_dir=run_dir,
            env=env,
            process_sample_interval_secs=args.process_sample_interval_secs,
        )
        stop = threading.Event()
        metrics_thread = threading.Thread(
            target=_poll_metrics,
            args=(base_url, run_dir / "metrics-samples.jsonl",
                  args.metrics_interval_secs, stop),
            daemon=True,
        )
        query_thread = threading.Thread(
            target=_query_load,
            args=(base_url, "file_0", args.query_qps, stop),
            daemon=True,
        )
        burst: dict[str, Any] = {"enabled": False}
        deep_modify: dict[str, Any] = {"enabled": False}
        sweep_only: dict[str, Any] = {"enabled": False}
        try:
            BENCH.wait_for_http(
                base_url,
                args.http_timeout_secs,
                process=proc,
                daemon_log_path=run_dir / "fd-rdd.log",
                expected_port=args.port,
            )
            metrics_thread.start()
            # settle 期保持零查询，让冷根走完 L1→L2→L3 降级并被轮转选中；
            # 查询负载与正式跑一致，只在测量窗内运行。
            print(f"settling {args.settle_secs:.0f}s (no query load)...", flush=True)
            time.sleep(args.settle_secs)
            query_thread.start()
            measure_secs = args.cycles * args.rotating_ttl_secs + 15.0
            print(f"measuring {args.cycles} cycles (~{measure_secs:.0f}s)...", flush=True)
            time.sleep(measure_secs)
            if args.burst:
                burst_timeout = args.burst_timeout_secs or (
                    args.rotating_ttl_secs + args.rotating_tick_secs + 10.0
                )
                burst = _run_burst_probe(
                    base_url,
                    roots["cold-a"],
                    burst_timeout,
                    root_level=args.burst_root_level,
                )
                time.sleep(5.0)
            if args.deep_modify_probe:
                if args.rotating_full_sweep_period_secs > args.rotating_ttl_secs * 2:
                    deep_modify = {
                        "enabled": False,
                        "skipped": "sweep period exceeds probe budget",
                    }
                    sweep_only = dict(deep_modify)
                else:
                    deep_modify = _run_deep_modify_probe(
                        base_url,
                        roots["cold-a"],
                        args.rotating_ttl_secs + args.rotating_tick_secs + 15.0,
                    )
                    sweep_only = _run_sweep_only_modify_probe(
                        base_url,
                        roots["cold-a"],
                        args.rotating_ttl_secs + args.rotating_tick_secs + 10.0,
                    )
                    time.sleep(2.0)
        finally:
            stop.set()
            samples.expect_process_exit()
            try:
                proc.send_signal(signal.SIGTERM)
                proc.wait(timeout=30)
            except Exception:  # noqa: BLE001 - 退出失败强制回收
                proc.kill()
                proc.wait(timeout=10)
            samples.stop()
            log_file.close()

    process_samples = BENCH.read_jsonl(run_dir / "process-samples.jsonl")
    deltas = analysis.sample_deltas(process_samples)
    windows = analysis.detect_scan_windows(
        deltas, spike_threshold=args.spike_threshold
    )
    startup_windows, cycle_windows = analysis.partition_windows(
        deltas, windows, min_cycle_start_secs=args.settle_secs
    )
    clk_tck = int(os.sysconf("SC_CLK_TCK"))
    cycles = analysis.summarize_cycles(deltas, cycle_windows, clk_tck=clk_tck)
    sample_rows = BENCH.read_jsonl(run_dir / "metrics-samples.jsonl")
    metrics_rows = [
        row["metrics"] for row in sample_rows if isinstance(row.get("metrics"), dict)
    ]
    watch_rows = [
        row["watch_state"]
        for row in sample_rows
        if isinstance(row.get("watch_state"), dict)
    ]
    guard_keys = (
        "query_guard_hold_count",
        "query_guard_hold_total_ns",
        "query_guard_hold_p50_us",
        "query_guard_hold_p95_us",
        "query_guard_hold_p99_us",
    )
    last_metrics = metrics_rows[-1] if metrics_rows else {}
    watch_summary = _watch_state_summary(watch_rows)
    payload: dict[str, Any] = {
        "run_label": run_dir.name,
        "cycles_requested": args.cycles,
        "integration_options": {
            "allow_cycle_shortfall": bool(args.allow_cycle_shortfall),
            "burst_root_level": bool(args.burst_root_level),
        },
        "config": vars(daemon_ns),
        "startup_windows": [list(window) for window in startup_windows],
        "cycles": cycles,
        "watch_state": watch_summary,
        "query_guard": {key: int(last_metrics.get(key, 0) or 0) for key in guard_keys},
        "burst": burst,
        "deep_modify": deep_modify,
        "sweep_only_modify": sweep_only,
        "calibration": (
            analysis.evaluate_calibration(cycles, reference=_load_reference(args))
            if args.calibrate
            else None
        ),
    }
    BENCH.atomic_write_json(run_dir / "fixture-report.json", payload)
    (run_dir / "FIXTURE-REPORT.md").write_text(
        analysis.render_report(payload), encoding="utf-8"
    )
    print(f"fixture report: {run_dir / 'FIXTURE-REPORT.md'}", flush=True)
    if len(cycles) < args.cycles:
        print(
            f"cycle shortfall: measured {len(cycles)}/{args.cycles}", flush=True
        )
        if not args.allow_cycle_shortfall:
            return 1
    if args.calibrate and not payload["calibration"]["pass"]:
        print("calibration FAILED", flush=True)
        return 1
    return 0


def main() -> int:
    return run(parse_args())


if __name__ == "__main__":
    sys.exit(main())
