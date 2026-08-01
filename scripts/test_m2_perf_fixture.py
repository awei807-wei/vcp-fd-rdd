"""m2_perf_fixture 纯分析函数与配置组装的单元测试。"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import m2_perf_fixture as fixture
import m2_perf_fixture_analysis as analysis


def _sample(elapsed: float, reads: int, writes: int, faults: int, ticks: int) -> dict:
    return {
        "elapsed_secs": elapsed,
        "read_syscalls": reads,
        "write_syscalls": writes,
        "minor_faults": faults,
        "cpu_ticks": ticks,
    }


def _cumulative_samples(read_plan: list[int]) -> list[dict]:
    rows = [_sample(0.0, 0, 0, 0, 0)]
    reads = writes = faults = ticks = 0
    elapsed = 0.0
    for delta in read_plan:
        elapsed += 0.5
        reads += delta
        writes += delta // 10
        faults += delta // 2
        ticks += delta // 100
        rows.append(_sample(elapsed, reads, writes, faults, ticks))
    return rows


class WindowDetectionTests(unittest.TestCase):
    def test_detects_two_windows_with_tail_gap_tolerance(self) -> None:
        plan = (
            [5] * 20
            + [6000, 800, 30, 200, 60]
            + [5] * 20
            + [5500, 400, 90]
            + [5] * 10
        )
        deltas = analysis.sample_deltas(_cumulative_samples(plan))
        windows = analysis.detect_scan_windows(deltas)

        self.assertEqual(len(windows), 2)
        first, second = windows
        # 第一窗从 6000 尖峰起，30 的静默样本被尾部间隙容忍，60 仍属活动尾。
        self.assertEqual(first, (20, 24))
        self.assertEqual(second, (45, 47))

    def test_detects_window_from_fault_spike_when_reads_are_quiet(self) -> None:
        # mountinfo 缓存后读尖峰消失，窗口必须仍能从缺页信号识别。
        rows = [_sample(0.0, 0, 0, 0, 0)]
        reads = faults = 0
        elapsed = 0.0
        fault_plan = [2] * 10 + [1800, 500, 450, 120] + [2] * 10
        for delta_faults in fault_plan:
            elapsed += 0.5
            reads += 9
            faults += delta_faults
            rows.append(_sample(elapsed, reads, 0, faults, 0))

        deltas = analysis.sample_deltas(rows)
        windows = analysis.detect_scan_windows(deltas)

        self.assertEqual(windows, [(10, 13)])

    def test_partition_separates_startup_window(self) -> None:
        plan = [5] * 4 + [7000, 300] + [5] * 100 + [6000, 200] + [5] * 10
        deltas = analysis.sample_deltas(_cumulative_samples(plan))
        windows = analysis.detect_scan_windows(deltas)
        startup, cycles = analysis.partition_windows(
            deltas, windows, min_cycle_start_secs=30.0
        )

        self.assertEqual(len(startup), 1)
        self.assertEqual(len(cycles), 1)
        self.assertLess(deltas[startup[0][0]]["elapsed_secs"], 30.0)
        self.assertGreaterEqual(deltas[cycles[0][0]]["elapsed_secs"], 30.0)

    def test_summarize_cycles_reports_totals_and_first_bucket(self) -> None:
        plan = [5] * 20 + [6000, 800, 30, 200, 60] + [5] * 10
        deltas = analysis.sample_deltas(_cumulative_samples(plan))
        windows = analysis.detect_scan_windows(deltas)
        cycles = analysis.summarize_cycles(deltas, windows, clk_tck=100)

        self.assertEqual(len(cycles), 1)
        row = cycles[0]
        self.assertEqual(row["read_syscalls"], 6000 + 800 + 30 + 200 + 60)
        self.assertEqual(row["first_bucket_read_syscalls"], 6000)
        self.assertEqual(
            row["minor_faults"],
            sum(value // 2 for value in (6000, 800, 30, 200, 60)),
        )
        self.assertAlmostEqual(
            row["cpu_core_seconds"],
            sum(value // 100 for value in (6000, 800, 30, 200, 60)) / 100,
            places=6,
        )
        self.assertAlmostEqual(row["duration_secs"], 2.0, places=3)


class CalibrationTests(unittest.TestCase):
    def _cycle(self, reads: float) -> dict:
        return {
            "read_syscalls": reads,
            "minor_faults": 4700.0,
            "cpu_core_seconds": 2.0,
        }

    def test_calibration_passes_within_tolerance_using_median(self) -> None:
        cycles = [self._cycle(11000.0), self._cycle(12000.0), self._cycle(40000.0)]
        result = analysis.evaluate_calibration(cycles)

        self.assertTrue(result["pass"])
        self.assertEqual(
            result["metrics"]["read_syscalls_per_cycle"]["measured_median"], 12000.0
        )

    def test_calibration_fails_beyond_tolerance(self) -> None:
        cycles = [self._cycle(20000.0), self._cycle(20500.0)]
        result = analysis.evaluate_calibration(cycles)

        self.assertFalse(result["pass"])
        self.assertFalse(result["metrics"]["read_syscalls_per_cycle"]["pass"])
        self.assertGreater(
            result["metrics"]["read_syscalls_per_cycle"]["deviation_ratio"], 0.30
        )

    def test_calibration_with_no_cycles_fails_explicitly(self) -> None:
        result = analysis.evaluate_calibration([])
        self.assertFalse(result["pass"])
        self.assertEqual(result["cycle_count"], 0)

    def test_cpu_deviation_is_informational_against_builtin_vm_reference(self) -> None:
        # CPU 随宿主硬件缩放：对内置 VM 参考只报告不把门。
        cycles = [
            {"read_syscalls": 11500.0, "minor_faults": 4700.0, "cpu_core_seconds": 0.9}
        ]
        result = analysis.evaluate_calibration(cycles)

        self.assertTrue(result["pass"])
        cpu = result["metrics"]["cpu_core_seconds_per_cycle"]
        self.assertFalse(cpu["gated"])
        self.assertFalse(cpu["pass"])

    def test_cpu_deviation_gates_against_same_host_baseline(self) -> None:
        cycles = [
            {"read_syscalls": 11500.0, "minor_faults": 4700.0, "cpu_core_seconds": 0.9}
        ]
        baseline = {
            "read_syscalls_per_cycle": 11500.0,
            "minor_faults_per_cycle": 4700.0,
            "cpu_core_seconds_per_cycle": 2.0,
        }
        result = analysis.evaluate_calibration(cycles, reference=baseline)

        self.assertFalse(result["pass"])
        self.assertTrue(result["metrics"]["cpu_core_seconds_per_cycle"]["gated"])


class ReportRenderTests(unittest.TestCase):
    def test_report_declares_cycle_shortfall_and_calibration(self) -> None:
        payload = {
            "run_label": "unit",
            "cycles_requested": 5,
            "config": {"rotating_ttl_secs": 45, "rotating_tick_secs": 15},
            "startup_windows": [],
            "cycles": [
                {
                    "start_elapsed_secs": 61.0,
                    "end_elapsed_secs": 63.0,
                    "duration_secs": 2.0,
                    "read_syscalls": 12000,
                    "write_syscalls": 900,
                    "minor_faults": 4600,
                    "cpu_core_seconds": 2.0,
                    "first_bucket_read_syscalls": 5600,
                }
            ],
            "calibration": analysis.evaluate_calibration(
                [
                    {
                        "read_syscalls": 12000.0,
                        "minor_faults": 4600.0,
                        "cpu_core_seconds": 2.0,
                    }
                ]
            ),
            "query_guard": {
                "query_guard_hold_p50_us": 100,
                "query_guard_hold_p95_us": 300,
                "query_guard_hold_p99_us": 500,
                "query_guard_hold_total_ns": 1_000_000,
                "query_guard_hold_count": 10,
            },
            "burst": {"enabled": True, "visible": True, "latency_secs": 1.2},
            "deep_modify": {
                "enabled": True,
                "visible": True,
                "flagged_secs": 0.5,
                "repaired_secs": 40.1,
                "baseline_tier": "ColdMmap",
                "updated_tier": "HotMemory",
            },
        }

        rendered = analysis.render_report(payload)

        self.assertIn("1/5", rendered)
        self.assertIn("calibration", rendered)
        self.assertIn("first bucket", rendered)
        self.assertIn("p50/p95/p99", rendered)
        self.assertIn("burst", rendered)
        self.assertIn("deep modify", rendered)
        self.assertIn("verify channel flagged", rendered)
        self.assertIn("sweep channel repaired", rendered)
        self.assertIn("ColdMmap", rendered)


class DaemonNamespaceTests(unittest.TestCase):
    def test_namespace_mirrors_formal_mechanism_parameters(self) -> None:
        ns = fixture.build_daemon_namespace(root=["/tmp/cold-a", "/tmp/cold-b"], port=6065)

        self.assertEqual(ns.rotating_budget, 128)
        self.assertEqual(ns.rotating_max_cost_per_root, 64)
        self.assertEqual(ns.rotating_max_dirs_per_tick, 8)
        self.assertEqual(ns.rotating_full_sweep_period_secs, 1800)
        self.assertEqual(ns.max_watch_dirs, 8)
        self.assertEqual(ns.l0_max_cost_per_root, 1)
        self.assertTrue(ns.fast_scan)
        self.assertFalse(ns.query_fast_scan_leases)
        # proc_sampler 在宿主机上是 ~5k 读/秒的全系统 /proc 扫描地板，fixture 必须关闭。
        self.assertFalse(ns.proc_sampler)

    def test_namespace_feeds_bench_write_config(self) -> None:
        ns = fixture.build_daemon_namespace(
            root=["/tmp/cold-a"],
            port=6065,
            rotating_ttl_secs=45,
            rotating_tick_secs=15,
            rotating_full_sweep_period_secs=0,
        )
        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = fixture.BENCH.write_config(ns, Path(tmp))
            content = cfg_path.read_text(encoding="utf-8")

        self.assertIn("rotating_cold_window_enabled = true", content)
        self.assertIn("rotating_cold_window_ttl_secs = 45", content)
        self.assertIn("rotating_full_sweep_period_secs = 0", content)
        self.assertIn("l1_l2_fast_scan_query_leases_enabled = false", content)
        self.assertIn("max_watch_dirs = 8", content)


class IntegrationProbeTests(unittest.TestCase):
    def test_integration_flags_are_opt_in(self) -> None:
        defaults = fixture.parse_args([])
        enabled = fixture.parse_args(
            [
                "--allow-cycle-shortfall",
                "--burst-root-level",
                "--sweep-completion-fence",
                "--integration-repair-deadline-secs",
                "75",
            ]
        )

        self.assertFalse(defaults.allow_cycle_shortfall)
        self.assertFalse(defaults.burst_root_level)
        self.assertFalse(defaults.sweep_completion_fence)
        self.assertEqual(defaults.integration_repair_deadline_secs, 0.0)
        self.assertTrue(enabled.allow_cycle_shortfall)
        self.assertTrue(enabled.burst_root_level)
        self.assertTrue(enabled.sweep_completion_fence)
        self.assertEqual(enabled.integration_repair_deadline_secs, 75.0)

    def test_watch_state_summary_uses_real_completed_cycle_progress(self) -> None:
        rows = [
            {
                "rotating_cold_window_cycle_id": 0,
                "rotating_cold_window_cycle_progress_pct": 100,
            },
            {
                "rotating_cold_window_cycle_id": 1,
                "rotating_cold_window_cycle_progress_pct": 50,
            },
            {
                "rotating_cold_window_cycle_id": 1,
                "rotating_cold_window_cycle_progress_pct": 100,
            },
            {
                "rotating_cold_window_cycle_id": 2,
                "rotating_cold_window_cycle_progress_pct": 100,
            },
        ]

        summary = fixture._watch_state_summary(rows)

        self.assertEqual(summary["rotating_cycle_id_min"], 0)
        self.assertEqual(summary["rotating_cycle_id_max"], 2)
        self.assertEqual(summary["rotating_cycle_progress_pct_max"], 100)
        self.assertEqual(summary["rotating_completed_cycle_ids"], [1, 2])
        self.assertEqual(summary["rotating_completed_cycles_observed"], 2)

    def test_root_level_burst_does_not_change_default_probe_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cold_root = Path(tmp)
            (cold_root / "d000").mkdir()
            with mock.patch.object(
                fixture.BENCH, "search_results", return_value=[]
            ), mock.patch.object(
                fixture.BENCH, "result_has_path", return_value=True
            ):
                default_probe = fixture._run_burst_probe(
                    "http://fixture", cold_root, 0.01
                )
                root_probe = fixture._run_burst_probe(
                    "http://fixture", cold_root, 0.01, root_level=True
                )

        self.assertEqual(Path(default_probe["path"]).parent.name, "d000")
        self.assertEqual(Path(root_probe["path"]).parent, cold_root)
        self.assertFalse(default_probe["root_level"])
        self.assertTrue(root_probe["root_level"])

    def test_sweep_completion_fence_uses_debug_only_before_single_search(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cold_root = Path(tmp)
            target = cold_root / "d150" / "file_150.txt"
            target.parent.mkdir()
            target.write_text("baseline\n", encoding="utf-8")
            debug_rows = [
                {
                    "dirs": [
                        {
                            "path": str(cold_root),
                            "rotating_cold_window_cycle_id": 7,
                            "rotating_cold_window_last_scan_seq": 10,
                            "rotating_cold_window_last_scan_cycle_id": 7,
                        }
                    ]
                },
                {
                    "dirs": [
                        {
                            "path": str(cold_root),
                            "rotating_cold_window_cycle_id": 8,
                            "rotating_cold_window_last_scan_seq": 10,
                            "rotating_cold_window_last_scan_cycle_id": 7,
                        }
                    ]
                },
                {
                    "dirs": [
                        {
                            "path": str(cold_root),
                            "rotating_cold_window_cycle_id": 9,
                            "rotating_cold_window_last_scan_seq": 11,
                            "rotating_cold_window_last_scan_cycle_id": 9,
                        }
                    ]
                },
            ]
            events_path = cold_root / "probe.jsonl"
            with mock.patch.object(
                fixture.BENCH, "debug_tiered_watch", side_effect=debug_rows
            ) as debug, mock.patch.object(
                fixture, "_search_entry",
                return_value={"freshness": "fresh", "index_tier": "HotMemory"},
            ) as search, mock.patch.object(
                fixture.time,
                "monotonic",
                side_effect=[100.0, 100.1, 100.2, 100.3, 100.4, 101.0],
            ), mock.patch.object(fixture.time, "sleep"):
                result = fixture._run_sweep_only_modify_probe(
                    "http://fixture",
                    cold_root,
                    70.0,
                    completion_fence=True,
                    full_sweep_period_secs=45,
                    ttl_secs=45,
                    tick_secs=15,
                    repair_deadline_secs=75,
                    probe_events_path=events_path,
                )
            events = [
                json.loads(line)
                for line in events_path.read_text(encoding="utf-8").splitlines()
            ]

        self.assertEqual(debug.call_count, 3)
        search.assert_called_once_with("http://fixture", target)
        self.assertTrue(result["completion_fence_observed"])
        self.assertEqual(result["pre_mutation_last_scan_seq"], 10)
        self.assertEqual(result["post_mutation_last_scan_seq"], 10)
        self.assertEqual(result["post_mutation_cycle_id"], 8)
        self.assertEqual(result["completion_last_scan_seq"], 11)
        self.assertEqual(result["completion_last_scan_cycle_id"], 9)
        self.assertEqual(result["completion_fence_timeout_secs"], 150.0)
        self.assertEqual(result["repair_deadline_secs"], 75)
        self.assertEqual(result["searches_before_fence"], 0)
        self.assertEqual(result["first_query_count"], 1)
        self.assertTrue(result["repaired_by_sweep"])
        self.assertEqual(
            [row["phase"] for row in events],
            [
                "pre_mutation_debug",
                "mutation_written",
                "post_mutation_debug",
                "poll_debug",
                "search",
            ],
        )

    def test_sweep_completion_fence_times_out_without_search(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cold_root = Path(tmp)
            target = cold_root / "d150" / "file_150.txt"
            target.parent.mkdir()
            target.write_text("baseline\n", encoding="utf-8")
            debug_row = {
                "dirs": [
                    {
                        "path": str(cold_root),
                        "rotating_cold_window_cycle_id": 7,
                        "rotating_cold_window_last_scan_seq": 10,
                        "rotating_cold_window_last_scan_cycle_id": 7,
                    }
                ]
            }
            events_path = cold_root / "probe.jsonl"
            with mock.patch.object(
                fixture.BENCH, "debug_tiered_watch", return_value=debug_row
            ), mock.patch.object(fixture, "_search_entry") as search, mock.patch.object(
                fixture.time,
                "monotonic",
                side_effect=[100.0, 100.1, 100.2, 175.2, 175.2],
            ), mock.patch.object(fixture.time, "sleep"):
                result = fixture._run_sweep_only_modify_probe(
                    "http://fixture",
                    cold_root,
                    70.0,
                    completion_fence=True,
                    full_sweep_period_secs=45,
                    ttl_secs=45,
                    tick_secs=15,
                    repair_deadline_secs=75,
                    probe_events_path=events_path,
                )

        search.assert_not_called()
        self.assertFalse(result["completion_fence_observed"])
        self.assertEqual(result["first_query_count"], 0)
        self.assertIn("timed out", result["error"])
        self.assertGreaterEqual(result["waited_secs"], 75.0)

    def test_default_sweep_probe_keeps_fixed_wait_behavior(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cold_root = Path(tmp)
            target = cold_root / "d150" / "file_150.txt"
            target.parent.mkdir()
            target.write_text("baseline\n", encoding="utf-8")
            with mock.patch.object(
                fixture.BENCH, "debug_tiered_watch"
            ) as debug, mock.patch.object(
                fixture, "_search_entry",
                return_value={"freshness": "fresh", "index_tier": "HotMemory"},
            ) as search, mock.patch.object(fixture.time, "sleep") as sleep:
                result = fixture._run_sweep_only_modify_probe(
                    "http://fixture", cold_root, 70.0
                )

        debug.assert_not_called()
        sleep.assert_called_once_with(70.0)
        search.assert_called_once_with("http://fixture", target)
        self.assertFalse(result["completion_fence"])


if __name__ == "__main__":
    unittest.main()
