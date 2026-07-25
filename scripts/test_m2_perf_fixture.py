"""m2_perf_fixture 纯分析函数与配置组装的单元测试。"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

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
                "latency_secs": 40.1,
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


if __name__ == "__main__":
    unittest.main()
