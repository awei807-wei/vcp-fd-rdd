from __future__ import annotations

import importlib.util
import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).with_name("m2-cold-window-vm-bench.py")
SPEC = importlib.util.spec_from_file_location("m2_cold_window_vm_bench", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
BENCH = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = BENCH
SPEC.loader.exec_module(BENCH)


def write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text(
        "".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows),
        encoding="utf-8",
    )


def memory_sample(
    elapsed_secs: float,
    *,
    rebuild: bool,
    hot_entries: int,
    manifest_entries: int,
    rss_bytes: int,
    l2_scale: int,
) -> dict[str, object]:
    return {
        "elapsed_secs": elapsed_secs,
        "endpoint": "/memory",
        "ok": True,
        "data": {
            "process_rss_bytes": rss_bytes,
            "base": {
                "file_count": hot_entries + manifest_entries,
                "hot_memory_entries": hot_entries,
                "manifest_only_entries": manifest_entries,
                "cold_segment_count": 1 if manifest_entries else 0,
                "cold_mmap_bytes": 700 * l2_scale,
            },
            "rebuild": {"in_progress": rebuild},
            "l2": {
                "estimated_bytes": 10 * l2_scale,
                "arena_bytes": 20 * l2_scale,
                "filekey_to_docid_bytes": 30 * l2_scale,
                "trigram_bytes": 40 * l2_scale,
                "parent_index_bytes": 50 * l2_scale,
                "parent_path_lookup_bytes": 60 * l2_scale,
            },
            "non_index_private_dirty_bytes": 800 * l2_scale,
        },
    }


class SummarySemanticsTests(unittest.TestCase):
    def test_first_query_uses_expected_existence_and_keeps_transport_health(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            write_jsonl(
                run_dir / "event-storm-samples.jsonl",
                [
                    {
                        "event_kind": "first_query",
                        "operation": "create_first_query",
                        "workload": "rw100",
                        "should_exist": True,
                        "first_query_exists": False,
                        "ok": True,
                        "latency_secs": 0.01,
                    },
                    {
                        "event_kind": "first_query",
                        "operation": "delete_first_query",
                        "workload": "rw100",
                        "should_exist": False,
                        "first_query_exists": False,
                        "ok": False,
                        "latency_secs": 0.02,
                    },
                    {
                        "event_kind": "first_query",
                        "operation": "delete_transport_failure_first_query",
                        "workload": "rw100",
                        "should_exist": False,
                        "first_query_exists": False,
                        "ok": False,
                        "error": "connection refused",
                        "latency_secs": 0.03,
                    },
                    {
                        "event_kind": "burst_checked",
                        "events_total": 3,
                        "ok": 2,
                        "missed": 1,
                    },
                ],
            )

            summary = BENCH.summarize(run_dir, "semantic", 0)

            first_query = summary["event_storm"]["first_query"]
            self.assertEqual(first_query["ok"], 1)
            self.assertEqual(first_query["missed"], 2)
            self.assertEqual(first_query["transport_ok"], 2)
            self.assertEqual(first_query["transport_failures"], 1)
            self.assertEqual(summary["event_storm"]["ok"], 1)
            self.assertEqual(summary["event_storm"]["missed"], 2)

    def test_inode_reuse_status_distinguishes_not_run_inconclusive_and_exercised(self) -> None:
        cases = [
            ([], "not_run"),
            (
                [
                    {
                        "event_kind": "inode_reuse_stress_summary",
                        "inode_reuse_attempts": 100,
                        "inode_reuse_observed": 0,
                        "tmpfs_mounted": False,
                    }
                ],
                "inconclusive",
            ),
            (
                [
                    {
                        "event_kind": "inode_reuse_stress_summary",
                        "inode_reuse_attempts": 100,
                        "inode_reuse_observed": 2,
                        "tmpfs_mounted": True,
                    }
                ],
                "exercised",
            ),
        ]
        for rows, expected in cases:
            with self.subTest(expected=expected), tempfile.TemporaryDirectory() as tmp:
                run_dir = Path(tmp)
                write_jsonl(run_dir / "event-storm-samples.jsonl", rows)
                summary = BENCH.summarize(run_dir, expected, 0)
                special = summary["event_storm"]["special"]
                self.assertEqual(special["inode_reuse_status"], expected)

    def test_memory_timeline_reports_phase_peaks_and_l2_components(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            write_jsonl(
                run_dir / "endpoint-samples.jsonl",
                [
                    memory_sample(
                        1.0,
                        rebuild=True,
                        hot_entries=100,
                        manifest_entries=0,
                        rss_bytes=110,
                        l2_scale=1,
                    ),
                    memory_sample(
                        1.5,
                        rebuild=False,
                        hot_entries=100,
                        manifest_entries=0,
                        rss_bytes=170,
                        l2_scale=2,
                    ),
                    memory_sample(
                        2.0,
                        rebuild=False,
                        hot_entries=0,
                        manifest_entries=100,
                        rss_bytes=80,
                        l2_scale=3,
                    ),
                    memory_sample(
                        3.0,
                        rebuild=False,
                        hot_entries=100,
                        manifest_entries=0,
                        rss_bytes=190,
                        l2_scale=4,
                    ),
                ],
            )
            write_jsonl(
                run_dir / "process-samples.jsonl",
                [
                    {"elapsed_secs": 1.0, "vmrss_bytes": 120},
                    {"elapsed_secs": 1.5, "vmrss_bytes": 180},
                    {"elapsed_secs": 2.0, "vmrss_bytes": 90},
                    {"elapsed_secs": 3.0, "vmrss_bytes": 200},
                ],
            )

            summary = BENCH.summarize(run_dir, "memory", 0)

            phases = summary["memory_timeline"]["phase_peaks"]
            self.assertEqual(phases["rebuild"]["process_rss_bytes_max"], 120)
            self.assertEqual(phases["initial_build_publish"]["process_rss_bytes_max"], 180)
            self.assertEqual(phases["cold_steady"]["process_rss_bytes_max"], 90)
            self.assertEqual(phases["hot_base_snapshot"]["process_rss_bytes_max"], 200)
            l2 = summary["memory_timeline"]["l2"]
            self.assertEqual(l2["components"]["estimated_bytes"]["max"], 40)
            self.assertEqual(l2["components"]["arena_bytes"]["max"], 80)
            self.assertEqual(l2["components"]["filekey_to_docid_bytes"]["max"], 120)
            self.assertEqual(l2["components"]["trigram_bytes"]["max"], 160)
            self.assertEqual(l2["components"]["parent_index_bytes"]["max"], 200)
            self.assertEqual(l2["components"]["parent_path_lookup_bytes"]["max"], 240)
            self.assertEqual(len(l2["series"]), 4)
            self.assertEqual(l2["series"][1]["base_hot_memory_entries"], 100)
            self.assertEqual(l2["series"][2]["base_manifest_only_entries"], 100)
            self.assertEqual(l2["series"][2]["base_cold_mmap_bytes"], 2100)
            self.assertEqual(l2["series"][3]["non_index_private_dirty_bytes"], 3200)

    def test_memory_series_preserves_boundaries_extremes_and_last_sample(self) -> None:
        endpoint_samples = []
        for idx in range(300):
            if idx < 60:
                hot_entries, manifest_entries = 100, 0
            elif idx < 120:
                hot_entries, manifest_entries = 0, 100
            elif idx < 180:
                hot_entries, manifest_entries = 100, 0
            elif idx < 240:
                hot_entries, manifest_entries = 0, 100
            else:
                hot_entries, manifest_entries = 100, 0
            scale = 10_000 if idx == 157 else idx + 1
            endpoint_samples.append(
                memory_sample(
                    float(idx),
                    rebuild=False,
                    hot_entries=hot_entries,
                    manifest_entries=manifest_entries,
                    rss_bytes=idx,
                    l2_scale=scale,
                )
            )

        timeline = BENCH.build_memory_timeline(endpoint_samples, [])
        series = timeline["l2"]["series"]
        elapsed = {point["elapsed_secs"] for point in series}
        self.assertLessEqual(len(series), 256)
        self.assertIn(0.0, elapsed)
        self.assertIn(59.0, elapsed)
        self.assertIn(60.0, elapsed)
        self.assertIn(119.0, elapsed)
        self.assertIn(120.0, elapsed)
        self.assertIn(157.0, elapsed)
        self.assertIn(179.0, elapsed)
        self.assertIn(180.0, elapsed)
        self.assertIn(239.0, elapsed)
        self.assertIn(240.0, elapsed)
        self.assertIn(299.0, elapsed)


class UdsPathTests(unittest.TestCase):
    def test_short_uds_path_is_stable_and_inside_tmp(self) -> None:
        long_run_dir = Path("/tmp") / ("very-long-benchmark-directory-" * 8)
        first = BENCH.short_uds_socket_path(long_run_dir)
        second = BENCH.short_uds_socket_path(long_run_dir)
        self.assertEqual(first, second)
        self.assertEqual(first.parent, Path("/tmp"))
        self.assertLessEqual(len(os.fsencode(first)), 100)

    def test_cleanup_uds_socket_removes_stale_socket_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            socket_path = Path(tmp) / "fd-rdd.sock"
            socket_path.touch()
            BENCH.cleanup_uds_socket(socket_path)
            self.assertFalse(socket_path.exists())


class ProcessSampleRunnerTests(unittest.TestCase):
    def test_process_sampler_runs_independently(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out_path = Path(tmp) / "process-samples.jsonl"
            runner = BENCH.ProcessSampleRunner(
                os.getpid(),
                out_path,
                time.monotonic(),
                0.01,
            )
            runner.start()
            time.sleep(0.06)
            runner.stop()

            self.assertEqual(runner.error, "")
            rows = BENCH.read_jsonl(out_path)
            self.assertGreaterEqual(len(rows), 2)
            self.assertTrue(all("vmrss_bytes" in row for row in rows))


if __name__ == "__main__":
    unittest.main()
