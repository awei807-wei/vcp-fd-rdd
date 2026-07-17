from __future__ import annotations

import importlib.util
import hashlib
import json
import os
import sys
import tempfile
import time
import unittest
from argparse import Namespace
from pathlib import Path
from unittest import mock


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


class QueryLeaseConfigTests(unittest.TestCase):
    def test_disabled_query_fast_scan_leases_are_written_to_tiered_config(self) -> None:
        args = BENCH.parse_args(
            [
                "--root",
                "/fixture/root",
                "--no-query-fast-scan-leases",
            ]
        )
        with tempfile.TemporaryDirectory() as tmp:
            config = BENCH.write_config(args, Path(tmp))
            text = config.read_text(encoding="utf-8")

        self.assertIn("l1_l2_fast_scan_query_leases_enabled = false", text)
        self.assertFalse(args.query_fast_scan_leases)


def memory_sample(
    elapsed_secs: float,
    *,
    rebuild: bool,
    hot_entries: int,
    manifest_entries: int,
    rss_bytes: int,
    l2_scale: int,
    owned_snapshot_state: str = "none",
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
            "rebuild": {
                "in_progress": rebuild,
                "owned_snapshot_state": owned_snapshot_state,
            },
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
    def test_post_cleanup_audit_summary_exposes_watcher_convergence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            write_jsonl(
                run_dir / "event-storm-samples.jsonl",
                [
                    {
                        "event_kind": "post_cleanup_audit",
                        "ok": True,
                        "watcher_ledger_cleared": True,
                        "cleanup_target_entries": 0,
                        "cleanup_target_ephemeral_watch_dirs": 0,
                        "cleanup_target_rotating_active_dirs": 0,
                        "target_ephemeral_watch": False,
                        "target_rotating_active": False,
                        "audit_after_lease_expiry": True,
                        "audit_before_next_rotation": True,
                        "audit_window_valid": True,
                    }
                ],
            )

            summary = BENCH.summarize(run_dir, "cleanup-audit", 0)

            self.assertEqual(
                summary["event_storm"]["post_cleanup_audit"],
                {
                    "count": 1,
                    "ok": 1,
                    "failures": 0,
                    "watcher_ledger_cleared": 1,
                    "cleanup_target_entries_max": 0,
                    "cleanup_target_ephemeral_watch_dirs_max": 0,
                    "cleanup_target_rotating_active_dirs_max": 0,
                    "target_ephemeral_watch_seen": False,
                    "target_rotating_active_seen": False,
                    "audit_after_lease_expiry": 1,
                    "audit_before_next_rotation": 1,
                    "audit_window_valid": 1,
                },
            )

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
                    {
                        "event_kind": "burst_cleanup",
                        "cleanup_target": "/fixture/current-burst",
                        "entries_estimated": 7,
                        "duration_secs": 0.025,
                        "ok": True,
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
            self.assertEqual(
                summary["event_storm"]["cleanup"],
                {
                    "count": 1,
                    "ok": 1,
                    "failures": 0,
                    "entries_estimated_total": 7,
                    "duration_p95_secs": 0.025,
                    "duration_max_secs": 0.025,
                },
            )

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

    def test_visibility_summary_counts_one_result_per_unique_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            write_jsonl(
                run_dir / "event-storm-samples.jsonl",
                [
                    {
                        "event_kind": "burst_started",
                        "elapsed_secs": 0.8,
                    },
                    {
                        "event_kind": "burst_written",
                        "selected_kind": "save100",
                        "events_total": 2,
                        "duration_secs": 0.01,
                    },
                    {"event_kind": "burst_checked", "events_total": 2, "ok": 2},
                    {"event_kind": "burst_checked", "events_total": 2, "ok": 2},
                    {
                        "event_kind": "visibility_probe",
                        "workload": "save100",
                        "path": "/fixture/a.txt",
                        "visible": True,
                        "latency_secs": 1.25,
                        "transport_failures": 0,
                    },
                    {
                        "event_kind": "visibility_probe",
                        "workload": "save100",
                        "path": "/fixture/b.txt",
                        "visible": False,
                        "latency_secs": 120.0,
                        "transport_failures": 1,
                    },
                ],
            )

            summary = BENCH.summarize(run_dir, "visibility", 0)

            self.assertEqual(summary["event_storm"]["bursts"], 1)
            self.assertEqual(summary["event_storm"]["checks"], 2)
            self.assertEqual(
                summary["event_storm"]["visibility"],
                {
                    "total": 2,
                    "visible": 1,
                    "timeouts": 1,
                    "success_rate": 0.5,
                    "latency_p50_secs": 1.25,
                    "latency_p95_secs": 1.25,
                    "latency_max_secs": 1.25,
                    "transport_failures": 1,
                    "by_workload": {
                        "save100": {
                            "total": 2,
                            "visible": 1,
                            "success_rate": 0.5,
                        }
                    },
                },
            )

    def test_process_and_waterline_summary_exposes_paired_cost_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            write_jsonl(
                run_dir / "process-samples.jsonl",
                [
                    {
                        "elapsed_secs": 0.0,
                        "cpu_pct": 0.0,
                        "cpu_ticks": 1000,
                        "read_bytes": 100,
                        "write_bytes": 200,
                        "read_syscalls": 10,
                        "write_syscalls": 20,
                        "minor_faults": 1000,
                        "major_faults": 2,
                    },
                    {
                        "elapsed_secs": 1.0,
                        "cpu_pct": 0.0,
                        "cpu_ticks": 1050,
                        "read_bytes": 150,
                        "write_bytes": 300,
                        "read_syscalls": 15,
                        "write_syscalls": 30,
                        "minor_faults": 1004,
                        "major_faults": 2,
                    },
                    {
                        "elapsed_secs": 3.0,
                        "cpu_pct": 0.0,
                        "cpu_ticks": 1100,
                        "read_bytes": 250,
                        "write_bytes": 500,
                        "read_syscalls": 30,
                        "write_syscalls": 50,
                        "minor_faults": 1010,
                        "major_faults": 3,
                    },
                ],
            )
            write_jsonl(
                run_dir / "endpoint-samples.jsonl",
                [
                    {
                        "ok": True,
                        "endpoint": "/watch-state",
                        "data": {
                            "dirty_queue_len": 2,
                            "waterline_soft_degraded": True,
                            "waterline_hard_degraded": False,
                            "waterline_effective_rotating_budget": 64,
                        },
                    },
                    {
                        "ok": True,
                        "endpoint": "/watch-state",
                        "data": {
                            "dirty_queue_len": 0,
                            "waterline_soft_degraded": False,
                            "waterline_hard_degraded": False,
                            "waterline_effective_rotating_budget": 128,
                        },
                    },
                ],
            )
            write_jsonl(
                run_dir / "event-storm-samples.jsonl",
                [
                    {
                        "event_kind": "burst_started",
                        "elapsed_secs": 0.8,
                    },
                    {
                        "event_kind": "burst_written",
                        "elapsed_secs": 1.0,
                        "events_total": 1,
                        "duration_secs": 0.01,
                    }
                ],
            )

            with mock.patch.object(BENCH.os, "sysconf", return_value=100):
                summary = BENCH.summarize(run_dir, "paired-cost", 0)

            process = summary["process"]
            self.assertEqual(process["cpu_core_seconds"], 1.0)
            self.assertEqual(process["read_bytes_delta"], 150)
            self.assertEqual(process["write_bytes_delta"], 300)
            self.assertEqual(process["read_syscalls_delta"], 20)
            self.assertEqual(process["write_syscalls_delta"], 30)
            self.assertEqual(process["minor_faults_delta"], 10)
            self.assertEqual(process["major_faults_delta"], 1)
            event_window = summary["process_after_first_burst"]
            self.assertEqual(event_window["sample_count"], 3)
            self.assertEqual(event_window["cpu_core_seconds"], 1.0)
            self.assertEqual(event_window["read_bytes_delta"], 150)
            self.assertEqual(event_window["minor_faults_delta"], 10)
            storm_window = summary["process_after_event_storm_start"]
            self.assertEqual(storm_window["sample_count"], 3)
            self.assertEqual(storm_window["cpu_core_seconds"], 1.0)
            watch = summary["watch_state"]
            self.assertEqual(watch["dirty_queue_len_last"], 0)
            self.assertEqual(watch["waterline_soft_degraded_samples"], 1)
            self.assertEqual(watch["waterline_soft_degraded_ratio"], 0.5)
            self.assertFalse(watch["waterline_soft_degraded_last"])
            self.assertEqual(watch["waterline_hard_degraded_samples"], 0)
            self.assertEqual(watch["waterline_effective_rotating_budget_last"], 128)

            BENCH.write_report(run_dir, summary)
            report = (run_dir / "REPORT.md").read_text(encoding="utf-8")
            for metric in (
                "process CPU core seconds",
                "process read bytes delta",
                "process minor faults delta",
                "event-storm-window CPU core seconds",
                "after-first-burst CPU core seconds",
                "dirty queue last",
                "waterline soft degraded ratio",
                "waterline effective rotating budget last",
                "event storm visibility success rate",
            ):
                self.assertIn(metric, report)

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
            BENCH.write_report(run_dir, summary)

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
            report = (run_dir / "REPORT.md").read_text(encoding="utf-8")
            self.assertIn("final snapshot window RSS max", report)
            self.assertIn("event storm cleanup failures", report)

    def test_owned_snapshot_state_is_initial_build_publish(self) -> None:
        sample = memory_sample(
            2.0,
            rebuild=False,
            hot_entries=0,
            manifest_entries=0,
            rss_bytes=512,
            l2_scale=9,
            owned_snapshot_state="writing",
        )

        timeline = BENCH.build_memory_timeline([sample], [])

        point = timeline["l2"]["series"][0]
        self.assertEqual(point["phase"], "initial_build_publish")
        self.assertEqual(point["owned_snapshot_state"], "writing")
        self.assertIn(
            "high-water mark",
            timeline["limitations"]["owned_snapshot_l2_semantics"],
        )
        self.assertIn(
            "same pristine read-only VM/disk snapshot",
            timeline["limitations"]["formal_ab_fixture_reset"],
        )

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

    def test_process_samples_after_last_endpoint_are_final_snapshot(self) -> None:
        endpoint_samples = [
            memory_sample(
                1.0,
                rebuild=False,
                hot_entries=0,
                manifest_entries=100,
                rss_bytes=80,
                l2_scale=1,
            ),
            memory_sample(
                2.0,
                rebuild=False,
                hot_entries=0,
                manifest_entries=100,
                rss_bytes=90,
                l2_scale=1,
            ),
            {
                "elapsed_secs": 2.2,
                "endpoint": "/watch-state",
                "ok": True,
                "data": {"l0_dirs": 1},
            },
        ]
        timeline = BENCH.build_memory_timeline(
            endpoint_samples,
            [
                {"elapsed_secs": 1.5, "vmrss_bytes": 95},
                {"elapsed_secs": 2.1, "vmrss_bytes": 100},
                {"elapsed_secs": 2.5, "vmrss_bytes": 450},
            ],
            shutdown_signal_elapsed_secs=2.4,
        )

        phases = timeline["phase_peaks"]
        self.assertEqual(phases["cold_steady"]["process_rss_bytes_max"], 100)
        self.assertEqual(
            phases["final_snapshot_window"]["process_rss_bytes_max"], 450
        )
        self.assertFalse(
            timeline["limitations"]["periodic_snapshot_lifecycle_available"]
        )

    def test_samples_after_endpoint_but_before_shutdown_signal_remain_steady(self) -> None:
        endpoint_samples = [
            memory_sample(
                1.0,
                rebuild=False,
                hot_entries=0,
                manifest_entries=100,
                rss_bytes=80,
                l2_scale=1,
            )
        ]
        timeline = BENCH.build_memory_timeline(
            endpoint_samples,
            [
                {"elapsed_secs": 1.5, "vmrss_bytes": 120},
                {"elapsed_secs": 2.5, "vmrss_bytes": 300},
            ],
            shutdown_signal_elapsed_secs=2.0,
        )

        self.assertEqual(
            timeline["phase_peaks"]["cold_steady"]["process_rss_bytes_max"],
            120,
        )
        self.assertEqual(
            timeline["phase_peaks"]["final_snapshot_window"][
                "process_rss_bytes_max"
            ],
            300,
        )

    def test_scale_and_tier_summary_use_current_endpoint_shapes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            (run_dir / "manifest.json").write_text(
                json.dumps(
                    {
                        "git_sha": "a" * 40,
                        "ab_parameter_fingerprint": "b" * 64,
                        "fixture_initial_file_count": 1_000_000,
                        "duration_secs": 3600,
                        "actual_duration_secs": 3601.25,
                        "runner_args": {
                            "rotating_max_dirs_per_tick": 4,
                            "rotating_tick_secs": 30,
                        },
                    }
                ),
                encoding="utf-8",
            )
            write_jsonl(
                run_dir / "endpoint-samples.jsonl",
                [
                    {
                        "elapsed_secs": 1.0,
                        "endpoint": "/status",
                        "ok": True,
                        "data": {"indexed_count": 1_000_123, "is_rebuilding": False},
                    },
                    {
                        "elapsed_secs": 1.1,
                        "endpoint": "/watch-state",
                        "ok": True,
                        "data": {
                            "l0_dirs": 2,
                            "l1_dirs": 3,
                            "l2_dirs": 5,
                            "l3_dirs": 7,
                            "rotating_cold_window_enabled": True,
                        },
                    },
                ],
            )

            summary = BENCH.summarize(run_dir, "current-shapes", 0)

            self.assertEqual(summary["scale_aware"]["index_total_files"], 1_000_123)
            self.assertEqual(summary["scale_aware"]["hot_dir_count"], 5)
            self.assertEqual(summary["scale_aware"]["cold_dir_count"], 12)
            self.assertEqual(
                summary["scale_aware"]["rotation_cycle_estimate_secs"], 90.0
            )
            self.assertEqual(summary["run_audit"]["git_sha"], "a" * 40)
            self.assertEqual(summary["run_audit"]["ab_parameter_fingerprint"], "b" * 64)
            self.assertEqual(summary["run_audit"]["fixture_initial_file_count"], 1_000_000)
            self.assertEqual(
                summary["tier_distribution"]["end_of_run"],
                {"L0": 2, "L1": 3, "L2": 5, "L3": 7, "unknown": 0},
            )


class PassiveCanaryShutdownTests(unittest.TestCase):
    @staticmethod
    def make_runner(run_dir: Path) -> BENCH.PassiveCanaryRunner:
        root = run_dir / "passive"
        root.mkdir()
        return BENCH.PassiveCanaryRunner(
            base_url="http://127.0.0.1:6060",
            root=root,
            out_path=run_dir / "canary-samples.jsonl",
            started_at=90.0,
            interval_secs=180.0,
            settle_secs=90.0,
            timeout_secs=0.0,
            start_delay_secs=60.0,
        )

    @staticmethod
    def scan_response(payload: dict[str, object]) -> mock.MagicMock:
        response = mock.MagicMock()
        response.__enter__.return_value = response
        response.read.return_value = json.dumps(payload).encode("utf-8")
        return response

    @classmethod
    def unstable_then_stable_responses(cls) -> list[mock.MagicMock]:
        return [
            cls.scan_response(
                {
                    "scanned": 3,
                    "changed": 2,
                    "deleted": 1,
                    "elapsed_ms": 17,
                    "stable": False,
                }
            ),
            cls.scan_response(
                {
                    "scanned": 4,
                    "changed": 1,
                    "deleted": 2,
                    "elapsed_ms": 11,
                    "stable": True,
                }
            ),
        ]

    def assert_post_paths(
        self,
        urlopen: mock.MagicMock,
        expected_paths: list[str],
    ) -> None:
        first_request = urlopen.call_args_list[0].args[0]
        self.assertEqual(first_request.get_method(), "POST")
        self.assertEqual(first_request.full_url, "http://127.0.0.1:6060/scan")
        self.assertEqual(urlopen.call_count, 2)
        for call in urlopen.call_args_list:
            request = call.args[0]
            self.assertEqual(
                json.loads(request.data.decode("utf-8")),
                {"paths": expected_paths},
            )
            self.assertGreater(call.kwargs["timeout"], 0)
            self.assertLessEqual(
                call.kwargs["timeout"],
                BENCH.PASSIVE_SHUTDOWN_RECONCILE_TIMEOUT_SECS,
            )

    @staticmethod
    def clean_initial_state() -> dict[str, object]:
        return {
            "fixture": {"trusted": True, "count_verified": True},
            "snapshot": {"fresh": True},
            "binary_sha256": "a" * 64,
            "git_sha": "b" * 40,
            "git_dirty": False,
            "artifact_provenance": {
                "verified": True,
                "validated_binary_sha256": "a" * 64,
                "execution_binary_sha256": "a" * 64,
            },
            "run_dir_preexisting": False,
            "collection_errors": [],
        }

    @staticmethod
    def build_execution(
        run_dir: Path,
        *,
        passive_canary_enabled: bool,
        shutdown_snapshot_quiesce_required: bool = False,
    ) -> dict[str, object]:
        write_jsonl(run_dir / "process-samples.jsonl", [{"vmrss_bytes": 1}])
        write_jsonl(
            run_dir / "endpoint-samples.jsonl",
            [{"endpoint": "/memory", "ok": True, "data": {}}],
        )
        return BENCH.build_execution_state(
            run_dir,
            requested_duration_secs=10,
            actual_duration_secs=10.0,
            exit_code=0,
            fatal_error="",
            process_sampler_error="",
            completion_reason="duration_elapsed",
            cleanup_errors=[],
            shutdown_signal_elapsed_secs=10.0,
            passive_canary_enabled=passive_canary_enabled,
            shutdown_snapshot_quiesce_required=(
                shutdown_snapshot_quiesce_required
            ),
        )

    def test_shutdown_reconcile_posts_owned_paths_and_retries_until_stable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            runner = self.make_runner(run_dir)
            runner.active = {"cycle": 7, "stage": "check_delete"}
            active_root = run_dir / "active-canary"
            burst_root = run_dir / "event-root" / "active-burst"
            event_root = burst_root.parent
            hot_root = run_dir / "hot-root"
            event_storm = mock.Mock()
            event_storm.active = {"burst_root": burst_root}
            event_storm.roots = [event_root]
            paths = BENCH.benchmark_shutdown_reconcile_paths(
                runner.root,
                active_root,
                event_storm,
                True,
                [hot_root, active_root],
            )
            with mock.patch.object(
                BENCH.urllib.request,
                "urlopen",
                side_effect=self.unstable_then_stable_responses(),
            ) as urlopen, mock.patch.object(BENCH.time, "sleep") as sleep:
                record = runner.reconcile_shutdown(paths)

            expected_paths = [
                str(runner.root),
                str(active_root),
                str(burst_root),
                str(event_root),
                str(hot_root),
            ]
            self.assert_post_paths(urlopen, expected_paths)
            sleep.assert_called_once()
            self.assertTrue(record["ok"])
            self.assertTrue(record["stable"])
            self.assertEqual(record["attempts"], 2)
            self.assertEqual(record["paths"], expected_paths)
            self.assertEqual(record["scanned"], 7)
            self.assertEqual(record["changed"], 3)
            self.assertEqual(record["deleted"], 3)
            self.assertEqual(record["daemon_elapsed_ms"], 28)
            self.assertEqual(record["active_cycle"], 7)
            self.assertEqual(record["active_stage"], "check_delete")

            rows = BENCH.read_jsonl(run_dir / "canary-samples.jsonl")
            self.assertEqual(rows, [record])
            summary = BENCH.summarize(run_dir, "reconcile-success", 0)
            self.assertEqual(
                summary["sample_counts"]["passive_shutdown_reconcile"],
                1,
            )
            self.assertEqual(
                summary["passive_shutdown_reconcile"],
                {
                    "count": 1,
                    "ok": 1,
                    "failures": 0,
                    "stable": True,
                    "attempts_max": 2,
                    "paths_max": 5,
                    "scanned_total": 7,
                    "changed_total": 3,
                    "deleted_total": 3,
                    "latency_max_secs": record["latency_secs"],
                },
            )

    def test_persistently_unstable_reconcile_is_recorded_and_gates_comparability(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            runner = self.make_runner(run_dir)
            unstable = {
                "scanned": 2,
                "changed": 1,
                "deleted": 1,
                "elapsed_ms": 3,
                "stable": False,
            }
            with mock.patch.object(
                BENCH,
                "post_json",
                side_effect=[unstable.copy(), unstable.copy(), unstable.copy()],
            ), mock.patch.object(BENCH.time, "sleep"):
                record = runner.reconcile_shutdown()

            self.assertFalse(record["ok"])
            self.assertFalse(record["stable"])
            self.assertEqual(record["attempts"], 3)
            self.assertEqual(record["scanned"], 6)
            self.assertEqual(record["changed"], 3)
            self.assertEqual(record["deleted"], 3)
            self.assertEqual(
                record["error"],
                "RuntimeError('POST /scan remained unstable after 3 attempts')",
            )
            self.assertIsNone(record["active_cycle"])
            self.assertIsNone(record["active_stage"])

            execution = self.build_execution(
                run_dir,
                passive_canary_enabled=True,
            )
            self.assertEqual(execution["passive_shutdown_reconcile_count"], 1)
            self.assertEqual(execution["passive_shutdown_reconcile_ok"], 0)
            self.assertEqual(execution["passive_shutdown_reconcile_failures"], 1)
            self.assertTrue(execution["passive_shutdown_reconcile_failed"])
            comparable, reasons = BENCH.evaluate_ab_comparability(
                self.clean_initial_state(),
                execution,
            )
            self.assertFalse(comparable)
            self.assertEqual(reasons, ["passive_shutdown_reconcile_failed"])

    def test_shutdown_reconcile_paths_are_deduplicated_and_capped(self) -> None:
        base = Path("/fixture")
        event_storm = mock.Mock()
        event_storm.active = {"burst_root": base / "active-burst"}
        event_storm.roots = [base / f"event-{idx}" for idx in range(8)]

        paths = BENCH.benchmark_shutdown_reconcile_paths(
            base / "passive",
            base / "active",
            event_storm,
            True,
            [base / "active", *[base / f"hot-{idx}" for idx in range(8)]],
        )

        self.assertEqual(len(paths), 10)
        self.assertEqual(
            paths[:3],
            [base / "passive", base / "active", base / "active-burst"],
        )
        self.assertEqual(len({str(path) for path in paths}), 10)

    def test_passive_mutations_emit_success_audit_records(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            runner = self.make_runner(run_dir)
            created = runner.root / "probe_create.txt"
            renamed = runner.root / "probe_rename.txt"
            created.write_text("probe", encoding="utf-8")
            runner.active = {
                "cycle": 4,
                "stage": "check_create",
                "created": created,
                "renamed": renamed,
                "stage_started_at": time.monotonic(),
                "due_at": time.monotonic(),
            }

            with mock.patch.object(runner, "record_first_query"), mock.patch.object(
                runner,
                "record_after_query_if_needed",
            ):
                runner.process_due(time.monotonic())
                runner.process_due(time.monotonic())

            rows = BENCH.read_jsonl(run_dir / "canary-samples.jsonl")
            rename = next(
                row for row in rows if row["operation"] == "passive_rename_applied"
            )
            delete = next(
                row for row in rows if row["operation"] == "passive_delete_applied"
            )
            self.assertEqual(rename["old_path"], str(created))
            self.assertEqual(rename["new_path"], str(renamed))
            self.assertTrue(rename["ok"])
            self.assertEqual(delete["path"], str(renamed))
            self.assertTrue(delete["ok"])
            self.assertFalse(created.exists())
            self.assertFalse(renamed.exists())

    def test_execution_state_requires_reconcile_only_for_passive_canary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            missing = self.build_execution(run_dir, passive_canary_enabled=True)
            comparable, reasons = BENCH.evaluate_ab_comparability(
                self.clean_initial_state(),
                missing,
            )
            self.assertFalse(comparable)
            self.assertEqual(reasons, ["passive_shutdown_reconcile_missing"])
            self.assertTrue(missing["passive_shutdown_reconcile_missing"])

            normal = self.build_execution(run_dir, passive_canary_enabled=False)
            comparable, reasons = BENCH.evaluate_ab_comparability(
                self.clean_initial_state(),
                normal,
            )
            self.assertTrue(comparable)
            self.assertEqual(reasons, [])
            self.assertEqual(normal["passive_shutdown_reconcile_count"], 0)
            self.assertEqual(normal["passive_shutdown_reconcile_ok"], 0)
            self.assertEqual(normal["passive_shutdown_reconcile_failures"], 0)
            self.assertFalse(normal["passive_shutdown_reconcile_missing"])
            self.assertFalse(normal["passive_shutdown_reconcile_failed"])

    def test_snapshot_quiesce_retries_rebuild_until_durable_ready(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            daemon_log = run_dir / "fd-rdd.log"
            daemon_log.write_text("fd-rdd ready.\n", encoding="utf-8")
            responses = [
                self.scan_response(
                    {
                        "ready": False,
                        "written": False,
                        "is_rebuilding": True,
                        "error": (
                            "direct_v7_unsupported: subtree move completeness "
                            "is unproven; rebuild required"
                        ),
                    }
                ),
                self.scan_response(
                    {
                        "ready": True,
                        "written": True,
                        "is_rebuilding": False,
                        "error": None,
                    }
                ),
                self.scan_response(
                    {
                        "ready": True,
                        "written": False,
                        "is_rebuilding": False,
                        "error": None,
                    }
                ),
            ]
            with mock.patch.object(
                BENCH.urllib.request,
                "urlopen",
                side_effect=responses,
            ) as urlopen, mock.patch.object(BENCH.time, "sleep") as sleep:
                record = BENCH.record_shutdown_snapshot_quiesce(
                    "http://127.0.0.1:6060",
                    run_dir / "shutdown-samples.jsonl",
                    started_at=time.monotonic() - 10.0,
                    timeout_secs=30.0,
                    daemon_log_path=daemon_log,
                )

            self.assertTrue(record["ok"])
            self.assertTrue(record["ready"])
            self.assertTrue(record["written"])
            self.assertTrue(record["rebuild_observed"])
            self.assertEqual(record["attempts"], 3)
            self.assertEqual(record["not_ready_responses"], 1)
            self.assertEqual(record["ready_confirmations"], 2)
            self.assertIn("subtree move completeness", record["last_daemon_error"])
            self.assertGreaterEqual(record["elapsed_secs"], 10.0)
            self.assertGreaterEqual(
                record["elapsed_secs"], record["started_elapsed_secs"]
            )
            self.assertTrue(record["daemon_log_window_valid"])
            self.assertEqual(
                record["daemon_log_offset_start"], daemon_log.stat().st_size
            )
            self.assertEqual(
                record["daemon_log_offset_end"], daemon_log.stat().st_size
            )
            self.assertEqual(sleep.call_count, 2)
            self.assertEqual(urlopen.call_count, 3)
            for call in urlopen.call_args_list:
                request = call.args[0]
                self.assertEqual(request.get_method(), "POST")
                self.assertEqual(
                    request.full_url,
                    "http://127.0.0.1:6060/snapshot",
                )
                self.assertEqual(json.loads(request.data.decode("utf-8")), {})

            self.assertEqual(
                BENCH.read_jsonl(run_dir / "shutdown-samples.jsonl"),
                [record],
            )
            execution = self.build_execution(
                run_dir,
                passive_canary_enabled=False,
                shutdown_snapshot_quiesce_required=True,
            )
            comparable, reasons = BENCH.evaluate_ab_comparability(
                self.clean_initial_state(), execution
            )
            self.assertTrue(comparable)
            self.assertEqual(reasons, [])
            self.assertEqual(execution["shutdown_snapshot_quiesce_count"], 1)
            self.assertEqual(execution["shutdown_snapshot_quiesce_ok"], 1)

    def test_snapshot_quiesce_failure_and_missing_record_gate_ab(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            with mock.patch.object(
                BENCH,
                "post_json",
                side_effect=OSError("snapshot endpoint unavailable"),
            ):
                record = BENCH.record_shutdown_snapshot_quiesce(
                    "http://127.0.0.1:6060",
                    run_dir / "shutdown-samples.jsonl",
                    started_at=time.monotonic(),
                    timeout_secs=1.0,
                )
            self.assertFalse(record["ok"])
            self.assertFalse(record["ready"])
            self.assertIn("snapshot endpoint unavailable", record["error"])

            failed = self.build_execution(
                run_dir,
                passive_canary_enabled=False,
                shutdown_snapshot_quiesce_required=True,
            )
            comparable, reasons = BENCH.evaluate_ab_comparability(
                self.clean_initial_state(), failed
            )
            self.assertFalse(comparable)
            self.assertEqual(reasons, ["shutdown_snapshot_quiesce_failed"])

        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            missing = self.build_execution(
                run_dir,
                passive_canary_enabled=False,
                shutdown_snapshot_quiesce_required=True,
            )
            comparable, reasons = BENCH.evaluate_ab_comparability(
                self.clean_initial_state(), missing
            )
            self.assertFalse(comparable)
            self.assertEqual(reasons, ["shutdown_snapshot_quiesce_missing"])

    def test_snapshot_quiesce_summary_captures_rebuild_cost_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            write_jsonl(
                run_dir / "shutdown-samples.jsonl",
                [
                    {
                        "operation": "shutdown_snapshot_quiesce",
                        "ok": True,
                        "ready": True,
                        "written": True,
                        "rebuild_observed": True,
                        "attempts": 3,
                        "not_ready_responses": 2,
                        "started_elapsed_secs": 100.0,
                        "elapsed_secs": 104.0,
                        "latency_secs": 4.0,
                        "daemon_log_window_valid": True,
                        "daemon_log_offset_start": 120,
                        "daemon_log_offset_end": 360,
                    }
                ],
            )
            write_jsonl(
                run_dir / "process-samples.jsonl",
                [
                    {"elapsed_secs": 99.0, "cpu_pct": 1.0, "vmrss_bytes": 10},
                    {"elapsed_secs": 101.0, "cpu_pct": 25.0, "vmrss_bytes": 30},
                    {"elapsed_secs": 103.0, "cpu_pct": 50.0, "vmrss_bytes": 40},
                    {"elapsed_secs": 105.0, "cpu_pct": 2.0, "vmrss_bytes": 20},
                ],
            )

            summary = BENCH.summarize(run_dir, "snapshot-quiesce", 0)
            quiesce = summary["shutdown_snapshot_quiesce"]
            self.assertEqual(quiesce["count"], 1)
            self.assertEqual(quiesce["ok"], 1)
            self.assertTrue(quiesce["ready"])
            self.assertTrue(quiesce["written"])
            self.assertTrue(quiesce["rebuild_observed"])
            self.assertTrue(quiesce["daemon_log_window_valid"])
            self.assertEqual(quiesce["daemon_log_offset_start"], 120)
            self.assertEqual(quiesce["daemon_log_offset_end"], 360)
            self.assertEqual(quiesce["attempts_max"], 3)
            self.assertEqual(quiesce["latency_max_secs"], 4.0)
            self.assertEqual(quiesce["process_sample_count"], 2)
            self.assertEqual(quiesce["cpu_pct_max"], 50.0)
            self.assertEqual(quiesce["rss_bytes_max"], 40)


class ManifestAuditTests(unittest.TestCase):
    def test_fixture_identity_reads_existing_marker_without_walking_tree(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "fixture"
            nested = root / "nested"
            nested.mkdir(parents=True)
            (root / ".fd-rdd-m2-fixture").write_text(
                "fd-rdd m2 realistic fixture\nseed=42\ntotal_files=1000000\n",
                encoding="utf-8",
            )

            with mock.patch.object(
                BENCH.os,
                "walk",
                side_effect=AssertionError("fixture tree must not be walked"),
            ):
                identity = BENCH.load_fixture_identity([root, nested])

            self.assertEqual(identity["file_count"], 1_000_000)
            self.assertEqual(identity["count_source"], "fixture_marker_declared")
            self.assertTrue(identity["trusted"])
            self.assertFalse(identity["count_verified"])
            self.assertEqual(identity["seed"], 42)

    def test_legacy_fixture_declaration_is_not_ab_comparable(self) -> None:
        initial = {
            "fixture": {
                "trusted": True,
                "count_verified": False,
                "count_source": "fixture_marker_declared",
            },
            "snapshot": {"fresh": True},
            "binary_sha256": "a" * 64,
            "git_sha": "b" * 40,
            "git_dirty": False,
            "artifact_provenance": {
                "verified": True,
                "validated_binary_sha256": "a" * 64,
                "execution_binary_sha256": "a" * 64,
            },
            "run_dir_preexisting": False,
            "collection_errors": [],
        }

        comparable, reasons = BENCH.evaluate_ab_comparability(initial)

        self.assertFalse(comparable)
        self.assertEqual(reasons, ["fixture_count_not_verified"])

    def test_fixture_without_manifest_is_unverified_without_tree_walk(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(
            BENCH.os,
            "walk",
            side_effect=AssertionError("fixture tree must not be walked"),
        ):
            identity = BENCH.load_fixture_identity([Path(tmp)])

        self.assertIsNone(identity["file_count"])
        self.assertEqual(identity["count_source"], "unverified")
        self.assertFalse(identity["trusted"])

    def test_completed_json_fixture_manifest_takes_priority_over_legacy_marker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / ".fd-rdd-m2-fixture").write_text(
                "fd-rdd m2 realistic fixture\nseed=1\ntotal_files=10\n",
                encoding="utf-8",
            )
            (root / ".fd-rdd-m2-fixture.json").write_text(
                json.dumps(
                    {
                        "completed": True,
                        "actual_file_count": 11,
                        "seed": 42,
                        "layout_version": 2,
                    }
                ),
                encoding="utf-8",
            )

            identity = BENCH.load_fixture_identity([root])

            self.assertEqual(identity["file_count"], 11)
            self.assertEqual(identity["count_source"], "fixture_manifest")
            self.assertTrue(identity["count_verified"])

    def test_ab_fingerprint_ignores_run_identity_but_covers_workload_and_scale(self) -> None:
        args = Namespace(
            repo="/checkout/a",
            binary="/checkout/a/target/release/fd-rdd",
            build="never",
            run_label="a",
            run_dir="/reports/a",
            port=6060,
            root=["/fixture"],
            duration_secs=3600,
            sample_interval_secs=2.0,
            process_sample_interval_secs=0.5,
            event_storm_ops=100,
            mixed_workload=True,
            workload_seed=42,
        )
        first_params = BENCH.manifest_runner_args(args)
        first = BENCH.ab_parameter_fingerprint(first_params)

        args.repo = "/checkout/b"
        args.binary = "/checkout/b/target/release/fd-rdd"
        args.run_label = "b"
        args.run_dir = "/reports/b"
        args.port = 6061
        second_params = BENCH.manifest_runner_args(args)
        second = BENCH.ab_parameter_fingerprint(second_params)
        self.assertEqual(first, second)

        args.event_storm_ops = 500
        changed_workload = BENCH.ab_parameter_fingerprint(BENCH.manifest_runner_args(args))
        self.assertNotEqual(first, changed_workload)
        args.event_storm_ops = 100
        args.workload_seed = 43
        self.assertNotEqual(
            first,
            BENCH.ab_parameter_fingerprint(BENCH.manifest_runner_args(args)),
        )

    def test_initial_state_fingerprint_covers_snapshot_rust_log_and_fixture_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            binary = root / "fd-rdd"
            binary.write_bytes(b"binary-v1")
            snapshot = root / "index.db"
            fixture = {
                "file_count": 1_000_000,
                "count_source": "fixture_marker_declared",
                "trusted": True,
                "identity_sha256": "a" * 64,
            }
            clean_state = BENCH.build_initial_state(
                snapshot,
                fixture,
                rust_log="info",
                binary=binary,
                git_sha="b" * 40,
                git_dirty=False,
                artifact_provenance={"verified": True},
            )
            clean_fingerprint = BENCH.initial_state_fingerprint(clean_state)
            self.assertTrue(clean_state["snapshot"]["fresh"])
            self.assertEqual(clean_state["binary_sha256"], hashlib.sha256(b"binary-v1").hexdigest())

            snapshot.write_bytes(b"old snapshot")
            stale_state = BENCH.build_initial_state(
                snapshot,
                fixture,
                rust_log="debug",
                binary=binary,
                git_sha="b" * 40,
                git_dirty=False,
                artifact_provenance={"verified": True},
            )
            self.assertFalse(stale_state["snapshot"]["fresh"])
            self.assertNotEqual(
                clean_fingerprint,
                BENCH.initial_state_fingerprint(stale_state),
            )

    def test_execution_audit_and_comparability_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            write_jsonl(
                run_dir / "event-storm-samples.jsonl",
                [
                    {"event_kind": "burst_written", "events_total": 10},
                    {"event_kind": "burst_write_failed", "ok": False},
                    {
                        "event_kind": "burst_cleanup",
                        "entries_estimated": 12,
                        "duration_secs": 0.25,
                        "ok": False,
                    },
                ],
            )
            execution = BENCH.build_execution_state(
                run_dir,
                requested_duration_secs=3600,
                actual_duration_secs=120.0,
                exit_code=0,
                fatal_error="",
                process_sampler_error="",
                completion_reason="interrupted",
                cleanup_errors=[],
            )
            initial = {
                "fixture": {"trusted": False, "count_source": "unverified"},
                "snapshot": {"fresh": False},
                "binary_sha256": "",
                "git_sha": "",
                "git_dirty": True,
                "artifact_provenance": {"verified": False},
                "collection_errors": [],
            }

            comparable, reasons = BENCH.evaluate_ab_comparability(initial, execution)

            self.assertFalse(comparable)
            self.assertIn("fixture_count_unverified", reasons)
            self.assertIn("snapshot_preexisting", reasons)
            self.assertIn("run_interrupted", reasons)
            self.assertIn("event_storm_write_failed", reasons)
            self.assertIn("event_storm_cleanup_failed", reasons)
            self.assertEqual(execution["event_storm_cleanups"], 1)
            self.assertEqual(execution["event_storm_cleanup_failures"], 1)
            self.assertEqual(execution["event_storm_cleanup_entries_estimated"], 12)
            self.assertEqual(execution["event_storm_cleanup_duration_secs"], 0.25)
            self.assertTrue(execution["fingerprint"])

    def test_comparability_accepts_clean_fixed_duration_run(self) -> None:
        initial = {
            "fixture": {
                "trusted": True,
                "count_verified": True,
                "count_source": "fixture_manifest",
            },
            "snapshot": {"fresh": True},
            "binary_sha256": "a" * 64,
            "git_sha": "b" * 40,
            "git_dirty": False,
            "artifact_provenance": {
                "verified": True,
                "validated_binary_sha256": "a" * 64,
                "execution_binary_sha256": "a" * 64,
            },
            "run_dir_preexisting": False,
            "collection_errors": [],
        }
        execution = {
            "requested_duration_secs": 3600,
            "duration_completed": True,
            "completion_reason": "duration_elapsed",
            "exit_code": 0,
            "fatal_error": "",
            "process_sampler_error": "",
            "cleanup_errors": [],
            "shutdown_signal_elapsed_secs": 3600.5,
            "event_storm_enabled": True,
            "event_storm_bursts": 8,
            "event_storm_write_failures": 0,
            "event_storm_cleanup_failures": 0,
            "unsupported_workloads": 0,
            "mixed_workload_enabled": True,
            "hot_churn_batches": 100,
            "hot_churn_errors": 0,
            "endpoint_sample_failures": 0,
            "process_sample_count": 7200,
            "memory_endpoint_sample_count": 1800,
            "final_snapshot_failed": False,
        }

        comparable, reasons = BENCH.evaluate_ab_comparability(initial, execution)

        self.assertTrue(comparable)
        self.assertEqual(reasons, [])

    def test_existing_auto_binary_has_no_source_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            binary = repo / "target" / "release" / "fd-rdd"
            binary.parent.mkdir(parents=True)
            binary.write_bytes(b"stale")
            (repo / "Cargo.lock").write_text("lock", encoding="utf-8")

            provenance = BENCH.build_if_needed(repo, binary, "auto", "a" * 40)

            self.assertFalse(provenance["verified"])
            self.assertFalse(provenance["built_this_run"])

    def test_never_build_accepts_a_matching_external_build_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            binary = repo / "target" / "release" / "fd-rdd"
            binary.parent.mkdir(parents=True)
            binary.write_bytes(b"candidate")
            cargo_lock = repo / "Cargo.lock"
            cargo_lock.write_text("lock", encoding="utf-8")
            receipt = repo / "build-provenance.json"
            receipt.write_text(
                json.dumps(
                    {
                        "schema": 3,
                        "build_succeeded": True,
                        "source_git_sha": "a" * 40,
                        "pre_build_git_sha": "a" * 40,
                        "post_build_git_sha": "a" * 40,
                        "build_worktree_clean": True,
                        "cargo_lock_sha256": BENCH.sha256_file(cargo_lock),
                        "binary_sha256": BENCH.sha256_file(binary),
                        "binary": str(binary.resolve()),
                        "compiler_artifact": str(binary.resolve()),
                        "cargo_args": [
                            *BENCH.BUILD_CARGO_ARGS,
                            "--target-dir",
                            str(binary.parent.parent.resolve()),
                        ],
                    }
                ),
                encoding="utf-8",
            )

            with mock.patch.object(
                BENCH, "git_worktree_dirty", return_value=False
            ):
                provenance = BENCH.build_if_needed(
                    repo,
                    binary,
                    "never",
                    "a" * 40,
                    receipt,
                )

            self.assertTrue(provenance["verified"])
            self.assertFalse(provenance["built_this_run"])
            self.assertEqual(provenance["receipt_validation_errors"], [])
            self.assertEqual(provenance["receipt_path"], str(receipt.resolve()))

    def test_never_build_rejects_a_receipt_when_current_worktree_is_dirty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            binary = repo / "target" / "release" / "fd-rdd"
            binary.parent.mkdir(parents=True)
            binary.write_bytes(b"candidate")
            cargo_lock = repo / "Cargo.lock"
            cargo_lock.write_text("lock", encoding="utf-8")
            receipt = repo / "build-provenance.json"
            receipt.write_text(
                json.dumps(
                    {
                        "schema": 3,
                        "build_succeeded": True,
                        "source_git_sha": "a" * 40,
                        "pre_build_git_sha": "a" * 40,
                        "post_build_git_sha": "a" * 40,
                        "build_worktree_clean": True,
                        "cargo_lock_sha256": BENCH.sha256_file(cargo_lock),
                        "binary_sha256": BENCH.sha256_file(binary),
                        "binary": str(binary.resolve()),
                        "compiler_artifact": str(binary.resolve()),
                        "cargo_args": [
                            *BENCH.BUILD_CARGO_ARGS,
                            "--target-dir",
                            str(binary.parent.parent.resolve()),
                        ],
                    }
                ),
                encoding="utf-8",
            )

            with mock.patch.object(BENCH, "git_worktree_dirty", return_value=True):
                provenance = BENCH.build_if_needed(
                    repo,
                    binary,
                    "never",
                    "a" * 40,
                    receipt,
                )

            self.assertFalse(provenance["verified"])
            self.assertIn(
                "current_worktree_dirty", provenance["receipt_validation_errors"]
            )

    def test_never_build_rejects_a_receipt_after_binary_changes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            binary = repo / "target" / "release" / "fd-rdd"
            binary.parent.mkdir(parents=True)
            binary.write_bytes(b"candidate")
            cargo_lock = repo / "Cargo.lock"
            cargo_lock.write_text("lock", encoding="utf-8")
            receipt = repo / "build-provenance.json"
            receipt.write_text(
                json.dumps(
                    {
                        "schema": 3,
                        "build_succeeded": True,
                        "source_git_sha": "a" * 40,
                        "pre_build_git_sha": "a" * 40,
                        "post_build_git_sha": "a" * 40,
                        "build_worktree_clean": True,
                        "cargo_lock_sha256": BENCH.sha256_file(cargo_lock),
                        "binary_sha256": BENCH.sha256_file(binary),
                        "binary": str(binary.resolve()),
                        "compiler_artifact": str(binary.resolve()),
                        "cargo_args": [
                            *BENCH.BUILD_CARGO_ARGS,
                            "--target-dir",
                            str(binary.parent.parent.resolve()),
                        ],
                    }
                ),
                encoding="utf-8",
            )
            binary.write_bytes(b"changed")

            provenance = BENCH.build_if_needed(
                repo,
                binary,
                "never",
                "a" * 40,
                receipt,
            )

            self.assertFalse(provenance["verified"])
            self.assertIn("binary_sha256_mismatch", provenance["receipt_validation_errors"])

    def test_always_build_records_source_to_artifact_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(
            BENCH.subprocess, "run"
        ) as run, mock.patch.object(
            BENCH, "git_head_sha", return_value="a" * 40
        ), mock.patch.object(
            BENCH, "git_worktree_dirty", return_value=False
        ):
            repo = Path(tmp)
            binary = repo / "target" / "release" / "fd-rdd"
            binary.parent.mkdir(parents=True)
            binary.write_bytes(b"candidate")
            (repo / "Cargo.lock").write_text("lock", encoding="utf-8")
            run.return_value.stdout = json.dumps(
                {
                    "reason": "compiler-artifact",
                    "target": {"name": "fd-rdd", "kind": ["bin"]},
                    "executable": str(binary.resolve()),
                }
            )

            provenance = BENCH.build_if_needed(repo, binary, "always", "a" * 40)

            self.assertTrue(provenance["verified"])
            self.assertTrue(provenance["built_this_run"])
            run.assert_called_once_with(
                BENCH.BUILD_CARGO_ARGS,
                cwd=repo,
                check=True,
                stdout=BENCH.subprocess.PIPE,
                text=True,
            )

    def test_stage_execution_binary_rejects_a_receipt_hash_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            binary = root / "source" / "fd-rdd"
            binary.parent.mkdir()
            binary.write_bytes(b"candidate")

            with self.assertRaisesRegex(
                RuntimeError, "artifact_binary_identity_changed_before_staging"
            ):
                BENCH.stage_execution_binary(binary, root / "run", "0" * 64)

    def test_stage_execution_binary_creates_a_private_read_only_copy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            binary = root / "source" / "fd-rdd"
            binary.parent.mkdir()
            binary.write_bytes(b"candidate")
            expected = BENCH.sha256_file(binary)

            staged, staged_sha = BENCH.stage_execution_binary(
                binary,
                root / "run",
                expected,
            )

            self.assertEqual(staged, root / "run" / "artifact" / "fd-rdd")
            self.assertEqual(staged_sha, expected)
            self.assertEqual(BENCH.sha256_file(staged), expected)
            self.assertEqual(staged.stat().st_mode & 0o777, 0o500)

    def test_atomic_manifest_update_leaves_no_partial_next_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "manifest.json"

            BENCH.atomic_write_json(path, {"run_state": "preparing"})
            BENCH.atomic_write_json(path, {"run_state": "failed"})

            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), {"run_state": "failed"})
            self.assertFalse(path.with_name("manifest.json.next").exists())

    def test_process_start_failure_is_persisted_in_early_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "fixture"
            root.mkdir()
            (root / ".fd-rdd-m2-fixture").write_text(
                "fd-rdd m2 realistic fixture\nseed=42\ntotal_files=10\n",
                encoding="utf-8",
            )
            binary = Path(tmp) / "fd-rdd"
            binary.write_bytes(b"binary")
            run_dir = Path(tmp) / "run"
            argv = [
                str(SCRIPT_PATH),
                "--root",
                str(root),
                "--repo",
                str(SCRIPT_PATH.parents[1]),
                "--binary",
                str(binary),
                "--build",
                "never",
                "--run-dir",
                str(run_dir),
                "--duration-secs",
                "1",
            ]
            with mock.patch.object(sys, "argv", argv):
                args = BENCH.parse_args()
            with (
                mock.patch.object(BENCH, "port_is_free", return_value=True),
                mock.patch.object(BENCH, "git_head_sha", return_value="a" * 40),
                mock.patch.object(BENCH, "git_worktree_dirty", return_value=False),
                mock.patch.object(
                    BENCH.subprocess,
                    "Popen",
                    side_effect=OSError("cannot execute benchmark daemon"),
                ),
            ):
                with self.assertRaisesRegex(OSError, "cannot execute"):
                    BENCH.run_single(args)

            manifest = json.loads(
                (run_dir / "manifest.json").read_text(encoding="utf-8")
            )
            self.assertEqual(manifest["run_state"], "failed")
            self.assertEqual(manifest["failure_stage"], "starting_daemon")
            self.assertEqual(manifest["completion_reason"], "startup_failed")
            self.assertIn("cannot execute", manifest["fatal_error"])

    def test_git_head_sha_records_the_exact_checkout(self) -> None:
        sha = BENCH.git_head_sha(SCRIPT_PATH.parents[1])
        self.assertRegex(sha, r"^[0-9a-f]{40}$")


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


class DaemonStartupTests(unittest.TestCase):
    def test_start_daemon_rechecks_port_immediately_before_spawn(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            events: list[tuple[str, object]] = []

            def port_is_free(port: int) -> bool:
                events.append(("port_check", port))
                return True

            def popen(*args: object, **kwargs: object) -> object:
                events.append(("popen", args[0]))
                raise OSError("spawn stopped for ordering assertion")

            with (
                mock.patch.object(BENCH, "port_is_free", side_effect=port_is_free),
                mock.patch.object(BENCH.subprocess, "Popen", side_effect=popen),
            ):
                with self.assertRaisesRegex(OSError, "ordering assertion"):
                    BENCH._start_daemon_process(
                        ["fd-rdd", "--http-port", "45680"],
                        port=45680,
                        run_dir=Path(tmp),
                        env={},
                        process_sample_interval_secs=0.5,
                    )

            self.assertEqual(
                events,
                [
                    ("port_check", 45680),
                    ("popen", ["fd-rdd", "--http-port", "45680"]),
                ],
            )

    def test_start_daemon_does_not_spawn_when_final_port_check_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(
            BENCH, "port_is_free", return_value=False
        ), mock.patch.object(BENCH.subprocess, "Popen") as popen:
            with self.assertRaisesRegex(SystemExit, "127.0.0.1:45680.*already in use"):
                BENCH._start_daemon_process(
                    ["fd-rdd", "--http-port", "45680"],
                    port=45680,
                    run_dir=Path(tmp),
                    env={},
                    process_sample_interval_secs=0.5,
                )

            popen.assert_not_called()

    def test_wait_for_http_requires_current_daemon_listening_log(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            daemon_log = Path(tmp) / "fd-rdd.log"
            daemon_log.write_text(
                "INFO HTTP Query Server listening on port 45679\n",
                encoding="utf-8",
            )
            process = mock.Mock()
            process.poll.return_value = None

            with mock.patch.object(
                BENCH, "http_json", return_value={"ok": True}
            ) as http_json:
                with self.assertRaisesRegex(
                    RuntimeError,
                    "timed out.*HTTP Query Server listening on port 45680",
                ):
                    BENCH.wait_for_http(
                        "http://127.0.0.1:45680",
                        0,
                        process=process,
                        daemon_log_path=daemon_log,
                        expected_port=45680,
                    )

            http_json.assert_not_called()

    def test_wait_for_http_accepts_health_only_after_log_proves_ownership(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            daemon_log = Path(tmp) / "fd-rdd.log"
            daemon_log.write_text(
                "INFO HTTP Query Server listening on port 45680\n",
                encoding="utf-8",
            )
            process = mock.Mock()
            process.poll.return_value = None

            with mock.patch.object(BENCH, "http_json", return_value={"ok": True}):
                BENCH.wait_for_http(
                    "http://127.0.0.1:45680",
                    1,
                    process=process,
                    daemon_log_path=daemon_log,
                    expected_port=45680,
                )

    def test_wait_for_http_fails_immediately_on_query_server_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            daemon_log = Path(tmp) / "fd-rdd.log"
            daemon_log.write_text(
                "ERROR Query server error: Address already in use (os error 98)\n",
                encoding="utf-8",
            )
            process = mock.Mock()
            process.poll.return_value = None

            with mock.patch.object(BENCH, "http_json") as http_json:
                with self.assertRaisesRegex(
                    RuntimeError, "query server startup failed.*Address already in use"
                ):
                    BENCH.wait_for_http(
                        "http://127.0.0.1:45680",
                        1,
                        process=process,
                        daemon_log_path=daemon_log,
                        expected_port=45680,
                    )

            http_json.assert_not_called()

    def test_wait_for_http_fails_immediately_when_daemon_exits(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            daemon_log = Path(tmp) / "fd-rdd.log"
            daemon_log.write_text("daemon initialization\n", encoding="utf-8")
            process = mock.Mock()
            process.poll.return_value = 23

            with mock.patch.object(BENCH, "http_json") as http_json:
                with self.assertRaisesRegex(RuntimeError, "exited.*code 23"):
                    BENCH.wait_for_http(
                        "http://127.0.0.1:45680",
                        1,
                        process=process,
                        daemon_log_path=daemon_log,
                        expected_port=45680,
                    )

            http_json.assert_not_called()


class ProcessSampleRunnerTests(unittest.TestCase):
    def test_process_sampler_includes_io_and_fault_counters(self) -> None:
        sample = next(BENCH.process_sampler(os.getpid()))

        for key in (
            "read_bytes",
            "write_bytes",
            "read_syscalls",
            "write_syscalls",
            "cpu_ticks",
            "minor_faults",
            "major_faults",
        ):
            self.assertIn(key, sample)
            self.assertGreaterEqual(sample[key], 0)

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

    def test_process_sampler_ignores_procfs_exit_races_after_daemon_exit(self) -> None:
        for error in (
            FileNotFoundError(2, "not found"),
            PermissionError(13, "permission denied"),
        ):
            with self.subTest(error=type(error).__name__):
                def failed_sampler(_pid: int):
                    raise error
                    yield {}  # pragma: no cover - makes this a generator

                with tempfile.TemporaryDirectory() as tmp, mock.patch.object(
                    BENCH, "process_sampler", failed_sampler
                ):
                    runner = BENCH.ProcessSampleRunner(
                        999_999,
                        Path(tmp) / "process-samples.jsonl",
                        time.monotonic(),
                        0.01,
                        process_running=lambda: False,
                    )
                    runner.start()
                    time.sleep(0.03)
                    runner.stop()

                    self.assertEqual(runner.error, "")

    def test_process_sampler_keeps_live_procfs_permission_errors_fatal(self) -> None:
        def failed_sampler(_pid: int):
            raise PermissionError(13, "permission denied")
            yield {}  # pragma: no cover - makes this a generator

        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(
            BENCH, "process_sampler", failed_sampler
        ):
            runner = BENCH.ProcessSampleRunner(
                999_999,
                Path(tmp) / "process-samples.jsonl",
                time.monotonic(),
                0.01,
                process_running=lambda: True,
            )
            runner.start()
            time.sleep(0.03)
            runner.stop()

            self.assertIn("PermissionError", runner.error)


class EventStormFixtureTests(unittest.TestCase):
    @staticmethod
    def event_storm_runner(roots: list[Path]) -> BENCH.EventStormRunner:
        return BENCH.EventStormRunner(
            base_url="http://127.0.0.1:1",
            roots=roots,
            out_path=roots[0] / "events.jsonl",
            started_at=time.monotonic(),
            start_delay_secs=0,
            interval_secs=1,
            settle_secs=0,
            timeout_secs=0,
            ops_per_burst=10,
            duration_budget_secs=10,
            time_skew_secs=3600,
            kinds=["rw100"],
            target_tiers=["L0", "L1", "L2", "L3"],
        )

    def test_post_cleanup_audit_cli_option_is_explicit_and_disabled_by_default(self) -> None:
        argv = [str(SCRIPT_PATH), "--root", "/tmp/probe-root"]
        with mock.patch.object(sys, "argv", argv):
            defaults = BENCH.parse_args()
        self.assertEqual(defaults.event_storm_post_cleanup_audit_secs, 0.0)
        self.assertEqual(defaults.event_storm_precondition_wait_secs, 0.0)
        self.assertEqual(defaults.event_storm_min_lease_remaining_secs, 0.0)

        argv.extend(
            (
                "--event-storm-post-cleanup-audit-secs",
                "22",
                "--event-storm-precondition-wait-secs",
                "60",
                "--event-storm-min-lease-remaining-secs",
                "18",
            )
        )
        with mock.patch.object(sys, "argv", argv):
            enabled = BENCH.parse_args()
        self.assertEqual(enabled.event_storm_post_cleanup_audit_secs, 22.0)
        self.assertEqual(enabled.event_storm_precondition_wait_secs, 60.0)
        self.assertEqual(enabled.event_storm_min_lease_remaining_secs, 18.0)

    def test_strict_protocol_waits_for_a_fresh_l3_lease_before_failing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            runner = BENCH.EventStormRunner(
                base_url="http://127.0.0.1:1",
                roots=[root],
                out_path=root / "events.jsonl",
                started_at=90.0,
                start_delay_secs=0,
                interval_secs=1,
                settle_secs=0,
                timeout_secs=0,
                ops_per_burst=10,
                duration_budget_secs=10,
                time_skew_secs=3600,
                kinds=["subtree_rename"],
                target_tiers=["L3"],
                strict_protocol=True,
                treatment_enabled=True,
                precondition_wait_secs=10,
                min_lease_remaining_secs=18,
            )
            evidence = {
                "target_m2_debug_ok": True,
                "target_m2_entry_present": True,
                "target_m2_active": False,
                "target_m2_action": "",
                "target_m2_observed_unix_secs": 100,
                "target_m2_expires_unix_secs": 0,
            }
            with (
                mock.patch.object(runner, "tier_for_root", return_value="L2"),
                mock.patch.object(
                    runner, "m2_evidence_for_root", return_value=evidence
                ),
            ):
                runner.start_cycle(100.0)
                self.assertEqual(runner.protocol_error, "")
                self.assertEqual(runner.cycle, 0)
                self.assertGreater(runner.next_start_at, 100.0)

                runner.start_cycle(111.0)

            self.assertIn("requested L3 but observed L2", runner.protocol_error)
            self.assertEqual(runner.cycle, 1)

    def test_strict_protocol_precondition_wait_budget_is_shared_across_bursts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            runner = self.event_storm_runner([root])
            runner.precondition_wait_secs = 10.0
            evidence: dict[str, object] = {}

            self.assertTrue(
                runner.defer_protocol_precondition(
                    100.0, root, "L3", "subtree_rename", "L2", "not ready", evidence
                )
            )
            self.assertTrue(
                runner.defer_protocol_precondition(
                    106.0, root, "L3", "subtree_rename", "L2", "not ready", evidence
                )
            )
            runner.finish_precondition_wait(106.0)

            self.assertTrue(
                runner.defer_protocol_precondition(
                    200.0, root, "L3", "subtree_rename", "L2", "not ready", evidence
                )
            )
            self.assertFalse(
                runner.defer_protocol_precondition(
                    204.0, root, "L3", "subtree_rename", "L2", "not ready", evidence
                )
            )

    def test_strict_protocol_requires_configured_lease_remaining_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            runner = self.event_storm_runner([root])
            runner.strict_protocol = True
            runner.treatment_enabled = True
            runner.min_lease_remaining_secs = 18.0
            evidence = {
                "target_m2_debug_ok": True,
                "target_m2_entry_present": True,
                "target_m2_active": True,
                "target_m2_action": "scan_only",
                "target_m2_observed_unix_secs": 100,
                "target_m2_expires_unix_secs": 117,
            }

            too_old = runner.protocol_precondition_error("L3", "L3", evidence)
            fresh = runner.protocol_precondition_error(
                "L3",
                "L3",
                {**evidence, "target_m2_expires_unix_secs": 118},
            )

            self.assertIn("remaining", too_old)
            self.assertEqual(fresh, "")

    def test_query_age_uses_each_query_completion_time(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            runner = self.event_storm_runner([root])
            runner.active = {
                "root": root,
                "requested_tier": "L3",
                "tier_before": "L3",
                "started_at": 100.0,
                "events": [
                    {
                        "operation": f"probe_{index}",
                        "workload": "subtree_rename",
                        "path": str(root / f"file-{index}.txt"),
                        "query": f"file-{index}.txt",
                        "should_exist": True,
                        "tier_before": "L3",
                        "burst_elapsed_secs": 0.5,
                    }
                    for index in range(2)
                ],
                "visibility_probes": [],
                "mutation_completed_unix_secs": 1,
            }
            after = {
                "target_m2_debug_ok": True,
                "target_m2_entry_present": True,
            }
            with (
                mock.patch.object(
                    BENCH,
                    "check_search_state_once",
                    side_effect=[
                        (True, True, 0.1, ""),
                        (True, True, 0.1, ""),
                    ],
                ),
                mock.patch.object(runner, "tier_for_root", return_value="L3"),
                mock.patch.object(
                    runner, "m2_evidence_for_root", return_value=after
                ),
                mock.patch.object(runner, "emit") as emit,
                mock.patch.object(
                    BENCH.time, "monotonic", side_effect=[106.5, 108.25]
                ),
            ):
                runner.run_query_pass(105.0, phase="delayed")

            queries = [
                call.args[0]
                for call in emit.call_args_list
                if call.args[0].get("event_kind") == "first_query"
            ]
            self.assertEqual([row["settle_secs"] for row in queries], [6.5, 8.25])
            self.assertEqual([row["event_age_secs"] for row in queries], [6.0, 7.75])

    def test_post_cleanup_audit_proves_expired_root_lease_and_watcher_ledger_cleared(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            runner = BENCH.EventStormRunner(
                base_url="http://127.0.0.1:1",
                roots=[root],
                out_path=root / "events.jsonl",
                started_at=time.monotonic(),
                start_delay_secs=0,
                interval_secs=1,
                settle_secs=0,
                timeout_secs=0,
                ops_per_burst=10,
                duration_budget_secs=10,
                time_skew_secs=3600,
                kinds=["subtree_rename"],
                target_tiers=["L3"],
                post_cleanup_audit_secs=5,
            )
            runner.cycle = 1
            runner.rotating_ttl_secs = 20
            runner.rotating_tick_secs = 45
            burst_root = runner.burst_root(root, "subtree-rename")
            (burst_root / "dir_b").mkdir(parents=True)
            runner.active = {
                "target_m2_fence": {
                    "target_m2_fence_expires_unix_secs": 100,
                }
            }

            with mock.patch.object(runner, "emit") as emit:
                runner.cleanup_burst_root(
                    burst_root,
                    root,
                    "subtree_rename",
                    phase="delayed_query",
                )
                self.assertEqual(len(runner.pending_cleanup_audits), 1)
                with mock.patch.object(
                    BENCH,
                    "debug_tiered_watch",
                    return_value={
                        "dirs": [
                            {
                                "path": str(root),
                                "ephemeral_watch": False,
                                "rotating_cold_window": False,
                                "rotating_cold_window_action": "",
                            }
                        ]
                    },
                ), mock.patch.object(BENCH.time, "time", return_value=110):
                    runner.poll_post_cleanup_audits(time.monotonic() + 10)

            record = emit.call_args_list[-1].args[0]
            self.assertEqual(record["event_kind"], "post_cleanup_audit")
            self.assertTrue(record["watcher_ledger_cleared"])
            self.assertFalse(record["cleanup_target_exists"])
            self.assertEqual(record["cleanup_target_entries"], 0)
            self.assertFalse(record["target_rotating_active"])
            self.assertFalse(record["target_ephemeral_watch"])
            self.assertTrue(record["audit_after_lease_expiry"])
            self.assertTrue(record["audit_before_next_rotation"])
            self.assertTrue(record["audit_window_valid"])
            self.assertEqual(runner.pending_cleanup_audits, [])

    def test_post_cleanup_audit_fails_when_deleted_subtree_keeps_active_watcher(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            runner = self.event_storm_runner([root])
            runner.post_cleanup_audit_secs = 5
            runner.cycle = 1
            burst_root = runner.burst_root(root, "subtree-rename")
            runner.pending_cleanup_audits = [
                {
                    "due_at": 1.0,
                    "selected_root": root,
                    "cleanup_target": burst_root,
                    "workload": "subtree_rename",
                    "cleanup_removed": True,
                }
            ]
            with mock.patch.object(
                BENCH,
                "debug_tiered_watch",
                return_value={
                    "dirs": [
                        {
                            "path": str(root),
                            "ephemeral_watch": False,
                            "rotating_cold_window": False,
                            "rotating_cold_window_action": "",
                        },
                        {
                            "path": str(burst_root / "dir_b"),
                            "ephemeral_watch": True,
                            "rotating_cold_window": False,
                        },
                    ]
                },
            ), mock.patch.object(runner, "emit") as emit:
                runner.poll_post_cleanup_audits(2.0)

            record = emit.call_args.args[0]
            self.assertFalse(record["ok"])
            self.assertFalse(record["watcher_ledger_cleared"])
            self.assertEqual(record["cleanup_target_ephemeral_watch_dirs"], 1)

    def test_select_root_never_reuses_event_storm_tree(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            runner = self.event_storm_runner([root])
            runner.cycle = 1
            current_storm = runner.burst_root(root, "rw100")
            current_descendant = current_storm / "nested"
            current_descendant.mkdir(parents=True)
            historical_descendant = (
                root
                / "fd-rdd-m2-event-storm-save100-previous-run-001"
                / "nested"
            )
            historical_descendant.mkdir(parents=True)
            runner.cycle = 2

            with mock.patch.object(
                BENCH,
                "debug_tiered_watch",
                return_value={
                    "dirs": [
                        {"path": str(current_storm), "watch_tier": "L2"},
                        {"path": str(current_descendant), "watch_tier": "L2"},
                        {"path": str(historical_descendant), "watch_tier": "L2"},
                    ]
                },
            ):
                selected = runner.select_root("L2")

            self.assertEqual(selected, root)
            self.assertNotEqual(selected, current_storm)
            self.assertNotEqual(selected, current_descendant)
            self.assertNotEqual(selected, historical_descendant)

    def test_max_bursts_stops_new_cycles_without_interrupting_active_cycle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            runner = self.event_storm_runner([root])
            runner.max_bursts = 1
            runner.cycle = 1
            runner.next_start_at = time.monotonic() - 1

            with mock.patch.object(runner, "start_cycle") as start_cycle:
                runner.tick(time.monotonic())

            start_cycle.assert_not_called()

    def test_fixed_root_schedule_ignores_treatment_tier_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            runner = self.event_storm_runner([root])
            runner.fixed_root_schedule = True

            with mock.patch.object(
                BENCH,
                "debug_tiered_watch",
                side_effect=AssertionError("fixed schedule must not inspect tier state"),
            ):
                runner.cycle = 1
                selected_first = runner.select_root("L3")
                runner.cycle = 2
                selected_second = runner.select_root("L3")

            self.assertEqual(selected_first, root)
            self.assertEqual(selected_second, root)

    def test_target_m2_evidence_is_bound_to_the_exact_selected_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            runner = self.event_storm_runner([root])
            with mock.patch.object(
                BENCH,
                "debug_tiered_watch",
                return_value={
                    "dirs": [
                        {
                            "path": str(root),
                            "rotating_cold_window_seen": True,
                            "rotating_cold_window": True,
                            "rotating_cold_window_action": "scan_only",
                            "rotating_cold_window_cycle_id": 7,
                            "rotating_cold_window_expires_unix_secs": 200,
                            "rotating_cold_window_last_scan_seq": 31,
                            "rotating_cold_window_last_scan_cycle_id": 7,
                            "rotating_cold_window_last_event_seq": 29,
                            "rotating_cold_window_last_event_cycle_id": 7,
                            "last_scan": 101,
                            "last_event": 102,
                        },
                        {
                            "path": str(root / "child"),
                            "rotating_cold_window_seen": False,
                        },
                    ]
                },
            ), mock.patch.object(BENCH.time, "time", return_value=150.25):
                evidence = runner.m2_evidence_for_root(root)

            self.assertEqual(
                evidence,
                {
                    "target_m2_debug_ok": True,
                    "target_m2_entry_present": True,
                    "target_m2_seen": True,
                    "target_m2_active": True,
                    "target_m2_action": "scan_only",
                    "target_m2_cycle_id": 7,
                    "target_m2_expires_unix_secs": 200,
                    "target_m2_last_scan_unix_secs": 101,
                    "target_m2_last_event_unix_secs": 102,
                    "target_m2_scan_seq": 31,
                    "target_m2_scan_cycle_id": 7,
                    "target_m2_event_seq": 29,
                    "target_m2_event_cycle_id": 7,
                    "target_m2_observed_unix_secs": 150,
                },
            )

    def test_burst_written_captures_a_post_mutation_m2_sequence_fence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            runner = self.event_storm_runner([root])
            before = {
                "target_m2_debug_ok": True,
                "target_m2_entry_present": True,
                "target_m2_active": True,
                "target_m2_action": "scan_only",
                "target_m2_cycle_id": 3,
                "target_m2_scan_seq": 10,
                "target_m2_event_seq": 4,
            }
            fence = {
                **before,
                "target_m2_scan_seq": 11,
                "target_m2_event_seq": 4,
            }
            emitted: list[dict[str, object]] = []
            with mock.patch.object(runner, "select_root", return_value=root), mock.patch.object(
                runner, "tier_for_root", return_value="L0"
            ), mock.patch.object(
                runner, "m2_evidence_for_root", side_effect=[before, fence]
            ), mock.patch.object(
                runner, "write_rw100", return_value=[]
            ), mock.patch.object(
                runner, "emit", side_effect=lambda row: emitted.append(dict(row))
            ):
                runner.start_cycle(time.monotonic())

            written = next(
                row for row in emitted if row.get("event_kind") == "burst_written"
            )
            self.assertEqual(written["target_m2_scan_seq"], 10)
            self.assertEqual(written["target_m2_fence_scan_seq"], 11)
            self.assertEqual(written["target_m2_fence_cycle_id"], 3)

    def test_tier_for_root_inherits_the_nearest_ancestor_not_a_sibling(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            fixture = Path(tmp).resolve()
            cold_a = fixture / "cold-a"
            selected = cold_a / "d040"
            selected.mkdir(parents=True)
            runner = self.event_storm_runner([cold_a])

            with mock.patch.object(
                BENCH,
                "debug_tiered_watch",
                return_value={
                    "dirs": [
                        {"path": str(fixture), "watch_tier": "L2"},
                        {"path": str(cold_a), "watch_tier": "L3"},
                        {"path": str(fixture / "cold-b"), "watch_tier": "L1"},
                    ]
                },
            ) as debug:
                tier = runner.tier_for_root(selected)

            self.assertEqual(tier, "L3")
            debug.assert_called_once_with(runner.base_url, cold_a)

    def test_tier_for_root_does_not_infer_a_parent_tier_from_a_descendant(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cold_a = Path(tmp).resolve() / "cold-a"
            cold_a.mkdir()
            runner = self.event_storm_runner([cold_a])

            with mock.patch.object(
                BENCH,
                "debug_tiered_watch",
                return_value={
                    "dirs": [
                        {"path": str(cold_a / "deep"), "watch_tier": "L3"},
                    ]
                },
            ):
                tier = runner.tier_for_root(cold_a)

            self.assertEqual(tier, "")

    def test_missing_exact_m2_entry_is_valid_negative_debug_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cold_a = Path(tmp).resolve() / "cold-a"
            selected = cold_a / "d040"
            selected.mkdir(parents=True)
            runner = self.event_storm_runner([cold_a])

            with mock.patch.object(
                BENCH,
                "debug_tiered_watch",
                return_value={
                    "dirs": [
                        {"path": str(cold_a), "watch_tier": "L3"},
                    ]
                },
            ), mock.patch.object(BENCH.time, "time", return_value=150.25):
                evidence = runner.m2_evidence_for_root(selected)

            self.assertEqual(
                evidence,
                {
                    "target_m2_debug_ok": True,
                    "target_m2_entry_present": False,
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
                    "target_m2_observed_unix_secs": 150,
                },
            )

            runner.strict_protocol = True
            runner.treatment_enabled = False
            self.assertEqual(
                runner.protocol_precondition_error("L3", "L3", evidence),
                "",
            )

    def test_strict_protocol_stops_before_mutation_when_tier_is_unproven(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            runner = self.event_storm_runner([root])
            runner.strict_protocol = True
            runner.treatment_enabled = True

            with mock.patch.object(
                runner, "tier_for_root", return_value=""
            ), mock.patch.object(
                runner,
                "m2_evidence_for_root",
                return_value={
                    "target_m2_debug_ok": False,
                    "target_m2_entry_present": False,
                    "target_m2_seen": False,
                    "target_m2_active": False,
                    "target_m2_action": "",
                    "target_m2_cycle_id": 0,
                    "target_m2_expires_unix_secs": 0,
                    "target_m2_last_scan_unix_secs": 0,
                    "target_m2_last_event_unix_secs": 0,
                    "target_m2_observed_unix_secs": 150,
                },
            ), mock.patch.object(runner, "write_rw100") as write:
                runner.start_cycle(time.monotonic())

            self.assertIn("requested L0 but observed unknown", runner.protocol_error)
            write.assert_not_called()

    def test_strict_protocol_does_not_create_a_missing_fixture_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve() / "missing"
            runner = self.event_storm_runner([root])
            runner.out_path = Path(tmp) / "events.jsonl"
            runner.strict_protocol = True

            with mock.patch.object(
                runner,
                "_emit_tier_distribution",
            ), mock.patch.object(
                runner,
                "select_root",
                return_value=root,
            ), mock.patch.object(runner, "write_rw100") as write:
                runner.start_cycle(time.monotonic())

            self.assertFalse(root.exists())
            self.assertIn("selected fixture root does not exist", runner.protocol_error)
            write.assert_not_called()

    def test_strict_protocol_rejects_a_lease_at_its_expiry_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            runner = self.event_storm_runner([root])
            runner.strict_protocol = True
            runner.treatment_enabled = True
            evidence = {
                "target_m2_debug_ok": True,
                "target_m2_entry_present": True,
                "target_m2_seen": True,
                "target_m2_active": True,
                "target_m2_action": "scan_only",
                "target_m2_cycle_id": 7,
                "target_m2_expires_unix_secs": 150,
                "target_m2_observed_unix_secs": 150,
            }

            error = runner.protocol_precondition_error("L3", "L3", evidence)

            self.assertEqual(error, "treatment target M2 lease is expired")

    def test_strict_protocol_accepts_an_active_lease_after_seen_cycle_resets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            runner = self.event_storm_runner([root])
            runner.strict_protocol = True
            runner.treatment_enabled = True
            evidence = {
                "target_m2_debug_ok": True,
                "target_m2_entry_present": True,
                "target_m2_seen": False,
                "target_m2_active": True,
                "target_m2_action": "ephemeral_watch",
                "target_m2_cycle_id": 0,
                "target_m2_expires_unix_secs": 200,
                "target_m2_observed_unix_secs": 150,
            }

            error = runner.protocol_precondition_error("L3", "L3", evidence)

            self.assertEqual(error, "")

    def test_fixed_schedule_uses_each_fully_covered_root_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            container = Path(tmp).resolve()
            roots = [container / f"cold-{index}" for index in range(1, 7)]
            for path in roots:
                path.mkdir()
            runner = self.event_storm_runner(roots)
            runner.fixed_root_schedule = True
            runner.start_delay_secs = 240
            runner.settle_secs = 120
            runner.interval_secs = 10
            runner.rotating_tick_secs = 30
            runner.rotating_ttl_secs = 180
            runner.rotating_dirs_per_tick = 8

            selected = []
            for cycle in range(1, 7):
                runner.cycle = cycle
                selected.append(runner.select_root("L3"))

            self.assertEqual(selected, roots)

    def test_process_sampling_diagnostics_detect_coverage_gaps_and_regressions(self) -> None:
        samples = [
            {"elapsed_secs": 0.5, "cpu_ticks": 10, "read_bytes": 100},
            {"elapsed_secs": 1.0, "cpu_ticks": 20, "read_bytes": 90},
            {"elapsed_secs": 10.0, "cpu_ticks": 30, "read_bytes": 120},
        ]

        result = BENCH.process_sampling_diagnostics(samples, 10.0)

        self.assertEqual(result["sample_coverage_ratio"], 0.95)
        self.assertEqual(result["sample_max_gap_secs"], 9.0)
        self.assertEqual(result["counter_regressions"], 1)

    def test_fixed_falsification_generators_match_806_assertions_and_38_probes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            runner = self.event_storm_runner([root])
            runner.ops_per_burst = 100
            runner.duration_budget_secs = 60
            runner.visibility_probes_per_burst = 8
            rows_and_roots: list[tuple[list[dict[str, object]], Path]] = []
            for cycle, (kind, writer) in enumerate(
                (
                    ("save100", runner.write_save100),
                    ("git-clone", runner.write_git_clone_fixture),
                    ("subtree-rename", runner.write_subtree_rename_avalanche),
                ),
                1,
            ):
                runner.cycle = cycle
                runner.current_burst_started_at = time.monotonic()
                rows = writer(root, "L3")
                rows_and_roots.append((rows, runner.burst_root(root, kind)))

            counts = [len(rows) for rows, _ in rows_and_roots]
            probes = [
                len(runner.select_visibility_probes(rows))
                for rows, _ in rows_and_roots
            ]
            self.assertEqual(counts, [200, 3, 200])
            self.assertEqual(probes, [8, 3, 8])
            self.assertEqual(sum(counts) * 2, 806)
            self.assertEqual(sum(probes) * 2, 38)

    def test_visibility_timestamp_is_taken_after_the_search_call(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            runner = self.event_storm_runner([root])
            probe = {
                "event": {
                    "path": str(root / "a.txt"),
                    "query": "a.txt",
                    "burst_elapsed_secs": 0.0,
                },
                "polls": 0,
                "transport_failures": 0,
                "visible_at": None,
                "visible_query_latency": 0.0,
            }
            runner.active = {
                "root": root,
                "started_at": 1.0,
                "visibility_probes": [probe],
                "visibility_next_poll_at": 0.0,
            }
            with mock.patch.object(
                BENCH,
                "check_search_state_once",
                return_value=(True, True, 0.25, ""),
            ), mock.patch.object(
                BENCH.time, "monotonic", return_value=12.5
            ):
                runner.poll_visibility(10.0)

            self.assertEqual(probe["visible_at"], 12.5)
            with mock.patch.object(runner, "tier_for_root", return_value="L3"), mock.patch.object(
                runner, "emit"
            ) as emit:
                runner.finalize_visibility_probes(20.0)
            record = emit.call_args.args[0]
            self.assertEqual(record["latency_secs"], 11.5)
            self.assertEqual(record["query_latency_secs"], 0.25)

    def test_deterministic_plan_seed_reuses_logical_paths_across_legs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            kwargs = {
                "base_url": "http://127.0.0.1:1",
                "roots": [root],
                "out_path": root / "events.jsonl",
                "started_at": time.monotonic(),
                "start_delay_secs": 0,
                "interval_secs": 1,
                "settle_secs": 0,
                "timeout_secs": 0,
                "ops_per_burst": 2,
                "duration_budget_secs": 10,
                "time_skew_secs": 3600,
                "kinds": ["save100"],
                "target_tiers": ["L3"],
                "deterministic_plan_seed": 42,
            }
            first = BENCH.EventStormRunner(**kwargs)
            second = BENCH.EventStormRunner(**kwargs)
            first.cycle = second.cycle = 1
            first.current_burst_started_at = second.current_burst_started_at = (
                time.monotonic()
            )

            first_rows = first.write_save100(root, "L3")
            first.cleanup_burst_root(
                first.burst_root(root, "save100"),
                root,
                "save100",
                phase="test",
            )
            second_rows = second.write_save100(root, "L3")

            self.assertEqual(
                [(row["path"], row["query"]) for row in first_rows],
                [(row["path"], row["query"]) for row in second_rows],
            )

    def test_repeated_bursts_use_unique_query_names(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            runner = self.event_storm_runner([root])
            runner.ops_per_burst = 2
            runner.current_burst_started_at = time.monotonic()
            runner.cycle = 1
            first = runner.write_save100(root, "L3")
            runner.cycle = 2
            second = runner.write_save100(root, "L3")

            first_queries = {str(row["query"]) for row in first}
            second_queries = {str(row["query"]) for row in second}
            self.assertTrue(first_queries.isdisjoint(second_queries))
            self.assertTrue(all(runner.query_token in query for query in first_queries))

    def test_select_root_accepts_stable_tier_anchor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            stable = root / "fixture-fd-rdd-m2-event-storm-anchor"
            stable.mkdir()
            runner = self.event_storm_runner([root])
            runner.cycle = 1

            with mock.patch.object(
                BENCH,
                "debug_tiered_watch",
                return_value={
                    "dirs": [
                        {"path": str(stable), "watch_tier": "L1"},
                        {"path": str(root), "watch_tier": "L3"},
                    ]
                },
            ):
                selected = runner.select_root("L1")

            self.assertEqual(selected, stable)

    def test_select_root_rejects_symlink_escape_and_falls_back_by_cycle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp).resolve()
            first_root = base / "first"
            second_root = base / "second"
            outside = base / "outside"
            first_root.mkdir()
            second_root.mkdir()
            outside.mkdir()
            escape = first_root / "safe-looking-link"
            try:
                escape.symlink_to(outside, target_is_directory=True)
            except OSError as exc:
                self.skipTest(f"directory symlinks unavailable: {exc}")
            runner = self.event_storm_runner([first_root, second_root])
            runner.cycle = 2

            with mock.patch.object(
                BENCH,
                "debug_tiered_watch",
                return_value={
                    "dirs": [
                        {"path": str(escape), "watch_tier": "L3"},
                        {"path": str(first_root), "watch_tier": "L2"},
                    ]
                },
            ):
                selected_once = runner.select_root("L3")
                selected_twice = runner.select_root("L3")

            self.assertEqual(selected_once, second_root)
            self.assertEqual(selected_twice, second_root)

    def test_npm_node_modules_are_hidden_and_package_root_has_visible_probe(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runner = BENCH.EventStormRunner(
                base_url="http://127.0.0.1:1",
                roots=[root],
                out_path=root / "events.jsonl",
                started_at=time.monotonic(),
                start_delay_secs=0,
                interval_secs=1,
                settle_secs=0,
                timeout_secs=0,
                ops_per_burst=10,
                duration_budget_secs=10,
                time_skew_secs=3600,
                kinds=["npm_install"],
                target_tiers=["L0"],
            )
            runner.current_burst_started_at = time.monotonic()

            records = runner.write_npm_install_fixture(root, "L0")

            node_module_rows = [
                row for row in records if "node_modules" in Path(row["path"]).parts
            ]
            self.assertTrue(node_module_rows)
            self.assertTrue(all(row["should_exist"] is False for row in node_module_rows))
            self.assertTrue(
                all(row["operation"] == "npm_node_modules_hidden" for row in node_module_rows)
            )
            package_root_rows = [
                row for row in records if row["operation"] == "npm_package_root_visible"
            ]
            self.assertEqual(len(package_root_rows), 1)
            self.assertTrue(package_root_rows[0]["should_exist"])

    def test_delayed_cleanup_removes_only_current_burst_and_emits_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            runner = self.event_storm_runner([root])
            runner.cycle = 3
            current = runner.burst_root(root, "rw100")
            sibling = root / "fd-rdd-m2-event-storm-save100-older-run-001"
            (current / "deep").mkdir(parents=True)
            (current / "deep" / "a.txt").write_text("a", encoding="utf-8")
            sibling.mkdir()
            (sibling / "keep.txt").write_text("keep", encoding="utf-8")
            runner.active = {
                "cycle": runner.cycle,
                "root": root,
                "burst_root": current,
                "events": [],
                "stage": "delayed_query",
                "started_at": time.monotonic(),
            }

            with mock.patch.object(runner, "run_query_pass"):
                runner.process_due(time.monotonic())

            self.assertFalse(current.exists())
            self.assertTrue((sibling / "keep.txt").exists())
            rows = [
                json.loads(line)
                for line in (root / "events.jsonl").read_text(encoding="utf-8").splitlines()
            ]
            cleanup = [row for row in rows if row.get("event_kind") == "burst_cleanup"]
            self.assertEqual(len(cleanup), 1)
            self.assertEqual(cleanup[0]["cleanup_target"], str(current))
            self.assertEqual(cleanup[0]["entries_estimated"], 2)
            self.assertTrue(cleanup[0]["ok"])
            self.assertGreaterEqual(cleanup[0]["duration_secs"], 0.0)

    def test_delayed_cleanup_records_failure_without_hiding_next_cycle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            runner = self.event_storm_runner([root])
            runner.cycle = 4
            current = runner.burst_root(root, "rw100")
            current.mkdir()
            runner.active = {
                "cycle": runner.cycle,
                "root": root,
                "burst_root": current,
                "events": [],
                "stage": "delayed_query",
                "started_at": time.monotonic(),
            }

            with mock.patch.object(runner, "run_query_pass"), mock.patch.object(
                BENCH.shutil,
                "rmtree",
                side_effect=PermissionError("fixture busy"),
            ):
                runner.process_due(time.monotonic())

            self.assertIsNone(runner.active)
            rows = [
                json.loads(line)
                for line in (root / "events.jsonl").read_text(encoding="utf-8").splitlines()
            ]
            cleanup = [row for row in rows if row.get("event_kind") == "burst_cleanup"]
            self.assertEqual(len(cleanup), 1)
            self.assertFalse(cleanup[0]["ok"])
            self.assertIn("PermissionError", cleanup[0]["error"])

    def test_visibility_probes_emit_once_when_paths_become_visible(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            runner = BENCH.EventStormRunner(
                base_url="http://127.0.0.1:1",
                roots=[root],
                out_path=root / "events.jsonl",
                started_at=time.monotonic(),
                start_delay_secs=0,
                interval_secs=1,
                settle_secs=120,
                timeout_secs=0,
                ops_per_burst=10,
                duration_budget_secs=10,
                time_skew_secs=3600,
                kinds=["git_clone"],
                target_tiers=["L3"],
                visibility_probes_per_burst=2,
                visibility_poll_interval_secs=1,
            )
            events = [
                {
                    "event_kind": "expected",
                    "operation": "clone_file_visible",
                    "workload": "git_clone",
                    "path": str(root / f"file-{index}.txt"),
                    "query": f"file-{index}.txt",
                    "should_exist": True,
                    "tier_before": "L3",
                    "write_elapsed_secs": 0.0,
                    "burst_elapsed_secs": 0.0,
                }
                for index in range(3)
            ]
            with mock.patch.object(runner, "select_root", return_value=root), mock.patch.object(
                runner, "tier_for_root", return_value="L3"
            ), mock.patch.object(
                runner, "write_git_clone_fixture", return_value=events
            ):
                runner.start_cycle(time.monotonic())

            assert runner.active is not None
            with mock.patch.object(
                BENCH,
                "check_search_state_once",
                side_effect=[
                    (True, True, 0.01, ""),
                    (True, True, 0.01, ""),
                    (False, False, 0.02, "connection reset"),
                    (False, False, 0.02, "connection reset"),
                ],
            ):
                runner.tick(float(runner.active["visibility_next_poll_at"]))
                runner.tick(float(runner.active["visibility_next_poll_at"]))
            runner.finalize_visibility_probes(time.monotonic())

            rows = [
                json.loads(line)
                for line in (root / "events.jsonl").read_text(encoding="utf-8").splitlines()
            ]
            probes = [row for row in rows if row.get("event_kind") == "visibility_probe"]
            self.assertEqual(len(probes), 2)
            self.assertEqual(len({row["path"] for row in probes}), 2)
            self.assertTrue(all(row["visible"] for row in probes))
            self.assertTrue(all(row["transport_failures"] == 1 for row in probes))

    def test_visibility_probe_timeout_is_finalized_before_cleanup(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            runner = BENCH.EventStormRunner(
                base_url="http://127.0.0.1:1",
                roots=[root],
                out_path=root / "events.jsonl",
                started_at=time.monotonic(),
                start_delay_secs=0,
                interval_secs=1,
                settle_secs=120,
                timeout_secs=0,
                ops_per_burst=10,
                duration_budget_secs=10,
                time_skew_secs=3600,
                kinds=["git_clone"],
                target_tiers=["L3"],
                visibility_probes_per_burst=1,
                visibility_poll_interval_secs=1,
            )
            runner.active = {
                "cycle": 1,
                "root": root,
                "burst_root": root / f"{BENCH.EVENT_STORM_DIR_PREFIX}{runner.run_id}-001",
                "selected_kind": "git_clone",
                "stage": "delayed_query",
                "started_at": time.monotonic() - 120,
                "events": [],
                "visibility_probes": [
                    {
                        "event": {
                            "workload": "git_clone",
                            "path": str(root / "missing.txt"),
                            "query": "missing.txt",
                            "tier_before": "L3",
                            "burst_elapsed_secs": 0.0,
                        },
                        "polls": 3,
                        "transport_failures": 0,
                        "last_error": "",
                        "completed": False,
                    }
                ],
            }
            with mock.patch.object(runner, "run_query_pass"), mock.patch.object(
                runner, "cleanup_burst_root"
            ):
                runner.process_due(time.monotonic())

            rows = [
                json.loads(line)
                for line in (root / "events.jsonl").read_text(encoding="utf-8").splitlines()
            ]
            probes = [row for row in rows if row.get("event_kind") == "visibility_probe"]
            self.assertEqual(len(probes), 1)
            self.assertFalse(probes[0]["visible"])
            self.assertTrue(probes[0]["timeout"])

    def test_hot_churn_randomness_is_reproducible_from_workload_seed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = BENCH.HotChurnRunner(
                "http://127.0.0.1:1",
                [root],
                root / "first.jsonl",
                time.monotonic(),
                workload_seed=123,
            )
            second = BENCH.HotChurnRunner(
                "http://127.0.0.1:1",
                [root],
                root / "second.jsonl",
                time.monotonic(),
                workload_seed=123,
            )

            self.assertEqual(
                [first.random.randint(10, 50) for _ in range(8)],
                [second.random.randint(10, 50) for _ in range(8)],
            )

    def test_sweep_returns_nonzero_when_a_variant_fails_protocol(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            sweep_config = root / "sweep.json"
            sweep_config.write_text(
                json.dumps({"variants": [{"label": "strict"}]}),
                encoding="utf-8",
            )
            args = Namespace(
                sweep_config=str(sweep_config),
                port=6060,
                repo=str(root),
            )
            failed = {
                "label": "strict",
                "fatal_error": "event_storm_protocol_failed: unproven tier",
                "fd_rdd_exit_code": 0,
                "ab_comparable": False,
            }

            with mock.patch.object(BENCH, "run_single", return_value=failed):
                result = BENCH.run_sweep(args)

            self.assertEqual(result, 1)


if __name__ == "__main__":
    unittest.main()
