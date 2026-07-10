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
            self.assertIn("final snapshot window RSS max", (run_dir / "REPORT.md").read_text(encoding="utf-8"))

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
            "artifact_provenance": {"verified": True},
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
            "artifact_provenance": {"verified": True},
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

    def test_always_build_records_source_to_artifact_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(
            BENCH.subprocess, "run"
        ) as run, mock.patch.object(BENCH, "git_head_sha", return_value="a" * 40):
            repo = Path(tmp)
            binary = repo / "target" / "release" / "fd-rdd"
            binary.parent.mkdir(parents=True)
            binary.write_bytes(b"candidate")
            (repo / "Cargo.lock").write_text("lock", encoding="utf-8")

            provenance = BENCH.build_if_needed(repo, binary, "always", "a" * 40)

            self.assertTrue(provenance["verified"])
            self.assertTrue(provenance["built_this_run"])
            run.assert_called_once_with(
                ["cargo", "build", "--release", "--locked"],
                cwd=repo,
                check=True,
            )

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


if __name__ == "__main__":
    unittest.main()
