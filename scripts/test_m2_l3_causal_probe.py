from __future__ import annotations

import tempfile
import unittest
import json
import io
from contextlib import redirect_stdout
from datetime import datetime
from pathlib import Path
from unittest import mock

import m2_l3_causal_probe as PROBE


def option_value(command: list[str], option: str) -> str:
    return command[command.index(option) + 1]


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value), encoding="utf-8")


def write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text(
        "".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows),
        encoding="utf-8",
    )


def write_passing_run(run_dir: Path) -> None:
    audit_ts = "2026-07-14T00:00:28.300Z"
    audit_unix = int(datetime.fromisoformat(audit_ts.replace("Z", "+00:00")).timestamp())
    lease_expires_unix = audit_unix - 8
    observed_unix = lease_expires_unix - 20
    burst_root = "/fixture/fd-rdd-m2-event-storm-subtree-rename"
    run_dir.mkdir(parents=True)
    write_json(
        run_dir / "manifest.json",
        {
            "run_state": "completed",
            "completion_reason": "duration_elapsed",
            "created_at": "2026-07-13T23:59:50.000Z",
            "finished_at": "2026-07-14T00:01:20.000Z",
            "fd_rdd_exit_code": 0,
            "ab_comparable": True,
            "ab_comparability_reasons": [],
            "runner_args": {
                "root": ["/fixture"],
                "event_storm_root": ["/fixture"],
                "event_storm": True,
                "event_storm_kind": "subtree_rename",
                "event_storm_target_tier": "L3",
                "event_storm_ops": 10,
                "event_storm_file_count": 10,
                "event_storm_depth": 3,
                "event_storm_start_delay_secs": 0.0,
                "event_storm_precondition_wait_secs": 60.0,
                "event_storm_min_lease_remaining_secs": 18.0,
                "event_storm_settle_secs": 5.0,
                "event_storm_timeout_secs": 0.0,
                "event_storm_max_bursts": 1,
                "event_storm_visibility_probes_per_burst": 10,
                "event_storm_visibility_poll_interval_secs": 0.25,
                "event_storm_post_cleanup_audit_secs": 22.0,
                "event_storm_interval_secs": 300.0,
                "event_storm_fixed_root_schedule": True,
                "event_storm_deterministic_plan": True,
                "event_storm_strict_protocol": True,
                "duration_secs": 80,
                "snapshot_interval_secs": 3600,
                "rotating_cold_window": True,
                "rotating_budget": 1,
                "rotating_tick_secs": 45,
                "rotating_ttl_secs": 20,
                "rotating_max_dirs_per_tick": 1,
                "max_watch_dirs": 1,
                "l0_max_cost_per_root": 1,
                "snapshot_path_disk": True,
                "workload_seed": 42,
                "port": 45678,
            },
        },
    )
    write_json(
        run_dir / "summary.json",
        {
            "fd_rdd_exit_code": 0,
            "fatal_error": "",
            "ab_comparable": True,
            "ab_comparability_reasons": [],
            "shutdown_snapshot_quiesce": {
                "count": 1,
                "ok": 1,
                "failures": 0,
                "ready": True,
                "written": True,
                "rebuild_observed": False,
                "daemon_log_window_valid": True,
                "last_daemon_error": "",
                "error": "",
            },
        },
    )
    rows: list[dict[str, object]] = [
        {
            "event_kind": "burst_started",
            "ts": "2026-07-14T00:00:00.000Z",
            "root": "/fixture",
            "requested_tier": "L3",
            "tier_before": "L3",
            "target_m2_debug_ok": True,
            "target_m2_entry_present": True,
            "target_m2_active": True,
            "target_m2_action": "ephemeral_watch",
            "target_m2_cycle_id": 7,
            "target_m2_scan_seq": 40,
            "target_m2_scan_cycle_id": 7,
            "target_m2_event_seq": 10,
            "target_m2_event_cycle_id": 7,
            "target_m2_observed_unix_secs": observed_unix,
            "target_m2_expires_unix_secs": lease_expires_unix,
        },
        {
            "event_kind": "burst_written",
            "ts": "2026-07-14T00:00:01.000Z",
            "root": "/fixture",
            "selected_kind": "subtree_rename",
            "requested_tier": "L3",
            "tier_before": "L3",
            "events_total": 20,
            "target_m2_debug_ok": True,
            "target_m2_entry_present": True,
            "target_m2_active": True,
            "target_m2_action": "ephemeral_watch",
            "target_m2_cycle_id": 7,
            "target_m2_scan_seq": 40,
            "target_m2_scan_cycle_id": 7,
            "target_m2_event_seq": 10,
            "target_m2_event_cycle_id": 7,
            "target_m2_fence_debug_ok": True,
            "target_m2_fence_entry_present": True,
            "target_m2_fence_active": True,
            "target_m2_fence_action": "ephemeral_watch",
            "target_m2_fence_cycle_id": 7,
            "target_m2_fence_scan_seq": 40,
            "target_m2_fence_scan_cycle_id": 7,
            "target_m2_fence_event_seq": 10,
            "target_m2_fence_event_cycle_id": 7,
            "target_m2_fence_expires_unix_secs": lease_expires_unix,
        },
    ]
    for index in range(10):
        suffix = f"level1_{index:02d}/level2_00/level3_00/deep_{index:04d}.txt"
        rows.extend(
            (
                {
                    "event_kind": "first_query",
                    "operation": "subtree_rename_new_visible_first_query",
                    "workload": "subtree_rename",
                    "path": f"{burst_root}/dir_b/{suffix}",
                    "should_exist": True,
                    "first_query_exists": True,
                    "correct": True,
                    "transport_ok": True,
                    "ok": True,
                    "event_age_secs": 5.2,
                },
                {
                    "event_kind": "first_query",
                    "operation": "subtree_rename_old_hidden_first_query",
                    "workload": "subtree_rename",
                    "path": f"{burst_root}/dir_a/{suffix}",
                    "should_exist": False,
                    "first_query_exists": False,
                    "correct": True,
                    "transport_ok": True,
                    "ok": True,
                    "event_age_secs": 5.2,
                },
                {
                    "event_kind": "visibility_probe",
                    "workload": "subtree_rename",
                    "path": f"{burst_root}/dir_b/{suffix}",
                    "visible": True,
                    "timeout": False,
                    "ok": True,
                    "latency_secs": 4.5,
                },
            )
        )
    rows.extend(
        (
            {
                "event_kind": "burst_checked",
                "ts": "2026-07-14T00:00:06.200Z",
                "root": "/fixture",
                "events_total": 20,
                "positive_total": 10,
                "positive_ok": 10,
                "target_m2_after_scan_seq": 41,
                "target_m2_after_scan_cycle_id": 7,
                "target_m2_after_event_seq": 10,
                "target_m2_after_event_cycle_id": 7,
                "target_m2_after_debug_ok": True,
                "target_m2_after_entry_present": True,
                "target_m2_after_active": True,
                "target_m2_after_action": "ephemeral_watch",
                "target_m2_after_cycle_id": 7,
            },
            {
                "event_kind": "burst_cleanup",
                "ts": "2026-07-14T00:00:06.300Z",
                "root": "/fixture",
                "cleanup_target": burst_root,
                "removed": True,
                "ok": True,
            },
            {
                "event_kind": "post_cleanup_audit",
                "ts": audit_ts,
                "root": "/fixture",
                "cleanup_target": burst_root,
                "debug_ok": True,
                "cleanup_removed": True,
                "cleanup_target_exists": False,
                "target_entry_present": True,
                "target_ephemeral_watch": False,
                "target_rotating_active": False,
                "target_rotating_action": "",
                "cleanup_target_entries": 0,
                "cleanup_target_ephemeral_watch_dirs": 0,
                "cleanup_target_rotating_active_dirs": 0,
                "watcher_ledger_cleared": True,
                "audit_unix_secs": audit_unix,
                "lease_expires_unix_secs": lease_expires_unix,
                "next_rotation_estimate_unix_secs": lease_expires_unix + 25,
                "audit_after_lease_expiry": True,
                "audit_before_next_rotation": True,
                "audit_window_valid": True,
                "ok": True,
            },
        )
    )
    write_jsonl(run_dir / "event-storm-samples.jsonl", rows)
    (run_dir / "fd-rdd.log").write_text(
        "2026-07-14T00:00:02.000Z INFO probe running\n"
        "2026-07-14T00:01:10.000Z INFO snapshot generation durably mounted\n",
        encoding="utf-8",
    )


class ProbeCommandTests(unittest.TestCase):
    def test_command_freezes_single_l3_rename_and_cleanup_snapshot_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            repo = base / "repo"
            binary = repo / "target" / "release" / "fd-rdd"
            fixture_root = base / "fixture" / "root"
            run_dir = base / "run" / "benchmark"

            command = PROBE.build_benchmark_command(
                repo_root=repo,
                binary=binary,
                run_dir=run_dir,
                fixture_root=fixture_root,
                build="never",
                port=45678,
            )

            self.assertEqual(option_value(command, "--port"), "45678")
            self.assertEqual(option_value(command, "--root"), str(fixture_root))
            self.assertEqual(
                option_value(command, "--event-storm-root"), str(fixture_root)
            )
            self.assertEqual(option_value(command, "--event-storm-kind"), "subtree_rename")
            self.assertEqual(option_value(command, "--event-storm-target-tier"), "L3")
            self.assertEqual(option_value(command, "--event-storm-file-count"), "10")
            self.assertEqual(option_value(command, "--event-storm-depth"), "3")
            self.assertEqual(option_value(command, "--event-storm-max-bursts"), "1")
            self.assertEqual(option_value(command, "--event-storm-start-delay-secs"), "0")
            self.assertEqual(
                option_value(command, "--event-storm-precondition-wait-secs"),
                "60",
            )
            self.assertEqual(
                option_value(command, "--event-storm-min-lease-remaining-secs"),
                "18",
            )
            self.assertEqual(option_value(command, "--event-storm-settle-secs"), "5")
            self.assertEqual(
                option_value(command, "--event-storm-post-cleanup-audit-secs"),
                "22",
            )
            self.assertEqual(option_value(command, "--duration-secs"), "80")
            self.assertEqual(option_value(command, "--snapshot-interval-secs"), "3600")
            self.assertEqual(option_value(command, "--rotating-tick-secs"), "45")
            self.assertEqual(option_value(command, "--rotating-ttl-secs"), "20")
            self.assertIn("--event-storm-strict-protocol", command)
            self.assertIn("--rotating-cold-window", command)
            self.assertNotIn("--passive-canary-root", command)

    def test_dry_run_prints_frozen_command_without_creating_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            suite_dir = base / "suite"
            fixture_root = base / "fixture" / "root"
            output = io.StringIO()

            with redirect_stdout(output):
                exit_code = PROBE.main(
                    [
                        "--run-dir",
                        str(suite_dir),
                        "--fixture-root",
                        str(fixture_root),
                        "--port",
                        "45679",
                        "--dry-run",
                    ]
                )

            payload = json.loads(output.getvalue())
            self.assertEqual(exit_code, 0)
            self.assertEqual(payload["mode"], "dry-run")
            self.assertEqual(payload["benchmark_run_dir"], str(suite_dir / "benchmark"))
            self.assertEqual(option_value(payload["command"], "--port"), "45679")
            self.assertIn("--event-storm-strict-protocol", payload["command"])
            self.assertFalse(suite_dir.exists())
            self.assertFalse(fixture_root.exists())

    def test_auto_port_is_resolved_before_building_the_runner_command(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            output = io.StringIO()

            with mock.patch.object(PROBE, "find_free_tcp_port", return_value=45680):
                with redirect_stdout(output):
                    exit_code = PROBE.main(
                        [
                            "--run-dir",
                            str(base / "suite"),
                            "--fixture-root",
                            str(base / "fixture" / "root"),
                            "--dry-run",
                        ]
                    )

            payload = json.loads(output.getvalue())
            self.assertEqual(exit_code, 0)
            self.assertEqual(option_value(payload["command"], "--port"), "45680")

    def test_default_suite_directory_is_unique_within_one_second(self) -> None:
        first = PROBE._default_suite_dir()
        second = PROBE._default_suite_dir()

        self.assertNotEqual(first, second)

    def test_main_executes_runner_and_writes_probe_level_summary_and_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            repo = base / "repo"
            (repo / "scripts").mkdir(parents=True)
            binary = repo / "target" / "release" / "fd-rdd"
            binary.parent.mkdir(parents=True)
            binary.write_bytes(b"binary")
            suite_dir = base / "suite"
            fixture_root = base / "fixture" / "root"

            def fake_run(command: list[str], log_path: Path) -> int:
                benchmark_dir = Path(option_value(command, "--run-dir"))
                write_passing_run(benchmark_dir)
                log_path.write_text("runner completed\n", encoding="utf-8")
                return 0

            output = io.StringIO()
            with mock.patch.object(PROBE, "_run_command", side_effect=fake_run):
                with mock.patch.object(
                    PROBE, "find_free_tcp_port", return_value=45681
                ):
                    with redirect_stdout(output):
                        exit_code = PROBE.main(
                            [
                                "--repo",
                                str(repo),
                                "--binary",
                                str(binary),
                                "--run-dir",
                                str(suite_dir),
                                "--fixture-root",
                                str(fixture_root),
                            ]
                        )

            summary = json.loads((suite_dir / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(exit_code, 0)
            self.assertEqual(summary["decision"], "pass")
            self.assertEqual(summary["runner_exit_code"], 0)
            self.assertEqual(summary["fixture"]["anchor_files"], 16)
            self.assertTrue((suite_dir / "REPORT.md").is_file())
            self.assertTrue((suite_dir / "command.json").is_file())
            self.assertIn('"decision": "pass"', output.getvalue())

    def test_completed_noncomparable_benchmark_uses_probe_verdict(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            repo = base / "repo"
            (repo / "scripts").mkdir(parents=True)
            binary = repo / "target" / "release" / "fd-rdd"
            binary.parent.mkdir(parents=True)
            binary.write_bytes(b"binary")
            suite_dir = base / "suite"
            fixture_root = base / "fixture" / "root"

            def fake_run(command: list[str], log_path: Path) -> int:
                benchmark_dir = Path(option_value(command, "--run-dir"))
                write_passing_run(benchmark_dir)
                for name in ("manifest.json", "summary.json"):
                    path = benchmark_dir / name
                    payload = json.loads(path.read_text(encoding="utf-8"))
                    payload["ab_comparable"] = False
                    payload["ab_comparability_reasons"] = [
                        "git_worktree_dirty",
                        "artifact_provenance_unverified",
                    ]
                    write_json(path, payload)
                log_path.write_text("A/B comparability gate failed\n", encoding="utf-8")
                return 1

            with mock.patch.object(PROBE, "_run_command", side_effect=fake_run):
                with redirect_stdout(io.StringIO()):
                    exit_code = PROBE.main(
                        [
                            "--repo",
                            str(repo),
                            "--binary",
                            str(binary),
                            "--run-dir",
                            str(suite_dir),
                            "--fixture-root",
                            str(fixture_root),
                            "--port",
                            "45682",
                        ]
                    )

            summary = json.loads(
                (suite_dir / "summary.json").read_text(encoding="utf-8")
            )
            self.assertEqual(exit_code, 0)
            self.assertEqual(summary["decision"], "pass")
            self.assertEqual(summary["runner_exit_code"], 1)
            self.assertNotIn("benchmark runner", " ".join(summary["gate_reasons"]))
            self.assertTrue(summary["runner_exit_accepted_as_ab_gate"])

    def test_nonzero_runner_with_unrecognized_gate_is_infrastructure_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            repo = base / "repo"
            (repo / "scripts").mkdir(parents=True)
            binary = repo / "target" / "release" / "fd-rdd"
            binary.parent.mkdir(parents=True)
            binary.write_bytes(b"binary")
            suite_dir = base / "suite"

            def fake_run(command: list[str], log_path: Path) -> int:
                write_passing_run(Path(option_value(command, "--run-dir")))
                log_path.write_text("unexpected runner gate\n", encoding="utf-8")
                return 1

            with mock.patch.object(PROBE, "_run_command", side_effect=fake_run):
                with redirect_stdout(io.StringIO()):
                    exit_code = PROBE.main(
                        [
                            "--repo",
                            str(repo),
                            "--binary",
                            str(binary),
                            "--run-dir",
                            str(suite_dir),
                            "--fixture-root",
                            str(base / "fixture" / "root"),
                            "--port",
                            "45684",
                        ]
                    )

            summary = json.loads(
                (suite_dir / "summary.json").read_text(encoding="utf-8")
            )
            self.assertEqual(exit_code, 2)
            self.assertEqual(summary["decision"], "infrastructure_error")
            self.assertIn("runner", " ".join(summary["gate_reasons"]))

    def test_nonzero_runner_does_not_ignore_fixture_audit_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            repo = base / "repo"
            (repo / "scripts").mkdir(parents=True)
            binary = repo / "target" / "release" / "fd-rdd"
            binary.parent.mkdir(parents=True)
            binary.write_bytes(b"binary")
            suite_dir = base / "suite"

            def fake_run(command: list[str], log_path: Path) -> int:
                benchmark_dir = Path(option_value(command, "--run-dir"))
                write_passing_run(benchmark_dir)
                for name in ("manifest.json", "summary.json"):
                    path = benchmark_dir / name
                    payload = json.loads(path.read_text(encoding="utf-8"))
                    payload["ab_comparable"] = False
                    payload["ab_comparability_reasons"] = [
                        "fixture_count_unverified"
                    ]
                    write_json(path, payload)
                log_path.write_text("fixture audit failed\n", encoding="utf-8")
                return 1

            with mock.patch.object(PROBE, "_run_command", side_effect=fake_run):
                with redirect_stdout(io.StringIO()):
                    exit_code = PROBE.main(
                        [
                            "--repo",
                            str(repo),
                            "--binary",
                            str(binary),
                            "--run-dir",
                            str(suite_dir),
                            "--fixture-root",
                            str(base / "fixture" / "root"),
                            "--port",
                            "45687",
                        ]
                    )

            summary = json.loads(
                (suite_dir / "summary.json").read_text(encoding="utf-8")
            )
            self.assertEqual(exit_code, 2)
            self.assertEqual(summary["decision"], "infrastructure_error")

    def test_runner_start_failure_is_persisted_as_infrastructure_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            repo = base / "repo"
            (repo / "scripts").mkdir(parents=True)
            binary = repo / "target" / "release" / "fd-rdd"
            binary.parent.mkdir(parents=True)
            binary.write_bytes(b"binary")
            suite_dir = base / "suite"

            with mock.patch.object(
                PROBE, "_run_command", side_effect=OSError("cannot spawn daemon")
            ):
                with redirect_stdout(io.StringIO()):
                    exit_code = PROBE.main(
                        [
                            "--repo",
                            str(repo),
                            "--binary",
                            str(binary),
                            "--run-dir",
                            str(suite_dir),
                            "--fixture-root",
                            str(base / "fixture" / "root"),
                            "--port",
                            "45683",
                        ]
                    )

            summary = json.loads(
                (suite_dir / "summary.json").read_text(encoding="utf-8")
            )
            self.assertEqual(exit_code, 2)
            self.assertEqual(summary["decision"], "infrastructure_error")
            self.assertIn("cannot spawn daemon", " ".join(summary["gate_reasons"]))
            self.assertTrue((suite_dir / "REPORT.md").is_file())

    def test_preexisting_suite_is_not_overwritten_or_claimed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            repo = base / "repo"
            (repo / "scripts").mkdir(parents=True)
            binary = repo / "target" / "release" / "fd-rdd"
            binary.parent.mkdir(parents=True)
            binary.write_bytes(b"binary")
            suite_dir = base / "suite"
            suite_dir.mkdir()
            sentinel = {"owner": "another-process"}
            write_json(suite_dir / "summary.json", sentinel)
            (suite_dir / "REPORT.md").write_text("foreign report\n", encoding="utf-8")
            output = io.StringIO()

            with mock.patch.object(PROBE, "_run_command") as run_command:
                with redirect_stdout(output):
                    exit_code = PROBE.main(
                        [
                            "--repo",
                            str(repo),
                            "--binary",
                            str(binary),
                            "--run-dir",
                            str(suite_dir),
                            "--fixture-root",
                            str(base / "fixture" / "root"),
                            "--port",
                            "45685",
                        ]
                    )

            self.assertEqual(exit_code, 2)
            self.assertEqual(
                json.loads((suite_dir / "summary.json").read_text(encoding="utf-8")),
                sentinel,
            )
            self.assertEqual(
                (suite_dir / "REPORT.md").read_text(encoding="utf-8"),
                "foreign report\n",
            )
            self.assertFalse(json.loads(output.getvalue())["artifacts_persisted"])
            run_command.assert_not_called()

    def test_analyze_only_recomputes_saved_benchmark_without_fixture_or_binary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "saved-benchmark"
            write_passing_run(run_dir)
            output = io.StringIO()

            with redirect_stdout(output):
                exit_code = PROBE.main(["--analyze-only", str(run_dir)])

            result = json.loads(
                (run_dir / "l3-causal-probe-summary.json").read_text(encoding="utf-8")
            )
            self.assertEqual(exit_code, 0)
            self.assertEqual(result["decision"], "pass")
            self.assertTrue((run_dir / "L3-CAUSAL-PROBE-REPORT.md").is_file())
            self.assertIn('"decision": "pass"', output.getvalue())

    def test_analyze_only_missing_run_reports_infrastructure_error_without_traceback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "missing-run"
            output = io.StringIO()

            with redirect_stdout(output):
                exit_code = PROBE.main(["--analyze-only", str(run_dir)])

            payload = json.loads(output.getvalue())
            self.assertEqual(exit_code, 2)
            self.assertEqual(payload["decision"], "infrastructure_error")
            self.assertFalse(payload["artifacts_persisted"])
            self.assertFalse(run_dir.exists())


class ProbeFixtureTests(unittest.TestCase):
    def test_fixture_is_unique_and_has_enough_stable_children_to_force_cold_cost(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "fixture" / "root"

            identity = PROBE.prepare_fixture(root)

            self.assertEqual(identity["anchor_dirs"], 16)
            self.assertEqual(identity["anchor_files"], 16)
            self.assertEqual(identity["actual_file_count"], 17)
            self.assertTrue(identity["completed"])
            self.assertRegex(identity["content_sha256"], r"^[0-9a-f]{64}$")
            manifest = root / ".fd-rdd-m2-fixture.json"
            self.assertTrue(manifest.is_file())
            self.assertEqual(
                json.loads(manifest.read_text(encoding="utf-8")),
                identity,
            )
            self.assertEqual(len(list(root.glob("anchor-*"))), 16)
            with self.assertRaises(FileExistsError):
                PROBE.prepare_fixture(root)


class ProbeAnalysisTests(unittest.TestCase):
    def test_passing_run_proves_all_five_causal_stages(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "benchmark"
            write_passing_run(run_dir)

            result = PROBE.analyze_run(run_dir)

            self.assertEqual(result["decision"], "pass")
            self.assertEqual(result["gate_reasons"], [])
            self.assertEqual(
                {name: stage["status"] for name, stage in result["stages"].items()},
                {
                    "protocol": "pass",
                    "causal_progress": "pass",
                    "rename_visibility": "pass",
                    "watcher_cleanup": "pass",
                    "snapshot_persistence": "pass",
                },
            )
            self.assertEqual(result["stages"]["rename_visibility"]["new_ok"], 10)
            self.assertEqual(result["stages"]["rename_visibility"]["old_ok"], 10)

            report = PROBE.render_report(result)
            self.assertIn("scan_seq `40 → 41`", report)
            self.assertIn("新路径 `10/10`", report)
            self.assertIn("ephemeral_watch=`False`", report)
            self.assertIn("ready=`True`", report)

    def test_no_sequence_progress_isolated_as_causal_progress_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "benchmark"
            write_passing_run(run_dir)
            rows = [
                json.loads(line)
                for line in (run_dir / "event-storm-samples.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            checked = next(row for row in rows if row["event_kind"] == "burst_checked")
            checked["target_m2_after_scan_seq"] = 40
            checked["target_m2_after_event_seq"] = 10
            write_jsonl(run_dir / "event-storm-samples.jsonl", rows)

            result = PROBE.analyze_run(run_dir)

            self.assertEqual(result["decision"], "fail")
            self.assertEqual(result["stages"]["causal_progress"]["status"], "fail")
            self.assertEqual(result["stages"]["rename_visibility"]["status"], "pass")

    def test_protocol_rejects_lease_too_short_for_focused_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "benchmark"
            write_passing_run(run_dir)
            rows = [
                json.loads(line)
                for line in (run_dir / "event-storm-samples.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            started = next(row for row in rows if row["event_kind"] == "burst_started")
            started["target_m2_expires_unix_secs"] = (
                int(started["target_m2_observed_unix_secs"]) + 17
            )
            write_jsonl(run_dir / "event-storm-samples.jsonl", rows)

            result = PROBE.analyze_run(run_dir)

            stage = result["stages"]["protocol"]
            self.assertEqual(stage["status"], "fail")
            self.assertIn("18", " ".join(stage["reasons"]))

    def test_progress_during_mutation_window_counts_in_the_same_lease_cycle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "benchmark"
            write_passing_run(run_dir)
            rows = [
                json.loads(line)
                for line in (run_dir / "event-storm-samples.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            written = next(row for row in rows if row["event_kind"] == "burst_written")
            checked = next(row for row in rows if row["event_kind"] == "burst_checked")
            written["target_m2_fence_scan_seq"] = 41
            written["target_m2_fence_scan_cycle_id"] = 7
            checked["target_m2_after_scan_seq"] = 41
            checked["target_m2_after_scan_cycle_id"] = 7
            write_jsonl(run_dir / "event-storm-samples.jsonl", rows)

            result = PROBE.analyze_run(run_dir)

            stage = result["stages"]["causal_progress"]
            self.assertEqual(stage["status"], "pass")
            self.assertTrue(stage["scan_advanced_during_mutation"])
            self.assertFalse(stage["scan_advanced_after_mutation"])

    def test_causal_progress_rejects_different_cycle_and_invalid_fence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "benchmark"
            write_passing_run(run_dir)
            rows = [
                json.loads(line)
                for line in (run_dir / "event-storm-samples.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            checked = next(row for row in rows if row["event_kind"] == "burst_checked")
            checked["target_m2_after_scan_cycle_id"] = 8
            checked["target_m2_after_event_cycle_id"] = 8
            write_jsonl(run_dir / "event-storm-samples.jsonl", rows)

            wrong_cycle = PROBE.analyze_run(run_dir)
            self.assertEqual(
                wrong_cycle["stages"]["causal_progress"]["status"], "fail"
            )

            checked["target_m2_after_scan_cycle_id"] = 7
            checked["target_m2_after_event_cycle_id"] = 7
            checked["target_m2_after_action"] = "fast_scan_lease"
            write_jsonl(run_dir / "event-storm-samples.jsonl", rows)
            wrong_action = PROBE.analyze_run(run_dir)
            self.assertEqual(
                wrong_action["stages"]["causal_progress"]["status"], "fail"
            )

            checked["target_m2_after_action"] = "ephemeral_watch"
            checked["target_m2_after_scan_seq"] = 42
            checked["target_m2_after_event_seq"] = 9
            write_jsonl(run_dir / "event-storm-samples.jsonl", rows)
            regressed = PROBE.analyze_run(run_dir)
            self.assertEqual(
                regressed["stages"]["causal_progress"]["status"], "fail"
            )

            written = next(row for row in rows if row["event_kind"] == "burst_written")
            checked["target_m2_after_scan_seq"] = 41
            checked["target_m2_after_event_seq"] = 10
            written["target_m2_fence_action"] = "scan_only"
            write_jsonl(run_dir / "event-storm-samples.jsonl", rows)
            invalid_fence = PROBE.analyze_run(run_dir)
            stage = invalid_fence["stages"]["causal_progress"]
            self.assertEqual(stage["status"], "fail")
            self.assertFalse(stage["fence_valid"])

    def test_one_missing_new_path_fails_rename_visibility(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "benchmark"
            write_passing_run(run_dir)
            rows = [
                json.loads(line)
                for line in (run_dir / "event-storm-samples.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            missing = next(
                row
                for row in rows
                if row.get("operation") == "subtree_rename_new_visible_first_query"
            )
            missing.update(first_query_exists=False, correct=False, ok=False)
            write_jsonl(run_dir / "event-storm-samples.jsonl", rows)

            result = PROBE.analyze_run(run_dir)

            stage = result["stages"]["rename_visibility"]
            self.assertEqual(stage["status"], "fail")
            self.assertEqual(stage["new_ok"], 9)

    def test_one_visible_old_path_fails_rename_visibility(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "benchmark"
            write_passing_run(run_dir)
            rows = [
                json.loads(line)
                for line in (run_dir / "event-storm-samples.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            stale = next(
                row
                for row in rows
                if row.get("operation") == "subtree_rename_old_hidden_first_query"
            )
            stale.update(first_query_exists=True, correct=False, ok=False)
            write_jsonl(run_dir / "event-storm-samples.jsonl", rows)

            result = PROBE.analyze_run(run_dir)

            stage = result["stages"]["rename_visibility"]
            self.assertEqual(stage["status"], "fail")
            self.assertEqual(stage["old_ok"], 9)

    def test_visibility_over_effective_sla_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "benchmark"
            write_passing_run(run_dir)
            rows = [
                json.loads(line)
                for line in (run_dir / "event-storm-samples.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            query = next(row for row in rows if row.get("event_kind") == "first_query")
            query["event_age_secs"] = 6.001
            write_jsonl(run_dir / "event-storm-samples.jsonl", rows)

            result = PROBE.analyze_run(run_dir)

            stage = result["stages"]["rename_visibility"]
            self.assertEqual(stage["status"], "fail")
            self.assertGreater(stage["observed_max_secs"], 6.0)

    def test_protocol_manifest_and_rename_paths_are_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "benchmark"
            write_passing_run(run_dir)
            manifest = json.loads(
                (run_dir / "manifest.json").read_text(encoding="utf-8")
            )
            manifest["runner_args"]["event_storm_depth"] = 2
            write_json(run_dir / "manifest.json", manifest)

            wrong_protocol = PROBE.analyze_run(run_dir)
            self.assertEqual(wrong_protocol["decision"], "infrastructure_error")
            self.assertIn("event_storm_depth", " ".join(wrong_protocol["gate_reasons"]))

            manifest["runner_args"]["event_storm_depth"] = 3
            write_json(run_dir / "manifest.json", manifest)
            rows = [
                json.loads(line)
                for line in (run_dir / "event-storm-samples.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            new_rows = [
                row
                for row in rows
                if row.get("operation")
                == "subtree_rename_new_visible_first_query"
            ]
            new_rows[1]["path"] = new_rows[0]["path"]
            write_jsonl(run_dir / "event-storm-samples.jsonl", rows)

            duplicate = PROBE.analyze_run(run_dir)
            stage = duplicate["stages"]["rename_visibility"]
            self.assertEqual(stage["status"], "fail")
            self.assertIn("唯一", " ".join(stage["reasons"]))

    def test_manifest_non_numeric_port_is_infrastructure_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "benchmark"
            write_passing_run(run_dir)
            manifest = json.loads(
                (run_dir / "manifest.json").read_text(encoding="utf-8")
            )
            manifest["runner_args"]["port"] = "not-a-port"
            write_json(run_dir / "manifest.json", manifest)

            result = PROBE.analyze_run(run_dir)

            self.assertEqual(result["decision"], "infrastructure_error")
            self.assertIn("port", " ".join(result["gate_reasons"]))

    def test_all_event_paths_must_be_bound_to_the_manifest_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "benchmark"
            write_passing_run(run_dir)
            rows = [
                json.loads(line)
                for line in (run_dir / "event-storm-samples.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            for row in rows:
                if isinstance(row.get("path"), str):
                    row["path"] = str(row["path"]).replace(
                        "/fixture", "/unrelated", 1
                    )
            write_jsonl(run_dir / "event-storm-samples.jsonl", rows)

            result = PROBE.analyze_run(run_dir)

            self.assertEqual(result["decision"], "infrastructure_error")
            self.assertIn("唯一根", " ".join(result["gate_reasons"]))

    def test_rename_paths_must_share_one_burst_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "benchmark"
            write_passing_run(run_dir)
            rows = [
                json.loads(line)
                for line in (run_dir / "event-storm-samples.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            for row in rows:
                path = row.get("path")
                if not isinstance(path, str):
                    continue
                if "/dir_b/" in path:
                    row["path"] = path.replace(
                        "/fd-rdd-m2-event-storm-subtree-rename/",
                        "/burst-new/",
                    )
                elif "/dir_a/" in path:
                    row["path"] = path.replace(
                        "/fd-rdd-m2-event-storm-subtree-rename/",
                        "/burst-old/",
                    )
            write_jsonl(run_dir / "event-storm-samples.jsonl", rows)

            result = PROBE.analyze_run(run_dir)

            self.assertEqual(result["decision"], "infrastructure_error")
            self.assertIn("burst", " ".join(result["gate_reasons"]))

    def test_cleanup_and_audit_targets_must_match_rename_burst_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "benchmark"
            write_passing_run(run_dir)
            rows = [
                json.loads(line)
                for line in (run_dir / "event-storm-samples.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            cleanup = next(row for row in rows if row["event_kind"] == "burst_cleanup")
            audit = next(
                row for row in rows if row["event_kind"] == "post_cleanup_audit"
            )
            cleanup["cleanup_target"] = "/fixture/cleanup-a"
            audit["cleanup_target"] = "/fixture/cleanup-b"
            write_jsonl(run_dir / "event-storm-samples.jsonl", rows)

            result = PROBE.analyze_run(run_dir)

            self.assertEqual(result["decision"], "infrastructure_error")
            self.assertIn("cleanup/audit", " ".join(result["gate_reasons"]))

    def test_audit_window_is_recomputed_from_raw_timestamps(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "benchmark"
            write_passing_run(run_dir)
            rows = [
                json.loads(line)
                for line in (run_dir / "event-storm-samples.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            audit = next(
                row for row in rows if row["event_kind"] == "post_cleanup_audit"
            )
            audit.update(
                audit_unix_secs=0,
                lease_expires_unix_secs=0,
                next_rotation_estimate_unix_secs=0,
                audit_after_lease_expiry=True,
                audit_before_next_rotation=True,
                audit_window_valid=True,
            )
            write_jsonl(run_dir / "event-storm-samples.jsonl", rows)

            missing_raw = PROBE.analyze_run(run_dir)
            self.assertEqual(missing_raw["decision"], "infrastructure_error")

            audit.update(
                audit_unix_secs=130,
                lease_expires_unix_secs=120,
                next_rotation_estimate_unix_secs=145,
                audit_after_lease_expiry=False,
                audit_before_next_rotation=False,
                audit_window_valid=True,
            )
            write_jsonl(run_dir / "event-storm-samples.jsonl", rows)
            inconsistent = PROBE.analyze_run(run_dir)
            self.assertEqual(inconsistent["decision"], "infrastructure_error")
            self.assertIn("audit", " ".join(inconsistent["gate_reasons"]))

    def test_manifest_and_summary_lifecycle_mismatch_is_infrastructure_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "benchmark"
            write_passing_run(run_dir)
            manifest_path = run_dir / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["fd_rdd_exit_code"] = 9
            manifest["fatal_error"] = "daemon failed"
            write_json(manifest_path, manifest)

            result = PROBE.analyze_run(run_dir)

            self.assertEqual(result["decision"], "infrastructure_error")
            self.assertIn("生命周期", " ".join(result["gate_reasons"]))

    def test_unknown_ab_reason_is_infrastructure_error_for_offline_analysis(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "benchmark"
            write_passing_run(run_dir)
            for name in ("manifest.json", "summary.json"):
                path = run_dir / name
                payload = json.loads(path.read_text(encoding="utf-8"))
                payload["ab_comparable"] = False
                payload["ab_comparability_reasons"] = [
                    "fixture_count_unverified"
                ]
                write_json(path, payload)

            result = PROBE.analyze_run(run_dir)

            self.assertEqual(result["decision"], "infrastructure_error")
            self.assertIn("A/B", " ".join(result["gate_reasons"]))

    def test_snapshot_recovery_before_mutation_is_ignored_but_after_mutation_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "benchmark"
            write_passing_run(run_dir)
            log_path = run_dir / "fd-rdd.log"
            log_path.write_text(
                "2026-07-13T23:59:00.000Z WARN Starting background rebuild: startup\n"
                "2026-07-14T00:00:02.000Z INFO probe running\n",
                encoding="utf-8",
            )
            before = PROBE.analyze_run(run_dir)
            self.assertEqual(before["stages"]["snapshot_persistence"]["status"], "pass")

            with log_path.open("a", encoding="utf-8") as handle:
                handle.write(
                    "2026-07-14T00:01:00.000Z ERROR "
                    "snapshot_upsert_unresolved: missing metadata\n"
                )
            after = PROBE.analyze_run(run_dir)
            self.assertEqual(after["stages"]["snapshot_persistence"]["status"], "fail")

    def test_future_mutation_timestamp_cannot_hide_snapshot_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "benchmark"
            write_passing_run(run_dir)
            rows = [
                json.loads(line)
                for line in (run_dir / "event-storm-samples.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            written = next(row for row in rows if row["event_kind"] == "burst_written")
            written["ts"] = "2099-01-01T00:00:00.000Z"
            write_jsonl(run_dir / "event-storm-samples.jsonl", rows)
            with (run_dir / "fd-rdd.log").open("a", encoding="utf-8") as handle:
                handle.write(
                    "2026-07-14T00:01:00.000Z ERROR "
                    "snapshot_upsert_unresolved: missing metadata\n"
                )

            result = PROBE.analyze_run(run_dir)

            self.assertEqual(result["decision"], "infrastructure_error")
            self.assertIn("时间", " ".join(result["gate_reasons"]))

    def test_snapshot_quiesce_not_ready_or_rebuild_observed_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "benchmark"
            write_passing_run(run_dir)
            summary_path = run_dir / "summary.json"
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            summary["shutdown_snapshot_quiesce"].update(
                ready=False,
                rebuild_observed=True,
            )
            write_json(summary_path, summary)

            result = PROBE.analyze_run(run_dir)

            stage = result["stages"]["snapshot_persistence"]
            self.assertEqual(stage["status"], "fail")
            self.assertFalse(stage["quiesce_ready"])
            self.assertTrue(stage["quiesce_rebuild_observed"])

    def test_watcher_audit_required_negative_fields_are_fail_closed(self) -> None:
        required = (
            "target_entry_present",
            "target_ephemeral_watch",
            "target_rotating_active",
            "target_rotating_action",
            "cleanup_target_entries",
            "cleanup_target_ephemeral_watch_dirs",
            "cleanup_target_rotating_active_dirs",
            "watcher_ledger_cleared",
        )
        for field in required:
            with self.subTest(field=field), tempfile.TemporaryDirectory() as tmp:
                run_dir = Path(tmp) / "benchmark"
                write_passing_run(run_dir)
                rows = [
                    json.loads(line)
                    for line in (run_dir / "event-storm-samples.jsonl")
                    .read_text(encoding="utf-8")
                    .splitlines()
                ]
                audit = next(
                    row
                    for row in rows
                    if row["event_kind"] == "post_cleanup_audit"
                )
                audit.pop(field)
                write_jsonl(run_dir / "event-storm-samples.jsonl", rows)

                result = PROBE.analyze_run(run_dir)

                self.assertEqual(result["decision"], "infrastructure_error")
                self.assertIn(field, " ".join(result["gate_reasons"]))

    def test_snapshot_required_negative_fields_are_fail_closed(self) -> None:
        required = (
            "rebuild_observed",
            "daemon_log_window_valid",
            "last_daemon_error",
            "error",
        )
        for field in required:
            with self.subTest(field=field), tempfile.TemporaryDirectory() as tmp:
                run_dir = Path(tmp) / "benchmark"
                write_passing_run(run_dir)
                summary_path = run_dir / "summary.json"
                summary = json.loads(summary_path.read_text(encoding="utf-8"))
                summary["shutdown_snapshot_quiesce"].pop(field)
                write_json(summary_path, summary)

                result = PROBE.analyze_run(run_dir)

                self.assertEqual(result["decision"], "infrastructure_error")
                self.assertIn(field, " ".join(result["gate_reasons"]))

    def test_runner_zero_cannot_contradict_noncomparable_benchmark(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "benchmark"
            write_passing_run(run_dir)
            for name in ("manifest.json", "summary.json"):
                path = run_dir / name
                payload = json.loads(path.read_text(encoding="utf-8"))
                payload["ab_comparable"] = False
                payload["ab_comparability_reasons"] = ["git_worktree_dirty"]
                write_json(path, payload)
            result = PROBE.analyze_run(run_dir)

            PROBE._apply_runner_exit_gate(0, result)

            self.assertEqual(result["decision"], "infrastructure_error")
            self.assertIn("退出 0", " ".join(result["gate_reasons"]))

    def test_analyze_only_malformed_field_is_structured_infrastructure_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "benchmark"
            write_passing_run(run_dir)
            rows = [
                json.loads(line)
                for line in (run_dir / "event-storm-samples.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            started = next(row for row in rows if row["event_kind"] == "burst_started")
            started["target_m2_observed_unix_secs"] = {"bad": 1}
            write_jsonl(run_dir / "event-storm-samples.jsonl", rows)
            output = io.StringIO()

            with redirect_stdout(output):
                exit_code = PROBE.main(["--analyze-only", str(run_dir)])

            printed = json.loads(output.getvalue())
            self.assertEqual(exit_code, 2)
            self.assertEqual(printed["decision"], "infrastructure_error")
            self.assertTrue((run_dir / "l3-causal-probe-summary.json").is_file())

    def test_repeated_watcher_remove_failure_fails_even_when_ledger_is_clear(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "benchmark"
            write_passing_run(run_dir)
            with (run_dir / "fd-rdd.log").open("a", encoding="utf-8") as handle:
                for _ in range(2):
                    handle.write(
                        '2026-07-14T00:01:00.000Z WARN tiered ephemeral watcher '
                        'remove failed for "/fixture": No watch was found\n'
                    )

            result = PROBE.analyze_run(run_dir)

            stage = result["stages"]["watcher_cleanup"]
            self.assertEqual(stage["status"], "fail")
            self.assertEqual(stage["remove_failure_repeat_max"], 2)

    def test_post_cleanup_ephemeral_watch_leak_is_reported_explicitly(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "benchmark"
            write_passing_run(run_dir)
            rows = [
                json.loads(line)
                for line in (run_dir / "event-storm-samples.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            audit = next(
                row for row in rows if row["event_kind"] == "post_cleanup_audit"
            )
            audit.update(
                target_ephemeral_watch=True,
                watcher_ledger_cleared=False,
                ok=False,
            )
            write_jsonl(run_dir / "event-storm-samples.jsonl", rows)

            result = PROBE.analyze_run(run_dir)

            stage = result["stages"]["watcher_cleanup"]
            self.assertEqual(stage["status"], "fail")
            self.assertTrue(stage["target_ephemeral_watch"])
            self.assertFalse(stage["watcher_ledger_cleared"])
            self.assertIn("ephemeral watch", " ".join(stage["reasons"]))

    def test_missing_or_out_of_window_cleanup_audit_is_infrastructure_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "benchmark"
            write_passing_run(run_dir)
            rows = [
                json.loads(line)
                for line in (run_dir / "event-storm-samples.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            without_audit = [
                row for row in rows if row["event_kind"] != "post_cleanup_audit"
            ]
            write_jsonl(run_dir / "event-storm-samples.jsonl", without_audit)

            missing = PROBE.analyze_run(run_dir)
            self.assertEqual(missing["decision"], "infrastructure_error")
            self.assertIn("audit", " ".join(missing["gate_reasons"]))

            audit = next(
                row for row in rows if row["event_kind"] == "post_cleanup_audit"
            )
            audit["audit_unix_secs"] = audit["next_rotation_estimate_unix_secs"]
            audit["ts"] = (
                datetime.fromtimestamp(
                    audit["audit_unix_secs"],
                )
                .astimezone()
                .isoformat()
            )
            audit.update(
                audit_after_lease_expiry=True,
                audit_before_next_rotation=False,
                audit_window_valid=False,
            )
            write_jsonl(run_dir / "event-storm-samples.jsonl", rows)
            wrong_window = PROBE.analyze_run(run_dir)
            self.assertEqual(wrong_window["decision"], "infrastructure_error")
            self.assertIn("时间窗", " ".join(wrong_window["gate_reasons"]))


if __name__ == "__main__":
    unittest.main()
