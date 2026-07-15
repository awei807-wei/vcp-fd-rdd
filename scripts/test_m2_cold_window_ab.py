#!/usr/bin/env python3
"""Tests for scripts/m2-cold-window-ab.py."""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import m2_cold_window_ab_diagnostics as ab_diagnostics
import m2_cold_window_ab_process as ab_process
import m2_cold_window_ab_result as ab_result
import m2_cold_window_ab_validation as ab_validation


SCRIPT = Path(__file__).with_name("m2-cold-window-ab.py")
SPEC = importlib.util.spec_from_file_location("m2_cold_window_ab", SCRIPT)
assert SPEC and SPEC.loader
ab = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ab)


class CommandTests(unittest.TestCase):
    def setUp(self) -> None:
        event_roots = tuple(f"cold-storm-{index:02d}" for index in range(1, 7))
        self.roots = {
            name: Path("/fixture") / name
            for name in ("cold-a", "cold-b", "hot", *event_roots)
        }

    def test_a_b_treatment_is_the_only_command_difference(self) -> None:
        run_dir = Path("/tmp/fd-rdd-m2-runs/fixed")
        command_a = ab.build_command("a", run_dir, self.roots)
        command_b = ab.build_command("b", run_dir, self.roots)

        self.assertEqual(command_a[:-1], command_b[:-1])
        self.assertEqual(command_a[-1], "--rotating-cold-window")
        self.assertEqual(command_b[-1], "--no-rotating-cold-window")
        self.assertNotIn("--no-rotating-cold-window", command_a)
        self.assertNotIn("--rotating-cold-window", command_b)

    def test_run_label_is_unique_even_when_attempt_directory_names_match(self) -> None:
        first = ab.build_command(
            "a",
            Path("/tmp/suite/block-01/a/attempt-01"),
            self.roots,
            profile="falsification",
        )
        second = ab.build_command(
            "b",
            Path("/tmp/suite/block-02/b/attempt-01"),
            self.roots,
            profile="falsification",
        )

        first_label = first[first.index("--run-label") + 1]
        second_label = second[second.index("--run-label") + 1]
        self.assertNotEqual(first_label, second_label)

    def test_fixed_command_contains_required_profile(self) -> None:
        command = ab.build_command("a", Path("/tmp/run"), self.roots)
        joined = " ".join(command)
        for expected in (
            "--build always",
            "--watch-mode tiered",
            "--tiered-profile balanced",
            "--duration-secs 3600",
            "--sample-interval-secs 10",
            "--process-sample-interval-secs 0.5",
            "--runtime-profile default",
            "--snapshot-interval-secs 300",
            "--canary-interval-secs 120",
            "--passive-canary-settle-secs 120",
            "--event-storm-start-delay-secs 180",
            "--event-storm-interval-secs 10",
            "--event-storm-settle-secs 60",
            "--event-storm-target-tier L0,L1,L2,L3",
            "--snapshot-path-disk",
            "--workload-seed 42",
        ):
            self.assertIn(expected, joined)

    def test_standard_profile_can_complete_the_full_tier_workload_matrix(self) -> None:
        profile = ab.PROFILES["standard"]
        workload_count = len(profile.event_kinds.split(","))
        tier_count = len(profile.target_tiers.split(","))
        burst_count = workload_count * tier_count
        required = (
            profile.event_start_delay_secs
            + burst_count * profile.event_settle_secs
            + (burst_count - 1) * profile.event_interval_secs
        )

        self.assertEqual(burst_count, 32)
        self.assertLessEqual(required, profile.duration_secs)

    def test_falsification_profile_fixes_root_workloads_and_single_sla_query(self) -> None:
        receipt = Path("/tmp/m2-suite/build-provenance.json")
        command_a = ab.build_command(
            "a",
            Path("/tmp/run"),
            self.roots,
            profile="falsification",
            artifact_provenance_receipt=receipt,
        )
        command_b = ab.build_command(
            "b",
            Path("/tmp/run"),
            self.roots,
            profile="falsification",
            artifact_provenance_receipt=receipt,
        )

        self.assertEqual(command_a[:-1], command_b[:-1])
        self.assertEqual(command_a[-1], "--rotating-cold-window")
        self.assertEqual(command_b[-1], "--no-rotating-cold-window")

        joined = " ".join(command_a)
        self.assertIn("--build never", joined)
        self.assertIn("--duration-secs 1500", joined)
        self.assertIn("--event-storm-kind save100,git_clone,subtree_rename", joined)
        self.assertIn("--event-storm-target-tier L3", joined)
        self.assertIn("--event-storm-start-delay-secs 240", joined)
        self.assertIn("--event-storm-interval-secs 60", joined)
        self.assertIn("--event-storm-visibility-probes-per-burst 8", joined)
        self.assertIn("--event-storm-visibility-poll-interval-secs 1", joined)
        self.assertIn("--event-storm-max-bursts 6", joined)
        self.assertIn("--event-storm-strict-protocol", command_a)
        self.assertIn("--event-storm-precondition-wait-secs 160", joined)
        self.assertIn("--event-storm-min-lease-remaining-secs 125", joined)
        self.assertIn(
            "--artifact-provenance-receipt /tmp/m2-suite/build-provenance.json",
            joined,
        )
        self.assertNotIn("--event-storm-immediate-query", command_a)
        self.assertNotIn("--canary-root", command_a)
        self.assertEqual(command_a.count("--event-storm-root"), 6)
        event_roots = [
            command_a[index + 1]
            for index, item in enumerate(command_a)
            if item == "--event-storm-root"
        ]
        self.assertEqual(
            event_roots,
            [f"/fixture/cold-storm-{index:02d}" for index in range(1, 7)],
        )
        daemon_roots = [
            command_a[index + 1]
            for index, item in enumerate(command_a)
            if item == "--root"
        ]
        self.assertEqual(
            daemon_roots,
            [
                "/fixture/cold-a",
                "/fixture/cold-b",
                "/fixture/hot",
                *[f"/fixture/cold-storm-{index:02d}" for index in range(1, 7)],
            ],
        )
        cold_roots = command_a[command_a.index("--cold-roots") + 1].split(",")
        self.assertEqual(
            cold_roots,
            [
                "/fixture/cold-a",
                "/fixture/cold-b",
                *[f"/fixture/cold-storm-{index:02d}" for index in range(1, 7)],
            ],
        )
        rotating_dirs_per_tick = int(
            command_a[command_a.index("--rotating-max-dirs-per-tick") + 1]
        )
        self.assertGreaterEqual(rotating_dirs_per_tick, len(cold_roots))

    def test_falsification_profile_assigns_one_independent_root_per_burst(self) -> None:
        profile = ab.PROFILES["falsification"]
        required_duration_secs = (
            profile.event_start_delay_secs
            + profile.max_bursts * profile.event_settle_secs
            + (profile.max_bursts - 1) * profile.event_interval_secs
        )

        self.assertEqual(len(profile.event_root_names), profile.max_bursts)
        self.assertEqual(len(set(profile.event_root_names)), profile.max_bursts)
        self.assertLessEqual(required_duration_secs, profile.duration_secs)

    def test_falsification_profile_keeps_each_check_inside_one_fresh_lease(self) -> None:
        profile = ab.PROFILES["falsification"]
        command = ab.build_command(
            "a",
            Path("/tmp/run"),
            self.roots,
            profile="falsification",
        )
        min_remaining = int(
            command[command.index("--event-storm-min-lease-remaining-secs") + 1]
        )
        precondition_wait = int(
            command[command.index("--event-storm-precondition-wait-secs") + 1]
        )
        required_duration = (
            profile.event_start_delay_secs
            + profile.max_bursts * profile.event_settle_secs
            + (profile.max_bursts - 1) * profile.event_interval_secs
            + precondition_wait
        )

        self.assertGreaterEqual(min_remaining, profile.event_settle_secs + 5)
        rotating_tick = int(command[command.index("--rotating-tick-secs") + 1])
        self.assertGreaterEqual(
            precondition_wait,
            min_remaining - 1 + rotating_tick,
        )
        self.assertEqual(
            profile.event_settle_secs + profile.event_interval_secs,
            180,
        )
        self.assertLessEqual(required_duration, profile.duration_secs)

    def test_falsification_dry_run_accepts_explicit_run_dir(self) -> None:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            result = ab.main(
                [
                    "a",
                    "--profile",
                    "falsification",
                    "--run-dir",
                    "/tmp/fixed-falsification-a",
                    "--dry-run",
                ]
            )

        rendered = output.getvalue()
        self.assertEqual(result, 0)
        self.assertIn("profile: falsification", rendered)
        self.assertIn("run_dir: /tmp/fixed-falsification-a", rendered)
        self.assertIn("--duration-secs 1500", rendered)

    def test_falsification_run_requires_suite_build_receipt_before_fixture(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "run"
            error = io.StringIO()
            with mock.patch.object(
                ab,
                "rebuild_fixture",
                side_effect=AssertionError("fixture must not be rebuilt"),
            ), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(
                error
            ):
                result = ab.main(
                    [
                        "a",
                        "--profile",
                        "falsification",
                        "--run-dir",
                        str(run_dir),
                    ]
                )

            self.assertEqual(result, 1)
            self.assertIn("artifact-provenance-receipt", error.getvalue())
            wrapper = json.loads(
                (run_dir / "ab-wrapper-result.json").read_text(encoding="utf-8")
            )
            self.assertEqual(wrapper["status"], "failed")

    def test_running_wrapper_result_does_not_precreate_benchmark_run_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "attempt-01"

            running = ab_result.write_wrapper_result(
                run_dir,
                status="running",
                exit_code=-1,
                variant="a",
                profile="falsification",
            )

            self.assertFalse(run_dir.exists())
            self.assertEqual(running, ab_result.running_wrapper_result_path(run_dir))
            ab_result.write_wrapper_result(
                run_dir,
                status="failed",
                exit_code=1,
                variant="a",
                profile="falsification",
            )
            self.assertTrue((run_dir / "ab-wrapper-result.json").is_file())
            self.assertFalse(running.exists())

    def test_same_run_dir_lock_rejects_second_wrapper_without_creating_run_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "attempt-01"
            error = io.StringIO()
            with ab_result.exclusive_wrapper_run_lock(run_dir), mock.patch.object(
                ab, "rebuild_fixture", side_effect=AssertionError("must not rebuild")
            ), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(
                error
            ):
                result = ab.main(["a", "--run-dir", str(run_dir)])

            self.assertEqual(result, 1)
            self.assertIn("另一 wrapper 占用", error.getvalue())
            self.assertFalse(run_dir.exists())

    def test_relative_run_dir_is_resolved_before_command_construction(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            previous = Path.cwd()
            output = io.StringIO()
            try:
                os.chdir(tmp)
                with contextlib.redirect_stdout(output):
                    result = ab.main(["a", "--dry-run", "--run-dir", "relative-run"])
            finally:
                os.chdir(previous)

            self.assertEqual(result, 0)
            self.assertIn(str(Path(tmp) / "relative-run"), output.getvalue())

    def test_dry_run_does_not_prepare_fixture_or_start_benchmark(self) -> None:
        output = io.StringIO()
        with tempfile.TemporaryDirectory() as temp_home:
            with mock.patch.dict(os.environ, {"HOME": temp_home}):
                with mock.patch.object(ab, "rebuild_fixture", side_effect=AssertionError):
                    with mock.patch.object(ab.subprocess, "Popen", side_effect=AssertionError):
                        with contextlib.redirect_stdout(output):
                            result = ab.main(["a", "--dry-run"])

        rendered = output.getvalue()
        self.assertEqual(result, 0)
        self.assertIn("variant: a (a_rotating)", rendered)
        self.assertIn("/tmp/fd-rdd-m2-runs/", rendered)
        self.assertIn("--rotating-cold-window", rendered)
        self.assertIn("--event-storm", rendered)


class FixtureTests(unittest.TestCase):
    def test_fixture_rebuild_is_confined_and_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as temp_home:
            home = Path(temp_home)
            root = home / ab.FIXTURE_DIR_NAME
            root.mkdir()
            (root / "stale").write_text("stale", encoding="utf-8")

            roots = ab.rebuild_fixture(root, home)

            self.assertFalse((root / "stale").exists())
            for cold_name in ("cold-a", "cold-b"):
                files = sorted(roots[cold_name].glob("d*/file_*.txt"))
                self.assertEqual(len(files), 300)
                self.assertEqual(files[0].relative_to(roots[cold_name]).as_posix(), "d000/file_000.txt")
                self.assertEqual(files[-1].relative_to(roots[cold_name]).as_posix(), "d299/file_299.txt")
            self.assertTrue((roots["hot"] / ".fd-rdd-m2-fixture.json").is_file())
            for index in range(1, 7):
                storm_root = roots[f"cold-storm-{index:02d}"]
                marker = json.loads(
                    (storm_root / ".fd-rdd-m2-fixture.json").read_text(
                        encoding="utf-8"
                    )
                )
                self.assertEqual(marker["actual_file_count"], 1)
                self.assertTrue((storm_root / "seed" / "sentinel.txt").is_file())

            with self.assertRaisesRegex(ValueError, "拒绝删除非专用 fixture 根"):
                ab.assert_safe_fixture_root(home, home)
            with self.assertRaisesRegex(ValueError, "拒绝删除非专用 fixture 根"):
                ab.assert_safe_fixture_root(home / "another-root", home)

    def test_fixture_symlink_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_home:
            home = Path(temp_home)
            target = home / "outside"
            target.mkdir()
            link = home / ab.FIXTURE_DIR_NAME
            link.symlink_to(target, target_is_directory=True)
            with self.assertRaisesRegex(ValueError, "拒绝删除非专用 fixture 根"):
                ab.assert_safe_fixture_root(link, home)

    def test_fixture_manifest_records_deterministic_content_hash(self) -> None:
        with tempfile.TemporaryDirectory() as temp_home:
            home = Path(temp_home)
            roots = ab.rebuild_fixture(home / ab.FIXTURE_DIR_NAME, home)
            manifest_path = roots["cold-a"] / ".fd-rdd-m2-fixture.json"
            first = json.loads(manifest_path.read_text(encoding="utf-8"))

            roots = ab.rebuild_fixture(home / ab.FIXTURE_DIR_NAME, home)
            second = json.loads(
                (roots["cold-a"] / ".fd-rdd-m2-fixture.json").read_text(
                    encoding="utf-8"
                )
            )

            self.assertEqual(first["layout_version"], "m2-cold-window-ab-v3")
            self.assertRegex(first["content_sha256"], r"^[0-9a-f]{64}$")
            self.assertEqual(first["content_sha256"], second["content_sha256"])

    def test_fixture_lock_rejects_a_concurrent_wrapper(self) -> None:
        with tempfile.TemporaryDirectory() as temp_home:
            home = Path(temp_home)
            with ab.exclusive_fixture_lock(home):
                with self.assertRaisesRegex(RuntimeError, "正被另一轮测试占用"):
                    with ab.exclusive_fixture_lock(home):
                        self.fail("并发 wrapper 不应取得 fixture 锁")


class MetricsGateTests(unittest.TestCase):
    @staticmethod
    def sample(enabled: bool, **overrides: object) -> dict[str, object]:
        sample: dict[str, object] = {
            "rotating_cold_window_enabled": enabled,
            "l2_dirs": 1,
            "l3_dirs": 0,
            "rotating_cold_window_active_dirs": 0,
            "rotating_cold_window_cycle_id": 0,
            "rotating_cold_window_cycle_progress_pct": 0,
            "rotating_cold_window_promoted_to_ephemeral": 0,
            "rotating_cold_window_fast_scan_lease_dirs": 0,
            "rotating_cold_window_scan_only_dirs": 0,
        }
        sample.update(overrides)
        return sample

    def test_double_true_is_rejected_for_b(self) -> None:
        reasons = ab.validate_metrics([self.sample(True), self.sample(True)], "b")
        self.assertTrue(any("期望始终为 false" in reason for reason in reasons))

    def test_missing_l2_l3_is_rejected(self) -> None:
        rows = [self.sample(True, l2_dirs=0, l3_dirs=0, rotating_cold_window_cycle_id=1)]
        reasons = ab.validate_metrics(rows, "a")
        self.assertTrue(any("未形成 L2 或 L3" in reason for reason in reasons))

    def test_a_without_activity_is_rejected(self) -> None:
        reasons = ab.validate_metrics([self.sample(True)], "a")
        self.assertTrue(any("A 组未观测到任何 M2 活动" in reason for reason in reasons))

    def test_b_with_activity_is_rejected(self) -> None:
        rows = [self.sample(False, rotating_cold_window_scan_only_dirs=1)]
        reasons = ab.validate_metrics(rows, "b")
        self.assertTrue(any("B 组出现了本应为 0" in reason for reason in reasons))

    def test_preflight_rejects_wrong_treatment_immediately(self) -> None:
        process = mock.Mock()
        process.poll.return_value = None
        rows = [self.sample(True, rotating_cold_window_cycle_id=1)]
        with mock.patch.object(ab_validation, "load_builtin_metrics", return_value=rows):
            with self.assertRaisesRegex(ab.GateError, "期望始终为 false"):
                ab.wait_for_preflight(process, Path("/tmp/run"), "b")

    def test_preflight_rejects_initially_noncomparable_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            (run_dir / "manifest.json").write_text(
                json.dumps(
                    {
                        "run_state": "starting",
                        "ab_comparable": False,
                        "ab_comparability_reasons": ["run_dir_preexisting"],
                    }
                ),
                encoding="utf-8",
            )
            process = mock.Mock()
            process.poll.return_value = None

            with self.assertRaisesRegex(ab.GateError, "run_dir_preexisting"):
                ab.wait_for_preflight(process, run_dir, "a")

    def test_preflight_accepts_cold_active_a(self) -> None:
        process = mock.Mock()
        process.poll.return_value = None
        rows = [self.sample(True, rotating_cold_window_cycle_id=1)]
        with mock.patch.object(ab_validation, "load_builtin_metrics", return_value=rows):
            with contextlib.redirect_stdout(io.StringIO()):
                ab.wait_for_preflight(process, Path("/tmp/run"), "a")

    def test_stop_benchmark_uses_graceful_sigint_first(self) -> None:
        process = mock.Mock()
        process.pid = 1234
        process.poll.return_value = None
        process.wait.return_value = 0
        with mock.patch.object(
            ab_process.os,
            "killpg",
            side_effect=[None, ProcessLookupError()],
        ) as killpg:
            errors = ab.stop_benchmark(process)
        self.assertEqual(errors, [])
        killpg.assert_any_call(1234, ab.signal.SIGINT)

    def test_stop_benchmark_preserves_signal_failure_and_falls_back(self) -> None:
        process = mock.Mock()
        process.pid = 1234
        process.poll.return_value = None
        process.wait.side_effect = [
            ab.subprocess.TimeoutExpired("runner", 1),
            0,
        ]

        with mock.patch.object(
            ab_process.os,
            "killpg",
            side_effect=[OSError("signal race"), None, None],
        ), mock.patch.object(
            ab_process,
            "_group_alive",
            side_effect=[True, True],
        ), mock.patch.object(
            ab_process,
            "_wait_group_exit",
            return_value=True,
        ):
            errors = ab.stop_benchmark(
                process,
                graceful_timeout_secs=1,
                fallback_timeout_secs=1,
            )

        self.assertTrue(any("signal race" in error for error in errors))
        self.assertTrue(any("未优雅停止" in error for error in errors))

    def test_complete_valid_run_passes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = Path(temp_dir)
            self.write_complete_run(run_dir, self.sample(True, rotating_cold_window_cycle_id=1))
            ab.validate_run(run_dir, "a")

    def test_manifest_not_comparable_is_rejected_even_if_summary_is_comparable(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = Path(temp_dir)
            self.write_complete_run(run_dir, self.sample(False))
            manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
            manifest["ab_comparable"] = False
            manifest["ab_comparability_reasons"] = ["final_snapshot_failed"]
            (run_dir / "manifest.json").write_text(
                json.dumps(manifest) + "\n", encoding="utf-8"
            )

            with self.assertRaisesRegex(ab.GateError, "final_snapshot_failed"):
                ab.validate_run(run_dir, "b")

    def test_missing_artifact_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = Path(temp_dir)
            self.write_complete_run(run_dir, self.sample(False))
            (run_dir / "REPORT.md").unlink()
            with self.assertRaisesRegex(ab.GateError, "REPORT.md"):
                ab.validate_run(run_dir, "b")

    def test_runner_not_comparable_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = Path(temp_dir)
            self.write_complete_run(run_dir, self.sample(False))
            (run_dir / "summary.json").write_text(
                json.dumps(
                    {
                        "fd_rdd_exit_code": 0,
                        "ab_comparable": False,
                        "ab_comparability_reasons": ["fixture_count_unverified"],
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ab.GateError, "fixture_count_unverified"):
                ab.validate_run(run_dir, "b")

    def test_runner_signal_termination_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = Path(temp_dir)
            self.write_complete_run(run_dir, self.sample(False))
            (run_dir / "summary.json").write_text(
                json.dumps({"fd_rdd_exit_code": -15, "ab_comparable": True}) + "\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ab.GateError, "退出码异常"):
                ab.validate_run(run_dir, "b")

    @staticmethod
    def write_complete_run(run_dir: Path, metric: dict[str, object]) -> None:
        for name in ab.REQUIRED_ARTIFACTS:
            content = "报告\n" if name == "REPORT.md" else "{}\n"
            (run_dir / name).write_text(content, encoding="utf-8")
        (run_dir / "summary.json").write_text(
            json.dumps({"fd_rdd_exit_code": 0, "ab_comparable": True}) + "\n",
            encoding="utf-8",
        )
        (run_dir / "manifest.json").write_text(
            json.dumps(
                {
                    "run_state": "completed",
                    "completion_reason": "duration_elapsed",
                    "ab_comparable": True,
                }
            )
            + "\n",
            encoding="utf-8",
        )
        (run_dir / "process-samples.jsonl").write_text(
            json.dumps({"elapsed_secs": 1, "cpu_pct": 0.1}) + "\n",
            encoding="utf-8",
        )
        (run_dir / "endpoint-samples.jsonl").write_text(
            json.dumps({"endpoint": "/watch-state", "ok": True}) + "\n",
            encoding="utf-8",
        )
        (run_dir / "event-storm-samples.jsonl").write_text(
            json.dumps({"event_kind": "burst_written", "events_total": 100}) + "\n",
            encoding="utf-8",
        )
        metrics_dir = run_dir / "reports" / "metrics"
        metrics_dir.mkdir(parents=True)
        (metrics_dir / "metrics_2026-07-12_00.json").write_text(
            json.dumps(metric) + "\n",
            encoding="utf-8",
        )


class FailureDiagnosticTests(unittest.TestCase):
    def test_unexpected_wrapper_exception_still_renders_failure_report(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_root = Path(temp_dir)
            roots = {
                name: Path("/fixture") / name
                for name in ("cold-a", "cold-b", "hot")
            }
            stderr = io.StringIO()
            with mock.patch.object(ab, "RUN_ROOT", run_root):
                with mock.patch.object(ab, "utc_stamp", return_value="broken"):
                    with mock.patch.object(
                        ab,
                        "exclusive_fixture_lock",
                        return_value=contextlib.nullcontext(),
                    ):
                        with mock.patch.object(ab, "rebuild_fixture", return_value=roots):
                            with mock.patch.object(
                                ab,
                                "start_runner_process",
                                side_effect=RuntimeError("unexpected spawn failure"),
                            ):
                                with contextlib.redirect_stdout(io.StringIO()):
                                    with contextlib.redirect_stderr(stderr):
                                        result = ab.main(["a"])

            self.assertEqual(result, 1)
            self.assertIn("RuntimeError: unexpected spawn failure", stderr.getvalue())
            self.assertIn("manifest=missing", stderr.getvalue())
            wrapper_result = json.loads(
                (run_root / "broken_a_rotating" / "ab-wrapper-result.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(wrapper_result["status"], "failed")
            self.assertEqual(wrapper_result["wrapper_exit_code"], 1)

    def test_runner_output_is_streamed_and_persisted(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            log_path = Path(temp_dir) / "runner.log"
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                process, capture = ab.start_runner_process(
                    [
                        sys.executable,
                        "-c",
                        "import sys; print('runner-out'); print('runner-err', file=sys.stderr)",
                    ],
                    SCRIPT.parent,
                    log_path,
                )
                self.assertEqual(process.wait(timeout=10), 0)
                self.assertEqual(capture.join(timeout=10), [])

            persisted = log_path.read_text(encoding="utf-8")
            self.assertIn("runner-out", persisted)
            self.assertIn("runner-err", persisted)
            self.assertIn("runner-out", stdout.getvalue())
            self.assertIn("runner-err", stdout.getvalue())

    def test_main_nonzero_runner_exit_surfaces_bounded_failure_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_root = Path(temp_dir)
            run_dir = run_root / "fixed_a_rotating"
            run_dir.mkdir()
            (run_dir / "manifest.json").write_text(
                json.dumps(
                    {
                        "run_state": "failed",
                        "failure_stage": "runtime",
                        "fatal_error": "event storm crashed",
                    }
                ),
                encoding="utf-8",
            )
            (run_dir / "fd-rdd.log").write_text(
                "ERROR daemon exited unexpectedly\n",
                encoding="utf-8",
            )
            process = mock.Mock()
            process.pid = 4242
            process.wait.return_value = 1
            process.poll.return_value = 1
            capture = mock.Mock()
            capture.join.return_value = []
            roots = {
                name: Path("/fixture") / name
                for name in ("cold-a", "cold-b", "hot")
            }
            stderr = io.StringIO()

            with mock.patch.object(ab, "RUN_ROOT", run_root):
                with mock.patch.object(ab, "utc_stamp", return_value="fixed"):
                    with mock.patch.object(
                        ab,
                        "exclusive_fixture_lock",
                        return_value=contextlib.nullcontext(),
                    ):
                        with mock.patch.object(ab, "rebuild_fixture", return_value=roots):
                            with mock.patch.object(
                                ab,
                                "start_runner_process",
                                return_value=(process, capture),
                            ):
                                with mock.patch.object(ab, "wait_for_preflight"):
                                    with mock.patch.object(
                                        ab, "cleanup_benchmark", return_value=[]
                                    ):
                                        with contextlib.redirect_stdout(io.StringIO()):
                                            with contextlib.redirect_stderr(stderr):
                                                result = ab.main(["a"])

            rendered = stderr.getvalue()
            self.assertEqual(result, 1)
            self.assertIn("底层 benchmark 失败，退出码 1", rendered)
            self.assertIn(f"失败现场目录：{run_dir}", rendered)
            self.assertIn("failure_stage=runtime", rendered)
            self.assertIn("event storm crashed", rendered)
            self.assertIn("daemon exited unexpectedly", rendered)

    def test_successful_wrapper_persists_a_passed_terminal_result(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = Path(temp_dir) / "attempt-01"
            process = mock.Mock()
            process.pid = 4242
            process.wait.return_value = 0
            capture = mock.Mock()
            capture.join.return_value = []
            roots = {
                name: Path("/fixture") / name
                for name in ("cold-a", "cold-b", "hot")
            }
            with mock.patch.object(
                ab,
                "exclusive_fixture_lock",
                return_value=contextlib.nullcontext(),
            ), mock.patch.object(
                ab, "rebuild_fixture", return_value=roots
            ), mock.patch.object(
                ab, "start_runner_process", return_value=(process, capture)
            ), mock.patch.object(
                ab, "wait_for_preflight"
            ), mock.patch.object(
                ab, "validate_run"
            ), contextlib.redirect_stdout(io.StringIO()):
                result = ab.main(["a", "--run-dir", str(run_dir)])

            wrapper_result = json.loads(
                (run_dir / "ab-wrapper-result.json").read_text(encoding="utf-8")
            )
            self.assertEqual(result, 0)
            self.assertEqual(wrapper_result["status"], "passed")
            self.assertEqual(wrapper_result["wrapper_exit_code"], 0)

    def test_missing_artifacts_are_reported_explicitly(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = Path(temp_dir) / "failed-run"
            run_dir.mkdir()
            (run_dir / "fd-rdd.log").write_text("daemon tail\n", encoding="utf-8")

            rendered = "\n".join(ab_diagnostics.collect_failure_diagnostics(run_dir))

            self.assertIn("manifest=missing", rendered)
            self.assertIn("summary=missing", rendered)
            self.assertIn("runner_log=missing", rendered)

    def test_malformed_summary_reason_type_enters_gate_diagnostics(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = Path(temp_dir)
            MetricsGateTests.write_complete_run(run_dir, MetricsGateTests.sample(False))
            (run_dir / "summary.json").write_text(
                json.dumps(
                    {
                        "fd_rdd_exit_code": 0,
                        "ab_comparable": False,
                        "ab_comparability_reasons": [{"code": "schema-damaged"}],
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ab.GateError, "schema-damaged"):
                ab.validate_run(run_dir, "b")

    def test_collect_failure_diagnostics_surfaces_runner_and_daemon_causes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = Path(temp_dir)
            (run_dir / "manifest.json").write_text(
                json.dumps(
                    {
                        "run_state": "failed",
                        "failure_stage": "runtime",
                        "completion_reason": "daemon_exit",
                        "fd_rdd_exit_code": 1,
                        "fatal_error": "runner exploded",
                        "cleanup_errors": ["daemon_shutdown_timeout"],
                        "ab_comparability_reasons": ["daemon_exit_failed"],
                        "execution": {
                            "completion_reason": "duration_elapsed",
                            "exit_code": 1,
                            "passive_canary_enabled": True,
                            "passive_shutdown_reconcile_count": 1,
                            "passive_shutdown_reconcile_ok": 0,
                            "passive_shutdown_reconcile_failures": 1,
                            "passive_shutdown_reconcile_failed": True,
                            "shutdown_snapshot_quiesce_required": True,
                            "shutdown_snapshot_quiesce_count": 1,
                            "shutdown_snapshot_quiesce_ok": 0,
                            "shutdown_snapshot_quiesce_failures": 1,
                            "shutdown_snapshot_quiesce_failed": True,
                            "event_storm_cleanup_failures": 2,
                            "final_snapshot_failed": True,
                        },
                    }
                ),
                encoding="utf-8",
            )
            (run_dir / "summary.json").write_text(
                json.dumps(
                    {
                        "fd_rdd_exit_code": 1,
                        "fatal_error": "summary failure",
                        "ab_comparability_reasons": ["final_snapshot_failed"],
                        "passive_shutdown_reconcile": {
                            "count": 1,
                            "ok": 0,
                            "failures": 1,
                        },
                        "shutdown_snapshot_quiesce": {
                            "count": 1,
                            "ok": 0,
                            "failures": 1,
                            "ready": False,
                            "last_daemon_error": "direct_v7_unsupported",
                        },
                    }
                ),
                encoding="utf-8",
            )
            (run_dir / "events.jsonl").write_text(
                json.dumps(
                    {
                        "event": "fatal_error",
                        "error": "event loop failed",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            (run_dir / "fd-rdd.log").write_text(
                "normal line\n"
                "2026 ERROR Final snapshot failed: snapshot_upsert_unresolved\n",
                encoding="utf-8",
            )

            rendered = "\n".join(ab_diagnostics.collect_failure_diagnostics(run_dir))

            self.assertIn(f"失败现场目录：{run_dir}", rendered)
            self.assertIn("failure_stage=runtime", rendered)
            self.assertIn("runner exploded", rendered)
            self.assertIn("daemon_shutdown_timeout", rendered)
            self.assertIn("final_snapshot_failed", rendered)
            self.assertIn("passive_shutdown_reconcile_failures=1", rendered)
            self.assertIn("passive_shutdown_reconcile_failed=True", rendered)
            self.assertIn("shutdown_snapshot_quiesce_failures=1", rendered)
            self.assertIn("shutdown_snapshot_quiesce_failed=True", rendered)
            self.assertIn("direct_v7_unsupported", rendered)
            self.assertIn("event_storm_cleanup_failures=2", rendered)
            self.assertIn("event loop failed", rendered)
            self.assertIn("snapshot_upsert_unresolved", rendered)
            self.assertIn(str(run_dir / "fd-rdd.log"), rendered)

    def test_collect_failure_diagnostics_bounds_log_output(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = Path(temp_dir)
            (run_dir / "fd-rdd.log").write_text(
                "".join(f"ERROR failure-{index:03d}\n" for index in range(100)),
                encoding="utf-8",
            )

            diagnostics = ab_diagnostics.collect_failure_diagnostics(run_dir)
            log_section = next(item for item in diagnostics if "fd-rdd.log 关键尾部" in item)

            self.assertNotIn("failure-000", log_section)
            self.assertIn("failure-099", log_section)
            self.assertLessEqual(
                len(log_section.splitlines()),
                ab_diagnostics.DIAGNOSTIC_LOG_LINES + 1,
            )

    def test_render_gate_error_prints_each_reason_on_its_own_line(self) -> None:
        stderr = io.StringIO()
        ab.render_failure_report(["退出码 1", "manifest: runtime failed"], stderr)

        self.assertEqual(
            stderr.getvalue().splitlines(),
            [
                "M2 A/B 门禁失败：",
                "- 退出码 1",
                "- manifest: runtime failed",
            ],
        )


if __name__ == "__main__":
    unittest.main()
