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


SCRIPT = Path(__file__).with_name("m2-cold-window-ab.py")
SPEC = importlib.util.spec_from_file_location("m2_cold_window_ab", SCRIPT)
assert SPEC and SPEC.loader
ab = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ab)


class CommandTests(unittest.TestCase):
    def setUp(self) -> None:
        self.roots = {
            "cold-a": Path("/fixture/cold-a"),
            "cold-b": Path("/fixture/cold-b"),
            "hot": Path("/fixture/hot"),
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
            "--event-storm-target-tier L0,L1,L2,L3",
            "--snapshot-path-disk",
            "--workload-seed 42",
        ):
            self.assertIn(expected, joined)

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
        with mock.patch.object(ab, "load_builtin_metrics", return_value=rows):
            with self.assertRaisesRegex(ab.GateError, "期望始终为 false"):
                ab.wait_for_preflight(process, Path("/tmp/run"), "b")

    def test_preflight_accepts_cold_active_a(self) -> None:
        process = mock.Mock()
        process.poll.return_value = None
        rows = [self.sample(True, rotating_cold_window_cycle_id=1)]
        with mock.patch.object(ab, "load_builtin_metrics", return_value=rows):
            with contextlib.redirect_stdout(io.StringIO()):
                ab.wait_for_preflight(process, Path("/tmp/run"), "a")

    def test_stop_benchmark_uses_graceful_sigint_first(self) -> None:
        process = mock.Mock()
        process.poll.return_value = None
        process.wait.return_value = 0
        errors = ab.stop_benchmark(process)
        self.assertEqual(errors, [])
        process.send_signal.assert_called_once_with(ab.signal.SIGINT)
        process.terminate.assert_not_called()
        process.kill.assert_not_called()

    def test_stop_benchmark_preserves_signal_failure_and_falls_back(self) -> None:
        process = mock.Mock()
        process.poll.side_effect = [None, None]
        process.send_signal.side_effect = ProcessLookupError("signal race")
        process.wait.return_value = 0

        errors = ab.stop_benchmark(process)

        self.assertTrue(any("signal race" in error for error in errors))
        process.terminate.assert_called_once()
        process.kill.assert_not_called()

    def test_complete_valid_run_passes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = Path(temp_dir)
            self.write_complete_run(run_dir, self.sample(True, rotating_cold_window_cycle_id=1))
            ab.validate_run(run_dir, "a")

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

    def test_runner_normal_sigterm_boundary_is_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = Path(temp_dir)
            self.write_complete_run(run_dir, self.sample(False))
            (run_dir / "summary.json").write_text(
                json.dumps({"fd_rdd_exit_code": -15, "ab_comparable": True}) + "\n",
                encoding="utf-8",
            )
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
                {"run_state": "completed", "completion_reason": "duration_elapsed"}
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
            process.wait.return_value = 1
            process.poll.return_value = 1
            roots = {
                name: Path("/fixture") / name
                for name in ("cold-a", "cold-b", "hot")
            }
            stderr = io.StringIO()

            with mock.patch.object(ab, "RUN_ROOT", run_root):
                with mock.patch.object(ab, "utc_stamp", return_value="fixed"):
                    with mock.patch.object(ab, "rebuild_fixture", return_value=roots):
                        with mock.patch.object(ab.subprocess, "Popen", return_value=process):
                            with mock.patch.object(ab, "wait_for_preflight"):
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
