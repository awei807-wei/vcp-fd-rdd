#!/usr/bin/env python3
"""Tests for scripts/m2-cold-window-ab.py."""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock


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
        ab.stop_benchmark(process)
        process.send_signal.assert_called_once_with(ab.signal.SIGINT)
        process.terminate.assert_not_called()
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


if __name__ == "__main__":
    unittest.main()
