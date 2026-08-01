#!/usr/bin/env python3
"""Focused tests for the standalone short-sweep integration proof."""

from __future__ import annotations

import hashlib
import json
import signal
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import m2_cold_window_sweep_integration as integration
import m2_cold_window_sweep_integration_evidence as evidence
import m2_cold_window_sweep_integration_process as integration_process
from m2_cold_window_ab_command import REPO_ROOT
from m2_cold_window_build_receipt import create_build_receipt


class SweepIntegrationTests(unittest.TestCase):
    @staticmethod
    def _write_json(path: Path, value: object) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value) + "\n", encoding="utf-8")

    def _source_suite(self, root: Path) -> Path:
        suite = root / "immutable-r9"
        binary = suite / integration.PRODUCT_BINARY_RELATIVE
        binary.parent.mkdir(parents=True)
        binary.write_bytes(b"formal-r9-product-binary")
        source_sha = "a" * 40
        receipt = create_build_receipt(
            REPO_ROOT,
            binary,
            source_sha,
            source_sha,
            source_sha,
        )
        self._write_json(suite / integration.BUILD_RECEIPT_NAME, receipt)
        return suite

    def _raw_attempt(
        self,
        output: Path,
        source_suite: Path,
        *,
        exit_code: int = 0,
        cycles: int = 1,
    ) -> Path:
        spec = integration.integration_spec(output)
        attempt = spec.base_dir / "attempt-01"
        attempt.mkdir(parents=True)
        identity = evidence.product_identity(source_suite)
        self._write_json(
            attempt / integration.RESULT_NAME,
            {
                "schema": 1,
                "status": "completed",
                "exit_code": exit_code,
                "run_dir": str(attempt),
                "source_suite": str(source_suite),
                "product_git_sha": identity["product_git_sha"],
                "product_binary_sha256": identity["product_binary_sha256"],
                "product_receipt_sha256": identity["product_receipt_sha256"],
                "harness_git_sha": "b" * 40,
                "harness_worktree_dirty": False,
                "protocol_fingerprint": integration.protocol_fingerprint(spec),
                "process_group_id": 1234,
                "started_at": "2026-08-01T00:00:00Z",
                "finished_at": "2026-08-01T00:08:00Z",
            },
        )
        self._write_json(
            attempt / "fixture-report.json",
            {
                "cycles_requested": 1,
                "cycles": [{} for _ in range(cycles)],
                "integration_options": {
                    "allow_cycle_shortfall": True,
                    "burst_root_level": True,
                },
                "watch_state": {
                    "rotating_cycle_id_min": 0,
                    "rotating_cycle_id_max": 7,
                    "rotating_cycle_progress_pct_max": 100,
                    "rotating_completed_cycles_observed": 7,
                },
                "config": {
                    "rotating_cold_window": True,
                    "fast_scan": True,
                    "query_fast_scan_leases": False,
                    "rotating_ttl_secs": 45,
                    "rotating_tick_secs": 15,
                    "rotating_full_sweep_period_secs": 45,
                },
                "burst": {
                    "enabled": True,
                    "visible": True,
                    "root_level": True,
                },
                "deep_modify": {
                    "enabled": True,
                    "visible": True,
                    "flagged_secs": 0.5,
                    "repaired_secs": 46.0,
                    "baseline_tier": "FrozenManifestOnly",
                    "updated_tier": "HotMemory",
                },
                "sweep_only_modify": {
                    "enabled": True,
                    "waited_secs": 70.0,
                    "first_query_freshness": "fresh",
                    "first_query_tier": "HotMemory",
                    "repaired_by_sweep": True,
                },
            },
        )
        (attempt / "FIXTURE-REPORT.md").write_text("# raw\n", encoding="utf-8")
        (attempt / "fd-rdd.log").write_text("fd-rdd ready.\n", encoding="utf-8")
        (attempt / "process-samples.jsonl").write_text("{}\n", encoding="utf-8")
        (attempt / "metrics-samples.jsonl").write_text("{}\n", encoding="utf-8")
        config = attempt / "config-home/fd-rdd/config.toml"
        config.parent.mkdir(parents=True)
        config.write_text("[general]\n", encoding="utf-8")
        (attempt.parent / "attempt-01.runner.log").write_text(
            "fixture completed\n", encoding="utf-8"
        )
        return attempt

    def test_command_uses_nonzero_full_sweep_period(self) -> None:
        spec = integration.integration_spec(Path("/tmp/integration"))
        command = integration.fixture_command(
            spec,
            spec.base_dir / "attempt-01",
            Path("/tmp/r9/fd-rdd"),
        )

        self.assertIn("--rotating-full-sweep-period-secs", command)
        index = command.index("--rotating-full-sweep-period-secs")
        self.assertEqual(command[index + 1], "45")
        self.assertIn("--deep-modify-probe", command)
        self.assertIn("--allow-cycle-shortfall", command)
        self.assertIn("--burst-root-level", command)

        payload = {
            "schema": 1,
            "cycles": 1,
            "port": 6261,
            "rotating_ttl_secs": 45,
            "rotating_tick_secs": 15,
            "rotating_full_sweep_period_secs": 45,
            "settle_secs": 150.0,
            "calibrate": False,
            "allow_cycle_shortfall": True,
            "burst": True,
            "burst_root_level": True,
            "deep_modify_probe": True,
        }
        expected = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        self.assertEqual(integration.protocol_fingerprint(spec), expected)

    def test_product_and_harness_identities_are_separate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = self._source_suite(root)
            output = root / "integration"
            attempt = self._raw_attempt(output, source, cycles=0)

            summary = evidence.recompute_summary(output, source, attempt)

            self.assertEqual(summary["product"]["product_git_sha"], "a" * 40)
            self.assertEqual(summary["harness"]["git_sha"], "b" * 40)
            self.assertNotEqual(
                summary["product"]["product_git_sha"],
                summary["harness"]["git_sha"],
            )
            self.assertEqual(summary["decision"], "pass")
            self.assertEqual(
                summary["integration"]["protocol"]["cycles_observed"], 7
            )
            self.assertEqual(
                summary["integration"]["protocol"]["syscall_cycles_observed"],
                0,
            )

    def test_watch_cycle_progress_is_required_when_shortfall_is_allowed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = self._source_suite(root)
            output = root / "integration"
            attempt = self._raw_attempt(output, source, cycles=0)
            report_path = attempt / "fixture-report.json"
            report = json.loads(report_path.read_text(encoding="utf-8"))
            report["watch_state"]["rotating_cycle_progress_pct_max"] = 99
            report["watch_state"]["rotating_completed_cycles_observed"] = 0
            self._write_json(report_path, report)

            summary = evidence.recompute_summary(output, source, attempt)

            self.assertEqual(summary["decision"], "fail")
            self.assertTrue(any("progress 100%" in row for row in summary["reasons"]))

    def test_any_completed_attempt_is_terminal_even_when_product_failed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = self._source_suite(root)
            output = root / "integration"
            attempt = self._raw_attempt(output, source, exit_code=1, cycles=0)

            with mock.patch.object(
                integration, "run_fixture_process"
            ) as run_process, mock.patch.object(
                integration, "git_head_sha", return_value="c" * 40
            ), mock.patch.object(
                integration, "git_worktree_dirty", return_value=True
            ):
                result = integration.run_integration_only(output, source)

            run_process.assert_not_called()
            self.assertEqual(result, 2)
            self.assertEqual(
                integration.completed_terminal_attempt(attempt.parent), attempt
            )
            self.assertFalse((attempt.parent / "attempt-02").exists())

    def test_running_attempt_allows_a_new_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            running = base / "attempt-01"
            running.mkdir()
            self._write_json(
                running / integration.RESULT_NAME,
                {"schema": 1, "status": "running", "exit_code": None},
            )

            self.assertIsNone(integration.completed_terminal_attempt(base))
            self.assertEqual(integration.next_attempt(base).name, "attempt-02")

    def test_evidence_recomputes_and_rejects_forged_pass_boolean(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = self._source_suite(root)
            output = root / "integration"
            attempt = self._raw_attempt(output, source, exit_code=1, cycles=0)
            self._write_json(
                output / integration.SUMMARY_NAME,
                {"decision": "pass", "protocol_valid": True},
            )
            self._write_json(output / integration.MANIFEST_NAME, {"schema": 1})

            bundle = evidence.write_evidence_bundle(output, source, attempt)
            persisted = json.loads(
                (output / integration.SUMMARY_NAME).read_text(encoding="utf-8")
            )

            self.assertEqual(persisted["decision"], "fail")
            self.assertTrue(any("终态" in reason for reason in persisted["reasons"]))
            with tarfile.open(bundle, "r:gz") as archive:
                self.assertIn("product/fd-rdd", archive.getnames())
                bundled = json.load(archive.extractfile(integration.SUMMARY_NAME))
            self.assertEqual(bundled["decision"], "fail")

    def test_dirty_harness_cannot_produce_a_passing_proof(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = self._source_suite(root)
            output = root / "integration"
            attempt = self._raw_attempt(output, source)
            result_path = attempt / integration.RESULT_NAME
            result = json.loads(result_path.read_text(encoding="utf-8"))
            result["harness_worktree_dirty"] = True
            self._write_json(result_path, result)

            summary = evidence.recompute_summary(output, source, attempt)

            self.assertEqual(summary["decision"], "fail")
            self.assertTrue(any("worktree" in reason for reason in summary["reasons"]))

    def test_evidence_rejects_tampered_product_binary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = self._source_suite(root)
            output = root / "integration"
            attempt = self._raw_attempt(output, source)
            (source / integration.PRODUCT_BINARY_RELATIVE).write_bytes(b"tampered")

            with self.assertRaises(integration.IntegrationEvidenceError):
                evidence.write_evidence_bundle(output, source, attempt)

    @mock.patch.object(
        integration_process, "_wait_group_gone", side_effect=[False, True]
    )
    @mock.patch.object(integration_process, "_group_alive", return_value=True)
    @mock.patch.object(integration_process.os, "killpg")
    def test_cleanup_uses_term_then_kill_fallback(
        self,
        killpg: mock.Mock,
        _group_alive: mock.Mock,
        _wait_group_gone: mock.Mock,
    ) -> None:
        integration_process.ensure_process_group_gone(4321)

        self.assertEqual(
            killpg.call_args_list,
            [mock.call(4321, signal.SIGTERM), mock.call(4321, signal.SIGKILL)],
        )

    def test_normal_and_exceptional_process_exits_both_cleanup_pgid(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            kwargs = {
                "product_git_sha": "a" * 40,
                "product_binary_sha256": "1" * 64,
                "product_receipt_sha256": "2" * 64,
                "harness_git_sha": "b" * 40,
                "harness_worktree_dirty": False,
                "source_suite": root / "r9",
                "expected_protocol_fingerprint": "3" * 64,
            }
            normal = mock.Mock(pid=1001)
            normal.wait.return_value = 0
            exceptional = mock.Mock(pid=1002)
            exceptional.wait.side_effect = KeyboardInterrupt
            with mock.patch.object(
                integration_process.subprocess,
                "Popen",
                side_effect=[normal, exceptional],
            ), mock.patch.object(
                integration_process, "ensure_process_group_gone"
            ) as normal_cleanup, mock.patch.object(
                integration_process, "terminate_and_reap_process_group"
            ) as exceptional_cleanup:
                integration_process.run_fixture_process(
                    ["fixture"], root / "attempt-01", **kwargs
                )
                with self.assertRaises(KeyboardInterrupt):
                    integration_process.run_fixture_process(
                        ["fixture"], root / "attempt-02", **kwargs
                    )

            normal_cleanup.assert_called_once_with(1001)
            exceptional_cleanup.assert_called_once_with(exceptional)
            terminal = json.loads(
                (root / "attempt-01" / integration.RESULT_NAME).read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(terminal["status"], "completed")
            self.assertEqual(terminal["product_git_sha"], "a" * 40)
            self.assertEqual(terminal["harness_git_sha"], "b" * 40)

    @mock.patch.object(integration_process, "ensure_process_group_gone")
    @mock.patch.object(integration_process, "_group_alive", return_value=True)
    @mock.patch.object(integration_process.os, "killpg")
    def test_exception_cleanup_reaps_leader_and_verifies_descendants(
        self,
        killpg: mock.Mock,
        _group_alive: mock.Mock,
        ensure_gone: mock.Mock,
    ) -> None:
        process = mock.Mock(pid=9876)
        process.wait.side_effect = [
            integration_process.subprocess.TimeoutExpired("fixture", 90),
            0,
        ]

        integration_process.terminate_and_reap_process_group(process)

        self.assertEqual(
            killpg.call_args_list,
            [mock.call(9876, signal.SIGTERM), mock.call(9876, signal.SIGKILL)],
        )
        ensure_gone.assert_called_once_with(9876)


if __name__ == "__main__":
    unittest.main()
