#!/usr/bin/env python3
"""Focused tests for the M2 falsification evidence bundle."""

from __future__ import annotations

import contextlib
import hashlib
import io
import json
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import m2_cold_window_falsification as falsification
import m2_cold_window_falsification_runner as runner
from m2_cold_window_build_receipt import SCHEMA as BUILD_RECEIPT_SCHEMA


class EvidenceBundleTests(unittest.TestCase):
    @staticmethod
    def _write_json(path: Path, value: object) -> None:
        path.write_text(json.dumps(value) + "\n", encoding="utf-8")

    @staticmethod
    def _write_outputs(
        suite_dir: Path,
        summary: dict[str, object],
    ) -> Path | None:
        with mock.patch.object(runner, "render_report", return_value="# report\n"):
            return runner._write_outputs(suite_dir, summary)

    def _suite(self, root: Path) -> tuple[Path, dict[str, object], list[Path]]:
        suite_dir = root / "suite"
        suite_dir.mkdir()
        receipt_path = suite_dir / runner.BUILD_RECEIPT_NAME
        receipt_path.write_text(
            json.dumps(
                {"schema": BUILD_RECEIPT_SCHEMA, "build_succeeded": True}
            )
            + "\n",
            encoding="utf-8",
        )
        receipt_sha256 = hashlib.sha256(receipt_path.read_bytes()).hexdigest()
        legs: list[dict[str, object]] = []
        attempts: list[Path] = []
        for index, spec in enumerate(
            falsification.build_leg_specs(suite_dir, 42),
            1,
        ):
            attempt = spec.base_dir / f"attempt-{index:02d}"
            attempt.mkdir(parents=True)
            attempts.append(attempt)
            event_root = attempt / "fixture"
            manifest = {
                "run_state": "completed",
                "completion_reason": "duration_elapsed",
                "ab_comparable": True,
                "run_dir": str(attempt),
                "runner_args": {
                    "watch_mode": "tiered",
                    "leg": index,
                    "event_storm_root": [str(event_root)],
                    "event_storm_fixed_root_schedule": True,
                    "event_storm_deterministic_plan": True,
                },
                "command": ["fd-rdd", "--watch-mode", "tiered"],
                "config": str(
                    attempt / "config-home" / "fd-rdd" / "config.toml"
                ),
                "ab_parameter_fingerprint": f"fingerprint-{index}",
                "execution_fingerprint": f"execution-{index}",
                "git_sha": "1" * 40,
                "binary_sha256": "2" * 64,
                "artifact_provenance": {
                    "receipt_sha256": receipt_sha256,
                    "cargo_lock_sha256": "4" * 64,
                    "validated_binary_sha256": "2" * 64,
                },
                "fixture": {"identity_sha256": "5" * 64},
                "ab_parameter_fingerprint_inputs": {
                    "runner_args": {"watch_mode": "tiered", "leg": index}
                },
            }
            self._write_json(attempt / "manifest.json", manifest)
            self._write_json(
                attempt / "summary.json",
                {
                    "fd_rdd_exit_code": 0,
                    "ab_comparable": True,
                    "process": {
                        "sample_count": 2,
                        "sample_first_elapsed_secs": 1.0,
                        "sample_last_elapsed_secs": 2.0,
                        "sample_coverage_secs": 1.0,
                        "sample_coverage_ratio": 1.0,
                    },
                    "process_after_event_storm_start": {"sample_count": 1},
                    "watch_state": {},
                    "shutdown_snapshot_quiesce": {},
                },
            )
            self._write_json(
                attempt / runner.WRAPPER_RESULT_NAME,
                {
                    "schema": runner.WRAPPER_RESULT_SCHEMA,
                    "status": "passed",
                    "wrapper_exit_code": 0,
                    "variant": spec.variant,
                    "profile": "falsification",
                    "run_dir": str(attempt),
                },
            )
            (attempt / "event-storm-samples.jsonl").write_text(
                "".join(
                    json.dumps(row) + "\n"
                    for row in (
                        {
                            "event_kind": "burst_written",
                            "cycle": 1,
                            "root": str(event_root),
                            "requested_tier": "L3",
                            "tier_before": "L3",
                            "selected_kind": "rw100",
                            "events_total": 100,
                            "within_budget": True,
                        },
                        {
                            "event_kind": "burst_checked",
                            "cycle": 1,
                            "query_phase": "delayed",
                        },
                        {
                            "event_kind": "first_query",
                            "cycle": 1,
                            "query_phase": "delayed",
                            "workload": "rw100",
                            "operation": "create",
                            "path": str(event_root / "created.txt"),
                            "query": "created.txt",
                            "should_exist": True,
                            "mutation_seq": 1,
                            "transport_ok": True,
                            "correct": True,
                        },
                        {
                            "event_kind": "visibility_probe",
                            "workload": "rw100",
                            "path": str(event_root / "created.txt"),
                            "visible": True,
                            "latency_secs": 0.1,
                        },
                    )
                ),
                encoding="utf-8",
            )
            (attempt / "fd-rdd.log").write_text(
                "fd-rdd ready.\n", encoding="utf-8"
            )
            config = attempt / "config-home" / "fd-rdd" / "config.toml"
            config.parent.mkdir(parents=True)
            config.write_text("[general]\n", encoding="utf-8")
            metrics = attempt / "reports" / "metrics"
            metrics.mkdir(parents=True)
            (metrics / "metrics.jsonl").write_text(
                '{"rss_bytes":1}\n',
                encoding="utf-8",
            )
            (attempt.parent / f"{attempt.name}.runner.log").write_text(
                "runner\n",
                encoding="utf-8",
            )
            legs.append(falsification.analyze_leg(spec, attempt))
        summary: dict[str, object] = {
            "schema": 1,
            "suite_dir": str(suite_dir),
            "sequence_seed": 42,
            "orders": ["".join(order) for order in falsification.balanced_block_orders(42)],
            "legs": legs,
            "infrastructure_error": "",
            "gate": {"decision": "pass", "reasons": []},
        }
        return suite_dir, summary, attempts

    def _assert_bundle_rejected(
        self,
        suite_dir: Path,
        summary: dict[str, object],
        expected_reason: str,
    ) -> None:
        bundle = self._write_outputs(suite_dir, summary)
        self.assertIsNone(bundle)
        persisted = json.loads(
            (suite_dir / "summary.json").read_text(encoding="utf-8")
        )
        self.assertEqual(persisted["gate"]["decision"], "fail")
        self.assertTrue(
            any(expected_reason in reason for reason in persisted["gate"]["reasons"]),
            persisted["gate"]["reasons"],
        )

    @staticmethod
    def _member_bytes(archive: tarfile.TarFile, name: str) -> bytes:
        stream = archive.extractfile(name)
        if stream is None:
            raise AssertionError(f"missing archive member: {name}")
        return stream.read()

    def test_success_bundle_is_closed_over_final_eight_run_dirs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            suite_dir, summary, attempts = self._suite(Path(tmp))
            historical = attempts[0].parent / "attempt-99"
            historical.mkdir()
            (historical / "manifest.json").write_text("{}\n", encoding="utf-8")
            (historical / "fd-rdd.log").write_text("old\n", encoding="utf-8")

            bundle = self._write_outputs(suite_dir, summary)

            self.assertIsNotNone(bundle)
            assert bundle is not None
            prefix = suite_dir.name
            with tarfile.open(bundle, "r:gz") as archive:
                names = set(archive.getnames())
                for required in (
                    "summary.json",
                    "manifest.json",
                    "REPORT.md",
                    runner.BUILD_RECEIPT_NAME,
                ):
                    self.assertIn(f"{prefix}/{required}", names)
                self.assertIn(f"{prefix}/evidence-manifest.json", names)
                self.assertIn(f"{prefix}/SHA256SUMS", names)
                self.assertFalse(any("attempt-99" in name for name in names))
                for attempt in attempts:
                    relative = attempt.relative_to(suite_dir)
                    base = Path(prefix) / relative
                    for required in (
                        "manifest.json",
                        "summary.json",
                        runner.WRAPPER_RESULT_NAME,
                        "event-storm-samples.jsonl",
                        "fd-rdd.log",
                        "runtime-config.json",
                    ):
                        self.assertIn(str(base / required), names)
                    self.assertIn(
                        str(base / "config-home" / "fd-rdd" / "config.toml"),
                        names,
                    )
                    self.assertIn(
                        str(base / "reports" / "metrics" / "metrics.jsonl"),
                        names,
                    )
                    self.assertIn(
                        str(
                            Path(prefix)
                            / attempt.relative_to(suite_dir).parent
                            / f"{attempt.name}.runner.log"
                        ),
                        names,
                    )
                inventory = json.loads(
                    self._member_bytes(
                        archive,
                        f"{prefix}/evidence-manifest.json",
                    )
                )
                checksum_text = self._member_bytes(
                    archive,
                    f"{prefix}/SHA256SUMS",
                ).decode("utf-8")
                checksums = {
                    line.split("  ", 1)[1]: line.split("  ", 1)[0]
                    for line in checksum_text.splitlines()
                }
                self.assertEqual(len(inventory["referenced_run_dirs"]), 8)
                self.assertEqual(
                    {member["path"] for member in inventory["members"]},
                    set(checksums) - {f"{prefix}/evidence-manifest.json"},
                )
                for name, expected in checksums.items():
                    actual = hashlib.sha256(self._member_bytes(archive, name)).hexdigest()
                    self.assertEqual(actual, expected, name)

    def test_line_delimited_metrics_with_json_suffix_are_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            suite_dir, summary, attempts = self._suite(Path(tmp))
            metrics_dir = attempts[0] / "reports" / "metrics"
            source = metrics_dir / "metrics.jsonl"
            target = metrics_dir / "metrics_2026-08-01_06.json"
            target.write_text(
                source.read_text(encoding="utf-8") * 2,
                encoding="utf-8",
            )
            source.unlink()

            bundle = self._write_outputs(suite_dir, summary)

            self.assertIsNotNone(bundle)

    def test_missing_required_leg_member_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            suite_dir, summary, attempts = self._suite(Path(tmp))
            (attempts[3] / "event-storm-samples.jsonl").unlink()

            bundle = self._write_outputs(suite_dir, summary)

            self.assertIsNone(bundle)
            self.assertFalse(
                suite_dir.with_name(f"{suite_dir.name}-evidence.tar.gz").exists()
            )
            persisted = json.loads(
                (suite_dir / "summary.json").read_text(encoding="utf-8")
            )
            self.assertEqual(persisted["gate"]["decision"], "fail")
            self.assertTrue(
                any(
                    "event-storm-samples.jsonl" in reason
                    for reason in persisted["gate"]["reasons"]
                )
            )

    def test_missing_build_provenance_fails_a_successful_suite(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            suite_dir, summary, _ = self._suite(Path(tmp))
            (suite_dir / runner.BUILD_RECEIPT_NAME).unlink()

            bundle = self._write_outputs(suite_dir, summary)

            self.assertIsNone(bundle)
            persisted = json.loads(
                (suite_dir / "summary.json").read_text(encoding="utf-8")
            )
            self.assertEqual(persisted["gate"]["decision"], "fail")
            self.assertTrue(
                any(
                    runner.BUILD_RECEIPT_NAME in reason
                    for reason in persisted["gate"]["reasons"]
                )
            )

    def test_replaced_valid_build_provenance_fails_digest_binding(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            suite_dir, summary, _ = self._suite(Path(tmp))
            self._write_json(
                suite_dir / runner.BUILD_RECEIPT_NAME,
                {"schema": 1, "build_succeeded": True, "replacement": True},
            )

            self._assert_bundle_rejected(
                suite_dir,
                summary,
                "SHA-256 与八腿审计记录不一致",
            )

    def test_missing_runner_log_fails_a_successful_suite(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            suite_dir, summary, attempts = self._suite(Path(tmp))
            (attempts[0].parent / f"{attempts[0].name}.runner.log").unlink()

            bundle = self._write_outputs(suite_dir, summary)

            self.assertIsNone(bundle)
            persisted = json.loads(
                (suite_dir / "summary.json").read_text(encoding="utf-8")
            )
            self.assertEqual(persisted["gate"]["decision"], "fail")
            self.assertTrue(
                any("runner.log" in reason for reason in persisted["gate"]["reasons"])
            )

    def test_empty_required_leg_evidence_fails_closed(self) -> None:
        cases = (
            ("event-storm-samples.jsonl", "event-storm"),
            ("fd-rdd.log", "fd-rdd.log"),
            ("config-home/fd-rdd/config.toml", "config.toml"),
            ("reports/metrics/metrics.jsonl", "metrics"),
        )
        for relative, reason in cases:
            with self.subTest(relative=relative), tempfile.TemporaryDirectory() as tmp:
                suite_dir, summary, attempts = self._suite(Path(tmp))
                (attempts[0] / relative).write_text("", encoding="utf-8")
                self._assert_bundle_rejected(suite_dir, summary, reason)

    def test_invalid_json_and_jsonl_evidence_fails_closed(self) -> None:
        cases = (
            ("manifest.json", "manifest"),
            ("summary.json", "summary"),
            (runner.WRAPPER_RESULT_NAME, "wrapper"),
            ("event-storm-samples.jsonl", "event-storm"),
            ("reports/metrics/metrics.jsonl", "metrics"),
        )
        for relative, reason in cases:
            with self.subTest(relative=relative), tempfile.TemporaryDirectory() as tmp:
                suite_dir, summary, attempts = self._suite(Path(tmp))
                (attempts[0] / relative).write_text("{broken\n", encoding="utf-8")
                self._assert_bundle_rejected(suite_dir, summary, reason)

    def test_terminal_state_mismatch_fails_closed(self) -> None:
        mutations = (
            ("manifest.json", "run_state", "failed", "manifest"),
            ("manifest.json", "completion_reason", "interrupted", "manifest"),
            ("manifest.json", "ab_comparable", False, "manifest"),
            ("manifest.json", "run_dir", "/tmp/wrong", "manifest"),
            ("summary.json", "fd_rdd_exit_code", 1, "summary"),
            ("summary.json", "ab_comparable", False, "summary"),
            (runner.WRAPPER_RESULT_NAME, "schema", 99, "wrapper"),
            (runner.WRAPPER_RESULT_NAME, "variant", "wrong", "wrapper"),
            (runner.WRAPPER_RESULT_NAME, "profile", "standard", "wrapper"),
            (runner.WRAPPER_RESULT_NAME, "run_dir", "/tmp/wrong", "wrapper"),
            (runner.WRAPPER_RESULT_NAME, "status", "failed", "wrapper"),
        )
        for relative, key, value, reason in mutations:
            with self.subTest(relative=relative, key=key), tempfile.TemporaryDirectory() as tmp:
                suite_dir, summary, attempts = self._suite(Path(tmp))
                path = attempts[0] / relative
                payload = json.loads(path.read_text(encoding="utf-8"))
                payload[key] = value
                self._write_json(path, payload)
                self._assert_bundle_rejected(suite_dir, summary, reason)

    def test_log_without_daemon_ready_marker_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            suite_dir, summary, attempts = self._suite(Path(tmp))
            (attempts[0] / "fd-rdd.log").write_text(
                "daemon started but never became ready\n", encoding="utf-8"
            )
            self._assert_bundle_rejected(suite_dir, summary, "ready")

    def test_empty_runner_log_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            suite_dir, summary, attempts = self._suite(Path(tmp))
            (attempts[0].parent / f"{attempts[0].name}.runner.log").write_text(
                "", encoding="utf-8"
            )
            self._assert_bundle_rejected(suite_dir, summary, "runner.log")

    def test_missing_protocol_check_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            suite_dir, summary, attempts = self._suite(Path(tmp))
            events_path = attempts[0] / "event-storm-samples.jsonl"
            rows = [
                json.loads(line)
                for line in events_path.read_text(encoding="utf-8").splitlines()
                if line
            ]
            events_path.write_text(
                "".join(
                    json.dumps(row) + "\n"
                    for row in rows
                    if row.get("event_kind") != "burst_checked"
                ),
                encoding="utf-8",
            )
            self._assert_bundle_rejected(suite_dir, summary, "burst_checked")

    def test_reanalysis_mismatch_fails_closed(self) -> None:
        mutations = (
            (("protocol", "physical_bursts"), 99),
            (("correctness", "primary_assertions"), 99),
            (("resources", "sample_count"), 99),
            (("stability", "background_rebuild_count"), 99),
            (("audit", "event_plan_sha256"), "0" * 64),
        )
        for path, value in mutations:
            with self.subTest(path=path), tempfile.TemporaryDirectory() as tmp:
                suite_dir, summary, _ = self._suite(Path(tmp))
                leg = summary["legs"][0]
                assert isinstance(leg, dict)
                section = leg[path[0]]
                assert isinstance(section, dict)
                section[path[1]] = value
                self._assert_bundle_rejected(suite_dir, summary, path[1])

    def test_resume_rejection_invalidates_old_bundle_and_pass_documents(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            suite_dir = Path(tmp) / "suite"
            suite_dir.mkdir()
            bundle = suite_dir.with_name(f"{suite_dir.name}-evidence.tar.gz")
            bundle.write_bytes(b"old evidence")
            (suite_dir / "summary.json").write_text(
                json.dumps({"gate": {"decision": "pass", "reasons": []}}),
                encoding="utf-8",
            )
            (suite_dir / "manifest.json").write_text(
                json.dumps({"sequence_seed": 42, "decision": "pass"}),
                encoding="utf-8",
            )
            (suite_dir / "REPORT.md").write_text("pass\n", encoding="utf-8")
            args = mock.Mock(sequence_seed=7, skip_build=True)

            with mock.patch.object(
                runner,
                "render_report",
                return_value="# fail\n",
            ), contextlib.redirect_stderr(io.StringIO()):
                result = runner._run_suite(
                    args,
                    suite_dir,
                    falsification.build_leg_specs(suite_dir, 7),
                    suite_dir / runner.BUILD_RECEIPT_NAME,
                )

            self.assertEqual(result, 1)
            self.assertFalse(bundle.exists())
            persisted_manifest = json.loads(
                (suite_dir / "manifest.json").read_text(encoding="utf-8")
            )
            self.assertEqual(persisted_manifest["sequence_seed"], 42)
            self.assertEqual(persisted_manifest["decision"], "fail")
            persisted_summary = json.loads(
                (suite_dir / "summary.json").read_text(encoding="utf-8")
            )
            self.assertEqual(persisted_summary["gate"]["decision"], "fail")

    def test_new_round_invalidates_old_evidence_before_build_preparation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            suite_dir = Path(tmp) / "suite"
            suite_dir.mkdir()
            bundle = suite_dir.with_name(f"{suite_dir.name}-evidence.tar.gz")
            bundle.write_bytes(b"old evidence")
            (suite_dir / "summary.json").write_text(
                json.dumps({"gate": {"decision": "pass", "reasons": []}}),
                encoding="utf-8",
            )
            (suite_dir / "manifest.json").write_text(
                json.dumps({"sequence_seed": 42, "decision": "pass"}),
                encoding="utf-8",
            )
            (suite_dir / "REPORT.md").write_text("pass\n", encoding="utf-8")

            def inspect_preparation(_suite_dir: Path, _skip_build: bool) -> str:
                self.assertFalse(bundle.exists())
                self.assertFalse((suite_dir / "summary.json").exists())
                manifest = json.loads(
                    (suite_dir / "manifest.json").read_text(encoding="utf-8")
                )
                self.assertEqual(manifest["decision"], "pending")
                return "stop after invalidation"

            args = mock.Mock(sequence_seed=42, skip_build=True)
            with mock.patch.object(
                runner,
                "_prepare_build",
                side_effect=inspect_preparation,
            ), contextlib.redirect_stdout(io.StringIO()):
                result = runner._run_suite(
                    args,
                    suite_dir,
                    falsification.build_leg_specs(suite_dir, 42),
                    suite_dir / runner.BUILD_RECEIPT_NAME,
                )

            self.assertEqual(result, 1)

    def test_failure_document_write_error_cannot_leave_pass_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            suite_dir, summary, _ = self._suite(Path(tmp))
            real_write = runner._write_output_documents
            calls = 0

            def fail_second_write(path: Path, value: dict[str, object]) -> None:
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise OSError("second write failed")
                real_write(path, value)

            with mock.patch.object(
                runner,
                "_write_evidence_bundle",
                side_effect=OSError("archive failed"),
            ), mock.patch.object(
                runner,
                "_write_output_documents",
                side_effect=fail_second_write,
            ), mock.patch.object(
                runner,
                "render_report",
                return_value="# report\n",
            ):
                bundle = runner._write_outputs(suite_dir, summary)

            self.assertIsNone(bundle)
            for name in ("summary.json", "manifest.json"):
                path = suite_dir / name
                if path.exists():
                    persisted = json.loads(path.read_text(encoding="utf-8"))
                    decision = (
                        persisted.get("decision")
                        or persisted.get("gate", {}).get("decision")
                    )
                    self.assertNotEqual(decision, "pass")


if __name__ == "__main__":
    unittest.main()
