#!/usr/bin/env python3
"""Tests for the M2 paired falsification suite."""

from __future__ import annotations

import contextlib
import io
import json
import os
import signal
import tarfile
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import m2_cold_window_falsification as falsification
import m2_cold_window_falsification_gate as gate
import m2_cold_window_falsification_report as report
import m2_cold_window_falsification_runner as runner
from m2_cold_window_ab_result import write_wrapper_result


def write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


def synthetic_leg(
    block: int,
    variant: str,
    *,
    visibility_rate: float,
    positive_rate: float = 1.0,
    cpu_core_seconds: float = 10.0,
    read_bytes: int = 1000,
    tier_before: str = "L3",
    snapshot_rebuild: bool = False,
    soft_ratio: float = 0.0,
) -> dict[str, object]:
    positive_total = 406
    primary_total = 806
    event_roots = [
        f"/home/test/fd-rdd-m2-roots/cold-storm-{index:02d}"
        for index in range(1, 7)
    ]
    order = "ab" if block <= 2 else "ba"
    position = (
        1
        if (order == "ab" and variant == "a")
        or (order == "ba" and variant == "b")
        else 2
    )
    burst_workloads = (
        ("save100", 100),
        ("git_clone", 3),
        ("subtree_rename", 100),
    ) * 2
    burst_ok = [round(total * positive_rate) for _, total in burst_workloads]
    desired_positive_ok = round(positive_total * positive_rate)
    burst_ok[0] += desired_positive_ok - sum(burst_ok)
    positive_by_burst = {
        str(index): {
            "total": total,
            "ok": ok,
            "success_rate": round(ok / total, 4),
            "workloads": [workload],
        }
        for index, ((workload, total), ok) in enumerate(
            zip(burst_workloads, burst_ok), 1
        )
    }
    positive_by_workload: dict[str, dict[str, object]] = {}
    for workload in ("save100", "git_clone", "subtree_rename"):
        rows = [
            row
            for row in positive_by_burst.values()
            if row["workloads"] == [workload]
        ]
        total = sum(int(row["total"]) for row in rows)
        ok = sum(int(row["ok"]) for row in rows)
        positive_by_workload[workload] = {
            "total": total,
            "ok": ok,
            "success_rate": round(ok / total, 4),
            "workloads": [workload],
        }
    ordinal = (block - 1) * 2 + position
    started = datetime(2026, 7, 12, tzinfo=timezone.utc) + timedelta(
        minutes=(ordinal - 1) * 21
    )
    finished = started + timedelta(minutes=20)
    return {
        "block": block,
        "position": position,
        "order": order,
        "variant": variant,
        "run_dir": f"/tmp/suite/block-{block:02d}/{variant}",
        "valid": True,
        "correctness": {
            "primary_assertions": primary_total,
            "primary_unique_paths": primary_total,
            "positive_total": positive_total,
            "positive_ok": desired_positive_ok,
            "positive_success_rate": positive_rate,
            "positive_by_burst": positive_by_burst,
            "positive_by_workload": positive_by_workload,
            "negative_total": primary_total - positive_total,
            "negative_ok": primary_total - positive_total,
            "negative_success_rate": 1.0,
            "transport_failures": 0,
            "visibility_transport_failures": 0,
            "visibility_total": 38,
            "visibility_visible": round(38 * visibility_rate),
            "visibility_success_rate": visibility_rate,
            "visibility_p95_secs": 2.0 if visibility_rate == 1.0 else 0.0,
            "visibility_by_workload": {
                workload: {
                    "total": total,
                    "ok": round(total * visibility_rate),
                    "success_rate": visibility_rate,
                    "workloads": [workload],
                    "p95_secs": 2.0 if visibility_rate == 1.0 else 0.0,
                }
                for workload, total in (
                    ("save100", 16),
                    ("git_clone", 6),
                    ("subtree_rename", 16),
                )
            },
            "duplicate_primary_paths": 0,
        },
        "protocol": {
            "physical_bursts": 6,
            "checks": 6,
            "requested_tiers": ["L3"],
            "tier_before": {tier_before: 6},
            "configured_event_roots": event_roots,
            "event_roots": event_roots,
            "workloads": {
                "git_clone": 2,
                "save100": 2,
                "subtree_rename": 2,
            },
            "events_total": primary_total,
            "within_budget_bursts": 6,
            "target_m2_debug_ok_bursts": 6,
            "target_m2_entry_present_bursts": 6 if variant == "a" else 0,
            "target_m2_seen_bursts": 6 if variant == "a" else 0,
            "target_m2_active_bursts": 6 if variant == "a" else 0,
            "target_m2_unexpired_bursts": 6 if variant == "a" else 0,
            "target_m2_causal_bursts": 6 if variant == "a" else 0,
            "target_m2_actions": (
                {"ephemeral_watch": 6} if variant == "a" else {}
            ),
            "target_m2_causal_actions": (
                {"ephemeral_watch": 6} if variant == "a" else {}
            ),
            "mutation_sequence_count": primary_total,
            "mutation_sequence_contiguous": True,
            "visibility_poll_count": 4560,
            "fixed_root_schedule": True,
            "deterministic_event_plan": True,
            "event_plan_sha256": "2" * 64,
        },
        "resources": {
            "sample_count": 100,
            "window_source": "full_run",
            "sample_first_elapsed_secs": 0.5,
            "sample_last_elapsed_secs": 1200.0,
            "sample_coverage_secs": 1199.5,
            "sample_coverage_ratio": 0.9996,
            "sample_max_gap_secs": 0.6,
            "counter_regressions": 0,
            "cpu_core_seconds": cpu_core_seconds,
            "read_bytes_delta": read_bytes,
            "write_bytes_delta": 2000,
            "read_syscalls_delta": 100,
            "write_syscalls_delta": 100,
            "minor_faults_delta": 200,
            "major_faults_delta": 0,
            "rss_bytes_p95": 20_000_000,
            "event_window_sample_count": 80,
            "event_window_cpu_core_seconds": cpu_core_seconds * 0.8,
            "event_window_read_bytes_delta": int(read_bytes * 0.8),
            "event_window_write_bytes_delta": 1600,
            "event_window_read_syscalls_delta": 80,
            "event_window_write_syscalls_delta": 80,
            "event_window_minor_faults_delta": 160,
            "event_window_major_faults_delta": 0,
            "event_window_rss_bytes_p95": 20_000_000,
        },
        "mechanism": {
            "rotating_active_dirs_max": 1 if variant == "a" else 0,
            "rotating_cycle_progress_pct_max": 100 if variant == "a" else 0,
            "rotating_promoted_last": 1 if variant == "a" else 0,
            "rotating_scan_only_last": 0,
            "rotating_budget_blocked_last": 0,
        },
        "stability": {
            "snapshot_rebuild_observed": snapshot_rebuild,
            "snapshot_ready": True,
            "snapshot_last_daemon_error": "rebuild" if snapshot_rebuild else "",
            "waterline_soft_degraded_ratio": soft_ratio,
            "waterline_soft_degraded_last": False,
            "waterline_hard_degraded_samples": 0,
            "waterline_hard_degraded_last": False,
            "dirty_queue_len_last": 0,
            "log_error_count": 0,
            "direct_v7_unsupported_count": 0,
            "background_rebuild_count": 0,
            "bootstrap_background_rebuild_count": 0,
            "post_ready_background_rebuild_count": 0,
            "snapshot_log_window_valid": True,
            "snapshot_quiesce_background_rebuild_count": 0,
            "unattributed_post_ready_background_rebuild_count": 0,
            "waterline_trigger_count": 0,
            "waterline_recover_count": 0,
        },
        "audit": {
            "git_sha": "a" * 40,
            "binary_sha256": "b" * 64,
            "receipt_sha256": "f" * 64,
            "cargo_lock_sha256": "1" * 64,
            "validated_binary_sha256": "b" * 64,
            "initial_state_fingerprint": "c" * 64,
            "fixture_identity_sha256": "d" * 64,
            "protocol_fingerprint": "e" * 64,
            "event_plan_sha256": "2" * 64,
            "daemon_started_at": started.isoformat().replace("+00:00", "Z"),
            "finished_at": finished.isoformat().replace("+00:00", "Z"),
            "actual_duration_secs": 1200.0,
        },
    }


class SequenceTests(unittest.TestCase):
    def test_balanced_blocks_are_deterministic_and_counterbalanced(self) -> None:
        first = falsification.balanced_block_orders(42)
        second = falsification.balanced_block_orders(42)

        self.assertEqual(first, second)
        self.assertEqual(len(first), 4)
        self.assertEqual(first.count(("a", "b")), 2)
        self.assertEqual(first.count(("b", "a")), 2)

    def test_leg_specs_create_eight_unique_attempt_bases(self) -> None:
        specs = falsification.build_leg_specs(Path("/tmp/suite"), seed=42)

        self.assertEqual(len(specs), 8)
        self.assertEqual({spec.block for spec in specs}, {1, 2, 3, 4})
        self.assertEqual({spec.variant for spec in specs}, {"a", "b"})
        self.assertEqual(len({spec.base_dir for spec in specs}), 8)


class LegAnalysisTests(unittest.TestCase):
    def test_stability_summary_splits_bootstrap_and_post_ready_rebuilds(self) -> None:
        summary = falsification._stability_summary(
            {},
            {"ready": True},
            "\n".join(
                (
                    "fd-rdd ready. Query via: http://localhost:6060/search?q=keyword",
                    "Starting background rebuild: startup bootstrap",
                    "Starting background rebuild: runtime",
                )
            ),
        )

        self.assertEqual(summary["background_rebuild_count"], 2)
        self.assertEqual(summary["bootstrap_background_rebuild_count"], 1)
        self.assertEqual(summary["post_ready_background_rebuild_count"], 1)

    def test_stability_summary_only_attributes_snapshot_recovery_inside_log_window(
        self,
    ) -> None:
        prefix = "\n".join(
            (
                "fd-rdd ready. Query via: http://localhost:6060/search?q=keyword",
                "Starting background rebuild: full build requested (strategy=Serial)",
                "",
            )
        )
        quiesce = (
            "Starting background rebuild: snapshot recovery (strategy=Serial)\n"
        )
        log_text = prefix + quiesce
        summary = falsification._stability_summary(
            {},
            {
                "ready": True,
                "rebuild_observed": True,
                "daemon_log_window_valid": True,
                "daemon_log_offset_start": len(prefix.encode("utf-8")),
                "daemon_log_offset_end": len(log_text.encode("utf-8")),
            },
            log_text,
        )

        self.assertEqual(summary["post_ready_background_rebuild_count"], 2)
        self.assertEqual(summary["snapshot_quiesce_background_rebuild_count"], 1)
        self.assertEqual(
            summary["unattributed_post_ready_background_rebuild_count"], 1
        )
        self.assertTrue(summary["snapshot_log_window_valid"])

    def test_legacy_generic_rebuild_after_http_ready_is_not_bootstrap(self) -> None:
        summary = falsification._stability_summary(
            {},
            {"ready": True},
            "\n".join(
                (
                    "HTTP Query Server listening on 127.0.0.1:6060",
                    "Starting background rebuild: full build requested",
                    "fd-rdd ready. Query via: http://localhost:6060/search?q=keyword",
                )
            ),
        )

        self.assertEqual(summary["bootstrap_background_rebuild_count"], 0)
        self.assertEqual(summary["post_ready_background_rebuild_count"], 1)

    def test_protocol_fingerprint_excludes_only_the_treatment(self) -> None:
        base = {
            "duration_secs": 1200,
            "event_storm_kind": "save100,git_clone,subtree_rename",
            "rotating_cold_window": True,
        }
        treatment = {
            "ab_parameter_fingerprint_inputs": {"runner_args": base}
        }
        baseline = {
            "ab_parameter_fingerprint_inputs": {
                "runner_args": {**base, "rotating_cold_window": False}
            }
        }
        changed = {
            "ab_parameter_fingerprint_inputs": {
                "runner_args": {**base, "duration_secs": 600}
            }
        }

        self.assertEqual(
            falsification._protocol_fingerprint(treatment),
            falsification._protocol_fingerprint(baseline),
        )
        self.assertNotEqual(
            falsification._protocol_fingerprint(treatment),
            falsification._protocol_fingerprint(changed),
        )

    def test_analyze_leg_uses_unique_primary_paths_and_physical_bursts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            (run_dir / "manifest.json").write_text(
                json.dumps(
                    {
                        "run_state": "completed",
                        "completion_reason": "duration_elapsed",
                        "ab_comparable": True,
                    }
                ),
                encoding="utf-8",
            )
            (run_dir / "summary.json").write_text(
                json.dumps(
                    {
                        "fd_rdd_exit_code": 0,
                        "ab_comparable": True,
                        "process": {
                            "sample_count": 5,
                            "cpu_core_seconds": 2.5,
                            "read_bytes_delta": 100,
                            "write_bytes_delta": 120,
                            "read_syscalls_delta": 8,
                            "write_syscalls_delta": 9,
                            "minor_faults_delta": 4,
                            "major_faults_delta": 0,
                            "rss_bytes_p95": 20,
                        },
                        "process_after_event_storm_start": {
                            "sample_count": 4,
                            "cpu_core_seconds": 1.8,
                            "read_bytes_delta": 90,
                            "write_bytes_delta": 100,
                            "read_syscalls_delta": 7,
                            "write_syscalls_delta": 8,
                            "minor_faults_delta": 3,
                            "major_faults_delta": 0,
                            "rss_bytes_p95": 19,
                        },
                        "process_after_first_burst": {
                            "sample_count": 4,
                            "cpu_core_seconds": 1.5,
                            "read_bytes_delta": 80,
                            "write_bytes_delta": 90,
                            "minor_faults_delta": 3,
                            "major_faults_delta": 0,
                            "rss_bytes_p95": 18,
                        },
                        "watch_state": {
                            "waterline_soft_degraded_ratio": 0.0,
                            "waterline_soft_degraded_last": False,
                            "dirty_queue_len_last": 0,
                        },
                        "shutdown_snapshot_quiesce": {
                            "rebuild_observed": False,
                            "last_daemon_error": "",
                        },
                    }
                ),
                encoding="utf-8",
            )
            write_jsonl(
                run_dir / "event-storm-samples.jsonl",
                [
                    {
                        "event_kind": "burst_written",
                        "root": "/home/test/fd-rdd-m2-roots/cold-a",
                        "requested_tier": "L3",
                        "tier_before": "L3",
                    },
                    {
                        "event_kind": "burst_checked",
                        "query_phase": "delayed",
                        "visibility_poll_count": 10,
                    },
                    {
                        "event_kind": "first_query",
                        "path": "/fixture/a.txt",
                        "should_exist": True,
                        "first_query_exists": True,
                        "correct": True,
                        "query_phase": "delayed",
                    },
                    {
                        "event_kind": "first_query",
                        "path": "/fixture/a.txt",
                        "should_exist": True,
                        "first_query_exists": True,
                        "correct": True,
                        "query_phase": "immediate",
                    },
                    {
                        "event_kind": "visibility_probe",
                        "path": "/fixture/a.txt",
                        "visible": True,
                        "latency_secs": 1.2,
                    },
                ],
            )
            (run_dir / "fd-rdd.log").write_text("clean\n", encoding="utf-8")
            spec = falsification.LegSpec(
                block=1,
                position=1,
                variant="a",
                order="ab",
                base_dir=run_dir.parent / "logical-leg",
            )

            result = falsification.analyze_leg(spec, run_dir)

            self.assertTrue(result["valid"])
            self.assertEqual(result["correctness"]["primary_unique_paths"], 1)
            self.assertEqual(result["correctness"]["primary_assertions"], 1)
            self.assertEqual(result["correctness"]["duplicate_primary_paths"], 0)
            self.assertEqual(result["protocol"]["physical_bursts"], 1)
            self.assertEqual(result["protocol"]["checks"], 1)
            self.assertEqual(result["resources"]["window_source"], "full_run")
            self.assertEqual(result["resources"]["cpu_core_seconds"], 2.5)

    def test_transport_failure_cannot_count_as_a_correct_negative_result(self) -> None:
        result = falsification._analyze_correctness(
            [
                {
                    "event_kind": "first_query",
                    "query_phase": "delayed",
                    "path": "/fixture/gone.txt",
                    "should_exist": False,
                    "correct": True,
                    "transport_ok": False,
                    "error": "connection refused",
                }
            ]
        )

        self.assertEqual(result["negative_total"], 1)
        self.assertEqual(result["negative_ok"], 0)
        self.assertEqual(result["negative_success_rate"], 0.0)
        self.assertEqual(result["transport_failures"], 1)

    def test_visibility_transport_failures_are_preserved_per_unique_probe(self) -> None:
        result = falsification._analyze_correctness(
            [
                {
                    "event_kind": "visibility_probe",
                    "path": "/fixture/a.txt",
                    "visible": True,
                    "transport_failures": 2,
                },
                {
                    "event_kind": "visibility_probe",
                    "path": "/fixture/b.txt",
                    "visible": False,
                    "transport_failures": 1,
                },
            ]
        )

        self.assertEqual(result["visibility_transport_failures"], 3)

    def test_event_plan_fingerprint_preserves_execution_order(self) -> None:
        manifest = {
            "runner_args": {
                "event_storm_root": ["/fixture/cold-a"],
                "event_storm_fixed_root_schedule": True,
                "event_storm_deterministic_plan": True,
            }
        }
        rows = [
            {
                "event_kind": "first_query",
                "query_phase": "delayed",
                "cycle": cycle,
                "workload": "save100",
                "operation": "save_visible",
                "path": f"/fixture/{cycle}.txt",
                "query": f"{cycle}.txt",
                "should_exist": True,
            }
            for cycle in (1, 2)
        ]

        forward = falsification._protocol_summary(manifest, rows)
        reverse = falsification._protocol_summary(manifest, list(reversed(rows)))

        self.assertNotEqual(
            forward["event_plan_sha256"], reverse["event_plan_sha256"]
        )

    def test_target_m2_causality_requires_same_cycle_progress_after_fence(self) -> None:
        burst = {
            "target_m2_debug_ok": True,
            "target_m2_entry_present": True,
            "target_m2_active": True,
            "target_m2_action": "scan_only",
            "target_m2_cycle_id": 7,
            "target_m2_observed_unix_secs": 100,
            "target_m2_expires_unix_secs": 200,
            "target_m2_last_scan_unix_secs": 90,
            "target_m2_last_event_unix_secs": 80,
            "target_m2_scan_seq": 10,
            "target_m2_event_seq": 8,
            "mutation_completed_unix_secs": 110,
            "target_m2_fence_debug_ok": True,
            "target_m2_fence_entry_present": True,
            "target_m2_fence_active": True,
            "target_m2_fence_action": "scan_only",
            "target_m2_fence_cycle_id": 7,
            "target_m2_fence_scan_seq": 10,
            "target_m2_fence_event_seq": 8,
        }
        check = {
            "target_m2_after_debug_ok": True,
            "target_m2_after_entry_present": True,
            "target_m2_after_last_scan_unix_secs": 111,
            "target_m2_after_last_event_unix_secs": 80,
            "target_m2_after_scan_seq": 11,
            "target_m2_after_scan_cycle_id": 7,
            "target_m2_after_event_seq": 8,
            "target_m2_after_event_cycle_id": 6,
        }

        self.assertTrue(falsification._target_m2_causal(burst, check))
        self.assertFalse(
            falsification._target_m2_causal(
                {**burst, "target_m2_fence_active": False}, check
            )
        )
        self.assertFalse(
            falsification._target_m2_causal(
                burst,
                {**check, "target_m2_after_scan_seq": 10},
            )
        )
        self.assertFalse(
            falsification._target_m2_causal(
                burst,
                {**check, "target_m2_after_scan_cycle_id": 8},
            ),
            "progress from a different rotating lease cycle must not be attributed",
        )

    def test_target_m2_causality_ignores_wall_clock_rollback(self) -> None:
        burst = {
            "target_m2_debug_ok": True,
            "target_m2_entry_present": True,
            "target_m2_active": True,
            "target_m2_action": "scan_only",
            "target_m2_cycle_id": 4,
            "target_m2_observed_unix_secs": 111,
            "mutation_completed_unix_secs": 110,
            "target_m2_fence_debug_ok": True,
            "target_m2_fence_entry_present": True,
            "target_m2_fence_active": True,
            "target_m2_fence_action": "scan_only",
            "target_m2_fence_cycle_id": 4,
            "target_m2_scan_seq": 20,
            "target_m2_event_seq": 0,
            "target_m2_fence_scan_seq": 20,
            "target_m2_fence_event_seq": 0,
        }
        check = {
            "target_m2_after_debug_ok": True,
            "target_m2_after_entry_present": True,
            "target_m2_after_last_scan_unix_secs": 50,
            "target_m2_after_scan_seq": 20,
            "target_m2_after_scan_cycle_id": 4,
        }

        self.assertFalse(
            falsification._target_m2_causal(burst, check),
            "wall-clock movement cannot replace a monotonic M2 sequence advance",
        )

    def test_target_m2_causality_accepts_progress_inside_the_mutation_window(self) -> None:
        burst = {
            "target_m2_debug_ok": True,
            "target_m2_entry_present": True,
            "target_m2_active": True,
            "target_m2_action": "ephemeral_watch",
            "target_m2_cycle_id": 9,
            "target_m2_scan_seq": 40,
            "target_m2_event_seq": 41,
            "target_m2_fence_debug_ok": True,
            "target_m2_fence_entry_present": True,
            "target_m2_fence_active": True,
            "target_m2_fence_action": "ephemeral_watch",
            "target_m2_fence_cycle_id": 9,
            "target_m2_fence_scan_seq": 40,
            "target_m2_fence_scan_cycle_id": 8,
            "target_m2_fence_event_seq": 42,
            "target_m2_fence_event_cycle_id": 9,
        }
        check = {
            "target_m2_after_debug_ok": True,
            "target_m2_after_entry_present": True,
            "target_m2_after_scan_seq": 40,
            "target_m2_after_scan_cycle_id": 8,
            "target_m2_after_event_seq": 42,
            "target_m2_after_event_cycle_id": 9,
        }

        self.assertTrue(falsification._target_m2_causal(burst, check))

    def test_target_m2_causality_rejects_ordinary_scan_after_lease_expiry(self) -> None:
        burst = {
            "target_m2_debug_ok": True,
            "target_m2_entry_present": True,
            "target_m2_active": True,
            "target_m2_action": "scan_only",
            "target_m2_cycle_id": 2,
            "target_m2_fence_debug_ok": True,
            "target_m2_fence_entry_present": True,
            "target_m2_fence_active": True,
            "target_m2_fence_action": "scan_only",
            "target_m2_fence_cycle_id": 2,
            "target_m2_scan_seq": 30,
            "target_m2_event_seq": 0,
            "target_m2_fence_scan_seq": 30,
            "target_m2_fence_event_seq": 0,
        }
        check = {
            "target_m2_after_debug_ok": True,
            "target_m2_after_entry_present": True,
            "target_m2_after_last_scan_unix_secs": 130,
            "target_m2_after_scan_seq": 30,
            "target_m2_after_scan_cycle_id": 2,
        }

        self.assertFalse(falsification._target_m2_causal(burst, check))


class GateTests(unittest.TestCase):
    def passing_legs(self) -> list[dict[str, object]]:
        legs: list[dict[str, object]] = []
        for block in range(1, 5):
            legs.append(synthetic_leg(block, "a", visibility_rate=1.0))
            legs.append(
                synthetic_leg(
                    block,
                    "b",
                    visibility_rate=0.0,
                    positive_rate=0.8,
                    cpu_core_seconds=10.0,
                    read_bytes=1000,
                )
            )
        legs.sort(key=lambda leg: (int(leg["block"]), int(leg["position"])))
        return legs

    def test_gate_passes_only_when_benefit_repeats_without_cost_or_recovery_failure(self) -> None:
        result = gate.evaluate_suite(self.passing_legs())

        self.assertEqual(result["decision"], "pass")
        self.assertEqual(result["benefit_blocks"], 4)
        self.assertEqual(result["reasons"], [])

    def test_gate_fails_closed_on_unready_snapshot_or_unrecovered_waterline(self) -> None:
        legs = self.passing_legs()
        legs[0]["stability"]["snapshot_ready"] = False
        legs[2]["stability"]["waterline_soft_degraded_last"] = True

        result = gate.evaluate_suite(legs)

        self.assertEqual(result["decision"], "fail")
        self.assertTrue(any("snapshot quiesce" in reason for reason in result["reasons"]))
        self.assertTrue(any("waterline" in reason for reason in result["reasons"]))

    def test_gate_rejects_hard_waterline_or_background_rebuild(self) -> None:
        legs = self.passing_legs()
        legs[0]["stability"]["waterline_hard_degraded_samples"] = 1
        legs[2]["stability"]["background_rebuild_count"] = 1
        legs[2]["stability"]["post_ready_background_rebuild_count"] = 1

        result = gate.evaluate_suite(legs)

        self.assertEqual(result["decision"], "fail")
        self.assertTrue(any("hard waterline" in reason for reason in result["reasons"]))
        self.assertTrue(any("background rebuild" in reason for reason in result["reasons"]))

    def test_gate_accepts_one_snapshot_quiesce_rebuild_when_it_finishes_ready(self) -> None:
        legs = self.passing_legs()
        legs[0]["stability"]["snapshot_rebuild_observed"] = True
        legs[0]["stability"]["snapshot_log_window_valid"] = True
        legs[0]["stability"]["background_rebuild_count"] = 1
        legs[0]["stability"]["post_ready_background_rebuild_count"] = 1
        legs[0]["stability"]["snapshot_quiesce_background_rebuild_count"] = 1
        legs[0]["stability"][
            "unattributed_post_ready_background_rebuild_count"
        ] = 0
        legs[0]["stability"]["direct_v7_unsupported_count"] = 1

        result = gate.evaluate_suite(legs)

        self.assertEqual(result["decision"], "pass")

    def test_gate_rejects_runtime_rebuild_even_when_snapshot_boolean_is_true(self) -> None:
        legs = self.passing_legs()
        stability = legs[0]["stability"]
        stability["snapshot_rebuild_observed"] = True
        stability["snapshot_log_window_valid"] = True
        stability["background_rebuild_count"] = 1
        stability["post_ready_background_rebuild_count"] = 1
        stability["snapshot_quiesce_background_rebuild_count"] = 0
        stability["unattributed_post_ready_background_rebuild_count"] = 1

        result = gate.evaluate_suite(legs)

        self.assertEqual(result["decision"], "fail")
        self.assertTrue(
            any("未归因" in reason or "窗口" in reason for reason in result["reasons"])
        )

    def test_gate_accepts_bootstrap_rebuild_but_rejects_unattributed_post_ready_rebuild(self) -> None:
        legs = self.passing_legs()
        legs[0]["stability"]["background_rebuild_count"] = 1
        legs[0]["stability"]["bootstrap_background_rebuild_count"] = 1

        bootstrap_only = gate.evaluate_suite(legs)

        self.assertEqual(bootstrap_only["decision"], "pass")

        legs[0]["stability"]["post_ready_background_rebuild_count"] = 1
        post_ready = gate.evaluate_suite(legs)

        self.assertEqual(post_ready["decision"], "fail")
        self.assertTrue(
            any("background rebuild" in reason for reason in post_ready["reasons"])
        )

    def test_gate_rejects_repeated_bootstrap_rebuilds(self) -> None:
        legs = self.passing_legs()
        legs[0]["stability"]["background_rebuild_count"] = 2
        legs[0]["stability"]["bootstrap_background_rebuild_count"] = 2

        result = gate.evaluate_suite(legs)

        self.assertEqual(result["decision"], "fail")
        self.assertTrue(
            any("bootstrap background rebuild 超过一次" in reason for reason in result["reasons"])
        )

    def test_gate_treats_legacy_unclassified_rebuild_as_post_ready(self) -> None:
        legs = self.passing_legs()
        stability = legs[0]["stability"]
        stability["background_rebuild_count"] = 1
        del stability["bootstrap_background_rebuild_count"]
        del stability["post_ready_background_rebuild_count"]
        del stability["snapshot_log_window_valid"]
        del stability["snapshot_quiesce_background_rebuild_count"]
        del stability["unattributed_post_ready_background_rebuild_count"]

        result = gate.evaluate_suite(legs)

        self.assertEqual(result["decision"], "fail")
        self.assertTrue(
            any("未归因到 snapshot quiesce" in reason for reason in result["reasons"])
        )

    def test_gate_requires_exact_target_entry_only_for_treatment(self) -> None:
        legs = self.passing_legs()
        self.assertEqual(gate.evaluate_suite(legs)["decision"], "pass")

        treatment = next(leg for leg in legs if leg["variant"] == "a")
        treatment["protocol"]["target_m2_entry_present_bursts"] = 5

        result = gate.evaluate_suite(legs)

        self.assertEqual(result["decision"], "fail")
        self.assertTrue(any("目标目录 entry" in reason for reason in result["reasons"]))

    def test_gate_rejects_baseline_that_never_reached_requested_l3(self) -> None:
        legs = self.passing_legs()
        for leg in legs:
            if leg["variant"] == "b":
                leg["protocol"]["tier_before"] = {"L1": 3}

        result = gate.evaluate_suite(legs)

        self.assertEqual(result["decision"], "fail")
        self.assertTrue(any("B 组未在 L3" in reason for reason in result["reasons"]))

    def test_gate_rejects_cross_leg_binary_or_protocol_mismatch(self) -> None:
        legs = self.passing_legs()
        legs[1]["audit"]["binary_sha256"] = "f" * 64
        legs[3]["audit"]["protocol_fingerprint"] = "0" * 64

        result = gate.evaluate_suite(legs)

        self.assertEqual(result["decision"], "fail")
        self.assertTrue(any("binary" in reason for reason in result["reasons"]))
        self.assertTrue(any("协议参数" in reason for reason in result["reasons"]))

    def test_gate_rejects_incomplete_or_out_of_fixture_workload(self) -> None:
        legs = self.passing_legs()
        legs[0]["protocol"]["event_roots"] = ["/tmp/not-the-fixture"]
        legs[1]["protocol"]["workloads"] = {"save100": 3}

        result = gate.evaluate_suite(legs)

        self.assertEqual(result["decision"], "fail")
        self.assertTrue(any("固定六根池" in reason for reason in result["reasons"]))
        self.assertTrue(any("workload" in reason for reason in result["reasons"]))

    def test_gate_rejects_baseline_daemon_instability(self) -> None:
        legs = self.passing_legs()
        legs[1]["stability"]["log_error_count"] = 1

        result = gate.evaluate_suite(legs)

        self.assertEqual(result["decision"], "fail")
        self.assertTrue(any("B 组 daemon" in reason for reason in result["reasons"]))

    def test_gate_rejects_missing_full_run_resource_window(self) -> None:
        legs = self.passing_legs()
        legs[0]["resources"]["sample_count"] = 0

        result = gate.evaluate_suite(legs)

        self.assertEqual(result["decision"], "fail")
        self.assertTrue(any("资源窗口" in reason for reason in result["reasons"]))

    def test_gate_rejects_partial_or_regressing_process_sampling(self) -> None:
        legs = self.passing_legs()
        legs[0]["resources"]["sample_coverage_ratio"] = 0.5
        legs[2]["resources"]["sample_max_gap_secs"] = 4.0
        legs[4]["resources"]["counter_regressions"] = 1

        result = gate.evaluate_suite(legs)

        self.assertEqual(result["decision"], "fail")
        self.assertTrue(any("采样覆盖率" in reason for reason in result["reasons"]))
        self.assertTrue(any("最大间隔" in reason for reason in result["reasons"]))
        self.assertTrue(any("计数器发生回退" in reason for reason in result["reasons"]))

    def test_gate_reports_paired_resource_and_roi_observations(self) -> None:
        result = gate.evaluate_suite(self.passing_legs())

        self.assertEqual(result["median_rss_p95_ratio"], 1.0)
        self.assertEqual(result["median_write_bytes_ratio"], 1.0)
        self.assertEqual(result["median_minor_faults_ratio"], 1.0)
        self.assertEqual(result["median_query_poll_load_ratio"], 1.0)
        self.assertEqual(result["paired"][0]["recovered_primary_paths"], 81)
        self.assertEqual(result["paired"][0]["cpu_seconds_per_recovered_path"], 0.0)

    def test_gate_rejects_write_or_query_load_asymmetry(self) -> None:
        legs = self.passing_legs()
        for leg in legs:
            if leg["variant"] == "a":
                leg["resources"]["write_bytes_delta"] = 3000
                leg["protocol"]["visibility_poll_count"] = 5000

        result = gate.evaluate_suite(legs)

        self.assertEqual(result["decision"], "fail")
        self.assertTrue(any("write_bytes" in reason for reason in result["reasons"]))
        self.assertTrue(any("polling" in reason for reason in result["reasons"]))

    def test_gate_rejects_one_pathological_block_hidden_by_the_median(self) -> None:
        legs = self.passing_legs()
        legs[0]["resources"]["cpu_core_seconds"] = 20.0

        result = gate.evaluate_suite(legs)

        self.assertEqual(result["median_cpu_ratio"], 1.0)
        self.assertEqual(result["decision"], "fail")
        self.assertTrue(
            any("block 1 配对 CPU" in reason for reason in result["reasons"])
        )

    def test_gate_uses_ratio_and_absolute_threshold_for_rss_and_minor_faults(self) -> None:
        legs = self.passing_legs()
        for leg in legs:
            if leg["variant"] == "a":
                leg["resources"]["rss_bytes_p95"] = 60_000_000
                leg["resources"]["minor_faults_delta"] = 20_500

        result = gate.evaluate_suite(legs)

        self.assertEqual(result["decision"], "fail")
        self.assertTrue(any("RSS p95" in reason for reason in result["reasons"]))
        self.assertTrue(any("minor faults" in reason for reason in result["reasons"]))

    def test_gate_rejects_missing_syscall_counter_signal(self) -> None:
        legs = self.passing_legs()
        legs[0]["resources"]["read_syscalls_delta"] = 0

        result = gate.evaluate_suite(legs)

        self.assertEqual(result["decision"], "fail")
        self.assertTrue(any("read syscall" in reason for reason in result["reasons"]))

    def test_gate_does_not_count_a_tiny_success_rate_delta_as_benefit(self) -> None:
        legs = self.passing_legs()
        for leg in legs:
            if leg["variant"] == "b":
                leg["correctness"]["positive_success_rate"] = 0.99
                leg["correctness"]["visibility_success_rate"] = 0.98

        result = gate.evaluate_suite(legs)

        self.assertEqual(result["benefit_blocks"], 0)
        self.assertEqual(result["decision"], "fail")

    def test_gate_does_not_treat_visibility_only_gain_as_primary_benefit(self) -> None:
        legs = self.passing_legs()
        for leg in legs:
            if leg["variant"] == "b":
                leg["correctness"]["positive_ok"] = 406
                leg["correctness"]["positive_success_rate"] = 1.0

        result = gate.evaluate_suite(legs)

        self.assertEqual(result["benefit_blocks"], 0)
        self.assertTrue(
            all(row["recovered_primary_paths"] == 0 for row in result["paired"])
        )

    def test_gate_requires_benefit_across_multiple_bursts_and_workloads(self) -> None:
        legs = self.passing_legs()
        for leg in legs:
            if leg["variant"] != "b":
                continue
            for burst, row in leg["correctness"]["positive_by_burst"].items():
                if burst == "1":
                    row["ok"] = 0
                    row["success_rate"] = 0.0
                else:
                    row["ok"] = row["total"]
                    row["success_rate"] = 1.0
            leg["correctness"]["positive_ok"] = 306
            leg["correctness"]["positive_success_rate"] = round(306 / 406, 4)

        result = gate.evaluate_suite(legs)

        self.assertEqual(result["benefit_blocks"], 0)
        self.assertTrue(
            all(row["benefited_burst_count"] == 1 for row in result["paired"])
        )

    def test_gate_rejects_slow_a_visibility_even_when_every_probe_recovers(self) -> None:
        legs = self.passing_legs()
        legs[0]["correctness"]["visibility_p95_secs"] = 119.9

        result = gate.evaluate_suite(legs)

        self.assertEqual(result["decision"], "fail")
        self.assertTrue(any("35 秒 SLA" in reason for reason in result["reasons"]))

    def test_gate_rejects_visibility_transport_failure(self) -> None:
        legs = self.passing_legs()
        legs[0]["correctness"]["visibility_transport_failures"] = 1

        result = gate.evaluate_suite(legs)

        self.assertEqual(result["decision"], "fail")
        self.assertTrue(any("visibility polling" in reason for reason in result["reasons"]))

    def test_gate_accepts_active_leases_after_seen_cycle_resets(self) -> None:
        legs = self.passing_legs()
        for leg in legs:
            if leg["variant"] == "a":
                leg["protocol"]["target_m2_seen_bursts"] = 0

        result = gate.evaluate_suite(legs)

        self.assertEqual(result["decision"], "pass")

    def test_gate_rejects_historical_seen_without_live_causal_m2_evidence(self) -> None:
        legs = self.passing_legs()
        treatment = legs[0]["protocol"]
        treatment["target_m2_active_bursts"] = 0
        treatment["target_m2_unexpired_bursts"] = 0
        treatment["target_m2_causal_bursts"] = 0
        treatment["target_m2_actions"] = {}

        result = gate.evaluate_suite(legs)

        self.assertEqual(result["decision"], "fail")
        self.assertTrue(any("有效 M2 租约" in reason for reason in result["reasons"]))
        self.assertTrue(any("写入后的 M2" in reason for reason in result["reasons"]))

    def test_gate_rejects_cross_session_pairing_even_if_metadata_order_is_valid(self) -> None:
        legs = self.passing_legs()
        legs[1]["audit"]["daemon_started_at"] = "2026-07-13T00:00:00Z"
        legs[1]["audit"]["finished_at"] = "2026-07-13T00:20:00Z"

        result = gate.evaluate_suite(legs)

        self.assertEqual(result["decision"], "fail")
        self.assertTrue(any("跨会话拼接" in reason for reason in result["reasons"]))

    def test_gate_rejects_negative_errors_or_wrong_mechanism_activity(self) -> None:
        legs = self.passing_legs()
        legs[1]["correctness"]["negative_success_rate"] = 0.99
        legs[2]["mechanism"]["rotating_active_dirs_max"] = 0
        legs[2]["mechanism"]["rotating_cycle_progress_pct_max"] = 0
        legs[2]["mechanism"]["rotating_promoted_last"] = 0
        legs[3]["mechanism"]["rotating_scan_only_last"] = 1

        result = gate.evaluate_suite(legs)

        self.assertEqual(result["decision"], "fail")
        self.assertTrue(any("负向主断言" in reason for reason in result["reasons"]))
        self.assertTrue(any("未观测到 M2 活动" in reason for reason in result["reasons"]))
        self.assertTrue(any("B 组出现 M2 活动" in reason for reason in result["reasons"]))

    def test_gate_rejects_duplicate_or_extra_legs(self) -> None:
        legs = self.passing_legs()
        legs.append(dict(legs[0]))

        result = gate.evaluate_suite(legs)

        self.assertEqual(result["decision"], "fail")
        self.assertTrue(any("必须精确包含 8 腿" in reason for reason in result["reasons"]))
        self.assertTrue(any("重复腿" in reason for reason in result["reasons"]))

    def test_gate_rejects_unbalanced_or_inconsistent_block_order(self) -> None:
        legs = self.passing_legs()
        for leg in legs:
            leg["order"] = "ab"
            leg["position"] = 1 if leg["variant"] == "a" else 2

        result = gate.evaluate_suite(legs)

        self.assertEqual(result["decision"], "fail")
        self.assertTrue(any("2×AB + 2×BA" in reason for reason in result["reasons"]))

    def test_gate_rejects_a_shuffled_execution_record(self) -> None:
        legs = self.passing_legs()
        legs[0], legs[1] = legs[1], legs[0]

        result = gate.evaluate_suite(legs)

        self.assertEqual(result["decision"], "fail")
        self.assertTrue(any("实际执行顺序" in reason for reason in result["reasons"]))

    def test_gate_rejects_a_wrong_positive_negative_partition(self) -> None:
        legs = self.passing_legs()
        legs[0]["correctness"]["positive_total"] = 806
        legs[0]["correctness"]["negative_total"] = 0

        result = gate.evaluate_suite(legs)

        self.assertEqual(result["decision"], "fail")
        self.assertTrue(any("正负断言分区" in reason for reason in result["reasons"]))

    def test_incomplete_gate_result_remains_strict_json(self) -> None:
        result = gate.evaluate_suite([])

        encoded = json.dumps(result, allow_nan=False)

        self.assertIn('"decision": "fail"', encoded)
        self.assertIsNone(result["median_cpu_ratio"])


class DriverTests(unittest.TestCase):
    def test_suite_outputs_include_a_bounded_single_file_evidence_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            suite_dir = Path(tmp) / "suite"
            suite_dir.mkdir()
            specs = falsification.build_leg_specs(suite_dir, 42)
            legs: list[dict[str, object]] = []
            (suite_dir / runner.BUILD_RECEIPT_NAME).write_text(
                json.dumps({"build_succeeded": True}) + "\n",
                encoding="utf-8",
            )
            attempts: list[Path] = []
            for spec in specs:
                attempt = spec.base_dir / "attempt-01"
                attempt.mkdir(parents=True)
                attempts.append(attempt)
                leg = synthetic_leg(
                    spec.block,
                    spec.variant,
                    visibility_rate=1.0 if spec.variant == "a" else 0.0,
                    positive_rate=1.0 if spec.variant == "a" else 0.8,
                )
                leg.update(
                    {
                        "position": spec.position,
                        "order": spec.order,
                        "run_dir": str(attempt),
                    }
                )
                legs.append(leg)
                (attempt / "manifest.json").write_text(
                    json.dumps(
                        {
                            "run_state": "completed",
                            "runner_args": {"watch_mode": "tiered"},
                            "command": ["fd-rdd", "--watch-mode", "tiered"],
                            "config": str(
                                attempt / "config-home" / "fd-rdd" / "config.toml"
                            ),
                        }
                    )
                    + "\n",
                    encoding="utf-8",
                )
                (attempt / "summary.json").write_text("{}\n", encoding="utf-8")
                (attempt / runner.WRAPPER_RESULT_NAME).write_text(
                    "{}\n", encoding="utf-8"
                )
                (attempt / "event-storm-samples.jsonl").write_text(
                    '{"event_kind":"burst_written"}\n', encoding="utf-8"
                )
                (attempt / "fd-rdd.log").write_text("ready\n", encoding="utf-8")
                for name in runner.EVIDENCE_OPTIONAL_ATTEMPT_FILES:
                    (attempt / name).write_text(f"{name}\n", encoding="utf-8")
                config = attempt / "config-home" / "fd-rdd" / "config.toml"
                config.parent.mkdir(parents=True)
                config.write_text("[general]\n", encoding="utf-8")
                metric = attempt / "reports" / "metrics" / "metrics.json"
                metric.parent.mkdir(parents=True)
                metric.write_text("{}\n", encoding="utf-8")
                runner_log = attempt.parent / "attempt-01.runner.log"
                runner_log.write_text("runner\n", encoding="utf-8")
            first_run = attempts[0]
            artifact = first_run / "artifact"
            artifact.mkdir()
            (artifact / "fd-rdd").write_bytes(b"binary")
            snapshot = first_run / "index.d"
            snapshot.mkdir()
            (snapshot / "manifest.json").write_text("secret\n", encoding="utf-8")
            build_target = suite_dir / "build-target"
            build_target.mkdir()
            (build_target / "fd-rdd").write_bytes(b"build")
            rogue_attempt = suite_dir / "rogue" / "attempt-secret"
            rogue_attempt.mkdir(parents=True)
            (rogue_attempt / "fd-rdd.log").write_text("rogue\n", encoding="utf-8")
            (suite_dir / "rogue" / "secret.runner.log").write_text(
                "rogue\n",
                encoding="utf-8",
            )
            outside = Path(tmp) / "outside.log"
            outside.write_text("outside\n", encoding="utf-8")
            symlink_attempt = specs[0].base_dir / "attempt-99"
            symlink_attempt.mkdir()
            (symlink_attempt / "fd-rdd.log").symlink_to(outside)
            summary = runner._suite_summary(suite_dir, 42, legs, "")

            bundle = runner._write_outputs(suite_dir, summary)

            self.assertIsNotNone(bundle)
            assert bundle is not None
            self.assertEqual(
                bundle,
                suite_dir.with_name("suite-evidence.tar.gz"),
            )
            with tarfile.open(bundle, "r:gz") as archive_file:
                names = set(archive_file.getnames())
            self.assertIn("suite/summary.json", names)
            for spec in specs:
                relative = spec.base_dir.relative_to(suite_dir)
                self.assertIn(
                    str(Path("suite") / relative / "attempt-01" / "manifest.json"),
                    names,
                )
                self.assertIn(
                    str(Path("suite") / relative / "attempt-01.runner.log"),
                    names,
                )
            first_relative = first_run.relative_to(suite_dir)
            self.assertIn(
                str(Path("suite") / first_relative / "ab-wrapper-result.json"),
                names,
            )
            self.assertIn(
                str(
                    Path("suite")
                    / first_relative
                    / "config-home"
                    / "fd-rdd"
                    / "config.toml"
                ),
                names,
            )
            self.assertIn(
                str(
                    Path("suite")
                    / first_relative
                    / "reports"
                    / "metrics"
                    / "metrics.json"
                ),
                names,
            )
            self.assertFalse(any("/artifact/" in name for name in names))
            self.assertFalse(any("/build-target/" in name for name in names))
            self.assertFalse(any("/index.d/" in name for name in names))
            self.assertFalse(any("/rogue/" in name for name in names))
            self.assertFalse(any("attempt-99/fd-rdd.log" in name for name in names))

    def test_evidence_bundle_failure_invalidates_the_old_bundle_and_suite(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            suite_dir = Path(tmp) / "suite"
            suite_dir.mkdir()
            summary = runner._suite_summary(
                suite_dir,
                42,
                [],
                "forced failure",
            )
            bundle = runner._write_outputs(suite_dir, summary)
            self.assertIsNotNone(bundle)
            assert bundle is not None and bundle.exists()

            with mock.patch.object(
                runner.tarfile,
                "open",
                side_effect=OSError("disk full"),
            ):
                failed_bundle = runner._write_outputs(suite_dir, summary)

            self.assertIsNone(failed_bundle)
            self.assertFalse(bundle.exists())
            persisted = json.loads(
                (suite_dir / "summary.json").read_text(encoding="utf-8")
            )
            self.assertEqual(persisted["gate"]["decision"], "fail")
            self.assertTrue(
                any(
                    "证据包生成失败：OSError: disk full" in reason
                    for reason in persisted["gate"]["reasons"]
                )
            )

    def test_failed_suite_prints_each_gate_reason_and_bundle_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            suite_dir = Path(tmp) / "suite"
            args = mock.Mock(sequence_seed=42, skip_build=True)
            specs = falsification.build_leg_specs(suite_dir, 42)
            output = io.StringIO()
            with mock.patch.object(
                runner, "_prepare_build", return_value="forced infrastructure failure"
            ), contextlib.redirect_stdout(output):
                result = runner._run_suite(
                    args,
                    suite_dir,
                    specs,
                    suite_dir / "build-provenance.json",
                )

            self.assertEqual(result, 1)
            self.assertIn("gate_reason: forced infrastructure failure", output.getvalue())
            self.assertIn("evidence_bundle:", output.getvalue())

    def test_leg_command_forwards_the_shared_build_receipt(self) -> None:
        spec = falsification.build_leg_specs(Path("/tmp/suite"), seed=42)[0]
        receipt = Path("/tmp/suite/build-provenance.json")
        binary = runner.suite_binary(Path("/tmp/suite"))

        command = runner.leg_command(
            spec,
            spec.base_dir / "attempt-01",
            receipt,
            binary,
        )

        self.assertIn("--artifact-provenance-receipt", command)
        receipt_index = command.index("--artifact-provenance-receipt")
        self.assertEqual(command[receipt_index + 1], str(receipt))
        binary_index = command.index("--binary")
        self.assertEqual(command[binary_index + 1], str(binary))

    def test_wrapper_process_forwards_sigterm_before_propagating_interrupt(self) -> None:
        process = mock.Mock()
        process.wait.side_effect = [
            runner.TerminationRequested(signal.SIGTERM),
            143,
        ]
        process.poll.return_value = None
        with mock.patch.object(runner.subprocess, "Popen", return_value=process) as popen:
            with self.assertRaises(runner.TerminationRequested):
                runner.run_wrapper_process(["wrapper"])

        process.send_signal.assert_called_once_with(signal.SIGTERM)
        self.assertTrue(popen.call_args.kwargs["start_new_session"])

    def test_wrapper_timeout_cleans_recorded_daemon_before_killing_wrapper_group(self) -> None:
        process = mock.Mock(pid=4321)
        process.wait.side_effect = [
            runner.TerminationRequested(signal.SIGTERM),
            runner.subprocess.TimeoutExpired("wrapper", 420),
            0,
        ]
        process.poll.return_value = None
        with mock.patch.object(
            runner.subprocess, "Popen", return_value=process
        ), mock.patch.object(runner, "_terminate_recorded_benchmark") as terminate, mock.patch.object(
            runner, "_kill_process_group"
        ) as kill_group:
            with self.assertRaises(runner.TerminationRequested):
                runner.run_wrapper_process(
                    ["wrapper", "--run-dir", "/tmp/suite/attempt-01"]
                )

        terminate.assert_called_once()
        kill_group.assert_called_once_with(4321)

    def test_orphan_cleanup_accepts_only_the_recorded_benchmark_group(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "attempt-01"
            write_wrapper_result(
                run_dir,
                status="running",
                exit_code=-1,
                variant="a",
                profile="falsification",
                benchmark_process_group=4321,
            )
            with mock.patch.object(
                runner.os, "getpgid", return_value=4321
            ), mock.patch.object(
                Path,
                "read_bytes",
                return_value=b"python3\0/scripts/m2-cold-window-vm-bench.py\0",
            ):
                self.assertEqual(runner._verified_benchmark_group(run_dir), 4321)
            with mock.patch.object(
                runner.os, "getpgid", return_value=4321
            ), mock.patch.object(
                Path, "read_bytes", return_value=b"python3\0/unrelated.py\0"
            ):
                self.assertEqual(runner._verified_benchmark_group(run_dir), 0)

    def test_resume_reuses_only_a_complete_temporally_adjacent_block(self) -> None:
        specs = falsification.build_leg_specs(Path("/tmp/suite"), seed=42)[:2]
        attempts = {
            spec.base_dir: spec.base_dir / "attempt-01" for spec in specs
        }

        with mock.patch.object(
            runner,
            "completed_attempt",
            side_effect=lambda base, *_args: attempts[base],
        ), mock.patch.object(
            runner, "_manifest_start_epoch", side_effect=[0.0, 1201.0]
        ):
            reusable = runner._reusable_block_attempts(specs, "receipt", "binary")

        self.assertEqual(reusable, attempts)

        with mock.patch.object(
            runner,
            "completed_attempt",
            side_effect=lambda base, *_args: attempts[base],
        ), mock.patch.object(
            runner, "_manifest_start_epoch", side_effect=[0.0, 3600.0]
        ):
            stale = runner._reusable_block_attempts(specs, "receipt", "binary")

        self.assertEqual(stale, {})

    def test_interrupted_suite_preserves_completed_legs_from_progress(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            suite_dir = Path(tmp)
            completed = synthetic_leg(1, "a", visibility_rate=1.0)

            def interrupt_with_progress(*_args: object) -> object:
                runner._atomic_json(
                    suite_dir / "progress.json", {"legs": [completed]}
                )
                raise runner.TerminationRequested(signal.SIGTERM)

            args = mock.Mock(sequence_seed=42, skip_build=True)
            specs = falsification.build_leg_specs(suite_dir, 42)
            with mock.patch.object(runner, "_prepare_build", return_value=""), mock.patch.object(
                runner, "_run_legs", side_effect=interrupt_with_progress
            ):
                result = runner._run_suite(
                    args,
                    suite_dir,
                    specs,
                    suite_dir / "build-provenance.json",
                )

            summary = json.loads(
                (suite_dir / "summary.json").read_text(encoding="utf-8")
            )
            self.assertEqual(result, 143)
            self.assertEqual(len(summary["legs"]), 1)

    def test_failed_attempt_is_reported_but_not_recorded_as_completed_progress(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            suite_dir = Path(tmp)
            receipt = suite_dir / "build-provenance.json"
            binary = runner.suite_binary(suite_dir)
            binary.parent.mkdir(parents=True)
            binary.write_bytes(b"binary")
            receipt.write_text("{}", encoding="utf-8")
            spec = falsification.build_leg_specs(suite_dir, 42)[0]
            failed = synthetic_leg(spec.block, spec.variant, visibility_rate=0.0)
            failed["valid"] = False
            with mock.patch.object(
                runner, "completed_attempt", return_value=None
            ), mock.patch.object(
                runner, "run_wrapper_process", return_value=1
            ), mock.patch.object(
                runner, "analyze_leg", return_value=failed
            ):
                legs, error = runner._run_legs([spec], suite_dir, receipt)

            progress = json.loads(
                (suite_dir / "progress.json").read_text(encoding="utf-8")
            )
            self.assertEqual(len(legs), 1)
            self.assertIn("退出码 1", error)
            self.assertEqual(progress["legs"], [])

    def test_build_release_writes_a_verifiable_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp) / "repo"
            suite_dir = Path(tmp) / "suite"
            binary = runner.suite_binary(suite_dir)
            binary.parent.mkdir(parents=True)
            binary.write_bytes(b"candidate")
            repo.mkdir()
            (repo / "Cargo.lock").write_text("lock", encoding="utf-8")
            completed = mock.Mock(
                returncode=0,
                stdout=json.dumps(
                    {
                        "reason": "compiler-artifact",
                        "target": {"name": "fd-rdd", "kind": ["bin"]},
                        "executable": str(binary),
                    }
                ),
            )
            with mock.patch.object(runner, "REPO_ROOT", repo), mock.patch.object(
                runner, "_git_head_sha", return_value="a" * 40
            ), mock.patch.object(
                runner, "_git_worktree_dirty", return_value=False
            ), mock.patch.object(
                runner.subprocess, "run", return_value=completed
            ):
                error = runner._build_release(suite_dir)

                receipt = suite_dir / "build-provenance.json"
                self.assertEqual(error, "")
                self.assertTrue(receipt.is_file())
                self.assertEqual(runner._validate_build_receipt(receipt, binary), "")

    def test_build_release_rejects_cargo_success_without_the_private_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp) / "repo"
            suite_dir = Path(tmp) / "suite"
            binary = runner.suite_binary(suite_dir)
            binary.parent.mkdir(parents=True)
            binary.write_bytes(b"stale")
            (repo / "Cargo.lock").parent.mkdir(parents=True)
            (repo / "Cargo.lock").write_text("lock", encoding="utf-8")
            with mock.patch.object(runner, "REPO_ROOT", repo), mock.patch.object(
                runner, "_git_head_sha", return_value="a" * 40
            ), mock.patch.object(
                runner, "_git_worktree_dirty", return_value=False
            ), mock.patch.object(
                runner.subprocess,
                "run",
                return_value=mock.Mock(returncode=0, stdout=""),
            ):
                error = runner._build_release(suite_dir)

            self.assertIn("compiler artifact", error)
            self.assertFalse((suite_dir / "build-provenance.json").exists())

    def test_build_release_refuses_a_dirty_worktree_before_cargo(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            suite_dir = Path(tmp) / "suite"
            with mock.patch.object(
                runner, "_git_head_sha", return_value="a" * 40
            ), mock.patch.object(
                runner, "_git_worktree_dirty", return_value=True
            ), mock.patch.object(
                runner.subprocess,
                "run",
                side_effect=AssertionError("cargo must not run for a dirty build"),
            ):
                error = runner._build_release(suite_dir)

            self.assertIn("工作区不干净", error)
            self.assertFalse((suite_dir / "build-provenance.json").exists())

    def test_build_release_refuses_a_head_change_during_cargo(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp) / "repo"
            suite_dir = Path(tmp) / "suite"
            binary = runner.suite_binary(suite_dir)
            binary.parent.mkdir(parents=True)
            binary.write_bytes(b"candidate")
            repo.mkdir()
            (repo / "Cargo.lock").write_text("lock", encoding="utf-8")
            cargo_output = json.dumps(
                {
                    "reason": "compiler-artifact",
                    "target": {"name": "fd-rdd", "kind": ["bin"]},
                    "executable": str(binary),
                }
            )
            with mock.patch.object(runner, "REPO_ROOT", repo), mock.patch.object(
                runner, "_git_head_sha", side_effect=["a" * 40, "b" * 40]
            ), mock.patch.object(
                runner, "_git_worktree_dirty", return_value=False
            ), mock.patch.object(
                runner.subprocess,
                "run",
                return_value=mock.Mock(returncode=0, stdout=cargo_output),
            ):
                error = runner._build_release(suite_dir)

            self.assertIn("HEAD 发生变化", error)
            self.assertFalse((suite_dir / "build-provenance.json").exists())

    def test_legacy_build_receipt_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp) / "repo"
            suite_dir = Path(tmp) / "suite"
            binary = runner.suite_binary(suite_dir)
            binary.parent.mkdir(parents=True)
            binary.write_bytes(b"candidate")
            repo.mkdir()
            (repo / "Cargo.lock").write_text("lock", encoding="utf-8")
            receipt = suite_dir / "build-provenance.json"
            receipt.write_text(
                json.dumps(
                    {
                        "schema": 1,
                        "build_succeeded": True,
                        "source_git_sha": "a" * 40,
                    }
                ),
                encoding="utf-8",
            )
            with mock.patch.object(runner, "REPO_ROOT", repo), mock.patch.object(
                runner, "_git_head_sha", return_value="a" * 40
            ), mock.patch.object(
                runner, "_git_worktree_dirty", return_value=False
            ):
                error = runner._validate_build_receipt(receipt, binary)

            self.assertIn("receipt_schema_mismatch", error)

    def test_skip_build_requires_an_existing_valid_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            suite_dir = Path(tmp) / "suite"
            with mock.patch.object(
                runner, "_run_legs", side_effect=AssertionError("must fail before legs")
            ):
                result = runner.main(["--skip-build", "--run-dir", str(suite_dir)])

            self.assertEqual(result, 1)
            summary = json.loads((suite_dir / "summary.json").read_text(encoding="utf-8"))
            self.assertTrue(
                any("build-provenance.json" in reason for reason in summary["gate"]["reasons"])
            )

    def test_resume_rejects_a_different_sequence_seed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            suite_dir = Path(tmp)
            (suite_dir / "manifest.json").write_text(
                json.dumps({"sequence_seed": 42}),
                encoding="utf-8",
            )

            error = runner._resume_error(suite_dir, 7)

            self.assertIn("sequence_seed=42", error)

    def test_resume_rejects_a_corrupt_sequence_seed_without_crashing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            suite_dir = Path(tmp)
            (suite_dir / "manifest.json").write_text(
                json.dumps({"sequence_seed": "not-an-integer"}),
                encoding="utf-8",
            )

            error = runner._resume_error(suite_dir, 42)

            self.assertIn("sequence_seed 无效", error)

    def test_completed_attempt_uses_latest_valid_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            for number, valid in ((1, True), (2, False), (3, True)):
                attempt = base / f"attempt-{number:02d}"
                attempt.mkdir()
                (attempt / "manifest.json").write_text(
                    json.dumps(
                        {
                            "run_state": "completed" if valid else "failed",
                            "completion_reason": "duration_elapsed",
                            "ab_comparable": valid,
                        }
                    ),
                    encoding="utf-8",
                )
                (attempt / "summary.json").write_text(
                    json.dumps(
                        {
                            "fd_rdd_exit_code": 0 if valid else 1,
                            "ab_comparable": valid,
                        }
                    ),
                    encoding="utf-8",
                )
                write_wrapper_result(
                    attempt,
                    status="passed" if valid else "failed",
                    exit_code=0 if valid else 1,
                    variant="a",
                    profile="falsification",
                )

            self.assertEqual(
                runner.completed_attempt(base),
                base / "attempt-03",
            )

    def test_completed_attempt_rejects_manifest_that_is_not_comparable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            attempt = Path(tmp) / "attempt-01"
            attempt.mkdir()
            (attempt / "manifest.json").write_text(
                json.dumps(
                    {
                        "run_state": "completed",
                        "completion_reason": "duration_elapsed",
                        "ab_comparable": False,
                    }
                ),
                encoding="utf-8",
            )
            (attempt / "summary.json").write_text(
                json.dumps({"fd_rdd_exit_code": 0, "ab_comparable": True}),
                encoding="utf-8",
            )

            self.assertIsNone(runner.completed_attempt(Path(tmp)))

    def test_completed_attempt_rejects_a_failed_or_missing_wrapper_result(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            for number, status in ((1, "failed"), (2, "missing")):
                attempt = base / f"attempt-{number:02d}"
                attempt.mkdir()
                (attempt / "manifest.json").write_text(
                    json.dumps(
                        {
                            "run_state": "completed",
                            "completion_reason": "duration_elapsed",
                            "ab_comparable": True,
                        }
                    ),
                    encoding="utf-8",
                )
                (attempt / "summary.json").write_text(
                    json.dumps({"fd_rdd_exit_code": 0, "ab_comparable": True}),
                    encoding="utf-8",
                )
                if status != "missing":
                    write_wrapper_result(
                        attempt,
                        status=status,
                        exit_code=1,
                        variant="a",
                        profile="falsification",
                    )

            self.assertIsNone(runner.completed_attempt(base))

    def test_completed_attempt_rejects_signal_terminated_daemon(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            attempt = base / "attempt-01"
            attempt.mkdir()
            (attempt / "manifest.json").write_text(
                json.dumps(
                    {
                        "run_state": "completed",
                        "completion_reason": "duration_elapsed",
                        "ab_comparable": True,
                    }
                ),
                encoding="utf-8",
            )
            (attempt / "summary.json").write_text(
                json.dumps(
                    {
                        "fd_rdd_exit_code": -signal.SIGTERM,
                        "ab_comparable": True,
                    }
                ),
                encoding="utf-8",
            )
            write_wrapper_result(
                attempt,
                status="passed",
                exit_code=0,
                variant="a",
                profile="falsification",
            )

            self.assertIsNone(runner.completed_attempt(base))
            spec = falsification.LegSpec(
                block=1,
                position=1,
                variant="a",
                order="ab",
                base_dir=base,
            )
            self.assertFalse(falsification.analyze_leg(spec, attempt)["valid"])

    def test_completed_attempt_rejects_stale_receipt_or_binary_identity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            attempt = base / "attempt-01"
            attempt.mkdir()
            (attempt / "manifest.json").write_text(
                json.dumps(
                    {
                        "run_state": "completed",
                        "completion_reason": "duration_elapsed",
                        "ab_comparable": True,
                    }
                ),
                encoding="utf-8",
            )
            (attempt / "summary.json").write_text(
                json.dumps(
                    {
                        "fd_rdd_exit_code": 0,
                        "ab_comparable": True,
                        "run_audit": {
                            "artifact_provenance": {
                                "receipt_sha256": "a" * 64,
                                "validated_binary_sha256": "b" * 64,
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            write_wrapper_result(
                attempt,
                status="passed",
                exit_code=0,
                variant="a",
                profile="falsification",
            )

            self.assertIsNone(
                runner.completed_attempt(
                    base,
                    "a",
                    expected_receipt_sha256="0" * 64,
                    expected_binary_sha256="b" * 64,
                )
            )
            self.assertIsNone(
                runner.completed_attempt(
                    base,
                    "a",
                    expected_receipt_sha256="a" * 64,
                    expected_binary_sha256="0" * 64,
                )
            )

    def test_completed_attempt_rejects_the_wrong_variant(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            attempt = base / "attempt-01"
            attempt.mkdir()
            (attempt / "manifest.json").write_text(
                json.dumps(
                    {
                        "run_state": "completed",
                        "completion_reason": "duration_elapsed",
                        "ab_comparable": True,
                    }
                ),
                encoding="utf-8",
            )
            (attempt / "summary.json").write_text(
                json.dumps({"fd_rdd_exit_code": 0, "ab_comparable": True}),
                encoding="utf-8",
            )
            write_wrapper_result(
                attempt,
                status="passed",
                exit_code=0,
                variant="b",
                profile="falsification",
            )

            self.assertIsNone(runner.completed_attempt(base, "a"))

    def test_suite_lock_rejects_a_second_runner(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            suite_dir = Path(tmp)
            with runner.exclusive_suite_lock(suite_dir):
                with self.assertRaisesRegex(RuntimeError, "另一进程占用"):
                    with runner.exclusive_suite_lock(suite_dir):
                        self.fail("同一 suite 不应被并发编排")

    def test_next_attempt_uses_the_highest_existing_number(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "attempt-01").mkdir()
            (base / "attempt-03").mkdir()
            (base / "attempt-invalid").mkdir()

            self.assertEqual(runner.next_attempt(base), base / "attempt-04")

    def test_dry_run_prints_all_legs_without_building_or_creating_suite(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            suite_dir = Path(tmp) / "suite"
            output = io.StringIO()
            with mock.patch.object(
                runner.subprocess,
                "run",
                side_effect=AssertionError("dry-run must not execute"),
            ), contextlib.redirect_stdout(output):
                result = runner.main(
                    ["--dry-run", "--run-dir", str(suite_dir), "--sequence-seed", "42"]
                )

            self.assertEqual(result, 0)
            self.assertFalse(suite_dir.exists())
            self.assertEqual(output.getvalue().count("--profile falsification"), 8)

    def test_relative_suite_dir_is_resolved_before_leg_commands(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            previous = Path.cwd()
            output = io.StringIO()
            try:
                os.chdir(tmp)
                with contextlib.redirect_stdout(output):
                    result = runner.main(
                        ["--dry-run", "--run-dir", "relative-suite"]
                    )
            finally:
                os.chdir(previous)

            self.assertEqual(result, 0)
            self.assertIn(str(Path(tmp) / "relative-suite"), output.getvalue())

    def test_skip_build_dry_run_reports_the_effective_plan(self) -> None:
        output = io.StringIO()
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(output):
            result = runner.main(
                ["--dry-run", "--skip-build", "--run-dir", str(Path(tmp) / "suite")]
            )

        self.assertEqual(result, 0)
        self.assertIn("build: skipped", output.getvalue())


class ReportTests(unittest.TestCase):
    def test_report_names_the_full_run_gate_and_screening_boundary(self) -> None:
        legs: list[dict[str, object]] = []
        for block in range(1, 5):
            legs.append(synthetic_leg(block, "a", visibility_rate=1.0))
            legs.append(
                synthetic_leg(
                    block,
                    "b",
                    visibility_rate=0.0,
                    positive_rate=0.8,
                )
            )
        legs.sort(key=lambda leg: (int(leg["block"]), int(leg["position"])))
        rendered = report.render_report(
            {"gate": gate.evaluate_suite(legs), "legs": legs}
        )

        self.assertIn("完整运行", rendered)
        self.assertIn("事件风暴窗口", rendered)
        self.assertIn("CPU 使用 service time", rendered)
        self.assertIn("write A/B", rendered)
        self.assertIn("incremental write / recovered path", rendered)
        self.assertIn("2000/2000", rendered)
        self.assertIn("主收益端点", rendered)
        self.assertIn("rebuild total:bootstrap:quiesce:unattributed", rendered)
        self.assertIn("筛查", rendered)
        self.assertIn("不等价于生产发布结论", rendered)

    def test_infrastructure_failure_writes_strict_resumable_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            suite_dir = Path(tmp)
            summary = runner._suite_summary(
                suite_dir,
                42,
                [],
                "release binary 构建失败",
            )

            runner._write_outputs(suite_dir, summary)

            parsed = json.loads((suite_dir / "summary.json").read_text(encoding="utf-8"))
            rendered = (suite_dir / "REPORT.md").read_text(encoding="utf-8")
            self.assertEqual(parsed["gate"]["decision"], "fail")
            self.assertIsNone(parsed["gate"]["median_cpu_ratio"])
            self.assertIn("`n/a`", rendered)

    def test_last_leg_infrastructure_failure_cannot_mark_suite_completed(self) -> None:
        legs: list[dict[str, object]] = []
        for block in range(1, 5):
            legs.append(synthetic_leg(block, "a", visibility_rate=1.0))
            legs.append(synthetic_leg(block, "b", visibility_rate=0.0))
        with tempfile.TemporaryDirectory() as tmp:
            suite_dir = Path(tmp)
            summary = runner._suite_summary(
                suite_dir,
                42,
                legs,
                "block 4 B 组退出码 1",
            )

            runner._write_outputs(suite_dir, summary)

            manifest = json.loads(
                (suite_dir / "manifest.json").read_text(encoding="utf-8")
            )
            self.assertEqual(manifest["run_state"], "failed")


if __name__ == "__main__":
    unittest.main()
