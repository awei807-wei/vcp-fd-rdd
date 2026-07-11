from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("m2-realistic-fixture.py")
SPEC = importlib.util.spec_from_file_location("m2_realistic_fixture", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
FIXTURE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = FIXTURE
SPEC.loader.exec_module(FIXTURE)


class PlanAllocationTests(unittest.TestCase):
    def test_section_targets_sum_exactly_to_requested_total(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for total in (1, 7, 999, 1_000_000, 1_000_003):
                plans = FIXTURE._build_plans(root, total, seed=42)
                self.assertEqual(sum(plan.target_files for plan in plans), total)
                self.assertTrue(all(plan.target_files >= 0 for plan in plans))

    def test_million_file_misc_section_is_only_the_remainder(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            plans = FIXTURE._build_plans(Path(tmp), 1_000_000, seed=42)
            targets = {plan.name: plan.target_files for plan in plans}

        self.assertEqual(targets["Projects"], 300_000)
        self.assertEqual(targets["Misc"], 63_000)


class ProjectScaffoldBudgetTests(unittest.TestCase):
    def test_project_scaffolding_is_included_in_section_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "Projects"
            plan = FIXTURE.SectionPlan(
                "Projects",
                "projects",
                root,
                target_files=150,
                num_dirs=10,
                files_per_dir=15,
                seed=42,
            )

            result = FIXTURE._generate_section(plan, progress_every=0)
            actual_files = sum(1 for path in root.rglob("*") if path.is_file())

        self.assertEqual(result.files_created, 150)
        self.assertEqual(actual_files, 150)


class CompletedManifestTests(unittest.TestCase):
    def plans(self, root: Path):
        return [
            FIXTURE.SectionPlan("Downloads", "downloads", root / "Downloads", 3, 1, 3, 42),
            FIXTURE.SectionPlan("Projects", "projects", root / "Projects", 2, 1, 2, 43),
        ]

    def test_completed_generation_writes_atomic_verified_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plans = self.plans(root)
            results = [
                FIXTURE.GenResult("Downloads", 3, 1, 0.1),
                FIXTURE.GenResult("Projects", 2, 1, 0.2),
            ]

            completed = FIXTURE._write_completed_fixture_manifest(
                root,
                seed=42,
                requested_total_files=5,
                plans=plans,
                results=results,
                generation_started_clean=True,
            )

            self.assertTrue(completed)
            manifest_path = root / ".fd-rdd-m2-fixture.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertTrue(manifest["completed"])
            self.assertEqual(manifest["layout_version"], "m2-realistic-v2")
            self.assertEqual(manifest["actual_file_count"], 5)
            self.assertEqual(manifest["seed"], 42)
            self.assertFalse(manifest_path.with_suffix(".json.tmp").exists())

    def test_incomplete_generation_revokes_previous_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest_path = root / ".fd-rdd-m2-fixture.json"
            manifest_path.write_text('{"completed":true}\n', encoding="utf-8")

            completed = FIXTURE._write_completed_fixture_manifest(
                root,
                seed=42,
                requested_total_files=5,
                plans=self.plans(root),
                results=[FIXTURE.GenResult("Downloads", 2, 1, 0.1)],
                generation_started_clean=True,
            )

            self.assertFalse(completed)
            self.assertFalse(manifest_path.exists())

    def test_non_clean_generation_cannot_claim_verified_count(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plans = self.plans(root)
            results = [
                FIXTURE.GenResult("Downloads", 3, 1, 0.1),
                FIXTURE.GenResult("Projects", 2, 1, 0.2),
            ]

            completed = FIXTURE._write_completed_fixture_manifest(
                root,
                seed=42,
                requested_total_files=5,
                plans=plans,
                results=results,
                generation_started_clean=False,
            )

            self.assertFalse(completed)
            self.assertFalse((root / ".fd-rdd-m2-fixture.json").exists())

    def test_requested_total_must_match_plan_and_actual_counts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plans = self.plans(root)
            results = [
                FIXTURE.GenResult("Downloads", 3, 1, 0.1),
                FIXTURE.GenResult("Projects", 2, 1, 0.2),
            ]

            completed = FIXTURE._write_completed_fixture_manifest(
                root,
                seed=42,
                requested_total_files=4,
                plans=plans,
                results=results,
                generation_started_clean=True,
            )

            self.assertFalse(completed)
            self.assertFalse((root / ".fd-rdd-m2-fixture.json").exists())


if __name__ == "__main__":
    unittest.main()
