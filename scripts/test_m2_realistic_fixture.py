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


if __name__ == "__main__":
    unittest.main()
