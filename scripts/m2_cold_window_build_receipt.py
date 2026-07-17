"""Shared build-receipt format for M2 benchmark artifact provenance."""

from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCHEMA = 4
CARGO_ARGS = [
    "cargo",
    "build",
    "--release",
    "--locked",
    "--message-format=json-render-diagnostics",
]


def cargo_args_for_target(target_dir: Path) -> list[str]:
    return [*CARGO_ARGS, "--target-dir", str(target_dir.resolve())]


def compiler_artifact_path(output: str, target_name: str = "fd-rdd") -> Path | None:
    """从 Cargo JSON 消息中提取本次 bin target 的真实 executable。"""
    artifact: Path | None = None
    for line in output.splitlines():
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(message, dict) or message.get("reason") != "compiler-artifact":
            continue
        target = message.get("target")
        executable = message.get("executable")
        if not isinstance(target, dict) or not isinstance(executable, str):
            continue
        kinds = target.get("kind", [])
        if target.get("name") == target_name and isinstance(kinds, list) and "bin" in kinds:
            artifact = Path(executable).resolve()
    return artifact


def git_head_sha(repo: Path) -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo,
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    value = result.stdout.strip().lower()
    if result.returncode == 0 and len(value) == 40 and all(
        character in "0123456789abcdef" for character in value
    ):
        return value
    return ""


def git_worktree_dirty(repo: Path) -> bool | None:
    try:
        result = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=normal"],
            cwd=repo,
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return bool(result.stdout.strip()) if result.returncode == 0 else None


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
    except OSError:
        return ""
    return digest.hexdigest()


def create_build_receipt(
    repo: Path,
    binary: Path,
    pre_build_git_sha: str,
    post_build_git_sha: str,
    planned_git_sha: str = "",
) -> dict[str, Any]:
    """Describe one successful locked release build without trusting path metadata."""
    return {
        "schema": SCHEMA,
        "build_succeeded": True,
        "built_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
            "+00:00", "Z"
        ),
        "planned_git_sha": planned_git_sha or pre_build_git_sha,
        "executed_git_sha": post_build_git_sha,
        "source_git_sha": post_build_git_sha,
        "pre_build_git_sha": pre_build_git_sha,
        "post_build_git_sha": post_build_git_sha,
        "build_worktree_clean": True,
        "cargo_lock_sha256": sha256_file(repo / "Cargo.lock"),
        "binary_sha256": sha256_file(binary),
        "binary": str(binary.resolve()),
        "compiler_artifact": str(binary.resolve()),
        "cargo_args": cargo_args_for_target(binary.parent.parent),
    }


def load_build_receipt(receipt_path: Path) -> dict[str, Any]:
    try:
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return receipt if isinstance(receipt, dict) else {}


def validate_build_receipt(
    receipt_path: Path,
    repo: Path,
    binary: Path,
    source_git_sha: str,
    planned_git_sha: str = "",
) -> list[str]:
    """Recompute every receipt identity field and return stable failure reasons."""
    if not receipt_path.exists():
        return ["receipt_missing"]
    receipt = load_build_receipt(receipt_path)
    if not receipt:
        return ["receipt_unreadable"]

    cargo_lock_sha256 = sha256_file(repo / "Cargo.lock")
    binary_sha256 = sha256_file(binary)
    schema = receipt.get("schema")
    receipt_source_git_sha = str(receipt.get("source_git_sha", ""))
    receipt_planned_git_sha = str(
        receipt.get("planned_git_sha") or receipt_source_git_sha
    )
    receipt_executed_git_sha = str(
        receipt.get("executed_git_sha") or receipt_source_git_sha
    )
    checks = (
        (schema in {3, SCHEMA}, "receipt_schema_mismatch"),
        (receipt.get("build_succeeded") is True, "build_not_succeeded"),
        (receipt.get("build_worktree_clean") is True, "build_worktree_not_clean"),
        (
            receipt_planned_git_sha
            == (planned_git_sha or source_git_sha)
            and bool(planned_git_sha or source_git_sha),
            "planned_git_sha_mismatch",
        ),
        (
            receipt_executed_git_sha == source_git_sha
            and bool(source_git_sha),
            "executed_git_sha_mismatch",
        ),
        (
            receipt.get("source_git_sha") == source_git_sha and bool(source_git_sha),
            "source_git_sha_mismatch",
        ),
        (
            receipt.get("pre_build_git_sha") == source_git_sha,
            "pre_build_git_sha_mismatch",
        ),
        (
            receipt.get("post_build_git_sha") == source_git_sha,
            "post_build_git_sha_mismatch",
        ),
        (
            receipt.get("cargo_lock_sha256") == cargo_lock_sha256
            and bool(cargo_lock_sha256),
            "cargo_lock_sha256_mismatch",
        ),
        (
            receipt.get("binary_sha256") == binary_sha256 and bool(binary_sha256),
            "binary_sha256_mismatch",
        ),
        (receipt.get("binary") == str(binary.resolve()), "binary_path_mismatch"),
        (
            receipt.get("compiler_artifact") == str(binary.resolve()),
            "compiler_artifact_mismatch",
        ),
        (
            receipt.get("cargo_args") == cargo_args_for_target(binary.parent.parent),
            "cargo_args_mismatch",
        ),
    )
    return [error for valid, error in checks if not valid]
