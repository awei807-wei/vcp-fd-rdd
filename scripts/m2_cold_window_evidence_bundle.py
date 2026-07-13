"""M2 证据的采集、归档与完整性校验。"""

from __future__ import annotations

import hashlib
import io
import json
import os
import tarfile
from pathlib import Path
from typing import Any

from m2_cold_window_evidence_collection import collect_evidence_records
from m2_cold_window_evidence_validation import (
    BUILD_RECEIPT_NAME,
    EVIDENCE_ATTEMPT_FILES,
    EVIDENCE_OPTIONAL_ATTEMPT_FILES,
    EVIDENCE_OPTIONAL_ROOT_FILES,
    EVIDENCE_REQUIRED_ATTEMPT_FILES,
    EVIDENCE_REQUIRED_ROOT_FILES,
    EVIDENCE_ROOT_FILES,
    EvidenceBundleError,
)


EVIDENCE_MANIFEST_NAME = "evidence-manifest.json"
EVIDENCE_CHECKSUMS_NAME = "SHA256SUMS"


def evidence_bundle_path(suite_dir: Path) -> Path:
    return suite_dir.with_name(f"{suite_dir.name}-evidence.tar.gz")


def remove_published_documents(suite_dir: Path) -> None:
    for name in ("summary.json", "REPORT.md", "manifest.json"):
        (suite_dir / name).unlink(missing_ok=True)


def invalidate_published_evidence(suite_dir: Path, *, remove_documents: bool = False) -> None:
    evidence_bundle_path(suite_dir).unlink(missing_ok=True)
    if remove_documents:
        remove_published_documents(suite_dir)


def _add_bytes_member(archive: tarfile.TarFile, name: str, content: bytes) -> None:
    info = tarfile.TarInfo(name=name)
    info.size = len(content)
    info.mode = 0o644
    info.mtime = 0
    archive.addfile(info, io.BytesIO(content))


def _member_sha256(archive: tarfile.TarFile, name: str) -> tuple[str, int]:
    stream = archive.extractfile(name)
    if stream is None:
        raise EvidenceBundleError(f"证据包成员不可读: {name}")
    digest = hashlib.sha256()
    size = 0
    while chunk := stream.read(1024 * 1024):
        digest.update(chunk)
        size += len(chunk)
    return digest.hexdigest(), size


def _validate_archive(
    path: Path,
    expected: dict[str, tuple[str, int]],
    inventory_name: str,
    checksums_name: str,
) -> None:
    with tarfile.open(path, "r:gz") as archive:
        names = archive.getnames()
        expected_names = set(expected) | {checksums_name}
        if len(names) != len(set(names)) or set(names) != expected_names:
            raise EvidenceBundleError("证据包成员集合不完整或重复")
        checksum_stream = archive.extractfile(checksums_name)
        if checksum_stream is None:
            raise EvidenceBundleError("证据包缺少 SHA256SUMS")
        try:
            lines = checksum_stream.read().decode("utf-8").splitlines()
            checksums = {
                name: digest for digest, name in (line.split("  ", 1) for line in lines)
            }
        except (UnicodeDecodeError, ValueError) as exc:
            raise EvidenceBundleError("SHA256SUMS 格式无效") from exc
        if len(checksums) != len(lines) or set(checksums) != set(expected):
            raise EvidenceBundleError("SHA256SUMS 成员集合不完整")
        for name, (expected_digest, expected_size) in expected.items():
            digest, size = _member_sha256(archive, name)
            if (digest, size) != (expected_digest, expected_size):
                raise EvidenceBundleError(f"证据包摘要不匹配: {name}")
            if checksums.get(name) != expected_digest:
                raise EvidenceBundleError(f"SHA256SUMS 摘要不匹配: {name}")
        inventory_stream = archive.extractfile(inventory_name)
        if inventory_stream is None:
            raise EvidenceBundleError("证据包缺少成员清单")
        try:
            inventory = json.loads(inventory_stream.read().decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise EvidenceBundleError("证据包成员清单无法解析") from exc
        listed = inventory.get("members", []) if isinstance(inventory, dict) else []
        paths = {row.get("path") for row in listed if isinstance(row, dict)}
        if paths != set(expected) - {inventory_name}:
            raise EvidenceBundleError("证据包成员清单与摘要集合不一致")


def _bundle_metadata(
    suite_dir: Path, summary: dict[str, Any]
) -> tuple[list[dict[str, Any]], dict[str, tuple[str, int]], str, str, bytes, bytes]:
    records, run_dirs = collect_evidence_records(suite_dir, summary)
    prefix = Path(suite_dir.name)
    inventory_name = str(prefix / EVIDENCE_MANIFEST_NAME)
    checksums_name = str(prefix / EVIDENCE_CHECKSUMS_NAME)
    inventory = {
        "schema": 1,
        "suite": suite_dir.name,
        "referenced_run_dirs": run_dirs,
        "required_policy": {
            "success_requires_exact_legs": 8,
            "success_root_files": sorted(EVIDENCE_REQUIRED_ROOT_FILES),
            "success_leg_files": sorted(EVIDENCE_REQUIRED_ATTEMPT_FILES),
            "success_semantics": (
                "terminal state, protocol events, ready log, config, metrics, "
                "runner log, and independent leg reanalysis"
            ),
            "optional": sorted(
                EVIDENCE_OPTIONAL_ROOT_FILES | EVIDENCE_OPTIONAL_ATTEMPT_FILES
            ),
        },
        "members": [
            {
                "path": str(prefix / record["relative"]),
                "sha256": record["sha256"],
                "size": record["size"],
                "required": record["required"],
                "category": record["category"],
            }
            for record in records
        ],
    }
    inventory_bytes = (
        json.dumps(inventory, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
    ).encode("utf-8")
    expected = {
        str(prefix / record["relative"]): (record["sha256"], record["size"])
        for record in records
    }
    expected[inventory_name] = (
        hashlib.sha256(inventory_bytes).hexdigest(),
        len(inventory_bytes),
    )
    checksums = "".join(
        f"{digest}  {name}\n"
        for name, (digest, _size) in sorted(expected.items())
    ).encode("utf-8")
    return records, expected, inventory_name, checksums_name, inventory_bytes, checksums


def _write_archive(
    path: Path,
    suite_name: str,
    records: list[dict[str, Any]],
    inventory_name: str,
    checksums_name: str,
    inventory: bytes,
    checksums: bytes,
) -> None:
    prefix = Path(suite_name)
    with tarfile.open(path, "w:gz") as archive:
        for record in records:
            name = str(prefix / record["relative"])
            if record["bytes"] is not None:
                _add_bytes_member(archive, name, record["bytes"])
            else:
                archive.add(record["path"], arcname=name, recursive=False)
        _add_bytes_member(archive, inventory_name, inventory)
        _add_bytes_member(archive, checksums_name, checksums)


def write_evidence_bundle(suite_dir: Path, summary: dict[str, Any]) -> Path:
    bundle = evidence_bundle_path(suite_dir)
    temporary = bundle.with_name(f".{bundle.name}.{os.getpid()}.tmp")
    bundle.unlink(missing_ok=True)
    temporary.unlink(missing_ok=True)
    try:
        metadata = _bundle_metadata(suite_dir, summary)
        records, expected, inventory_name, checksums_name, inventory, checksums = metadata
        _write_archive(
            temporary,
            suite_dir.name,
            records,
            inventory_name,
            checksums_name,
            inventory,
            checksums,
        )
        _validate_archive(temporary, expected, inventory_name, checksums_name)
        os.replace(temporary, bundle)
        _validate_archive(bundle, expected, inventory_name, checksums_name)
    except Exception:
        bundle.unlink(missing_ok=True)
        raise
    finally:
        temporary.unlink(missing_ok=True)
    return bundle
