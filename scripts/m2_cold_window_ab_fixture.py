"""M2 A/B 专用 fixture 的安全重建与内容身份。"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Protocol

import fcntl

from m2_cold_window_ab_command import (
    COLD_DIR_COUNT,
    FALSIFICATION_EVENT_ROOT_NAMES,
    FIXTURE_DIR_NAME,
    FIXTURE_ROOT_NAMES,
    WORKLOAD_SEED,
)


def fixture_root(home: Path | None = None) -> Path:
    base = home if home is not None else Path(os.environ.get("HOME", str(Path.home())))
    return base.expanduser().resolve() / FIXTURE_DIR_NAME


def fixture_lock_path(home: Path | None = None) -> Path:
    root = fixture_root(home)
    return root.parent / f".{FIXTURE_DIR_NAME}.lock"


@contextmanager
def exclusive_fixture_lock(home: Path | None = None) -> Iterator[Path]:
    """独占 fixture 生命周期；并发 wrapper 立即失败，不等待或破坏目录。"""
    path = fixture_lock_path(home)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as stream:
        try:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(f"M2 A/B fixture 正被另一轮测试占用：{path}") from exc
        try:
            yield path
        finally:
            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def assert_safe_fixture_root(path: Path, home: Path | None = None) -> Path:
    """只允许重建 $HOME/fd-rdd-m2-roots，拒绝扩大删除边界。"""
    expected = fixture_root(home)
    candidate = path.expanduser().resolve()
    if candidate != expected or candidate == candidate.parent:
        raise ValueError(f"拒绝删除非专用 fixture 根：{candidate}（仅允许 {expected}）")
    return candidate


class _Digest(Protocol):
    def update(self, data: bytes, /) -> None: ...


def _hash_fixture_entry(
    digest: _Digest,
    relative_path: Path,
    content: bytes,
) -> None:
    digest.update(relative_path.as_posix().encode("utf-8"))
    digest.update(b"\0")
    digest.update(content)
    digest.update(b"\0")


def _write_fixture_manifest(
    root: Path,
    file_count: int,
    content_sha256: str,
) -> None:
    payload = {
        "completed": True,
        "actual_file_count": file_count,
        "seed": WORKLOAD_SEED,
        "layout_version": "m2-cold-window-ab-v3",
        "content_sha256": content_sha256,
    }
    (root / ".fd-rdd-m2-fixture.json").write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def rebuild_fixture(
    path: Path | None = None,
    home: Path | None = None,
) -> dict[str, Path]:
    """重建确定性 fixture，并在生成过程中计算完整内容身份。"""
    root = assert_safe_fixture_root(path or fixture_root(home), home)
    if root.exists() or root.is_symlink():
        shutil.rmtree(root)

    roots = {name: root / name for name in FIXTURE_ROOT_NAMES}
    for cold_name in ("cold-a", "cold-b"):
        cold_root = roots[cold_name]
        digest = hashlib.sha256()
        for index in range(COLD_DIR_COUNT):
            relative = Path(f"d{index:03d}") / f"file_{index:03d}.txt"
            content = f"fd-rdd M2 cold fixture {index:03d}\n".encode("utf-8")
            target = cold_root / relative
            target.parent.mkdir(parents=True)
            target.write_bytes(content)
            _hash_fixture_entry(digest, relative, content)
        _write_fixture_manifest(cold_root, COLD_DIR_COUNT, digest.hexdigest())

    roots["hot"].mkdir(parents=True)
    _write_fixture_manifest(roots["hot"], 0, hashlib.sha256().hexdigest())

    for root_name in FALSIFICATION_EVENT_ROOT_NAMES:
        event_root = roots[root_name]
        relative = Path("seed") / "sentinel.txt"
        content = f"fd-rdd M2 cold storm anchor {root_name}\n".encode("utf-8")
        target = event_root / relative
        target.parent.mkdir(parents=True)
        target.write_bytes(content)
        digest = hashlib.sha256()
        _hash_fixture_entry(digest, relative, content)
        _write_fixture_manifest(event_root, 1, digest.hexdigest())
    return roots
