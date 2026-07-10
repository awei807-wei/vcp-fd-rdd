#!/usr/bin/env python3
# m2-realistic-fixture.py - 生成 ~1M 文件的逼真用户主目录结构（M2 真实规模基准）
#
# 设计目标：
# - 模拟真实用户主目录的目录树与文件分布，覆盖 L0/L1/L2/L3 各层级
# - 使用稀疏文件（truncate）模拟大体积媒体，避免真实写入 1GB 数据
# - 多进程并行生成，目标 <10 分钟完成 1M 文件
# - 安全：--root 必须显式指定；拒绝 /、$HOME 直接、空路径
# - 可复现：--seed 固定随机种子；--dry-run 仅打印计划
#
# 用法示例：
#   python3 scripts/m2-realistic-fixture.py \
#       --root "$HOME/fd-rdd-m2-realistic" \
#       --total-files 1000000 --workers 4
#   # 小规模验证：
#   python3 scripts/m2-realistic-fixture.py --root /tmp/m2-small --total-files 100 --dry-run

from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path

MiB = 1024 * 1024
KiB = 1024
GiB = 1024 * 1024 * 1024

DEFAULT_TARGET = 1_000_000
FIXTURE_MANIFEST_VERSION = 1

# 各区段的默认比例（按 1M 目标缩放，--total-files 改变时整体等比缩放）
# 字段: (name, weight, dirs_per_million, files_per_dir, kind)
SECTIONS = [
    {
        "name": "Downloads",
        "weight": 0.05,        # 50,000
        "dirs_per_million": 200,
        "files_per_dir": 250,
        "kind": "downloads",
    },
    {
        "name": "Pictures",
        "weight": 0.20,        # 200,000
        "dirs_per_million": 600,
        "files_per_dir": 333,
        "kind": "pictures",
    },
    {
        "name": "Documents",
        "weight": 0.03,        # 30,000
        "dirs_per_million": 500,
        "files_per_dir": 60,
        "kind": "documents",
    },
    {
        "name": "Projects",
        "weight": 0.30,        # 300,000
        "dirs_per_million": 2000,
        "files_per_dir": 150,
        "kind": "projects",
    },
    {
        "name": "Music",
        "weight": 0.10,        # 100,000
        "dirs_per_million": 500,
        "files_per_dir": 200,
        "kind": "music",
    },
    {
        "name": "Videos",
        "weight": 0.005,       # 5,000
        "dirs_per_million": 50,
        "files_per_dir": 100,
        "kind": "videos",
    },
    {
        "name": ".config",
        "weight": 0.002,       # 2,000
        "dirs_per_million": 300,
        "files_per_dir": 7,
        "kind": "config",
    },
    {
        "name": ".cache",
        "weight": 0.20,        # 200,000
        "dirs_per_million": 1000,
        "files_per_dir": 200,
        "kind": "cache",
    },
    {
        "name": ".local/share",
        "weight": 0.05,        # 50,000
        "dirs_per_million": 800,
        "files_per_dir": 63,
        "kind": "appdata",
    },
    {
        "name": "Misc",
        "weight": 0.163,       # ~163,000 filler
        "dirs_per_million": 1000,
        "files_per_dir": 163,
        "kind": "misc",
    },
]


# ---------------------------------------------------------------------------
# 内容生成器：返回 (相对路径列表, 文件大小字节, 是否稀疏, 内容 bytes or None)
# 为减少 IPC 体积，worker 内部生成完整任务并直接写盘，仅返回计数。
# ---------------------------------------------------------------------------

_CODE_SNIPPETS = [
    "fn main() {{ println!(\"hello {i}\"); }}\n",
    "export function handler(req, res) {{ return res.json({{ ok: true, id: {i} }}); }}\n",
    "import os\n\ndef process(i={i}):\n    return os.getpid()\n",
    "package main\n\nfunc work(i int) int {{ return i * 2 }}\n",
    "pub struct Item {{ pub id: u32 }}\n\nimpl Item {{ fn new(i: u32) -> Self {{ Self {{ id: i }} }} }}\n",
]

_DOC_TEXT = (
    "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
    "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. "
    "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris. "
)


def _config_content(kind: str, i: int) -> bytes:
    """生成 TOML/JSON/YAML 配置内容（预计算取模值，避免 .format 表达式限制）。"""
    m10 = i % 10
    m100 = i % 100
    choice = i % 3
    if choice == 0:
        return f"[app]\nport = 808{m10}\nhost = \"127.0.0.1\"\n".encode()
    if choice == 1:
        return f'{{"name":"svc-{i}","version":"1.0.{m100}"}}\n'.encode()
    return f"server:\n  port: 300{m10}\n  workers: 4\n".encode()


@dataclass
class SectionPlan:
    name: str
    kind: str
    root: Path          # 该 section 在 test_root 下的根
    target_files: int
    num_dirs: int
    files_per_dir: int
    seed: int


@dataclass
class GenResult:
    name: str
    files_created: int
    dirs_created: int
    elapsed_s: float


def _human_bytes(n: float) -> str:
    if n >= GiB:
        return f"{n / GiB:.2f} GB"
    if n >= MiB:
        return f"{n / MiB:.2f} MB"
    if n >= KiB:
        return f"{n / KiB:.2f} KB"
    return f"{n:.0f} B"


# ---------------------------------------------------------------------------
# 各 kind 的路径生成与写盘策略
# ---------------------------------------------------------------------------

def _rel_dirs_for_kind(kind: str, rng: random.Random, num_dirs: int) -> list[list[str]]:
    """生成 num_dirs 个相对子目录（相对 section root），体现各 kind 的真实层级。"""
    out: list[list[str]] = []
    if kind == "pictures":
        # 2023/01..12, 2024/01..12, 2025/01..06
        months = [f"{y}/{m:02d}" for y in (2023, 2024) for m in range(1, 13)]
        months += [f"2025/{m:02d}" for m in range(1, 7)]
        for i in range(num_dirs):
            base = months[i % len(months)]
            # 加一层子目录避免单目录过大
            out.append([*base.split("/"), f"roll{i % 8}"])
    elif kind == "projects":
        # project-a/src/module/sub, project-b/..., 含 target/ node_modules/
        proj_names = ["project-a", "project-b", "project-c", "project-d", "project-e"]
        sub_layouts = [
            ["src", "core"],
            ["src", "api"],
            ["tests", "unit"],
            ["tests", "integration"],
            ["src", "utils"],
            ["target", "debug"],
            ["node_modules", "pkg"],
            ["src", "module", "submodule"],
        ]
        for i in range(num_dirs):
            proj = proj_names[i % len(proj_names)]
            sub = sub_layouts[i % len(sub_layouts)]
            out.append([proj, *sub])
    elif kind == "downloads":
        buckets = ["", "browser_cache", "partial", "archives", "isos"]
        for i in range(num_dirs):
            b = buckets[i % len(buckets)]
            out.append([b, f"d{i:04d}"] if b else [f"d{i:04d}"])
    elif kind == "music":
        for i in range(num_dirs):
            artist = f"artist{(i % 50):03d}"
            album = f"album{(i % 5):02d}"
            out.append([artist, album])
    elif kind == "videos":
        for i in range(num_dirs):
            out.append([f"series{i % 10}", f"season{i % 5}"])
    elif kind == "config":
        apps = ["app-a", "app-b", "app-c", "app-d"]
        for i in range(num_dirs):
            out.append([apps[i % len(apps)], f"conf{i:03d}"])
    elif kind == "cache":
        for i in range(num_dirs):
            out.append([f"cache-{i % 50:02d}", f"bucket{i:04d}"])
    elif kind == "appdata":
        apps = ["appdata-a", "appdata-b", "appdata-c"]
        for i in range(num_dirs):
            out.append([apps[i % len(apps)], f"data{i:04d}"])
    else:  # misc / documents
        for i in range(num_dirs):
            out.append([f"grp{i % 20:02d}", f"dir{i:05d}"])
    return out


def _exts_for_kind(kind: str) -> list[str]:
    if kind == "downloads":
        return [".pdf", ".zip", ".deb", ".dmg", ".iso", ".tar.gz", ".part", ".bin"]
    if kind == "pictures":
        return [".jpg", ".png", ".heic", ".jpeg"]
    if kind == "documents":
        return [".pdf", ".docx", ".md", ".txt", ".odt"]
    if kind == "projects":
        return [".rs", ".py", ".js", ".ts", ".go", ".json", ".toml", ".md", ".lock"]
    if kind == "music":
        return [".mp3", ".flac", ".ogg"]
    if kind == "videos":
        return [".mp4", ".mkv", ".avi"]
    if kind == "config":
        return [".toml", ".json", ".yaml", ".conf", ".ini"]
    if kind == "cache":
        return [".cache", ".tmp", ".dat", ""]  # 部分无扩展名
    if kind == "appdata":
        return [".db", ".log", ".json", ".dat", ".txt"]
    return [".txt", ".dat", ".bin"]


def _file_size_and_sparse(kind: str, rng: random.Random) -> tuple[int, bool]:
    """返回 (size_bytes, use_sparse_truncate)。稀疏文件只 truncate，不写实际数据。"""
    if kind == "videos":
        return rng.randint(100 * MiB, GiB), True
    if kind == "music":
        return rng.randint(3 * MiB, 10 * MiB), True
    if kind == "downloads":
        r = rng.random()
        if r < 0.2:  # iso / dmg
            return rng.randint(200 * MiB, 800 * MiB), True
        return rng.randint(1 * KiB, 5 * MiB), False
    if kind == "pictures":
        return rng.randint(10 * KiB, 500 * KiB), False
    if kind == "documents":
        return rng.randint(1 * KiB, 100 * KiB), False
    if kind == "projects":
        return rng.randint(1 * KiB, 50 * KiB), False
    if kind == "config":
        return rng.randint(100, 10 * KiB), False
    if kind == "cache":
        return rng.randint(100, 10 * KiB), False
    if kind == "appdata":
        return rng.randint(1 * KiB, 100 * KiB), False
    return rng.randint(1 * KiB, 10 * KiB), False


def _content_for(kind: str, ext: str, i: int, size: int) -> bytes:
    """生成少量标记内容（fd-rdd 只索引路径与元数据，内容可极简）。"""
    if ext in (".rs", ".py", ".js", ".ts", ".go"):
        return _CODE_SNIPPETS[i % len(_CODE_SNIPPETS)].format(i=i).encode()
    if ext in (".toml", ".json", ".yaml", ".conf", ".ini"):
        return _config_content(kind, i)
    if ext in (".md", ".txt"):
        return (_DOC_TEXT * (1 + (i % 3))).encode()
    # 二进制/媒体：只写一个小 marker（大体积部分由稀疏 truncate 撑起）
    return f"FD-RDD-M2-FIXTURE|kind={kind}|i={i}\n".encode()


def _write_file(path: Path, content: bytes, size: int, sparse: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        f.write(content)
        if sparse and size > len(content):
            # 用 truncate 创建稀疏文件：文件逻辑大小为 size，但不占用实际磁盘块
            f.truncate(size)


def _make_git_dir(proj_root: Path, rng: random.Random, i: int) -> int:
    """在每个 project 下创建一个 .git 目录，含 objects/refs/HEAD/config。返回创建文件数。"""
    git = proj_root / ".git"
    count = 0
    # HEAD
    _write_file(git / "HEAD", b"ref: refs/heads/main\n", len(b"ref: refs/heads/main\n"), False)
    count += 1
    # config
    cfg = "[core]\n\trepositoryformatversion = 0\n\tfilemode = true\n"
    _write_file(git / "config", cfg.encode(), len(cfg), False)
    count += 1
    # refs/heads/main
    ref = (git / "refs" / "heads" / "main")
    sha = f"{i:040x}"
    _write_file(ref, sha.encode() + b"\n", len(sha) + 1, False)
    count += 1
    # objects: 模拟若干 loose object
    obj_dir = git / "objects" / "pack"
    for j in range(8):
        p = obj_dir / f"pack-{i:010d}-{j:02d}.pack"
        _write_file(p, b"PACK\n", 16 * KiB, True)
        count += 1
    # COMMIT_EDITMSG / index
    _write_file(git / "COMMIT_EDITMSG", b"initial\n", 8, False)
    count += 1
    _write_file(git / "index", b"DIRC\n", 4 * KiB, True)
    count += 1
    return count


def _make_node_modules(proj_root: Path, rng: random.Random, i: int) -> int:
    """为部分 project 创建 node_modules 占位。返回文件数。"""
    nm = proj_root / "node_modules"
    count = 0
    pkgs = ["react", "lodash", "express", "chalk", "axios", "left-pad"]
    for pkg in pkgs:
        pkg_dir = nm / pkg
        # package.json
        pj = f'{{"name":"{pkg}","version":"1.{i % 50}.0","main":"index.js"}}\n'
        _write_file(pkg_dir / "package.json", pj.encode(), len(pj), False)
        count += 1
        # index.js
        idx = f"module.exports = function() {{ return '{pkg}-{i}'; }};\n"
        _write_file(pkg_dir / "index.js", idx.encode(), len(idx), False)
        count += 1
    return count


def _generate_section(plan: SectionPlan, progress_every: int = 10_000) -> GenResult:
    rng = random.Random(plan.seed)
    exts = _exts_for_kind(plan.kind)
    rel_dirs = _rel_dirs_for_kind(plan.kind, rng, plan.num_dirs)
    files_created = 0
    dirs_created = 0
    start = time.time()

    # 每个 section 内均匀分配文件
    per_dir = plan.files_per_dir
    remaining = plan.target_files
    project_roots_seen: set[Path] = set()

    for d_idx, rel in enumerate(rel_dirs):
        if remaining <= 0:
            break
        this_count = min(per_dir, remaining)
        remaining -= this_count
        dir_path = plan.root.joinpath(*rel) if rel else plan.root
        try:
            dir_path.mkdir(parents=True, exist_ok=True)
        except OSError:
            pass
        dirs_created += 1

        for fi in range(this_count):
            ext = rng.choice(exts)
            size, sparse = _file_size_and_sparse(plan.kind, rng)
            fname = f"file_{d_idx:06d}_{fi:06d}{ext}"
            fpath = dir_path / fname
            content = _content_for(plan.kind, ext, d_idx * 1000 + fi, size)
            try:
                _write_file(fpath, content, size, sparse)
                files_created += 1
            except OSError:
                pass

            if progress_every and files_created % progress_every == 0:
                el = time.time() - start
                rate = files_created / el if el > 0 else 0.0
                print(
                    f"[{plan.name}] files={files_created}/{plan.target_files} "
                    f"dirs={dirs_created} rate={rate:.0f}/s",
                    flush=True,
                )

        # 为 projects kind 额外注入 .git / node_modules（每个 project 根一次）
        if plan.kind == "projects":
            # 取 project 顶层名作为 project root
            proj_root = plan.root / rel[0]
            if proj_root not in project_roots_seen:
                project_roots_seen.add(proj_root)
                try:
                    files_created += _make_git_dir(proj_root, rng, d_idx)
                    if (d_idx % 2) == 0:
                        files_created += _make_node_modules(proj_root, rng, d_idx)
                except OSError:
                    pass

    # 处理 remaining > 0（dirs 不足以装下 target_files 的情况）：追加扁平目录
    extra_bucket = 0
    while remaining > 0:
        this_count = min(per_dir, remaining)
        remaining -= this_count
        dir_path = plan.root / "_overflow" / f"b{extra_bucket:05d}"
        try:
            dir_path.mkdir(parents=True, exist_ok=True)
            dirs_created += 1
        except OSError:
            pass
        for fi in range(this_count):
            ext = rng.choice(exts)
            size, sparse = _file_size_and_sparse(plan.kind, rng)
            fname = f"ovf_{extra_bucket:05d}_{fi:06d}{ext}"
            fpath = dir_path / fname
            content = _content_for(plan.kind, ext, 999000 + extra_bucket * 1000 + fi, size)
            try:
                _write_file(fpath, content, size, sparse)
                files_created += 1
            except OSError:
                pass
            if progress_every and files_created % progress_every == 0:
                el = time.time() - start
                rate = files_created / el if el > 0 else 0.0
                print(
                    f"[{plan.name}/overflow] files={files_created} rate={rate:.0f}/s",
                    flush=True,
                )
        extra_bucket += 1

    elapsed = time.time() - start
    return GenResult(
        name=plan.name,
        files_created=files_created,
        dirs_created=dirs_created,
        elapsed_s=elapsed,
    )


# ---------------------------------------------------------------------------
# 规划与安全
# ---------------------------------------------------------------------------

def _validate_root(root: Path) -> None:
    raw = "" if root is None else str(root)
    if raw.strip() in ("", "."):
        raise SystemExit(
            "[error] --root 不能为空或 '.'（避免误用当前目录作为 1M 文件根）。"
        )
    try:
        rp = str(root.resolve())
    except OSError:
        rp = raw
    if rp in ("", "/"):
        raise SystemExit(f"[error] 拒绝使用危险根路径：{rp!r}（不允许 / 或空路径）")
    home = os.environ.get("HOME", "")
    if home and rp == home:
        raise SystemExit(f"[error] 拒绝直接使用 $HOME（{home}）作为根路径；请指定子目录。")
    # 额外防护：拒绝当前工作目录（避免误操作把 fixture 写进仓库）
    try:
        cwd = str(Path.cwd().resolve())
    except OSError:
        cwd = ""
    if cwd and rp == cwd:
        raise SystemExit(
            f"[error] 拒绝使用当前工作目录（{cwd}）作为根路径；请指定独立子目录。"
        )
    if rp.rstrip("/") == "":
        raise SystemExit("[error] 拒绝使用根路径 / 。")


def _build_plans(test_root: Path, total_files: int, seed: int) -> list[SectionPlan]:
    scale = total_files / DEFAULT_TARGET
    plans: list[SectionPlan] = []
    for idx, sec in enumerate(SECTIONS):
        target = max(0, int(round(sec["weight"] * total_files)))
        num_dirs = max(1, int(round(sec["dirs_per_million"] * scale)))
        cfg_fpd = sec["files_per_dir"]
        # 若按 scale 算出的单目录文件数远超配置特征值，扩大 dirs 数量以保持单目录规模合理
        naive_fpd = target // num_dirs if num_dirs else target
        if cfg_fpd and naive_fpd > cfg_fpd * 3:
            num_dirs = max(num_dirs, max(1, target // cfg_fpd))
        files_per_dir = target // num_dirs if num_dirs else target
        files_per_dir = max(1, files_per_dir)
        plans.append(
            SectionPlan(
                name=sec["name"],
                kind=sec["kind"],
                root=test_root / sec["name"],
                target_files=target,
                num_dirs=num_dirs,
                files_per_dir=files_per_dir,
                seed=seed + idx * 9973,
            )
        )
    return plans


def _print_plan(plans: list[SectionPlan]) -> None:
    total = sum(p.target_files for p in plans)
    total_dirs = sum(p.num_dirs for p in plans)
    print("==== Fixture Plan (dry-run) ====")
    print(f"{'Section':<16}{'Kind':<12}{'Files':>10}{'Dirs':>8}{'Files/Dir':>12}{'Path'}")
    for p in plans:
        fpd = p.target_files // max(1, p.num_dirs)
        print(
            f"{p.name:<16}{p.kind:<12}{p.target_files:>10}{p.num_dirs:>8}{fpd:>12}  {p.root}"
        )
    print(f"{'TOTAL':<28}{total:>10}{total_dirs:>8}")
    print("================================")


def _clean_root(test_root: Path) -> None:
    if test_root.exists():
        print(f"[clean] removing existing fixture at {test_root} ...")
        shutil.rmtree(test_root)


def _atomic_write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, sort_keys=True, indent=2)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    dir_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _write_completed_fixture_manifest(
    test_root: Path,
    *,
    seed: int,
    requested_total_files: int,
    plans: list[SectionPlan],
    results: list[GenResult],
    generation_started_clean: bool,
) -> bool:
    result_by_name = {result.name: result for result in results}
    completed = generation_started_clean and len(result_by_name) == len(plans) and all(
        result_by_name.get(plan.name) is not None
        and result_by_name[plan.name].files_created == plan.target_files
        for plan in plans
    )
    manifest_path = test_root / ".fd-rdd-m2-fixture.json"
    if not completed:
        manifest_path.unlink(missing_ok=True)
        return False

    actual_file_count = sum(result.files_created for result in results)
    actual_dir_count = sum(result.dirs_created for result in results)
    _atomic_write_json(
        manifest_path,
        {
            "schema_version": FIXTURE_MANIFEST_VERSION,
            "layout_version": "m2-realistic-v1",
            "completed": True,
            "seed": seed,
            "requested_total_files": requested_total_files,
            "actual_file_count": actual_file_count,
            "actual_dir_count": actual_dir_count,
            "sections": [
                {
                    "name": plan.name,
                    "target_files": plan.target_files,
                    "files_created": result_by_name[plan.name].files_created,
                    "dirs_created": result_by_name[plan.name].dirs_created,
                }
                for plan in plans
            ],
        },
    )
    return True


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(
        description="生成 ~1M 文件的逼真主目录结构（fd-rdd M2 真实规模基准）"
    )
    ap.add_argument("--root", type=Path, required=True, help="测试根目录（必须显式指定）")
    ap.add_argument("--total-files", type=int, default=DEFAULT_TARGET, help="目标文件总数（默认 1000000）")
    ap.add_argument("--seed", type=int, default=42, help="随机种子（复现用，默认 42）")
    ap.add_argument("--workers", type=int, default=4, help="并行 worker 数（默认 4）")
    ap.add_argument("--clean", action="store_true", help="生成前先删除已存在的 fixture")
    ap.add_argument("--dry-run", action="store_true", help="仅打印计划，不创建文件")
    args = ap.parse_args(argv)

    _validate_root(args.root)

    if args.total_files <= 0:
        raise SystemExit(f"[error] --total-files 必须为正数（收到 {args.total_files}）")
    if args.workers < 1:
        raise SystemExit(f"[error] --workers 必须 >=1（收到 {args.workers}）")

    test_root: Path = args.root
    plans = _build_plans(test_root, args.total_files, args.seed)

    _print_plan(plans)

    if args.dry_run:
        print("[dry-run] 不创建任何文件。")
        return 0

    try:
        generation_started_clean = not test_root.exists() or next(test_root.iterdir(), None) is None
    except OSError:
        generation_started_clean = False
    if args.clean:
        _clean_root(test_root)
        generation_started_clean = True

    test_root.mkdir(parents=True, exist_ok=True)
    # Any generation attempt mutates the fixture. Revoke an older completion
    # declaration before the first write so an interrupted rebuild can never be
    # mistaken for a verified A/B input.
    (test_root / ".fd-rdd-m2-fixture.json").unlink(missing_ok=True)
    # 写入 marker，便于外部工具识别
    (test_root / ".fd-rdd-m2-fixture").write_text(
        f"fd-rdd m2 realistic fixture\nseed={args.seed}\ntotal_files={args.total_files}\n",
        encoding="utf-8",
    )

    workers = max(1, args.workers)
    overall_start = time.time()
    results: list[GenResult] = []

    if workers == 1 or len(plans) <= 1:
        for p in plans:
            r = _generate_section(p)
            results.append(r)
            print(
                f"[done] {r.name}: files={r.files_created} dirs={r.dirs_created} "
                f"elapsed={r.elapsed_s:.1f}s",
                flush=True,
            )
    else:
        with ProcessPoolExecutor(max_workers=workers) as ex:
            future_map = {ex.submit(_generate_section, p): p for p in plans}
            for fut in as_completed(future_map):
                p = future_map[fut]
                try:
                    r = fut.result()
                except Exception as e:  # noqa: BLE001
                    print(f"[error] section {p.name} failed: {e}", file=sys.stderr)
                    continue
                results.append(r)
                print(
                    f"[done] {r.name}: files={r.files_created} dirs={r.dirs_created} "
                    f"elapsed={r.elapsed_s:.1f}s",
                    flush=True,
                )

    total_files_created = sum(r.files_created for r in results)
    total_dirs_created = sum(r.dirs_created for r in results)
    manifest_completed = _write_completed_fixture_manifest(
        test_root,
        seed=args.seed,
        requested_total_files=args.total_files,
        plans=plans,
        results=results,
        generation_started_clean=generation_started_clean,
    )
    elapsed = time.time() - overall_start
    print("==== Summary ====")
    print(f"root             : {test_root}")
    print(f"total files      : {total_files_created}")
    print(f"total dirs       : {total_dirs_created}")
    print(f"elapsed          : {elapsed:.1f}s")
    print(f"throughput       : {total_files_created / elapsed:.0f} files/s" if elapsed > 0 else "throughput: N/A")
    print(f"verified manifest: {'yes' if manifest_completed else 'no'}")
    print("=================")
    if not manifest_completed:
        print(
            "[error] fixture generation was incomplete; verified manifest was not written",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
