# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.6.14] - 2026-05-02

### Runtime footprint hardening

- Added default index-time directory exclusions for dependency/build/cache trees such as `.git`, `.cache`, `.cargo`, `.npm`, `.pnpm-store`, `.yarn`, `node_modules`, `target`, `dist`, `build`, and `vendor`.
- Wired exclusions through cold full build, incremental scans, fast sync collection, and watcher event filtering so excluded directories do not enter the index instead of being merely demoted at query scoring time.
- Added repeatable `--exclude-dir NAME` CLI overrides and `exclude_dirs` config support; existing config files without the field keep the default exclusion list.
- On startup, existing `config.toml` files that do not declare `exclude_dirs` are migrated by appending the default list, making the exclusions visible and user-editable.
- Added `GET /memory` to expose the same `MemoryReport` data as JSON for RSS/smaps/index/overlay/event-pipeline attribution.
- Included `BaseIndexData` in memory attribution and removed the redundant runtime `entries_by_path` clone; v7 snapshots still write the segment for format compatibility, but runtime uses a single `FileEntryIndex`.
- Triggered allocator collection after v7 snapshot loading and WAL replay to reduce startup decode/build high-water RSS.
- Removed the unused `FileEntryIndex` path permutation and tightened `PathTableV2` entry layout to reduce per-file base overhead.
- Changed newly written v7 path-table segments to store the compressed `PathTableV2` representation directly while keeping legacy full-path segment loading support.
- Added automatic high-water RSS trimming in the memory report loop, a one-shot idle trim after event bursts, and `GET/POST /trim` for manual allocator collection.
- Replaced runtime `ParentIndex` per-directory `RoaringBitmap` storage with sorted `Vec<u32>` direct-child doc IDs, keeping v7 parent segment compatibility while reducing many small heap allocations.
- Added `watch_enabled` config and `--no-watch` to run in static snapshot/manual-scan mode, allowing watcher memory attribution and low-RSS read-only operation.
- Added `watch_mode = "recursive" | "tiered" | "off"` plus `--watch-mode`; tiered mode admits only budgeted hot directory candidates into L0 and scans rejected candidates with a bounded warm-scan loop.
- Added `GET /watch-state` to expose watcher mode, L0/L1 counts, estimated watch budget use, scan backlog, and tiered scheduler notes.
- Raised the default periodic snapshot batch gate so a handful of filesystem events stay in WAL/DeltaBuffer instead of materializing the full base every snapshot interval.

### PersistentIndex storage migration

- Migrated `PersistentIndex` runtime storage to `FileEntry + Vec<Vec<u8>>` absolute paths.
- Kept `CompactMeta + PathArena` only as legacy v4/v5 loading and v5/v6 compatibility export formats.
- Kept `PathTableV2 + FileEntryIndex` as export-time read-only structures for `BaseIndexData` / v7 snapshots instead of maintaining them on the event hot path.
- Updated overlong-path behavior: runtime indexing is no longer constrained by the legacy `u16 path_len` arena format.

## [0.6.12] - 2026-05-02

### Wrap-up stabilization

- Restored the `tests` branch to a compiling and fully tested state after divergent wrap-up merges.
- Removed `TieredIndex` `disk_layers` hot-path state and deleted the unused `src/index/tiered/disk_layer.rs`.
- Kept `TieredIndex` startup on v7 snapshots or empty rebuild fallback; removed reintroduced LSM/v6 mmap loading from the query hot path.
- Removed synchronous deep directory scans from event apply on directory rename.
- Reconnected WAL replay after v7 snapshot loading and empty startup.
- Fixed `snapshot_v7` serialization to match the current `ParentIndex` structure without `compat_dir_to_files`.
- Added query-time BaseIndex refresh when direct L2 writes make the base snapshot stale.

## [0.6.11] - 2026-05-01

### Phase 8: DeltaBuffer 硬容量上限 + PathTable 内存优化 + Hybrid Crawler 清理

- **DeltaBuffer 硬容量上限**：默认 `max_capacity = 256 * 1024`，`insert`/`apply_events` 返回 `bool` 表示是否因容量限制丢弃
- **容量满自动 flush**：`TieredEvents::apply_tiered_events` 在检测到 `apply_events` 返回 `false` 时自动触发 flush
- **PathTable + FileEntry**：新增 `PathTable`（`path_to_id`/`id_to_path` 双射表），`FileEntry` 用 `PathId: u32` 替代 `PathBuf`，目标节省约 75% 内存（800MB → 200MB）
- **Hybrid Crawler 清理**：删除 `startup_reconcile`、`spawn_rebuild`、`reconcile_degraded_root`、overflow recovery loop、`DirtyTracker`；`spawn_fast_sync`/`spawn_repair` 签名简化，移除定时器驱动，改为纯事件驱动
- **编译/测试验证**：`cargo check` 0 errors 0 warnings；`cargo test --lib` 145 passed；`cargo test --test '*'` 2 passed；`cargo build --release` 成功

## [0.6.10] - 2026-05-01

### Phase 7: ParentIndex 增量维护正确性修复 + 死代码清理

- 在 `finish_rebuild` 完成 L2 替换后重建 ParentIndex，确保 rebuild 后的 parent 查询正确
- 在 `apply_events_inner`/`apply_events_inner_drain` 修改 L2 后重建 ParentIndex
- 在 `apply_upserted_metas_inner` 修改 L2 后重建 ParentIndex
- 进一步清理死代码警告（PathArenaSet 等）

## [0.6.9] - 2026-05-01

### Phase 6: ParentIndex query acceleration + dead code cleanup

- Integrate ParentIndex into query path for `parent:`/`infolder:` filters
- Add `extract_parent_filter()` to `CompiledQuery`
- Add `parent_candidates()` to `PersistentIndex`
- Clean up dead_code warnings (DiskLayer.id, fill_from_compaction, PathArenaSet methods, DeltaBuffer.capacity)

## [0.6.8] - 2026-05-01

### Phase 5: DeltaBuffer Default Enable

- **Default-enable DeltaBuffer**: Removed `USE_DELTA_BUFFER` environment variable gate. DeltaBuffer is now the sole overlay mechanism for event buffering.
- **Removed deprecated `overlay_state`**: Deleted `OverlayState` struct and `update_overlay_for_events` method from `src/index/tiered/events.rs`.
- **Removed `pending_events`**: Deleted the `pending_events: Mutex<Vec<EventRecord>>` field from `TieredIndex` and all related push/drain logic.
- **Simplified query paths**: `collect_all_live_metas` and `execute_query_plan` now unconditionally read from `delta_buffer`.
- **Simplified memory reporting**: `file_count()` and `memory_report()` no longer branch on environment variable.
- **Simplified snapshot flush**: `snapshot_now` unconditionally uses `delta_buffer` for dirty checks and drain.
- **Updated `finish_rebuild`**: Replaced `overlay_state` clear with `delta_buffer.clear()`.
- **Code reduction**: ~362 lines removed across 7 files in `src/index/tiered/`.

## [0.6.7] - 2026-05-01

### Changed

- **Phase 4: ParentIndex Default Enable + Deprecated Cleanup**
  - **Default enabled** `ParentIndex`: removed `USE_PARENT_INDEX` environment variable A/B switch. `fast_sync` Phase3 delete alignment now always uses `delete_alignment_with_parent_index()` (O(D)), removing the legacy `for_each_live_meta_in_dirs()` O(N) fallback.
  - **Removed** `for_each_live_meta_in_dirs()` method: no longer has any callers after the fallback path removal.
  - **Build ParentIndex at startup**: `rebuild_parent_index()` is now automatically called after each snapshot recovery path (LSM / v6 / v2-v5) in `load_or_empty_with_options`, eliminating cold-start latency on first `fast_sync`.
  - **Removed** deprecated `startup_reconcile()` and `spawn_rebuild()`:
    - Deleted both deprecated functions from `src/index/tiered/sync.rs`
    - Removed `startup_reconcile()` call from `src/main.rs`
    - Removed related test cases from `src/index/tiered/tests.rs`
  - Net code reduction from eliminating fallback paths and deprecated code.

## [0.6.6] - 2026-05-01

### Added

- **Phase 3: DeltaBuffer Unified Incremental Buffer**: Introduced `DeltaBuffer` module to unify `overlay_state` and `pending_events`.
  - Added `src/index/delta_buffer.rs`: `DeltaBuffer` backed by `HashMap<Vec<u8>, DeltaState>` mapping paths to their latest delta state (Live/Deleted).
  - Capacity limit 256K entries (default `262_144`), solving the original `pending_events` 4096 overflow issue.
  - State transitions: `Create/Modify` → `Live`, `Delete` → `Deleted`, `Rename` marks from→Deleted and to→Live simultaneously.
  - Refactored `TieredIndex`: added `delta_buffer` field, keeping old `overlay_state` / `pending_events` as fallback.
  - Refactored query path: `execute_query_plan` / `collect_all_live_metas` now read deleted/upserted/live state from `DeltaBuffer`.
  - Refactored event application path: `begin_apply_batch` / `update_overlay_for_events` changed to `update_delta_buffer_for_events`.
  - Environment variable `USE_DELTA_BUFFER=1` enables the new path; old path remains as fallback when unset.
  - 133 unit tests pass, integration tests pass.

## [0.6.5] - 2026-05-01

### Added

- **Phase 2: ParentIndex Module**: Introduced `ParentIndex` to eliminate the core performance bottleneck in `fast_sync` Phase3.
  - Added `src/index/parent_index.rs`: `ParentIndex` backed by `HashMap<PathBuf, RoaringTreemap>` mapping parent directories to their live `DocId`s.
  - `PersistentIndex` now carries `parent_index: RwLock<Option<ParentIndex>>`.
  - Added `rebuild_parent_index()` invoked after rebuild and snapshot to warm the index.
  - `fast_sync` Phase3 supports `USE_PARENT_INDEX` environment variable for A/B switching between:
    - `delete_alignment_with_parent_index()` (O(D) where D = dirty directory count)
    - Legacy `for_each_live_meta_in_dirs()` (O(N) full scan) as fallback.

## [0.6.4] - 2026-05-01

### Added

- **Benchmark Framework (Phase 0)**: Comprehensive benchmarking and profiling infrastructure.
  - `scripts/bench.sh`: Automated benchmark suite covering compilation time, startup latency, baseline RSS/CPU, query latency (warm/cold), and event-storm throughput.
  - `scripts/profile.sh`: Performance profiling harness for `perf` (flamegraph) and `dhat` (heap allocation).
  - `src/stats/mod.rs`: `StatsCollector` runtime metrics collection (RSS, CPU, query latency, event throughput).
  - `src/query/server.rs`: `/metrics` HTTP endpoint exposing Prometheus-compatible stats.
  - `BENCHMARK.md`: Baseline documentation for reproducible benchmarks.
  - `.github/workflows/bench.yml`: CI integration for automated regression detection.

### Removed

- **Dead Code Cleanup (Phase 1)**: Minimal cleanup targeting code that conflicts with the "low-footprint, long-running" positioning.
  - Deleted `src/index/tiered/compaction.rs` (~383 lines). Fast compaction path remains inline; legacy slow path removed.
  - Deleted polling functions from `src/index/recovery.rs`.
  - Deleted RSS trim loop and associated CLI arguments (`--rss-trim-interval`, `--malloc-trim`).
  - Deleted `dyn_walk_and_enqueue` and related dynamic walk helpers.
  - Marked rebuild/recovery functions in `src/sync.rs` as `#[deprecated]` (to be removed in v0.7.0).
  - Simplified CLI argument surface by removing unused tuning knobs.

## [0.6.3] - 2026-04-26

### Changed

- **BTreeMap → HashMap for internal index maps**: `filekey_to_docid` and `path_hash_to_id` changed from `BTreeMap` to `HashMap`, saving ~48 MB RSS at million-file scale. These maps only perform point lookups, inserts, and deletes — no ordering dependency.
- **EventPipeline default channel_size reduced**: Default `channel_size` in `EventPipeline::new()` reduced from 262144 to 131072 (~11 MB saved). Production path already overrides via CLI default (65536).
- **FAST_COMPACTION enabled by default**: `unwrap_or(false)` changed to `unwrap_or(true)` — the fast compaction path (`compact_layers_fast`) using bitmap OR merging is now the default, eliminating per-meta allocation spikes.
- **short_component_index key type optimized**: Changed from `HashMap<Box<[u8]>, RoaringTreemap>` to `HashMap<u16, RoaringTreemap>` using big-endian encoding for 1-2 byte path components, eliminating ~3 MB of heap metadata overhead (previously 21:1 overhead-to-data ratio).

### Added

- **L1 Cache path_index O(1) fast path**: Added `exact_path()` method to `Matcher` trait. `WfnMatcher` with `FullPath` scope now takes an O(1) lookup path via the existing `path_index` instead of O(N) full scan.

### Fixed

- **Dynamic directory monitoring**: Newly created directories (from `git clone`, `npm install`, `mkdir`) now automatically receive recursive inotify watches. The event processing loop detects `Create(Folder)` events and calls `watcher.watch(new_dir, Recursive)` dynamically.

## [0.6.2] - 2026-04-26

### Fixed

- **False-negative search results**: `trigram_candidates` in both `PersistentIndex` and `MmapIndex` returned `Some(empty bitmap)` instead of `None` when trigram intersection was empty, blocking fallback to `short_hint_candidates` and full scan. Changed to return `None`.
- **upsert race condition**: Added `upsert_lock` (write lock held during entire rename/new-file path) to prevent query-write races causing trigram index/metas inconsistency.
- **pending_events visibility gap**: Reordered `apply_events` before `remove_from_pending`, and integrated `pending_events` scan into `execute_query_plan`, ensuring debounce-window files are always visible.
- **file_count() snapshot inconsistency**: Added `apply_gate.read()` lock to prevent reading intermediate state between L2 swap and disk_layers update during snapshot.
- **file_count() undercount**: Changed to sum L2 + all disk_layers + overlay_upserted.
- **CI inotify limit exhaustion**: Raised `max_user_watches` to 1048576 and added `max_queued_events=524288` in CI workflow.
- **CI performance thresholds**: Relaxed CPU 100% duration threshold from 3000ms to 10000ms and RSS peak from 400MB to 600MB for 2-core CI runners.

### Changed

- **CompactMeta.mtime optimization**: `mtime` field changed from `Option<SystemTime>` (16B) to `i64` nanosecond timestamp (8B), saving ~8 MB at million-file scale.
- **filekey_to_docid / path_hash_to_id**: Changed from `HashMap` to `BTreeMap` in v0.6.2 (reverted to `HashMap` in v0.6.3 for further memory savings).

## [0.6.1] - 2026-04-25

### Added

- `Config::save()` method: `Config` now implements `serde::Serialize`, allowing the active configuration to be written back to `~/.config/fd-rdd/config.toml` in TOML format.
- First-run auto-configuration: on first startup, if `~/.config/fd-rdd/config.toml` does not exist, `--root` is required. After a successful start the default configuration is automatically saved. Subsequent launches need no arguments; simply run `fd-rdd`.
- Large-scale hybrid correctness test (`tests/p2_large_scale_hybrid.rs`): an 800K-file integration test that validates incremental indexing under realistic developer workflows (git clone, npm install, single-file CRUD). Marked with `#[ignore]` for explicit CI invocation.

### Fixed

- musl target build failure: switched `reqwest` dev-dependency to `rustls-tls`, removing the musl cross-compile dependency on system OpenSSL.
- CI `musl-build` job: added `musl-tools` installation step so the musl cross-compile environment is complete.
- `cargo fmt` formatting check: formatted all Rust sources so `cargo fmt --all -- --check` passes cleanly.
- Clippy `dead_code` / `unused` warnings: added module-level `#![allow(dead_code, unused)]` in test helper modules (`tests/common/`, `tests/fixtures/`) so `cargo clippy --all-targets -- -D warnings` passes.

## [0.6.0] - 2026-04-20

### Added

- `snapshot_loop` minimum interval guard (10s): prevents cascading snapshot triggers from high-frequency overlay flush requests.
- PendingMoveMap rename matching: resolves file disappearance caused by cross-batch renames.
- Dynamic delay back-pressure: monitors channel watermark and injects sleep when >80%, preventing OOM during event storms such as `npm install`.
- i_generation generational validation: uses `FS_IOC_GETVERSION` to fetch inode generation, completely eliminating ghost files caused by inode reuse.
- Directory rename deep sync: triggers deep fast-sync recursively when a directory rename is detected.
- Unicode NFC normalization: integrates `unicode-normalization`; all paths are forced to NFC to eliminate encoding traps.
- fd-rdd Stress CI: systematic stress tests covering overlay visibility, rename avalanches, concurrent intermediate states, mmap safety, trigram skew, etc.

### Fixed

- `snapshot_now` data visibility window: moved `export_segments_v6()` before L2 swap and serialized inside `apply_gate.write()` lock, eliminating the query data loss window between swap and disk_layers push.
- Compaction frequency causing CPU/RAM spikes: thresholds raised from `2 delta / 30s cooldown` to `8 delta / 4 max_deltas / 300s cooldown`, significantly reducing compaction frequency and temporary allocation in million-file scenarios.
- Watcher channel batch event overflow: default `event_channel_size` raised from 4096 to 262144, lowering the probability of silent event drops during bulk operations such as git clone / extraction.
- Fast-sync fallback latency: cooldown shortened from 5s to 1s, max-staleness from 30s to 5s, enabling faster incremental catch-up after overflow and reducing perceived "new file not found" latency.
- `execute_query_plan` overlay visibility: merged `overlay.upserted_paths` to ensure newly created files are visible in L2/L3 queries.
- Tombstones/trigrams atomicity: ensures `tombstones.insert` happens before `remove_trigrams`, eliminating the query miss window during deletion.
- GitHub Actions `fd-rdd` startup command line fragility: pressure workflow and smoke nodes changed to single-line startup, avoiding `\` being mistakenly passed to clap and causing tests to fail before daemon starts.
- Nightly ThreadSanitizer ABI mismatch: sanitizer job changed to use job-level `RUSTFLAGS=-Z sanitizer=thread`, ensuring the current crate, dependencies, and std use the same sanitizer ABI.
- `snapshot_now` synchronous stage blocking: moved synchronous stage to `spawn_blocking`, avoiding blocking the tokio runtime; also enforces `MIN_SNAPSHOT_INTERVAL` when `interval=0` to prevent high-frequency snapshot cascades.
- `apply_gate` write-lock starvation: uses `try_write` instead of `write`, avoiding persistent write-lock hold causing tokio worker read-priority starvation.
- `compute_highlights` Chinese UTF-8 out-of-bounds panic: cherry-picked main-branch fix where matching advanced start by `+1`, causing multi-byte UTF-8 Chinese characters to slice in the middle of a character on the next round; now advances by the actual matched substring byte length.
- Chinese exact query test missing `generation` field: added missing `generation` field to `FileKey` in `chinese_exact_query_via_trigram` test.
- inotify `max_user_watches` exhaustion causing deep subdirectory watch silent failure: `handle_notify_result()` in `watcher.rs` no longer silently drops notify errors; actively identifies ENOSPC (errno 28 / "no space") and marks all related directories dirty. Added `watch_roots_enhanced()` to estimate per-root watch demand before adding recursive watches; if system limit is tight, marks the root as degraded and records it in `DirtyTracker`, making failure observable.
- Hybrid Crawler degraded-root incremental reconciliation: in `stream.rs`, replaced simple polling fallback with Hybrid Crawler background task. Maintains 60s fast-sync for `failed_roots`; adds 30s reconciliation loop for `degraded_roots`, iterating DFS (max depth 20, skipping hidden dirs and ignore paths) over the directory tree and comparing mtime, marking changed subdirectories dirty via `DirtyTracker::record_overflow_paths()`, which triggers existing overflow recovery logic automatically.
- Fast-sync safety margin breakage causing new file loss in degraded mode: `reconcile_degraded_root` used `last_sync_ns - 10s` to detect changed directories, but `fast_sync` `DirtyScope::Dirs` branch re-filtered `root_dirs` with raw `cutoff_ns`, causing reconciled changed dirs to be incorrectly filtered. Unified to `cutoff_ns.saturating_sub(10_000_000_000)` so the safety margin is consistent end-to-end.
- Fast-sync semaphore race causing dirty state false consumption: when `spawn_fast_sync` was skipped due to semaphore contention, the old code incorrectly called `tracker.finish_sync()`, clearing dirty markers and `sync_in_progress`, causing changed directories to lose indexing opportunities. Changed to call the new `tracker.rollback_sync(scope)` to roll back dirty state and `sync_in_progress`, ensuring retry on next scheduling.
