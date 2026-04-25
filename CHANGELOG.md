# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
