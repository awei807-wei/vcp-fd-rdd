# 轻量迭代：任务清单（手动 RSS Trim / “抽水”）

## 目标

在 rebuild/full_build 完成后，主动触发一次分配器回吐，缓解“索引已缩小但 RSS 长期卡高水位”的现象。

## 任务

- [√] 依赖：引入 `libc`（用于 glibc `malloc_trim`）
- [√] 实现：在 `spawn_rebuild` / `spawn_full_build` 完成后调用 `maybe_trim_rss()`
  - [√] `mimalloc` feature：调用 `mi_collect(true)`
  - [√] glibc（Linux+gnu 且非 mimalloc）：调用 `malloc_trim(0)`
- [√] 文档：补齐 wiki 说明与 CHANGELOG 记录
- [√] 验证：`cargo test` 通过（含 `--features mimalloc`）

