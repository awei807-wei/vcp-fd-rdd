# 代码债务清理 Phase 1 需求

## 背景

2026-06-17 对 fd-rdd 进行了全面的代码风格审计和架构评审（见 `helloagents/wiki/code-style-audit-report.md`、`helloagents/wiki/architecture-review-report.md`）。审计发现项目工程基础扎实（CI/CD 完善、测试覆盖全面），但存在若干"屎山化"早期症状：误导性桩代码、大段复制粘贴、锁策略不一致、工具函数散落。

本方案包选取 **P0-P2 中风险可控、收益明确** 的问题进行首批治理，不触碰 `tiered_watch.rs` 拆分等大规模重构（留待 Phase 2）。

## 问题

### P0：误导性桩代码
- `src/query/dsl.rs:523-524` — `HardlinkDupe` / `ContentDupe` 编译时静默返回 `CompiledExpr::True`（匹配所有文件）。用户执行 `dupe:` 查询会得到错误语义的结果而非报错。

### P1：复制粘贴
- `src/index/l2_partition.rs` — `export_segments_v6` 与 `export_segments_v6_to_writer` 近乎逐行重复（~300 行），含重复常量 `TRIGRAM_SENTINEL`、`FKM_MAGIC`、`FKM_VERSION`、`FKM_FLAG_LEGACY`、`FKM_FLAG_RKYV`。

### P1：工具函数散落
- `fn unix_secs()` 在 **6 个文件**中完全相同地复制（`main.rs:928`、`stats/metrics_reporter.rs:537`、`event/tiered_watch.rs:3936`、`index/tiered/snapshot.rs:239`、`index/tiered/mod.rs:318`、`index/tiered/events.rs:403`）。

### P2：锁策略不一致
- `src/io_governor.rs:4,227,249,250,269,273` — 使用 `std::sync::Mutex` + `.lock().unwrap()`，而项目其余位置统一使用 `parking_lot::RwLock`（无 poisoning）。一旦持锁线程 panic 导致 poison，热路径 `before_io` 会直接 panic 传播。

## 目标

1. `HardlinkDupe` / `ContentDupe` 编译时返回明确的 `QueryCompileError`，不再静默返回 True。
2. `l2_partition.rs` 段导出逻辑去重：抽取公共 `write_v6_segments_to_writer`，常量提升为模块级 `const`。
3. `unix_secs()` 提取到 `src/util.rs`，6 处本地副本改为 `use crate::util::unix_secs`。
4. `io_governor.rs` 的 `std::sync::Mutex` 替换为 `parking_lot::Mutex`，移除所有 `.lock().unwrap()`。

## 验收

- `cargo test -q` 全量通过，无新增失败。
- `cargo fmt --check` 通过。
- `cargo clippy --all-targets -- -D warnings` 通过。
- `dupe:` 查询返回编译错误而非全量结果。
- `unix_secs` 在 src/ 中仅 `util.rs` 一处定义。
- `io_governor.rs` 中无 `std::sync::Mutex` 和 `.lock().unwrap()`。
- `l2_partition.rs` 中无重复的 `TRIGRAM_SENTINEL` / `FKM_*` 常量定义。

## 边界

- **不拆分** `tiered_watch.rs`（5822 行）— 留待 Phase 2。
- **不拆分** `main()` / `stream::start()` God 函数 — 留待 Phase 2。
- **不处理** 循环依赖解耦 — 需要架构级调整，单独方案包。
- **不处理** 魔法数字提取、内联测试分离等 Low 级问题。
