# 代码债务清理 Phase 1 任务

## 实施任务

### P0：修复误导性桩代码
- [x] `src/query/dsl.rs` — `HardlinkDupe` / `ContentDupe` 编译返回 `QueryCompileError` 而非 `CompiledExpr::True`
- [x] 若 `QueryCompileError` 无 `UnsupportedAtom` 变体，新增之
- [x] 更新相关测试用例（如有 `dupe:` 查询测试）

### P1：提取 unix_secs() 到 util.rs
- [x] `src/util.rs` — 添加 `pub fn unix_secs() -> u64`
- [x] `src/main.rs` — 删除本地 `fn unix_secs()`，改为 `use crate::util::unix_secs`
- [x] `src/stats/metrics_reporter.rs` — 同上
- [x] `src/event/tiered_watch.rs` — 同上（注意 cfg(test) 模块）
- [x] `src/index/tiered/snapshot.rs` — 同上
- [x] `src/index/tiered/mod.rs` — 同上
- [x] `src/index/tiered/events.rs` — 同上
- [x] `src/index/tiered/tests.rs` — 检查 `unix_secs_for_test` 是否可复用

### P1：去重 l2_partition.rs 段导出
- [x] 提取 `TRIGRAM_SENTINEL`、`FKM_MAGIC`、`FKM_VERSION`、`FKM_FLAG_LEGACY`、`FKM_FLAG_RKYV` 为模块级 `const`
- [x] 抽取 `build_v6_segments` 公共构建逻辑
- [x] 抽取 `write_v6_segments_to_writer` 公共写入逻辑
- [x] `export_segments_v6` 改为调用公共函数
- [x] `export_segments_v6_to_writer` 改为调用公共函数
- [x] `export_segments_v6_compacted_to_writer` 改为调用公共函数

### P2：统一 io_governor.rs 锁策略
- [x] `src/io_governor.rs` — `use std::sync::Mutex` → `use parking_lot::Mutex`
- [x] 所有 `.lock().unwrap()` → `.lock()`
- [x] 确认无 `std::sync::Mutex` 残留

## 测试项

- [x] `dupe:` 查询返回编译错误（非空结果）
- [x] `unix_secs` 在 src/ 中仅 util.rs 一处定义
- [x] `io_governor.rs` 中无 `.lock().unwrap()`
- [x] l2_partition 段导出结果与重构前一致（快照测试）

## 验收标准

- [x] `cargo test -q` 全量通过
- [ ] `cargo fmt --check`
- [ ] `cargo clippy --all-targets -- -D warnings`
- [x] `git diff --check`
