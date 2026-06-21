# M1-1 查询返回前 Top-K 验真方案

> 状态：已完成。

## 范围

- `src/config.rs`：新增 `[query]` 配置表和默认值。
- `src/main.rs`：加载配置后把查询验真预算应用到 `TieredIndex`。
- `src/index/tiered/mod.rs` / `load.rs`：保存查询验真运行时配置。
- `src/index/tiered/query.rs`：在冷层/base 查询返回前执行预算化同步验真。
- `src/index/tiered/tests.rs`：补齐删除、identity/mtime 变化、宽泛查询预算和 lazy validation 默认口径测试。
- README/CHANGELOG/wiki 同步产品口径与配置。

## 配置语义

- `query.max_verify_per_query`：默认 150，约束单次查询最多同步验证的冷层候选数。
- `query.verify_timeout_ms`：默认 75ms，约束单次查询同步验证耗时上限。
- `query.allow_sync_readdir`：默认 false。本阶段不实现查询线程同步 readdir；若配置为 true 也只保留为前向兼容字段，不改变行为。
- `lazy_validation_enabled` 默认改为 false；显式开启后仍走后台补偿，返回 `Unknown` / `validated=false`。

## 查询执行策略

1. 每次 `execute_query_plan` 创建一个 `QueryVerifyBudget`，记录剩余验证次数、deadline、是否允许同步 readdir。
2. `validate_cold_result` 只在预算允许时执行 `metadata()`；预算耗尽时停止继续扫描冷层候选，并返回当前已确认结果。
3. 删除路径：写入 Delete tombstone、入 DirtyQueue、计 stale hit，不返回。
4. identity 或 mtime 变化：返回当前文件 metadata，标记 `freshness = "changed"`、`validated=true`，入 DirtyQueue；不会作为 `StaleChecked` 返回。
5. 不做同步 readdir 扩大范围；冷目录候选不全由 M1-3 sliced repair 与 health 字段解释。

## 风险控制

- 不改快照格式、不改 LSM/base 数据格式。
- 不改变 hot overlay 结果路径：hot 结果仍视为当前事件路径结果。
- synthetic 测试中 `FileKey.dev <= 1` 的非真实文件系统 fixture 保持 Unknown，避免把无 backing file 的历史测试误转为 tombstone。
