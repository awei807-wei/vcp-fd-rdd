# 轻量迭代：弹性计算接回（AdaptiveScheduler -> TieredIndex / IndexBuilder）

任务清单:
- [√] 在 `src/index/tiered.rs` 注入 `AdaptiveScheduler`，并在 `spawn_full_build/spawn_rebuild` 选择策略后执行构建
- [√] 在 `src/index/l3_cold.rs` 增加 `full_build_with_strategy`，按策略控制扫描并行度
- [√] 在 `src/core/rdd.rs` 为 `FsScanRDD` 增加可控并行扫描（基于 `ignore` 的 parallel walker）
- [√] 更新知识库：`helloagents/wiki/*`、`helloagents/CHANGELOG.md`、回忆录
- [√] 运行测试：`cargo test`（含 `--features mimalloc`）
