# 阶段 C：任务清单（LSM + Flush/Compaction）

## 目标

- [√] 长期 mmap 基座 + 内存 Delta：启动不做全量 hydration
- [√] 目录化布局：Manifest + 多 Segment 文件（可增量 Flush）
- [√] 查询合并：newest→oldest 的正确覆盖语义（含跨段 delete）
- [√] Flush：Delta 刷盘为新 Segment（并在运行时即时加入查询层）
- [√] Compaction：多段触发后台合并为新 Base（控制段数量）
- [√] 可观测：MemoryReport 增加段数量指标
- [√] 测试：覆盖 LSM delete 与 delete→recreate 的合并语义（`cargo test` 通过）
- [√] 文档：补齐 wiki（阶段 C 架构、Manifest、段合并语义）
- [√] 迁移：开发完成后迁移方案包到 `helloagents/history/2026-02/` 并更新 `history/index.md`
