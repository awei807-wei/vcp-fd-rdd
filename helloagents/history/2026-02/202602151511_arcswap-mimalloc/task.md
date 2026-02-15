# Step 1：ArcSwap + mimalloc（动态止血）— 任务清单

- [√] 依赖：引入 `arc-swap`，并将 `TieredIndex.l2` 改为 `ArcSwap<PersistentIndex>`
- [√] 重建：`spawn_rebuild/spawn_full_build` 改为“后台构建新索引→回放 pending 事件→原子切换”
- [√] 一致性：在 `apply_events` 增加 pending 事件缓冲（重建期间收集）
- [√] 缓存：索引切换时清空 L1，避免返回过期缓存
- [√] 分配器：引入 `mimalloc` 可选 feature，并设置为全局分配器（feature 开关）
- [√] 测试：新增“重建切换不丢事件”的用例
- [√] 文档：更新 `helloagents/wiki`（重建语义与动态内存止血说明）
- [√] 迁移：开发完成后迁移方案包到 `helloagents/history/2026-02/` 并更新 `history/index.md`

> 验证备注：当前环境无法访问 crates.io（无网络/DNS），因此未能本地运行 `cargo test` 进行编译验证；已补齐单元测试用例与实现，待有网络环境下执行验证。

> ✅ 后续验证：2026-02-15 已在本机执行 `cargo test` 通过（当时 15 个测试用例全部通过；后续阶段 C 增加用例后测试数上升，但该改动已被覆盖回归）。
