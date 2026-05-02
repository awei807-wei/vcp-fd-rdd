# 历史记录索引

## 2026-05

- `202605021500_v7-direct-base-load`：v7 快照直接挂载为 BaseIndexData，避免启动回灌 L2；snapshot 边界改写当前可见全集并补齐 v7 FileEntryIndex 反序列化排序重建。
- `202605021430_large-scale-pressure-tuning`：跑通 ignored 大规模压测，修复新目录 watch 竞态，冷启动改保守串行并节流，压低 initial_indexing CPU/RSS 峰值。
- `202605021313_hotpath-materialize-boundary`：将 BaseIndex/ParentIndex 全量 materialize 从普通事件热路径移到 snapshot/rebuild/兼容边界，fast_sync 删除对齐改走 BaseIndex。
- `202605021152_persistentindex-fileentry-paths`：将 `PersistentIndex` 运行时主存储迁移到 `FileEntry + paths`，旧 `CompactMeta + PathArena` 降级为兼容读写格式。
- `20260502_wrapup-stabilization`：收敛 causal-chain 与 wrap-up 后续合并问题，恢复编译/测试，移除 `disk_layers` 热路径和目录 rename 同步深扫。
