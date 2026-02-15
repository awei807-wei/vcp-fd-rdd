# 任务清单：events.wal

- [ ] 实现 `storage::wal::WalStore`：append / seal / replay / cleanup
- [ ] 扩展 LSM manifest：加入 `wal_seal_id` checkpoint（版本升级且兼容旧版本）
- [ ] `TieredIndex` 集成：
  - apply_events 前写 WAL
  - snapshot_now：seal + 传递 seal_id 给 snapshot/manifest + 成功后 cleanup
  - load_or_empty：加载 segments 后回放 WAL
- [ ] CLI/日志：输出 WAL 回放与 checkpoint 信息
- [ ] 测试：WAL 编解码、seal/replay、manifest 兼容
- [√] 知识库同步与方案包迁移

状态更新:
- [√] 实现 `storage::wal::WalStore`：append / seal / replay / cleanup
- [√] 扩展 LSM manifest：加入 `wal_seal_id` checkpoint（版本升级且兼容旧版本）
- [√] `TieredIndex` 集成（apply 前写 WAL；snapshot seal+checkpoint；启动回放）
- [√] CLI/日志：输出 WAL 回放信息；`--ignore-path` 已可用于排除日志反馈回路
- [√] 测试：WAL append/seal/replay 单测；全量 `cargo test` 覆盖
- [ ] 知识库同步与方案包迁移
