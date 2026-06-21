# M1-3 分片 repair 与有界最终一致观测任务

## 实施任务

- [√] 扩展 DirtyQueueEntry cursor 与合并/retry 规则
- [√] 新增单目录 `scan_dir_slice`，限制 max_entries≈512、max_ms≈20
- [√] PeriodicColdScan 未完成 slice 重新入 DirtyQueue
- [√] StartupRepairDeferred 保留递归 fast-sync，覆盖启动期 rename subtree 深层补偿
- [√] `/health` 新增 cold_sweep_last_completed、cold_sweep_period_estimate、dirty_backlog
- [√] README/CHANGELOG/tests/wiki 同步“结果真实但可能不全 + 有界追平”口径

## 测试项

- [√] 大目录 sliced repair 首次只处理 slice 上界并产生 backlog
- [√] 重新入队后从 cursor 继续，不重复从头扫描
- [√] slice 完成后更新 cold_sweep_last_completed
- [√] `/health` 序列化新增字段

## 验收标准

- [√] `cargo test -q sliced_repair`
- [√] `cargo test -q health`
- [√] `cargo test -q query`
- [√] `cargo test -q`
- [√] `cargo fmt --check`
- [√] `git diff --check`
