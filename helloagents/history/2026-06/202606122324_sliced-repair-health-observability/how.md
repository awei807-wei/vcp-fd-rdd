# M1-3 分片 repair 与有界最终一致观测设计

## 当前行号复核

- `src/index/tiered/sync.rs:611`：dirty queue entry 处理入口。
- `src/index/tiered/sync.rs:678`：周期 cold scan sliced repair 调用点。
- `src/index/tiered/sync.rs:1027`：通用 budgeted scan 当前实现。
- `src/index/tiered/sync.rs:1204`：`scan_dir_repair_slice_with_project_markers`。
- `src/query/server.rs:31`：`HealthTelemetry`。
- `src/query/server.rs:153`：`HealthResponse`。
- `src/main.rs:459`：health provider 组装 cold sweep/backlog 字段。
- `src/main.rs:1099`：dirty queue loop。
- `src/main.rs:1352`：tiered scan loop 入队 PeriodicColdScan。

## 实现策略

1. DirtyQueue 支持 slice cursor
   - 扩展 `DirtyQueueEntry` 增加 `repair_cursor: Option<DirtyRepairCursor>`。
   - cursor 只保存当前目录路径和 last_seen path，保持 runtime-only，不写 snapshot。
   - DirtyScopeKey 继续按 scope 合并；如果同一目录已有待处理 entry，保留更早的 cursor 或回退为无 cursor，避免跳过未扫描区间或丢全量扫描。

2. 分片扫描函数
   - 新增 `scan_dir_slice(dir, cursor, max_entries, max_ms, project_markers, discard_if_event_seq_advances)`。
   - Unix 下使用 `opendir/readdir/telldir/seekdir` 读取一层目录并携带 offset cursor，非 Unix 回退为 `std::fs::read_dir` + offset。
   - 单 slice 最多处理 512 条或 20ms；处理文件/目录 metadata 后复用现有 upsert 逻辑。
   - 返回 `completed` 与 `next_cursor`，未完成则重新入 DirtyQueue。

3. DirtyQueue 重入
   - PeriodicColdScan 走 sliced path。
   - StartupRepairDeferred 仍保留递归 fast-sync，避免启动期 rename subtree 深层补偿被浅层 slice 漏掉。
   - FastScanChangedDir 仍保留 fast_sync，避免影响 5 秒热路径。
   - 未完成 slice 使用同 reason / priority 重入，not_before 使用现有 debounce。

4. 观测字段
   - TieredIndex 增加原子状态：last completed unix secs、period estimate secs。
   - health provider 填入 `dirty_backlog = index.dirty_queue_len()`。
   - `cold_sweep_period_estimate` 默认来自 `tiered.l2_scan_interval_secs` / `l3_scan_interval_secs` 中的最大可解释周期，至少为当前 PeriodicColdScan 间隔。

## 边界

- 不新增外部依赖。
- 不修改 stable snapshot / WAL 格式。
- 查询线程仍禁止同步 readdir。
- 如目录在 slice 间删除或不可读，当前 entry 标记失败并按现有 retry 规则处理。
