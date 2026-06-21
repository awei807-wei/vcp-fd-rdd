# 任务清单（未确认 / 不可执行）

## 状态门禁

- 方案状态：未确认。
- 执行权限：不可执行。
- 任务状态：全部为 `[?]`，表示需要用户确认后才能拆成正式实施任务。
- 禁止事项：不得基于本方案包直接运行 `~exec`；不得修改代码默认行为；不得迁移本包到 history 表示已执行。

## 待确认决策

- [?] 确认是否采用 “Budgeted Adaptive Indexing / 预算约束下的自适应索引” 作为技术路线名称。
- [?] 确认是否把 P0 定义为 “返回前 Top-K verify + subtree tombstone + query IO 硬上限”。
- [?] 确认默认查询是否必须返回 `validated=true` 的结果，或保留 lazy validation 默认。
- [?] 确认是否新增 `query.max_verify_per_query`、`query.verify_timeout_ms`、`query.allow_sync_readdir=false`。
- [?] 确认是否将 lazy validation 改为低功耗 profile / 后台补偿策略，而非默认正确性策略。
- [?] 确认是否引入 `RecordState::{Confirmed,Suspect,Tombstone}`。
- [?] 确认 base index 是否需要持久化 `size` 和 `ctime_ns`，以及可接受的 mmap / snapshot 体积增长。
- [?] 确认 subtree tombstone 的结构、TTL、内存上限与 prefix filter 位置。
- [?] 确认 repair 是否改为 `scan_dir_slice(max_entries, max_ms)` 游标式分片。
- [?] 确认是否把 dir-only fingerprint 作为 directory manifest 之前的轻量哨兵。
- [?] 确认是否把默认 `watch_mode` 从 `recursive` 迁移到 `tiered`，或先只做配置建议。
- [?] 确认 `/proc` sampler 是否作为 P1 experimental 功能接入。
- [?] 确认 proc sampler 指标：`proc_scan_duration_ms`、`pids_seen`、`pids_denied`、`fdinfo_read_count`、`readlink_count`。
- [?] 确认统一 Budget Manager 的优先级顺序和降级策略。
- [?] 确认网络盘继续保持客户端 best-effort / scan-only，不承诺实时。
- [?] 确认 Server Mode 是否作为 P2 独立大版本，不进入 P0/P1。

## 不可执行任务占位

- [?] 拆出正式方案包：P0.5 rename 事件短线止痛；~~修订现有方案包~~（2026-06-11 已完成修订：`202606061413_待执行_fix-rename-event-missing-new-file` 已明确只把孤立 `RenameMode::To` 当 Create，待确认后可执行）
- [?] 拆出正式方案包：P0-1 query-time Top-K verify；明确配置、默认 lazy validation 策略、HTTP/UDS 返回语义和 verify 预算。
- [?] 拆出正式方案包：P0-2 runtime subtree tombstone；第一版优先运行时 prefix filter，是否持久化另行确认。
- [?] 拆出正式方案包：P0-3 sliced repair；先设计 cursor、DirtyQueue 扩展、slice 上限和 retry 语义。
- [?] 设计 P0 query verifier 配置和 HTTP/UDS 返回语义。
- [?] 设计 subtree tombstone 数据结构和 prefix filter 查询入口。
- [?] 设计 repair slice 游标、retry 和 DirtyQueue 交互。
- [?] 设计 Budget Manager API：申请、消耗、拒绝、降级、metrics。
- [?] 设计 proc sampler 的权限边界、采样上限和 watch lease 接口。
- [?] 设计默认 tiered profile 迁移验证矩阵。
- [?] 设计 Server Mode 的最小协议边界，但不实施。
- [?] 生成正式可执行方案包前，重新确认所有文件行号仍与当前代码一致。

## 验证要求（仅供后续正式方案参考）

- [?] P0 后续必须补测试：删除文件不返回、mtime 变化返回 changed 或不返回、宽泛查询 verify 不超过预算。
- [?] P0 后续必须补测试：大目录删除使用 subtree tombstone 减少 stat 数。
- [?] P0 后续必须补测试：query 线程不执行同步 readdir。
- [?] P1 后续必须补测试：proc sampler 不越权、不因 `/proc` 大量 fd 造成 CPU 峰值。
- [?] P1 后续必须补测试：tiered 默认策略不扩大网络/FUSE strict SLA。
- [?] P2 后续必须补测试：server scan checkpoint 可恢复，remote query 不伪造实时性。
