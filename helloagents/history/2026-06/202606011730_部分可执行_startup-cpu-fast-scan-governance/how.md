# 启动 CPU 峰值治理方案设计

> 状态：部分可执行（2026-06-13 复核）。Phase 1 核心止血已落地（`5b52d62`、`d25c91d`）；Phase 2/4 已并入 `202606011812_待执行_lease-hotset-fast-scan-shrink`；Phase 3 已收缩为 M3 的 hotset-only sentinel registry。

## 总体策略

本包只保留已落地止血记录和 M3 关系说明，不再作为独立实施入口。后续启动 CPU 治理沿 M3 执行：lease hotset 承担 5 秒 fast scan SLA，普通冷目录交给有界最终一致；sentinel 持久化只服务 hotset，初始补扫与真实变化队列拆分并入同一里程碑。

## Phase 1：启动期 fast scan 背压

新增启动期专用预算，默认比 steady-state 更保守：

- `tiered_watch.fast_scan_startup_grace_secs`：启动期窗口，建议默认 120 秒。
- `tiered_watch.fast_scan_startup_bootstrap_budget_per_tick`：启动期每 tick sentinel 注册预算，建议默认 128 或 256。
- `tiered_watch.fast_scan_startup_readdir_budget_per_tick`：启动期初始补扫 readdir 预算，建议默认 32 或 64。
- `tiered_watch.fast_scan_startup_stat_budget_per_tick`：启动期 sentinel stat 预算，建议默认按 SLA 所需最小值附近配置，例如 512。

实现要点：

- `spawn_tiered_fast_scan_loop` 根据 daemon uptime 或 loop start instant 选择 startup/steady-state budget。
- 初始补扫队列积压时暂停 bootstrap，避免无限扩大待扫描队列。
- steady-state 真实 sentinel changed-dir 继续优先处理，避免为了启动 backfill 牺牲用户刚创建文件的 5 秒发现。

## Phase 2：区分初始补扫与真实变化（并入 M3）

将新注册 sentinel 的补扫从真实变化中拆出来：

- 新增 `DirtyReason::FastScanBootstrapDir` 或在 fast scan result 中区分 `initial_dirs` / `changed_dirs`。
- DirtyQueue 对 `FastScanBootstrapDir` 使用低优先级或独立小批量。
- `FastScanChangedDir` 保持高优先级，用于真实目录项变化。
- `/metrics` 和 `/watch-state` 分别统计 initial/backfill 与 real changed-dir。

关键收益：

- 启动恢复工作可以慢慢做。
- 用户运行期新增文件仍走快速路径。
- CPU 峰值和 SLA 路径不再互相抢预算。

执行位置：`202606011812_待执行_lease-hotset-fast-scan-shrink` 的“启动治理 Phase 2”。

## Phase 3：持久化 sentinel registry（收缩为 hotset-only）

不再持久化全部 L1/L2 sentinel registry。M3 只持久化 active lease hotset sentinel，clean shutdown + stable snapshot 时按保守门禁复用：

持久化字段建议以 M3 为准，包括：

- path
- lease_kind / lease priority / expires_at / last_used_at
- mount_id / fstype / mount source identity
- fast scan mount class
- directory sentinel signature：dev、ino、mtime、ctime、nlink
- last_checked_unix_ms
- strict_sla_allowed / sentinel_state
- registry_version
- config fingerprint：roots、exclude_dirs、include_hidden、follow_symlinks、fs policy 关键项
- stable snapshot generation / WAL seal id

恢复规则：

- 仅当 `last_clean_shutdown = true`、`snapshot_source = stable`、`wal_gap_detected = false`、`recovery_requires_rebuild = false` 时尝试恢复。
- config fingerprint 不匹配时丢弃 registry，走限速 bootstrap。
- mount identity 不匹配或 fstype 变为 untrusted 时降级为需要重新 bootstrap。
- 恢复成功的 hotset sentinel 可继续按 fast scan tick 覆盖；恢复不可信时只进入限速 backfill，不能报告 strict SLA ok。

执行位置：`202606011812_待执行_lease-hotset-fast-scan-shrink` 的“hotset-only sentinel registry”。

## Phase 4：观测与验收（并入 M3）

M3 新增或调整 `/watch-state`、`/health`、metrics 字段：

- `fast_scan_hotset_lease_count`
- `fast_scan_hotset_sentinel_count`
- `fast_scan_explicit_lease_count`
- `fast_scan_auto_lease_count`
- `fast_scan_lease_evictions`
- `fast_scan_lease_renewals`
- `fast_scan_initial_backfill_pending`
- `fast_scan_real_changed_dirs`
- `fast_scan_apply_dropped_stale_batches`
- `fast_scan_scan_workers_active`
- `fast_scan_io_budget_limited_count`
- `cold_sweep_period_estimate`
- `dirty_backlog`

验收测试：

- clean shutdown 后重启：只恢复 hotset sentinel registry，不产生全量 L1/L2 initial scan。
- config 改变后重启：丢弃 registry，走限速 bootstrap。
- unclean shutdown 后重启：不信任 registry，走限速 bootstrap。
- 启动 backfill 期间 hotset 新建文件：真实 changed-dir 优先，仍在 5 秒 SLA 窗口内可搜索。
- 大冷段 fixture：启动期 bootstrap/readdir 不超过配置预算。

执行位置：`202606011812_待执行_lease-hotset-fast-scan-shrink` 的“启动治理 Phase 4”。

## 风险与取舍

- 过度降低启动预算会延长冷目录追平时间，但不得超过配置可解释上界。
- sentinel 持久化必须保守，宁可丢弃重建 hotset registry，也不能错误声称 strict SLA ok。
- 需要避免 runtime state 写入自身被 watcher 监听，可沿用现有 snapshot/runtime state ignore path。
