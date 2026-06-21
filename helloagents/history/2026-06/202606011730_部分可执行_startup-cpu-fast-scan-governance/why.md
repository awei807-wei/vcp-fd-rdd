# 启动 CPU 峰值治理方案原因

> 状态：部分可执行（2026-06-13 复核）。
>
> - Phase 1 的"补扫积压暂停 bootstrap"、"流式早停 + 冷却"已由提交 `5b52d62`、`d25c91d` 落地；启动期专用预算字段（`fast_scan_startup_*`）保留为 M3 后按实测决定的延后项。
> - Phase 2（初始补扫与真实变化队列拆分）与 Phase 4（观测字段）已并入 `202606011812_待执行_lease-hotset-fast-scan-shrink`。
> - Phase 3 不再做“为所有 L1/L2 目录持久化 sentinel registry”，已收缩为 M3 的 hotset-only sentinel registry。

## 背景

用户反馈每次启动 `fd-rdd --watch-mode tiered` 时 CPU 会暴涨，怀疑是索引数据库不可信时触发重建。运行态复核显示当前不是 hard rebuild：

- `/health.snapshot_source = "stable"`
- `/health.recovery_requires_rebuild = false`
- `/health.startup_repair_ran = false`
- `/status.is_rebuilding = false`

当前更符合 fast scan 启动覆盖成本集中释放：旧语义把全部 L1/L2 已知目录纳入 5 秒覆盖，sentinel 是运行时内存态，重启后需要重新 bootstrap；新增 sentinel 会触发一次 `FastScanChangedDir` 初始补扫，这能避免漏索引，但在大目录/大冷段场景会把启动 CPU 峰值放大。

## 问题本质

启动后需要恢复一致性，但不应该每次都把“所有非 L0 已知目录”当作全新未知状态集中补扫。当前问题由三个因素叠加：

1. fast scan sentinel registry 不持久化，clean shutdown 后也要重新覆盖。
2. bootstrap 默认预算偏激进：每 tick 最多注册 2048 个 sentinel、stat 5000 个目录、readdir 512 个 changed dir。
3. 初始补扫和真实目录变化共用同一 `FastScanChangedDir` 路径，无法对启动 backfill 单独降优先级、限速和观测。

## 收口目标

- 本包不再作为 M3 独立实施入口，M3 实施入口统一为 `202606011812_待执行_lease-hotset-fast-scan-shrink`。
- clean shutdown + stable snapshot 启动时，只恢复 hotset sentinel，不恢复全部 L1/L2 sentinel。
- 启动 CPU 峰值通过 hotset 预算、初始补扫/真实变化队列拆分、观测字段解释和控制。
- `/watch-state` / `/health` 能解释 hotset 规模、initial backfill、真实 changed-dir、冷目录追平上界。

## 非目标

- 不再承诺全部 L1/L2 目录 5 秒发现。
- 不实现全量 sentinel registry 持久化。
- 不把超大 `Documents` / `Downloads` 整棵强行注册 L0 watcher。
- 不用全量 rebuild 掩盖 sentinel 恢复问题。
