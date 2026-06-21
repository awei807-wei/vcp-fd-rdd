# M3 lease hotset 正式化 + fast scan 收缩任务

## 实施任务

- [√] 复核当前代码行号，更新本包 `how.md` 的锚点
- [√] 新增/改造 lease hotset 数据结构，覆盖触发源、TTL、续租、驱逐、预算
- [√] 接入 lease 触发源：查询命中反推、stale hit、project marker、L0 事件、proc sampler、显式配置
- [√] 将 fast scan sentinel 覆盖范围从全部 L1/L2 收缩为 active lease hotset
- [√] 将 sentinel registry 持久化收缩为 hotset-only，并实现 clean/stable/WAL/config/mount 恢复门禁
- [√] 并入启动治理 Phase 2：拆分初始补扫与真实变化队列，保证真实 hotset 变化优先
- [√] 并入启动治理 Phase 4：新增 hotset lease/backfill/真实变化/预算受限观测字段
- [-] 评估是否需要并行 scan worker；本里程碑保持单 worker，已沿用 event_seq stale-batch guard，保留 `fast_scan_scan_workers_active` 与 dropped stale 观测字段
- [√] 改写 `tests/p1_fast_scan_sla.rs` 为“hotset 内 5 秒 + 冷目录有界最终一致”两组断言
- [√] README / CHANGELOG / tests README / watcher wiki 同步 SLA 与产品口径
- [√] 更新 `helloagents/plan/202606011730_部分可执行_startup-cpu-fast-scan-governance`，标记 Phase 2/4 已并入 M3、Phase 3 改为 hotset-only

## 验收测试

- [√] Hotset 内 create 5 秒内可搜索
- [√] Hotset 内 delete 5 秒内不再返回
- [√] Hotset 内 rename 5 秒内旧路径消失、新路径可搜
- [√] 冷目录 create 不要求 5 秒，但在配置 cold sweep / dirty repair 周期内追平
- [√] `/health` 能解释冷目录追平上界：`cold_sweep_period_estimate`、`dirty_backlog`、hotset/backfill 字段
- [√] 低性能 profile 下稳态 CPU 相比全量 L1/L2 fast scan 语义显著下降；结构性验收为 bootstrap 与 tick 只枚举 active lease hotset，不再枚举全部 L1/L2/cold snapshot，真实数值仍需非沙箱低性能 profile 长稳采集
- [√] clean restart 后只恢复 hotset sentinel，不全量恢复 L1/L2 sentinel
- [√] registry 不可信时进入限速 backfill，不报告 hotset strict SLA ok

## 验证命令

- [√] `cargo fmt`
- [√] `cargo check`
- [√] `cargo test -q fast_scan --lib`
- [√] `cargo test -q query_hotset_dirs`
- [√] `cargo test -q query_hotset_dirs_uses_result_parent_dirs`
- [√] `cargo test -q tiered_watch --lib`
- [√] `cargo test -q health`
- [√] `cargo test -q --test p1_fast_scan_sla`
- [√] `cargo test -q`
- [√] `cargo fmt --check`
- [√] `git diff --check`
