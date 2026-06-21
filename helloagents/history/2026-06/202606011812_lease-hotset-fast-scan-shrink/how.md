# M3 lease hotset 正式化 + fast scan 收缩设计与落地记录

> 状态：已实施并归档。原方案由“全部 L1/L2 fast scan 5 秒覆盖”收缩为“active lease hotset 5 秒 + 冷目录有界最终一致”。

## 落地位置

- `src/event/tiered_watch.rs`
  - `FastScanLeaseKind`、`FastScanLease`、`FastScanSentinelState`、hotset lease map、sentinel map、initial backfill queue 与 real changed queue。
  - `grant_fast_scan_lease(s)` / TTL / 续租 / 非 explicit 驱逐 / hotset 预算。
  - `bootstrap_fast_scan_dirs` 只接受未覆盖且有效的 lease，不再从全部 L1/L2/cold snapshot 枚举。
  - `fast_scan_tick` 拆分 `initial_dirs` 与 `changed_dirs`，并只对 active/backfill hotset sentinel 做检查。
  - `persist_fast_scan_registry` / `restore_fast_scan_registry` 只持久化 hotset sentinel，并以 clean shutdown、snapshot source、WAL checkpoint、config fingerprint、mount identity 做恢复门禁。
  - runtime report 输出 hotset lease/sentinel、explicit/auto、eviction/renewal、initial backfill、real changed、dropped stale batch、scan worker、I/O budget limited 等字段。

- `src/config.rs`
  - 新增 `l1_l2_fast_scan_hotset_max_leases`、`l1_l2_fast_scan_lease_ttl_secs`、`l1_l2_fast_scan_proc_sampler_lease_ttl_secs`、`l1_l2_fast_scan_explicit_lease_ttl_secs`、`l1_l2_fast_scan_sentinel_registry_max_entries`。

- `src/main.rs`
  - 启动时应用 fast scan 配置、恢复 hotset registry、seed `hot_dirs` 显式 lease。
  - `/search` 查询命中通过 query server provider 发放 `Query` lease。
  - dirty queue 对 stale hit 发放 `StaleHit` lease，project marker 发放 `ProjectMarker` lease，L0 事件与 proc sampler 分别发放 `L0Event` / `ProcSampler` lease。
  - fast scan loop 从 `pending_fast_scan_lease_dirs` 注册 sentinel，`initial_dirs` 入 `FastScanBootstrapDir`，`changed_dirs` 入 `FastScanChangedDir`。
  - clean shutdown final snapshot 后持久化 hotset registry。

- `src/event/sync.rs`、`src/index/tiered/sync.rs`
  - 新增 `DirtyReason::FastScanBootstrapDir`，默认低优先级。
  - `FastScanChangedDir` 继续走 fast-sync apply，`FastScanBootstrapDir` 走低优先级 depth=1 补扫。
  - 低优先级 stale batch apply 带 event-seq guard，旧扫描结果会丢弃并计入 `fast_scan_apply_dropped_stale_batches`。
  - `cold_sweep_period_estimate` 在 L3 disabled 时不再被默认 L3 interval 放大。

- `src/query/server.rs`
  - `/search` 对返回结果前 32 个路径反推 hotset 目录：文件结果取父目录，目录结果取自身。
  - `/health` / diagnostics 序列化新增 hotset 与 backfill 观测字段。

- `tests/p1_fast_scan_sla.rs`
  - 改为真实 daemon hotset SLA + cold eventual consistency：先通过 seed 查询建立 hotset lease，再验证 hotset create/delete/rename 5 秒窗口；冷目录 create 只要求在配置 cold sweep / dirty repair 上界内可搜索。

## 当前语义

- L0 watcher：强实时。
- Lease hotset：fast scan 目标 5 秒，只覆盖 active lease hotset。
- 普通 L1/L2/L3：不承诺 5 秒；通过 DirtyQueue、PeriodicColdScan、query repair 和 cold sweep 提供有界最终一致。
- 查询正确性：返回前验真仍是正确性来源，fast scan 只负责补全与新鲜度。

## Lease 触发源

- 查询命中反推：查询结果目录进入 `Query` lease；目录结果直接 lease 目录自身。
- Stale hit：返回前验真发现 stale/missing/changed 后记录父目录，由 fast scan loop 发放 `StaleHit` lease。
- Project marker：dirty scan 发现项目 marker 后，项目根登记为候选并发放 `ProjectMarker` lease。
- L0 事件：L0 watcher 事件父目录发放 `L0Event` lease；L0 覆盖目录不计入 strict fast scan backfill 缺口。
- Proc sampler：同用户写句柄采样目录发放短 TTL `ProcSampler` lease。
- 显式配置：`tiered_watch.hot_dirs` seed 为 `Explicit` lease；`l1_l2_fast_scan_explicit_lease_ttl_secs = 0` 表示永久。

路径形态 query miss 只进入 DirtyQueue 做冷目录补偿，不直接发放 fast scan lease，避免冷目录因补偿查询扩张 hotset。

## TTL / 续租 / 驱逐 / 总预算

- 自动 lease 默认 TTL：`l1_l2_fast_scan_lease_ttl_secs`，默认 1800 秒。
- Proc sampler TTL：`l1_l2_fast_scan_proc_sampler_lease_ttl_secs`，默认 300 秒。
- Explicit TTL：`l1_l2_fast_scan_explicit_lease_ttl_secs`，默认 0，即永久。
- 总预算：`l1_l2_fast_scan_hotset_max_leases`，默认 512。
- 续租：同路径 lease 会更新 `last_used_unix_secs`、延长 `expires_unix_secs`、提高优先级和 `source_score`，并累加 `renew_count`。
- 驱逐：预算满时驱逐非 explicit 中低优先级、低 score、低续租、即将过期的 lease；预算仍不足时 `/watch-state` / `/health` 标记 budget degraded。

## Sentinel 持久化（hotset-only）

- registry 写入稳定快照目录下的 `fast-scan-hotset-registry.json`。
- 持久化字段包含 path、lease kind/priority/expires/last used、sentinel signature、mount id/fstype/class、strict/state、config fingerprint、snapshot source 与 WAL checkpoint。
- 恢复必须满足 previous clean shutdown、registry clean shutdown、snapshot source 匹配且稳定、WAL checkpoint 匹配、config fingerprint 匹配、mount identity 匹配。
- 不可信 registry 保留为 backfill pending hotset 候选，进入限速 backfill；backfill 完成前 `fast_scan_local_strict_ok=false`。

## 启动治理并入项

- Phase 2：`FastScanBootstrapDir` 与 `FastScanChangedDir` 已拆分。初始补扫低优先级，真实 sentinel 变化保持 normal 优先级，避免启动 backfill 挤占运行期变化。
- Phase 3：原全量 sentinel registry 已裁剪为 hotset-only registry。
- Phase 4：新增 hotset/backfill/real change/dropped stale/budget limited 观测字段，并同步 `/watch-state`、`/health`、diagnostics、metrics JSONL、README、CHANGELOG、tests README 与 watcher wiki。

## 并行 scan worker 评估

本里程碑保持单 worker，不引入并行 scan worker。理由：当前瓶颈已通过“只扫描 hotset + 初始/真实队列拆分 + 低优先级 stale apply guard”结构性降低；并行 scan worker 会引入额外 apply ordering 风险。若后续引入并行 worker，必须携带 `event_seq` / `dir_epoch` / `scan_id`，并由单线程或按目录分区的 apply actor 串行提交。
