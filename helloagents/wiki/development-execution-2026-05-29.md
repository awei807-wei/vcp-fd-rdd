# 2026-05-29 开发执行记录

## 执行范围

本轮执行覆盖合并开发计划中的三个前置范围：

- 批次 0：基线与执行护栏。
- 批次 1.1：Runtime Boundary Closure 的 mount policy / FUSE probe 小批次。
- 批次 1.2：Runtime Boundary Closure 的 quarantine sidecar verify 小批次。

后续 case/pathconf、clock/io diagnostics、ArcSwap/MemoryReport、memory_light、query experience、watcher balanced、storage cleanup 均未在本轮展开。

## 批次 0 基线

执行前后已确认的命令基线：

- `cargo fmt --check`：通过。
- `cargo check -q`：通过。
- `cargo test -q fs_policy --lib`：8 passed。
- `cargo test -q tiered_diagnostics_include_shared_mount_policy_counters --lib`：1 passed。
- `cargo test -q`：通过；库测试 294 passed，集成测试批次通过，存在既有 ignored 测试。
- `git diff --check`：通过。
- 隔离 daemon smoke：`scripts/smoke-search-syntax.sh --root /home/shiyi/Downloads/vcp-FD/fd-rdd --cleanup` 返回 PASS。

隔离 daemon 使用 `/tmp/fd-rdd-smoke` 与 HTTP 6060 端口，验证后已停止。smoke 期间 `/status` 返回 `indexed_count=254`、`is_rebuilding=false`；`/health.diagnostics.watchers` 已包含 mount policy reason matrix 字段，当前普通本地 root 基线计数为 0。

本轮前置观测中已记录的可用接口字段：

- `/memory`：可观察 RSS、base file count、manifest-only entries、L2 tombstone count 等内存拆项。
- `/health`：保留 summary 字段，并包含 `diagnostics.system/storage/security/clocks/watchers/io`。
- `/watch-state`：recursive / tiered watcher 状态、L0/L1/L2/L3、预算与 backlog。
- `/debug/tiered-watch`：单目录 watcher runtime 诊断。

## Storage Legacy 只读审计

本轮只做只读清单，不删除、不迁移、不隔离旧路径。真正 cleanup 留到批次 7。

前置开发期间不可随意触碰的兼容边界：

- `src/storage/snapshot.rs`：v2-v5 legacy snapshot、v6 mmap snapshot、LSM manifest、`lsm_append_delta_v6`、`lsm_replace_base_v6`、v6 segment 读取与写入兼容路径。
- `src/storage/snapshot_v7.rs`：当前 v7 cold mmap 主路径，同时保留 legacy v7、旧 40B entry、旧 basename trigram 段回退、v6 segments 构建 v7 的边界。
- `src/index/mmap_index.rs`：v6 mmap 查询兼容层，包含 legacy FileKey map 反查和旧段 fallback。
- `src/index/l2_partition.rs`：`PersistentIndex` 当前 runtime 主结构，同时仍导出 v6 segments、legacy metas、FileKeyMap 兼容表。
- `src/index/tiered/memory.rs`：`refresh_base()` 会全量 materialize L2 到 base，只能保留在 snapshot/rebuild/测试兼容或明确 fallback 边界。
- `src/index/tiered/query.rs`：base 为空且 L2 有内容时仍会调用 `refresh_base()`，主要服务测试和内部直接写 L2 的兼容路径；批次 2A 需要重点审计 guard 和 full-scan metrics。
- `src/index/tiered/sync.rs`：fast-sync 的局部扫描和删除对齐仍有 `refresh_base()` fallback，应避免在普通事件热路径扩大使用。
- `src/index/l1_cache.rs`：L1 cache 仍保持 path/FileKey 语义，generation-aware key 留到后续评估。
- `tests/p0_storage_compat.rs`、`tests/p1_*` 中的 v6/v7/WAL/LSM compat 测试是兼容边界护栏，不应在前置批次删除。

风险等级：

- 高：`refresh_base()` 被重新带入普通 query hot path 或高频 event loop。
- 高：删除 v6/v7/LSM 兼容读取而未补 old snapshot upgrade/reload fixture。
- 中：修改 `PersistentIndex` 导出 v6 segments 的布局后未同步 mmap/LSM 兼容测试。
- 中：L1 cache key 在 generation 语义未稳定前外泄到对外 API。

## 批次 1.1 实施记录

已完成：

- 新增 `SharedMountPolicyCounters`，支持跨 full scan、rebuild、fast-sync、immediate scan、dynamic watch 共享 mount policy 拒绝计数。
- `FsPolicy::check_path_counted()` 统一执行 policy 并记录 deny reason、allow override、FUSE probe timeout 计数。
- `FsScanRDD` 接收共享 counters，serial 和 parallel walker 的 `filter_entry` 均记录被拒绝挂载。
- `IndexBuilder` 在 full build、strategy full build、incremental scan 中向 `FsScanRDD` 注入 counters。
- `TieredIndex` 持有共享 counters，并在 `DiagnosticSource` 中累加到 `/health.diagnostics.watchers`。
- fast-sync 与 `scan_dirs_with_depth` walker 接入同一 counters。
- `EventPipeline` 在 `WatchCommand::Add`、`AddEphemeral`、`Replace`、`ReplaceEphemeral` 和新建目录 dynamic watch 前执行 mount policy；拒绝时回滚 tiered / ephemeral runtime reservation，并跳过 `watch()` 与后续深扫。
- FUSE / SSHFS 可疑 mount 支持后台 probe timeout cache；扫描线程只消费 `Pending/Ready/TimedOut/Failed` 缓存状态，pending/timeout/failed 时保守拒绝，不在扫描线程直接执行可能挂起的 `readdir`。
- `fs_policy.fuse_probe_timeout_ms` 通过 `TieredIndex` 贯穿 full build、rebuild、fast-sync、immediate scan、dynamic watch 和 ephemeral watch；显式 `allow_mounts` 继续绕过 probe 并记录 `allowed_override_count`。
- 新增 diagnostics 单元测试，验证共享 counters 会进入 `DiagnosticReport.watchers`。
- 新增 FUSE probe 单元测试，覆盖后台 ready、timeout 只计一次、显式 allow mount 放行。

未完成，留给批次 1 后续小提交：

- 真实拒绝挂载点的集成 fixture 尚未补齐。
- case/pathconf、clock/io diagnostics 未在本轮实施。

## 批次 1.2 实施记录

已完成：

- `TieredIndex` 启动加载时在 attach WAL 后读取 `quarantine-sidecar.json`，先恢复 active quarantine roots 并安装 Freeze Gate。
- WAL replay 改为保留 root/file 记录的原始顺序分段回放：遇到 `OFFLINE_ROOT` / `ONLINE_ROOT` 立即刷新 quarantine state 和 Freeze Gate，后续文件事件按当时的 freeze 状态进入或被拦截。
- daemon 启动时在事件管道前启动 sidecar verify worker；worker 读取当前 mount table，验证 `fs_uuid` 或 `major:minor + source + fstype`，不接受 root path 单独命中。
- 设备恢复时先 append `ONLINE_ROOT` 到 WAL；写入成功后才解除 freeze、更新 sidecar online 状态，并把 affected prefixes 加入 DirtyQueue 做局部 scan。
- 设备仍离线或 identity 不匹配时保持 Freeze Gate，不写 `ONLINE_ROOT`，不产生破坏性 tombstone。
- `/health.diagnostics.storage` 增加 `quarantine_verify_pending` 与 `quarantine_verified_roots`，结合既有 `quarantine_roots`、`freeze_gates`、`freeze_blocked_events` 观察 sidecar verify 状态。
- 新增回归测试覆盖 sidecar restore 先装 Freeze Gate、online verify 后 WAL/解冻/局部 scan、identity mismatch 维持冻结，以及 WAL 中 `ONLINE_ROOT` 后续文件事件不被旧 sidecar freeze 误拦截。

## 方案包执行顺序

当前执行顺序记录如下：

- `202605291305_runtime-boundary-closure`：进行中，已完成 mount policy / FUSE probe 与 quarantine verify 小批次；下一步进入 case/pathconf。
- `202605291314_arcswap-zero-copy-governance`：待执行批次 2A。
- `202605291313_disk-first-memory-light`：待执行批次 2B。
- `202605291316_hardlink-physical-grouping`：待执行批次 3。
- `202605291311_query-experience-features`：待执行批次 4 和批次 5。
- `202605291312_watcher-balanced-evolution`：待执行批次 6。
- `202605291315_storage-legacy-cleanup`：待执行批次 7。

批次 1 尚未完成，因此 `202605291305_runtime-boundary-closure` 不迁移到 `history/`。
