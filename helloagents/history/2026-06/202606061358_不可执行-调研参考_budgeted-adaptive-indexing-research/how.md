# 技术调研证据与实施收益（未确认 / 不可执行）

## 状态门禁

- 方案状态：未确认。
- 执行权限：不可执行。
- 本文件用途：把用户输入设想与当前代码事实精确锚定到文件、行号和函数。

## 1. 用户态事件能力边界

结论：当前项目已经摸到普通用户态可稳定使用的文件事件边界，默认仍应以 inotify / notify + stat/readdir 补偿为地基。

证据：

- `README.md:13`：项目定义为启动扫描后通过 inotify 持续增量更新。
- `helloagents/wiki/fanotify-prestudy.md:5`：fanotify 暂不适合作为默认 watcher 后端。
- `helloagents/wiki/fanotify-prestudy.md:25`：fanotify 必须 opt-in、capability-gated、失败回退。
- `helloagents/wiki/fanotify-prestudy.md:37`：FID-only 事件不能直接写 tombstone 或 live overlay。
- `helloagents/wiki/fanotify-prestudy.md:40`：fanotify overflow / fs error 必须触发 bounded reconciliation。
- `helloagents/wiki/fanotify-prestudy.md:83`：不对用户真实 home 全盘启用 fanotify 实验。
- `helloagents/wiki/fanotify-prestudy.md:91`：默认仍使用 notify tiered watcher。

实施收益：

- 不再投入主线精力追逐更深事件源。
- 把 fanotify 保留为实验后端或 benchmark 对象。
- 主线聚焦 verified results、bounded repair、budgeted watcher。

## 2. Index Core 现状

当前已有紧凑索引记录，但不是用户设想中的完整 `FileRecord`。

代码证据：

- `src/index/file_entry_v2.rs:20` `pub struct FileEntry`。
- `src/index/file_entry_v2.rs:21` `dev: u64`。
- `src/index/file_entry_v2.rs:22` `ino: u64`。
- `src/index/file_entry_v2.rs:23` `generation: u32`。
- `src/index/file_entry_v2.rs:24` `path_idx: u32`，并复用高位编码 kind。
- `src/index/file_entry_v2.rs:25` `mtime_ns: i64`。
- `src/core/rdd.rs:135` `pub struct FileMeta` 是运行时更完整 meta 结构。
- `src/index/base_index.rs:845` `fn entry_to_meta` 把 base entry 转回 `FileMeta`。
- `src/index/base_index.rs:849` base meta 当前 `size: 0`。
- `src/index/base_index.rs:855` base meta 当前 `ctime: None`。

差距：

- 缺少持久化 `size`、`ctime_ns`、`RecordState`。
- 没有 `Confirmed / Suspect / Tombstone` 状态机。
- `FileEntry` 更偏内存与 mmap 紧凑表示，不是验真状态模型。

收益：

- 增加 `RecordState` 可让 query verifier、repair 和 dirty manager 明确区分可信/可疑/删除。
- 增加 `size/ctime` 可提高 changed 判断质量，但会增加持久化体积，需要评估。

## 3. Query Verifier 现状

同步验真逻辑存在，但默认配置选择 lazy validation，当前不满足“返回前必须 verify”的强 SLA。

代码证据：

- `src/index/tiered/query.rs:700` `fn validate_cold_result` 是冷层命中校验入口。
- `src/index/tiered/query.rs:717` 如果 lazy validation 开启，先 `try_enqueue_lazy_validation`。
- `src/index/tiered/query.rs:719` lazy validation 分支返回 `QueryResultFreshness::Unknown`。
- `src/index/tiered/query.rs:723` lazy validation 分支返回 `validated = false`。
- `src/index/tiered/query.rs:729` 非 lazy 分支执行 `std::fs::metadata(&meta.path)`。
- `src/index/tiered/query.rs:746` 文件不存在时 `apply_query_delete`。
- `src/index/tiered/query.rs:773` 文件身份或 mtime 变化时标记 stale 并返回 changed result。
- `src/index/tiered/query.rs:794` 未变化时返回 `StaleChecked`。
- `src/index/tiered/query.rs:798` 未变化时返回 `validated = true`。
- `src/config.rs:689` 默认 `lazy_validation_enabled: true`。
- `src/index/tiered/lazy_validation.rs:163` `spawn_lazy_validation_worker`。
- `src/index/tiered/lazy_validation.rs:200` `fn validate_lazy_job` 执行异步 metadata 校验与修复。
- `src/query/server.rs:390` `search_handler` 是 HTTP 查询入口。
- `src/query/server.rs:405` 查询在 `spawn_blocking` 中执行。
- `src/query/server.rs:408` HTTP 查询有整体 timeout。
- `src/query/server.rs:440` HTTP 响应映射返回 `freshness / index_tier / validated`。

差距：

- 缺少 `max_verify_per_query`、`verify_timeout_ms`、`allow_sync_readdir=false` 等 query verifier 配置。
- 默认 `validated=false` 的冷结果仍可能返回。
- 当前 query 线程不会递归扫真实文件系统，这是好事；但同步 verify 缺少独立硬预算。

收益：

- P0 实施后可把“搜索返回必须验真”变成可测 SLA。
- 对删除、rename、mtime 变化的用户可见假结果会明显减少。
- 可以把 lazy validation 降级为低功耗 profile 或后台补偿，而不是默认正确性路径。

## 4. Tombstone 现状

当前 tombstone 是 docid 级删除掩码，不是 subtree tombstone。

代码证据：

- `src/index/base_index.rs:387` `pub struct BaseIndexData`。
- `src/index/base_index.rs:392` `pub tombstones: RoaringBitmap`。
- `src/index/base_index.rs:434` tombstone 内存统计来自 RoaringBitmap serialized size。
- `src/index/base_index.rs:545` resident query 会跳过 tombstone docid。
- `README.md:186` 文档说明冷层/base 命中已删除时会写入 tombstone 并屏蔽旧结果。

差距：

- 没有 `Tombstone::Subtree { root_path, generation, expires_at }`。
- 没有 prefix tombstone filter。
- 没有 tombstone TTL、compaction、subtree merge。

收益：

- 大规模 `rm -rf node_modules` 或删除图片目录时，可避免逐文件 tombstone 和逐文件 stat miss。
- 宽泛查询时先 prefix filter，可减少 query verify 风暴。

## 5. Dirty Manager / Repair Worker 现状

DirtyQueue 已经是统一补偿入口，但 repair 分片还不够严格。

代码证据：

- `README.md:190`：DirtyQueue 合并 inotify 冷层事件、查询 stale hit、路径形态 query miss、周期冷层扫描、启动修复和 overflow recovery。
- `src/event/sync.rs:70` `pub enum DirtyReason`。
- `src/event/sync.rs:82` `DirtyReason::default_priority` 定义优先级。
- `src/event/sync.rs:133` `pub struct DirtyQueue`。
- `src/event/sync.rs:213` `DirtyQueue::enqueue`。
- `src/event/sync.rs:242` `DirtyQueue::pop_ready`。
- `src/event/sync.rs:276` `DirtyQueue::retry`。
- `src/main.rs:1088` `fn spawn_dirty_queue_loop`。
- `src/index/tiered/sync.rs:472` `process_dirty_entry_with_project_markers_and_manifest_skip_dirs`。
- `src/index/tiered/sync.rs:579` `fast_sync`。
- `src/index/tiered/sync.rs:840` `scan_dirs_with_depth_and_project_markers_budgeted`。
- `src/index/tiered/sync.rs:904` budget_ms 检查开始。
- `src/index/tiered/sync.rs:906` 超过 budget_ms 时停止。
- `src/index/tiered/sync.rs:1017` `scan_dirs_periodic_cold_outcome_with_project_markers`。
- `src/index/tiered/sync.rs:1053` 周期冷扫使用 `10_000` entry 上限。

差距：

- 还不是 `scan_dir_slice(dir, max_entries=512, max_ms=20)` 这种游标式严格分片。
- `fast_sync` 会对 dirty dir 做 depth=1 对齐，虽然有 IO governor，但不是统一 repair budget。
- 查询线程不会同步 repair，这是符合设想的；但 dirty repair 的上限还可进一步硬化。

收益：

- 巨型目录下 repair 不再形成明显 IO 峰值。
- DirtyQueue backlog 可预测，便于 `/health` 和 `/watch-state` 做 SLA 解释。

## 6. Directory Fingerprint / Manifest 现状

项目已经有更强的 directory manifest，而不只是目录本身 mtime/ctime fingerprint。

代码证据：

- `README.md:233`：L2/L3 periodic cold scan 维护 directory manifest。
- `src/index/tiered/directory_manifest.rs:14` manifest 记录 `child_mtime_hash`。
- `src/index/tiered/directory_manifest.rs:34` summary 记录 child count、name hash、mtime hash 等。
- `src/index/tiered/sync.rs:1071` `directory_manifest_summary`。
- `src/index/tiered/sync.rs:1125` 遍历目录子项构建 manifest。
- `src/index/tiered/sync.rs:1157` `update_directory_manifests_for_dirs`。

差距：

- 当前 manifest 构建仍会枚举目录子项。
- 用户设想的低成本 dir-only fingerprint 可作为更轻量的一层，用于先判断是否需要进入 manifest/readdir。

收益：

- 在冷目录上先用 dir-only fingerprint 降低进入 readdir 的概率。
- 与现有 manifest 结合后，可以形成 “dir stat -> manifest summary -> slice repair” 三段式成本控制。

## 7. Temp Watch Pool / Tiered Watch 现状

已有 tiered watcher 和 ephemeral watch，方向与用户设想高度一致。

代码证据：

- `README.md:206`：Ephemeral Watch 是短窗口反复 dirty scope 下的临时 watcher 租约。
- `src/event/tiered_watch.rs:332` `pub struct EphemeralWatchConfig`。
- `src/event/tiered_watch.rs:395` `pub struct TieredWatchRuntime`。
- `src/config.rs:481` `TieredWatchConfig::default`。
- `src/config.rs:505` 默认 `ephemeral_watch_budget: 256`。
- `src/config.rs:506` 默认 `ephemeral_watch_ttl_secs: 600`。
- `src/config.rs:507` 默认 `ephemeral_idle_secs: 120`。
- `src/config.rs:508` 默认 `ephemeral_max_cost_per_root: 64`。
- `src/main.rs:1299` `spawn_tiered_scan_loop`。
- `src/main.rs:1346` `spawn_tiered_fast_scan_loop`。
- `src/main.rs:1432` `maybe_send_ephemeral_watch_command`。
- `src/event/stream.rs:75` `WatchCommand` 支持 Add/Remove/Replace 和 Ephemeral 变体。
- `src/event/stream.rs:367` `WatchCommand::Add` 真实调用 `watcher.watch`。
- `src/event/stream.rs:401` `WatchCommand::AddEphemeral` 真实调用 `watcher.watch`。
- `src/event/stream.rs:458` `WatchCommand::Remove` 真实调用 `watcher.unwatch`。
- `src/event/stream.rs:474` `WatchCommand::RemoveEphemeral` 真实调用 `watcher.unwatch`。

差距：

- `src/config.rs:680` 默认 `watch_enabled: true`。
- `src/config.rs:681` 默认 `watch_mode: WatchMode::Recursive`，不是 tiered。
- 当前 ephemeral 触发来源主要来自 dirty scan / project marker / fast scan，未接入 `/proc` 写 fd 采样。

收益：

- 默认迁移到 tiered/balanced 后，可降低 inotify watch 配额和启动期 watch 成本。
- 接入 proc sampler 后，可让临时 watch 更贴近真实写入现场。

## 8. Inotify / Event Pipeline 现状

事件管道已经有 bounded channel、priority create、overflow/Rescan 处理和动态 watch。

代码证据：

- `src/event/watcher.rs:43` `handle_notify_result`。
- `src/event/watcher.rs:52` notify `need_rescan()` 会记录 rescan signal。
- `src/event/watcher.rs:57` Create 事件进入 priority channel。
- `src/event/watcher.rs:61` channel 水位高时主动 sleep。
- `src/event/watcher.rs:68` 使用 bounded channel 阻塞发送而不是无限堆积。
- `src/event/watcher.rs:75` ENOSPC / inotify watch limit 会记录警告。
- `src/event/watcher.rs:224` `watch_roots_enhanced` 返回 watch 失败的 degraded roots。
- `src/event/stream.rs:257` `EventPipeline::start`。
- `src/event/stream.rs:751` `need_rescan()` 事件入 `DirtyScope::All` + `DirtyReason::OverflowRecovery`。
- `src/event/stream.rs:763` tiered runtime 记录事件路径。
- `src/event/stream.rs:767` 事件触发 dirty dirs 入队。

差距：

- 事件仍不能作为正确性来源。
- overflow 后依赖补偿队列，仍需更硬的 repair budget。

收益：

- 当前事件管道已经足够作为 P1/P2 能力底座。
- P0 应优先补 query verifier 和 tombstone，而不是重写事件管道。

## 9. Proc Sampler 现状

当前未发现 `/proc/<pid>/fd` / `/proc/<pid>/fdinfo` 写句柄采样器。

搜索证据：

- 全局搜索 `readlink|fdinfo|/proc/<pid>|/proc/.*/fd|write fd|writable fd|ProcSampler|proc_sampler` 在 `src/` 下无写句柄采样器命中。
- `src/fs_policy.rs:76` 读取 `/proc/self/mountinfo`，用途是 mount policy。
- `src/io_governor.rs:303` 读取 `/proc/pressure/io`，用途是 IO pressure。
- 现有 `/proc` 命中主要是 smaps/statm/pressure/mountinfo 诊断或历史记录，不是活跃写 fd 采样。

差距：

- 无 `ProcSampler` 模块。
- 无 `proc_scan_duration_ms / pids_seen / fdinfo_read_count / readlink_count` 等指标。
- 无 sampler -> temp watch lease 的连接。

收益：

- 对 ComfyUI、下载器、编译器、编辑器等“持续写入目录”可提高热路径新鲜度。
- 但它不是正确性来源，应排在 P0 之后。

## 10. Budget Manager 现状

当前已有 IO governor 与多处局部预算，但没有统一 Budget Manager。

代码证据：

- `src/io_governor.rs:10` `pub struct IoGovernorConfig`。
- `src/io_governor.rs:23` `IoGovernorConfig::default`。
- `src/io_governor.rs:26` 默认 `enabled: false`。
- `src/io_governor.rs:35` 默认 `stat_rate_per_sec = 50_000`。
- `src/io_governor.rs:153` `pub struct IoGovernor`。
- `src/io_governor.rs:217` `IoGovernor::before_io`。
- `src/index/tiered/sync.rs:638` fast_sync 过滤目录时调用 IO governor。
- `src/index/tiered/sync.rs:715` fast_sync metadata 前调用 IO governor。
- `src/index/tiered/sync.rs:787` 删除对齐 stat 前调用 IO governor。
- `src/index/tiered/lazy_validation.rs:208` lazy validation stat 前调用 IO governor。
- `src/config.rs:487` tiered scan 默认 `scan_items_per_sec: 5_000`。
- `src/config.rs:488` tiered scan 默认 `scan_ms_per_tick: 20`。
- `src/config.rs:497` fast scan stat budget per tick。
- `src/config.rs:498` fast scan readdir budget per tick。

差距：

- 没有统一优先级：query verify > inotify apply > proc sampler > repair > fingerprint > compaction。
- 没有按模块申请预算的 API。
- 默认 IO governor 关闭，与“所有后台动作都有硬预算”仍有差距。

收益：

- 可把资源策略从“局部 throttle”升级为“全局调度”。
- 能明确降级顺序：暂停 repair、降低 proc 频率、暂停 fingerprint、淘汰 low-priority temp watch、保留 query verify。

## 11. 网络盘 / NAS 现状

客户端网络盘实时性已经被保守处理，但 Server Mode 未实现。

代码证据：

- `README.md:200` roots 配置示例包含 `allow_remote = false`。
- `README.md:235` 网络/FUSE fast scan 默认 best-effort，不承诺 strict SLA。
- `README.md:241` 当前 runtime 不实现 RemoteAgent。
- `src/fs_policy.rs:109` `FsPolicyConfig::allow_remote`。
- `src/fs_policy.rs:127` 默认 `allow_remote: false`。
- `src/fs_policy.rs:146` `default_deny_fstypes`。
- `src/fs_policy.rs:148` 默认 deny 包括 `nfs`, `nfs4`, `cifs`, `smb3`, `fuse`, `fuseblk`。
- `src/fs_policy.rs:458` `!allow_remote && is_remote_fstype` 时拒绝。
- `src/fs_policy.rs:528` `is_remote_fstype` 判定 nfs/cifs/smb3/fuse.* 为 remote。
- `src/query/server.rs:354` 本机 HTTP server 暴露 `/search`。
- `src/query/server.rs:365` HTTP server 绑定 `127.0.0.1`。

差距：

- 没有 NAS-side daemon 协议。
- 没有 server scan checkpoint / pause / resume。
- 没有远程权限模型。
- 现有 HTTP API 是本机调试/查询接口，不是 Server Mode 架构。

收益：

- 对 100T NAS，Server Mode 是唯一合理方向。
- 但这应作为 P2 独立大版本，不能混入 Local Mode P0。

## 推荐实施顺序

本调研包本身不可执行。后续实施前，需要把下面几个方向拆成独立正式方案包；每个正式方案包必须重新核对当前代码行号、明确非目标、验收标准和回滚策略。

### 需要单独计划的正式方案包

#### 1. P0.5：rename 事件短线止痛

用途：优先缓解“刚下载完的文件搜不到”的体感问题。

计划时必须明确：

- 修改范围：`src/event/stream.rs:merge_events_in_place` 和同文件测试模块。
- 语义边界：只能把确认的孤立 `RenameMode::To` 当 Create；不能把所有单路径 rename 都当 Create。
- 合并策略：同批次 `Create` / `Rename` 不被后续 `Modify` 覆盖。
- 验收：补单元测试覆盖孤立 RenameTo、paired rename 后跟 Modify、Create 后跟 Modify。

#### 2. P0-1：query-time Top-K verify

用途：把“索引只是候选源”落实到查询返回语义，优先减少假结果。

计划时必须明确：

- 配置项：`query.max_verify_per_query`、`query.verify_timeout_ms`、`query.allow_sync_readdir=false`。
- 默认策略：默认关闭 lazy validation，还是只对 Top-K 做同步 verify、其余保持 lazy。
- 查询语义：验真失败的候选是过滤、返回 changed，还是进入异步修复后不返回。
- API 语义：HTTP/UDS 返回的 `validated`、`freshness`、timeout 状态如何表达。
- 验收：删除文件不返回、mtime/identity 变化不作为 confirmed result 返回、宽泛查询 verify 不超过预算。

#### 3. P0-2：runtime subtree tombstone

用途：先解决大目录删除后的查询风暴，第一版不强行碰 snapshot/compaction 持久化格式。

计划时必须明确：

- 第一版范围：只做运行时 subtree tombstone + prefix filter，还是同步设计持久化。
- 数据结构：`root_path`、`generation`、`expires_at`、内存上限、合并规则。
- 接入点：query 候选进入 Top-K verify 前先做 prefix filter。
- 产生条件：大量 verify miss、目录删除事件、repair 发现整棵子树消失时如何生成 subtree tombstone。
- 验收：删除大目录后宽泛查询 stat 次数显著下降，TTL 后可清理，不误伤重建后的同名目录。

#### 4. P0-3：sliced repair

用途：把 DirtyQueue / repair 对大目录的 IO 峰值变成可预测的短 slice。

计划时必须明确：

- cursor 策略：Rust `read_dir` 没有稳定可持久化 cursor，需选择内存 iterator、文件名 checkpoint 或重新扫描跳过策略。
- DirtyQueue 结构：是否扩展 `DirtyScope` / `DirtyQueueEntry` 携带 cursor、generation、slice progress。
- slice 上限：`max_entries`、`max_ms`、`max_stat_per_sec` 与现有 IoGovernor 的关系。
- retry 语义：未扫完、目录变化、权限失败、目录消失时如何重新入队。
- 验收：百万 entry 目录 repair 不出现长时间阻塞，backlog 和 slice 进度可在 health/metrics 中解释。

### 暂不优先计划的方向

- `/proc` 写 fd sampler：P1 热路径新鲜度优化，不能作为正确性来源。
- 统一 Budget Manager：P1 架构收口，需等 P0 verify / tombstone / repair 的资源模型更清楚后再统一。
- Server Mode：P2 独立大版本，不能混进 Local Mode P0。

### P0：正确性地基

- 新增 query-time Top-K verify 配置。
- 默认或 profile 支持返回前同步验真。
- 新增 subtree tombstone + prefix tombstone filter。
- 新增 query verify 统计和硬 timeout。
- repair 改成严格分片预算。

### P1：后台成本与热路径新鲜度

- 引入统一 Budget Manager。
- 评估默认 `watch_mode = "tiered"`。
- 增加 `/proc` 写 fd sampler。
- sampler 只续租 temp/ephemeral watch，不直接写索引。

### P2：Server Mode

- NAS-side daemon。
- 断点扫描 / pause / resume。
- remote search API。
- 单用户 trusted home NAS 权限模型。
