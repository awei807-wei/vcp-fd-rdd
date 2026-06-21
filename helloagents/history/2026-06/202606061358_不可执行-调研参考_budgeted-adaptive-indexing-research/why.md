# 预算约束自适应索引技术调研（未确认 / 不可执行）

## 状态门禁

- 方案状态：未确认。
- 执行权限：不可执行。
- 处理规则：本方案包只记录技术调研、代码证据和收益判断；不得被 `~exec` 直接执行。
- 解锁条件：需要用户明确确认实施范围、优先级和默认配置策略后，另建正式可执行方案包。

## 用户输入设想摘要

本轮用户给出的最优设想是放弃"Linux 版 Everything 完全复刻"，改为 **Budgeted Adaptive Indexing / 预算约束下的自适应索引**：

- 本机路径尽量实时，冷路径最终一致。
- 搜索结果必须验真。
- 网络盘 / NAS 不在客户端承诺实时，建议转为服务端索引。
- 后台动作必须有硬预算，宁愿降低新鲜度，也不能拖垮机器。
- Local Mode 使用 `proc` 发现活跃写目录、临时 `inotify` watch 盯热目录、`stat` 做查询验真、后台 repair 还债。
- Server Mode 面向 NAS / 大容量文件服务器，索引器跑在数据所在机器，客户端只做远程查询。

## 核心结论

基于当前代码和知识库，fd-rdd 已经接近"普通用户态 Linux 文件事件系统"的稳定能力边界：默认事件源仍是 `notify` / inotify，且项目已经承认 watcher 不可靠、网络 / FUSE best-effort、L3 最终一致。

关键证据：

- `README.md:13` 明确当前产品是启动扫描后通过 inotify 持续增量更新的 daemon。
- `README.md:190` 把 DirtyQueue 定义为冷层补偿统一入口，来源包括 inotify 冷层事件、查询 stale hit、query miss、周期冷层扫描、启动修复和 overflow recovery。
- `README.md:235` 明确 fast scan 对本地可信文件系统提供覆盏目标，对 NFS/SMB/FUSE/SSHFS/rclone 等 untrusted 路径只报告 best-effort。
- `README.md:241` 明确当前 runtime 不实现 RemoteAgent，也不会用 `statx(FORCE_SYNC)` 伪造网络 strict SLA。
- `README.md:243` 明确 L3 是最终一致层，不代表实时 watcher 覆盖。
- `helloagents/wiki/fanotify-prestudy.md:5` 明确 fanotify 暂不适合作为默认 watcher 后端。
- `helloagents/wiki/fanotify-prestudy.md:25` 规定 fanotify 若存在也只能 notification-only、opt-in、capability-gated，并失败回退 tiered watcher。
- `helloagents/wiki/fanotify-prestudy.md:37` 指出 FID-only 事件不能直接写 tombstone 或 live overlay。
- `helloagents/wiki/fanotify-prestudy.md:40` 指出 FAN_Q_OVERFLOW / FAN_FS_ERROR 必须进入 bounded reconciliation。
- `helloagents/wiki/fanotify-prestudy.md:83` 明确不对用户真实 home 全盘启用 fanotify 实验。
- `helloagents/wiki/fanotify-prestudy.md:91` 明确默认仍使用 notify tiered watcher。

因此，本项目下一阶段最大收益不是追逐更深事件源，而是把现有边界上的正确性与预算闭环做硬：返回前验真、subtree tombstone、统一预算、分片 repair、默认 balanced/tiered 策略、再视情况加入 `/proc` 写句柄采样器。

## 当前满足度总览

| 能力 | 当前状态 | 结论 |
|---|---|---|
| 索引核心 | 已保存 `dev/ino/generation/mtime/kind/path_idx`，但 base meta 缺 `size/ctime` | 半满足 |
| 查询验真 | 同步验真函数存在，但默认 lazy validation 异步返回 | 半满足 |
| Tombstone | 已有 docid 级 RoaringBitmap tombstone | 不满足 subtree / TTL 目标 |
| Dirty / Repair | DirtyQueue、fast-sync、周期 scan、directory manifest 已有 | 半满足分片 repair |
| Temp Watch Pool | Tiered watcher + Ephemeral Watch 已有 | 基本满足，但默认非 tiered |
| Proc Sampler | 未发现 `/proc/<pid>/fdinfo` 写句柄采样器 | 未满足 |
| Budget Manager | 已有 IoGovernor 与局部预算 | 未满足统一预算总控 |
| 网络盘降级 | 默认 remote/FUSE 保守拒绝或 best-effort | 基本满足 |
| Server Mode | 只有本机 HTTP/UDS 查询 API，无 NAS RemoteAgent | 未满足 |

## 破局之法：P0 正确性闭环

系统性卡点不是继续追逐更深的文件事件源，也不是单个 rename bug，而是当前框架还不能兑现三个可对用户承诺的 SLA：

1. **查询必须验真**：返回结果不能只是索引候选，Top-K 候选必须在返回前做有上限的 `stat` verify。
2. **后台必须硬预算**：DirtyQueue、repair、fingerprint、watch add、validation 都不能在大目录或冷路径上形成无上限 IO。
3. **大目录删除不能风暴**：删除 `node_modules/`、图片库、构建目录时，不能退化成十万级逐文件 tombstone 或逐文件 stat miss。

代码已经建立了 `inotify + DirtyQueue + lazy validation + tiered watch + directory manifest` 的框架；真正的 P0 是把这些能力收口成正确性闭环：

### P0-1：查询返回前 Top-K 验真

当前 `lazy_validation_enabled: true` 允许查询返回 `validated=false` 的冷结果，这是“索引候选被当成事实返回”的直接来源。

破局点：

- 新增 `query.max_verify_per_query`，默认建议 100-200。
- 新增 `query.verify_timeout_ms`，默认建议 50-100ms。
- 默认关闭 lazy validation，或只对 Top-K 候选同步验真。
- 验真失败（文件不存在、mtime/identity 变化）直接过滤，不作为 confirmed result 返回。
- 查询线程禁止同步 `readdir`，只允许 Top-K `stat`。

目标口径从“索引可能过期”改成“返回结果是真的，但可能不全”。

### P0-2：Subtree Tombstone

当前 `tombstones: RoaringBitmap` 只能逐 docid 标记删除。用户删除大目录后，宽泛查询仍可能触发大量逐文件 miss。

破局点：

- 新增 `Tombstone::Subtree { root_path, generation, expires_at }`。
- 查询时先做 prefix tombstone filter，整批过滤被 subtree tombstone 覆盖的候选。
- TTL 后进入 base compaction 或清理。

目标是把“十万次 stat miss”降成“一次 prefix check”。

### P0-3：Repair 严格分片

当前 DirtyQueue 和 fast_sync 已经存在，但对巨型 dirty dir 仍需要更硬的切片边界。

破局点：

- 改成 `scan_dir_slice(dir, cursor, max_entries=512, max_ms=20)` 游标式分片。
- 未完成的 slice 重新入 DirtyQueue，携带 cursor 续扫。
- 让 DirtyQueue backlog 可预测，并能通过 `/health` 解释。

目标是把百万文件目录 repair 从“一次性扫完”改成“多次短 slice 慢慢还债”。

## P0.5 短线止痛：rename 事件修复

用户当前最大体感痛点是 **刚下载完的文件搜不到**。这类问题常见于下载器写入 `.crdownload` / `.part` 后 `rename()` 到最终文件名；inotify 会把 rename 拆成 `RenameFrom` + `RenameTo`，需要事件管道正确保留结构性语义。

当前代码证据支持两个可修复点：

1. **单路径 RenameTo 没有被保留为结构性事件**（`src/event/stream.rs:merge_events_in_place`）：当 `RenameFrom` 未进入管道或跨批次配对失败时，单路径 `Modify(Name(To))` 会走普通事件分支并转换为 `EventType::Modify`。

2. **Rename / Create 可能被后续 Modify 覆盖**（`src/event/stream.rs:merge_events_in_place`）：同批次内，rename 配对成功后同路径又有 Modify 事件，当前 merge 策略是“后到覆盖先到”，结构性语义可能丢失。

修复方向：

- 只把确认的孤立 `RenameMode::To` 当 `EventType::Create` 处理；`RenameMode::From` 不能误当 Create。
- 同批次内 `Create` / `Rename` 不被后续 `Modify` 覆盖，结构性变化优先级高于内容变化。

这属于 P0.5 快速止痛，不替代 P0 正确性闭环。它应通过独立可执行方案包处理：

**已建立可执行方案包**：`helloagents/plan/202606061413_待执行_fix-rename-event-missing-new-file/`（2026-06-11 已按本包要求修订口径：只把孤立 `RenameMode::To` 当 Create）

## 收益判断

### P0 最值得做（主破局）

- Query-time Top-K verify：把"索引只是候选源"落实为默认 SLA，直接降低假结果风险。
- Subtree tombstone：避免大目录删除后产生大量 file tombstone / stat miss 风暴。
- 查询 IO 硬上限：确保宽泛查询不会触发无上限 `stat`、`readdir` 或 repair。
- Repair 分片预算：让 DirtyQueue 对巨型目录更可控。

### P0.5 最紧急（已有方案包）

- **修复 rename 事件导致新文件搜不到**：直接缓解用户当前最高频体感痛点，修改集中，风险低；但它是短线止痛，不是系统性正确性地基。

### P1 值得做但不应先于 P0

- 默认 `watch_mode = "tiered"` 的迁移评估。
- `/proc` 写句柄采样器，只作为 hot-path freshness 线索源，不作为正确性来源。
- 统一 Budget Manager，把 query verify、repair、watch add、fast scan、tombstone compaction 统一纳入优先级。

### P2 是独立大版本

- Server Mode / NAS index daemon / remote query API。
- 需要断点扫描、远程权限模型、超大规模索引容量与内存上限验证。

## 不确定因素

⚠️ 不确定因素：默认查询应该"同步验真后返回"还是"保持 lazy validation 低延迟返回"需要产品 SLA 决策。

- 假设：用户更重视不返回假结果。
- 保守决策：P0 方案应优先设计返回前 Top-K verify，并把 lazy validation 改为可选降级策略。
- 备选：保留 lazy validation 默认，但 `/search?verified=true` 或配置 profile 强制同步验真。

⚠️ 不确定因素：默认 watcher 是否从 recursive 切到 tiered。

- 假设：低资源和预算约束优先于全盘实时口径。
- 保守决策：先做配置迁移评估和真实数据集对照，不在本未确认方案包中直接改变默认值。
