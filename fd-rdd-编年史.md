# fd-rdd 编年史（立项 -> 2026-05-04）

> 目的：把”为什么这么做、先后顺序、关键分歧与落地结果”按时间线写清楚，便于对外讨论。
>
> 本文档由 `helloagents/` 目录下的项目开发历史记录（task + why + how）汇总而成。

## 0. 立项（目标与约束）

立项目标是一句话：在 Linux 上做出接近 Everything 的使用体验——**常驻守护进程 + API 查询**，让“搜文件名”成为毫秒级交互。

同时，立项一开始就遇到三个现实约束：

1. **百万级文件规模**：任何 per-file 的堆分配/字符串 header/HashMap 桶开销都会被放大。
2. **notify 不可靠**：事件风暴下丢事件是常态，必须把“丢了怎么办”写进架构，而不是寄希望于 watcher 永不溢出。
3. **冷启动与常驻内存矛盾**：如果启动就把索引 hydration 成堆对象，冷启动和 RSS 会被索引体积“直接定价”；若要冷启动秒开，必须走 mmap/段式/按需分页。

因此路线从一开始就不是“把 fd 包起来”，而是要构建一套可持续演进的索引系统：

- 正确性闭环：overflow -> rebuild 兜底
- 可用性优先：重建期间查询不中断（永远有一个可用版本）
- 表示法优先：用 DocId/压缩 posting/连续 arena 把“索引体积”打到能接受的水平

## 1. 2026-02-14：v0.2.x 压测排障与路线纠偏

这一阶段的主题是：从“误区”回到“可用系统”。

- 误区确认：
  - “删了不降”经常不是泄漏，而是 allocator 高水位 + 容器 capacity 不收缩 + 历史结构累积。
  - “事件必达”是错觉：notify 在风暴下必然丢，必须设计补偿机制。
- 持久化策略纠偏：
  - 放弃依赖成熟度不足的 WAL crate，改为 **atomic snapshot**：tmp 写入 + fsync + rename 原子替换 + 目录 fsync。
  - 校验失败即回退/重建，保证“坏快照不拖垮服务”。
- 可靠性优先项：
  - 修死锁风险：统一读写锁加锁顺序，避免“边写边查”互锁。
  - overflow 兜底：记录 drops，并触发后台 rebuild（带 cooldown，避免风暴自激）。
- 内存与写入峰值：
  - posting 从 HashSet 过渡到更紧凑表示，避免桶/指针开销。
  - 快照写入从“先拼巨型 Vec”改为“流式写入”，降低峰值内存。

这一阶段产出：把系统从“能跑但会漂移/会卡死”推进到“可长期跑、有兜底闭环”。

## 2. 2026-02-15：收益最大化路线的分阶段落地

这一日的主题是：把“表示法 -> 持久化 -> LSM 演进 -> 动态运行”串成一条因果链。

### 2.1 阶段 A：内存表示法打地基（DocId + Roaring + PathArena）

- posting 元素从 `FileKey(dev,ino)` 转为 `DocId(u32)`，为压缩与段式布局建立前提。
- trigram posting 接入 `RoaringBitmap(DocId)`，查询做位图交集 + 精确 matcher。
- 路径从百万次堆分配的 PathBuf 收敛为 `PathArena` 连续 blob，主表收敛为 Vec 紧凑布局。

### 2.2 Step 1：动态止血（ArcSwap + mimalloc 可选）

- 生产语义 Bug 修复：overflow 触发 rebuild 时不再“原地 reset 导致查询不可用”，而是后台构建新索引后 **ArcSwap 原子替换**。
- 可选 mimalloc：用于对照 allocator 碎片/回吐行为，隔离“结构优化”与“分配器噪声”。

### 2.3 阶段 A+：事件风暴稳定性与可观测

- rebuild 冷却与合并（cooldown + coalesce），防止风暴频繁重扫自激。
- 内存报告拆项：把 Arena/HashMap capacity/Roaring data 透明化，避免“靠猜”。
- root 相对路径压缩：arena 存相对路径，元数据存 root_id，快照升级并校验 roots_hash。

### 2.4 阶段 B：v6 段式快照（mmap + lazy decode）

- 段式物理布局：Trigram/Metadata/Path/Postings 等独立段，支持独立校验与迁移。
- 启动加载优先 mmap：避免反序列化 hydration；posting 采用 lazy decode（按命中解压）。
- 校验策略优化：从“先 mmap 再校验”改为“read/seek 流式校验后再 mmap”，避免启动时无谓触页推高 Private_Clean RSS。

### 2.5 阶段 C：LSM（长期 mmap 基座 + 内存 Delta）与 Compaction

- 引入 `index.d/` 目录布局：`MANIFEST.bin` + `seg-*.db/.del`。
- 查询合并 newest -> oldest：用 blocked 集合实现覆盖语义与 delete->recreate。
- Flush：把内存 Delta 追加为新段；Compaction：阈值触发后台合并为新 base 并 best-effort 清理旧段。

### 2.6 动态侧“死结”拆解与止血（RSS/事件反馈回路）

压测中遇到“删完索引变小但 RSS 粘住”的现象，进一步用 `smaps_rollup` 拆分出两类来源：

- Anonymous/Private_Dirty：堆高水位/allocator 行为（可通过 rebuild 后手动 trim 缓解）
- Private_Clean：file-backed（mmap 段被触页后的常驻下界，根因是历史段数量/体积）

对应落地：

- rebuild/full_build 结束触发一次手动 trim（glibc/mimalloc 分别处理）。
- watcher 默认忽略 snapshot/index.d 路径，避免“索引写入反哺 watcher”的反馈回路；并提供 `--ignore-path` 扩展忽略项。
- 补齐“影子内存”统计：overlay/pending 纳入 MemoryReport；pending_events 按路径去重避免 rebuild 期间堆积。

### 2.7 神经重连（AQE 动态调度接回）

- `AdaptiveScheduler` 实际接入 rebuild/full_build：根据系统负载选择并行度。
- 扫描层落地为 ignore parallel walker（可控 threads），降低百万级扫描对系统交互的冲击。

### 2.8 记忆连续（events.wal）

- 引入 `events.wal` 追加型日志：事件批次 apply 前 best-effort 追加写入。
- snapshot flush 边界执行 seal（`events.wal.seal-*`），并把 `wal_seal_id` checkpoint 写入 manifest。
- 启动加载 segments 后按 checkpoint 回放 WAL，实现“最后一次 snapshot 之后”的增量就绪。

## 3. 截至 2026-02-15 的系统形态（简表）

- 查询链路：L1 -> L2(内存 Delta) -> Disk segments(mmap, newest->oldest)
- 一致性闭环：notify overflow -> 后台 rebuild；启动恢复：segments + WAL replay
- 持久化：v6 段式容器 + LSM 目录布局 + WAL checkpoint

## 4. 未完成/延后项（下一步讨论焦点）

- 段级过滤（Bloom/bitset）：减少无效段触碰，降低 page fault 与 CPU
- 更工业化 compaction（leveled/代际）：平滑写放大与合并抖动
- 更强 WAL 语义：fsync 策略、序列号与去重、gap verify、以及与 watcher 的边界定义

## 5. 2026-02-16：段物理回收闭环 + THP(always) 下的“空壳 RSS”治理

### 5.1 Compaction 后的物理段回收（GC stale segments）

观察到的现象：即使 compaction 产生了新的 base，旧的 `seg-*.db/.del` 若仍留在磁盘上，会在后续重启/运维中造成“历史垃圾段”累积，占用不必要的磁盘与潜在的加载/校验成本。

落地策略：

- 在 compaction/replace-base 成功更新 `MANIFEST.bin` 后，扫描 `index.d/`，删除 manifest 未引用的旧段文件（best-effort）。
- 单个文件删除失败不应阻断 compaction 主流程：只记录告警并继续，避免“清理失败 = compaction 失败”的级联风险。

这一步把“LSM 合并”从逻辑层推进到物理层闭环：manifest 是 SSOT，磁盘上只保留 live 段。

### 5.2 THP(always) 与 mimalloc：为什么空索引也能吃 100MB

在 `THP=always` 的系统配置下，发现即使启动为“空索引（no-snapshot/no-watch/no-build）”，进程 RSS 仍可能达到 100MB 量级。进一步拆分 `smaps_rollup` 后发现 RSS 主要由匿名大页（THP）贡献：

- mimalloc 的 segment/arena 可能以大虚拟区间（例如 1GB）管理；
- THP(always) 会以 2MB 粒度 commit；
- 只要触碰到少量页，也会把整张 2MB 大页计入 RSS（例如 50 个大页 ~= 100MB）。

治理策略（更可靠的方案）：

- 通过 mimalloc 的编译期开关禁用 THP 提示路径（`no_thp` -> `MI_NO_THP=1`），避免依赖“进入 main() 后再 set_var”的时序。

### 5.3 启动参数语义澄清（避免误判）

- `--no-build` 只禁用“空索引时的后台全量扫描”，不影响快照/LSM 段的加载。
- `--no-snapshot` 才会跳过快照加载；若不想 watcher 注入实时事件，还需 `--no-watch`。

### 5.4 冷启动离线变更检测：目录 mtime crawl → stale 则跳过 disk segments 并重建

在“百万文件时代写入的大段仍在磁盘，但实际上文件已离线删除”的场景下，仅靠“段格式校验 + roots 匹配”会把旧段挂载进查询链路：

- 查询触页（mmap）会把历史大段从磁盘读入物理内存，表现为 RSS 突发暴涨（而内存索引结构统计仍很小）。
- 旧段中的幽灵记录还可能返回脏结果（已不存在的路径）。

核心事实：inotify/fanotify 只能覆盖“在线”事件，停机期间的变更没有内核级事件账本可回放，必须在冷启动阶段主动检测。

落地兜底（Level 1，保守但正确）：

- 在 manifest 中记录 `last_build_ts`（实现上为 `last_build_ns`）。
- 冷启动加载 LSM 段之前，递归遍历 roots 下的目录树，仅对目录做 `stat`（不对文件做 `stat`），并在发现任意目录 `mtime > last_build_ns` 时 early-exit。
- 一旦判 stale：不把旧 disk segments 挂载进查询链路（避免触页与脏结果），并触发后台 full rebuild 重建一致性。

## 6. 2026-02-17：v0.4.0 语义锚定（身份合并）与查询/存储架构补强

这一阶段的主题是：把“文件事件”从路径视角升级为身份视角，并用更强的层契约把后续演进钉死在可维护的轨道上。

- 语义锚定：引入 `FileIdentifier`（路径/文件身份双来源），rename 升级为“双身份”语义，事件合并从“按路径覆盖”升级为“按身份归并 + path_hint”。
- WAL 协议升级：WAL v2 以“非破坏性升级”方式落地（v1 seal 归档，回放端同时支持 v1/v2），避免线上升级把历史数据变成不可恢复状态。
- 层契约明确：抽象出 `IndexLayer`（query_keys/get_meta）并写清契约，为后续 mmap layer、MergedView 的可替换性铺路。

影响与结果：

- rename/覆盖等现实文件系统语义不再被“路径最后写入者”误导，漂移窗口显著收敛。
- 后续“多层索引合并”的复杂度被强制约束在明确的层边界内，不再靠散落的约定维持一致性。

## 7. 2026-02-18：MergedView / Zero-copy 演进与测试锚定

这一阶段的主题是：把“多段、多层、多版本”的现实，收敛成一个稳定可解释的查询视图，并把关键语义用测试钉死。

- MergedView：v6 快照增加 FileKeyMap 段；PersistentIndex/MmapIndex 都实现 `IndexLayer`；TieredIndex 查询改为“FileKey newest→oldest 去重 + path 屏蔽集合”，对 rename-from tombstone、同路径替换写等场景更贴近用户预期。
- Zero-copy Evolution：FileKeyMap 段加入 magic/header 与 rkyv gate，兼容 legacy 与 rkyv 双格式；rkyv 校验用 OnceLock 缓存，避免热路径重复线性校验。
- 预过滤与回归：trigram 预过滤扩展到路径组件并引入哨兵能力探测，对旧段安全降级；新增级联查询覆盖语义回归测试，保证“新段覆盖旧段”的行为长期不退化。
- LSM Hygiene：校验升级为 CRC32C 并兼容旧段；compaction/replace-base 走 live-only 重写，推动 tombstone 物理回收从“想法”变成“默认路径”。

小结：到 v0.4.0 这一轮，系统从“可用”进一步走到“可演进”，关键语义有了结构化表达与可自动验证的锚点。

## 8. 2026-03：从“可用”到“可治理”（内存观测、压测脚本与链路补齐）

这一月的主题是：把难以讨论的现象变成可观测、可复现、可裁决的指标，并把 daemon/client 的链路从“能跑”补齐为“可长期用”。

- 内存治理：围绕 RSS/Private_Dirty 的“高水位不回吐”问题，补齐 MemoryReport 拆项与归因口径（index_estimated/non-index PD/heap 高水位信号、disk tombstones 等），让“是否增长”从主观印象变成数据问题。
- 压测与验收脚本化：fs-churn 增加 soak/plateau/verdict 等脚本与归因摘要，形成一键 PASS/FAIL 的最小验收闭环，并把 warmup/settle 等细节固化为可复现流程。
- 守护进程链路：落地 UDS 流式查询与 fast-sync 语义，逐步把 overflow 的补偿从“必然全量 rebuild”迁移到“优先增量修复（必要时再重建）”。
- 写入节奏治理：为周期性 flush 增加最小事件数/字节数门槛，小批次变更继续保留在 WAL/L2，避免段文件在低频波动下被碎片化增长。
- 查询语义升级：引入 Query DSL（AND/OR/NOT/短语、doc/pic/video、ext/dm/size、wfn/regex + Smart-Case），让“写代码找文件”的表达力从单字符串跃迁到可组合语义。
- 多索引源与隐藏项：明确多 `--root` 的使用方式，并提供 `--include-hidden` 作为显式开关，在保持默认行为的同时允许纳入 dotfiles/dotdirs。

## 9. 2026-04：查询与安全收口（验证器补强、fuzzy 接入、排序重构、模块拆分）

这一月的主题是：对外体验与安全边界同步收口，同时把核心模块拆开，为后续继续迭代留出工程空间。

- 可靠性补强：DAG 规划器与 verifier 从“占位/空壳”升级为可用实现（拓扑、并行层、缺口检测），并接入 overflow/recovery 链路，减少隐性漂移。
- fuzzy 查询接入：`FzfIntegration` 同时接入 HTTP `/search`、UDS 协议与 `fd-rdd-query` 客户端，新增显式 `mode=fuzzy`，并把 fuzzy 分数与 rank 分数做综合排序。
- 性能与安全修复：提升 event channel 默认容量应对批量变更；dirty flag 改为 AtomicBool 消除竞态；PersistentIndex 查询增加 limit 防止无界结果；trigram 交集优化降低持锁成本；DocId 溢出改为显式失败而非静默截断。
- 对外接口增强：补齐更多过滤器与排序/高亮等查询体验要素，并提供即时扫描 API 以支持“对指定目录立刻索引”的交互需求。
- 安全收口（2026-04-12）：HTTP 默认监听改为 `127.0.0.1`；`--root` 改为必传并移除默认遍历 `$HOME`；`fd-rdd-query --spawn` 透传 root，避免 daemon 无 root 误扫。
- 评分体系重构（2026-04-12）：从“深度主导”升级为多维启发式（match quality × basename 乘子 + 边界感知 + Smart dot-file + node_modules 动态隔离，深度降级为 tiebreaker），解决深层项目中“浅层无关结果抢榜”的常见失败。
- 工程结构：将超大单文件 `tiered.rs` 拆分为 13 个子模块，职责分离，降低后续改动成本与回归风险。

## 10. 2026-04-17：代码质量清理

无功能变更，纯工程债务清理：

- 大文件拆分：`snapshot.rs`（1867 行）、`dsl.rs`（1262 行）按职责分割。
- DRY 去重：`EventPipeline` 3 个构造函数合并为 `new_with_config_and_ignores`。
- 死代码消除：`DAGScheduler` 移除、未使用的 Filter 变体删除、`#[allow(dead_code)]` 消除。
- 跨平台兼容：`local_date_range` 补 non-Unix 支持、4K 页大小硬编码改为动态查询。
- 测试工具统一：`unique_tmp_dir` 在 5+ 文件中重复定义 → 统一到 `tests/common/`。

## 11. 2026-04-23：v0.6.0 核心重构

v0.5.8 积累的 P0 问题促成了这一轮大重构：

- **snapshot 可见性窗口**：`snapshot_now` 中先 swap L2 再写 disk_layers，中间态查询漏数据。修复：在 `apply_gate.write()` 锁内先序列化再 swap。
- **compaction 过频**：2 delta / 30s 冷却导致百万级文件场景下 CPU/RAM 尖峰。上调为 8 delta / 4 max / 300s 冷却。
- **event channel 溢出**：4096 默认值在 git clone/npm install 下静默丢事件。提升到 262144，并增加 >80% 水位 sleep 背压。
- **fast-sync 太慢**：5s 冷却 + 30s 最大延迟 → 感知上等很久。压缩为 1s 冷却 + 5s 最大延迟。
- **跨批次 rename 丢失**：PendingMoveMap 撮合 From/To 成对事件，解决 rename 后文件消失。
- **inode 复用幽灵文件**：引入 `FS_IOC_GETVERSION` 获取 `i_generation`，彻底识别 inode 复用。
- **query overlay 不可见**：overlay upserted_paths 合并进查询计划，新文件实时可见。
- Unicode NFC 规范化：全路径统一 NFC，消除编码陷阱。
- CI 压力测试：系统化覆盖 overlay 可见性、rename 雪崩、并发中间态、mmap 安全、trigram 倾斜。

## 12. 2026-04-24~25：v0.6.0 后修与 v0.6.1 发布

- musl 构建修复：`reqwest` 改用 `rustls-tls`，CI 增加 `musl-tools`。
- 中文高亮 panic 修复：`abs_pos + 1` → `abs_pos + matched_len`。
- `snapshot_now` 改为 `spawn_blocking` 避免阻塞异步运行时。
- `apply_gate` 写饥饿：`.write()` → `.try_write()`。
- 离线变更恢复：WAL 回放自动识别 crash 窗口期变更。
- v0.6.1 发布：补自动配置保存（首次启动 `--root` 后 persist 到 `config.toml`）、fmt/clippy 全通过。

## 13. 2026-04-25：ENOSPC + Hybrid Crawler + v0.6.2~v0.6.3

- ENOSPC 主动捕获：notify error code 28 不再被静默吞掉。
- Hybrid Crawler：degraded root 的后台周期性修复任务（60s fast-sync + 30s 调和循环），DFS 遍历对比 mtime。
- v0.6.2：修复 6 个查询正确性 bug——空 trigram bitmap 短路阻断回退、upsert 竞态窗口、pending_events apply 顺序、query_limit 跳过 pending、`file_count()` 快照期间不一致。
- v0.6.3 内存优化：`BTreeMap` → `HashMap`（省 ~48MB）、`short_component_index` 键从 `Box<[u8]>` 改为栈上 `u16`（省 ~3MB）、FAST_COMPACTION 默认启用、L1 `path_index` O(1) 快速路径、新目录动态监控修复。

## 14. 2026-05-01~02：v0.6.4 到 v0.6.14 —— 收尾稳定化

这是最密集的一轮连续迭代，主题是：把 v0.6.0 打开的所有分支收口到一个可长期运行的稳定基线。

- **Phase 0/1（v0.6.4）**：benchmark 框架 + 死代码清理（-578 行），移除 compaction 旧路径、RSS trim loop、deprecated CLI 参数。
- **Phase 2/3/4（v0.6.5~v0.6.7）**：ParentIndex 引入 → 默认启用，`parent:`/`infolder:` 查询从 O(N) 全扫降为 O(D) 脏目录扫描；DeltaBuffer 统一替代 overlay_state + pending_events。
- **Phase 5/6（v0.6.8~v0.6.9）**：DeltaBuffer 默认启用，移除双轨增量维护；ParentIndex 接入查询路径预过滤。
- **Phase 7/8（v0.6.10~v0.6.11）**：ParentIndex rebuild 正确性修复；DeltaBuffer 硬容量上限（256K）防 OOM；Hybrid Crawler 清理。
- **v0.6.12**：收尾——恢复 tests 分支编译、删除 `disk_layers` 热路径状态、v7 snapshot 启动路径收敛、v6 mmap 不再带回查询热路径、删除目录 rename 事件中的同步深度扫描、WAL 回放修复。
- **v0.6.13**：PersistentIndex 运行时主存储从 `CompactMeta + PathArena` 迁移到 `FileEntry + Vec<Vec<u8>>` 绝对路径字节表；`PathTableV2 + FileEntryIndex` 仅在 BaseIndexData / v7 快照导出时构建。
- **v0.6.14**：Base 内存压缩（运行时不再保留第二份 FileEntryIndex）；v7 加载高水位优化（直接保存压缩 PathTableV2）；断电恢复基础设施（stable.v7 轮转、runtime-state.json、WAL 截断尾统计）；RSS 回吐补齐（heap high-water 主动 trim + `/trim` 端点）；ParentIndex 轻量化（`Vec<u32>` 替代 `RoaringBitmap`）；Watcher 可关闭（`--no-watch`）；Tiered watcher 预览（预算受控热点监听）；索引入口硬排除（默认跳过 `.git`、`node_modules` 等）；小批 snapshot 保护（周期 snapshot 增加最小事件门槛）。

## 15. 2026-05-03：v0.6.15 —— 断电恢复 + Tiered Watcher 闭环

当前版本。主题：把 v0.6.14 的"预览"推进到"CI 覆盖可验证"的完成态。

- **稳定快照恢复**：`stable.v7` / `stable.prev.v7` / `stable.next.v7` 三文件轮转协议 + fsync + 加载验证；`runtime-state.json` 记录 clean shutdown 标记；`repair-meta.json` 记录 repair scan 元数据；startup recovery 决策链：snapshot → WAL replay → repair scan → rebuild fallback。
- **Tiered watcher 预算闭环**：`Create(Folder)` / rename-in 产生的新目录不再绕过 `max_watch_dirs` 直接注册 watcher，而是先进入 TieredWatchRuntime 按递归目录估算 cost 申请 L0；watch 注册失败释放预留预算；父级 L0 降级同步释放动态子目录状态。
- **可观测性补齐**：`/watch-state` 增加 `promotion_budget_blocked` 与 `watch_budget_utilization_pct`；`/health` 增加 L1/L2/L3、watch 预算使用率和预算阻塞 issue；`GET /metrics` 接入真实运行态数据。
- **CI 覆盖扩展**：新增 `p1_poweroff_resume` 测试覆盖 SIGTERM final snapshot + clean shutdown 标记 + 重启增量可见；CI `Poweroff recovery regression` job 显式执行四组恢复测试；Stress CI 降噪（轮询替代固定 sleep）。
- **仓库清理**：`helloagents` 从 git 跟踪中移除，仅保留本地工作知识库。

## 16. 截至 2026-05-04 的系统形态

- **查询链路**：L1 cache → DeltaBuffer（按路径去重）→ BaseIndexData（mmap PathTableV2 + Trigram + ParentIndex）
- **一致性闭环**：notify watcher → bounded channel + debounce → 事件应用；溢出 → fast-sync 增量修复；必要时 → rebuild 兜底
- **持久化**：v7 stable snapshot（CRC32C + 轮转） + events.wal + runtime-state.json
- **Watcher**：tiered 模式——L0 预算受控热点监听 + L1 有界 warm scan + 动态升降级
- **内存模型**：Base mmap（OS 按需触页）+ DeltaBuffer（硬容量上限）+ heap high-water 主动 trim
- **对外接口**：HTTP `/search` + UDS 流式协议 + `/scan` + `/health` + `/metrics` + `/memory` + `/watch-state` + `/trim`

## 17. 仍待推进的方向

- **段级过滤（Bloom/bitset）**：减少无效段触页，降低 page fault 与 CPU
- **更工业化 compaction**：leveled/代际策略平滑写放大与合并抖动
- **更强 WAL 语义**：fsync 策略、序列号去重、gap verify、与 watcher 边界精确定义
- **Benchmark 持续追踪**：当前 BENCHMARK.md 数据多为 TBD，需要 CI 自动化采集
- **多平台支持**：Linux 为主，macOS 实验性，尚无 Windows 计划
