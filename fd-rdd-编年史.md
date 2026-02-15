# fd-rdd 编年史（立项 -> 2026-02-15）

> 目的：把“为什么这么做、先后顺序、关键分歧与落地结果”按时间线写清楚，便于对外讨论。

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

