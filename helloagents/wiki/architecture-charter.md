# fd-rdd 立项架构书（Charter）

> 版本：v0.3.0+（与当前实现对齐）  
> 用途：对外解释“系统是什么、为何这样设计、核心不变量与演进路线”。

## 1. 问题定义

在 Linux 上提供接近 Everything 的体验：常驻守护进程 + API 查询，让“搜文件名/路径”成为毫秒级交互。

核心挑战：

- 规模：百万级文件会把任何 per-file 的指针/桶/堆分配放大成 RSS 灾难。
- 动态：文件系统持续变化，且 watcher 在风暴下会丢事件，不能把一致性建立在“事件必达”假设上。
- 冷启动：索引越大，若启动需要反序列化 hydration，启动耗时与 RSS 会被数据体积直接定价。

## 2. 目标与非目标

目标（必须满足）：

- 在线可用性优先：重建/合并期间查询不中断（允许短暂陈旧，但不允许“索引清空不可用”）。
- 一致性可恢复：overflow/异常后可自动回到一致状态（兜底全量重建 + 增量回放）。
- 冷启动快：优先 mmap/按需分页，避免“加载=反序列化堆对象”的秒级开销。
- 内存可解释：能拆分出结构体开销、file-backed 常驻、以及“影子内存”的来源。

非目标（暂不做或延后）：

- 完整的内容索引/全文检索（当前聚焦文件名/路径匹配）。
- 跨机器分布式一致性（单机守护进程优先）。
- watcher 的“绝对不丢事件”（做不到；以补偿闭环替代）。

## 3. 架构总览（Tiered Index）

```
          Query
           |
        +--v--+
        |  L1 |  热缓存（命中即返）
        +--+--+
           |
        +--v--+
        |  L2 |  内存 Delta（可变索引）
        +--+--+
           |
   newest  |  oldest
        +--v----------------------+
        |  Disk Segments (mmap)   |  base + deltas（LSM 目录）
        +-------------------------+
```

关键点：

- **L2 只承载“最近变更”**，长期基座落在 mmap 只读段，避免全量 hydration。
- **查询合并语义 newest -> oldest**：保证覆盖与 delete->recreate 的正确性。
- **重建/切换通过 ArcSwap**：后台构建新版本，一次性换指针，读不中断。

## 4. 数据表示（为什么能省内存）

### 4.1 DocId + RoaringBitmap

- posting 元素统一为 `DocId(u32)`，为压缩与段式布局建立前提。
- trigram -> posting 使用 `RoaringBitmap(DocId)`，查询阶段做位图交集得到候选集。

### 4.2 PathArena + 紧凑元数据

- 路径存入连续 `PathArena(Vec<u8>)`，元数据仅存 `(root_id, off, len)`。
- 主表以 Vec 紧凑布局（DocId 作为下标），避免 HashMap 桶开销主导 RSS。

## 5. 持久化（v6 段式 + LSM 目录 + WAL）

### 5.1 v6 段式容器（mmap-friendly）

- 单文件容器 `index.db` 兼容 v2~v6 读取；v6 为 segment descriptors + per-segment checksum。
- 加载时尽量避免无谓触页：先流式校验，再 mmap。

### 5.2 LSM 目录布局（index.d/）

- `MANIFEST.bin`：段列表与 checkpoint（原子替换写入）
- `seg-*.db`：只读段（复用 v6 容器）
- `seg-*.del`：跨段删除 sidecar（按路径 bytes）

### 5.3 events.wal（Append-only Log）

目的：把一致性恢复从“只能全盘 rebuild”推进到“可增量回放”。

- 事件批次在 apply 前追加写入 `index.d/events.wal`（崩溃尾部截断可容忍）
- snapshot 边界 `seal()`：切分为 `events.wal.seal-*`
- manifest 记录 `wal_seal_id` checkpoint：启动时只回放 seal_id > checkpoint 的 sealed WAL + 当前 WAL，避免重复回放

## 6. 事件处理与一致性闭环

- 正常路径：watcher -> debounce/merge ->（WAL append）-> apply 到 L2/overlay
- 风暴路径：channel overflow 记录 drops，并触发 rebuild（带 cooldown + 合并）
- rebuild 期间：pending_events 按路径去重（只保留最新事件），避免堆积

## 7. 可观测与运维要点

- MemoryReport：拆分 L2 capacity/roaring 体积、disk segments 数量、overlay/pending 影子内存。
- watcher 反馈回路：默认忽略 `snapshot_path` 与 `index.d/`；额外用 `--ignore-path` 排除日志等路径。

## 8. 演进路线（后续）

已完成：

- 阶段 A/A+：表示法压缩 + 动态可用性止血 + 可观测
- 阶段 B：v6 段式快照（mmap + lazy decode）
- 阶段 C：LSM（base + deltas）+ Flush/Compaction
- events.wal：增量回放账本

未完成（下一轮重点）：

- 段级过滤（Bloom/bitset）：减少无效段触碰与 page fault
- leveled compaction/代际：平滑写放大与合并抖动
- WAL 更强语义：fsync 策略/序列号/去重与 gap verify

