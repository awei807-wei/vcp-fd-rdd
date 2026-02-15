# Step 1 动态止血：ArcSwap 后台重建 + mimalloc（可选）

## 目标

- rebuild 期间查询不中断（不再“原地 reset 清空索引”）
- rebuild/事件并发下不丢事件（pending 回放）
- 动态内存碎片噪声可控（可选 mimalloc 作为全局分配器）

## 重建语义（高层）

1. 触发 rebuild（例如 watcher overflow）后，进入 `in_progress`
2. 后台线程构建 new L2（全量扫描灌入）
3. rebuild 期间发生的事件进入 pending 缓冲
4. 构建完成后回放 pending 到 new L2
5. 持锁判空后执行 `ArcSwap.store(new_l2)` 原子切换；同时清空 L1

> 说明：切换期间允许短暂“读到旧数据”，但不允许“索引被清空导致查询不可用”。

补充：为避免事件风暴触发频繁重建，`TieredIndex` 内部增加了 rebuild 冷却与请求合并（cooldown + coalesce），在冷却期内将多次 rebuild 请求合并为一次执行。

## mimalloc 开关

编译时启用 feature：

- `cargo build --release --features mimalloc`

用途：

- 在“动态更新/重建”压测中观察 RSS 回吐与碎片行为
- 隔离 allocator 噪声，避免把 RSS 高水位误判为数据结构问题（为 Step 2 mmap 段式持久化评估做准备）

## 手动“抽水”（RSS Trim）

在 **rebuild/full_build 完成**的瞬间，触发一次“手动回吐”，用于压制“高水位 RSS 常驻”：

- 启用 `mimalloc` feature：调用 `mi_collect(true)` 促使 mimalloc 回收空闲页
- 否则（Linux + glibc）：调用 `malloc_trim(0)` 尽可能把空闲块归还 OS

目的：当索引结构体已经缩小（L2 估算很小）但 `smaps_rollup` 的 `Anonymous/Private_Dirty` 仍很高时，提供一次明确的“战后清理”动作。

## 弹性计算（AdaptiveScheduler）

动机：百万级扫描/重建时，如果“无脑拉满并发”，会造成系统卡顿、抢占在线查询与 watcher 管线；而一刀切串行又会拖慢恢复速度。

做法：在 `TieredIndex` 内注入 `AdaptiveScheduler`，在 `spawn_full_build/spawn_rebuild` 启动后台构建前：

- `adjust_parallelism()`：基于 load average + 内存压力调整目标并行度
- `select_strategy(Task::ColdBuild { .. })`：选择 `Serial` 或 `Parallel { shards, .. }`

构建执行：`IndexBuilder::full_build_with_strategy` 将策略下推到扫描层，最终由 `FsScanRDD::for_each_meta`（基于 `ignore` 的 parallel walker）执行可控并行扫描。
