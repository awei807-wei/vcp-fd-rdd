# fd-rdd 可靠性审计记录：死锁与事件溢出兜底

本文记录 fd-rdd v0.2 在“百万文件压测 + 并发查询 + 事件风暴”场景下的两个关键可靠性问题与对应修复策略。

## 1. 读写锁死锁风险（L2 查询 vs 写入）

### 现象

当后台构建/事件写入与前台查询并发进行时，如果 L2 内部多个 `RwLock` 的加锁顺序在读路径与写路径不一致，可能出现卡死：

- 查询线程持有 `files.read()`，随后尝试 `trigram_index.read()`
- 写入线程持有 `trigram_index.write()`，随后尝试 `files.write()`

两者互相等待即死锁。

### 处理原则

- **统一加锁顺序**：让查询路径也遵循与写入路径一致的顺序
- **避免跨锁持有**：候选集计算阶段先读取 `trigram_index`，释放后再读取 `files/tombstones`

## 2. 事件队列溢出导致索引漂移（notify 不可靠）

### 现象

notify 事件在高频变更下会出现 channel 满导致丢弃（overflow）。当事件丢弃发生后：

- 索引的增量更新失去完整性
- 长跑后会出现“索引与真实文件系统不一致”（漂移）

### 处理原则

- 允许丢弃（背压优先，避免 OOM），但必须记录 `overflow_drops`
- **一旦发生 overflow**，触发“兜底重建”以恢复一致性：
  - **不再原地清空 L2**：后台构建 *新* 索引（new L2），构建完成后 **ArcSwap 原子切换**
  - rebuild 期间的事件先进入 pending 缓冲，切换前回放到 new L2，避免丢事件
  - 切换时清空 L1（避免返回过期缓存）
  - 为避免风暴下频繁重建，添加最小重建冷却时间（cooldown），并在冷却期内合并 rebuild 请求（merge/coalesce）

### 2.1 “自触发”事件风暴（索引写入反哺 watcher）

如果 watch roots 覆盖了 fd-rdd 自己写入的路径（例如 snapshot 的 `index.db/index.d`，或将日志重定向到被 watch 的目录内），会形成反馈回路：

- fd-rdd 写 snapshot/segment/log
- watcher 捕获 Modify/Create
- 事件管道合并并 apply → 继续写日志/快照

表现：

- `Event Pipeline.total events` 快速增长
- `overflow` 频繁出现，进而触发 rebuild
- RSS/Anonymous 高水位长期不回落（本质是“事件风暴 + 临时分配”的高水位常驻）

缓解（已落地）：

- 事件管道默认忽略 `snapshot_path` 与派生 `index.d/`，避免索引自身写入反哺 watcher
- CLI 支持 `--ignore-path`（可重复）手动排除日志文件等路径

## 3. 快照写入的内存峰值（OOM 风险）

### 现象

快照写入如果把序列化 body 再复制拼成一个巨型 buffer，会造成明显的瞬时内存峰值，甚至在大索引下 OOM。

### 处理原则

- v5（bincode）采用流式序列化写入（`bincode::serialize_into(file)`），避免先构建超大 `Vec<u8>` body
- v6（段式）写入为 manifest + segments（按 8B 对齐），并为每段计算 checksum；仍保留 INCOMPLETE → COMMITTED 两阶段 header
- 仍保留 atomic replacement（tmp + fsync + rename + fsync(dir)）语义

## 4. 删除/重建后 RSS 不下降（高水位效应）

### 现象

在“百万文件建索引 → 大量删除 → 索引条目数明显变小”的情况下，内存报告（L2 估算）可能已经大幅降低，但进程 RSS 仍保持在较高水平。

这通常不是泄漏，而是：

- allocator 不会立刻把空闲堆内存归还 OS（RSS 不回落）
- `HashMap`/`Vec` 等容器 capacity 只增不减（len 变小但 capacity 保留）
- 高峰值阶段（构建/快照/事件风暴）造成的临时分配使 RSS 抬升并“粘住”

### 处理原则

- 正确性优先：索引一致性靠“overflow → rebuild/补扫”闭环，不以 RSS 变化作为一致性判断
- 稳态回收手段（按代价递增）：
  - 重建关键结构并替换旧结构（通过 drop 释放旧 capacity；配合 ArcSwap 可避免“原地 reset”导致查询不可用）
  - 可选：切换 `mimalloc` 作为全局分配器（用于动态更新场景下的碎片回吐对照，隔离 allocator 噪声）
  - 可选：`malloc_trim(0)`（glibc 环境下尝试回吐 RSS，可能带来性能波动）
  - 进程级回收：worker 子进程/自重启（最可靠的 RSS 回落方式）
