# fd-rdd 当前架构（截至 2026-02-16）

> 目标：常驻守护进程 + HTTP API 查询；在百万文件规模下保持可用、可恢复、可观测。

## 1. 总体形态：查询与写入解耦

- **查询链路（read path）**：纯内存/只读 mmap，不做扫盘
- **写入链路（write path）**：watcher 事件 → 内存 Delta → 周期性 Flush 到磁盘段（LSM）
- **兜底链路（repair path）**：overflow/不可信快照 → 后台 full rebuild

## 2. 核心组件

### 2.1 TieredIndex（索引总控）

代码位置：`src/index/tiered.rs`

- **L1 Cache**：小型热缓存（加速重复查询）
- **L2 PersistentIndex（内存 Delta）**：实时可变索引（事件写入目标）
- **Disk Layers（mmap 段）**：只读基座/历史段（newest→oldest 合并）
- **OverlayState（影子集合）**：跨段屏蔽集合（delete/upsert 路径 bytes），用于：
  - newest→oldest 合并时屏蔽更老段的同路径结果
  - flush 时生成 `.del` sidecar（跨段 tombstone）
- **WAL（events.wal）**：事件追加日志，用于停机后回放“在线期间未 flush 的增量”
- **Rebuild 状态机**：支持后台重建与 ArcSwap 原子切换；重建期间事件缓冲并按路径去重

### 2.2 SnapshotStore（持久化与 LSM）

代码位置：`src/storage/snapshot.rs`

持久化布局（默认）：

- legacy：`index.db`（兼容读取 v2~v6；v6 为段式容器）
- LSM：`index.d/`
  - `MANIFEST.bin`：LSM manifest（base_id/delta_ids/wal_seal_id/last_build_ns）
  - `seg-{id:016x}.db`：v6 段式快照（mmap + lazy decode）
  - `seg-{id:016x}.del`：按路径 bytes 的 tombstone sidecar
  - `events.wal`（以及 `events.wal.seal-*`）：追加型事件日志

### 2.3 EventPipeline（watcher + debounce）

代码位置：`src/event/*`

- notify watcher 监听 roots，做 bounded channel + debounce
- 默认忽略快照路径（`index.db/index.d`），避免“索引写入反哺 watcher”的反馈回路
- 事件批量进入 `TieredIndex::apply_events`：
  - 先 best-effort 追加写入 WAL（replay 场景禁用写回）
  - 更新 overlay（delete/upsert 屏蔽集合）
  - 更新 L2（内存 Delta）与 L1（热缓存）

## 3. 查询语义（newest → oldest）

代码位置：`src/index/tiered.rs`（`query()`）

1. 构建 matcher（trigram + 精确 matcher）
2. L1 命中则返回
3. 初始化 `blocked` 集合：
   - 先加入 overlay.deleted_paths（屏蔽更老段）
4. 查询顺序：
   - L2（内存 Delta，newest）
   - Disk layers（mmap 段，newest→oldest）
5. 对每条结果按路径 bytes 去重/屏蔽：已 blocked 则跳过，否则加入 blocked 并输出

效果：同一路径遵循 newest 覆盖 oldest；delete 在后续层中会屏蔽更老层的同路径结果。

## 4. Flush/Compaction/GC（写放大控制）

### 4.1 Flush（内存 Delta → 新 delta segment）

代码位置：`src/index/tiered.rs`（`snapshot_now()`）

- 写锁窗口内：
  - seal WAL（切分出 `events.wal.seal-*`）
  - ArcSwap 将当前 L2 Delta 指针换成新的空 Delta
  - 将旧 Delta 导出为 v6 bytes 写入新 `seg-*.db`
  - 将 overlay delete（扣除同路径 upsert 的抵消项）写入 `seg-*.del`
  - 更新 `MANIFEST.bin` 追加该 delta，并写入 `wal_seal_id` checkpoint

### 4.2 Compaction（多 delta → 新 base）

代码位置：`src/index/tiered.rs`（`compact_layers()`）

- oldest→newest 合并段：
  - 应用 `.del` tombstone
  - 将 live metas 灌入新索引并导出为新 base
- 原子更新 manifest：base=新段，delta=[]

### 4.3 物理 GC（清理 stale segments）

代码位置：`src/storage/snapshot.rs`（`gc_stale_segments()`）

- compaction/replace-base 成功后，删除 `index.d/` 下 manifest 未引用的旧 `seg-*.db/.del`（best-effort）

## 5. 冷启动一致性：离线变更检测（stale → rebuild）

问题：停机期间发生删除/移动，watcher 不在线且 WAL 无法凭空记录这些事件；旧段会包含幽灵记录，查询触页导致 RSS 暴涨并产生脏结果。

落地兜底（Level 1）：

- manifest 记录 `last_build_ns`
- 冷启动加载 LSM 段之前，对 roots 做递归目录 mtime crawl（只 stat 目录；发现任意 `dir.mtime > last_build_ns` early-exit）
- 一旦判 stale：
  - 不挂载 disk layers（Disk Segments=0）
  - 从空索引启动并触发后台 full rebuild

## 6. 观测与“内存报表 vs RSS”差异

代码位置：`src/stats/*`

- MemoryReport 统计的是索引堆结构（L1/L2/overlay/rebuild 等）
- RSS 还包含：
  - mmap 段触页后的 file-backed 常驻（Private_Clean）
  - allocator/运行时/线程等基础开销
  - THP(always) 下匿名大页导致的空壳 RSS（已通过 mimalloc `no_thp` 编译期开关治理）

## 7. 运行参数（常用）

- `--root PATH`：指定索引根；不传默认 `$HOME`（以及存在时的 `/tmp/vcp_test_data`）
- `--no-snapshot`：跳过快照加载，从空索引启动
- `--no-build`：禁用空索引时的后台 full_build 扫盘
- `--no-watch`：禁用 watcher（只跑查询/快照循环）
- `--ignore-path PATH`：额外忽略 watcher 路径前缀
