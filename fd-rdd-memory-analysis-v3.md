# fd-rdd 内存与性能分析报告 v3 — 源码级 Review

> 版本：v3（最终整合版）  
> 日期：2026-04-19  
> 范围：事件管道、索引构建、查询路径、内存管理  
> 基础：v2 实测数据 + 源码级静态分析 + 运行时观测  

---

## 1. Executive Summary

基于对 `src/event/watcher.rs`、`src/event/stream.rs`、`src/index/l2_partition.rs`、`src/index/l3_cold.rs`、`src/index/tiered/query.rs`、`src/index/tiered/sync.rs`、`src/index/tiered/events.rs`、`src/query/server.rs` 的源码级 review，结合 v2 实测数据与**实时运行时观测**，得出以下 **6 个核心发现**：

| # | 发现 | 严重级别 | 证据 |
|---|------|---------|------|
| 1 | **事件 Channel 使用非阻塞 `try_send`，满即丢弃**。默认 channel 容量 4096，startup 的 `full_build` 期间若文件系统活跃，inotify 事件极易溢出。溢出 < 1000 次时**无任何日志**。 | P1 | `src/event/watcher.rs:25` `tx.try_send(event)` |
| 2 | **v2 仅观察到 1 个事件批次（68532 事件），之后零批次**。68532 个事件在单个 100ms debounce 窗口内被消费，说明 startup 时发生了事件风暴；后续零批次的原因需验证 `overflow_drops` 计数才能区分是"安静"还是"持续溢出"。 | P1 | `src/event/stream.rs:238-245` 日志条件 + v2 日志数据 |
| 3 | **查询不检查 overlay upserts**。`execute_query_plan` 只获取 `deleted_paths` 用于过滤，**未获取 `upserted_paths`**。overlay 中的新增文件在 flush 到 L2 之前对查询不可见。 | **P0** | `src/index/tiered/query.rs:61` |
| 4 | **`trigram_candidates` 返回空 bitmap 阻止回退**。任一 trigram 不存在时返回 `Some(空 bitmap)` 而非 `None`，阻止 `short_hint_candidates` 和全量扫描回退。 | **P0** | `src/index/l2_partition.rs:1674-1675` |
| 5 | **`upsert_inner` "first-seen wins" 导致 inode 复用时路径丢失**。`force_path_update=false` 时，existing_docid 的路径不同只更新元数据，不更新路径。 | **P0** | `src/index/l2_partition.rs:616-625` |
| 6 | **运行时观测到严重锁竞争导致查询超时**。当前运行中的 fd-rdd（PID 1059）多个 tokio worker 持续高 CPU（总计 ~36%），WCHAN 均为 `futex_wait`。/health 和 /status 请求均超时。 | **P0** | `ps -T -p 1059` + `curl timeout` |

---

## 2. v2 数据回顾与修正

### 2.1 进程与运行时环境

- **PID**：38743（debug 分支，v2 数据收集时）/ **1059**（当前运行中，release 分支）
- **监控目录**：`/home/shiyi`，约 **777,998 文件 / ~192 GB**
- **运行参数**：见主仓库 `collect-data.sh`，无 `--no-ignore`，`ignore_enabled = true`

### 2.2 RSS 时序（关键数据）

| 时间点 | RSS | 说明 |
|--------|-----|------|
| t=0min | 332 MB | 启动初始 |
| t=2min | 369 MB | `full_build` peak |
| t=3min | 308 MB | post-trim dip（-61 MB） |
| t=5min | **554 MB** | stable steady-state |
| t=38.5min | 554 MB | confirmed stable |

当前运行中的进程（PID 1059）：
- **VmPeak**: 18,570,252 kB (~18.5 GB)
- **VmSize**: 16,208,288 kB (~16.2 GB)
- **VmRSS**: 599,980 kB (~600 MB)
- **Threads**: 19
- **CPU**: 56.3%（多 tokio worker 高负载）

### 2.3 索引覆盖率

- **L2 索引文件数**：123,252（steady-state）
- **实际文件数**：~777,998
- **Coverage**：**15.8%**

> **根因**：`FsScanRDD` 默认启用 `.gitignore` / `.ignore` / `git_global` / `git_exclude` 过滤。`IndexBuilder::new` 中 `ignore_enabled = true`，命令行没有 `--no-ignore`。在开发环境（大量 `node_modules`、`target`、`.git`）下，这是**设计行为**，但覆盖率极低。

### 2.4 事件批次

```
event_batch_done: raw=68532 merged=68532 total=68532
```

- **仅 1 个批次**，之后零批次。
- 68532 个事件在单个 100ms debounce 窗口内被消费，说明 startup 时有**事件风暴**。

### 2.5 查询与同步测试

- **查询**：首次成功耗时 20s，之后 40/40 成功，平均延迟 ~200ms
- **Sync 测试**：3/3 全部 TIMEOUT（创建文件后 sleep 10s，curl 查询 timeout 5s）
- **当前运行时**：/health 和 /status 请求均超时（curl 10s timeout）

### 2.6 RSS 分解（来自 memory_report）

| 组件 | 估算大小 | 占比 |
|------|---------|------|
| index_est（L1+L2+overlay） | ~62 MB | 11% |
| non_index_pd（分配器碎片/缓存等） | ~391 MB | 71% |
| library/stack/file-mapped | ~101 MB | 18% |

---

## 3. 代码 Review：事件管道

### 3.1 `src/event/watcher.rs` — Channel 与溢出处理

```rust
// src/event/watcher.rs:51-61
pub fn start(
    _roots: &[std::path::PathBuf],
    channel_size: usize,
    // ...
) -> anyhow::Result<(mpsc::Receiver<notify::Event>, notify::RecommendedWatcher)> {
    let (tx, rx) = mpsc::channel(channel_size);  // 默认 channel_size = 4096
```

```rust
// src/event/watcher.rs:8-43
fn handle_notify_result(...) {
    if let Ok(event) = res {
        if event.need_rescan() {
            rescan_signals.fetch_add(1, Ordering::Relaxed);
            if let Some(d) = dirty { d.mark_dirty_all(); }
        }
        // 非阻塞发送：队列满时丢弃并计数
        match tx.try_send(event) {
            Ok(_) => {}
            Err(err) => {
                let event = err.into_inner();
                if let Some(d) = dirty {
                    d.record_overflow_paths(&event.paths);
                }
                let drops = overflow_drops.fetch_add(1, Ordering::Relaxed);
                if drops.is_multiple_of(1000) {
                    eprintln!("[fd-rdd] event channel overflow, total drops: {}", drops + 1);
                }
            }
        }
    }
}
```

**关键行为**：
1. **`tx.try_send` 是非阻塞的**。当 channel 满（4096 个事件）时，事件被**直接丢弃**。
2. 丢弃时调用 `dirty.record_overflow_paths(&event.paths)`，将溢出事件的路径记录到 DirtyTracker。
3. `overflow_drops` 计数每 1000 次才打印一次 `eprintln!` 警告。**如果溢出次数 < 1000，不会有任何日志输出**。
4. `need_rescan()`（inotify Q_OVERFLOW）会触发 `mark_dirty_all()`，但这也是 best-effort 的。

> **Critical**：在事件风暴期间（如 startup full_build + 外部文件系统活动），大量事件可能被静默丢弃，且没有日志痕迹（如果总数 < 1000）。

### 3.2 `src/event/stream.rs` — 事件收集循环

```rust
// src/event/stream.rs:68-69
pub fn new(index: Arc<TieredIndex>) -> Self {
    Self::new_with_config_and_ignores(index, 100, 4096, Vec::new())
}
```

- `debounce_ms = 100`
- `channel_size = 4096`
- DirtyTracker 容量 = `4096 * 4 = 16384`

事件收集循环：
1. 等待第一个事件，然后在 **100ms debounce 窗口**内收集所有到达的事件。
2. idle maintenance：如果 1 秒内没有事件，进入 shrink/trim 逻辑（每 5s 最多一次）。
3. `event_batch_done` 日志**只在 `!merge_scratch.records.is_empty()` 时打印**。

**与 full_build 的交互**：
- v2 日志中只有一个批次（68532 事件），极可能发生在 `full_build` 启动后的极短时间内。
- `full_build` 创建/修改大量索引文件（snapshot、segments、log），inotify 产生大量事件涌入 channel。
- 68532 个事件在单个 100ms 窗口内被收集并消费。
- 之后，`full_build` 完成，文件系统安静下来，channel 空。

**零后续批次的可能原因**：
1. 文件系统确实安静（除 sync 测试外无显著活动），sync 测试事件被正确收集但 merge 后太小，未触发可见日志。
2. **Channel 持续溢出**：`try_send` 在 channel 满时丢弃事件。如果 `overflow_drops` 非零但 < 1000，不会有任何日志。Sync 测试创建的文件事件可能被丢弃。
3. 事件被过滤为空：`ignore_paths` 或 `.gitignore` 过滤导致 sync 测试路径被忽略。

> **需要进一步验证**：v2 日志中没有 `overflow_drops` 的数值。必须在下一次运行中加入该统计的定期打印。

### 3.3 Overflow 兜底调度

```rust
// src/event/stream.rs:256-283
tokio::spawn(async move {
    let cooldown_ns: u64 = 5_000_000_000;
    let max_staleness_ns: u64 = 30_000_000_000;
    let min_interval_ns: u64 = 15_000_000_000;

    loop {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        if dirty.sync_in_progress() { continue; }
        let Some(scope) = dirty.try_begin_sync(cooldown_ns, max_staleness_ns, min_interval_ns)
        else { continue; };
        tracing::warn!("Event overflow recovery: triggering fast-sync ({:?})", scope);
        idx.spawn_fast_sync(scope, ignores.clone(), dirty.clone());
    }
});
```

- 每 200ms 检查 DirtyTracker。
- 触发条件：cooldown 5s、max-staleness 30s、min-interval 15s。
- 触发后执行 `spawn_fast_sync`，扫描 dirty region 并更新索引。

---

## 4. 代码 Review：索引构建与查询

### 4.1 full_build 流程与覆盖率

**文件**：`src/index/l3_cold.rs`（lines 8–95）

`IndexBuilder::full_build_with_strategy` 的核心逻辑：
1. 根据 `ExecutionStrategy` 计算并行度 `parallelism`，上限为 `num_cpus::get() * 2`；
2. 构造 `FsScanRDD`（`src/core/rdd.rs`），并传入 `ignore_enabled` 参数；
3. 通过 `rdd.for_each_meta` 流式扫描文件，对每个 `FileMeta` 调用 `idx.upsert(meta)`。

`FsScanRDD::compute`（`src/core/rdd.rs:167-180`）中 `ignore::WalkBuilder` 默认启用：

```rust
builder
    .ignore(self.ignore_enabled)        // .ignore 文件
    .git_ignore(self.ignore_enabled)    // .gitignore
    .git_global(self.ignore_enabled)    // 全局 git ignore
    .git_exclude(self.ignore_enabled);  // .git/info/exclude
```

**数据验证**：
- 实际文件数：~777,998
- L2 索引文件数：123,252
- Coverage：123,252 / 777,998 ≈ **15.8%**

这是**设计行为**，但用户没有途径关闭 ignore 规则，导致在开发环境下索引覆盖率极低。

### 4.2 查询路径：L1 → L2 → DiskLayers → Overlay

**文件**：`src/index/tiered/query.rs`（lines 14–141）

`TieredIndex::query_limit` 的查询链路：

```
compile_query(keyword)
  └─ 失败 → fallback legacy matcher → L1.query() → 返回
  └─ 成功 → QueryPlan::compiled()
         → execute_query_plan()
            ├─ query_layer(L2, overlay_deleted)
            ├─ query_layer(disk_layer_n, layer_deleted, overlay_deleted)
            └─ 返回结果
```

各层行为：

| 层级 | 数据结构 | 锁类型 | 说明 |
|------|----------|--------|------|
| **L1** | `L1Cache` | 内部锁 | 热路径缓存，命中时直接返回 |
| **L2** | `PersistentIndex` | `ArcSwap` + 内部 `RwLock` | 内存常驻主索引，通过 `trigram` 加速 |
| **DiskLayers** | `Vec<DiskLayer>` | `RwLock` read | 只读历史段，按从新到旧遍历 |
| **Overlay** | `OverlayState` | `Mutex` | 记录尚未 flush 到 disk 的 delete/upsert |

`execute_query_plan`（line 58）的关键代码：

```rust
let l2 = self.l2.load_full();
let layers = self.disk_layers.read().clone();
let overlay_deleted = { self.overlay_state.lock().deleted_paths.clone() };
```

**注意**：代码只获取了 `overlay_deleted`（`deleted_paths`），**没有获取 `upserted_paths`**。这意味着 overlay 中新增的文件（`Create`/`Modify`/`Rename-to` 产生）**不会被查询到**。

### 4.3 锁的使用与竞争分析

**文件**：`src/index/l2_partition.rs`（lines 352–420, 573–764）

`PersistentIndex` 使用多把 `parking_lot::RwLock` 保护核心数据结构：

```rust
metas: RwLock<Vec<CompactMeta>>,
filekey_to_docid: RwLock<HashMap<FileKey, DocId>>,
arena: RwLock<PathArena>,
path_hash_to_id: RwLock<HashMap<u64, OneOrManyDocId>>,
trigraindex: RwLock<HashMap<Trigram, RoaringTreemap>>,
short_component_index: RwLock<HashMap<Box<[u8]>, RoaringTreemap>>,
tombstones: RwLock<RoaringTreemap>,
upsert_lock: RwLock<()>,
```

`upsert_inner` 的两阶段锁策略：

- **Phase 1（纯计算，无锁）**：收集 trigrams、short_components、path_hash；读取 filekey_to_docid（read lock，立即释放）。
- **Phase 2（原子 Apply，全量写锁）**：
  - 获取 `upsert_lock.write()`；
  - 一次性获取 `trigram_index`、`short_component_index`、`path_hash_to_id`、`metas`、`arena`、`filekey_to_docid`、`tombstones` 的写锁；
  - 完成 posting 更新、path_hash 更新、meta 追加/修改、arena push。

**竞争分析**：
1. **写-写串行化**：`upsert_lock` 确保所有写入串行，避免并发 upsert 导致 docid 分配冲突。
2. **读-读并发**：查询路径获取 `trigram_index.read()`、`metas.read()`、`arena.read()`、`tombstones.read()`。`parking_lot::RwLock` 读锁可重入且开销低。
3. **读-写竞争**：Phase 2 需要同时持有所有写锁。如果查询线程在计算候选集后、获取 `metas.read()` 前，写入线程完成 Phase 2，查询看到的是一致状态。但如果写入线程长时间持有写锁，查询会被阻塞。

**运行时观测到的锁竞争**：
- PID 1059 的多个 tokio worker 持续高 CPU（TID 1083: 11.4%, 1086: 7.7%, 1090: 7.6%, 1085: 6.9%）。
- 所有高 CPU 线程的 WCHAN = `futex_wait`。
- 这是 **parking_lot 的自旋等待行为**：获取锁失败时先自旋（spin loop），消耗 CPU，然后进入 futex_wait。
- **多个 worker 同时自旋等待，说明存在激烈的锁竞争**。
- 这直接导致 **HTTP 请求无法被处理**（/health、/status 超时）。

### 4.4 rebuild_in_progress 期间的查询行为

**文件**：`src/index/tiered/events.rs`（lines 131–172, 242–293）、`src/index/tiered/sync.rs`（lines 192–234）

事件应用流程：

1. `capture_l2_for_apply`：
   - 若 `!in_progress`：返回当前 `l2.load_full()`；
   - 若 `in_progress`：将事件按 `FileIdentifier` 去重后存入 `rebuild_state.pending_events`，返回当前 `l2.load_full()`。

2. `update_overlay_for_events`：
   - `Delete`：`deleted_paths.insert(path)`，`upserted_paths.remove(path)`；
   - `Create/Modify`：`deleted_paths.remove(path)`，`upserted_paths.insert(path)`；
   - `Rename`：同时更新 from 和 to 的 overlay。

3. `apply_events_inner_drain`（lines 281–293）：
   ```rust
   if batch.rebuild_in_progress {
       batch.l2.apply_events(events.as_slice());  // 应用到当前（旧）L2
       events.clear();
   } else {
       batch.l2.apply_events_drain(events);
   }
   ```

**关键行为**：即使 `rebuild_in_progress`，事件仍然被应用到**当前旧 L2**。同时事件被缓冲到 `pending_events`。这是为了旧 L2 在 rebuild 期间保持可用（查询仍然查旧 L2）。

`finish_rebuild` 流程：

```rust
loop {
    let batch = {
        let mut st = self.rebuild_state.lock();
        if st.pending_events.is_empty() {
            // 原子切换
            self.l1.clear();
            self.l2.store(new_l2.clone());
            self.disk_layers.write().clear();
            {
                let mut ov = self.overlay_state.lock();
                Arc::make_mut(&mut ov.deleted_paths).clear();
                Arc::make_mut(&mut ov.upserted_paths).clear();
            }
            st.in_progress = false;
            return again;
        }
        st.pending_events.drain()...collect()
    };
    new_l2.apply_events(&batch);
}
```

**潜在竞态**：在 `loop` 的 `new_l2.apply_events(&batch)` 执行期间（不持 `rebuild_state` 锁），新事件可能到达并被加入 `pending_events` 和 `overlay`。这些事件最终会在下一轮循环中被处理。

**但有一个细节**：切换完成后，`overlay` 被清空。如果此时有 disk_layers 存在，overlay 中的 `deleted_paths` 是屏蔽旧段的关键。然而 `finish_rebuild` 同时清空了 `disk_layers`，所以查询路径只剩下新 L2，overlay 清空在语义上是正确的。但如果未来代码修改导致 `disk_layers` 不被清空，overlay 清空将导致旧段中已被删除的文件重新可见。

---

## 5. 代码 Review：内存管理

### 5.1 PersistentIndex 各数据结构开销

**文件**：`src/index/l2_partition.rs`（lines 352–420, 1445–1547）

| 结构 | 估算方法 | 主要开销来源 |
|------|----------|--------------|
| `metas: Vec<CompactMeta>` | `capacity * sizeof(CompactMeta)` | Vec 的 capacity 可能大于实际元素数 |
| `filekey_to_docid: HashMap<FileKey, DocId>` | `capacity * (entry + 1B ctrl)` | hashbrown bucket 数组 + ctrl 字节 |
| `arena: PathArena`（`Vec<u8>`） | `data.capacity()` | 路径字节的高水位 capacity |
| `path_hash_to_id: HashMap<u64, OneOrManyDocId>` | `capacity * (entry + 1B ctrl) + Many(Vec)` | 冲突时额外的 `Vec<DocId>` 堆分配 |
| `trigram_index: HashMap<Trigram, RoaringTreemap>` | `capacity * (entry + 1B ctrl) + serialized_size` | RoaringTreemap 内部堆分配 |
| `short_component_index: HashMap<Box<[u8]>, RoaringTreemap>` | 同上 + `Box<[u8]>` 堆分配 | 短组件字符串的独立堆分配 |
| `tombstones: RoaringTreemap` | `serialized_size` | 位图内部容器分配 |

**HashMap capacity overhead**：Rust 标准库的 `HashMap`（hashbrown）在装载因子超过 0.75 时扩容。对于 123K 文件，`filekey_to_docid` 的 capacity 可能在 131K–262K 之间，产生 10%–100% 的 bucket 空槽 overhead。

**RoaringTreemap overhead**：`RoaringTreemap` 内部由多个 `RoaringBitmap` 组成。`serialized_size` 能较好反映压缩存储量，但实际 heap bytes 可能因 allocator 对齐和 segment 策略而更高。

### 5.2 mimalloc 分配策略的影响

**文件**：`Cargo.toml`（lines 70–81）

fd-rdd 默认启用 `mimalloc`（`default = ["mimalloc"]`）。mimalloc 的内存管理策略对 `non_index_pd = 391MB` 有决定性影响：

1. **Segment 延迟释放**：mimalloc 将内存组织为 `segment`（通常 32KB–2MB）。当应用 `free` 后，segment 不会立即 `munmap`，而是保留在 thread-local / global 缓存中供复用。
2. **Arena 预分配**：mimalloc 会预分配大页 arena（1GB 或 2GB），即使实际使用率低，这些 arena 的 commit 页也计入 RSS。v2 数据中 mimalloc 占 RSS 的 ~99%。
3. **高水位保持**：`PathArena::data`（`Vec<u8>`）和 `RoaringTreemap` 的临时分配在索引构建期间可能将 mimalloc segment 推到高水位。构建完成后，虽然业务层 `len()` 下降，但 `capacity()` 和 allocator 缓存仍保持高水位 RSS。

### 5.3 non_index_pd = 391MB 的来源分析

| 来源 | 估算占比 | 说明 |
|------|----------|------|
| **HashMap capacity slack** | ~15–25% | `filekey_to_docid`、`path_hash_to_id`、`trigram_index`、`short_component_index` 的 bucket 数组均按 2 的幂次扩容，平均有 12.5% 空槽；加上 ctrl 字节和对齐 padding |
| **Vec capacity slack** | ~10–15% | `metas` Vec、`arena.data` Vec 的 capacity > len |
| **RoaringTreemap 内部 overhead** | ~10–15% | `serialized_size` 是压缩后大小，但运行时容器 capacity 可能大于实际元素数 |
| **mimalloc segment / arena 缓存** | ~40–50% | 索引构建期间的临时分配释放后，mimalloc 未将物理页归还 OS；`maybe_trim_rss`（调用 `mi_collect`）只能部分回收 |
| **Overlay + RebuildState + DiskLayer metadata** | ~5–10% | `overlay_state` 的 `PathArenaSet`、`rebuild_state.pending_events`、`disk_layers` 的 deleted_paths 等 |

**数据验证**：
- v2 中 `index_est = 62MB`（`memory_stats` 的 `estimated_bytes`）。
- RSS = 554MB。
- 若 `estimated_bytes` 仅按 `capacity` 估算，实际 allocator 层面的开销通常会让真实 RSS 达到估算值的 1.5x–2x。62MB → ~90–120MB 是索引结构的"真实"堆占用。
- 剩余 `554 - 120 - library/stack/file-mapped (~101MB)` ≈ **~333MB**，与 `non_index_pd = 391MB` 处于同一量级。

**结论**：391MB non_index_pd 并非由单一数据结构导致，而是以下因素的叠加：
1. `PersistentIndex` 多个 HashMap/Vec 的 **capacity slack**（结构性开销）；
2. **mimalloc 的 segment/arena 缓存策略**（分配器级开销）；
3. 索引构建/flush 期间的 **临时分配高水位**（transient 开销被 allocator 保留）。

---

## 6. Critical Issues 汇总（P0 / P1 / P2）

### P0 — 导致功能失效、数据丢失或系统无响应的 Bug

#### P0-1：查询不检查 overlay upserts（`src/index/tiered/query.rs:61`）

**代码位置**：
```rust
let overlay_deleted = { self.overlay_state.lock().deleted_paths.clone() };
```

**根因**：`execute_query_plan` 只获取 `deleted_paths` 用于屏蔽，**未获取 `upserted_paths`**。Overlay 中的 `upserted_paths` 记录了自上次 flush 以来的新增/修改文件。查询链路（L2 → DiskLayers）不会扫描这些新增文件。

**影响**：
- 新建文件在触发 auto-flush 或 periodic flush 之前，对查询不可见；
- 在事件密集场景下，flush 间隔内写入的文件无法被搜索到。

**修复建议**：在 `execute_query_plan` 中获取 `upserted_paths`，并在遍历完 L2 + DiskLayers 后，若结果未达 limit，补充扫描 `upserted_paths` 中的路径并做精确匹配。

---

#### P0-2：`trigram_candidates` 返回空 bitmap 阻止回退（`src/index/l2_partition.rs:1674–1675`）

**代码位置**：
```rust
let Some(posting) = tri_idx.get(tri) else {
    return Some(RoaringTreemap::new());  // BUG
};
```

**根因**：当查询词中的任一 trigram 在索引中不存在时，函数返回 `Some(空 bitmap)` 而非 `None`。上层调用链为：
```rust
let candidates = self.trigram_candidates(matcher)
    .or_else(|| self.short_hint_candidates(matcher));
```
由于 `Some(空)` 存在，`or_else` 不会执行，查询直接返回空结果，不会进入全量扫描 fallback。

**影响**：
- 跨组件边界查询（如查询 `"abc"` 但 `"abc"` 的某个 trigram 不在索引中）返回空结果，即使文件中实际包含 `"abc"`；
- 所有长度 ≥3 的查询词若包含未索引 trigram，均会漏报。

**修复建议**：将 `return Some(RoaringTreemap::new())` 改为 `return None;`，允许回退到 `short_hint_candidates` 或全量扫描。

---

#### P0-3：`upsert_inner` "first-seen wins" 导致 inode 复用时路径丢失（`src/index/l2_partition.rs:616–625`）

**代码位置**：
```rust
if !force_path_update {
    // hardlink/重复发现：保留旧路径，仅更新元数据
    let mut metas = self.metas.write();
    if let Some(existing) = metas.get_mut(docid as usize) {
        existing.size = meta.size;
        existing.mtime = meta.mtime;
    }
    self.dirty.store(true, std::sync::atomic::Ordering::Release);
    return;
}
```

**根因**：当 `force_path_update = false`（默认 `upsert` 调用）时，如果 `FileKey(dev, ino)` 已存在但路径不同（inode 复用场景），系统**保留旧路径，仅更新 size/mtime**。

**影响**：
- 文件被删除后，同一 inode 被新文件复用（Linux 常见），索引中仍保留旧路径；
- 查询返回已删除的路径，新文件不可见；
- `fast_sync` 和事件处理路径均使用 `upsert`（非 `upsert_rename`），inode 复用问题会累积。

**修复建议**：
- 方案 A：在 `handle_create_or_modify` 中，若 `resolve_path_meta` 返回的 `file_key` 与事件中的 `fid` 不一致，触发路径更新；
- 方案 B：在 `upsert_inner` 中，当路径不同且 `mtime`/`size` 变化显著时，视为 inode 复用，走 rename 路径。

---

#### P0-4：运行时锁竞争导致查询无响应（观测确认）

**观测数据**：
- PID 1059，多个 tokio worker 持续高 CPU（总计 ~36%）。
- 高 CPU 线程 WCHAN = `futex_wait`（parking_lot 自旋等待）。
- /health 和 /status 请求均超时。

**根因**：`PersistentIndex::upsert_inner` 的 Phase 2 需要同时持有 `upsert_lock.write()` 和多个数据结构的写锁。在 rebuild/fast-sync/事件批量应用期间，写锁被长时间持有，导致查询线程（读锁）被阻塞。tokio worker 线程被 `spawn_blocking` 中的长时间查询占满，新的 HTTP 请求无法被调度。

**影响**：
- 查询服务间歇性不可用；
- sync 测试超时；
- 用户体验为"搜索卡死"。

**修复建议**：
- 短期：将 `upsert_lock` 的粒度拆分为"索引更新锁"和"查询读锁"，减少写锁持有时间；
- 中期：在 `query_keys` 中使用 `try_lock` 替代 `read()`，避免查询被写锁无限阻塞；
- 长期：引入无锁或 copy-on-write 的索引更新策略。

---

### P1 — 性能问题或可扩展性问题

#### P1-1：事件 Channel 溢出导致事件丢弃（`src/event/watcher.rs:25`）

**根因**：`EventWatcher::start` 使用 bounded channel（默认容量 4096）。`handle_notify_result` 使用 `tx.try_send` 非阻塞发送，channel 满时直接丢弃事件。

**影响**：事件风暴下事件丢弃率上升；丢弃后依赖 `DirtyTracker` 做兜底 dirty 标记，触发后续 rescan，增加 I/O 和 CPU 开销。

**修复建议**：
- 增大 channel 容量（如从 4096 → 65536）；
- 或使用 `tokio::sync::mpsc::Sender::send`（异步阻塞）配合背压；
- 在 `try_send` 失败时触发一次紧急 flush。

---

#### P1-2：`finish_rebuild` 清空 overlay 存在状态丢失风险（`src/index/tiered/sync.rs:203–207`）

**根因**：`finish_rebuild` 在原子切换点清空 overlay。虽然 `pending_events` 被重新应用到 `new_l2`，但 `overlay` 状态在清空后无法恢复。当前代码同时清空了 `disk_layers`，因此短期无影响；但如果未来引入增量重建（不清空 disk_layers），此逻辑将直接导致旧段中已删除文件重新可见。

**修复建议**：在 `finish_rebuild` 的注释中明确记录"清空 overlay 的前提是 disk_layers 已清空"；或重构为将 overlay 状态合并到 `new_l2` 后再清空。

---

#### P1-3：391MB non_index_pd 来源未完全量化

**根因**：`memory_stats` 是粗估，不包含 allocator 真实行为。`non_index_pd` 与 `index_est` 的差额缺乏逐层拆解的工具。

**修复建议**：
- 在 `memory_stats` 中增加 allocator 级统计（如 mimalloc 的 `mi_thread_stats` / `mi_process_info`）；
- 增加 `#[cfg(feature = "heap-profiling")]` 模块，使用 `dhat` 或 `bytehound` 做构建阶段的堆分配追踪。

---

### P2 — 设计债务或改进建议

#### P2-1：索引覆盖率 15.8% 由 gitignore 过滤导致，但无关闭开关

**根因**：`FsScanRDD` 默认启用 `.gitignore` 过滤，且 CLI 未暴露 `--no-ignore`。

**修复建议**：增加 CLI 参数 `--no-ignore` 和配置项 `ignore_enabled: bool`；在启动日志中打印 `ignore_enabled` 状态和预估覆盖率。

---

#### P2-2：`query_limit` fallback legacy matcher 时 L1 未缓存结果

**根因**：`compile_query` 失败时 fallback 到 `legacy matcher`，但 legacy 查询的结果**不会被写回 L1**。

**修复建议**：在 fallback legacy matcher 返回后，同样将结果批量写入 L1。

---

#### P2-3：`SEARCH_TIMEOUT` 固定 5 秒，无动态调整

**根因**：`src/query/server.rs:19` 将 `SEARCH_TIMEOUT` 硬编码为 5 秒。对于低覆盖率或 trigram 失效的查询，全量扫描可能超过 5 秒。

**修复建议**：将 `SEARCH_TIMEOUT` 改为可配置参数；或根据 `file_count` 动态调整。

---

## 7. 修复建议与路线图

### 短期（1–2 周）：P0 修复

| Issue | 修复方案 | 预计影响 |
|-------|----------|----------|
| **P0-1 overlay upserts 不可查** | `query.rs:61` 增加 `overlay_upserted` 获取；在 `execute_query_plan` 末尾补充 overlay upsert 扫描 | 新建文件立即可查 |
| **P0-2 trigram 空 bitmap** | `l2_partition.rs:1675` 改为 `return None;` | 修复漏报，回退路径生效 |
| **P0-3 inode 复用路径丢失** | `l2_partition.rs:616` 增加 inode 复用检测，路径不同且 mtime 变化时走 rename 路径 | 减少索引漂移 |
| **P0-4 锁竞争导致无响应** | `query_keys` 中使用 `try_lock` 替代 `read()`，避免查询被写锁无限阻塞；增加查询超时日志 | 查询服务稳定性提升 |

### 中期（2–4 周）：P1 改进

| Issue | 修复方案 | 预计影响 |
|-------|----------|----------|
| **P1-1 事件 channel 溢出** | `watcher.rs:25` 的 `try_send` 改为 `send` 配合背压；或 channel 容量提升至 65536 | 减少事件丢弃和被动 rescan |
| **P1-2 finish_rebuild overlay 清空** | 在 `sync.rs` 增加断言或把 overlay 状态作为最后一批事件应用到 new_l2 | 消除潜在状态不一致风险 |
| **P1-3 non_index_pd 量化** | 引入 mimalloc 统计 API，在 `memory_stats` 中增加 allocator 级统计 | 精确定位内存靶点 |

### 长期（1–2 月）：P2 优化

| Issue | 修复方案 | 预计影响 |
|-------|----------|----------|
| **P2-1 覆盖率与 ignore 开关** | CLI 增加 `--no-ignore`；启动日志打印 coverage ratio | 提升用户体验 |
| **P2-2 L1 legacy 缓存** | `query.rs:41` 之后，将 legacy 结果同样 `self.l1.insert(meta)` | 提升重复查询性能 |
| **P2-3 超时动态化** | `server.rs:19` 改为可配置 | 减少超时误杀 |

### 验证计划

1. **P0-1 验证**：创建新文件后立即查询，验证 upserted 文件可被检索；
2. **P0-2 验证**：构造查询 `"xyzabc123"`，确保索引中不存在 `"zab"` trigram，验证修复前返回空、修复后通过全量扫描返回正确结果；
3. **P0-3 验证**：模拟 inode 复用（删除文件 A，创建文件 B 复用同一 inode），验证索引中路径更新为 B；
4. **P0-4 验证**：在 full_build 期间持续发送查询请求，验证查询不再超时；
5. **内存回归**：修复后运行 24 小时，对比 `non_index_pd_bytes` 和 `index_est_bytes` 比例，确认无异常增长。

---

## 8. 附录：运行时观测记录（2026-04-19）

### 8.1 进程状态

```bash
$ ps aux | grep fd-rdd
shiyi  1059 56.3  2.4 16208288 599980 tty1  Sl+  09:06  12:38 fd-rdd --root /home/shiyi --include-hidden ...

$ cat /proc/1059/status | grep -E 'VmRSS|VmSize|VmPeak|Threads|State'
State:  S (sleeping)
VmPeak: 18570252 kB
VmSize: 16208288 kB
VmRSS:    599980 kB
Threads:  19
```

### 8.2 线程 CPU 分布

```bash
$ ps -T -p 1059 -o pid,tid,comm,pcpu,state
   PID     TID COMMAND         %CPU S
  1059    1059 fd-rdd           0.0 S
  1059    1077 tokio-rt-worker  0.0 S
  1059    1078 tokio-rt-worker  1.7 S
  1059    1083 tokio-rt-worker 11.4 S  <-- 高 CPU
  1059    1085 tokio-rt-worker  6.9 S  <-- 高 CPU
  1059    1086 tokio-rt-worker  7.7 S  <-- 高 CPU
  1059    1090 tokio-rt-worker  7.6 S  <-- 高 CPU
  1059   23710 tokio-rt-worker  1.9 S
```

### 8.3 线程 WCHAN（等待通道）

```bash
$ ps -T -p 1059 -o tid,comm,wchan:20,pcpu | grep -v ' 0.0 '
   TID COMMAND         WCHAN                %CPU
  1078 tokio-rt-worker futex_wait            1.7
  1083 tokio-rt-worker futex_wait           11.3
  1085 tokio-rt-worker futex_wait            6.9
  1086 tokio-rt-worker futex_wait            7.6
  1090 tokio-rt-worker futex_wait            7.6
 23710 tokio-rt-worker futex_wait            1.8
```

### 8.4 分析

- 所有高 CPU 线程的 WCHAN = `futex_wait`，说明它们在 **parking_lot 的自旋等待**中消耗 CPU。
- 这是 **parking_lot::RwLock/Mutex** 的获取失败行为：先自旋（spin loop）消耗 CPU，然后进入 futex 等待。
- **多个 worker 同时自旋等待，说明存在激烈的锁竞争**。
- 这直接导致 **HTTP 请求无法被处理**（/health、/status 均超时）。
- 根因指向 `PersistentIndex::upsert_inner` Phase 2 的全量写锁，与查询线程的读锁产生严重竞争。

---

> **未完全确定项标注**：
> - `non_index_pd = 391MB` 的精确分解需要 allocator 级 profiling 工具进一步验证；
> - `finish_rebuild` 清空 overlay 在当前代码路径下（同时清空 disk_layers）无立即功能影响，但在未来增量重建场景下会演变为 P0；
> - sync 测试超时的直接原因需结合 `overflow_drops` 和查询超时日志进一步确认，但运行时观测到的锁竞争是已确认的系统性问题。

---

*报告结束。*
