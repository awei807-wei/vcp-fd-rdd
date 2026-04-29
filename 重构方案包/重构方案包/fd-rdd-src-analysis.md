# fd-rdd 项目 src/ 目录结构分析报告

> 分析分支：`tests`  
> 仓库：https://github.com/awei807-wei/vcp-fd-rdd  
> 分析时间：2025-01-28

---

## 一、目录结构总览

```
src/
├── bin/              # 二进制入口子目录
├── core/             # 核心 RDD / Lineage / DAG / 分区逻辑
│   ├── adaptive.rs
│   ├── dag.rs
│   ├── lineage.rs
│   ├── mod.rs
│   ├── partition.rs
│   └── rdd.rs
├── event/            # 文件系统事件监听与恢复
│   ├── ignore_filter.rs
│   ├── mod.rs
│   ├── recovery.rs
│   ├── stream.rs
│   ├── verify.rs
│   └── watcher.rs
├── index/            # 多级索引系统
│   ├── tiered/       # 三级分层索引（L1/L2/L3）
│   │   ├── arena.rs
│   │   ├── compaction.rs
│   │   ├── disk_layer.rs
│   │   ├── events.rs
│   │   ├── load.rs
│   │   ├── memory.rs
│   │   ├── mod.rs
│   │   ├── query.rs
│   │   ├── query_plan.rs
│   │   ├── rebuild.rs
│   │   ├── snapshot.rs
│   │   ├── sync.rs
│   │   └── tests.rs
│   ├── content_filter.rs
│   ├── l1_cache.rs
│   ├── l2_partition.rs
│   ├── l3_cold.rs
│   ├── mmap_index.rs
│   └── mod.rs
├── query/            # 查询引擎与 HTTP 服务
│   ├── dsl.rs
│   ├── fzf.rs
│   ├── matcher.rs
│   ├── mod.rs
│   ├── scoring.rs
│   ├── server.rs
│   └── socket.rs
├── stats/            # 统计信息
├── storage/          # 存储层（snapshot/WAL）
├── config.rs
├── lib.rs
├── main.rs
└── util.rs
```

---

## 二、特别关注文件 —— 存在性验证

| 文件路径 | 状态 | 备注 |
|---|---|---|
| `src/index/l2_partition.rs` | ✅ 存在 | L2 持久索引核心文件 |
| `src/index/tiered/sync.rs` | ✅ 存在 | Fast-sync / 后台重建 |
| `src/index/tiered/events.rs` | ✅ 存在 | 事件应用与 overlay 管理 |
| `src/index/tiered/compaction.rs` | ✅ 存在 | Delta 段合并 |
| `src/index/tiered/snapshot.rs` | ✅ 存在 | 快照持久化 |
| `src/index/tiered/mod.rs` | ✅ 存在 | tiered 子模块入口 |
| `src/index/mmap_index.rs` | ✅ 存在 | Mmap 冷数据层 |
| `src/event/stream.rs` | ✅ 存在 | 事件管道（EventPipeline） |
| `src/event/watcher.rs` | ✅ 存在 | inotify / notify 封装 |
| `src/event/recovery.rs` | ✅ 存在 | DirtyTracker 溢出恢复 |
| `src/core/rdd.rs` | ✅ 存在 | RDD 与 FileKey 定义 |
| `src/core/lineage.rs` | ✅ 存在 | EventRecord / FileIdentifier |
| `src/query/server.rs` | ✅ 存在 | HTTP 查询服务（非 `query_server.rs`） |

> ⚠️ 注：用户报告中提到的 `src/query/query_server.rs` 实际文件名为 **`src/query/server.rs`**。

---

## 三、Cargo.toml 依赖分析

**文件**: `Cargo.toml`（94 行 / 73 loc）

```toml
[package]
name = "fd-rdd"
version = "0.6.3"
edition = "2021"
authors = ["Piko & Aegis"]
description = "Event-driven elastic file indexer with RDD lineage"
license = "MIT"

[dependencies]
rkyv = { version = "0.7", optional = true, features = ["validation", "size_32"] }
rayon = "1.8"
crossbeam = "0.8"
arc-swap = "1.6"
tokio = { version = "1.35", features = ["rt-multi-thread", "macros", "net", "io-util", "io-std", "time", "signal", "sync", "fs"] }
axum = "0.7"
notify = "6.1"
ignore = "0.4"
memmap2 = "0.9"
roaring = "0.10"
serde = { version = "1.0", features = ["derive"] }
bincode = "1.3"
libc = "0.2"
dashmap = "5.5"
parking_lot = "0.12"
sysinfo = "0.30"
num_cpus = "1.16"
clap = { version = "4.4", features = ["derive"] }
fuzzy-matcher = "0.3"
wildmatch = "2.1"
regex = "1"
anyhow = "1.0"
toml = "0.8"
xxhash-rust = { version = "0.8", features = ["xxh3"] }
thiserror = "1.0"
```

**依赖分类**：
- **并行计算**：`rayon`, `crossbeam`
- **索引原子切换**：`arc-swap`
- **异步运行时**：`tokio`
- **Web 接口**：`axum`
- **文件系统事件**：`notify`
- **扫描引擎**：`ignore`
- **内存映射**：`memmap2`
- **位图压缩**：`roaring`
- **序列化**：`serde`, `bincode`, `rkyv`(optional)
- **内存结构**：`dashmap`, `parking_lot`
- **系统信息**：`sysinfo`, `num_cpus`
- **CLI/匹配**：`clap`, `fuzzy-matcher`, `wildmatch`, `regex`
- **错误处理**：`anyhow`, `thiserror`

---

## 四、关键文件详细分析

### 4.1 `src/core/mod.rs`（6 行）

**模块声明**：
```rust
pub mod adaptive;
pub mod lineage;
pub mod partition;
pub mod rdd;
```

**公开重导出（pub use）**：
- `AdaptiveScheduler`, `ExecutionStrategy`, `Task` ← adaptive
- `EventRecord`, `EventType`, `FileIdentifier` ← lineage
- `BuildLineage`, `BuildRDD`, `FileKey`, `FileKeyEntry`, `FileMeta`, `FsScanRDD`, `Partition` ← rdd

---

### 4.2 `src/core/rdd.rs`（428 行 / 379 loc）

**主要结构体 / 特征**：

| 定义 | 类型 | 行号（约） |
|---|---|---|
| `FileKey` | `struct` | ~37 |
| `FileMeta` | `struct` | （依赖 core/mod.rs 的 pub use） |
| `BuildRDD` | `trait` | （推断存在，被 mod.rs 导出） |
| `BuildLineage` | `trait` | （推断存在，被 mod.rs 导出） |
| `FileKeyEntry` | `struct` | （被 mod.rs 导出，rkyv 归档用） |

**FileKey 字段**（行 37-45）：
```rust
pub struct FileKey {
    pub dev: u64,
    pub ino: u64,
    pub generation: u32,   // ext4 i_generation，用于 inode 复用区分
}
```

**主要常量**：
- `FS_IOC_GETVERSION: libc::c_ulong = 0x8008_7601`（Linux-only，获取文件 generation）

---

### 4.3 `src/core/lineage.rs`（78 行 / 70 loc）

**主要枚举 / 结构体**：

| 定义 | 类型 | 行号（约） |
|---|---|---|
| `FileIdentifier` | `enum` | ~13 |
| `EventType` | `enum` | ~34 |
| `EventRecord` | `struct` | ~47 |

**FileIdentifier**（行 13-18）：
```rust
pub enum FileIdentifier {
    Path(PathBuf),
    Fid { dev: u64, ino: u64 },
}
```

**EventType**（行 34-43）：
```rust
pub enum EventType {
    Create,
    Delete,
    Modify,
    Rename { from: FileIdentifier, from_path_hint: Option<PathBuf> },
}
```

**EventRecord**（行 47-62）：
```rust
pub struct EventRecord {
    pub seq: u64,
    pub timestamp: std::time::SystemTime,
    pub event_type: EventType,
    pub id: FileIdentifier,
    pub path_hint: Option<PathBuf>,
}
```

---

### 4.4 `src/event/stream.rs`（~420 行）

**主要结构体**：

| 定义 | 类型 | 说明 |
|---|---|---|
| `EventPipeline` | `struct` | 事件管道：bounded channel + debounce + 批量应用 |
| `MergeScratch` | `struct` (private) | 合并去重临时缓冲区 |
| `MergedEvent` | `struct` (private) | 合并后事件 |

**EventPipeline 字段**（关键）：
- `index: Arc<TieredIndex>`
- `debounce_ms: u64`
- `channel_size: usize`
- `ignore_paths: Vec<PathBuf>`
- `ignore_filter: Option<IgnoreFilter>`
- 多个 `Arc<AtomicU64>` 统计计数器

**主要常量**：
- `PENDING_MOVE_TIMEOUT: Duration = Duration::from_secs(10)` — 跨批次 Rename 配对超时

---

### 4.5 `src/event/watcher.rs`（~220 行）

**主要结构体 / 类型**：

| 定义 | 类型 | 说明 |
|---|---|---|
| `EventWatcher` | `struct` | 文件系统事件监听器 |
| `WatcherBundle` | `type` | 监听器启动返回的五元组 `(priority_rx, normal_rx, priority_tx, normal_tx, watcher)` |

**主要函数**：
- `check_inotify_limit(root_count: usize) -> Option<u64>` — 检查 Linux inotify watch 上限
- `watch_roots(...)` — 注册监听路径
- `watch_roots_enhanced(...)` — 增强版，返回 `(failed_roots, degraded_roots)`

---

### 4.6 `src/event/recovery.rs`（~170 行）

**主要结构体 / 枚举**：

| 定义 | 类型 | 说明 |
|---|---|---|
| `DirtyScope` | `enum` | 脏区域范围：`All { cutoff_ns }` / `Dirs { cutoff_ns, dirs }` |
| `DirtyTracker` | `struct` | overflow 兜底：把"丢事件"转成 dirty region |

**DirtyTracker 字段**：
- `max_dirty_dirs: usize`
- `roots: Vec<PathBuf>`
- `state: Mutex<DirtyState>`
- `first_dirty_ns`, `last_activity_ns`, `sync_in_progress`, `last_sync_ns`（Atomic）

---

### 4.7 `src/index/mod.rs`（~20 行）

**模块声明**：
```rust
pub mod content_filter;
pub mod l1_cache;
pub mod l2_partition;
pub mod l3_cold;
pub mod mmap_index;
pub mod tiered;
```

**Trait 定义**：
```rust
pub trait IndexLayer: Send + Sync {
    fn query_keys(&self, matcher: &dyn Matcher) -> Vec<FileKey>;
    fn get_meta(&self, key: FileKey) -> Option<FileMeta>;
    fn file_count_estimate(&self) -> usize { 0 }
}
```

**公开重导出**：
- `L1Cache`, `PersistentIndex`, `IndexSnapshotV2/3/4/5`, `IndexBuilder`, `MmapIndex`, `TieredIndex`

---

### 4.8 `src/index/l2_partition.rs`（~900+ 行）

**主要类型 / 结构体**：

| 定义 | 类型 | 说明 |
|---|---|---|
| `DocId` | `type` | `pub type DocId = u64` |
| `Trigram` | `type` (private) | `[u8; 3]` |
| `CompactMeta` | `struct` | v5+ 紧凑元数据（含 root_id） |
| `CompactMetaV4` | `struct` | v4 旧格式（绝对路径） |
| `IndexSnapshotV2/V3/V4/V5` | `struct` | 各版本快照格式 |
| `V6Segments` | `struct` | v6 段式导出（mmap 用） |
| `PersistentIndex` | `struct` | **L2 核心索引** |

**PersistentIndex 字段**（行 ~195-220）：
```rust
pub struct PersistentIndex {
    roots: Vec<PathBuf>,
    roots_bytes: Vec<Vec<u8>>,
    metas: RwLock<Vec<CompactMeta>>,
    filekey_to_docid: RwLock<HashMap<FileKey, DocId>>,
    arena: RwLock<PathArena>,
    path_hash_to_id: RwLock<HashMap<u64, OneOrManyDocId>>,
    trigram_index: RwLock<HashMap<Trigram, RoaringTreemap>>,
    short_component_index: RwLock<HashMap<u16, RoaringTreemap>>,
    tombstones: RwLock<RoaringTreemap>,
    upsert_lock: RwLock<()>,
    dirty: AtomicBool,
}
```

---

### 4.9 `src/index/mmap_index.rs`（~600+ 行）

**主要结构体**：

| 定义 | 类型 | 说明 |
|---|---|---|
| `MmapIndex` | `struct` | Mmap 冷数据层查询 |

**MmapIndex 字段**：
```rust
pub struct MmapIndex {
    snap: Arc<MmapSnapshotV6>,
    tomb_cache: Mutex<Option<RoaringTreemap>>,
    filekey_map_cache: Mutex<Option<Arc<Vec<u8>>>>,
    short_component_cache: ShortComponentCache,
    #[cfg(feature = "rkyv")]
    validated_rkyv: OnceLock<anyhow::Result<()>>,
}
```

**主要常量**（v6 格式）：
- `META_REC_SIZE: usize = 40` — MetaRecordV6 定长
- `TRI_REC_SIZE: usize = 12` — TrigramEntryV6 定长
- `FILEKEY_MAP_REC_SIZE: usize = 24` — FileKeyMap 新格式
- `FILEKEY_MAP_REC_SIZE_OLD: usize = 20` — FileKeyMap 旧格式
- `FKM_MAGIC: [u8; 4] = *b"FKM\0"`
- `FKM_HDR_SIZE: usize = 8`
- `FKM_FLAG_LEGACY: u16 = 0`
- `FKM_FLAG_RKYV: u16 = 1`

---

### 4.10 `src/index/tiered/mod.rs`（94 行 / 83 loc）

**模块声明**（公开 / 私有混合）：
```rust
pub(crate) mod arena;
mod compaction;
mod disk_layer;
pub(crate) mod events;
mod load;
mod memory;
mod query;
mod query_plan;
pub(crate) mod rebuild;
mod snapshot;
pub(crate) mod sync;
#[cfg(test)]
mod tests;
```

**主要结构体**：

| 定义 | 类型 | 说明 |
|---|---|---|
| `TieredIndex` | `struct` | **三级索引总入口** |

**TieredIndex 字段**（行 ~70-94，部分）：
```rust
pub struct TieredIndex {
    pub l1: L1Cache,
    pub l2: ArcSwap<PersistentIndex>,
    pub(crate) disk_layers: RwLock<Vec<DiskLayer>>,
    pub l3: IndexBuilder,
    pub(crate) scheduler: Mutex<AdaptiveScheduler>,
    pub roots: Vec<PathBuf>,
    // ... 其余字段（overlay, wal, pending, flush 控制等）
}
```

**主要常量**：
| 常量 | 值 | 说明 |
|---|---|---|
| `REBUILD_COOLDOWN` | `Duration::from_secs(60)` | 重建冷却 |
| `COMPACTION_DELTA_THRESHOLD` | `8` | delta 段合并阈值 |
| `COMPACTION_MAX_DELTAS_PER_RUN` | `4` | 单次合并最大 delta 数 |
| `COMPACTION_COOLDOWN` | `Duration::from_secs(300)` | 合并冷却 |

---

### 4.11 `src/index/tiered/events.rs`（~300+ 行）

**主要结构体**：

| 定义 | 类型 | 说明 |
|---|---|---|
| `ApplyBatchState<'a>` | `struct` | 批量应用状态（含 gate 读锁） |
| `OverlayState` | `struct` | overlay 管理：deleted_paths + upserted_paths |

**TieredIndex 主要方法**：
- `apply_events(&self, events: &[EventRecord])`
- `apply_events_drain(&self, events: &mut Vec<EventRecord>)`
- `set_auto_flush_limits(...)`
- `set_periodic_flush_batch_limits(...)`
- `begin_apply_batch(...)` — 获取 apply gate + WAL + overlay + L1 失效
- `update_overlay_for_events(...)` — 更新 overlay 状态
- `invalidate_l1_for_events(...)` — 使 L1 缓存失效

---

### 4.12 `src/index/tiered/sync.rs`（~350+ 行）

**主要结构体**：

| 定义 | 类型 | 说明 |
|---|---|---|
| `FastSyncReport` | `struct` | fast-sync 结果统计 |

**FastSyncReport 字段**：
```rust
pub(crate) struct FastSyncReport {
    pub(crate) dirs_scanned: usize,
    pub(crate) upsert_events: usize,
    pub(crate) delete_events: usize,
}
```

**TieredIndex 主要方法**：
- `startup_reconcile(...)` — 启动离线补偿
- `spawn_full_build(...)` — 后台全量构建
- `spawn_rebuild(...)` — 后台重建
- `spawn_fast_sync(...)` — 溢出兜底 fast-sync
- `fast_sync(...)` — 执行 fast-sync
- `scan_dirs_immediate(...)` / `scan_dirs_immediate_deep(...)` — 即时扫描

---

### 4.13 `src/query/server.rs`（~250+ 行）

**主要结构体 / 配置**：

| 定义 | 类型 | 说明 |
|---|---|---|
| `QueryServer` | `struct` | HTTP 查询服务入口 |
| `QueryServerConfig` | `struct` (private) | 服务端配置 |
| `QueryServerState` | `struct` (private) | Axum 状态 |
| `HealthTelemetry` | `struct` | 健康度遥测数据 |
| `SearchParams` | `struct` (Deserialize) | /search 查询参数 |
| `SearchResult` | `struct` (Serialize) | 搜索结果 |
| `ScanParams` | `struct` (Deserialize) | /scan 参数 |
| `ScanResponse` | `struct` (Serialize) | 扫描响应 |
| `StatusResponse` | `struct` (Serialize) | /status 响应 |
| `HealthResponse` | `struct` (Serialize) | /health 响应 |

**主要常量**：
| 常量 | 值 | 说明 |
|---|---|---|
| `DEFAULT_SEARCH_LIMIT` | `100` | 默认搜索返回数 |
| `MAX_SEARCH_LIMIT` | `10_000` | 最大搜索返回数 |
| `SEARCH_TIMEOUT` | `Duration::from_secs(5)` | 查询超时 |

**HTTP 路由**：
- `GET /search` — search_handler
- `GET /status` — status_handler
- `GET /health` — health_handler
- `POST /scan` — scan_handler

---

## 五、模块依赖关系图

```
main.rs / lib.rs
    ├── core/
    │   ├── rdd.rs ─────── FileKey, FileMeta, BuildRDD, BuildLineage, FsScanRDD, Partition, FileKeyEntry
    │   ├── lineage.rs ─── FileIdentifier, EventType, EventRecord
    │   ├── adaptive.rs ── AdaptiveScheduler, ExecutionStrategy, Task
    │   ├── dag.rs
    │   └── partition.rs
    ├── event/
    │   ├── watcher.rs ─── EventWatcher, WatcherBundle
    │   ├── stream.rs ──── EventPipeline
    │   ├── recovery.rs ── DirtyTracker, DirtyScope
    │   ├── ignore_filter.rs
    │   └── verify.rs
    ├── index/
    │   ├── mod.rs ─────── IndexLayer (trait)
    │   ├── l1_cache.rs ── L1Cache
    │   ├── l2_partition.rs ── PersistentIndex, DocId, CompactMeta, V6Segments
    │   ├── l3_cold.rs ─── IndexBuilder
    │   ├── mmap_index.rs ── MmapIndex
    │   └── tiered/
    │       ├── mod.rs ─── TieredIndex（三级索引总入口）
    │       ├── events.rs ── ApplyBatchState, OverlayState
    │       ├── sync.rs ─── FastSyncReport
    │       ├── rebuild.rs
    │       ├── snapshot.rs
    │       ├── compaction.rs
    │       ├── disk_layer.rs
    │       ├── query.rs
    │       ├── query_plan.rs
    │       ├── arena.rs
    │       ├── memory.rs
    │       ├── load.rs
    │       └── tests.rs
    ├── query/
    │   ├── server.rs ──── QueryServer, HealthTelemetry
    │   ├── matcher.rs
    │   ├── scoring.rs
    │   ├── fzf.rs
    │   ├── dsl.rs
    │   ├── socket.rs
    │   └── mod.rs
    ├── storage/
    ├── stats/
    ├── config.rs
    └── util.rs
```

---

## 六、源码位置索引准确性确认

| 索引项 | 实际路径 | 准确性 |
|---|---|---|
| `src/index/l2_partition.rs` | ✅ `src/index/l2_partition.rs` | **准确** |
| `src/index/tiered/sync.rs` | ✅ `src/index/tiered/sync.rs` | **准确** |
| `src/index/tiered/events.rs` | ✅ `src/index/tiered/events.rs` | **准确** |
| `src/index/tiered/compaction.rs` | ✅ `src/index/tiered/compaction.rs` | **准确** |
| `src/index/tiered/snapshot.rs` | ✅ `src/index/tiered/snapshot.rs` | **准确** |
| `src/index/tiered/mod.rs` | ✅ `src/index/tiered/mod.rs` | **准确** |
| `src/index/mmap_index.rs` | ✅ `src/index/mmap_index.rs` | **准确** |
| `src/event/stream.rs` | ✅ `src/event/stream.rs` | **准确** |
| `src/event/watcher.rs` | ✅ `src/event/watcher.rs` | **准确** |
| `src/event/recovery.rs` | ✅ `src/event/recovery.rs` | **准确** |
| `src/core/rdd.rs` | ✅ `src/core/rdd.rs` | **准确** |
| `src/core/lineage.rs` | ✅ `src/core/lineage.rs` | **准确** |
| `src/query/query_server.rs` | ⚠️ 实际为 `src/query/server.rs` | **路径需修正** |

---

## 七、关键数据结构汇总

### 7.1 核心枚举

```rust
// core/lineage.rs
pub enum FileIdentifier {
    Path(PathBuf),
    Fid { dev: u64, ino: u64 },
}

pub enum EventType {
    Create,
    Delete,
    Modify,
    Rename { from: FileIdentifier, from_path_hint: Option<PathBuf> },
}

// event/recovery.rs
pub enum DirtyScope {
    All { cutoff_ns: u64 },
    Dirs { cutoff_ns: u64, dirs: Vec<PathBuf> },
}
```

### 7.2 核心结构体

```rust
// core/lineage.rs
pub struct EventRecord {
    pub seq: u64,
    pub timestamp: std::time::SystemTime,
    pub event_type: EventType,
    pub id: FileIdentifier,
    pub path_hint: Option<PathBuf>,
}

// core/rdd.rs
pub struct FileKey {
    pub dev: u64,
    pub ino: u64,
    pub generation: u32,
}

// event/stream.rs
pub struct EventPipeline {
    index: Arc<TieredIndex>,
    debounce_ms: u64,
    channel_size: usize,
    ignore_paths: Vec<PathBuf>,
    ignore_filter: Option<IgnoreFilter>,
    // ... 多个 AtomicU64 统计字段
}

// index/l2_partition.rs
pub struct PersistentIndex {
    roots: Vec<PathBuf>,
    metas: RwLock<Vec<CompactMeta>>,
    filekey_to_docid: RwLock<HashMap<FileKey, DocId>>,
    arena: RwLock<PathArena>,
    trigram_index: RwLock<HashMap<Trigram, RoaringTreemap>>,
    tombstones: RwLock<RoaringTreemap>,
    // ...
}

// index/tiered/mod.rs
pub struct TieredIndex {
    pub l1: L1Cache,
    pub l2: ArcSwap<PersistentIndex>,
    pub(crate) disk_layers: RwLock<Vec<DiskLayer>>,
    pub l3: IndexBuilder,
    pub(crate) scheduler: Mutex<AdaptiveScheduler>,
    pub roots: Vec<PathBuf>,
    // ...
}

// query/server.rs
pub struct QueryServer {
    pub index: Arc<TieredIndex>,
    config: QueryServerConfig,
    health_provider: Arc<dyn Fn() -> HealthTelemetry + Send + Sync>,
}
```

### 7.3 核心 Trait

```rust
// index/mod.rs
pub trait IndexLayer: Send + Sync {
    fn query_keys(&self, matcher: &dyn Matcher) -> Vec<FileKey>;
    fn get_meta(&self, key: FileKey) -> Option<FileMeta>;
    fn file_count_estimate(&self) -> usize { 0 }
}

// core/rdd.rs（由 mod.rs 导出）
pub trait BuildRDD { ... }
pub trait BuildLineage { ... }
```

### 7.4 核心常量

| 常量 | 值 | 文件 |
|---|---|---|
| `REBUILD_COOLDOWN` | `Duration::from_secs(60)` | `tiered/mod.rs` |
| `COMPACTION_DELTA_THRESHOLD` | `8` | `tiered/mod.rs` |
| `COMPACTION_MAX_DELTAS_PER_RUN` | `4` | `tiered/mod.rs` |
| `COMPACTION_COOLDOWN` | `Duration::from_secs(300)` | `tiered/mod.rs` |
| `PENDING_MOVE_TIMEOUT` | `Duration::from_secs(10)` | `event/stream.rs` |
| `DEFAULT_SEARCH_LIMIT` | `100` | `query/server.rs` |
| `MAX_SEARCH_LIMIT` | `10_000` | `query/server.rs` |
| `SEARCH_TIMEOUT` | `Duration::from_secs(5)` | `query/server.rs` |
| `META_REC_SIZE` | `40` | `index/mmap_index.rs` |
| `TRI_REC_SIZE` | `12` | `index/mmap_index.rs` |
| `FILEKEY_MAP_REC_SIZE` | `24` | `index/mmap_index.rs` |
| `FKM_MAGIC` | `*b"FKM\0"` | `index/mmap_index.rs` |

---

*报告结束*
