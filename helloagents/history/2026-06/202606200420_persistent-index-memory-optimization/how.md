# PersistentIndex 内存优化方案设计

> 状态：待实施。详细调查见 `helloagents/wiki/m2-memory-investigation-20260620.md`。

## 总体策略

分四个阶段逐步消除 PersistentIndex 中的路径冗余存储和索引结构浪费。每阶段独立可合并，互不阻塞（P1 和 P2 有依赖关系除外）。

- **Phase 1 (P3)**：trigram RoaringTreemap→RoaringBitmap——最低风险，先做
- **Phase 2 (P1)**：运行时引入 PathArena——最大收益，中等风险
- **Phase 3 (P2)**：消除 parent_path_table 重复——依赖 P1，最大单项节省
- **Phase 4 (P4)**：filekey_to_docid 改排序 Vec——独立优化

---

## Phase 1：trigram RoaringTreemap → RoaringBitmap

**位置**：`src/index/l2_partition.rs`

**现状**：

```rust
// line 2
use roaring::RoaringTreemap;

// line 422
trigram_index: RwLock<HashMap<Trigram, RoaringTreemap>>,

// line 425
tombstones: RwLock<RoaringTreemap>,
```

DocId 在运行时按 `entries.len() as DocId` 顺序分配（line 797），1M 文件远小于 2³²。v6 快照序列化时已经 downcast 到 u32（line 1383: `posting.iter().map(|v| v as u32).collect()`）。

**修改**：

1. 将 `trigram_index` 和 `tombstones` 的 `RoaringTreemap` 替换为 `RoaringBitmap`
2. 所有写入点（`insert_trigrams`、`remove_trigrams`、`mark_deleted`）的 DocId 从 `u64` 改为 `as u32`
3. 所有查询点（`trigram_candidates`、`tombstones.contains`）同步改
4. 保持 DocId 类型定义为 `u64` 不变（避免全项目改型），只在 trigram/tombstone 边界做 `as u32` 转换

**涉及代码位置**：
- line 2: import
- line 422, 425: 字段定义
- line 512, 515: 初始化
- line 660-664: `insert_trigrams()`
- line 670-675: `remove_trigrams()`
- line 813-816: `mark_deleted()` tombstone
- line 988-992: rebuild trigram
- line 1366-1367: v6 序列化（已有 u32 转换，简化）
- line 1998-2005: `rebuild_derived_indexes` trigram
- line 2047-2073: `trigram_candidates()` 查询

**内存节省**：~50 MB（100 MB → 50 MB）

**风险**：低。机械替换。如果索引文件超过 2³²（~42 亿），DocId 会溢出 u32，但这是不现实的场景。

---

## Phase 2：运行时引入 PathArena

**位置**：`src/index/l2_partition.rs`

**现状**：

```rust
// line 411
paths: RwLock<Vec<Vec<u8>>>,  // 每个文件一个独立堆分配的 Vec<u8>
```

100 万个 `Vec<u8>`，每个 24B header + 60B 路径 + 对齐 = ~84 MB，外加 mimalloc 碎片 ~30 MB。

PathArena（line 92-157）已经存在，提供连续字节存储 + 偏移量访问，但只用于快照格式。

**修改**：

### 步骤 1：给 PersistentIndex 添加 arena 字段

```rust
pub struct PersistentIndex {
    roots: Vec<PathBuf>,
    roots_bytes: Vec<Vec<u8>>,
    entries: RwLock<Vec<FileEntry>>,
    // 替换 paths: RwLock<Vec<Vec<u8>>>
    path_arena: RwLock<PathArena>,       // 连续字节存储
    path_offsets: RwLock<Vec<PathOffset>>, // DocId -> (off, len)
    filekey_to_docid: RwLock<HashMap<FileKey, DocId>>,
    // ... 其余不变
}

struct PathOffset {
    off: u32,
    len: u16,
}
```

### 步骤 2：修改写入路径

- `alloc_docid()` (line 802)：`self.path_arena.write().push_bytes(abs_path_bytes)` → 记录 (off, len)
- `from_snapshot_v5()` (line 555)：直接保留快照的 PathArena，不再展开为 `Vec<Vec<u8>>`
- `update_entry_path()` (line 1901)：arena append 新路径，旧路径变 dead space（快照保存时回收）

### 步骤 3：修改读取路径（15 处）

所有 `paths.get(docid as usize)` 改为通过 `path_offsets` 取 (off, len) 再从 `path_arena.get_bytes(off, len)` 读取。

关键读取点：
- `query()` (line 862)：trigram 候选过滤——读路径字节给 matcher
- `for_each_live_meta()` (line 914)：flush/compaction 遍历
- `rebuild_parent_index()` (line 960)：构建父目录索引
- `lookup_docid_by_path()` (line 2038)：hash 冲突验证
- `get_meta()` (line 2249)：IndexLayer trait 返回 FileMeta
- `build_legacy_metas()` (line 1956)：快照序列化（不再需要临时 PathArena）

### 步骤 4：快照保存简化

`build_legacy_metas()` (line 1954) 当前每次保存快照时临时构建 PathArena。引入运行时 PathArena 后，直接序列化已有的 arena，省掉临时构建开销。

**内存节省**：~48 MB（84 MB → 66 MB，外加碎片消除 ~30 MB）

**风险**：中。涉及 15 个读取点和一个写入路径。rename 导致 arena dead space，但快照保存时自然回收。需要确保 arena 在并发读写下安全（已有 `RwLock`）。

---

## Phase 3：消除 parent_path_table 重复

**位置**：`src/index/l2_partition.rs:441-493`

**现状**：

```rust
struct RebuildPathTable {
    path_to_id: HashMap<Vec<u8>, u32>,  // ← 复制全部路径字节
    id_to_path: Vec<Vec<u8>>,            // ← 再次复制全部路径字节
    dirs: HashSet<u32>,
}
```

`rebuild_parent_index()` (line 956) 调用时，把所有 1M 路径 intern 进去，造成 ~168 MB 临时内存峰值。

**修改**：

依赖 Phase 2 的 PathArena。将 `RebuildPathTable` 改为存储 arena 偏移量：

```rust
struct RebuildPathTable {
    // path hash -> DocId（不再存 Vec<u8> key）
    path_hash_to_id: HashMap<u64, u32>,
    // DocId -> (arena_off, arena_len)，引用 Phase 2 的 PathArena
    id_to_path_offset: Vec<PathOffset>,
    dirs: HashSet<u32>,
}
```

`intern()` 方法不再 clone 路径字节，而是计算 hash 并记录偏移量。

**内存节省**：~168 MB（从 168 MB 临时峰值降到 ~16 MB 偏移量数组）

**风险**：中。依赖 Phase 2。`intern()` 的 hash 计算需要与 `path_hash_to_id` 的 hash 函数一致。rebuild 期间 arena 不变（持读锁），偏移量有效。

---

## Phase 4：filekey_to_docid 改排序 Vec

**位置**：`src/index/l2_partition.rs:416`

**现状**：

```rust
filekey_to_docid: RwLock<HashMap<FileKey, DocId>>,
```

FileKey 20B + DocId 8B + HashMap entry 开销 = ~56 MB for 1M files。

**修改**：

```rust
// 替换为排序 Vec
filekey_to_docid: RwLock<Vec<(FileKey, DocId)>>,  // 按 (dev, ino, gen) 排序
```

- 写入：upsert 时插入并保持排序（对小批量用 `slice::binary_search` + `insert`）
- 读取：二分查找 O(log N)，1M 条目约 20 次比较
- 批量 rebuild：先收集全部，排序后一次性写入

v6 快照已经序列化排序的 FileKeyMap（line 1435），可以直接加载为排序 Vec。

**涉及代码位置**：line 416（字段）、line 631-646（clear/rebuild）、line 690/711（upsert）、line 719（rename 查找）、line 775/804（delete）、line 1436（序列化）、line 1583（反序列化）、line 1920-1938（memory_stats）、line 2244（get_meta by FileKey）

**内存节省**：~28 MB（56 MB → 28 MB）

**风险**：中。upsert 的 `insert + shift` 在大 Vec 上是 O(N)，但 event 批量处理时可以先攒一批再排序插入。查询性能从 O(1) 变 O(log N)，对 event 处理可接受。

---

## 不修改的部分

- `path_hash_to_id: HashMap<u64, OneOrManyDocId>`——保留。它用于路径→DocId 反查，与 filekey_to_docid 功能不同（路径 vs inode）。可考虑后续换 FxHashMap 减少开销，但不在本方案范围。
- `entries: Vec<FileEntry>`——已经是 32B 紧凑布局，无需改动。
- DocId 类型保持 `u64`——全项目改型 blast radius 太大，只在 trigram/tombstone 边界做 `as u32`。
- mimalloc 保留为全局分配器——Phase 1-3 消除大量小分配后碎片自然降低。

---

## 收益汇总

| Phase | 优化项 | 估算节省 | 累计节省 | 改动文件 |
|---|---|---|---|---|
| P1 | trigram RoaringBitmap | 50 MB | 50 MB | l2_partition.rs |
| P2 | 运行时 PathArena | 48 MB | 98 MB | l2_partition.rs |
| P3 | parent_path_table 去重 | 168 MB | 266 MB | l2_partition.rs |
| P4 | filekey_to_docid 排序 Vec | 28 MB | 294 MB | l2_partition.rs |
| — | 碎片自然降低（P2/P3 副产品） | ~50 MB | 344 MB | — |
| **合计** | | **344 MB** | | |

**预期 RSS**：760 MB - 344 MB = **~416 MB**（1M 文件）

剩余 ~216 MB 与 200 MB 目标的差距主要来自 `path_hash_to_id` HashMap（48 MB）、parent_index 结构、运行时开销（线程栈、代码段等 ~30 MB）。这些留作后续优化。

---

## 风险与验证

### 风险

1. **Phase 2 PathArena 并发安全**：arena 写入（upsert/rename）和读取（query）并发。已有 `RwLock` 保护，但需确认 arena append 不导致 realloc 时读端看到不一致数据。方案：arena 用 `Arc<Vec<u8>>`，写入时 `Arc::make_mut`（COW），读端持有旧 Arc 不受影响。

2. **Phase 3 rebuild 期间 arena 一致性**：`rebuild_parent_index()` 持 arena 读锁，期间不能有 upsert。当前已有 `upsert_lock` 保护，rebuild 在 dirty queue 处理线程中执行，与 upsert 串行。

3. **Phase 4 排序 Vec 插入性能**：单条 upsert 的 `Vec::insert` 是 O(N)。缓解：event 批量处理时先攒一批 `(FileKey, DocId)`，排序后 merge insert。或者用 `BTreeMap` 作为 staging，定期 merge 到排序 Vec。

4. **快照兼容性**：v4/v5/v6/v7 快照格式不变。运行时 PathArena 只影响内存结构，序列化时仍用现有格式（v6 segments / v7 mmap）。

### 验证

见 task.md 测试项。

---

## 实施顺序建议

```
Phase 1 (trigram bitmap)     ← 独立，先做，低风险
    ↓
Phase 2 (runtime PathArena)  ← 核心，中等风险
    ↓
Phase 3 (parent table dedup) ← 依赖 Phase 2
    ↓
Phase 4 (filekey sorted Vec) ← 独立，可与 Phase 2 并行
```

每个 Phase 独立提交、独立测试、独立可回滚。
