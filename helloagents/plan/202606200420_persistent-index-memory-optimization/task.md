# PersistentIndex 内存优化任务清单

> 状态：待实施。分四个 Phase，每 Phase 独立可合并。

## Phase 1：trigram RoaringTreemap → RoaringBitmap

### 实施任务

- [ ] 修改 `src/index/l2_partition.rs` import：添加 `use roaring::RoaringBitmap`
- [ ] 修改 `trigram_index` 字段类型：`HashMap<Trigram, RoaringTreemap>` → `HashMap<Trigram, RoaringBitmap>`
- [ ] 修改 `tombstones` 字段类型：`RoaringTreemap` → `RoaringBitmap`
- [ ] 修改 `insert_trigrams()`：DocId `as u32` 后插入 RoaringBitmap
- [ ] 修改 `remove_trigrams()`：DocId `as u32` 后移除
- [ ] 修改 `mark_deleted()`：tombstone 用 `as u32`
- [ ] 修改 `trigram_candidates()`：返回 `RoaringBitmap`，交集操作适配
- [ ] 修改 `rebuild_derived_indexes()`：rebuild trigram 用 RoaringBitmap
- [ ] 修改 v6 序列化：去掉多余的 `as u32` 转换（已经是 u32）
- [ ] 修改 `memory_stats()`：估算用 RoaringBitmap 的 `serialized_size`

### 测试项

- [ ] `cargo test -q trigram` 全通过
- [ ] `cargo test -q tombstone` 全通过
- [ ] `cargo test -q` 无新增失败
- [ ] 查询结果正确性验证：trigram 候选集与修改前一致

### 验收标准

- [ ] `cargo check` 通过
- [ ] `cargo test` 全通过
- [ ] RSS 对比：1M 文件下 trigram 相关内存下降 ~50 MB

---

## Phase 2：运行时引入 PathArena

### 实施任务

- [ ] 定义 `PathOffset { off: u32, len: u16 }` 结构
- [ ] 给 `PersistentIndex` 添加 `path_arena: RwLock<PathArena>` 和 `path_offsets: RwLock<Vec<PathOffset>>`
- [ ] 移除 `paths: RwLock<Vec<Vec<u8>>>` 字段
- [ ] 修改 `alloc_docid()`：用 `path_arena.write().push_bytes()` + 记录 offset
- [ ] 修改 `from_snapshot_v5()`：直接保留快照 PathArena，不再展开为 Vec<Vec<u8>>
- [ ] 修改 `update_entry_path()`：arena append 新路径
- [ ] 修改 15 个读取点（见 how.md Phase 2 步骤 3）：`paths.get(docid)` → `arena.get_bytes(off, len)`
- [ ] 修改 `build_legacy_metas()`：直接序列化已有 arena，不再临时构建
- [ ] 修改 `memory_stats()`：路径估算改用 arena 字节数

### 测试项

- [ ] `cargo test -q l2_partition` 全通过
- [ ] `cargo test -q persistent` 全通过
- [ ] `cargo test -q snapshot` 全通过
- [ ] `cargo test -q query` 全通过
- [ ] `cargo test -q` 无新增失败
- [ ] 路径正确性：query 结果路径与修改前完全一致
- [ ] rename 正确性：rename 后旧路径隐藏、新路径可见
- [ ] 快照保存/加载：round-trip 后路径完整

### 验收标准

- [ ] `cargo check` 通过
- [ ] `cargo test` 全通过
- [ ] RSS 对比：1M 文件下路径存储内存下降 ~48 MB
- [ ] 查询延迟无退化（p95 不超过修改前 10%）

---

## Phase 3：消除 parent_path_table 重复

### 实施任务

- [ ] 修改 `RebuildPathTable` 结构：`HashMap<Vec<u8>, u32>` → `HashMap<u64, u32>`（path hash → DocId）
- [ ] 修改 `id_to_path: Vec<Vec<u8>>` → `id_to_path_offset: Vec<PathOffset>`（引用 Phase 2 的 arena）
- [ ] 修改 `intern()` 方法：不再 clone 路径，改记 arena offset + hash
- [ ] 修改 `rebuild_parent_index()`：适配新结构
- [ ] 修改 `delete_alignment_with_parent_index()`：适配
- [ ] 修改 `memory_stats()`：parent_path_table 估算适配

### 测试项

- [ ] `cargo test -q parent` 全通过
- [ ] `cargo test -q rebuild` 全通过
- [ ] `cargo test -q directory` 全通过
- [ ] `cargo test -q` 无新增失败
- [ ] 目录查询正确性：`/path/to/dir/` 下文件列表与修改前一致

### 验收标准

- [ ] `cargo check` 通过
- [ ] `cargo test` 全通过
- [ ] rebuild 期间峰值内存下降 ~168 MB

---

## Phase 4：filekey_to_docid 改排序 Vec

### 实施任务

- [ ] 修改 `filekey_to_docid` 字段：`HashMap<FileKey, DocId>` → `Vec<(FileKey, DocId)>`
- [ ] 实现排序维护：upsert 时 `binary_search` + `insert`
- [ ] 实现 `rebuild_derived_indexes()` 中的批量排序插入
- [ ] 修改所有 `filekey_to_docid.get(&fkey)` → `binary_search`
- [ ] 修改 v6 序列化/反序列化：直接用排序 Vec
- [ ] 修改 `memory_stats()` 估算

### 测试项

- [ ] `cargo test -q filekey` 全通过
- [ ] `cargo test -q hardlink` 全通过
- [ ] `cargo test -q rename` 全通过
- [ ] `cargo test -q` 无新增失败
- [ ] hardlink 检测正确性：同 inode 多路径正确识别

### 验收标准

- [ ] `cargo check` 通过
- [ ] `cargo test` 全通过
- [ ] filekey 查找内存下降 ~28 MB
- [ ] event 处理吞吐无显著退化（batch apply 场景）

---

## 整体验收标准

- [ ] 四个 Phase 全部合并后 `cargo test` 全通过
- [ ] 1M 文件基准测试 RSS 从 ~760 MB 降到 ~416 MB 以下
- [ ] 查询正确性无退化（canary create/rename/delete 全通过）
- [ ] 查询延迟 p95 不超过修改前 10%
- [ ] 快照保存/加载 round-trip 完整
- [ ] `cargo bench`（如有）无显著退化
