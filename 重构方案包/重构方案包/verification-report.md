# 性能瓶颈描述源码级验证报告

**验证仓库**: https://github.com/awei807-wei/vcp-fd-rdd/tree/tests  
**验证日期**: 2024  
**验证方法**: 直接访问 GitHub raw 文件，逐行核对源码

---

## 1. snapshot_now 阻塞事件管线

| 项目 | 结论 |
|------|------|
| **报告描述** | `apply_gate.write()` 在 `src/index/tiered/snapshot.rs` 第 19-195 行，持有期间阻塞所有事件处理，序列化分配 ~400MB 临时内存 |
| **验证结果** | ⚠️ 部分准确 |

### 实际代码关键片段

**`src/index/tiered/snapshot.rs`**

```rust
Line 15: const MIN_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(10);

Line 19-22: pub async fn snapshot_now<S>(self: &Arc<Self>, store: Arc<S>) -> anyhow::Result<()>

Line 25-86: let result = tokio::task::spawn_blocking(move || {
Line 26:     let _wg = match idx.apply_gate.try_write() {   // <-- 注意：是 try_write()，不是 write()
Line 27:         Some(guard) => guard,
Line 28:         None => {
Line 29:             tracing::debug!("apply_gate busy, deferring snapshot");
Line 30:             return None;
Line 31:         }
Line 32:     };
...
Line 60:     let segs = delta.export_segments_v6();   // <-- 序列化发生在此处，在 try_write 守卫范围内
...
Line 86: }) // spawn_blocking 结束
```

### 偏差分析

1. **锁类型错误**: 报告写的是 `apply_gate.write()`，但源码实际使用的是 `apply_gate.try_write()`。
   - `try_write()` 是非阻塞的：如果获取不到锁，会立即返回 `None` 并推迟 snapshot，不会阻塞等待。
   - 但一旦 `try_write()` 成功，锁守卫 `_wg` 会从 **Line 26 持有到 Line 86**（spawn_blocking 闭包结束），此期间确实会阻塞其他需要 `apply_gate.write()` 的代码路径。

2. **行号偏差**: 报告说 "第 19-195 行"，实际 `spawn_blocking` 闭包持有锁的范围是 **Line 25-86**，远小于 195 行。后续 Line 88-170 是 `.await` 后的异步 IO 操作（`replace_base_v6` / `append_delta_v6`），不在锁内。

3. **~400MB 临时内存**: 无法在源码中直接验证具体数值。`export_segments_v6()` 确实会分配大量临时 `Vec`（metas_bytes、tombstones_bytes、trigram_table_bytes、postings_blob_bytes 等），但具体峰值取决于数据量，400MB 是运行时观测值。

---

## 2. compaction 双索引内存

| 项目 | 结论 |
|------|------|
| **报告描述** | `maybe_spawn_compaction` 在 `src/index/tiered/compaction.rs` 第 21-87 行，8 delta 阈值，遍历 base+4 deltas，构建新 PersistentIndex 导致双索引内存峰值 ~300MB |
| **验证结果** | ⚠️ 部分准确 |

### 实际代码关键片段

**`src/index/tiered/compaction.rs`**

```rust
Line 21: pub(super) fn maybe_spawn_compaction<S>(self: &Arc<Self>, store: Arc<S>) {
Line 25:     let mut layers = self.disk_layers.read().clone();
Line 26:     let delta_count = layers.len().saturating_sub(1);
Line 27:     if delta_count < COMPACTION_DELTA_THRESHOLD {   // 阈值 = 8（已在 mod.rs 确认）
Line 28:         return;
Line 29:     }
Line 31:     let max_layers = 1 + COMPACTION_MAX_DELTAS_PER_RUN;  // = 1 + 4 = 5
Line 32:     if layers.len() > max_layers {
Line 33:         layers.truncate(max_layers);   // 只保留 base + 最老的 4 个 delta
Line 34:     }
...
Line 52:     tokio::spawn(async move {
...
Line 67:         let result = if use_fast {
Line 68:             idx.compact_layers_fast(store, layers).await   // 进入实际合并
Line 69:         } else {
Line 70:             idx.compact_layers(store, layers).await
Line 71:         };
```

**`compact_layers` (`compaction.rs` Line 89-137)**
```rust
Line 127: let merged = PersistentIndex::new_with_roots(roots.clone());
Line 129: for layer in &layers_snapshot {
Line 130:     layer.deleted_paths.for_each_bytes(|p| { merged.mark_deleted_by_path(&pb); });
Line 134:     layer.idx.for_each_live_meta(|m| merged.upsert_rename(m));
Line 137: }
Line 137: let segs = merged.export_segments_v6_compacted();
```

**`compact_layers_fast` (`compaction.rs` Line 215-305)**
```rust
Line 253: let merged = PersistentIndex::new_with_roots(roots.clone());
Line 255: let mut final_metas: Vec<FileMeta> = Vec::new();
Line 257: let mut layer_mappings: Vec<Vec<Option<DocId>>> = Vec::with_capacity(layers_snapshot.len());
...
Line 275: let mut merged_trigrams: std::collections::HashMap<[u8; 3], roaring::RoaringTreemap> =
Line 276:     std::collections::HashMap::new();
...
Line 303: merged.fill_from_compaction(roots.clone(), final_metas, merged_trigrams);
Line 305: let segs = merged.export_segments_v6_compacted();
```

### 偏差分析

1. **行号偏差**: 报告说 "第 21-87 行" 导致双索引内存，但 `maybe_spawn_compaction` 只是一个**触发/调度函数**，真正的双索引构建发生在 `compact_layers` (**Line 127-137**) 和 `compact_layers_fast` (**Line 253-305**) 中。`compact_layers_fast` 尤其明显：它在内存中同时保留了：
   - 老的 `layers_snapshot`（base + up to 4 deltas，通过 mmap 加载的现有索引）
   - 新的 `merged` PersistentIndex（正在构建的新索引）
   - `final_metas` Vec（临时收集的活跃元数据）
   - `merged_trigrams` HashMap（临时合并的倒排表）
   - `layer_mappings` Vec（docid 映射表）

2. **~300MB 峰值**: 源码无法直接验证具体数值，但**双索引内存峰值机制确实存在**。

3. **阈值**: 8 delta 阈值 (`COMPACTION_DELTA_THRESHOLD=8`) 和 `COMPACTION_MAX_DELTAS_PER_RUN=4` 都是准确的。

---

## 3. 冷启动 syscall 链

| 项目 | 结论 |
|------|------|
| **报告描述** | `FileKey::from_path_and_metadata` 在 `src/core/rdd.rs` 第 52-93 行，每文件 open(O_PATH) + ioctl(FS_IOC_GETVERSION) + close = 3 syscall。`FsScanRDD::for_each_meta` 在 `src/core/rdd.rs` 第 200-222 行，8M 文件 × 4 syscall = 3200 万次 |
| **验证结果** | ⚠️ 部分准确 |

### 实际代码关键片段

**`src/core/rdd.rs`**

```rust
Line 25-45: pub fn get_file_generation(path: &std::path::Path) -> u32 {
Line 28:     let c_path = match CString::new(path.as_os_str().as_encoded_bytes()) { ... };
Line 32:     let fd = unsafe { libc::open(c_path.as_ptr(), libc::O_PATH | libc::O_CLOEXEC, 0) };
Line 33:     if fd < 0 { return 0; }
Line 36:     let mut generation: i32 = 0;
Line 37:     const FS_IOC_GETVERSION: libc::c_ulong = 0x8008_7601;
Line 38:     let ret = unsafe { libc::ioctl(fd, FS_IOC_GETVERSION as _, &mut generation) };
Line 39:     unsafe { libc::close(fd) };
...
Line 53-66: impl FileKey {
Line 53:     pub fn from_path_and_metadata(path: &std::path::Path, meta: &std::fs::Metadata) -> Option<Self> {
Line 58:         use std::os::unix::fs::MetadataExt;
Line 60:         let generation = get_file_generation(path);   // <-- 调用 3 syscall
Line 61:         Some(Self { dev: meta.dev(), ino: meta.ino(), generation })
```

```rust
Line 200-222: pub fn for_each_meta(&self, sink: impl Fn(FileMeta) + Send + Sync + 'static) {
Line 201:     let sink: Arc<dyn Fn(FileMeta) + Send + Sync> = Arc::new(sink);
Line 203:     if self.parallelism <= 1 {
Line 204:         for p in self.partitions() {
Line 205:             for item in self.compute(p) { sink(item); }
Line 206:         }
Line 207:         return;
Line 208:     }
Line 212:     for p in self.partitions() {
Line 213:         scan_partition_parallel(p, self.parallelism, ..., sink.clone());
Line 220:     }
Line 222: }
```

### 偏差分析

1. **syscall 数量错误**: 报告说 "每文件 × 4 syscall = 3200 万次"，但实际 `get_file_generation` 中明确只有 **3 个 syscall**：
   - `libc::open(..., O_PATH | O_CLOEXEC, 0)` — Line 32
   - `libc::ioctl(fd, FS_IOC_GETVERSION, ...)` — Line 38
   - `libc::close(fd)` — Line 39
   
   正确计算应为 **8M 文件 × 3 syscall = 2400 万次**，不是 3200 万次。

2. **行号偏差**: 报告说 `FileKey::from_path_and_metadata` 在第 52-93 行，但实际该函数主体在 **Line 53-66**（unix 路径），`get_file_generation` 在 **Line 25-45**。93 行远远超出了实际函数范围。

3. **FsScanRDD::for_each_meta 行号**: 报告说第 200-222 行，实际确实是 **Line 200-222**，✅ 准确。

---

## 4. upsert_lock 全局串行化

| 项目 | 结论 |
|------|------|
| **报告描述** | 并行扫描但串行写入，多线程无效 |
| **验证结果** | ✅ 准确 |

### 实际代码关键片段

**`src/index/l2_partition.rs`**（PersistentIndex 定义）

```rust
/// upsert 写锁：保护 alloc_docid → insert_trigrams / insert_path_hash 的原子性，
/// 防止写入-查询竞态导致 trigram 索引与 metas 不一致。
upsert_lock: RwLock<()>,
```

```rust
fn upsert_inner(&self, mut meta: FileMeta, force_path_update: bool) {
    ...
    // 新文件：分配 docid 并写入
    let _guard = self.upsert_lock.write();
    let Some(docid) = self.alloc_docid(fkey, new_root_id, &new_rel_bytes, meta.size, mtime_to_ns(meta.mtime)) else {
        return;
    };
    self.insert_trigrams(docid, meta.path.as_path());
    self.insert_path_hash(docid, meta.path.as_path());
    ...
}
```

**`src/index/l3_cold.rs`**（full_build_with_strategy）

```rust
pub fn full_build_with_strategy(&self, index: &Arc<PersistentIndex>, strategy: ExecutionStrategy) {
    let parallelism = ...; // 可以 > 1
    let rdd = FsScanRDD::from_roots(...).with_parallelism(parallelism);
    let idx = index.clone();
    rdd.for_each_meta(move |meta: FileMeta| {
        idx.upsert(meta);   // 所有并行 walker 线程竞争同一个 idx.upsert_lock.write()
        ...
    });
}
```

### 结论

- `FsScanRDD::for_each_meta` 确实使用 `ignore::WalkBuilder::build_parallel()` 进行**并行目录遍历**（`threads(parallelism)`）。
- 但 sink 闭包中的 `idx.upsert(meta)` 最终调用 `PersistentIndex::upsert_inner()`，该函数在**新文件插入路径**和**rename 路径更新路径**上都会获取 `upsert_lock.write()`。
- 由于 `upsert_lock` 是一个 `RwLock<()>` 的**写锁**，所有并行 walker 线程在该锁上**完全串行化**。
- 因此："并行扫描但串行写入，多线程无效" 的描述 **完全准确**。

---

## 5. main.rs 启动流程

| 项目 | 结论 |
|------|------|
| **报告描述** | `spawn_full_build` 在 `src/main.rs` 第 197-200 行，`startup_reconcile` 在 `src/main.rs` 第 232-234 行 |
| **验证结果** | ✅ 准确 |

### 实际代码关键片段

**`src/main.rs`**

```rust
Line 197: // 4) 若索引为空，后台全量构建
Line 198: if index.file_count() == 0 && !args.no_build {
Line 199:     index.spawn_full_build();
Line 200: }
...
Line 230: // 5.5) 启动阶段 best-effort 补偿停机期间的离线变更。
Line 231: // 仅在已有索引内容时执行，避免与空索引冷启动 full_build 重复做全量工作。
Line 232: if index.file_count() > 0 {
Line 233:     let _ = index.startup_reconcile(&startup_ignore_paths);
Line 234: }
```

### 结论

- 行号基本准确（报告说 197-200 和 232-234，实际确实是这些行号范围）。
- `spawn_full_build` 在索引为空时启动，调用 `l3_cold.rs` 的 `full_build_with_strategy`（即上面验证的全局串行化路径）。
- `startup_reconcile` 在索引非空时启动，做离线变更补偿。

---

## 6. TieredIndex 常量

| 项目 | 结论 |
|------|------|
| **报告描述** | `REBUILD_COOLDOWN=60s`, `COMPACTION_DELTA_THRESHOLD=8`, `COMPACTION_MAX_DELTAS_PER_RUN=4`, `COMPACTION_COOLDOWN=300s` 在 `src/index/tiered/mod.rs` 第 35-41 行 |
| **验证结果** | ✅ 准确 |

### 实际代码关键片段

**`src/index/tiered/mod.rs`**

```rust
Line 35: const REBUILD_COOLDOWN: Duration = Duration::from_secs(60);
Line 37: const COMPACTION_DELTA_THRESHOLD: usize = 8;
Line 39: const COMPACTION_MAX_DELTAS_PER_RUN: usize = 4;
Line 41: const COMPACTION_COOLDOWN: Duration = Duration::from_secs(300);
```

### 结论

全部常量名称、数值、行号完全准确。

---

## 7. MIN_SNAPSHOT_INTERVAL

| 项目 | 结论 |
|------|------|
| **报告描述** | `MIN_SNAPSHOT_INTERVAL=10s` 在 `src/index/tiered/snapshot.rs` 第 15 行 |
| **验证结果** | ✅ 准确 |

### 实际代码关键片段

**`src/index/tiered/snapshot.rs`**

```rust
Line 15: const MIN_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(10);
```

### 结论

名称、数值、行号完全准确。

---

## 总体验证结论

| # | 验证点 | 结论 | 关键偏差 |
|---|--------|------|----------|
| 1 | snapshot_now 阻塞 + 序列化 | ⚠️ 部分准确 | `try_write()` 而非 `write()`；行号范围偏大；~400MB 不可源码验证 |
| 2 | compaction 双索引内存 | ⚠️ 部分准确 | 双索引机制正确，但 "第21-87行" 是调度函数，实际构建在 compact_layers/compact_layers_fast 中 |
| 3 | 冷启动 syscall 链 | ⚠️ 部分准确 | 3 syscall/文件（非4个），8M×3=24M次（非32M次） |
| 4 | upsert_lock 全局串行化 | ✅ 准确 | 完全验证：PersistentIndex.upsert_lock.write() 使并行 walker 串行化 |
| 5 | main.rs 启动流程 | ✅ 准确 | 行号基本吻合 |
| 6 | TieredIndex 常量 | ✅ 准确 | 全部正确 |
| 7 | MIN_SNAPSHOT_INTERVAL | ✅ 准确 | 完全正确 |

### 核心发现

1. **最严重的已确认瓶颈**: `upsert_lock` 全局写锁（验证点4）。`full_build_with_strategy` 使用并行 walker，但所有工作线程在 `PersistentIndex.upsert_lock.write()` 上完全串行。这是一个**设计级瓶颈**，源码级已完全证实。

2. **syscall 开销被高估**: 报告将每文件的 syscall 数算成 4 个，实际 `get_file_generation` 中只有 3 个（open, ioctl, close）。对于 8M 文件，这相差 **800 万次 syscall**。

3. **snapshot 锁机制已优化**: 从 `write()` 改为 `try_write()` 是一个重要的防饥饿改进（GitHub commit 6e04b18 的 message 也证实了这一点："fix: use try_write for apply_gate to avoid writer-priority starvation"），报告仍然描述为 `write()` 是不准确的。

4. **compaction 双索引确实存在**: `compact_layers_fast` 在内存中同时保留老 layers + 新 `merged` + `final_metas` + `merged_trigrams`，双索引内存峰值机制准确。
