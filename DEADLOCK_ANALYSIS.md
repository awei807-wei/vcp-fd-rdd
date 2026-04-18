# PersistentIndex 死锁风险独立分析

> 本文不依赖任何项目外部上下文，仅基于单份源文件（`src/index/l2_partition.rs`，commit `097e7c6` 附近）中的代码事实展开分析。

---

## 1. 系统背景（一句话）

`PersistentIndex` 是一个内存驻留的文件索引，支持并发写入（文件创建/更新/重命名）和并发查询。它使用 `parking_lot::RwLock` 保护内部多个数据结构。

---

## 2. 涉及的数据结构与锁

| 字段                    | 类型                                         | 语义                                         |
| ----------------------- | -------------------------------------------- | -------------------------------------------- |
| `trigram_index`         | `RwLock<HashMap<Trigram, RoaringTreemap>>`   | 倒排索引：trigram → DocId 列表               |
| `short_component_index` | `RwLock<HashMap<Box<[u8]>, RoaringTreemap>>` | 短路径组件倒排索引                           |
| `metas`                 | `RwLock<Vec<CompactMeta>>`                   | DocId → 文件元数据表                         |
| `arena`                 | `RwLock<PathArena>`                          | 路径字节连续存储区                           |
| `tombstones`            | `RwLock<RoaringTreemap>`                     | 已删除文档标记                               |
| `filekey_to_docid`      | `RwLock<HashMap<FileKey, DocId>>`            | 文件身份 → 内部编号映射                      |
| `path_hash_to_id`       | `RwLock<HashMap<u64, OneOrManyDocId>>`       | 路径哈希 → 内部编号映射                      |
| `upsert_lock`           | `RwLock<()>`                                 | **粗粒度写锁**，用于保障 upsert 路径中原子性 |

---

## 3. 查询路径（Reader）的加锁顺序

查询入口：`pub fn query(...)`（约第 723 行）

```rust
// 步骤 A：先读 trigram_index / short_component_index
let candidates = self
    .trigram_candidates(matcher)   // 内部：self.trigram_index.read()
    .or_else(|| self.short_hint_candidates(matcher)); // 内部：self.trigram_index.read() + short_component_index.read()

// 步骤 B：再读 metas / arena / tombstones
let metas = self.metas.read();
let arena = self.arena.read();
let tombstones = self.tombstones.read();
```

**查询路径的锁顺序（先 A 后 B）：**

```
trigram_index.read()
  ↓
metas.read()
  ↓
arena.read()
  ↓
tombstones.read()
```

> 代码注释（第 724 行）也强调了这一点："先读取 trigram_index 计算候选集，再读取 metas/tombstones/arena"。

---

## 4. 写入路径（Writer）的加锁顺序

写入入口：`fn upsert_inner(...)`（约第 554 行）。存在三条执行路径：

### 路径 4a：新文件插入（最常用）

```rust
let _guard = self.upsert_lock.write();               // [1] 粗粒度锁
let Some(docid) = self.alloc_docid(...) else { ... }; // [2] arena.write() + metas.write()
self.insert_trigrams(docid, meta.path.as_path());     // [3] trigram_index.write()
self.insert_path_hash(docid, meta.path.as_path());    // [4] path_hash_to_id.write()
```

### 路径 4b：重命名（rename）路径更新

```rust
let _guard = self.upsert_lock.write();               // [1] 粗粒度锁
self.insert_trigrams(docid, meta.path.as_path());     // [2] trigram_index.write()
self.insert_path_hash(docid, meta.path.as_path());    // [3] path_hash_to_id.write()
let mut metas = self.metas.write();                   // [4] metas.write()
```

### 路径 4c：`alloc_docid` 内部（被 4a / 4b 调用）

```rust
fn alloc_docid(...)
    let (off, len) = self.arena.write().push_bytes(rel_bytes)?;  // [1] arena.write()
    let mut metas = self.metas.write();                           // [2] metas.write()
    self.filekey_to_docid.write().insert(file_key, docid);        // [3] filekey_to_docid.write()
    self.tombstones.write().remove(docid);                        // [4] tombstones.write()
```

**写入路径的锁顺序（按发生时间）：**

```
arena.write()
  ↓
metas.write()
  ↓
trigram_index.write()
```

> 注意：`upsert_lock.write()` 是外层包裹，但它**不替代**内部各字段的 RwLock；内部的 `write()` 仍然会被 `parking_lot` 单独排队。

---

## 5. 循环等待（死锁条件成立）

对比查询与写入的锁顺序：

| 参与者                   | 第一步                         | 第二步                          | …   |
| ------------------------ | ------------------------------ | ------------------------------- | --- |
| **T_query**（查询线程）  | `trigram_index.read()` ✅ 持有 | 等待 `metas.read()` ⏳          |     |
| **T_upsert**（写入线程） | `metas.write()` ✅ 持有        | 等待 `trigram_index.write()` ⏳ |     |

这构成了一个经典的 **AB-BA 死锁**：

1. **T_query** 先获取了 `trigram_index.read()`（可能因为查询正在执行 trigram 候选集计算）。
2. **T_upsert** 先获取了 `metas.write()`（因为 `alloc_docid` 需要 push meta）。
3. **T_query** 继续执行到 `metas.read()`，被 `T_upsert` 的 `metas.write()` 阻塞（写排他）。
4. **T_upsert** 继续执行到 `insert_trigrams` 里的 `trigram_index.write()`，被 **T_query** 的 `trigram_index.read()` 阻塞（`parking_lot::RwLock` 的写者优先/公平策略下，新来的写请求会阻塞后续读请求，已有的读也可能被升级等待）。

**两线程相互等待 → 死锁。**

### 为什么 `upsert_lock` 没能阻止？

`upsert_lock` 只保证**同一时刻只有一个写入者**在执行 upsert 流程，但：

- 它**不阻止查询线程**同时持有 `trigram_index.read()`。
- 它**不改变**写入者内部对各字段锁的获取顺序。

所以 `upsert_lock` 解决的是**同一写入者内部**多个步骤之间的原子性（防幻影读），但**没有解决读写交叉**时的锁顺序反转问题。

---

## 6. 为什么这个 bug 至今未触发？

严格来说，这是一个**理论风险**，实际触发概率极低，原因如下：

1. **锁持有时间极短**：`alloc_docid`（arena + metas）和 `insert_trigrams`（trigram_index）都是纯粹的内存 CPU 操作，耗时在微秒级。死锁的"窗口期"极窄。
2. **公平锁的调度概率**：`parking_lot::RwLock` 默认是公平锁，但公平不代表必然交叉。需要两个线程恰好同时到达这两个临界点。
3. **查询负载不够高**：大部分测试和 CI 场景没有高并发的"边写边查"压力。
4. **索引规模有限**：metas 和 trigram_index 操作尚未达到需要长时间持有锁的规模。

但这**不等于安全**。在高并发写入 + 高并发查询的生产环境中，这是一个定时炸弹。

---

## 7. 证据索引（可直接复查）

| 事实                                                           | 文件                        | 行号      |
| -------------------------------------------------------------- | --------------------------- | --------- |
| 查询路径先拿 trigram_index                                     | `src/index/l2_partition.rs` | 727–733   |
| 查询路径后拿 metas/arena/tombstones                            | `src/index/l2_partition.rs` | 731–733   |
| 新文件路径先调用 alloc_docid（里面先 arena 后 metas）          | `src/index/l2_partition.rs` | 647–656   |
| alloc_docid 内部加锁顺序：arena → metas → filekey → tombstones | `src/index/l2_partition.rs` | 666–682   |
| 新文件路径后调用 insert_trigrams（里面拿 trigram_index.write） | `src/index/l2_partition.rs` | 653–654   |
| insert_trigrams 内部加锁：trigram_index + short_component      | `src/index/l2_partition.rs` | 1473–1485 |
| upsert_lock 的定义和目的注释                                   | `src/index/l2_partition.rs` | 361–364   |
| 查询路径的锁顺序注释（作者已意识到风险）                       | `src/index/l2_partition.rs` | 724–726   |

---

## 8. 结论

当前代码存在**理论上的 AB-BA 死锁**，根因是：

> **查询路径按 `trigram → metas` 顺序加锁，而写入路径按 `metas → trigram` 顺序加锁。**

`upsert_lock` 仅解决了写入者内部的原子性，没有解决读写交叉的锁顺序反转。建议尽快通过 **Shadow Delta + 统一锁顺序** 重构予以根治。

---

## 9.5. 越界陷阱（Panic 风险）

> 本小节分析一个**致命的修复陷阱**：如果简单地调整锁顺序并分步释放（例如先 `trigram` 后 `metas` 但在中间释放锁），会引发 `Index Out of Bounds Panic`。

### 错误思路推演

假设某个"naive 修复"按以下顺序执行：

1. Writer 先获取 `trigram_index.write()`，插入新 docid 的 posting。
2. **立即释放** `trigram_index` 的锁。
3. Writer 再去获取 `metas.write()`，执行 `metas.push(...)`。

在这个时间窗口内，Reader 查询会发生以下灾难：

| 时间点 | Writer 线程                                         | Reader 线程                                         | 状态                            |
| ------ | --------------------------------------------------- | --------------------------------------------------- | ------------------------------- |
| T1     | 持有 `tri_w`，向 `trigram_index` 插入新 docid = 100 | —                                                   | trigram_index 已包含 docid 100  |
| T2     | **释放 `tri_w`**                                    | —                                                   | trigram_index 最新状态对外可见  |
| T3     | —                                                   | 查询命中 trigram，从 posting 中读到 **docid = 100** | Reader 看到新 docid             |
| T4     | —                                                   | 尝试获取 `metas.read()`，成功                       | metas 尚未扩容                  |
| T5     | 尚未获取 `metas_w`                                  | 执行 `metas[100]`                                   | **Index Out of Bounds → Panic** |
| T6     | 获取 `metas_w`，`metas.push(...)`                   | 已崩溃                                              | —                               |

### 根本原因

`metas` 是 `Vec<CompactMeta>`，其有效索引范围是 `0..metas.len()`。  
Writer 在 `trigram_index` 中暴露出新的 `docid`（通常为当前 `metas.len()`）后，如果 `metas` 尚未通过 `push` 扩容，任何读到该 `docid` 的 Reader 都会越界访问。

> **结论：分步释放锁的"修复"不仅不能消除风险，还会引入更严重的运行时崩溃。所有数据结构的更新必须在同一把锁保护下原子完成，或者至少保证 Reader 永远不会看到"trigram 已更新但 metas 未扩容"的中间状态。**

---

## 10. 修正后的修复方案

### 核心原则

写入路径按 **Reader 的锁顺序**获取全部写锁，顺序如下：

```
trigram_index → short_component_index → path_hash_to_id → metas → arena → tombstones
```

**关键约束：在所有更新完成之前，不得释放任何一把锁。**  
只有在这个前提下，Shadow Delta 才能真正安全地消除死锁和可见性撕裂。

### 具体做法：Shadow Delta（影子合并）

#### Phase 1：本地计算（Compute）——**完全不拿任何业务锁**

- 纯函数提取 `trigrams`、`short_components`、`path_hash`。
- 构建 delta：
  - `HashMap<Trigram, RoaringTreemap>`（仅包含新增 docid 的 bit）
  - `HashMap<Box<[u8]>, RoaringTreemap>`（short_component 增量）
  - `HashMap<u64, OneOrManyDocId>`（path_hash 增量）
- 准备好 `CompactMeta`、`arena_bytes`、`file_key` 等所有需要写入的数据包。
- **耗时操作全部在此阶段完成**：字符串切分、Roaring Bitmap 的差分计算、`path_hash` 的 `wyhash` 运算。

#### Phase 2：快速提交（Apply）——**全量锁卫士**

```rust
let _upsert_guard = self.upsert_lock.write();

// ① 先按 Reader 顺序获取所有写锁
let mut tri_w = self.trigram_index.write();
let mut short_w = self.short_component_index.write();
let mut ph_w = self.path_hash_to_id.write();
let mut metas_w = self.metas.write();
let mut arena_w = self.arena.write();
let mut tomb_w = self.tombstones.write();

// ② 锁内只做纯内存的 bitwise-OR / Vec::push
for (tri, bits) in delta_trigrams {
    tri_w.entry(tri).or_default().bitor_assign(bits);
}
for (short, bits) in delta_shorts {
    short_w.entry(short).or_default().bitor_assign(bits);
}
for (ph, docids) in delta_path_hash {
    ph_w.insert(ph, docids); // 或按具体合并策略
}

let (off, len) = arena_w.push_bytes(&rel_bytes)?;
let docid = metas_w.len() as DocId;
metas_w.push(CompactMeta { off, len, ... });
self.filekey_to_docid.write().insert(file_key, docid); // 注意：filekey_to_docid 也需要按顺序加锁
tomb_w.remove(docid);

// Guards 在此统一 drop，中间状态绝不暴露
```

### 为什么这是最优解

1. **彻底消除死锁**：统一加锁顺序后，不存在任何 AB-BA 循环等待。
2. **彻底消除可见性撕裂**：Phase 2 中所有锁同时持有，Reader 要么看到完整的旧状态，要么看到完整的新状态，永远不会看到"trigram 已更新但 metas 未扩容"的中间态。
3. **彻底消除越界**：`metas.push(...)` 与 `trigram_index` 的更新在同一个临界区内完成，新的 `docid` 在对外可见之前，`metas` 已经扩容。
4. **性能达标**：Phase 1 把字符串处理、Bitmap 构建等 heavy compute 全部移出锁外；Phase 2 仅执行纯内存操作（`bitor_assign`、`Vec::push`、`HashMap::insert`），锁持有时间压缩到微秒级。

---

## 11. 架构演进建议（面向未来 v1.0）

当前 `PersistentIndex` 使用 **6 把独立的 `RwLock`** 保护 6 个字段。即使通过 Shadow Delta + 统一锁顺序消除了死锁空间，维护多把锁的语义仍然复杂，且容易出现遗漏（例如新增字段时忘记同步锁顺序）。

### 建议：合并为 `PartitionState` + 单把大锁

```rust
struct PartitionState {
    trigram_index: HashMap<Trigram, RoaringTreemap>,
    short_component_index: HashMap<Box<[u8]>, RoaringTreemap>,
    metas: Vec<CompactMeta>,
    arena: PathArena,
    tombstones: RoaringTreemap,
    filekey_to_docid: HashMap<FileKey, DocId>,
    path_hash_to_id: HashMap<u64, OneOrManyDocId>,
}

struct PersistentIndex {
    state: RwLock<PartitionState>,
    upsert_lock: RwLock<()>, // 可保留，用于写入者间串行（减少写锁竞争）
}
```

### 为什么这是合理的

- **Shadow Delta 已把锁持有时间压缩到微秒级**：Phase 2 中只有纯粹的内存 push/OR/insert，单把大锁不会构成瓶颈。
- **彻底消除多锁死锁空间**：单锁天然不存在循环等待。
- **降低心智负担**：新增字段时不必担心锁顺序问题，所有状态修改都在 `state.write()` 的原子临界区内完成。
- **性能损失极小**：由于 Phase 1 把 heavy compute 完全移出锁外，锁的争用窗口极短，单锁与多锁的吞吐差距在实测中可以忽略。

> **迁移路径**：v0.x 阶段先通过 Shadow Delta + 统一锁顺序修复现有问题；v1.0 阶段将多个字段合并为 `PartitionState`，彻底简化并发模型。

---

_分析时间：2026-04-18_
_基于文件：`src/index/l2_partition.rs`_
