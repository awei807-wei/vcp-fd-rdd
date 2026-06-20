# 周期扫描 stat() 风暴优化方案设计

> 状态：待确认。详细调查见 `helloagents/wiki/m2-scan-performance-investigation-20260620.md`。

## 总体策略

用 Linux 目录 mtime 作为免费变更信号，在周期扫描入口做预检：目录 mtime 没变就跳过整个扫描，将未变化目录的 stat() 从 30 万次降到 1 次。对于确实变化的目录，改用 readdir 批量名单比对来检测删除，避免逐文件 stat。

分三个 Phase，每 Phase 独立可合并：

- **Phase 1 (P0)**：目录 mtime 预检——最大收益，最低风险
- **Phase 2 (P1)**：readdir 批量删除对齐——消除删除检测的 stat 风暴
- **Phase 3 (P2)**：修复 manifest 大目录上限——让大目录也能受益于 manifest 跳过

---

## Phase 1：目录 mtime 预检

**位置**：`src/index/tiered/sync.rs` + `src/index/tiered/directory_manifest.rs`

**现状**：

`scan_dir_repair_slice_with_project_markers`（sync.rs:1222）在 manifest skip 检查之后直接进入 sliced scan，不检查目录 mtime。即使目录自上次扫描后没有任何变化，也会对全部子文件做 stat()。

**原理**：

Linux 内核保证：当目录内有文件被创建、删除、重命名时，目录的 mtime 会更新。文件内容修改（不涉及增删）不改变目录 mtime，但这类修改由 inotify watcher（L0）或 lazy validation 覆盖，周期扫描的职责是发现增删，不需要检测内容修改。

**修改**：

### 步骤 1：给 DirectoryManifest 添加 dir_mtime_ns 字段

**文件**：`src/index/tiered/directory_manifest.rs`

```rust
pub(crate) struct DirectoryManifest {
    // ... 现有字段 ...
    pub dir_mtime_ns: i64,  // 新增：目录自身的 mtime
}
```

### 步骤 2：添加 dir_unchanged 方法

```rust
impl DirectoryManifestStore {
    /// 检查目录 mtime 是否与上次记录一致。
    /// 返回 true 表示目录自上次扫描后没有文件增删，可以安全跳过。
    pub fn dir_unchanged(&self, path: &Path, current_mtime_ns: i64) -> bool {
        self.manifests.lock()
            .get(path)
            .map(|m| m.dir_mtime_ns == current_mtime_ns)
            .unwrap_or(false) // 没有记录过 = 不能跳过，需要首次扫描
    }

    /// 记录目录 mtime，在扫描完成后调用。
    pub fn record_dir_mtime(&self, path: &Path, mtime_ns: i64) {
        let mut manifests = self.manifests.lock();
        manifests.entry(path.to_path_buf())
            .or_insert_with(|| DirectoryManifest {
                // ... 默认值 ...
                dir_mtime_ns: mtime_ns,
            })
            .dir_mtime_ns = mtime_ns;
    }
}
```

### 步骤 3：在周期扫描入口加 mtime 预检

**文件**：`src/index/tiered/sync.rs`，函数 `scan_dir_repair_slice_with_project_markers`

在 manifest skip 检查之后、sliced scan 之前（约 line 1261），插入：

```rust
// 新增：目录 mtime 预检
if cursor.is_none() {
    if let Ok(dir_meta) = std::fs::symlink_metadata(dir) {
        if let Ok(dir_modified) = dir_meta.modified() {
            let dir_mtime_ns = mtime_to_ns(Some(dir_modified));
            if self.directory_manifests.dir_unchanged(dir, dir_mtime_ns) {
                // 目录没变，跳过整个扫描
                return SlicedScanOutcome {
                    outcome: ScanOutcome::default(),
                    manifest_skipped: true,
                    completed: true,
                    next_cursor: None,
                    dropped_stale_batch: false,
                };
            }
        }
    }
}
```

### 步骤 4：扫描完成后记录目录 mtime

在 sliced scan 完成后（约 line 1400），记录当前目录 mtime：

```rust
// 扫描完成后记录 mtime，供下次预检使用
if let Ok(dir_meta) = std::fs::symlink_metadata(dir) {
    if let Ok(dir_modified) = dir_meta.modified() {
        let dir_mtime_ns = mtime_to_ns(Some(dir_modified));
        self.directory_manifests.record_dir_mtime(dir, dir_mtime_ns);
    }
}
```

**内存节省**：0（新增字段每目录 8 字节，可忽略）

**性能收益**：未变化目录从 30 万次 stat() 降到 1 次 stat()。稳态下（大部分目录大部分时间不变）扫描时间从 15-60 秒降到 <1 毫秒。

**风险**：低。目录 mtime 是内核保证的可靠信号。唯一遗漏是"文件内容修改但未增删文件"的场景，但这由 L0 inotify 和 lazy validation 覆盖，不在周期扫描职责内。

---

## Phase 2：readdir 批量删除对齐

**位置**：`src/index/tiered/sync.rs:990-996`

**现状**：

`fast_sync` 的删除对齐逻辑对索引中的每个文件逐个 stat() 检查是否存在：

```rust
for (_doc_id, path) in to_delete {
    match std::fs::symlink_metadata(&path) {  // ← 每个索引文件一次 stat
        Ok(_) => continue,           // 文件还在
        Err(e) if e.kind() == NotFound => {} // 文件被删了
        Err(_) => continue,
    }
    // ... 创建 delete 事件 ...
}
```

30 万文件的目录做一次删除对齐 = 30 万次 stat()。

**修改**：

改为一次 `readdir` 获取当前文件名集合，与索引中的文件名做集合差集：

```rust
// 1. 一次 readdir 获取当前目录所有文件名
let current_names: HashSet<OsString> = match std::fs::read_dir(dir) {
    Ok(entries) => entries.filter_map(|e| e.ok().map(|e| e.file_name())).collect(),
    Err(_) => return, // 目录不存在，全部标记删除
};

// 2. 与索引中的文件名做差集，找出被删的
for (doc_id, path) in to_delete {
    let name = path.file_name();
    if let Some(name) = name {
        if !current_names.contains(name) {
            // 文件不在 readdir 结果中 = 被删除
            // ... 创建 delete 事件 ...
        }
    }
}
```

**性能收益**：删除对齐从 30 万次 stat() 降到 1 次 readdir + N 次 HashSet 查找（纳秒级）。对于 30 万文件的目录，从 15-30 秒降到 <100 毫秒。

**内存开销**：`HashSet<OsString>` 临时分配，30 万条目约 20-30MB。扫描完成后释放。代码注释提到曾因"大临时分配"回避此方案，但当前逐文件 stat() 的代价远大于临时内存。

**风险**：低。readdir 是原子快照，与逐个 stat() 的语义一致。`HashSet` 内存是短暂峰值，比 30 万次 syscall 的 CPU/IO 开销小得多。

---

## Phase 3：修复 manifest 大目录上限

**位置**：`src/index/tiered/sync.rs:1533`（`directory_manifest_summary_bounded`）

**现状**：

manifest summary 计算被限制在 512 条以内（`REPAIR_SLICE_MAX_ENTRIES`）。超过 512 条的目录返回 `complete=false`，manifest skip 永远不生效。

**修改**：

新增一个不受 512 限制的 summary 计算函数：

```rust
/// 计算目录 manifest summary，不限制条目数。
/// 仅用于 mtime 预检失败后的二次确认（目录确实变了，但内容可能没变）。
fn directory_manifest_summary_unbounded(
    &self,
    dir: &Path,
) -> Option<DirectoryManifestSummary> {
    let entries = std::fs::read_dir(dir).ok()?;
    let mut child_count = 0u64;
    let mut names_hash = xx3_hasher::new();
    let mut child_mtime_hash = xx3_hasher::new();
    let mut min_mtime_ns = i64::MAX;
    let mut max_mtime_ns = i64::MIN;

    for entry in entries {
        let entry = entry.ok()?;
        let name = entry.file_name();
        let meta = entry.metadata().ok()?; // 仍需 stat，但只在 mtime 预检失败后走这里
        let mtime_ns = mtime_to_ns(meta.modified().ok());

        names_hash.update(name.as_encoded_bytes());
        child_mtime_hash.update(name.as_encoded_bytes());
        child_mtime_hash.update(&mtime_ns.to_le_bytes());
        min_mtime_ns = min_mtime_ns.min(mtime_ns);
        max_mtime_ns = max_mtime_ns.max(mtime_ns);
        child_count += 1;
    }

    Some(DirectoryManifestSummary {
        child_count,
        names_hash: names_hash.digest(),
        child_mtime_hash: child_mtime_hash.digest(),
        min_mtime_ns,
        max_mtime_ns,
    })
}
```

在 `scan_dir_repair_slice_with_project_markers` 中，mtime 预检失败后（目录 mtime 变了），先尝试 unbounded summary 比对。如果 summary 匹配（可能只是 mtime 精度问题），仍可跳过。summary 不匹配才进入 sliced scan。

**性能收益**：对于"目录 mtime 变了但内容实际没变"的场景（例如 touch 目录、atime 更新等），unbounded summary 可以避免无意义的全量扫描。对于真正变化的目录，summary 计算的 stat() 成本与 sliced scan 相同，没有额外开销。

**风险**：低。summary 是纯增量逻辑——不匹配时回退到原有 sliced scan，行为不变。

---

## 不修改的部分

- **sliced scan 的 slice 大小**（512 条）——这不是瓶颈，瓶颈是每个 slice 里的 stat()，不是 slice 数量
- **`read_dir_slice` 的 libc::readdir 实现**——已经用原生 getdents，效率足够
- **fast_sync 的 `visit_dirs_since`**——已经用了目录 mtime 信号，是正确的实现
- **L0 inotify watcher 策略**——不在本方案范围，watch 预算问题是独立架构问题

---

## 收益汇总

| Phase | 优化项 | 场景 | 优化前 | 优化后 |
|---|---|---|---|---|
| P1 | 目录 mtime 预检 | 未变化的 30 万文件目录 | 30 万次 stat, 15-60s | **1 次 stat, <1ms** |
| P2 | readdir 批量删除对齐 | 删除对齐 30 万文件 | 30 万次 stat, 15-30s | **1 次 readdir, <100ms** |
| P3 | manifest 大目录上限 | 大目录 manifest 跳过 | 永远不跳过 | 可跳过 |
| — | P1+P2 组合 | 稳态周期扫描 | 60-90s | **<1ms** |

**预期效果**：
- 热层 canary 成功率从 0/30 提升——Projects/ 虽然 mtime 变了需要扫描，但扫描速度大幅提升（P2 消除删除对齐的 stat 风暴）
- L1→L2 降级速度加快——空扫描从 15-60s 降到 <1ms，目录能快速完成降级
- M2 旋转窗口有更多目标——更多目录降级到 L2/L3 后，旋转窗口可以服务它们
- CPU 和 IO 开销下降——稳态下大部分目录不变，不再做无意义的 stat()

---

## 风险与验证

### 风险

1. **mtime 精度问题**：某些文件系统（如 FAT32）的 mtime 精度只有 2 秒，可能漏检。但 fd-rdd 的目标平台是 Linux ext4/xfs/btrfs，mtime 精度为纳秒级，不存在此问题。

2. **目录 mtime 被人为修改**：用户 `touch -m` 修改目录 mtime 会导致预检失效（误认为有变化），但这只是回退到正常扫描，不会漏检。

3. **readdir 内存峰值**（P2）：30 万文件的 `HashSet<OsString>` 约 20-30MB 临时内存。对于内存紧张的环境，可以分批 readdir + 比对，但实测 30MB 峰值远小于 760MB 的索引 RSS，可接受。

4. **rename 不改变文件数**：`mv a b` 在同一目录内会改变目录 mtime（因为目录条目变了），预检能正确检测。跨目录 rename 会改变两个目录的 mtime，都能被检测。

### 验证

见 task.md 测试项。

---

## 实施顺序

```
Phase 1 (mtime 预检)       ← 先做，最大收益，~20 行
    ↓
Phase 2 (readdir 对齐)     ← 消除删除检测 stat 风暴，~60 行
    ↓
Phase 3 (manifest 上限)    ← 补全 manifest 机制，~50 行
```

每个 Phase 独立提交、独立测试、独立可回滚。Phase 1 和 Phase 2 互不依赖，可以并行开发。
