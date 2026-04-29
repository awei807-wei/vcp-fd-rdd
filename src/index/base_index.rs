//! 只读基础索引（参见 `重构方案包/causal-chain-report.md` §8.3.5）。
//!
//! `BaseIndex` 把 [`PathTable`] / [`ByFileKey`] / [`ByPathIdx`] / [`ParentIndex`] / 墓碑
//! 组合成一个可被并发查询的快照。运行期通过 [`crate::index::delta_buffer::DeltaBuffer`]
//! 表达增量；snapshot 时把 delta 合并回 base。
//!
//! 当前阶段（2A 脚手架）
//! - 基底是 `Vec<u8>` / `Vec<FileEntry>`，零拷贝 mmap 接入留给 v7（§8.4 第四阶段）。
//! - **不**接入 `TieredIndex`；纯内存模型 + 完整单测，目的是把数据结构层确定下来。
//! - Trigram / short_component 倒排尚未集成（保持现有 `PersistentIndex.trigram_index`
//!   的 `HashMap<[u8;3], RoaringTreemap>` 形态）；2C 阶段会迁移。
//!
//! 用法摘要
//! ```ignore
//! let mut builder = BaseIndexBuilder::new();
//! builder.push("/a/foo.txt", FileKey { dev: 1, ino: 10, generation: 0 }, 100, -1);
//! builder.push("/a/bar.txt", FileKey { dev: 1, ino: 11, generation: 0 }, 200, -1);
//! let base = builder.build();
//! assert_eq!(base.file_count(), 2);
//! ```

use std::collections::HashSet;
use std::path::Path;

use roaring::RoaringTreemap;

use crate::core::FileKey;
use crate::index::delta_buffer::DeltaBuffer;
use crate::index::file_entry::{ByFileKey, ByPathIdx, FileEntry};
use crate::index::parent_index::{parent_hash_bytes, ParentIndex};
use crate::index::path_table::PathTable;

/// 输入条目：构建期持有完整 path bytes，build 时排序、生成 path_idx。
#[derive(Debug, Clone)]
struct PendingEntry {
    path: Vec<u8>,
    file_key: FileKey,
    size: u64,
    mtime_ns: i64,
}

/// 构造器：累积 (path, file_key, size, mtime) → 一次性 build 出 [`BaseIndex`]。
#[derive(Debug, Default)]
pub struct BaseIndexBuilder {
    pending: Vec<PendingEntry>,
}

impl BaseIndexBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            pending: Vec::with_capacity(cap),
        }
    }

    /// 添加一条目；调用顺序与最终 path_idx 无关。
    pub fn push(&mut self, path: impl AsRef<[u8]>, file_key: FileKey, size: u64, mtime_ns: i64) {
        self.pending.push(PendingEntry {
            path: path.as_ref().to_vec(),
            file_key,
            size,
            mtime_ns,
        });
    }

    pub fn len(&self) -> usize {
        self.pending.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// 排序 → 写 PathTable → 生成 FileEntries → 构建 ParentIndex → 完成。
    pub fn build(mut self) -> BaseIndex {
        // 1) 字典序排序 path（front-encoding 压缩前提）。
        self.pending
            .sort_by(|a, b| a.path.as_slice().cmp(b.path.as_slice()));

        // 2) 写入 PathTable，并记下每条 path 的 path_idx。
        let mut path_table = PathTable::new();
        let mut entries: Vec<FileEntry> = Vec::with_capacity(self.pending.len());
        let mut parent_index = ParentIndex::new();

        for pe in &self.pending {
            let path_idx = path_table.push(&pe.path);
            entries.push(FileEntry::new(pe.file_key, path_idx, pe.size, pe.mtime_ns));

            // ParentIndex：父目录 hash → DocId（DocId 这里就是 entries 的下标，
            // 也即 by_path 排序前的位置；后续 BaseIndex 用 by_path 视图，由调用方负责
            // 把 entries.iter().enumerate() 的 index 当作 DocId）。
            let parent_bytes = parent_path_bytes(&pe.path);
            let doc_id = (entries.len() - 1) as u64;
            parent_index.insert(parent_hash_bytes(parent_bytes), doc_id);
        }

        // 3) 排序视图：共享同一份 `Arc<[FileEntry]>`（按 path_idx 排序）。
        // by_path_idx 直接持有；by_file_key 维护一个 perm 数组排序到 file_key 序。
        // 8M 条 / 40B entry 时 perm 比第二份 entries 省 8×（4B vs 40B）≈ 290MB。
        let entries: std::sync::Arc<[FileEntry]> = std::sync::Arc::from(entries);
        let by_file_key = ByFileKey::with_shared(std::sync::Arc::clone(&entries));
        let by_path_idx = ByPathIdx::with_shared(entries);

        BaseIndex {
            path_table,
            by_file_key,
            by_path_idx,
            parent_index,
            tombstones: RoaringTreemap::new(),
        }
    }
}

/// 只读基础索引快照。
#[derive(Debug)]
pub struct BaseIndex {
    path_table: PathTable,
    by_file_key: ByFileKey,
    by_path_idx: ByPathIdx,
    parent_index: ParentIndex,
    tombstones: RoaringTreemap,
}

impl BaseIndex {
    pub fn empty() -> Self {
        BaseIndexBuilder::new().build()
    }

    pub fn path_table(&self) -> &PathTable {
        &self.path_table
    }

    pub fn by_file_key(&self) -> &ByFileKey {
        &self.by_file_key
    }

    pub fn by_path_idx(&self) -> &ByPathIdx {
        &self.by_path_idx
    }

    pub fn parent_index(&self) -> &ParentIndex {
        &self.parent_index
    }

    pub fn tombstones(&self) -> &RoaringTreemap {
        &self.tombstones
    }

    /// 标记 DocId 为已删除（运行期 base 上的逻辑删除会经由 `DeltaBuffer.removed`，
    /// 但 build 后离线打 tombstone 也是合法的，例如 mmap base 加载后发现陈旧 entries）。
    pub fn mark_tombstone(&mut self, doc_id: u64) {
        self.tombstones.insert(doc_id);
    }

    /// 直接从 v7 持久化结构构造（参见 `重构方案包/causal-chain-report.md` §8.9）。
    ///
    /// v7 保证 `entries` 按 `path_idx`（也即 abs path 字典序）升序排列，且 `path_table`
    /// 已经是最终形态——所以这里**复用**而不是 rebuild：
    /// - `path_table` 移交所有权，不再走 `BaseIndexBuilder` 的 push 重建路径；
    /// - `entries` 直接喂给 `ByPathIdx::build`（其内部 sort 保持稳定）；
    /// - `ByFileKey` 仍要重排（v7 不持久化 file_key 排序视图，重建 = O(N log N)）；
    /// - `ParentIndex` **留空**：调用方（[`crate::index::l2_partition::PersistentIndex::from_snapshot_v7`]）
    ///   会立即在外层 `PersistentIndex.parent_index` 里把同一份数据重建出来，
    ///   而 production 查询路径只读外层那个字段；本结构内部的 `parent_index` 没
    ///   消费者就让它保持空。这把 cold-start 解析路径次数从 2 砍到 1。
    /// - 内部 `tombstones` 留空：v7 tombstones 由调用方装回外层
    ///   `PersistentIndex.tombstones`（这是查询路径实际读的字段）。
    ///
    /// 与 `BaseIndexBuilder::push(...).build()` 路径相比，省掉了：
    /// 1. 每条 entry 一次 `path_table.resolve()`（≈ N × anchor lookup + memcpy）；
    /// 2. 全量 `pending` 排序（v7 已排序）；
    /// 3. 把所有 path bytes push 进新 `PathTable`（重做一次 front-encoding）。
    ///
    /// 8M 文件冷启动节省 ≈ 800MB 临时分配 + 8-15s CPU。
    ///
    /// **不变量警告**：返回的 `BaseIndex` 的 `parent_index` 是空的，因此
    /// [`Self::live_doc_ids_in_dir`] / [`Self::live_doc_ids_in_dirs`] 会返回空集——
    /// 这两个 API 当前只在本模块的单测里直接调用，production 走 PersistentIndex。
    /// 如果未来 BaseIndex 接入新的查询入口，需要回到本函数补上 parent_index 构建。
    pub fn from_v7_snapshot(snap: crate::storage::snapshot_v7::V7Snapshot) -> Self {
        let crate::storage::snapshot_v7::V7Snapshot {
            path_table,
            entries,
            ..
        } = snap;

        // 共享 entries（参见 [`Self::build`] 同段说明）。v7 entries 已按 path_idx
        // 升序，by_path_idx 直接持 Arc，by_file_key 仅多 4B/条 perm。
        let entries: std::sync::Arc<[FileEntry]> = std::sync::Arc::from(entries);
        let by_file_key = ByFileKey::with_shared(std::sync::Arc::clone(&entries));
        let by_path_idx = ByPathIdx::with_shared(entries);

        BaseIndex {
            path_table,
            by_file_key,
            by_path_idx,
            parent_index: ParentIndex::new(),
            tombstones: RoaringTreemap::new(),
        }
    }

    /// 当前 base 的 live 文件数。
    pub fn file_count(&self) -> u64 {
        let total = self.by_file_key.len() as u64;
        total.saturating_sub(self.tombstones.len())
    }

    /// 解析某 DocId 的完整路径字节。
    pub fn resolve_path(&self, doc_id: u64) -> Option<Vec<u8>> {
        // BaseIndexBuilder 把 entries.iter().enumerate() 的 idx 作为 DocId，
        // 而 by_path_idx 是按 path_idx 排序后的视图——刚好同顺序，因为 build 前
        // 已经按 path 字典序排序，path_idx 单调递增。
        let entry = self.by_path_idx.entries().get(doc_id as usize)?;
        self.path_table.resolve(entry.path_idx)
    }

    /// 列出某父目录下的所有 live DocId。
    pub fn live_doc_ids_in_dir(&self, parent_path: impl AsRef<[u8]>) -> RoaringTreemap {
        let h = parent_hash_bytes(parent_path.as_ref());
        match self.parent_index.files_in_dir(h) {
            Some(bm) => bm - &self.tombstones,
            None => RoaringTreemap::new(),
        }
    }

    /// 多父目录并集 - tombstones（fast_sync 删除对齐用得上）。
    pub fn live_doc_ids_in_dirs<I, S>(&self, parents: I) -> RoaringTreemap
    where
        I: IntoIterator<Item = S>,
        S: AsRef<[u8]>,
    {
        let hashes = parents.into_iter().map(|p| parent_hash_bytes(p.as_ref()));
        let combined = self.parent_index.files_in_dirs(hashes);
        combined - &self.tombstones
    }

    /// 估算 base 的常驻内存（不含 mmap 冷页；与 §8.7 内存表对应）。
    pub fn estimated_bytes(&self) -> u64 {
        let path_bytes = self.path_table.data_len() as u64 + self.path_table.anchors_bytes() as u64;
        // entries 在 by_file_key / by_path_idx 之间通过 Arc 共享，只计一份；
        // by_file_key 额外占 perm（u32/条）。
        let entries_bytes =
            (self.by_path_idx.len() as u64).saturating_mul(std::mem::size_of::<FileEntry>() as u64);
        let perm_bytes = self.by_file_key.perm_bytes();
        let parent_bytes = self.parent_index.estimated_bytes();
        let tombstone_bytes = self.tombstones.serialized_size() as u64;
        path_bytes
            .saturating_add(entries_bytes)
            .saturating_add(perm_bytes)
            .saturating_add(parent_bytes)
            .saturating_add(tombstone_bytes)
    }

    /// 把 delta 合并进当前 base，返回**新的** BaseIndex。语义：
    ///
    /// - 输入：当前 base 的 live entries（剔除 self.tombstones 和 delta.removed）+
    ///   delta.added 的所有 entries。
    /// - file_key 去重：若 delta.added 与 base.live 中同 FileKey 共存（理论上不应发生，
    ///   但 rename 路径下可能短暂出现），**delta 优先**——更新比 base 旧路径更可信。
    /// - 输出 BaseIndex：path_table 重新按字典序构建、tombstones 清空、
    ///   ParentIndex 重新建立。
    /// - 不修改输入的 self（消耗 self 但不破坏 delta 引用）。
    ///
    /// 注：delta.removed 中如果有 doc_id 在 self 里找不到对应 entry（异常状态），
    /// 仅记 trace、不阻塞 merge。
    pub fn merge_with_delta(self, delta: &DeltaBuffer) -> BaseIndex {
        // 1) 收集所有候选 (path_bytes, file_key, size, mtime_ns)。
        //    delta 优先：先 push delta.added，再 push base.live；用 HashSet 去重 file_key。
        let mut seen_keys: HashSet<FileKey> = HashSet::new();
        let mut candidates: Vec<(Vec<u8>, FileKey, u64, i64)> =
            Vec::with_capacity(self.by_path_idx.len() + delta.added().len());

        // 1a) delta.added (优先)。
        for (entry, path_bytes) in delta.iter_added_with_path() {
            let fk = entry.file_key();
            if seen_keys.insert(fk) {
                candidates.push((path_bytes.to_vec(), fk, entry.size, entry.mtime_ns));
            }
        }

        // 1b) base.live：剔除 self.tombstones 与 delta.removed。
        let removed = delta.removed();
        for (doc_id, entry) in self.by_path_idx.entries().iter().enumerate() {
            let did = doc_id as u64;
            if self.tombstones.contains(did) || removed.contains(did) {
                continue;
            }
            let fk = entry.file_key();
            if !seen_keys.insert(fk) {
                continue; // 已被 delta 同 FileKey 覆盖
            }
            // 通过 base 的 PathTable 解析 path bytes。
            let Some(path_bytes) = self.path_table.resolve(entry.path_idx) else {
                tracing::warn!(
                    "BaseIndex.merge_with_delta: path_idx {} unresolvable; dropping entry",
                    entry.path_idx
                );
                seen_keys.remove(&fk); // rollback
                continue;
            };
            candidates.push((path_bytes, fk, entry.size, entry.mtime_ns));
        }

        // 2) 用 builder 重建（builder 内部会按字典序排序 + 构造 PathTable + ParentIndex）。
        let mut builder = BaseIndexBuilder::with_capacity(candidates.len());
        for (path, fk, size, mtime_ns) in candidates {
            builder.push(path, fk, size, mtime_ns);
        }
        builder.build()
    }
}

/// 提取“父目录字节”：等价于 `Path::parent()` 但作用在原始 bytes，
/// 兼顾 `/` 与（少量场景下）`\` 分隔符。
pub fn parent_path_bytes(path_bytes: &[u8]) -> &[u8] {
    let p = Path::new(std::str::from_utf8(path_bytes).unwrap_or(""));
    if let Some(parent) = p.parent() {
        let s = parent.as_os_str().as_encoded_bytes();
        // 在 path_bytes 内切片：要求 parent 是 prefix 才能复用切片；否则退回常量 root。
        if path_bytes.starts_with(s) {
            return &path_bytes[..s.len()];
        }
    }
    // 兜底：手动找最后一个 '/'。
    if let Some(pos) = path_bytes.iter().rposition(|&b| b == b'/') {
        if pos == 0 {
            return &path_bytes[..1]; // "/"
        }
        return &path_bytes[..pos];
    }
    &[]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fk(ino: u64) -> FileKey {
        FileKey {
            dev: 1,
            ino,
            generation: 0,
        }
    }

    #[test]
    fn empty_base_has_zero_files() {
        let base = BaseIndex::empty();
        assert_eq!(base.file_count(), 0);
        assert!(base.path_table().is_empty());
    }

    #[test]
    fn parent_path_bytes_extraction() {
        assert_eq!(parent_path_bytes(b"/home/a/b.txt"), b"/home/a");
        assert_eq!(parent_path_bytes(b"/foo.txt"), b"/");
        // 根 "/" 的父目录约定为它自身：fast_sync 用父目录哈希做 lookup，
        // 把 "/" 自我映射比返回空串更合理（不会与“无前缀”路径相撞）。
        assert_eq!(parent_path_bytes(b"/"), b"/");
        assert_eq!(parent_path_bytes(b"foo"), b"");
    }

    #[test]
    fn build_indexes_simple_tree() {
        let mut b = BaseIndexBuilder::new();
        b.push("/a/x.txt", fk(1), 100, -1);
        b.push("/a/y.txt", fk(2), 200, -1);
        b.push("/b/z.txt", fk(3), 300, -1);
        let base = b.build();

        assert_eq!(base.file_count(), 3);

        // by_file_key 找得到。
        let (_, e) = base.by_file_key().find(fk(1)).unwrap();
        assert_eq!(e.file_key(), fk(1));
        assert!(base.by_file_key().find(fk(99)).is_none());

        // resolve 出来的 path 对应。
        for doc_id in 0..3u64 {
            let p = base.resolve_path(doc_id).unwrap();
            assert!(p.starts_with(b"/"));
        }
    }

    #[test]
    fn live_doc_ids_in_dir_subtracts_tombstones() {
        let mut b = BaseIndexBuilder::new();
        b.push("/d/a.txt", fk(1), 0, -1);
        b.push("/d/b.txt", fk(2), 0, -1);
        b.push("/d/c.txt", fk(3), 0, -1);
        let mut base = b.build();

        let live = base.live_doc_ids_in_dir(b"/d");
        assert_eq!(live.len(), 3);

        // 标一条 tombstone 后再查。
        let one_doc_id = base.by_path_idx().entries()[1].path_idx as u64;
        // by_path_idx 的 path_idx 与 doc_id 一致（build 时按字典序排序），所以可直接用 1。
        let _ = one_doc_id;
        base.mark_tombstone(1);
        let live = base.live_doc_ids_in_dir(b"/d");
        assert_eq!(live.len(), 2);
    }

    #[test]
    fn live_doc_ids_in_dirs_unions_then_subtracts_tombstones() {
        let mut b = BaseIndexBuilder::new();
        b.push("/d1/a", fk(1), 0, -1);
        b.push("/d2/b", fk(2), 0, -1);
        b.push("/d2/c", fk(3), 0, -1);
        b.push("/d3/d", fk(4), 0, -1);
        let mut base = b.build();
        base.mark_tombstone(0); // /d1/a

        let live = base.live_doc_ids_in_dirs([b"/d1".as_slice(), b"/d2".as_slice()]);
        // /d1 -> 1 file (now 0 after tombstone), /d2 -> 2 files
        assert_eq!(live.len(), 2);
    }

    #[test]
    fn estimated_bytes_reasonable_for_empty_index() {
        let base = BaseIndex::empty();
        // 空索引应近 0；允许少量 hashmap 容量带来的常量开销。
        assert!(base.estimated_bytes() < 4 * 1024);
    }

    #[test]
    fn build_preserves_doc_id_to_path_mapping() {
        let mut b = BaseIndexBuilder::new();
        // 故意乱序 push；build 内部排序应让 DocId 与字典序对齐。
        b.push("/z.txt", fk(3), 0, -1);
        b.push("/a.txt", fk(1), 0, -1);
        b.push("/m.txt", fk(2), 0, -1);
        let base = b.build();

        let p0 = base.resolve_path(0).unwrap();
        let p1 = base.resolve_path(1).unwrap();
        let p2 = base.resolve_path(2).unwrap();
        assert_eq!(p0, b"/a.txt");
        assert_eq!(p1, b"/m.txt");
        assert_eq!(p2, b"/z.txt");
    }

    /// merge_with_delta 的核心场景：delta 增、base 删（tombstone）、delta 删（removed）。
    /// 期望 merge 后新 base 等价于 (base.live - delta.removed) ∪ delta.added。
    #[test]
    fn merge_with_delta_combines_live_base_and_delta_added() {
        // base：a / b / c。
        let mut bb = BaseIndexBuilder::new();
        bb.push("/a", fk(1), 0, -1);
        bb.push("/b", fk(2), 0, -1);
        bb.push("/c", fk(3), 0, -1);
        let base = bb.build();
        assert_eq!(base.file_count(), 3);

        // delta：删 b（doc_id=1），新增 d。
        let mut delta = DeltaBuffer::new();
        delta.set_base_file_count(base.file_count());
        delta.mark_removed(1); // b
        delta.upsert_added(fk(4), b"/d", 0, -1);

        let merged = base.merge_with_delta(&delta);

        assert_eq!(merged.file_count(), 3); // a, c, d
        assert!(
            merged.tombstones().is_empty(),
            "merged base should have empty tombstones"
        );

        // by_file_key 上找得到 1/3/4，找不到 2。
        assert!(merged.by_file_key().find(fk(1)).is_some());
        assert!(
            merged.by_file_key().find(fk(2)).is_none(),
            "fk=2 was removed"
        );
        assert!(merged.by_file_key().find(fk(3)).is_some());
        assert!(merged.by_file_key().find(fk(4)).is_some());

        // 字典序应按 a, c, d 排列（b 已删）。
        let p0 = merged.resolve_path(0).unwrap();
        let p1 = merged.resolve_path(1).unwrap();
        let p2 = merged.resolve_path(2).unwrap();
        assert_eq!(p0, b"/a");
        assert_eq!(p1, b"/c");
        assert_eq!(p2, b"/d");
    }

    /// delta.removed 应优先于 base 的 tombstones——两者并集都被剔除。
    #[test]
    fn merge_skips_both_base_tombstones_and_delta_removed() {
        let mut bb = BaseIndexBuilder::new();
        bb.push("/a", fk(1), 0, -1);
        bb.push("/b", fk(2), 0, -1);
        bb.push("/c", fk(3), 0, -1);
        let mut base = bb.build();
        base.mark_tombstone(0); // a 被旧 base 标记墓碑

        let mut delta = DeltaBuffer::new();
        delta.set_base_file_count(base.file_count());
        delta.mark_removed(2); // c

        let merged = base.merge_with_delta(&delta);
        assert_eq!(merged.file_count(), 1); // 只剩 b
        assert!(merged.by_file_key().find(fk(1)).is_none());
        assert!(merged.by_file_key().find(fk(2)).is_some());
        assert!(merged.by_file_key().find(fk(3)).is_none());
    }

    /// 同 FileKey 在 delta 与 base 都存在时（rename 短暂窗口），delta 优先。
    #[test]
    fn merge_delta_overrides_base_on_same_filekey() {
        let mut bb = BaseIndexBuilder::new();
        bb.push("/old_path", fk(1), 100, -1);
        let base = bb.build();

        let mut delta = DeltaBuffer::new();
        delta.set_base_file_count(base.file_count());
        // 同 FileKey 但 path 不同（rename 后未来得及 mark_removed 的中间状态）。
        delta.upsert_added(fk(1), b"/new_path", 999, 1234);

        let merged = base.merge_with_delta(&delta);
        assert_eq!(merged.file_count(), 1);
        let (_, e) = merged.by_file_key().find(fk(1)).unwrap();
        assert_eq!(e.size, 999, "delta value should win");
        assert_eq!(e.mtime_ns, 1234);
        assert_eq!(merged.resolve_path(0).unwrap(), b"/new_path");
    }

    /// merge 后再次 merge 一个空 delta 应得到等价结果（幂等性）。
    #[test]
    fn merge_with_empty_delta_is_identity() {
        let mut bb = BaseIndexBuilder::new();
        bb.push("/a", fk(1), 0, -1);
        bb.push("/b", fk(2), 0, -1);
        let base = bb.build();
        let original_count = base.file_count();
        let original_paths: Vec<Vec<u8>> = (0..original_count)
            .map(|i| base.resolve_path(i).unwrap())
            .collect();

        let delta = DeltaBuffer::new();
        let merged = base.merge_with_delta(&delta);

        assert_eq!(merged.file_count(), original_count);
        for (i, p) in original_paths.iter().enumerate() {
            assert_eq!(merged.resolve_path(i as u64).unwrap(), *p);
        }
    }

    /// 端到端：base 有 5 条，delta 删 2 条、加 3 条 → merge 后 6 条；新 base 的
    /// ParentIndex 也要正确反映新结构。
    #[test]
    fn merge_rebuilds_parent_index() {
        let mut bb = BaseIndexBuilder::new();
        bb.push("/d1/a", fk(1), 0, -1);
        bb.push("/d1/b", fk(2), 0, -1);
        bb.push("/d2/c", fk(3), 0, -1);
        bb.push("/d2/d", fk(4), 0, -1);
        bb.push("/d3/e", fk(5), 0, -1);
        let base = bb.build();

        // 删 d1/a 和 d3/e；加 d2/x 与 d4/y。
        let mut delta = DeltaBuffer::new();
        delta.set_base_file_count(base.file_count());
        delta.mark_removed(0); // d1/a (字典序第 0)
        delta.mark_removed(4); // d3/e (字典序第 4)
        delta.upsert_added(fk(10), b"/d2/x", 0, -1);
        delta.upsert_added(fk(11), b"/d4/y", 0, -1);

        let merged = base.merge_with_delta(&delta);

        // /d1 现在只剩 b。
        let live_d1 = merged.live_doc_ids_in_dir(b"/d1");
        assert_eq!(live_d1.len(), 1);
        // /d2 现在有 c、d、x = 3 条。
        let live_d2 = merged.live_doc_ids_in_dir(b"/d2");
        assert_eq!(live_d2.len(), 3);
        // /d3 全空。
        let live_d3 = merged.live_doc_ids_in_dir(b"/d3");
        assert_eq!(live_d3.len(), 0);
        // /d4 新建：1 条。
        let live_d4 = merged.live_doc_ids_in_dir(b"/d4");
        assert_eq!(live_d4.len(), 1);
    }
}
