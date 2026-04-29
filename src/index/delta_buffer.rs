//! 运行时增量缓存（参见 `重构方案包/causal-chain-report.md` §8.3.6）。
//!
//! `BaseIndex`（mmap 只读）+ `DeltaBuffer`（运行时 read-write）= 当前可见状态。
//!
//! 查询语义
//! ```text
//! visible(file) = (file ∈ base AND base_doc_id(file) ∉ delta.removed)
//!              OR file ∈ delta.added
//! ```
//!
//! ## Path 存储
//! Delta 必须能独立解析它新增条目的完整 path——base 的 [`crate::index::path_table::PathTable`]
//! 是 append-only 的差量编码结构、不适合运行时高频写入。所以 DeltaBuffer 自带一份
//! 朴素的 [`Vec<u8>`] arena：
//!
//! - `path_arena: Vec<u8>` 存所有新增 path 的原始字节，按到达顺序追加。
//! - `added: Vec<FileEntry>` 中 `path_idx` 字段被复用为 `path_arena` 中的字节 offset。
//!   （在 base 中同一字段是 [`crate::index::path_table::PathTable`] 的逻辑 idx；
//!   两种语义靠"entry 来自哪个容器"区分，merge 时由 [`Self::iter_added_with_path`]
//!   把 delta 的 offset 翻译成最终 PathTable 的 idx。）
//! - `added_path_lens: Vec<u16>` 与 `added` 同长度，给出每条 path 的字节数。
//!
//! ## 内存控制
//! 当 `len(added) + len(removed)` 超过 `max_entries`，调用方应触发一次 snapshot 把
//! delta 合并到 base 然后重置；这是"最大延迟换最小常驻内存"的关键。默认
//! `max_entries = 262_144`（256K），与 §8.6 第二阶段的 `auto_flush` 阈值一致。

use roaring::RoaringTreemap;

use crate::core::FileKey;
use crate::index::file_entry::FileEntry;

/// 默认的增量缓存上限（条目数）。
pub const DEFAULT_MAX_ENTRIES: usize = 262_144;

/// 运行时增量缓存。
#[derive(Debug)]
pub struct DeltaBuffer {
    /// Delta 自有 path arena：append-only `Vec<u8>`。
    path_arena: Vec<u8>,
    /// 自上次 snapshot 以来新增 / 修改的 file entries。
    /// `entry.path_idx` 在这里被复用为 `path_arena` 中的字节 offset。
    /// 顺序保留插入序；按 file_key 排序时由调用方一次性 [`Self::sorted_added_by_file_key`]。
    added: Vec<FileEntry>,
    /// 每条 [`Self::added`] 条目对应 path 的字节数（与 `added` 同长度）。
    added_path_lens: Vec<u16>,
    /// 自上次 snapshot 以来在 base 中被屏蔽的 DocId（删除 / rename-from 的旧 path）。
    removed: RoaringTreemap,
    /// 上次 snapshot 时的 base 文件计数，用于 [`Self::file_count`] 估算。
    base_file_count: u64,
    /// 软上限。超过即应触发 snapshot；本结构不会强制丢弃，仅汇报"满"。
    max_entries: usize,
}

impl DeltaBuffer {
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_MAX_ENTRIES)
    }

    pub fn with_capacity(max_entries: usize) -> Self {
        Self {
            path_arena: Vec::new(),
            added: Vec::new(),
            added_path_lens: Vec::new(),
            removed: RoaringTreemap::new(),
            base_file_count: 0,
            max_entries: max_entries.max(1),
        }
    }

    /// 记录"新增/修改了一个文件"。同 file_key 多次写入会以最新一次为准。
    ///
    /// `path` 不要求字典序、可任意顺序到达；merge 时由 [`Self::sorted_added_by_path`]
    /// 一次性排序。`path_arena` 仅追加，不回收旧条目的 path bytes（是否值得 GC
    /// 取决于 rename 风暴的强度；当前选择简单——max_entries 触发 snapshot 后整体清空）。
    ///
    /// 路径长度超过 `u16::MAX (65535)` 的条目会被静默丢弃并打 warn——与
    /// [`crate::index::l2_partition::PathArena::push_bytes`] 行为一致。
    pub fn upsert_added(
        &mut self,
        file_key: FileKey,
        path: &[u8],
        size: u64,
        mtime_ns: i64,
    ) -> bool {
        let path_len: u16 = match path.len().try_into() {
            Ok(n) => n,
            Err(_) => {
                tracing::warn!(
                    "DeltaBuffer: dropping path longer than {} bytes ({} bytes)",
                    u16::MAX,
                    path.len()
                );
                return false;
            }
        };
        // 1) 写新 path 到 arena，记录 offset。
        let path_off: u32 = match self.path_arena.len().try_into() {
            Ok(n) => n,
            Err(_) => {
                tracing::warn!(
                    "DeltaBuffer: path_arena full ({} bytes); refusing further inserts until snapshot",
                    self.path_arena.len()
                );
                return false;
            }
        };
        self.path_arena.extend_from_slice(path);
        let new_entry = FileEntry::new(file_key, path_off, size, mtime_ns);

        // 2) 同 file_key 已存在：原地覆盖（注意：旧 path 在 arena 里仍占字节但不再被引用，
        //    snapshot/reset 时一并回收）。
        if let Some(pos) = self.added.iter().position(|e| e.file_key() == file_key) {
            self.added[pos] = new_entry;
            self.added_path_lens[pos] = path_len;
        } else {
            self.added.push(new_entry);
            self.added_path_lens.push(path_len);
        }
        true
    }

    /// 强制追加一条 added，**不**检查 file_key 是否已存在。
    /// 用于 `alloc_docid` 这类需要"docid 空间严格 = base_count + added.len()"对齐的写入路径——
    /// [`Self::upsert_added`] 在同 file_key 已存在时会原地替换，会破坏这个对齐。
    pub fn append_added(
        &mut self,
        file_key: FileKey,
        path: &[u8],
        size: u64,
        mtime_ns: i64,
    ) -> bool {
        let path_len: u16 = match path.len().try_into() {
            Ok(n) => n,
            Err(_) => {
                tracing::warn!(
                    "DeltaBuffer: dropping path longer than {} bytes ({} bytes)",
                    u16::MAX,
                    path.len()
                );
                return false;
            }
        };
        let path_off: u32 = match self.path_arena.len().try_into() {
            Ok(n) => n,
            Err(_) => {
                tracing::warn!(
                    "DeltaBuffer: path_arena full ({} bytes); refusing further inserts until snapshot",
                    self.path_arena.len()
                );
                return false;
            }
        };
        self.path_arena.extend_from_slice(path);
        self.added
            .push(FileEntry::new(file_key, path_off, size, mtime_ns));
        self.added_path_lens.push(path_len);
        true
    }

    /// 在指定 idx 位置原地更新 entry——用于 rename / metadata-only 更新这类
    /// "已知 docid 的 entry 改写"场景，保证 docid → idx 对齐不被打乱。
    /// 旧 path 字节仍留在 path_arena（不回收），与 [`Self::upsert_added`] 同等对待。
    /// idx 越界返回 false。
    pub fn update_added_at(
        &mut self,
        idx: usize,
        file_key: FileKey,
        path: &[u8],
        size: u64,
        mtime_ns: i64,
    ) -> bool {
        if idx >= self.added.len() {
            return false;
        }
        let path_len: u16 = match path.len().try_into() {
            Ok(n) => n,
            Err(_) => {
                tracing::warn!(
                    "DeltaBuffer: dropping update for path longer than {} bytes",
                    u16::MAX
                );
                return false;
            }
        };
        let path_off: u32 = match self.path_arena.len().try_into() {
            Ok(n) => n,
            Err(_) => return false,
        };
        self.path_arena.extend_from_slice(path);
        self.added[idx] = FileEntry::new(file_key, path_off, size, mtime_ns);
        self.added_path_lens[idx] = path_len;
        true
    }

    /// 标记 base 中某个 DocId 已被删除/renamed-from。重复 mark 是幂等的。
    pub fn mark_removed(&mut self, base_doc_id: u64) {
        self.removed.insert(base_doc_id);
    }

    /// 取消之前对某 base DocId 的删除标记（例如 delete → recreate-with-same-fk 抵消）。
    pub fn unmark_removed(&mut self, base_doc_id: u64) -> bool {
        self.removed.remove(base_doc_id)
    }

    /// 撤销一次 added：若同 file_key 存在则移除并返回 true。
    /// 注意：被撤的 path bytes 仍留在 arena（不回收），与 upsert 同等对待。
    pub fn drop_added_by_file_key(&mut self, fk: FileKey) -> bool {
        if let Some(pos) = self.added.iter().position(|e| e.file_key() == fk) {
            self.added.swap_remove(pos);
            self.added_path_lens.swap_remove(pos);
            true
        } else {
            false
        }
    }

    /// 已新增 / 修改的条目（保留插入顺序）。
    pub fn added(&self) -> &[FileEntry] {
        &self.added
    }

    /// 解析某条 [`Self::added`] 索引位置对应的 path bytes。
    /// `idx_in_added` 越界返回 None。
    pub fn added_path_bytes(&self, idx_in_added: usize) -> Option<&[u8]> {
        let entry = self.added.get(idx_in_added)?;
        let len = *self.added_path_lens.get(idx_in_added)? as usize;
        let off = entry.path_idx as usize;
        self.path_arena.get(off..off + len)
    }

    /// 在 base 中被屏蔽的 DocId 集合。
    pub fn removed(&self) -> &RoaringTreemap {
        &self.removed
    }

    pub fn base_file_count(&self) -> u64 {
        self.base_file_count
    }

    pub fn set_base_file_count(&mut self, n: u64) {
        self.base_file_count = n;
    }

    pub fn max_entries(&self) -> usize {
        self.max_entries
    }

    /// 当前 delta 条目数（added + removed），不直接对应 RSS，但是触发 snapshot 的依据。
    pub fn len(&self) -> usize {
        self.added.len().saturating_add(self.removed.len() as usize)
    }

    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty()
    }

    /// 是否达到/超过 [`Self::max_entries`]，调用方应把它当作 snapshot 触发信号。
    pub fn is_full(&self) -> bool {
        self.len() >= self.max_entries
    }

    /// 估算当前总文件数（base + added - removed），不会爆出负数。
    pub fn file_count(&self) -> u64 {
        let added = self.added.len() as u64;
        let removed = self.removed.len();
        self.base_file_count
            .saturating_add(added)
            .saturating_sub(removed)
    }

    /// 用于 snapshot：返回按 file_key 排序的 added 拷贝（与原始 added 同序）。
    /// **不**带 path 信息——只用于 file_key 二分场景。需要 path 一起的，请用
    /// [`Self::iter_added_with_path`]。
    pub fn sorted_added_by_file_key(&self) -> Vec<FileEntry> {
        let mut copy = self.added.clone();
        copy.sort_by(FileEntry::cmp_by_file_key);
        copy
    }

    /// 迭代 (entry, path_bytes) 元组，按 added 的插入序输出。
    /// merge 时常用：调用方可以一边读取 path bytes、一边把这些 entry 插进新 base。
    pub fn iter_added_with_path(&self) -> impl Iterator<Item = (FileEntry, &[u8])> + '_ {
        self.added
            .iter()
            .zip(self.added_path_lens.iter())
            .map(move |(entry, &plen)| {
                let off = entry.path_idx as usize;
                let bytes = &self.path_arena[off..off + plen as usize];
                (*entry, bytes)
            })
    }

    /// snapshot 完成后调用：清空 delta（含 path_arena），并把 base 文件计数同步为新值。
    pub fn reset_after_snapshot(&mut self, new_base_file_count: u64) {
        self.added.clear();
        self.added_path_lens.clear();
        self.path_arena.clear();
        self.removed.clear();
        self.base_file_count = new_base_file_count;
    }

    /// 粗略内存占用：path_arena 容量 + added Vec 容量 + lens Vec 容量 + removed bitmap。
    pub fn estimated_bytes(&self) -> u64 {
        let arena_bytes = self.path_arena.capacity() as u64;
        let added_bytes =
            (self.added.capacity() as u64).saturating_mul(std::mem::size_of::<FileEntry>() as u64);
        let lens_bytes = (self.added_path_lens.capacity() as u64).saturating_mul(2);
        let removed_bytes = self.removed.serialized_size() as u64;
        arena_bytes
            .saturating_add(added_bytes)
            .saturating_add(lens_bytes)
            .saturating_add(removed_bytes)
    }
}

impl Default for DeltaBuffer {
    fn default() -> Self {
        Self::new()
    }
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
    fn defaults_have_room() {
        let d = DeltaBuffer::new();
        assert_eq!(d.max_entries(), DEFAULT_MAX_ENTRIES);
        assert!(d.is_empty());
        assert!(!d.is_full());
        assert_eq!(d.file_count(), 0);
    }

    #[test]
    fn upsert_added_then_lookup_path() {
        let mut d = DeltaBuffer::new();
        d.upsert_added(fk(1), b"/a/x.txt", 100, -1);
        d.upsert_added(fk(2), b"/a/y.txt", 200, -1);
        assert_eq!(d.added().len(), 2);
        assert_eq!(d.file_count(), 2);
        assert_eq!(d.added_path_bytes(0).unwrap(), b"/a/x.txt");
        assert_eq!(d.added_path_bytes(1).unwrap(), b"/a/y.txt");
    }

    #[test]
    fn upsert_added_replaces_existing_file_key_with_new_path() {
        let mut d = DeltaBuffer::new();
        d.upsert_added(fk(1), b"/old.txt", 100, -1);
        d.upsert_added(fk(1), b"/new_renamed.txt", 999, -1);
        assert_eq!(d.added().len(), 1);
        // Path 已替换为新值。
        assert_eq!(d.added_path_bytes(0).unwrap(), b"/new_renamed.txt");
        assert_eq!(d.added()[0].size, 999);
        // 旧 path 仍占 path_arena 字节（不回收）—— estimated_bytes 应反映这个。
        assert!(d.estimated_bytes() >= b"/old.txt".len() as u64);
    }

    #[test]
    fn rejects_overlong_path() {
        let mut d = DeltaBuffer::new();
        let huge = vec![b'a'; (u16::MAX as usize) + 1];
        let ok = d.upsert_added(fk(1), &huge, 0, -1);
        assert!(!ok, "should reject u16-overflow path");
        assert_eq!(d.added().len(), 0);
    }

    #[test]
    fn iter_added_with_path_is_in_insert_order() {
        let mut d = DeltaBuffer::new();
        d.upsert_added(fk(3), b"/c", 0, -1);
        d.upsert_added(fk(1), b"/a", 0, -1);
        d.upsert_added(fk(2), b"/b", 0, -1);
        let collected: Vec<(u64, Vec<u8>)> = d
            .iter_added_with_path()
            .map(|(e, p)| (e.file_key().ino, p.to_vec()))
            .collect();
        assert_eq!(
            collected,
            vec![
                (3, b"/c".to_vec()),
                (1, b"/a".to_vec()),
                (2, b"/b".to_vec())
            ]
        );
    }

    #[test]
    fn mark_removed_increments_len_and_file_count() {
        let mut d = DeltaBuffer::new();
        d.set_base_file_count(10);
        d.mark_removed(3);
        d.mark_removed(7);
        assert_eq!(d.removed().len(), 2);
        assert_eq!(d.file_count(), 8);
    }

    #[test]
    fn unmark_removed_cancels_a_prior_remove() {
        let mut d = DeltaBuffer::new();
        d.mark_removed(5);
        assert!(d.unmark_removed(5));
        assert!(!d.unmark_removed(5));
        assert_eq!(d.removed().len(), 0);
    }

    #[test]
    fn drop_added_by_file_key_works() {
        let mut d = DeltaBuffer::new();
        d.upsert_added(fk(1), b"/x", 0, -1);
        d.upsert_added(fk(2), b"/y", 0, -1);
        assert!(d.drop_added_by_file_key(fk(1)));
        assert!(!d.drop_added_by_file_key(fk(1)));
        assert_eq!(d.added().len(), 1);
        assert_eq!(d.added()[0].file_key(), fk(2));
        assert_eq!(d.added_path_bytes(0).unwrap(), b"/y");
    }

    #[test]
    fn is_full_triggers_at_threshold() {
        let mut d = DeltaBuffer::with_capacity(3);
        assert!(!d.is_full());
        d.upsert_added(fk(1), b"/a", 0, -1);
        d.upsert_added(fk(2), b"/b", 0, -1);
        d.mark_removed(99);
        assert!(d.is_full(), "len={}", d.len());
    }

    #[test]
    fn reset_after_snapshot_clears_delta_including_arena() {
        let mut d = DeltaBuffer::new();
        d.set_base_file_count(5);
        d.upsert_added(fk(1), b"/x", 0, -1);
        d.mark_removed(2);
        assert_eq!(d.file_count(), 5);
        let bytes_before = d.estimated_bytes();
        assert!(bytes_before > 0);

        d.reset_after_snapshot(7);
        assert!(d.is_empty());
        assert_eq!(d.base_file_count(), 7);
        assert_eq!(d.file_count(), 7);
        assert!(d.added_path_bytes(0).is_none());
    }

    #[test]
    fn sorted_added_returns_stable_ordered_copy() {
        let mut d = DeltaBuffer::new();
        d.upsert_added(fk(3), b"/c", 0, -1);
        d.upsert_added(fk(1), b"/a", 0, -1);
        d.upsert_added(fk(2), b"/b", 0, -1);
        let sorted = d.sorted_added_by_file_key();
        let inos: Vec<u64> = sorted.iter().map(|e| e.file_key().ino).collect();
        assert_eq!(inos, vec![1, 2, 3]);
        // 原始 added 不被修改（保留插入序）。
        let orig: Vec<u64> = d.added().iter().map(|e| e.file_key().ino).collect();
        assert_eq!(orig, vec![3, 1, 2]);
    }

    #[test]
    fn file_count_does_not_underflow() {
        let mut d = DeltaBuffer::new();
        d.set_base_file_count(2);
        d.mark_removed(0);
        d.mark_removed(1);
        d.mark_removed(2);
        d.mark_removed(3);
        assert_eq!(d.file_count(), 0);
    }
}
