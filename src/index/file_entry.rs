//! 紧凑文件条目：替换 `CompactMeta`（参见 `重构方案包/causal-chain-report.md` §8.3.2）。
//!
//! 与现有 [`crate::index::l2_partition::CompactMeta`] 的差异：
//! - 不再存 `(root_id, path_off, path_len)` 三元间接引用，而是改为单一 `path_idx`
//!   指向 [`crate::index::path_table::PathTable`] 的全局 idx。
//! - 字段顺序经过手工排布（dev/ino/generation 内联）以保证 `#[repr(C)]` + 自然对齐
//!   下整体 40 字节，可直接 mmap 读取。
//!   - 注：直接嵌入 [`FileKey`] 会因为 `dev: u64, ino: u64, generation: u32`
//!     的尾部对齐填充把结构体撑到 48 字节；这里通过把 `path_idx`（u32）放在
//!     `generation`（u32）后面来填掉这 4 字节填充，从而压回 40。
//!
//! 本模块**不**接入 `TieredIndex`；它是 §8.6 第二阶段（数据结构层）的脚手架，
//! 等到第二阶段集成步骤（§8.6 第二阶段"修改: src/index/l2_partition.rs"）才会上线。

use std::sync::Arc;

use crate::core::FileKey;

/// 紧凑文件条目。`#[repr(C)]` + 字段对齐保证 mmap 直读。
///
/// 字段总和: 8 + 8 + 4 + 4 + 8 + 8 = 40 字节。
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileEntry {
    /// 文件系统设备号（[`FileKey::dev`]）。
    pub dev: u64,
    /// inode 号（[`FileKey::ino`]）。
    pub ino: u64,
    /// inode 复用代号（[`FileKey::generation`]，ext4 i_generation）。
    pub generation: u32,
    /// 路径在 [`PathTable`] 中的全局 index。放在 `generation` 之后利用 4B 对齐缝。
    ///
    /// [`PathTable`]: crate::index::path_table::PathTable
    pub path_idx: u32,
    /// 文件大小（字节）。
    pub size: u64,
    /// 修改时间（纳秒，自 UNIX_EPOCH）；`-1` 表示未知。
    pub mtime_ns: i64,
}

const _: () = assert!(std::mem::size_of::<FileEntry>() == 40);
const _: () = assert!(std::mem::align_of::<FileEntry>() == 8);

impl FileEntry {
    pub fn new(file_key: FileKey, path_idx: u32, size: u64, mtime_ns: i64) -> Self {
        Self {
            dev: file_key.dev,
            ino: file_key.ino,
            generation: file_key.generation,
            path_idx,
            size,
            mtime_ns,
        }
    }

    /// 重新组装出 [`FileKey`]。在 hot path 调用时编译器一般会内联到字段访问。
    pub fn file_key(&self) -> FileKey {
        FileKey {
            dev: self.dev,
            ino: self.ino,
            generation: self.generation,
        }
    }

    /// 用于 `entries.sort_by(FileEntry::cmp_by_file_key)`。
    pub fn cmp_by_file_key(a: &Self, b: &Self) -> std::cmp::Ordering {
        a.file_key().cmp(&b.file_key())
    }

    /// 用于 `entries.sort_by(FileEntry::cmp_by_path_idx)`。
    pub fn cmp_by_path_idx(a: &Self, b: &Self) -> std::cmp::Ordering {
        a.path_idx.cmp(&b.path_idx)
    }
}

/// 按 `file_key` 升序排序的视图（用于 O(log N) 二分查找文件身份）。
///
/// 内部不再独立持有 `Vec<FileEntry>`：与 [`ByPathIdx`] 共享同一个 `Arc<[FileEntry]>`
/// （以 path_idx 顺序排列），并仅维护一个 `Vec<u32>` 排序置换数组指向条目。
/// 8M 条 / 40 字节 entry 时，单条目副本 = 320MB；改用 perm 后这边只占 32MB。
#[derive(Debug, Clone)]
pub struct ByFileKey {
    /// 与 [`ByPathIdx::entries`] 共享的规范条目数组（按 path_idx 升序）。
    entries: Arc<[FileEntry]>,
    /// 置换数组：`perm[i]` 是 entries 内偏移；按 `entries[perm[i]].file_key()` 升序。
    perm: Vec<u32>,
}

impl Default for ByFileKey {
    fn default() -> Self {
        Self::with_shared(Arc::from([] as [FileEntry; 0]))
    }
}

impl ByFileKey {
    /// 共享构造：从已经按 path_idx 排序的 `Arc<[FileEntry]>` 算 file_key 排序的 perm。
    /// 这是 [`crate::index::base_index::BaseIndex`] 复用 entries 的入口。
    pub fn with_shared(entries: Arc<[FileEntry]>) -> Self {
        let mut perm: Vec<u32> = (0..entries.len() as u32).collect();
        perm.sort_by(|&a, &b| {
            entries[a as usize]
                .file_key()
                .cmp(&entries[b as usize].file_key())
        });
        Self { entries, perm }
    }

    /// 兼容入口：把 owned `Vec<FileEntry>` 装进 Arc 后走 [`Self::with_shared`]。
    /// 调用方若已有 Arc，直接用 `with_shared` 避免再 alloc。
    pub fn build(entries: Vec<FileEntry>) -> Self {
        Self::with_shared(Arc::from(entries))
    }

    /// 共享底层条目（用于 [`ByPathIdx`] 复用）。
    pub fn shared_entries(&self) -> &Arc<[FileEntry]> {
        &self.entries
    }

    pub fn len(&self) -> usize {
        self.perm.len()
    }

    pub fn is_empty(&self) -> bool {
        self.perm.is_empty()
    }

    /// 通过 `file_key` 二分查找，返回 (perm-内偏移, &FileEntry)。
    /// 同 file_key 多条时返回首个出现的位置（perm 内最小下标）。
    pub fn find(&self, key: FileKey) -> Option<(usize, &FileEntry)> {
        match self
            .perm
            .binary_search_by(|&pi| self.entries[pi as usize].file_key().cmp(&key))
        {
            Ok(mut idx) => {
                while idx > 0 && self.entries[self.perm[idx - 1] as usize].file_key() == key {
                    idx -= 1;
                }
                Some((idx, &self.entries[self.perm[idx] as usize]))
            }
            Err(_) => None,
        }
    }

    /// perm 数组字节占用——给 `memory_stats` 用。
    pub fn perm_bytes(&self) -> u64 {
        self.perm.capacity() as u64 * std::mem::size_of::<u32>() as u64
    }
}

/// 按 `path_idx` 升序排序的视图（用于按路径范围扫描，例如 `/home/x/*` 这类前缀查询）。
///
/// 持有规范的 `Arc<[FileEntry]>`——同一份与 [`ByFileKey::shared_entries`] 共享。
#[derive(Debug, Clone)]
pub struct ByPathIdx {
    entries: Arc<[FileEntry]>,
}

impl Default for ByPathIdx {
    fn default() -> Self {
        Self::with_shared(Arc::from([] as [FileEntry; 0]))
    }
}

impl ByPathIdx {
    /// 共享构造：要求 entries 已按 path_idx 升序（caller 保证）。BaseIndex 走 v7 加载或
    /// `BaseIndexBuilder::build` 时这个不变量天然成立——前者 v7 的 entries 本就字典序，
    /// 后者 builder 排序后 path_idx 单调递增。
    pub fn with_shared(entries: Arc<[FileEntry]>) -> Self {
        Self { entries }
    }

    /// 兼容入口：把 owned `Vec<FileEntry>` sort 后装进 Arc。
    pub fn build(mut entries: Vec<FileEntry>) -> Self {
        entries.sort_by(FileEntry::cmp_by_path_idx);
        Self::with_shared(Arc::from(entries))
    }

    /// 共享底层条目。
    pub fn shared_entries(&self) -> &Arc<[FileEntry]> {
        &self.entries
    }

    pub fn entries(&self) -> &[FileEntry] {
        &self.entries
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// 半开区间二分查找：返回 path_idx 在 `[lo, hi)` 区间内的所有条目。
    pub fn range(&self, lo: u32, hi: u32) -> &[FileEntry] {
        if lo >= hi {
            return &[];
        }
        let start = self.entries.partition_point(|e| e.path_idx < lo);
        let end = self.entries.partition_point(|e| e.path_idx < hi);
        &self.entries[start..end]
    }

    pub fn find(&self, path_idx: u32) -> Option<(usize, &FileEntry)> {
        match self.entries.binary_search_by(|e| e.path_idx.cmp(&path_idx)) {
            Ok(idx) => Some((idx, &self.entries[idx])),
            Err(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fk(dev: u64, ino: u64) -> FileKey {
        FileKey {
            dev,
            ino,
            generation: 0,
        }
    }

    fn fe(dev: u64, ino: u64, path_idx: u32) -> FileEntry {
        FileEntry::new(fk(dev, ino), path_idx, 0, -1)
    }

    #[test]
    fn file_entry_size_and_alignment() {
        assert_eq!(std::mem::size_of::<FileEntry>(), 40);
        assert_eq!(std::mem::align_of::<FileEntry>(), 8);
    }

    #[test]
    fn by_file_key_finds_entry() {
        let view = ByFileKey::build(vec![fe(1, 30, 0), fe(1, 10, 1), fe(1, 20, 2), fe(2, 5, 3)]);

        let (_, entry) = view.find(fk(1, 20)).unwrap();
        assert_eq!(entry.path_idx, 2);

        let (_, entry) = view.find(fk(2, 5)).unwrap();
        assert_eq!(entry.path_idx, 3);

        assert!(view.find(fk(99, 99)).is_none());

        // 验证从 FileEntry 重建出的 FileKey 与原始一致。find(min_key) 应命中第一条。
        let (idx, e) = view.find(fk(1, 10)).unwrap();
        assert_eq!(idx, 0);
        assert_eq!(e.file_key(), fk(1, 10));
    }

    #[test]
    fn by_file_key_returns_first_when_duplicate_keys() {
        // duplicate file_key (e.g. hardlinks) — find returns first occurrence.
        let view = ByFileKey::build(vec![fe(1, 10, 5), fe(1, 10, 6), fe(1, 10, 7)]);
        let (idx, entry) = view.find(fk(1, 10)).unwrap();
        assert_eq!(idx, 0);
        assert_eq!(entry.path_idx, 5);
    }

    #[test]
    fn by_path_idx_finds_entry() {
        let view = ByPathIdx::build(vec![fe(1, 10, 30), fe(1, 20, 10), fe(1, 30, 20)]);
        assert_eq!(view.find(20).unwrap().1.path_idx, 20);
        assert_eq!(view.find(10).unwrap().1.path_idx, 10);
        assert!(view.find(99).is_none());
    }

    #[test]
    fn by_path_idx_range_query() {
        let view = ByPathIdx::build(vec![
            fe(1, 10, 1),
            fe(1, 20, 5),
            fe(1, 30, 10),
            fe(1, 40, 15),
            fe(1, 50, 20),
        ]);
        // [5, 15) → path_idx 5 and 10
        let r = view.range(5, 15);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].path_idx, 5);
        assert_eq!(r[1].path_idx, 10);

        let r = view.range(0, 100);
        assert_eq!(r.len(), 5);

        let r = view.range(20, 20);
        assert!(r.is_empty(), "empty range when lo == hi");

        let r = view.range(100, 200);
        assert!(r.is_empty(), "empty range when above all");
    }

    #[test]
    fn views_share_same_data_independently() {
        let entries = vec![fe(2, 5, 99), fe(1, 10, 1), fe(1, 5, 50)];
        let by_key = ByFileKey::build(entries.clone());
        let by_path = ByPathIdx::build(entries);

        // by_key 的 perm 顺序由 find 暴露：min file_key = (1, 5)
        let (_, e) = by_key.find(fk(1, 5)).unwrap();
        assert_eq!(e.path_idx, 50);
        // by_path 仍然给出按 path_idx 排序的 slice
        assert_eq!(by_path.entries()[0].path_idx, 1);
    }

    #[test]
    fn shared_entries_dedup_via_arc() {
        // 共享构造时两个视图必须指向同一份底层 entries——否则 perm 优化无意义。
        use std::sync::Arc;
        let entries: Arc<[FileEntry]> = Arc::from(vec![fe(1, 5, 1), fe(1, 10, 2), fe(2, 5, 3)]);
        let by_key = ByFileKey::with_shared(Arc::clone(&entries));
        let by_path = ByPathIdx::with_shared(Arc::clone(&entries));
        assert!(Arc::ptr_eq(
            by_key.shared_entries(),
            by_path.shared_entries()
        ));
        assert!(Arc::ptr_eq(by_key.shared_entries(), &entries));
    }
}
