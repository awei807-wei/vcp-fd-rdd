//! 父目录倒排索引：parent_hash → RoaringTreemap of DocIds.
//!
//! 用途
//! - 将“某目录下所有 live 文件”查询从 O(N_total_metas) 降到 O(子树大小 + bitmap 操作)。
//! - 替代 [`PersistentIndex::for_each_live_meta_in_dirs`] 中的全量 metas 扫描
//!   （参见 `重构方案包/causal-chain-report.md` §一·链路 1 与 §三 ★★★★★ 杀伤力位）。
//!
//! 设计权衡
//! - 键采用 64-bit 父路径哈希：与 `PersistentIndex::path_hash_to_id` 共享同一种哈希
//!   函数，便于将“按目录定位文件”和“按文件路径定位 DocId”用同一根索引体系。
//! - 值采用 [`roaring::RoaringTreemap`]：与现有 trigram 倒排同类容器，AND/OR
//!   有 SIMD 路径，且对稀疏/稠密集合都自适应。
//! - 不区分 root：父目录哈希已经是绝对路径字节的哈希，跨 root 不会冲突。
//!
//! 一致性
//! - 调用方负责在新增/删除/改名时维护：
//!   * 新建文件 → `insert(parent_hash, doc_id)`
//!   * 删除文件 → `remove(parent_hash, doc_id)`
//!   * 改名文件 → `rename_parent(old, new, doc_id)`（即使 old==new 也安全）
//! - 本结构内部不读元数据，纯散列表 + bitmap，可独立测试。

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use roaring::RoaringTreemap;

/// 用 `std::collections::hash_map::DefaultHasher` 计算路径字节的 64-bit 哈希。
///
/// 与 `crate::index::l2_partition::path_hash_bytes` 保持一致；当后者重构为
/// 显式哈希器时，这里同步切换即可。
pub fn parent_hash_bytes(bytes: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

/// 父目录倒排索引。
#[derive(Debug, Default)]
pub struct ParentIndex {
    entries: HashMap<u64, RoaringTreemap>,
    /// 已记录的 (parent, doc) 关系数量；便于估算内存与做 invariant 校验。
    total_entries: u64,
}

impl ParentIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// 把 `doc_id` 加到 `parent_hash` 的 bitmap。重复 insert 是幂等的。
    pub fn insert(&mut self, parent_hash: u64, doc_id: u64) {
        let posting = self.entries.entry(parent_hash).or_default();
        if posting.insert(doc_id) {
            self.total_entries = self.total_entries.saturating_add(1);
        }
    }

    /// 从 `parent_hash` 的 bitmap 中移除 `doc_id`。空 bitmap 会被回收，避免长尾内存占用。
    pub fn remove(&mut self, parent_hash: u64, doc_id: u64) {
        if let Some(posting) = self.entries.get_mut(&parent_hash) {
            if posting.remove(doc_id) {
                self.total_entries = self.total_entries.saturating_sub(1);
            }
            if posting.is_empty() {
                self.entries.remove(&parent_hash);
            }
        }
    }

    /// 改名/移动：把 `doc_id` 从 `old` 桶迁到 `new` 桶。
    /// `old == new` 时直接保证 `new` 桶里有 `doc_id`，不重复维护计数。
    pub fn rename_parent(&mut self, old_parent_hash: u64, new_parent_hash: u64, doc_id: u64) {
        if old_parent_hash == new_parent_hash {
            self.insert(new_parent_hash, doc_id);
            return;
        }
        self.remove(old_parent_hash, doc_id);
        self.insert(new_parent_hash, doc_id);
    }

    /// 单目录查询：返回该 parent 下的所有 DocId（含可能已被 tombstone）。
    /// 调用方负责后续与 tombstones 做差集（`bitmap - &tombstones`）。
    pub fn files_in_dir(&self, parent_hash: u64) -> Option<&RoaringTreemap> {
        self.entries.get(&parent_hash)
    }

    /// 多目录查询：返回所有给定 parent 下的 DocId 并集。
    pub fn files_in_dirs<I>(&self, parent_hashes: I) -> RoaringTreemap
    where
        I: IntoIterator<Item = u64>,
    {
        let mut combined = RoaringTreemap::new();
        for h in parent_hashes {
            if let Some(posting) = self.entries.get(&h) {
                combined |= posting;
            }
        }
        combined
    }

    /// 桶数（不同父目录数），用于内存报告。
    pub fn dir_count(&self) -> usize {
        self.entries.len()
    }

    /// (parent, doc) 关系总数，便于做 invariant 校验。
    pub fn total_entries(&self) -> u64 {
        self.total_entries
    }

    /// 粗略内存占用（HashMap 桶 + RoaringTreemap serialized size 估算）。
    pub fn estimated_bytes(&self) -> u64 {
        // Rust 1.x HashMap 每条目约 48-64B（包含 hash + key + value 指针）。
        let map_overhead = (self.entries.capacity() as u64).saturating_mul(56);
        let posting_bytes: u64 = self
            .entries
            .values()
            .map(|bm| bm.serialized_size() as u64)
            .sum();
        map_overhead.saturating_add(posting_bytes)
    }

    /// 清空（用于 rebuild 后重新填充）。
    pub fn clear(&mut self) {
        self.entries.clear();
        self.total_entries = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(s: &str) -> u64 {
        parent_hash_bytes(s.as_bytes())
    }

    #[test]
    fn insert_and_lookup_basic() {
        let mut idx = ParentIndex::new();
        idx.insert(h("/home/a"), 1);
        idx.insert(h("/home/a"), 2);
        idx.insert(h("/home/b"), 3);

        assert_eq!(idx.total_entries(), 3);
        assert_eq!(idx.dir_count(), 2);

        let bm = idx.files_in_dir(h("/home/a")).unwrap();
        assert!(bm.contains(1));
        assert!(bm.contains(2));
        assert!(!bm.contains(3));
    }

    #[test]
    fn insert_idempotent() {
        let mut idx = ParentIndex::new();
        idx.insert(h("/x"), 7);
        idx.insert(h("/x"), 7);
        assert_eq!(idx.total_entries(), 1);
        assert_eq!(idx.files_in_dir(h("/x")).unwrap().len(), 1);
    }

    #[test]
    fn remove_compacts_empty_bucket() {
        let mut idx = ParentIndex::new();
        idx.insert(h("/x"), 1);
        idx.insert(h("/x"), 2);
        idx.remove(h("/x"), 1);
        assert_eq!(idx.total_entries(), 1);
        assert_eq!(idx.dir_count(), 1);

        idx.remove(h("/x"), 2);
        assert_eq!(idx.total_entries(), 0);
        assert_eq!(idx.dir_count(), 0, "empty bucket should be reclaimed");
    }

    #[test]
    fn remove_unknown_is_noop() {
        let mut idx = ParentIndex::new();
        idx.remove(h("/missing"), 5);
        idx.insert(h("/x"), 1);
        idx.remove(h("/x"), 999); // doc absent
        assert_eq!(idx.total_entries(), 1);
    }

    #[test]
    fn rename_moves_docid() {
        let mut idx = ParentIndex::new();
        idx.insert(h("/old"), 42);
        idx.rename_parent(h("/old"), h("/new"), 42);
        assert!(idx.files_in_dir(h("/old")).is_none());
        let new_bm = idx.files_in_dir(h("/new")).unwrap();
        assert!(new_bm.contains(42));
        assert_eq!(idx.total_entries(), 1);
    }

    #[test]
    fn rename_self_is_idempotent() {
        let mut idx = ParentIndex::new();
        idx.insert(h("/d"), 1);
        idx.rename_parent(h("/d"), h("/d"), 1);
        assert_eq!(idx.total_entries(), 1);
        assert!(idx.files_in_dir(h("/d")).unwrap().contains(1));
    }

    #[test]
    fn files_in_dirs_unions_postings() {
        let mut idx = ParentIndex::new();
        idx.insert(h("/a"), 1);
        idx.insert(h("/a"), 2);
        idx.insert(h("/b"), 3);
        idx.insert(h("/c"), 4);

        let union = idx.files_in_dirs([h("/a"), h("/b")]);
        assert_eq!(union.len(), 3);
        assert!(union.contains(1));
        assert!(union.contains(2));
        assert!(union.contains(3));
        assert!(!union.contains(4));
    }

    #[test]
    fn files_in_dirs_handles_missing_keys() {
        let mut idx = ParentIndex::new();
        idx.insert(h("/a"), 1);
        let union = idx.files_in_dirs([h("/a"), h("/nonexistent")]);
        assert_eq!(union.len(), 1);
    }

    #[test]
    fn clear_resets_state() {
        let mut idx = ParentIndex::new();
        idx.insert(h("/x"), 1);
        idx.insert(h("/x"), 2);
        idx.clear();
        assert_eq!(idx.total_entries(), 0);
        assert_eq!(idx.dir_count(), 0);
        assert!(idx.files_in_dir(h("/x")).is_none());
    }

    #[test]
    fn many_files_in_one_dir_uses_bitmap_compression() {
        let mut idx = ParentIndex::new();
        for i in 0..10_000u64 {
            idx.insert(h("/big"), i);
        }
        assert_eq!(idx.total_entries(), 10_000);
        let bm = idx.files_in_dir(h("/big")).unwrap();
        assert_eq!(bm.len(), 10_000);
        // RoaringTreemap with 10k contiguous values compresses to ~< 32KB serialized.
        assert!(idx.estimated_bytes() < 64 * 1024);
    }
}
