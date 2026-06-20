pub mod base_index;
pub mod case_policy;
pub mod content_filter;
pub mod delta_buffer;
pub mod file_entry_v2;
pub mod l1_cache;
pub mod l2_partition;
pub mod l3_cold;
pub mod mmap_index;
pub mod parent_index;
pub mod path_table_v2;
pub mod tiered;

use crate::core::{FileKey, FileMeta};
use crate::query::Matcher;
use crate::util::pathbuf_from_encoded_vec;

use file_entry_v2::FileEntry;

/// Build a `FileMeta` from a `FileEntry` and its raw encoded path bytes.
///
/// `size`/`ctime`/`atime` are left as zero/`None` — the on-disk entry only
/// carries `mtime`, matching the historical behaviour of the snapshot and
/// base-index decoders.
pub(crate) fn entry_to_meta(entry: &FileEntry, path_bytes: &[u8]) -> FileMeta {
    FileMeta {
        file_key: entry.file_key(),
        path: pathbuf_from_encoded_vec(path_bytes.to_vec()),
        size: 0,
        mtime: if entry.mtime_ns >= 0 {
            Some(std::time::UNIX_EPOCH + std::time::Duration::from_nanos(entry.mtime_ns as u64))
        } else {
            None
        },
        ctime: None,
        atime: None,
        kind: entry.kind(),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PathFreshness {
    Missing,
    Unchanged,
    Changed,
}

/// L2/L3 索引层抽象：为 MergedView 与 mmap/rkyv layer 预留。
///
/// ## 契约（重要）
/// - `query_keys` 返回的 key 顺序必须"越新越靠前"（便于上层 O(1) 先到先得去重）。
/// - `query_keys` 严禁返回 tombstoned keys（删除语义由各层内部保证）。
pub trait IndexLayer: Send + Sync {
    fn query_keys(&self, matcher: &dyn Matcher) -> Vec<FileKey>;
    fn get_meta(&self, key: FileKey) -> Option<FileMeta>;
    fn file_count_estimate(&self) -> usize {
        0
    }
}

pub use l1_cache::L1Cache;
pub use l2_partition::{
    HardlinkGroup, IndexSnapshotV2, IndexSnapshotV3, IndexSnapshotV4, IndexSnapshotV5,
    PersistentIndex, PhysicalDedupeStats,
};
pub use l3_cold::IndexBuilder;
pub use mmap_index::MmapIndex;
pub use parent_index::{ParentIndex, ParentIndexDelta, PathTable};
pub use tiered::TieredIndex;
