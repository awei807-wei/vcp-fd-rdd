pub mod l1_cache;
pub mod l2_partition;
pub mod l3_cold;
pub mod mmap_index;
pub mod tiered;

use crate::core::{FileKey, FileMeta};
use crate::query::Matcher;

/// L2/L3 索引层抽象：为 MergedView 与 mmap/rkyv layer 预留。
///
/// ## 契约（重要）
/// - `query_keys` 返回的 key 顺序必须“越新越靠前”（便于上层 O(1) 先到先得去重）。
/// - `query_keys` 严禁返回 tombstoned keys（删除语义由各层内部保证）。
pub trait IndexLayer: Send + Sync {
    fn query_keys(&self, matcher: &dyn Matcher) -> Vec<FileKey>;
    fn get_meta(&self, key: FileKey) -> Option<FileMeta>;
}

pub use l1_cache::L1Cache;
pub use l2_partition::{
    IndexSnapshotV2, IndexSnapshotV3, IndexSnapshotV4, IndexSnapshotV5, PersistentIndex,
};
pub use l3_cold::IndexBuilder;
pub use mmap_index::MmapIndex;
pub use tiered::TieredIndex;
