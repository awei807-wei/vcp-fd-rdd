//! L2 持久索引（`PersistentIndex`）及其快照格式。
//!
//! 本模块原为单一 2200+ 行的"上帝模块"，已按职责拆分为若干子模块：
//!
//! - [`path_arena`]: 路径压缩 arena（`PathArena`）
//! - [`snapshot_format`][]: 快照/段格式定义（`IndexSnapshotV2`–`V5`、`V6Segments`、常量）
//! - [`dedupe`]: hardlink 去重统计（`HardlinkGroup`、`PhysicalDedupeStats`）
//! - [`helpers`]: trigram / mtime / 路径哈希等自由函数与小型辅助类型
//! - [`types`][]: `OneOrManyDocId`
//! - [`construct`]: `PersistentIndex` 的构造与快照加载
//! - [`upsert`]: upsert / mark_deleted / path_freshness 等写入路径
//! - [`query`]: 查询、`IndexLayer` 实现、`to_base_index_data`
//! - [`parent`]: hardlink 分组、`ParentIndex` 重建与目录候选
//! - [`events`]: 事件应用（`apply_events` / `handle_*`）
//! - [`export`][]: 快照/段导出（`export_snapshot_v5`、`export_segments_v6*`）
//! - [`stats`][]: 内存统计、compaction、reset
//! - [`internal`]: trigram / path_hash 增删、docid 查找等内部方法
//!
//! `PersistentIndex` 的私有字段定义在本文件中，所有子模块作为其后代模块均可直接访问。
//! 跨子模块调用的私有方法提升为 `pub(super)`，外部 API 可见性保持不变。

use std::collections::HashMap;
use std::path::PathBuf;

use parking_lot::RwLock;
use roaring::RoaringBitmap;

use crate::index::file_entry_v2::FileEntry;

use self::filekey_index::CompactFileKeyIndex;
use self::parent_path::ParentPathLookup;
use self::path_store::PathStore;
use self::types::OneOrManyDocId;

mod construct;
mod dedupe;
mod events;
mod export;
mod filekey_index;
mod helpers;
mod internal;
mod parent;
mod parent_path;
mod path_arena;
mod path_store;
mod query;
mod snapshot_format;
mod stats;
mod types;
mod upsert;

pub use dedupe::{physical_dedupe_stats_from_metas, HardlinkGroup, PhysicalDedupeStats};
pub use path_arena::PathArena;
pub use snapshot_format::{
    CompactMeta, CompactMetaV4, IndexSnapshotV2, IndexSnapshotV3, IndexSnapshotV4, IndexSnapshotV5,
    V6Segments,
};

pub(crate) use helpers::mtime_to_ns;

/// Trigram：3 字节子串，用于倒排索引加速查询
type Trigram = [u8; 3];

/// DocId：L2 内部紧凑文档编号（posting 的元素类型）
pub type DocId = u32;

/// L2: 持久索引（内存常驻，可直接查询；trigram 倒排加速）
///
/// ## 单路径策略 (Single-Path Policy)
/// 一个 `FileKey(dev, ino)` 只存储一条路径（最先发现的那个）。
/// Hardlink 的其他路径视为"不在索引中"。
/// 理由：简单、可预测、够用。如需多路径支持，需扩展为 `FileKey -> Vec<PathBuf>`。
pub struct PersistentIndex {
    /// root 列表（root_id -> root Path）。root_id=0 固定为 "/" 作为兜底。
    roots: Vec<PathBuf>,
    roots_bytes: Vec<Vec<u8>>,
    /// DocId -> FileEntry
    entries: RwLock<Vec<FileEntry>>,
    /// DocId -> absolute path bytes，连续 arena + 紧凑引用。
    paths: RwLock<PathStore>,
    /// FileKey -> representative DocId.
    ///
    /// This is not the search primary key. Multiple live paths may share the
    /// same FileKey when hardlinks are present.
    filekey_to_docid: RwLock<CompactFileKeyIndex>,

    /// 路径反查：hash(path_bytes) -> DocId（或少量冲突列表）
    path_hash_to_id: RwLock<HashMap<u64, OneOrManyDocId>>,

    /// Trigram 倒排索引：trigram -> RoaringBitmap(DocId)
    trigram_index: RwLock<HashMap<Trigram, RoaringBitmap>>,

    /// 墓碑标记（DocId）
    tombstones: RwLock<RoaringBitmap>,

    /// 脏标记（自上次快照后是否有变更）
    dirty: std::sync::atomic::AtomicBool,
    /// 阶段 2: ParentIndex，替代 for_each_live_meta_in_dirs
    parent_index: RwLock<Option<crate::index::parent_index::ParentIndex>>,
    /// 配套 PathTable，用于将 PathBuf 映射到 path_idx
    parent_path_table: RwLock<Option<ParentPathLookup>>,
}

impl Default for PersistentIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests;
