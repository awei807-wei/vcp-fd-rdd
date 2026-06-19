use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::core::{FileKey, FileMeta};

use super::path_arena::PathArena;

// ── v6 段导出共享常量 ──

/// 能力哨兵：用于 mmap layer 区分"新段（全组件 trigram）"与"旧段（仅 basename trigram）"。
/// - path 组件不允许包含 NUL，因此 [0,0,0] 不会与真实 trigram 冲突。
/// - posting 置空即可（只用 key 存在性探测）。
pub(super) const TRIGRAM_SENTINEL: [u8; 3] = [0, 0, 0];

/// FileKeyMap header magic.
pub(super) const FKM_MAGIC: [u8; 4] = *b"FKM\0";
/// FileKeyMap header version.
pub(super) const FKM_VERSION: u16 = 1;
#[cfg(not(feature = "rkyv"))]
pub(super) const FKM_FLAG_LEGACY: u16 = 0;
#[cfg(feature = "rkyv")]
pub(super) const FKM_FLAG_RKYV: u16 = 1;

/// `build_all_segments` 产出的全部段 bytes，供 `export_segments_v6` 和
/// `export_segments_v6_to_writer` 共享。
pub(super) struct BuiltSegments {
    pub roots_bytes: Vec<u8>,
    pub path_arena_bytes: Arc<Vec<u8>>,
    pub metas_bytes: Vec<u8>,
    pub tombstones_bytes: Vec<u8>,
    pub trigram_table_bytes: Vec<u8>,
    pub postings_blob_bytes: Vec<u8>,
    pub filekey_map_bytes: Vec<u8>,
}

/// 旧紧凑元数据（v4 快照）：不包含 root_id（存储的是绝对路径字节）
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompactMetaV4 {
    pub file_key: FileKey,
    pub path_off: u32,
    pub path_len: u16,
    pub size: u64,
    pub mtime: Option<std::time::SystemTime>,
}

/// 紧凑元数据（v5 起）：以 DocId 为下标（Vec 紧凑布局）
///
/// - arena 存储 root 相对路径 bytes（不含 root 前缀）
/// - root_id 指向 `PersistentIndex.roots[root_id]`
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompactMeta {
    pub file_key: FileKey,
    pub root_id: u16,
    pub path_off: u32,
    pub path_len: u16,
    pub mtime_ns: i64,
}

/// 旧快照格式 v2（兼容读取）
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct IndexSnapshotV2 {
    pub files: HashMap<FileKey, FileMeta>,
    pub path_to_id: HashMap<PathBuf, FileKey>,
    pub tombstones: HashSet<FileKey>,
}

impl IndexSnapshotV2 {
    pub fn new() -> Self {
        Self {
            files: HashMap::new(),
            path_to_id: HashMap::new(),
            tombstones: HashSet::new(),
        }
    }
}

/// 旧快照格式 v3（兼容读取）：不落盘 path_to_id（可从 files 重建）
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct IndexSnapshotV3 {
    pub files: HashMap<FileKey, FileMeta>,
    pub tombstones: HashSet<FileKey>,
}

impl IndexSnapshotV3 {
    pub fn new() -> Self {
        Self {
            files: HashMap::new(),
            tombstones: HashSet::new(),
        }
    }
}

/// 新快照格式 v4：落盘紧凑布局（arena + metas + tombstones）
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct IndexSnapshotV4 {
    pub arena: PathArena,
    pub metas: Vec<CompactMetaV4>,
    pub tombstones: Vec<u32>,
}

impl IndexSnapshotV4 {
    pub fn new() -> Self {
        Self {
            arena: PathArena::new(),
            metas: Vec::new(),
            tombstones: Vec::new(),
        }
    }
}

/// 新快照格式 v5：落盘紧凑布局（arena(root-relative) + metas(root_id+offset/len) + tombstones）
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexSnapshotV5 {
    /// 用于校验：root 列表（含 "/" 兜底）是否与当前运行时一致
    pub roots_hash: u64,
    pub arena: PathArena,
    pub metas: Vec<CompactMeta>,
    pub tombstones: Vec<u32>,
}

impl IndexSnapshotV5 {
    pub fn new(roots_hash: u64) -> Self {
        Self {
            roots_hash,
            arena: PathArena::new(),
            metas: Vec::new(),
            tombstones: Vec::new(),
        }
    }
}

/// v6 段式快照：由 PersistentIndex 导出为一组"可独立校验"的段（供 storage/snapshot 写入）。
///
/// 说明：
/// - v6 的核心目标是：冷启动 mmap + lazy decode（posting 按需解码）
/// - 这里仅导出段的 raw bytes；物理布局/校验与原子替换由 storage 层负责
#[derive(Clone, Debug)]
pub struct V6Segments {
    pub roots_bytes: Arc<Vec<u8>>,
    pub path_arena_bytes: Arc<Vec<u8>>,
    pub metas_bytes: Arc<Vec<u8>>,
    pub trigram_table_bytes: Arc<Vec<u8>>,
    pub postings_blob_bytes: Arc<Vec<u8>>,
    pub tombstones_bytes: Arc<Vec<u8>>,
    pub filekey_map_bytes: Arc<Vec<u8>>,
}
