use std::path::{Path, PathBuf};

/// 索引文件 Header
pub(crate) const MAGIC: u32 = 0xFDDD_0002;
pub(crate) const VERSION_V6: u32 = 6; // legacy: SimpleChecksum
pub(crate) const VERSION_V7: u32 = 7; // CRC32C (Castagnoli)
pub(crate) const VERSION_CURRENT: u32 = VERSION_V7;
pub(crate) const VERSION_COMPAT_V5: u32 = 5;
pub(crate) const VERSION_COMPAT_V4: u32 = 4;
pub(crate) const VERSION_COMPAT_V3: u32 = 3;
pub(crate) const VERSION_COMPAT_V2: u32 = 2;
pub(crate) const STATE_COMMITTED: u32 = 0x0000_0001;
pub(crate) const STATE_INCOMPLETE: u32 = 0xFFFF_FFFF;
pub(crate) const HEADER_SIZE: usize = 4 + 4 + 4 + 4 + 4; // magic + version + state + data_len + checksum

// Safety guards: prevent memory DoS via corrupted headers/segments.
pub(crate) const MAX_V6_MANIFEST_BYTES: usize = 16 * 1024 * 1024; // 16 MiB
pub(crate) const MAX_V6_ROOTS_SEGMENT_BYTES: u64 = 1024 * 1024; // 1 MiB

/// 原子快照存储（atomic replacement）
///
/// 落盘流程（低峰值，允许 seek）：
/// 1) 写 index.db.tmp 的 INCOMPLETE header（len/checksum 先置 0）
/// 2) `bincode::serialize_into(file)` 流式写 body，并边写边计算 checksum / data_len
/// 3) seek 回开头覆盖 COMMITTED header（写入真实 len/checksum）
/// 4) fsync(tmpfile) — 确保数据落盘
/// 5) rename(tmp, target) — 原子替换（POSIX 保证）
/// 6) fsync(dir) — 确保目录项更新落盘
///
/// 安全性保证：
/// - 如果在步骤 2-3 之间崩溃，tmp 文件可能不完整，但 target 不受影响
/// - 如果在步骤 4 之前崩溃，target 保持旧快照
/// - 如果在步骤 5 之前崩溃，rename 可能丢失但 target 仍是旧快照
/// - 加载时校验 magic + version + data_len + checksum，任何不一致都拒绝
pub struct SnapshotStore {
    path: PathBuf,
    pub(crate) compaction_lock: tokio::sync::Mutex<()>,
}

impl SnapshotStore {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            compaction_lock: tokio::sync::Mutex::new(()),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 派生出的 LSM 目录布局路径（默认与 legacy index.db 同名的 index.d）。
    ///
    /// 仅用于 watcher 事件过滤（避免 index 写入反哺 watcher）。
    pub fn derived_lsm_dir_path(&self) -> PathBuf {
        self.lsm_dir_path()
    }

    /// 兼容旧布局：当 `self.path` 指向目录（如 index.d）时，legacy db 路径为同名 index.db。
    pub(crate) fn legacy_db_path(&self) -> PathBuf {
        if self.path.extension().and_then(|s| s.to_str()) == Some("db") {
            return self.path.clone();
        }
        if self.path.extension().and_then(|s| s.to_str()) == Some("d") {
            return self.path.with_extension("db");
        }
        if self.path.is_dir() {
            return self.path.join("index.db");
        }
        self.path.with_extension("db")
    }

    /// LSM 目录布局路径（默认与 legacy index.db 同名的 index.d）。
    pub(crate) fn lsm_dir_path(&self) -> PathBuf {
        if self.path.extension().and_then(|s| s.to_str()) == Some("d") || self.path.is_dir() {
            return self.path.clone();
        }
        self.path.with_extension("d")
    }
}

// Re-exports from submodules to preserve the public API surface.
pub use crate::storage::lsm::{parse_lsm_seg_id, LsmLoadedLayers, LsmSegmentLoaded};
pub use crate::storage::snapshot_legacy::{LegacySnapshot, LoadedSnapshot};
pub use crate::storage::snapshot_v6::MmapSnapshotV6;

// ---------------------------------------------------------------------------
// SegmentStore trait impl
// ---------------------------------------------------------------------------

impl crate::storage::traits::SegmentStore for SnapshotStore {
    fn path(&self) -> &Path {
        self.path()
    }

    fn derived_lsm_dir_path(&self) -> PathBuf {
        self.derived_lsm_dir_path()
    }

    fn load_v6_mmap_if_valid(
        &self,
        expected_roots: &[PathBuf],
    ) -> anyhow::Result<Option<MmapSnapshotV6>> {
        self.load_v6_mmap_if_valid(expected_roots)
    }

    fn load_if_valid<'a>(
        &'a self,
    ) -> crate::storage::traits::StorageFuture<'a, anyhow::Result<Option<LoadedSnapshot>>> {
        Box::pin(async move { SnapshotStore::load_if_valid(self).await })
    }

    fn load_lsm_if_valid(
        &self,
        expected_roots: &[PathBuf],
    ) -> anyhow::Result<Option<LsmLoadedLayers>> {
        self.load_lsm_if_valid(expected_roots)
    }

    fn lsm_last_build_ns(&self) -> anyhow::Result<Option<u64>> {
        self.lsm_last_build_ns()
    }

    fn lsm_manifest_wal_seal_id(&self) -> anyhow::Result<u64> {
        self.lsm_manifest_wal_seal_id()
    }

    fn gc_stale_segments(&self) -> anyhow::Result<usize> {
        self.gc_stale_segments()
    }
}

impl crate::storage::traits::SegmentWriter for SnapshotStore {
    fn append_delta_v6<'a>(
        &'a self,
        segs: &'a crate::index::l2_partition::V6Segments,
        deleted_paths: &'a [Vec<u8>],
        expected_roots: &'a [PathBuf],
        wal_seal_id: u64,
    ) -> crate::storage::traits::StorageFuture<'a, anyhow::Result<LsmSegmentLoaded>> {
        Box::pin(async move {
            self.lsm_append_delta_v6(segs, deleted_paths, expected_roots, wal_seal_id)
                .await
        })
    }

    fn replace_base_v6<'a>(
        &'a self,
        segs: &'a crate::index::l2_partition::V6Segments,
        expected_prev: Option<(u64, Vec<u64>)>,
        expected_roots: &'a [PathBuf],
        wal_seal_id: u64,
    ) -> crate::storage::traits::StorageFuture<'a, anyhow::Result<LsmSegmentLoaded>> {
        Box::pin(async move {
            self.lsm_replace_base_v6(segs, expected_prev, expected_roots, wal_seal_id)
                .await
        })
    }
}

impl crate::storage::traits::WalFactory for SnapshotStore {
    fn open_wal(
        &self,
    ) -> anyhow::Result<std::sync::Arc<dyn crate::storage::traits::WriteAheadLog + Send + Sync>>
    {
        Ok(std::sync::Arc::new(
            crate::storage::wal::WalStore::open_in_dir(self.derived_lsm_dir_path())?,
        ))
    }
}
