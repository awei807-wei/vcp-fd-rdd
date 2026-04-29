//! Storage layer trait abstractions.
//!
//! These traits define the generic interfaces for snapshot/segment storage
//! and write-ahead log, allowing alternative implementations (e.g. in-memory
//! stores for testing) without changing consumer code.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use crate::core::EventRecord;
use crate::index::l2_partition::V6Segments;
use crate::storage::snapshot::{LsmLoadedLayers, LsmSegmentLoaded, MmapSnapshotV6};
use crate::storage::wal::WalReplayResult;

pub type StorageFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

// ---------------------------------------------------------------------------
// SegmentStore – snapshot / LSM segment persistence
// ---------------------------------------------------------------------------

/// Abstraction over segment-based snapshot storage (legacy bincode + v6 mmap +
/// LSM directory layout).
///
/// The synchronous subset covers the read path which is the primary
/// polymorphism point; async write methods remain on the concrete
/// [`super::SnapshotStore`] for now to avoid an `async_trait` dependency.
pub trait SegmentStore {
    /// Root path of this store (the `index.db` or directory path).
    fn path(&self) -> &Path;

    /// Derived LSM directory path (used by watcher event filtering).
    fn derived_lsm_dir_path(&self) -> PathBuf;

    /// Load a v6 mmap snapshot if the on-disk file is valid and roots match.
    fn load_v6_mmap_if_valid(
        &self,
        expected_roots: &[PathBuf],
    ) -> anyhow::Result<Option<MmapSnapshotV6>>;

    /// Validate a legacy snapshot file's header. **M4-C 之后**永远返回 `Ok(None)`：
    /// v2-v5 反序列化已删除，调用方需要走 v6/v7 mmap 或全量重扫。保留这个 hook 方便
    /// 兼容性测试继续验证 header 拒绝行为。
    fn load_if_valid<'a>(&'a self) -> StorageFuture<'a, anyhow::Result<Option<()>>>;

    /// Load the full LSM layer stack (base + deltas) if a valid manifest exists.
    fn load_lsm_if_valid(
        &self,
        expected_roots: &[PathBuf],
    ) -> anyhow::Result<Option<LsmLoadedLayers>>;

    /// Read the `last_build_ns` timestamp from the LSM manifest (cold-start
    /// offline change detection).
    fn lsm_last_build_ns(&self) -> anyhow::Result<Option<u64>>;

    /// Read the `wal_seal_id` from the LSM manifest.
    fn lsm_manifest_wal_seal_id(&self) -> anyhow::Result<u64>;

    /// Remove stale segment files no longer referenced by the manifest.
    fn gc_stale_segments(&self) -> anyhow::Result<usize>;
}

// ---------------------------------------------------------------------------
// WriteAheadLog – append-only event journal
// ---------------------------------------------------------------------------

/// Abstraction over the write-ahead log used to persist file-system events
/// between snapshot checkpoints.
pub trait WriteAheadLog {
    /// Directory that contains the WAL files.
    fn dir(&self) -> &Path;

    /// Append a batch of events to the current WAL file.
    fn append(&self, events: &[EventRecord]) -> anyhow::Result<()>;

    /// Seal the current WAL (rename to `events.wal.seal-<id>`) and open a
    /// fresh one.  Returns the seal id.
    fn seal(&self) -> anyhow::Result<u64>;

    /// Delete sealed WAL files whose seal id ≤ `seal_id`.
    fn cleanup_sealed_up_to(&self, seal_id: u64) -> anyhow::Result<()>;

    /// Replay all events from sealed WALs with id > `checkpoint_seal_id`,
    /// plus the current WAL.
    fn replay_since_seal(&self, checkpoint_seal_id: u64) -> anyhow::Result<WalReplayResult>;
}

// ---------------------------------------------------------------------------
// SegmentWriter / WalFactory – write path persistence abstractions
// ---------------------------------------------------------------------------

/// Async write-side abstraction for LSM snapshot persistence.
pub trait SegmentWriter {
    /// Append the current in-memory delta as a new LSM segment.
    fn append_delta_v6<'a>(
        &'a self,
        segs: &'a V6Segments,
        deleted_paths: &'a [Vec<u8>],
        expected_roots: &'a [PathBuf],
        wal_seal_id: u64,
    ) -> StorageFuture<'a, anyhow::Result<LsmSegmentLoaded>>;

    /// Replace the current LSM base (and an optional compacted delta prefix)
    /// with a freshly written base segment.
    fn replace_base_v6<'a>(
        &'a self,
        segs: &'a V6Segments,
        expected_prev: Option<(u64, Vec<u64>)>,
        expected_roots: &'a [PathBuf],
        wal_seal_id: u64,
    ) -> StorageFuture<'a, anyhow::Result<LsmSegmentLoaded>>;

    /// 写 v7 companion 文件（参见 `重构方案包/causal-chain-report.md` §8.9）。
    /// 失败一律视为 non-fatal——LSM 仍是权威路径。
    /// 默认实现 = no-op，便于内存测试 store 跳过。
    fn write_v7_companion(&self, _bytes: &[u8]) -> anyhow::Result<()> {
        Ok(())
    }

    /// 读 v7 companion 文件，缺失/损坏返回 Ok(None)；失败 fallback 到 LSM。
    /// 默认实现 = 永远没有 v7 companion。
    fn load_v7_companion(&self) -> anyhow::Result<Option<crate::storage::snapshot_v7::V7Snapshot>> {
        Ok(None)
    }

    /// mmap 加载 v7 companion——`path_table.data` 借用 mmap，不复制路径字节。
    /// 缺失/损坏/不支持的 store 返回 Ok(None)；上层会退回到 [`Self::load_v7_companion`]。
    /// 默认实现 = 不支持（None），便于内存测试 store 跳过。
    fn load_v7_companion_mmap(
        &self,
    ) -> anyhow::Result<Option<crate::storage::snapshot_v7::V7Snapshot>> {
        Ok(None)
    }
}

/// Factory trait that opens the project's WAL implementation behind a trait
/// object, so callers don't need to depend on the concrete `WalStore`.
pub trait WalFactory {
    fn open_wal(&self) -> anyhow::Result<std::sync::Arc<dyn WriteAheadLog + Send + Sync>>;
}

/// Unified storage backend abstraction used by the tiered index.
pub trait StorageBackend: SegmentStore + SegmentWriter + WalFactory + Send + Sync {}

impl<T> StorageBackend for T where T: SegmentStore + SegmentWriter + WalFactory + Send + Sync {}

impl<T> SegmentStore for Arc<T>
where
    T: SegmentStore + ?Sized,
{
    fn path(&self) -> &Path {
        self.as_ref().path()
    }

    fn derived_lsm_dir_path(&self) -> PathBuf {
        self.as_ref().derived_lsm_dir_path()
    }

    fn load_v6_mmap_if_valid(
        &self,
        expected_roots: &[PathBuf],
    ) -> anyhow::Result<Option<MmapSnapshotV6>> {
        self.as_ref().load_v6_mmap_if_valid(expected_roots)
    }

    fn load_if_valid<'a>(&'a self) -> StorageFuture<'a, anyhow::Result<Option<()>>> {
        self.as_ref().load_if_valid()
    }

    fn load_lsm_if_valid(
        &self,
        expected_roots: &[PathBuf],
    ) -> anyhow::Result<Option<LsmLoadedLayers>> {
        self.as_ref().load_lsm_if_valid(expected_roots)
    }

    fn lsm_last_build_ns(&self) -> anyhow::Result<Option<u64>> {
        self.as_ref().lsm_last_build_ns()
    }

    fn lsm_manifest_wal_seal_id(&self) -> anyhow::Result<u64> {
        self.as_ref().lsm_manifest_wal_seal_id()
    }

    fn gc_stale_segments(&self) -> anyhow::Result<usize> {
        self.as_ref().gc_stale_segments()
    }
}

impl<T> SegmentWriter for Arc<T>
where
    T: SegmentWriter + ?Sized,
{
    fn append_delta_v6<'a>(
        &'a self,
        segs: &'a V6Segments,
        deleted_paths: &'a [Vec<u8>],
        expected_roots: &'a [PathBuf],
        wal_seal_id: u64,
    ) -> StorageFuture<'a, anyhow::Result<LsmSegmentLoaded>> {
        self.as_ref()
            .append_delta_v6(segs, deleted_paths, expected_roots, wal_seal_id)
    }

    fn replace_base_v6<'a>(
        &'a self,
        segs: &'a V6Segments,
        expected_prev: Option<(u64, Vec<u64>)>,
        expected_roots: &'a [PathBuf],
        wal_seal_id: u64,
    ) -> StorageFuture<'a, anyhow::Result<LsmSegmentLoaded>> {
        self.as_ref()
            .replace_base_v6(segs, expected_prev, expected_roots, wal_seal_id)
    }

    fn write_v7_companion(&self, bytes: &[u8]) -> anyhow::Result<()> {
        self.as_ref().write_v7_companion(bytes)
    }

    fn load_v7_companion(&self) -> anyhow::Result<Option<crate::storage::snapshot_v7::V7Snapshot>> {
        self.as_ref().load_v7_companion()
    }

    fn load_v7_companion_mmap(
        &self,
    ) -> anyhow::Result<Option<crate::storage::snapshot_v7::V7Snapshot>> {
        self.as_ref().load_v7_companion_mmap()
    }
}

impl<T> WalFactory for Arc<T>
where
    T: WalFactory + ?Sized,
{
    fn open_wal(&self) -> anyhow::Result<Arc<dyn WriteAheadLog + Send + Sync>> {
        self.as_ref().open_wal()
    }
}

// ---------------------------------------------------------------------------
// MmapOpen – memory-mapped file creation
// ---------------------------------------------------------------------------

/// Minimal trait for opening a memory-mapped mutable region.
pub trait MmapOpen {
    /// Open (or create) a file at `path` with the given `size` and return a
    /// writable mmap handle.
    fn open_mut(&self, path: &Path, size: u64) -> anyhow::Result<memmap2::MmapMut>;
}
