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
use crate::storage::quarantine::RootStateRecord;
use crate::storage::snapshot::LsmSegmentLoaded;
use crate::storage::wal::{WalDurability, WalReplayResult};

pub type StorageFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

// ---------------------------------------------------------------------------
// SegmentStore – snapshot / LSM segment persistence
// ---------------------------------------------------------------------------

/// Runtime storage boundary used by the tiered index.
///
/// Legacy v2-v6 snapshot and old LSM readers intentionally stay on the concrete
/// [`super::snapshot::SnapshotStore`] compatibility API. They are not part of
/// this runtime trait so hot startup/query paths cannot accidentally depend on
/// those formats again.
pub trait SegmentStore {
    /// Root path of this store (the `index.db` or directory path).
    fn path(&self) -> &Path;

    /// Derived LSM directory path (used by watcher event filtering).
    fn derived_lsm_dir_path(&self) -> PathBuf;

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

    /// Append root-level offline/online state events to the current WAL file.
    fn append_root_events(&self, _records: &[RootStateRecord]) -> anyhow::Result<()> {
        Ok(())
    }

    /// Seal the current WAL (rename to `events.wal.seal-<id>`) and open a
    /// fresh one.  Returns the seal id.
    fn seal(&self) -> anyhow::Result<u64>;

    /// Delete sealed WAL files whose seal id ≤ `seal_id`.
    fn cleanup_sealed_up_to(&self, seal_id: u64) -> anyhow::Result<()>;

    /// Replay all events from sealed WALs with id > `checkpoint_seal_id`,
    /// plus the current WAL.
    fn replay_since_seal(&self, checkpoint_seal_id: u64) -> anyhow::Result<WalReplayResult>;

    /// Configure WAL fsync behavior. Implementations that do not support this
    /// may keep the default flush-only semantics.
    fn set_durability(&self, _durability: WalDurability) {}

    /// Return current WAL fsync behavior.
    fn durability(&self) -> WalDurability {
        WalDurability::FlushOnly
    }
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
}

/// Factory trait that opens the project's WAL implementation behind a trait
/// object, so callers don't need to depend on the concrete `WalStore`.
pub trait WalFactory {
    fn open_wal(&self) -> anyhow::Result<std::sync::Arc<dyn WriteAheadLog + Send + Sync>>;
}

/// Unified storage backend abstraction used by the tiered index.
pub trait StorageBackend: SegmentStore + WalFactory + Send + Sync {}

impl<T> StorageBackend for T where T: SegmentStore + WalFactory + Send + Sync {}

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
