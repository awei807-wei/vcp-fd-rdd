//! Storage layer trait abstractions.
//!
//! These traits define the generic interfaces for snapshot/segment storage
//! and write-ahead log, allowing alternative implementations (e.g. in-memory
//! stores for testing) without changing consumer code.

use std::path::{Path, PathBuf};

use crate::core::EventRecord;
use crate::storage::snapshot::{LsmLoadedLayers, MmapSnapshotV6};
use crate::storage::wal::WalReplayResult;

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
    fn replay_since_seal(
        &self,
        checkpoint_seal_id: u64,
    ) -> anyhow::Result<WalReplayResult>;
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
