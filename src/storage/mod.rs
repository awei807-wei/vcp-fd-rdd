pub mod checksum;
pub mod error;
pub mod mmap;
pub mod quarantine;
pub mod recovery_audit;
pub mod serde;
pub mod snapshot;
pub mod snapshot_legacy;
pub mod snapshot_v7;
pub mod traits;
pub mod wal;

pub use snapshot::SnapshotStore;

use std::path::Path;

/// Open `dir` and fsync it, logging a warning on failure.
///
/// This is the shared tail of every atomic-rename sequence: after `rename`
/// the parent directory entry must be persisted to survive a crash. Failures
/// are logged rather than propagated because, on most filesystems, a failed
/// directory fsync does not indicate data loss for the rename itself.
pub(crate) fn fsync_dir(dir: &Path) {
    match std::fs::File::open(dir) {
        Ok(d) => {
            if let Err(e) = d.sync_all() {
                tracing::warn!("fsync directory failed: {e}");
            }
        }
        Err(e) => {
            tracing::warn!("fsync_dir: could not open directory {:?}: {}", dir, e);
        }
    }
}

/// Atomically replace `path` with content produced by `write_fn`.
///
/// Ensures the parent directory exists, creates a temporary sidecar at
/// `path.with_extension(tmp_ext)`, invokes `write_fn(&mut file)` to fill it,
/// fsyncs the file, renames it into place, and fsyncs the parent directory.
///
/// `write_fn` may perform arbitrary writes (including `seek`-based header
/// patching) and returns an arbitrary value — typically the byte count or
/// `()` — which is passed back to the caller for logging.
pub(crate) fn atomic_write<R, F>(path: &Path, tmp_ext: &str, write_fn: F) -> anyhow::Result<R>
where
    F: FnOnce(&mut std::fs::File) -> anyhow::Result<R>,
{
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension(tmp_ext);
    let mut file = std::fs::File::create(&tmp)?;
    let result = write_fn(&mut file)?;
    file.sync_all()?;
    std::fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        fsync_dir(parent);
    }
    Ok(result)
}
