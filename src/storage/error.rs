//! Storage layer error types.
//!
//! Provides precise, structured error information for the storage subsystem,
//! replacing the blanket `anyhow::Result` in internal functions with
//! domain-specific variants. Public APIs that are part of trait signatures
//! or heavily used by external callers remain on `anyhow::Result` for
//! compatibility; internal helpers are migrated to [`StorageResult`].
//!
//! ## Migration Status
//!
//! The migration from `anyhow::Result` to `StorageResult` is **incomplete**.
//! Internal helper functions that decode binary segments have been migrated
//! (`decode_path_table`, `decode_tombstones`, `lsm_decode_manifest_body`, etc.).
//! The following categories remain on `anyhow::Result` by design:
//!
//! - **Public trait methods** (`WriteAheadLog`, `StorageBackend`) — trait
//!   signatures use `anyhow::Result` for ecosystem compatibility.
//! - **Top-level entry points** (`SnapshotStore::load_if_valid`,
//!   `lsm_read_manifest`) — they aggregate multiple internal calls and
//!   convert `StorageError` via `?` at the boundary.
//! - **Async writers** (`write_atomic_v5_bincode`, `write_atomic_v6`) —
//!   bincode serialization errors are not yet mapped to `StorageError`.
//!
//! TODO: complete the migration in a future pass, starting with the async
//! writers and then the trait methods.

use thiserror::Error;

/// Result alias for storage operations that use [`StorageError`].
pub type StorageResult<T> = Result<T, StorageError>;

/// Errors that can occur within the storage layer.
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("snapshot file not found: {0}")]
    SnapshotNotFound(std::path::PathBuf),

    #[error("snapshot checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: u32, actual: u32 },

    #[error("snapshot magic number mismatch: {0:#x}")]
    MagicMismatch(u32),

    #[error("snapshot version unsupported: {0}")]
    UnsupportedVersion(u32),

    #[error("snapshot data length mismatch: expected {expected}, got {actual}")]
    LengthMismatch { expected: usize, actual: usize },

    #[error("snapshot state incomplete")]
    Incomplete,

    #[error("WAL error: {0}")]
    Wal(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Serialize(String),

    #[error("deserialization error: {0}")]
    Deserialize(String),

    #[error("data conversion error: {0}")]
    Convert(#[from] std::array::TryFromSliceError),

    #[error("integer conversion error: {0}")]
    IntConvert(#[from] std::num::TryFromIntError),

    #[error("storage corruption: {0}")]
    Corruption(String),

    #[error("LSM error: {0}")]
    Lsm(String),

    #[error("bincode error: {0}")]
    Bincode(String),
}
