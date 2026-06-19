//! Shared constants for snapshot format versions V2–V7.
//!
//! Extracted from `snapshot.rs` to break the bidirectional dependency
//! between `snapshot.rs` and `snapshot_legacy.rs`.

/// 索引文件 Header magic
pub(crate) const MAGIC: u32 = 0xFDDD_0002;

/// COMMITTED state flag for snapshot header.
pub(crate) const STATE_COMMITTED: u32 = 0x0000_0001;

/// INCOMPLETE state flag for snapshot header.
pub(crate) const STATE_INCOMPLETE: u32 = 0xFFFF_FFFF;

/// Header size: magic + version + state + data_len + checksum (each u32)
pub(crate) const HEADER_SIZE: usize = 4 + 4 + 4 + 4 + 4;
