//! Legacy V2–V5 snapshot compatibility code.
//!
//! This module isolates the old bincode-based snapshot format (versions 2–5)
//! from the current V6/V7 mmap-segment logic in [`super::snapshot`].  The code
//! is kept for database upgrades from older releases and must not be deleted.

use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;

use crate::index::l2_partition::{
    IndexSnapshotV2, IndexSnapshotV3, IndexSnapshotV4, IndexSnapshotV5, PersistentIndex,
};
use crate::storage::checksum::Crc32c;
use crate::storage::snapshot::SnapshotStore;
use crate::storage::snapshot_common::{HEADER_SIZE, MAGIC, STATE_COMMITTED, STATE_INCOMPLETE};

// ─────────────────────────────────────────────────────────────────────────────
// Legacy version constants
// ─────────────────────────────────────────────────────────────────────────────

pub(crate) const VERSION_COMPAT_V5: u32 = 5;
pub(crate) const VERSION_COMPAT_V4: u32 = 4;
pub(crate) const VERSION_COMPAT_V3: u32 = 3;
pub(crate) const VERSION_COMPAT_V2: u32 = 2;

/// Returns `true` when `version` is one of the legacy V2–V5 snapshot versions.
pub(crate) fn is_legacy_version(version: u32) -> bool {
    version == VERSION_COMPAT_V2
        || version == VERSION_COMPAT_V3
        || version == VERSION_COMPAT_V4
        || version == VERSION_COMPAT_V5
}

// ─────────────────────────────────────────────────────────────────────────────
// LoadedSnapshot enum + LegacySnapshot trait
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub enum LoadedSnapshot {
    V5(IndexSnapshotV5),
    V4(IndexSnapshotV4),
    V3(IndexSnapshotV3),
    V2(IndexSnapshotV2),
}

/// 统一的老版本快照反序列化 trait：用于消除 v2-v5 的复制粘贴式分发。
pub trait LegacySnapshot: Sized {
    const VERSION_NAME: &'static str;
    fn deserialize_bincode(body: &[u8]) -> anyhow::Result<Self>;
    fn into_loaded(self) -> LoadedSnapshot;
}

macro_rules! impl_legacy_snapshot {
    ($ty:ty, $name:expr) => {
        impl LegacySnapshot for $ty {
            const VERSION_NAME: &'static str = $name;
            fn deserialize_bincode(body: &[u8]) -> anyhow::Result<Self> {
                Ok(bincode::deserialize::<$ty>(body)?)
            }
            fn into_loaded(self) -> LoadedSnapshot {
                LoadedSnapshot::from(self)
            }
        }
    };
}

impl_legacy_snapshot!(IndexSnapshotV2, "v2");
impl_legacy_snapshot!(IndexSnapshotV3, "v3");
impl_legacy_snapshot!(IndexSnapshotV4, "v4");
impl_legacy_snapshot!(IndexSnapshotV5, "v5");

impl From<IndexSnapshotV2> for LoadedSnapshot {
    fn from(v: IndexSnapshotV2) -> Self {
        LoadedSnapshot::V2(v)
    }
}
impl From<IndexSnapshotV3> for LoadedSnapshot {
    fn from(v: IndexSnapshotV3) -> Self {
        LoadedSnapshot::V3(v)
    }
}
impl From<IndexSnapshotV4> for LoadedSnapshot {
    fn from(v: IndexSnapshotV4) -> Self {
        LoadedSnapshot::V4(v)
    }
}
impl From<IndexSnapshotV5> for LoadedSnapshot {
    fn from(v: IndexSnapshotV5) -> Self {
        LoadedSnapshot::V5(v)
    }
}

fn load_legacy_snapshot<T: LegacySnapshot>(body: &[u8]) -> anyhow::Result<Option<LoadedSnapshot>> {
    match T::deserialize_bincode(body) {
        Ok(snap) => Ok(Some(snap.into_loaded())),
        Err(e) => {
            tracing::warn!("Snapshot {} deserialize failed: {}", T::VERSION_NAME, e);
            Ok(None)
        }
    }
}

/// Dispatch legacy snapshot loading based on the on-disk version number.
///
/// Called by [`SnapshotStore::load_if_valid`] for V2–V5 snapshots.
pub(crate) fn load_legacy_by_version(
    body: &[u8],
    version: u32,
) -> anyhow::Result<Option<LoadedSnapshot>> {
    match version {
        VERSION_COMPAT_V2 => load_legacy_snapshot::<IndexSnapshotV2>(body),
        VERSION_COMPAT_V3 => load_legacy_snapshot::<IndexSnapshotV3>(body),
        VERSION_COMPAT_V4 => load_legacy_snapshot::<IndexSnapshotV4>(body),
        _ => load_legacy_snapshot::<IndexSnapshotV5>(body),
    }
}

impl LoadedSnapshot {
    /// 将加载出的老版本快照转换为 PersistentIndex，统一消除 tiered/load.rs 里的复制粘贴。
    pub fn into_persistent_index(self, roots: Vec<PathBuf>) -> PersistentIndex {
        match self {
            LoadedSnapshot::V5(snap) => {
                tracing::info!("Loaded index snapshot v5: {} docs", snap.metas.len());
                PersistentIndex::from_snapshot_v5(snap, roots)
            }
            LoadedSnapshot::V4(snap) => {
                tracing::info!("Loaded index snapshot v4: {} docs", snap.metas.len());
                PersistentIndex::from_snapshot_v4(snap, roots)
            }
            LoadedSnapshot::V3(snap) => {
                tracing::info!("Loaded index snapshot v3: {} files", snap.files.len());
                PersistentIndex::from_snapshot_v3(snap, roots)
            }
            LoadedSnapshot::V2(snap) => {
                tracing::info!("Loaded index snapshot v2: {} files", snap.files.len());
                PersistentIndex::from_snapshot_v2(snap, roots)
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ChecksumWriter — only used by write_atomic_v5_bincode
// ─────────────────────────────────────────────────────────────────────────────

pub(crate) struct ChecksumWriter<'a, W: Write> {
    inner: &'a mut W,
    checksum: Crc32c,
    bytes: u64,
}

impl<'a, W: Write> ChecksumWriter<'a, W> {
    fn new(inner: &'a mut W) -> Self {
        Self {
            inner,
            checksum: Crc32c::new(),
            bytes: 0,
        }
    }

    fn finish(self) -> (u64, u32) {
        (self.bytes, self.checksum.finalize())
    }
}

impl<'a, W: Write> Write for ChecksumWriter<'a, W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.checksum.update(&buf[..n]);
        self.bytes += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Legacy V5 bincode writer (impl SnapshotStore)
// ─────────────────────────────────────────────────────────────────────────────

impl SnapshotStore {
    /// 原子写入快照 v5（bincode；兼容保留）
    pub async fn write_atomic_v5_bincode(&self, snap: &IndexSnapshotV5) -> anyhow::Result<()> {
        let path = self.legacy_db_path();

        // 1) 写 INCOMPLETE header（len/checksum 先置 0），然后流式写 body。
        // 这样可避免把整个 body 序列化进一个巨型 Vec，降低峰值内存，缓解 RSS 漂移。
        let data_len: u32 = crate::storage::atomic_write(&path, "db.tmp", |file| {
            {
                let mut header = [0u8; HEADER_SIZE];
                header[0..4].copy_from_slice(&MAGIC.to_le_bytes());
                header[4..8].copy_from_slice(&VERSION_COMPAT_V5.to_le_bytes());
                header[8..12].copy_from_slice(&STATE_INCOMPLETE.to_le_bytes());
                header[12..16].copy_from_slice(&0u32.to_le_bytes()); // data_len placeholder
                header[16..20].copy_from_slice(&0u32.to_le_bytes()); // checksum placeholder
                file.write_all(&header)?;
            }

            // 2) 流式写 body 并计算长度/校验
            let (data_len_u64, checksum) = {
                let mut cw = ChecksumWriter::new(file);
                bincode::serialize_into(&mut cw, snap)?;
                cw.finish()
            };

            let data_len: u32 = data_len_u64
                .try_into()
                .map_err(|_| anyhow::anyhow!("Snapshot too large (>{} bytes)", u32::MAX))?;

            // 3) seek 回开头覆盖 COMMITTED header
            file.seek(SeekFrom::Start(0))?;
            {
                let mut header = [0u8; HEADER_SIZE];
                header[0..4].copy_from_slice(&MAGIC.to_le_bytes());
                header[4..8].copy_from_slice(&VERSION_COMPAT_V5.to_le_bytes());
                header[8..12].copy_from_slice(&STATE_COMMITTED.to_le_bytes());
                header[12..16].copy_from_slice(&data_len.to_le_bytes());
                header[16..20].copy_from_slice(&checksum.to_le_bytes());
                file.write_all(&header)?;
            }

            Ok(data_len)
        })?;

        tracing::info!(
            "Snapshot written: {} files, {} bytes",
            snap.metas.len(),
            HEADER_SIZE + data_len as usize
        );
        Ok(())
    }
}
