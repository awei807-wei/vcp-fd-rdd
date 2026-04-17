use crate::index::l2_partition::{
    IndexSnapshotV2, IndexSnapshotV3, IndexSnapshotV4, IndexSnapshotV5, PersistentIndex,
};
use crate::storage::snapshot::{
    SnapshotStore, HEADER_SIZE, MAGIC, STATE_COMMITTED, STATE_INCOMPLETE, VERSION_COMPAT_V2,
    VERSION_COMPAT_V3, VERSION_COMPAT_V4, VERSION_COMPAT_V5,
};
use crate::storage::snapshot_v6::ChecksumWriter;
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;

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

pub fn load_legacy_snapshot<T: LegacySnapshot>(
    body: &[u8],
) -> anyhow::Result<Option<LoadedSnapshot>> {
    match T::deserialize_bincode(body) {
        Ok(snap) => Ok(Some(snap.into_loaded())),
        Err(e) => {
            tracing::warn!("Snapshot {} deserialize failed: {}", T::VERSION_NAME, e);
            Ok(None)
        }
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

impl SnapshotStore {
    /// 加载快照（校验 magic/version/state/checksum）
    pub async fn load_if_valid(&self) -> anyhow::Result<Option<LoadedSnapshot>> {
        let path = self.legacy_db_path();
        if !path.exists() {
            return Ok(None);
        }

        let data = tokio::fs::read(&path).await?;
        if data.len() < HEADER_SIZE {
            tracing::warn!("Snapshot too small, ignoring");
            return Ok(None);
        }

        // 解析 header
        let magic = u32::from_le_bytes(data[0..4].try_into()?);
        let version = u32::from_le_bytes(data[4..8].try_into()?);
        let state = u32::from_le_bytes(data[8..12].try_into()?);
        let data_len = u32::from_le_bytes(data[12..16].try_into()?) as usize;
        let stored_checksum = u32::from_le_bytes(data[16..20].try_into()?);

        if magic != MAGIC {
            tracing::warn!("Snapshot magic mismatch: {:#x} != {:#x}", magic, MAGIC);
            return Ok(None);
        }
        // v6/v7 走 mmap 段式加载路径，不在本方法中处理
        use crate::storage::snapshot::{VERSION_V6, VERSION_V7};
        if version == VERSION_V6 || version == VERSION_V7 {
            return Ok(None);
        }

        if version != VERSION_COMPAT_V5
            && version != VERSION_COMPAT_V4
            && version != VERSION_COMPAT_V3
            && version != VERSION_COMPAT_V2
        {
            tracing::warn!(
                "Snapshot version mismatch: {} not in [{}, {}, {}, {}]",
                version,
                VERSION_COMPAT_V2,
                VERSION_COMPAT_V3,
                VERSION_COMPAT_V4,
                VERSION_COMPAT_V5
            );
            return Ok(None);
        }
        if state != STATE_COMMITTED {
            tracing::warn!("Snapshot state INCOMPLETE, ignoring");
            return Ok(None);
        }

        let body = &data[HEADER_SIZE..];
        if body.len() != data_len {
            tracing::warn!("Snapshot data length mismatch");
            return Ok(None);
        }

        // 校验 checksum（简单 CRC32 替代）
        use crate::storage::checksum::simple_checksum;
        let computed = simple_checksum(body);
        if computed != stored_checksum {
            tracing::warn!(
                "Snapshot checksum mismatch: {} != {}",
                computed,
                stored_checksum
            );
            return Ok(None);
        }

        match version {
            VERSION_COMPAT_V2 => load_legacy_snapshot::<IndexSnapshotV2>(body),
            VERSION_COMPAT_V3 => load_legacy_snapshot::<IndexSnapshotV3>(body),
            VERSION_COMPAT_V4 => load_legacy_snapshot::<IndexSnapshotV4>(body),
            _ => load_legacy_snapshot::<IndexSnapshotV5>(body),
        }
    }

    /// 原子写入快照 v5（bincode；兼容保留）
    pub async fn write_atomic_v5_bincode(&self, snap: &IndexSnapshotV5) -> anyhow::Result<()> {
        let path = self.legacy_db_path();
        // 确保目录存在
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let tmp_path = path.with_extension("db.tmp");

        // 1) 写 INCOMPLETE header（len/checksum 先置 0），然后流式写 body。
        // 这样可避免把整个 body 序列化进一个巨型 Vec，降低峰值内存，缓解 RSS 漂移。
        let mut file = std::fs::File::create(&tmp_path)?;
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
            let mut cw = ChecksumWriter::new(&mut file);
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

        // 4) fsync — 确保数据与 header 都落盘
        file.sync_all()?;

        // 4) rename 原子替换（POSIX 保证原子性）
        std::fs::rename(&tmp_path, &path)?;

        // 5) fsync(dir) — 确保目录项更新落盘
        if let Some(parent) = path.parent() {
            if let Ok(dir) = std::fs::File::open(parent) {
                if let Err(e) = dir.sync_all() {
                    tracing::warn!("fsync directory failed: {e}");
                }
            }
        }

        tracing::info!(
            "Snapshot written: {} files, {} bytes",
            snap.metas.len(),
            HEADER_SIZE + data_len as usize
        );
        Ok(())
    }
}
