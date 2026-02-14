use crate::index::l2_partition::IndexSnapshotV2;
use crate::index::l2_partition::IndexSnapshotV3;
use crate::index::l2_partition::IndexSnapshotV4;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::{Path, PathBuf};
use tokio::fs;

/// 索引文件 Header
const MAGIC: u32 = 0xFDDD_0002;
const VERSION_CURRENT: u32 = 4;
const VERSION_COMPAT_V3: u32 = 3;
const VERSION_COMPAT_V2: u32 = 2;
const STATE_COMMITTED: u32 = 0x0000_0001;
const STATE_INCOMPLETE: u32 = 0xFFFF_FFFF;
const HEADER_SIZE: usize = 4 + 4 + 4 + 4 + 4; // magic + version + state + data_len + checksum

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
}

#[derive(Clone, Debug)]
pub enum LoadedSnapshot {
    V4(IndexSnapshotV4),
    V3(IndexSnapshotV3),
    V2(IndexSnapshotV2),
}

struct SimpleChecksum {
    hash: u32,
    pending: [u8; 4],
    pending_len: usize,
}

impl SimpleChecksum {
    fn new() -> Self {
        Self {
            hash: 0,
            pending: [0u8; 4],
            pending_len: 0,
        }
    }

    fn update(&mut self, mut data: &[u8]) {
        if self.pending_len > 0 {
            let need = 4 - self.pending_len;
            let take = need.min(data.len());
            self.pending[self.pending_len..self.pending_len + take].copy_from_slice(&data[..take]);
            self.pending_len += take;
            data = &data[take..];

            if self.pending_len == 4 {
                self.process_chunk(self.pending);
                self.pending_len = 0;
                self.pending = [0u8; 4];
            }
        }

        while data.len() >= 4 {
            let chunk: [u8; 4] = data[..4].try_into().expect("slice len checked");
            self.process_chunk(chunk);
            data = &data[4..];
        }

        if !data.is_empty() {
            self.pending[..data.len()].copy_from_slice(data);
            self.pending_len = data.len();
        }
    }

    fn finalize(mut self) -> u32 {
        if self.pending_len > 0 {
            let mut buf = [0u8; 4];
            buf[..self.pending_len].copy_from_slice(&self.pending[..self.pending_len]);
            self.process_chunk(buf);
        }
        self.hash
    }

    fn process_chunk(&mut self, chunk: [u8; 4]) {
        self.hash = self.hash.wrapping_add(u32::from_le_bytes(chunk));
        self.hash = self.hash.rotate_left(7);
    }
}

struct ChecksumWriter<'a, W: Write> {
    inner: &'a mut W,
    checksum: SimpleChecksum,
    bytes: u64,
}

impl<'a, W: Write> ChecksumWriter<'a, W> {
    fn new(inner: &'a mut W) -> Self {
        Self {
            inner,
            checksum: SimpleChecksum::new(),
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

impl SnapshotStore {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 加载快照（校验 magic/version/state/checksum）
    pub async fn load_if_valid(&self) -> anyhow::Result<Option<LoadedSnapshot>> {
        if !self.path.exists() {
            return Ok(None);
        }

        let data = fs::read(&self.path).await?;
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
        if version != VERSION_CURRENT
            && version != VERSION_COMPAT_V3
            && version != VERSION_COMPAT_V2
        {
            tracing::warn!(
                "Snapshot version mismatch: {} not in [{}, {}, {}]",
                version,
                VERSION_COMPAT_V2,
                VERSION_COMPAT_V3,
                VERSION_CURRENT
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
        let computed = simple_checksum(body);
        if computed != stored_checksum {
            tracing::warn!(
                "Snapshot checksum mismatch: {} != {}",
                computed,
                stored_checksum
            );
            return Ok(None);
        }

        if version == VERSION_COMPAT_V2 {
            match bincode::deserialize::<IndexSnapshotV2>(body) {
                Ok(v2) => Ok(Some(LoadedSnapshot::V2(v2))),
                Err(e) => {
                    tracing::warn!("Snapshot v2 deserialize failed: {}", e);
                    Ok(None)
                }
            }
        } else if version == VERSION_COMPAT_V3 {
            match bincode::deserialize::<IndexSnapshotV3>(body) {
                Ok(snap) => Ok(Some(LoadedSnapshot::V3(snap))),
                Err(e) => {
                    tracing::warn!("Snapshot v3 deserialize failed: {}", e);
                    Ok(None)
                }
            }
        } else {
            match bincode::deserialize::<IndexSnapshotV4>(body) {
                Ok(snap) => Ok(Some(LoadedSnapshot::V4(snap))),
                Err(e) => {
                    tracing::warn!("Snapshot v4 deserialize failed: {}", e);
                    Ok(None)
                }
            }
        }
    }

    /// 原子写入快照（纯顺序写，无 seek）
    pub async fn write_atomic(&self, snap: &IndexSnapshotV4) -> anyhow::Result<()> {
        // 确保目录存在
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let tmp_path = self.path.with_extension("db.tmp");

        // 1) 写 INCOMPLETE header（len/checksum 先置 0），然后流式写 body。
        // 这样可避免把整个 body 序列化进一个巨型 Vec，降低峰值内存，缓解 RSS 漂移。
        let mut file = std::fs::File::create(&tmp_path)?;
        {
            let mut header = [0u8; HEADER_SIZE];
            header[0..4].copy_from_slice(&MAGIC.to_le_bytes());
            header[4..8].copy_from_slice(&VERSION_CURRENT.to_le_bytes());
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
            header[4..8].copy_from_slice(&VERSION_CURRENT.to_le_bytes());
            header[8..12].copy_from_slice(&STATE_COMMITTED.to_le_bytes());
            header[12..16].copy_from_slice(&data_len.to_le_bytes());
            header[16..20].copy_from_slice(&checksum.to_le_bytes());
            file.write_all(&header)?;
        }

        // 4) fsync — 确保数据与 header 都落盘
        file.sync_all()?;

        // 4) rename 原子替换（POSIX 保证原子性）
        std::fs::rename(&tmp_path, &self.path)?;

        // 5) fsync(dir) — 确保目录项更新落盘
        if let Some(parent) = self.path.parent() {
            if let Ok(dir) = std::fs::File::open(parent) {
                let _ = dir.sync_all();
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

/// 简单校验和（非加密，仅用于完整性检测）
fn simple_checksum(data: &[u8]) -> u32 {
    let mut c = SimpleChecksum::new();
    c.update(data);
    c.finalize()
}
