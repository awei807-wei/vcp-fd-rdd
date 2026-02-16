use crate::index::l2_partition::IndexSnapshotV2;
use crate::index::l2_partition::IndexSnapshotV3;
use crate::index::l2_partition::IndexSnapshotV4;
use crate::index::l2_partition::IndexSnapshotV5;
use crate::index::l2_partition::V6Segments;
use memmap2::Mmap;
use std::collections::HashSet;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::{Path, PathBuf};
use tokio::fs;

/// 索引文件 Header
const MAGIC: u32 = 0xFDDD_0002;
const VERSION_CURRENT: u32 = 6;
const VERSION_COMPAT_V5: u32 = 5;
const VERSION_COMPAT_V4: u32 = 4;
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
    V5(IndexSnapshotV5),
    V4(IndexSnapshotV4),
    V3(IndexSnapshotV3),
    V2(IndexSnapshotV2),
}

/// v6：mmap 段式快照（只读视图）
#[derive(Clone)]
pub struct MmapSnapshotV6 {
    mmap: std::sync::Arc<Mmap>,
    pub roots: Vec<Vec<u8>>,
    pub path_arena: std::ops::Range<usize>,
    pub metas: std::ops::Range<usize>,
    pub trigram_table: std::ops::Range<usize>,
    pub postings_blob: std::ops::Range<usize>,
    pub tombstones: std::ops::Range<usize>,
}

impl std::fmt::Debug for MmapSnapshotV6 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MmapSnapshotV6")
            .field("roots_count", &self.roots.len())
            .field("path_arena_len", &self.path_arena.len())
            .field("metas_len", &self.metas.len())
            .field("trigram_table_len", &self.trigram_table.len())
            .field("postings_blob_len", &self.postings_blob.len())
            .field("tombstones_len", &self.tombstones.len())
            .finish()
    }
}

impl MmapSnapshotV6 {
    pub fn bytes(&self) -> &[u8] {
        self.mmap.as_ref()
    }

    pub fn slice(&self, r: std::ops::Range<usize>) -> &[u8] {
        &self.bytes()[r]
    }

    pub fn path_arena_bytes(&self) -> &[u8] {
        self.slice(self.path_arena.clone())
    }

    pub fn metas_bytes(&self) -> &[u8] {
        self.slice(self.metas.clone())
    }

    pub fn trigram_table_bytes(&self) -> &[u8] {
        self.slice(self.trigram_table.clone())
    }

    pub fn postings_blob_bytes(&self) -> &[u8] {
        self.slice(self.postings_blob.clone())
    }

    pub fn tombstones_bytes(&self) -> &[u8] {
        self.slice(self.tombstones.clone())
    }
}

// v6 manifest（简单二进制，不依赖第三方；后续可替换为 rkyv archived）
const V6_MANIFEST_MAGIC: u32 = 0x5646_444D; // "VFD M" (little-endian)
const V6_MANIFEST_VERSION: u32 = 1;

#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum V6SegKind {
    Roots = 1,
    PathArena = 2,
    Metas = 3,
    TrigramTable = 4,
    PostingsBlob = 5,
    Tombstones = 6,
}

#[derive(Clone, Copy, Debug)]
struct V6SegDesc {
    kind: V6SegKind,
    version: u32,
    offset: u64,
    len: u64,
    checksum: u32,
}

fn align_up(v: usize, a: usize) -> usize {
    (v + (a - 1)) & !(a - 1)
}

fn encode_roots_segment(roots: &[PathBuf]) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;
    let mut roots = roots.to_vec();
    roots.sort_by(|a, b| a.as_os_str().as_bytes().cmp(b.as_os_str().as_bytes()));
    roots.dedup();
    roots.retain(|p| p != Path::new("/"));
    roots.insert(0, PathBuf::from("/"));

    let mut out = Vec::new();
    let count: u16 = roots.len().try_into().unwrap_or(u16::MAX);
    out.extend_from_slice(&count.to_le_bytes());
    for r in roots.iter().take(count as usize) {
        let b = r.as_os_str().as_bytes();
        let len: u16 = b.len().try_into().unwrap_or(u16::MAX);
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&b[..len as usize]);
    }
    out
}

fn decode_roots_segment(mut bytes: &[u8]) -> anyhow::Result<Vec<Vec<u8>>> {
    if bytes.len() < 2 {
        anyhow::bail!("roots segment too small");
    }
    let count = u16::from_le_bytes(bytes[0..2].try_into().unwrap()) as usize;
    bytes = &bytes[2..];
    let mut roots = Vec::with_capacity(count);
    for _ in 0..count {
        if bytes.len() < 2 {
            anyhow::bail!("roots segment truncated");
        }
        let len = u16::from_le_bytes(bytes[0..2].try_into().unwrap()) as usize;
        bytes = &bytes[2..];
        if bytes.len() < len {
            anyhow::bail!("roots segment truncated");
        }
        roots.push(bytes[..len].to_vec());
        bytes = &bytes[len..];
    }
    Ok(roots)
}

fn read_file_range(file: &mut std::fs::File, offset: u64, len: u64) -> anyhow::Result<Vec<u8>> {
    use std::io::Read;

    file.seek(SeekFrom::Start(offset))?;
    let n: usize = len
        .try_into()
        .map_err(|_| anyhow::anyhow!("range too large"))?;
    let mut buf = vec![0u8; n];
    file.read_exact(&mut buf)?;
    Ok(buf)
}

fn compute_file_checksum(file: &mut std::fs::File, offset: u64, len: u64) -> anyhow::Result<u32> {
    use std::io::Read;

    file.seek(SeekFrom::Start(offset))?;

    let mut hasher = SimpleChecksum::new();
    let mut buffer = [0u8; 64 * 1024]; // 64KB stack buffer
    let mut remaining = len;
    while remaining > 0 {
        let to_read = (remaining.min(buffer.len() as u64)) as usize;
        file.read_exact(&mut buffer[..to_read])?;
        hasher.update(&buffer[..to_read]);
        remaining -= to_read as u64;
    }
    Ok(hasher.finalize())
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

    /// 派生出的 LSM 目录布局路径（默认与 legacy index.db 同名的 index.d）。
    ///
    /// 仅用于 watcher 事件过滤（避免 index 写入反哺 watcher）。
    pub fn derived_lsm_dir_path(&self) -> PathBuf {
        self.lsm_dir_path()
    }

    /// 兼容旧布局：当 `self.path` 指向目录（如 index.d）时，legacy db 路径为同名 index.db。
    fn legacy_db_path(&self) -> PathBuf {
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
    fn lsm_dir_path(&self) -> PathBuf {
        if self.path.extension().and_then(|s| s.to_str()) == Some("d") || self.path.is_dir() {
            return self.path.clone();
        }
        self.path.with_extension("d")
    }

    fn lsm_manifest_path(&self) -> PathBuf {
        self.lsm_dir_path().join("MANIFEST.bin")
    }

    /// 读取 LSM manifest 的 last_build_ns（用于冷启动离线变更检测）。
    pub fn lsm_last_build_ns(&self) -> anyhow::Result<Option<u64>> {
        let p = self.lsm_manifest_path();
        if !p.exists() {
            return Ok(None);
        }
        let m = lsm_read_manifest(&p)?;
        Ok(Some(m.last_build_ns))
    }

    /// 读取当前 LSM manifest 的 wal_seal_id（用于 compaction 时保持 checkpoint 不回退）。
    pub fn lsm_manifest_wal_seal_id(&self) -> anyhow::Result<u64> {
        let p = self.lsm_manifest_path();
        if !p.exists() {
            return Ok(0);
        }
        Ok(lsm_read_manifest(&p)?.wal_seal_id)
    }

    fn lsm_seg_db_path(&self, id: u64) -> PathBuf {
        self.lsm_dir_path().join(format!("seg-{id:016x}.db"))
    }

    fn lsm_seg_del_path(&self, id: u64) -> PathBuf {
        self.lsm_dir_path().join(format!("seg-{id:016x}.del"))
    }

    /// Compaction 完成后，清理不再被 manifest 引用的旧 segment 文件。
    ///
    /// 说明:
    /// - 只会删除 LSM 目录下形如 `seg-{id:016x}.db` / `seg-{id:016x}.del` 的文件。
    /// - 删除单个文件失败不会中断（避免 compaction 因清理失败而失败）。
    pub fn gc_stale_segments(&self) -> anyhow::Result<usize> {
        let manifest = lsm_read_manifest(&self.lsm_manifest_path())?;
        let live_ids: HashSet<u64> = std::iter::once(manifest.base_id)
            .chain(manifest.delta_ids.iter().copied())
            .collect();

        let mut removed = 0usize;
        for entry in std::fs::read_dir(self.lsm_dir_path())? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(id) = parse_lsm_seg_id(&name) {
                if !live_ids.contains(&id) {
                    let path = entry.path();
                    match std::fs::remove_file(&path) {
                        Ok(()) => removed += 1,
                        Err(e) => {
                            // 删除失败不应阻断 compaction；保守地记录并继续。
                            tracing::warn!("LSM gc stale segment remove failed: {:?}: {}", path, e);
                        }
                    }
                }
            }
        }

        Ok(removed)
    }

    pub fn load_v6_mmap_if_valid(
        &self,
        expected_roots: &[PathBuf],
    ) -> anyhow::Result<Option<MmapSnapshotV6>> {
        let path = self.legacy_db_path();
        if !path.exists() {
            return Ok(None);
        }

        // 仅同步读取 header（20B），避免把整个文件读入内存。
        //
        // 注意：这里刻意将“校验”与“mmap”解耦。
        // 旧实现会先 mmap 再对整段做 checksum，这会在启动时触碰大量页，推高进程 RSS（Private_Clean）。
        // 新实现先用 read+seek 流式校验（只影响 page cache，不显著进入进程 RSS），校验通过后再 mmap。
        let mut file = std::fs::File::open(&path)?;
        let file_len = file.metadata()?.len();
        let mut header = [0u8; HEADER_SIZE];
        use std::io::Read;
        file.read_exact(&mut header)?;

        let magic = u32::from_le_bytes(header[0..4].try_into()?);
        let version = u32::from_le_bytes(header[4..8].try_into()?);
        let state = u32::from_le_bytes(header[8..12].try_into()?);
        let manifest_len = u32::from_le_bytes(header[12..16].try_into()?) as usize;
        let manifest_checksum = u32::from_le_bytes(header[16..20].try_into()?);

        if magic != MAGIC || version != VERSION_CURRENT {
            return Ok(None);
        }
        if state != STATE_COMMITTED {
            tracing::warn!("Snapshot v6 state INCOMPLETE, ignoring");
            return Ok(None);
        }

        if file_len < (HEADER_SIZE + manifest_len) as u64 {
            tracing::warn!("Snapshot v6 too small, ignoring");
            return Ok(None);
        }

        // read manifest (streaming)
        file.seek(SeekFrom::Start(HEADER_SIZE as u64))?;
        let mut manifest = vec![0u8; manifest_len];
        file.read_exact(&mut manifest)?;
        let computed = simple_checksum(&manifest);
        if computed != manifest_checksum {
            tracing::warn!(
                "Snapshot v6 manifest checksum mismatch: {} != {}",
                computed,
                manifest_checksum
            );
            return Ok(None);
        }

        // parse manifest
        if manifest.len() < 16 {
            tracing::warn!("Snapshot v6 manifest too small");
            return Ok(None);
        }
        let m_magic = u32::from_le_bytes(manifest[0..4].try_into().unwrap());
        let m_ver = u32::from_le_bytes(manifest[4..8].try_into().unwrap());
        let seg_count = u32::from_le_bytes(manifest[8..12].try_into().unwrap()) as usize;
        if m_magic != V6_MANIFEST_MAGIC || m_ver != V6_MANIFEST_VERSION {
            tracing::warn!("Snapshot v6 manifest magic/version mismatch");
            return Ok(None);
        }
        let desc_base = 16;
        let desc_size = 32usize;
        if manifest.len() < desc_base + seg_count * desc_size {
            tracing::warn!("Snapshot v6 manifest truncated");
            return Ok(None);
        }

        let mut segs: std::collections::HashMap<u32, V6SegDesc> = std::collections::HashMap::new();
        for i in 0..seg_count {
            let off = desc_base + i * desc_size;
            let d = &manifest[off..off + desc_size];
            let kind_u32 = u32::from_le_bytes(d[0..4].try_into().unwrap());
            let version = u32::from_le_bytes(d[4..8].try_into().unwrap());
            let offset = u64::from_le_bytes(d[8..16].try_into().unwrap());
            let len = u64::from_le_bytes(d[16..24].try_into().unwrap());
            let checksum = u32::from_le_bytes(d[24..28].try_into().unwrap());
            let kind = match kind_u32 {
                1 => V6SegKind::Roots,
                2 => V6SegKind::PathArena,
                3 => V6SegKind::Metas,
                4 => V6SegKind::TrigramTable,
                5 => V6SegKind::PostingsBlob,
                6 => V6SegKind::Tombstones,
                _ => continue,
            };
            segs.insert(
                kind_u32,
                V6SegDesc {
                    kind,
                    version,
                    offset,
                    len,
                    checksum,
                },
            );
        }

        let required = [
            V6SegKind::Roots as u32,
            V6SegKind::PathArena as u32,
            V6SegKind::Metas as u32,
            V6SegKind::TrigramTable as u32,
            V6SegKind::PostingsBlob as u32,
            V6SegKind::Tombstones as u32,
        ];
        if !required.iter().all(|k| segs.contains_key(k)) {
            tracing::warn!("Snapshot v6 missing required segments");
            return Ok(None);
        }

        fn u64_add_checked(a: u64, b: u64) -> Option<u64> {
            a.checked_add(b)
        }

        let mut ranges: std::collections::HashMap<u32, std::ops::Range<usize>> =
            std::collections::HashMap::new();
        let mut roots_bytes: Option<Vec<u8>> = None;

        for (k, d) in segs.iter() {
            let end_u64 = match u64_add_checked(d.offset, d.len) {
                Some(v) => v,
                None => return Ok(None),
            };
            if end_u64 > file_len {
                tracing::warn!("Snapshot v6 segment out of bounds");
                return Ok(None);
            }

            let start: usize = match usize::try_from(d.offset) {
                Ok(v) => v,
                Err(_) => return Ok(None),
            };
            let len: usize = match usize::try_from(d.len) {
                Ok(v) => v,
                Err(_) => return Ok(None),
            };
            let end = match start.checked_add(len) {
                Some(v) => v,
                None => return Ok(None),
            };

            // streaming checksum (avoid touching mmap pages at startup)
            let c = if d.kind == V6SegKind::Roots {
                let bytes = read_file_range(&mut file, d.offset, d.len)?;
                let c = simple_checksum(&bytes);
                roots_bytes = Some(bytes);
                c
            } else {
                compute_file_checksum(&mut file, d.offset, d.len)?
            };

            if c != d.checksum {
                tracing::warn!("Snapshot v6 segment checksum mismatch (kind={})", k);
                return Ok(None);
            }
            ranges.insert(*k, start..end);
        }

        // roots check
        let roots_expected = encode_roots_segment(expected_roots);
        let roots_bytes = roots_bytes.unwrap_or_default();
        if roots_bytes.as_slice() != roots_expected.as_slice() {
            tracing::warn!("Snapshot v6 roots mismatch, ignoring snapshot");
            return Ok(None);
        }

        let roots = decode_roots_segment(&roots_bytes).unwrap_or_default();

        // finally mmap (keep cold)
        let mmap = unsafe { Mmap::map(&file)? };
        let mmap = std::sync::Arc::new(mmap);

        Ok(Some(MmapSnapshotV6 {
            mmap,
            roots,
            path_arena: ranges.get(&(V6SegKind::PathArena as u32)).unwrap().clone(),
            metas: ranges.get(&(V6SegKind::Metas as u32)).unwrap().clone(),
            trigram_table: ranges
                .get(&(V6SegKind::TrigramTable as u32))
                .unwrap()
                .clone(),
            postings_blob: ranges
                .get(&(V6SegKind::PostingsBlob as u32))
                .unwrap()
                .clone(),
            tombstones: ranges.get(&(V6SegKind::Tombstones as u32)).unwrap().clone(),
        }))
    }

    /// 从任意路径加载 v6（用于 LSM 目录下的 seg-*.db）。
    pub fn load_v6_mmap_from_path_if_valid(
        path: &Path,
        expected_roots: &[PathBuf],
    ) -> anyhow::Result<Option<MmapSnapshotV6>> {
        if !path.exists() {
            return Ok(None);
        }
        let store = SnapshotStore::new(path.to_path_buf());
        store.load_v6_mmap_if_valid(expected_roots)
    }

    /// 加载快照（校验 magic/version/state/checksum）
    pub async fn load_if_valid(&self) -> anyhow::Result<Option<LoadedSnapshot>> {
        let path = self.legacy_db_path();
        if !path.exists() {
            return Ok(None);
        }

        let data = fs::read(&path).await?;
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
        // v6 走 mmap 段式加载路径，不在本方法中处理
        if version == VERSION_CURRENT {
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
        } else if version == VERSION_COMPAT_V4 {
            match bincode::deserialize::<IndexSnapshotV4>(body) {
                Ok(snap) => Ok(Some(LoadedSnapshot::V4(snap))),
                Err(e) => {
                    tracing::warn!("Snapshot v4 deserialize failed: {}", e);
                    Ok(None)
                }
            }
        } else {
            match bincode::deserialize::<IndexSnapshotV5>(body) {
                Ok(snap) => Ok(Some(LoadedSnapshot::V5(snap))),
                Err(e) => {
                    tracing::warn!("Snapshot v5 deserialize failed: {}", e);
                    Ok(None)
                }
            }
        }
    }

    /// 原子写入快照 v5（bincode；兼容保留）
    pub async fn write_atomic_v5_bincode(&self, snap: &IndexSnapshotV5) -> anyhow::Result<()> {
        let path = self.legacy_db_path();
        // 确保目录存在
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
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

    /// 原子写入快照 v6（段式 + mmap + lazy decode）
    pub async fn write_atomic_v6(&self, segs: &V6Segments) -> anyhow::Result<()> {
        let path = self.legacy_db_path();
        // 确保目录存在
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }

        // 组装 segments（顺序固定，便于调试）
        let seg_list: Vec<(V6SegKind, u32, Vec<u8>)> = vec![
            (V6SegKind::Roots, 1, segs.roots_bytes.clone()),
            (V6SegKind::PathArena, 1, segs.path_arena_bytes.clone()),
            (V6SegKind::Metas, 1, segs.metas_bytes.clone()),
            (V6SegKind::TrigramTable, 1, segs.trigram_table_bytes.clone()),
            (V6SegKind::PostingsBlob, 1, segs.postings_blob_bytes.clone()),
            (V6SegKind::Tombstones, 1, segs.tombstones_bytes.clone()),
        ];

        // 先计算 offsets/len/checksum，得到 manifest
        let seg_count = seg_list.len();
        let manifest_len = 16 + seg_count * 32;
        let mut cursor = align_up(HEADER_SIZE + manifest_len, 8);

        let mut descs: Vec<V6SegDesc> = Vec::with_capacity(seg_count);
        for (kind, ver, bytes) in &seg_list {
            let start = cursor;
            let len = bytes.len();
            let checksum = simple_checksum(bytes);
            descs.push(V6SegDesc {
                kind: *kind,
                version: *ver,
                offset: start as u64,
                len: len as u64,
                checksum,
            });
            cursor = align_up(start + len, 8);
        }

        let mut manifest = Vec::with_capacity(manifest_len);
        manifest.extend_from_slice(&V6_MANIFEST_MAGIC.to_le_bytes());
        manifest.extend_from_slice(&V6_MANIFEST_VERSION.to_le_bytes());
        manifest.extend_from_slice(&(seg_count as u32).to_le_bytes());
        manifest.extend_from_slice(&0u32.to_le_bytes()); // reserved
        for d in &descs {
            manifest.extend_from_slice(&(d.kind as u32).to_le_bytes());
            manifest.extend_from_slice(&d.version.to_le_bytes());
            manifest.extend_from_slice(&d.offset.to_le_bytes());
            manifest.extend_from_slice(&d.len.to_le_bytes());
            manifest.extend_from_slice(&d.checksum.to_le_bytes());
            manifest.extend_from_slice(&0u32.to_le_bytes()); // reserved
        }
        debug_assert_eq!(manifest.len(), manifest_len);

        let manifest_checksum = simple_checksum(&manifest);

        let tmp_path = path.with_extension("db.tmp");
        let mut file = std::fs::File::create(&tmp_path)?;

        // 1) INCOMPLETE header（manifest_len/checksum 先置 0）
        {
            let mut header = [0u8; HEADER_SIZE];
            header[0..4].copy_from_slice(&MAGIC.to_le_bytes());
            header[4..8].copy_from_slice(&VERSION_CURRENT.to_le_bytes());
            header[8..12].copy_from_slice(&STATE_INCOMPLETE.to_le_bytes());
            header[12..16].copy_from_slice(&0u32.to_le_bytes());
            header[16..20].copy_from_slice(&0u32.to_le_bytes());
            file.write_all(&header)?;
        }

        // 2) 写 manifest + segments（按 desc offset 对齐补零）
        file.write_all(&manifest)?;
        let mut written = HEADER_SIZE + manifest.len();
        let pad0 = align_up(written, 8) - written;
        if pad0 > 0 {
            file.write_all(&vec![0u8; pad0])?;
            written += pad0;
        }

        for (i, (_kind, _ver, bytes)) in seg_list.into_iter().enumerate() {
            let d = &descs[i];
            let target_off: usize = d.offset.try_into().unwrap_or(written);
            if target_off > written {
                let pad = target_off - written;
                file.write_all(&vec![0u8; pad])?;
                written += pad;
            }
            file.write_all(&bytes)?;
            written += bytes.len();
            let pad = align_up(written, 8) - written;
            if pad > 0 {
                file.write_all(&vec![0u8; pad])?;
                written += pad;
            }
        }

        // 3) COMMITTED header：写入 manifest_len/checksum
        file.seek(SeekFrom::Start(0))?;
        {
            let mut header = [0u8; HEADER_SIZE];
            header[0..4].copy_from_slice(&MAGIC.to_le_bytes());
            header[4..8].copy_from_slice(&VERSION_CURRENT.to_le_bytes());
            header[8..12].copy_from_slice(&STATE_COMMITTED.to_le_bytes());
            header[12..16].copy_from_slice(&(manifest.len() as u32).to_le_bytes());
            header[16..20].copy_from_slice(&manifest_checksum.to_le_bytes());
            file.write_all(&header)?;
        }

        file.sync_all()?;
        std::fs::rename(&tmp_path, &path)?;
        if let Some(parent) = path.parent() {
            if let Ok(dir) = std::fs::File::open(parent) {
                let _ = dir.sync_all();
            }
        }

        tracing::info!(
            "Snapshot v6 written: metas={} bytes={}",
            segs.metas_bytes.len() / 40,
            written
        );
        Ok(())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// LSM directory layout (Manifest + segments)

const LSM_MANIFEST_MAGIC: u32 = 0x314D_534C; // "LSM1" little-endian
const LSM_MANIFEST_VERSION: u32 = 3;
const LSM_MANIFEST_HEADER_SIZE: usize = 4 + 4 + 4 + 4; // magic + ver + body_len + checksum

#[derive(Clone, Debug, Default)]
struct LsmManifest {
    next_id: u64,
    base_id: u64,
    delta_ids: Vec<u64>,
    wal_seal_id: u64,
    /// 上次认为“索引与磁盘现实一致”的时间戳（Unix epoch nanos）。
    ///
    /// 用途：冷启动时用于检测停机期间的离线变更（目录 mtime crawl）。
    last_build_ns: u64,
}

#[derive(Clone, Debug)]
pub struct LsmSegmentLoaded {
    pub id: u64,
    pub snap: MmapSnapshotV6,
    pub deleted_paths: Vec<Vec<u8>>,
}

#[derive(Clone, Debug)]
pub struct LsmLoadedLayers {
    pub base: Option<LsmSegmentLoaded>,
    pub deltas: Vec<LsmSegmentLoaded>,
    pub wal_seal_id: u64,
}

fn lsm_encode_manifest_body(m: &LsmManifest) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + 8 + 4 + m.delta_ids.len() * 8 + 8 + 8);
    out.extend_from_slice(&m.next_id.to_le_bytes());
    out.extend_from_slice(&m.base_id.to_le_bytes());
    let n: u32 = m.delta_ids.len().try_into().unwrap_or(u32::MAX);
    out.extend_from_slice(&n.to_le_bytes());
    for &id in m.delta_ids.iter().take(n as usize) {
        out.extend_from_slice(&id.to_le_bytes());
    }
    out.extend_from_slice(&m.wal_seal_id.to_le_bytes());
    out.extend_from_slice(&m.last_build_ns.to_le_bytes());
    out
}

fn lsm_decode_manifest_body(body: &[u8]) -> anyhow::Result<LsmManifest> {
    if body.len() < 8 + 8 + 4 {
        anyhow::bail!("LSM manifest body too small");
    }
    let next_id = u64::from_le_bytes(body[0..8].try_into()?);
    let base_id = u64::from_le_bytes(body[8..16].try_into()?);
    let n = u32::from_le_bytes(body[16..20].try_into()?) as usize;
    let mut delta_ids = Vec::with_capacity(n);
    let mut off = 20;
    for _ in 0..n {
        if off + 8 > body.len() {
            anyhow::bail!("LSM manifest body truncated");
        }
        let id = u64::from_le_bytes(body[off..off + 8].try_into()?);
        delta_ids.push(id);
        off += 8;
    }
    // v2: trailing wal_seal_id；v1: missing -> 0
    let wal_seal_id = if off + 8 <= body.len() {
        let v = u64::from_le_bytes(body[off..off + 8].try_into()?);
        off += 8;
        v
    } else {
        0
    };
    // v3: trailing last_build_ns；v2/v1: missing -> 0
    let last_build_ns = if off + 8 <= body.len() {
        u64::from_le_bytes(body[off..off + 8].try_into()?)
    } else {
        0
    };
    Ok(LsmManifest {
        next_id,
        base_id,
        delta_ids,
        wal_seal_id,
        last_build_ns,
    })
}

fn lsm_read_manifest(path: &Path) -> anyhow::Result<LsmManifest> {
    use std::io::Read;
    let mut f = std::fs::File::open(path)?;
    let mut hdr = [0u8; LSM_MANIFEST_HEADER_SIZE];
    f.read_exact(&mut hdr)?;
    let magic = u32::from_le_bytes(hdr[0..4].try_into()?);
    let ver = u32::from_le_bytes(hdr[4..8].try_into()?);
    let body_len = u32::from_le_bytes(hdr[8..12].try_into()?) as usize;
    let checksum = u32::from_le_bytes(hdr[12..16].try_into()?);
    if magic != LSM_MANIFEST_MAGIC || !(ver == 1 || ver == 2 || ver == LSM_MANIFEST_VERSION) {
        anyhow::bail!("LSM manifest magic/version mismatch");
    }
    let mut body = vec![0u8; body_len];
    f.read_exact(&mut body)?;
    if simple_checksum(&body) != checksum {
        anyhow::bail!("LSM manifest checksum mismatch");
    }
    lsm_decode_manifest_body(&body)
}

fn now_unix_nanos() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn lsm_write_manifest_atomic(path: &Path, m: &LsmManifest) -> anyhow::Result<()> {
    let body = lsm_encode_manifest_body(m);
    let body_len: u32 = body.len().try_into().unwrap_or(u32::MAX);
    let checksum = simple_checksum(&body);

    let tmp = path.with_extension("bin.tmp");
    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(&LSM_MANIFEST_MAGIC.to_le_bytes())?;
    f.write_all(&LSM_MANIFEST_VERSION.to_le_bytes())?;
    f.write_all(&body_len.to_le_bytes())?;
    f.write_all(&checksum.to_le_bytes())?;
    f.write_all(&body)?;
    f.sync_all()?;
    std::fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        if let Ok(dir) = std::fs::File::open(parent) {
            let _ = dir.sync_all();
        }
    }
    Ok(())
}

fn parse_lsm_seg_id(name: &str) -> Option<u64> {
    let s = name.strip_prefix("seg-")?;
    let s = s.strip_suffix(".db").or_else(|| s.strip_suffix(".del"))?;
    if s.is_empty() || s.len() > 16 {
        return None;
    }
    u64::from_str_radix(s, 16).ok()
}

const LSM_DEL_MAGIC: u32 = 0x314C_4544; // "DEL1"
const LSM_DEL_VERSION: u32 = 1;

fn lsm_write_deleted_paths_atomic(path: &Path, deleted_paths: &[Vec<u8>]) -> anyhow::Result<()> {
    let tmp = path.with_extension("del.tmp");
    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(&LSM_DEL_MAGIC.to_le_bytes())?;
    f.write_all(&LSM_DEL_VERSION.to_le_bytes())?;
    let count: u32 = deleted_paths.len().try_into().unwrap_or(u32::MAX);
    f.write_all(&count.to_le_bytes())?;
    for p in deleted_paths.iter().take(count as usize) {
        let len: u16 = p.len().try_into().unwrap_or(u16::MAX);
        f.write_all(&len.to_le_bytes())?;
        f.write_all(&p[..len as usize])?;
    }
    f.sync_all()?;
    std::fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        if let Ok(dir) = std::fs::File::open(parent) {
            let _ = dir.sync_all();
        }
    }
    Ok(())
}

fn lsm_read_deleted_paths(path: &Path) -> anyhow::Result<Vec<Vec<u8>>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    use std::io::Read;
    let mut f = std::fs::File::open(path)?;
    let mut hdr = [0u8; 12];
    f.read_exact(&mut hdr)?;
    let magic = u32::from_le_bytes(hdr[0..4].try_into()?);
    let ver = u32::from_le_bytes(hdr[4..8].try_into()?);
    let count = u32::from_le_bytes(hdr[8..12].try_into()?) as usize;
    if magic != LSM_DEL_MAGIC || ver != LSM_DEL_VERSION {
        anyhow::bail!("LSM del magic/version mismatch");
    }
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let mut lenb = [0u8; 2];
        if f.read_exact(&mut lenb).is_err() {
            break;
        }
        let len = u16::from_le_bytes(lenb) as usize;
        let mut buf = vec![0u8; len];
        f.read_exact(&mut buf)?;
        out.push(buf);
    }
    Ok(out)
}

impl SnapshotStore {
    /// LSM：加载目录化 segments（base + delta）。
    pub fn load_lsm_if_valid(
        &self,
        expected_roots: &[PathBuf],
    ) -> anyhow::Result<Option<LsmLoadedLayers>> {
        let mpath = self.lsm_manifest_path();
        if !mpath.exists() {
            return Ok(None);
        }

        let manifest = lsm_read_manifest(&mpath)?;
        let mut base = None;
        if manifest.base_id != 0 {
            let id = manifest.base_id;
            let snap =
                Self::load_v6_mmap_from_path_if_valid(&self.lsm_seg_db_path(id), expected_roots)?;
            let Some(snap) = snap else {
                return Ok(None);
            };
            let deleted_paths =
                lsm_read_deleted_paths(&self.lsm_seg_del_path(id)).unwrap_or_default();
            base = Some(LsmSegmentLoaded {
                id,
                snap,
                deleted_paths,
            });
        }

        let mut deltas = Vec::with_capacity(manifest.delta_ids.len());
        for &id in &manifest.delta_ids {
            let snap =
                Self::load_v6_mmap_from_path_if_valid(&self.lsm_seg_db_path(id), expected_roots)?;
            let Some(snap) = snap else {
                tracing::warn!(
                    "LSM delta segment corrupted or invalid, skipping: id={}",
                    id
                );
                continue;
            };
            let deleted_paths =
                lsm_read_deleted_paths(&self.lsm_seg_del_path(id)).unwrap_or_default();
            deltas.push(LsmSegmentLoaded {
                id,
                snap,
                deleted_paths,
            });
        }

        Ok(Some(LsmLoadedLayers {
            base,
            deltas,
            wal_seal_id: manifest.wal_seal_id,
        }))
    }

    /// LSM：追加一个 delta segment（v6 + sidecar .del），并更新 manifest。
    pub async fn lsm_append_delta_v6(
        &self,
        segs: &V6Segments,
        deleted_paths: &[Vec<u8>],
        expected_roots: &[PathBuf],
        wal_seal_id: u64,
    ) -> anyhow::Result<LsmSegmentLoaded> {
        let dir = self.lsm_dir_path();
        fs::create_dir_all(&dir).await?;

        // 读取/初始化 manifest
        let mpath = self.lsm_manifest_path();
        let mut manifest = if mpath.exists() {
            lsm_read_manifest(&mpath)?
        } else {
            LsmManifest {
                next_id: 1,
                base_id: 0,
                delta_ids: Vec::new(),
                wal_seal_id: 0,
                last_build_ns: 0,
            }
        };

        let id = manifest.next_id.max(1);
        manifest.next_id = id.saturating_add(1);
        manifest.delta_ids.push(id);
        manifest.wal_seal_id = wal_seal_id;
        manifest.last_build_ns = now_unix_nanos();

        // 先写 segment 与 sidecar；manifest 最后写入（崩溃时最多留下孤儿段）。
        let seg_path = self.lsm_seg_db_path(id);
        SnapshotStore::new(seg_path.clone())
            .write_atomic_v6(segs)
            .await?;
        lsm_write_deleted_paths_atomic(&self.lsm_seg_del_path(id), deleted_paths)?;
        lsm_write_manifest_atomic(&mpath, &manifest)?;

        let snap = Self::load_v6_mmap_from_path_if_valid(&seg_path, expected_roots)?
            .ok_or_else(|| anyhow::anyhow!("LSM: failed to load freshly written segment"))?;
        Ok(LsmSegmentLoaded {
            id,
            snap,
            deleted_paths: deleted_paths.to_vec(),
        })
    }

    /// LSM：用新的 base segment 替换当前 base+delta（并清空 delta 列表）。
    pub async fn lsm_replace_base_v6(
        &self,
        segs: &V6Segments,
        expected_prev: Option<(u64, Vec<u64>)>,
        expected_roots: &[PathBuf],
        wal_seal_id: u64,
    ) -> anyhow::Result<LsmSegmentLoaded> {
        let dir = self.lsm_dir_path();
        fs::create_dir_all(&dir).await?;

        let mpath = self.lsm_manifest_path();
        let mut manifest = if mpath.exists() {
            lsm_read_manifest(&mpath)?
        } else {
            LsmManifest {
                next_id: 1,
                base_id: 0,
                delta_ids: Vec::new(),
                wal_seal_id: 0,
                last_build_ns: 0,
            }
        };

        if let Some((base_id, delta_ids)) = expected_prev {
            if manifest.base_id != base_id || manifest.delta_ids != delta_ids {
                anyhow::bail!("LSM manifest changed, aborting compaction");
            }
        }

        let id = manifest.next_id.max(1);
        manifest.next_id = id.saturating_add(1);
        manifest.base_id = id;
        manifest.delta_ids.clear();
        manifest.wal_seal_id = wal_seal_id;
        manifest.last_build_ns = now_unix_nanos();

        let seg_path = self.lsm_seg_db_path(id);
        SnapshotStore::new(seg_path.clone())
            .write_atomic_v6(segs)
            .await?;
        lsm_write_deleted_paths_atomic(&self.lsm_seg_del_path(id), &[])?;
        lsm_write_manifest_atomic(&mpath, &manifest)?;

        let snap = Self::load_v6_mmap_from_path_if_valid(&seg_path, expected_roots)?
            .ok_or_else(|| anyhow::anyhow!("LSM: failed to load freshly written base"))?;
        Ok(LsmSegmentLoaded {
            id,
            snap,
            deleted_paths: Vec::new(),
        })
    }
}

/// 简单校验和（非加密，仅用于完整性检测）
fn simple_checksum(data: &[u8]) -> u32 {
    let mut c = SimpleChecksum::new();
    c.update(data);
    c.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{FileKey, FileMeta};
    use crate::index::{MmapIndex, PersistentIndex};
    use crate::query::matcher::create_matcher;

    fn unique_tmp_dir(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("fd-rdd-{}-{}", tag, nanos))
    }

    #[tokio::test]
    async fn v6_segment_snapshot_roundtrip_query() {
        let root = unique_tmp_dir("v6");
        std::fs::create_dir_all(&root).unwrap();

        let idx = PersistentIndex::new_with_roots(vec![root.clone()]);

        let p1 = root.join("alpha_test.txt");
        let p2 = root.join("beta_test.txt");
        std::fs::write(&p1, b"a").unwrap();
        std::fs::write(&p2, b"b").unwrap();

        idx.upsert(FileMeta {
            file_key: FileKey { dev: 1, ino: 1 },
            path: p1.clone(),
            size: 1,
            mtime: None,
        });
        idx.upsert(FileMeta {
            file_key: FileKey { dev: 1, ino: 2 },
            path: p2.clone(),
            size: 1,
            mtime: None,
        });

        let store = SnapshotStore::new(root.join("index.db"));
        let segs = idx.export_segments_v6();
        store.write_atomic_v6(&segs).await.unwrap();

        let snap = store
            .load_v6_mmap_if_valid(&[root.clone()])
            .unwrap()
            .expect("load v6");

        let mmap_idx = MmapIndex::new(snap);
        let m = create_matcher("alpha");
        let r = mmap_idx.query(m.as_ref());
        assert_eq!(r.len(), 1);
        assert!(r[0].path.to_string_lossy().contains("alpha_test"));
    }

    #[tokio::test]
    async fn v6_roots_mismatch_rejects_load() {
        let root = unique_tmp_dir("v6-roots");
        std::fs::create_dir_all(&root).unwrap();

        let idx = PersistentIndex::new_with_roots(vec![root.clone()]);
        idx.upsert(FileMeta {
            file_key: FileKey { dev: 1, ino: 1 },
            path: root.join("alpha_test.txt"),
            size: 1,
            mtime: None,
        });

        let store = SnapshotStore::new(root.join("index.db"));
        let segs = idx.export_segments_v6();
        store.write_atomic_v6(&segs).await.unwrap();

        let other_root = unique_tmp_dir("other");
        let snap = store.load_v6_mmap_if_valid(&[other_root]).unwrap();
        assert!(snap.is_none());
    }

    #[test]
    fn gc_stale_segments_removes_unreferenced_files() {
        let root = unique_tmp_dir("lsm-gc");
        std::fs::create_dir_all(&root).unwrap();

        let store = SnapshotStore::new(root.join("index.db"));
        std::fs::create_dir_all(store.lsm_dir_path()).unwrap();

        let manifest = LsmManifest {
            next_id: 5,
            base_id: 1,
            delta_ids: vec![2],
            wal_seal_id: 0,
            last_build_ns: 1,
        };
        lsm_write_manifest_atomic(&store.lsm_manifest_path(), &manifest).unwrap();

        // live: 1,2; stale: 3,4
        for id in [1u64, 2, 3, 4] {
            std::fs::write(store.lsm_seg_db_path(id), b"db").unwrap();
            std::fs::write(store.lsm_seg_del_path(id), b"del").unwrap();
        }
        std::fs::write(store.lsm_dir_path().join("unrelated.tmp"), b"x").unwrap();

        let removed = store.gc_stale_segments().unwrap();
        assert_eq!(removed, 4);

        assert!(store.lsm_seg_db_path(1).exists());
        assert!(store.lsm_seg_del_path(1).exists());
        assert!(store.lsm_seg_db_path(2).exists());
        assert!(store.lsm_seg_del_path(2).exists());

        assert!(!store.lsm_seg_db_path(3).exists());
        assert!(!store.lsm_seg_del_path(3).exists());
        assert!(!store.lsm_seg_db_path(4).exists());
        assert!(!store.lsm_seg_del_path(4).exists());

        assert!(store.lsm_dir_path().join("unrelated.tmp").exists());
    }
}
