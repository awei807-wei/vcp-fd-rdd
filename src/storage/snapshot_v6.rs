use crate::index::l2_partition::V6Segments;
use crate::storage::checksum::{Checksum32, Crc32c, SimpleChecksum, simple_checksum};
use crate::storage::snapshot::{
    SnapshotStore, HEADER_SIZE, MAGIC, MAX_V6_MANIFEST_BYTES, MAX_V6_ROOTS_SEGMENT_BYTES,
    STATE_COMMITTED, STATE_INCOMPLETE, VERSION_CURRENT, VERSION_V6, VERSION_V7,
};
use memmap2::Mmap;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

// v6 manifest（简单二进制，不依赖第三方；后续可替换为 rkyv archived）
const V6_MANIFEST_MAGIC: u32 = 0x5646_444D; // "VFD M" (little-endian)
const V6_MANIFEST_VERSION: u32 = 1;

#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum V6SegKind {
    Roots = 1,
    PathArena = 2,
    Metas = 3,
    TrigramTable = 4,
    PostingsBlob = 5,
    Tombstones = 6,
    FileKeyMap = 7,
}

#[derive(Clone, Copy, Debug)]
pub struct V6SegDesc {
    pub kind: V6SegKind,
    pub version: u32,
    pub offset: u64,
    pub len: u64,
    pub checksum: u32,
}

/// v6：mmap 段式快照（只读视图）
#[derive(Clone)]
pub struct MmapSnapshotV6 {
    pub mmap: std::sync::Arc<Mmap>,
    pub roots: Vec<Vec<u8>>,
    pub path_arena: std::ops::Range<usize>,
    pub metas: std::ops::Range<usize>,
    pub trigram_table: std::ops::Range<usize>,
    pub postings_blob: std::ops::Range<usize>,
    pub tombstones: std::ops::Range<usize>,
    pub file_key_map: Option<std::ops::Range<usize>>,
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
            .field(
                "file_key_map_len",
                &self.file_key_map.as_ref().map(|r| r.len()),
            )
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

    pub fn file_key_map_bytes(&self) -> Option<&[u8]> {
        self.file_key_map.clone().map(|r| self.slice(r))
    }
}

pub(crate) fn align_up(v: usize, a: usize) -> usize {
    (v + (a - 1)) & !(a - 1)
}

pub(crate) fn encode_roots_segment(roots: &[PathBuf]) -> Vec<u8> {
    let mut roots = roots.to_vec();
    roots.sort_by(|a, b| {
        a.as_os_str()
            .as_encoded_bytes()
            .cmp(b.as_os_str().as_encoded_bytes())
    });
    roots.dedup();
    roots.retain(|p| p != Path::new("/"));
    // 强制将根目录 "/" 插入为第一个 root。
    // 设计意图：确保根目录始终存在于 root 列表中，这样下游的遍历/查询逻辑
    // 可以依赖该条目进行路径解析，而不会因为用户未显式传入 "/" 而缺失根节点。
    roots.insert(0, PathBuf::from("/"));

    let mut out = Vec::new();
    let count: u16 = roots.len().try_into().unwrap_or(u16::MAX);
    out.extend_from_slice(&count.to_le_bytes());
    for r in roots.iter().take(count as usize) {
        let b = r.as_os_str().as_encoded_bytes();
        let len: u16 = b.len().try_into().unwrap_or(u16::MAX);
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&b[..len as usize]);
    }
    out
}

pub(crate) fn decode_roots_segment(mut bytes: &[u8]) -> anyhow::Result<Vec<Vec<u8>>> {
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

pub(crate) fn read_file_range(file: &mut std::fs::File, offset: u64, len: u64) -> anyhow::Result<Vec<u8>> {
    use std::io::Read;

    file.seek(SeekFrom::Start(offset))?;
    let n: usize = len
        .try_into()
        .map_err(|_| anyhow::anyhow!("range too large"))?;
    let mut buf = vec![0u8; n];
    file.read_exact(&mut buf)?;
    Ok(buf)
}

pub(crate) fn compute_file_checksum_with(
    file: &mut std::fs::File,
    offset: u64,
    len: u64,
    v7_crc32c: bool,
) -> anyhow::Result<u32> {
    use std::io::Read;

    file.seek(SeekFrom::Start(offset))?;

    let mut hasher = if v7_crc32c {
        Checksum32::Crc32c(Crc32c::new())
    } else {
        Checksum32::Simple(SimpleChecksum::new())
    };
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

pub(crate) struct ChecksumWriter<'a, W: Write> {
    inner: &'a mut W,
    checksum: Crc32c,
    bytes: u64,
}

impl<'a, W: Write> ChecksumWriter<'a, W> {
    pub(crate) fn new(inner: &'a mut W) -> Self {
        Self {
            inner,
            checksum: Crc32c::new(),
            bytes: 0,
        }
    }

    pub(crate) fn finish(self) -> (u64, u32) {
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

        if manifest_len > MAX_V6_MANIFEST_BYTES {
            tracing::warn!(
                "Snapshot v6 manifest too large ({} bytes), ignoring",
                manifest_len
            );
            return Ok(None);
        }

        if magic != MAGIC || !(version == VERSION_V6 || version == VERSION_V7) {
            return Ok(None);
        }
        if state != STATE_COMMITTED {
            tracing::warn!("Snapshot v6 state INCOMPLETE, ignoring");
            return Ok(None);
        }

        let v7 = version == VERSION_V7;

        if file_len < (HEADER_SIZE + manifest_len) as u64 {
            tracing::warn!("Snapshot v6 too small, ignoring");
            return Ok(None);
        }

        // read manifest (streaming)
        file.seek(SeekFrom::Start(HEADER_SIZE as u64))?;
        let mut manifest = vec![0u8; manifest_len];
        file.read_exact(&mut manifest)?;
        let computed = if v7 {
            let mut c = Crc32c::new();
            c.update(&manifest);
            c.finalize()
        } else {
            simple_checksum(&manifest)
        };
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
        let Some(required_len) = seg_count
            .checked_mul(desc_size)
            .and_then(|v| v.checked_add(desc_base))
        else {
            tracing::warn!("Snapshot v6 manifest invalid seg_count");
            return Ok(None);
        };
        if manifest.len() < required_len {
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
                7 => V6SegKind::FileKeyMap,
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
                if d.len > MAX_V6_ROOTS_SEGMENT_BYTES {
                    tracing::warn!("Snapshot v6 roots segment too large, ignoring snapshot");
                    return Ok(None);
                }
                let bytes = read_file_range(&mut file, d.offset, d.len)?;
                let c = if v7 {
                    let mut c = Crc32c::new();
                    c.update(&bytes);
                    c.finalize()
                } else {
                    simple_checksum(&bytes)
                };
                roots_bytes = Some(bytes);
                c
            } else {
                compute_file_checksum_with(&mut file, d.offset, d.len, v7)?
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
        // SAFETY: The file is opened read-only and we use map_copy_read_only (MAP_PRIVATE)
        // so that external modifications to the file after mapping do not affect
        // the process memory. The mapping lifetime is tied to the Arc<Mmap>.
        let mmap = unsafe { memmap2::MmapOptions::new().map_copy_read_only(&file)? };
        let mmap = std::sync::Arc::new(mmap);

        let path_arena = match ranges.get(&(V6SegKind::PathArena as u32)) {
            Some(r) => r.clone(),
            None => {
                tracing::warn!("Snapshot v6 missing path_arena segment");
                return Ok(None);
            }
        };
        let metas = match ranges.get(&(V6SegKind::Metas as u32)) {
            Some(r) => r.clone(),
            None => {
                tracing::warn!("Snapshot v6 missing metas segment");
                return Ok(None);
            }
        };
        let trigram_table = match ranges.get(&(V6SegKind::TrigramTable as u32)) {
            Some(r) => r.clone(),
            None => {
                tracing::warn!("Snapshot v6 missing trigram_table segment");
                return Ok(None);
            }
        };
        let postings_blob = match ranges.get(&(V6SegKind::PostingsBlob as u32)) {
            Some(r) => r.clone(),
            None => {
                tracing::warn!("Snapshot v6 missing postings_blob segment");
                return Ok(None);
            }
        };
        let tombstones = match ranges.get(&(V6SegKind::Tombstones as u32)) {
            Some(r) => r.clone(),
            None => {
                tracing::warn!("Snapshot v6 missing tombstones segment");
                return Ok(None);
            }
        };

        Ok(Some(MmapSnapshotV6 {
            mmap,
            roots,
            path_arena,
            metas,
            trigram_table,
            postings_blob,
            tombstones,
            file_key_map: ranges.get(&(V6SegKind::FileKeyMap as u32)).cloned(),
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

    /// 原子写入快照 v6（段式 + mmap + lazy decode）
    pub async fn write_atomic_v6(&self, segs: &V6Segments) -> anyhow::Result<()> {
        let path = self.legacy_db_path();
        // 确保目录存在
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // 组装 segments（顺序固定，便于调试）
        let seg_list: Vec<(V6SegKind, u32, Vec<u8>)> = vec![
            (V6SegKind::Roots, 1, segs.roots_bytes.clone()),
            (V6SegKind::PathArena, 1, segs.path_arena_bytes.clone()),
            (V6SegKind::Metas, 1, segs.metas_bytes.clone()),
            (V6SegKind::TrigramTable, 1, segs.trigram_table_bytes.clone()),
            (V6SegKind::PostingsBlob, 1, segs.postings_blob_bytes.clone()),
            (V6SegKind::Tombstones, 1, segs.tombstones_bytes.clone()),
            (V6SegKind::FileKeyMap, 1, segs.filekey_map_bytes.clone()),
        ];

        // 先计算 offsets/len/checksum，得到 manifest
        let seg_count = seg_list.len();
        let manifest_len = 16 + seg_count * 32;
        let mut cursor = align_up(HEADER_SIZE + manifest_len, 8);

        let mut descs: Vec<V6SegDesc> = Vec::with_capacity(seg_count);
        let v7 = true; // 写入端固定使用 v7 (CRC32C)；读取端兼容 v6/v7。
        for (kind, ver, bytes) in &seg_list {
            let start = cursor;
            let len = bytes.len();
            let checksum = if v7 {
                let mut c = Crc32c::new();
                c.update(bytes);
                c.finalize()
            } else {
                simple_checksum(bytes)
            };
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

        let manifest_checksum = if v7 {
            let mut c = Crc32c::new();
            c.update(&manifest);
            c.finalize()
        } else {
            simple_checksum(&manifest)
        };

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
                if let Err(e) = dir.sync_all() {
                    tracing::warn!("fsync directory failed: {e}");
                }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{FileKey, FileMeta};
    use crate::index::{MmapIndex, PersistentIndex};
    use crate::query::matcher::create_matcher;
    use crate::test_util::unique_tmp_dir;

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
            ctime: None,
            atime: None,
        });
        idx.upsert(FileMeta {
            file_key: FileKey { dev: 1, ino: 2 },
            path: p2.clone(),
            size: 1,
            mtime: None,
            ctime: None,
            atime: None,
        });

        let store = SnapshotStore::new(root.join("index.db"));
        let segs = idx.export_segments_v6();
        store.write_atomic_v6(&segs).await.unwrap();

        let snap = store
            .load_v6_mmap_if_valid(std::slice::from_ref(&root))
            .unwrap()
            .expect("load v6");

        assert!(snap.file_key_map.is_some());

        let mmap_idx = MmapIndex::new(snap);
        let m = create_matcher("alpha", true);
        let r = mmap_idx.query(m.as_ref());
        assert_eq!(r.len(), 1);
        assert!(r[0].path.to_string_lossy().contains("alpha_test"));

        let m2 = mmap_idx
            .get_meta_by_key(FileKey { dev: 1, ino: 1 })
            .expect("get_meta_by_key");
        assert!(m2.path.to_string_lossy().contains("alpha_test"));
    }

    #[tokio::test]
    async fn v6_filekey_map_legacy_without_header_still_loads() {
        let root = unique_tmp_dir("v6-filekey-legacy-nohdr");
        std::fs::create_dir_all(&root).unwrap();

        let idx = PersistentIndex::new_with_roots(vec![root.clone()]);

        let p1 = root.join("alpha_test.txt");
        std::fs::write(&p1, b"a").unwrap();
        idx.upsert(FileMeta {
            file_key: FileKey { dev: 1, ino: 1 },
            path: p1.clone(),
            size: 1,
            mtime: None,
            ctime: None,
            atime: None,
        });

        let store = SnapshotStore::new(root.join("index.db"));
        let mut segs = idx.export_segments_v6();

        // 模拟旧段：FileKeyMap 为裸 20B 定长表（无 magic/header）
        if segs.filekey_map_bytes.len() >= 8 {
            segs.filekey_map_bytes = segs.filekey_map_bytes[8..].to_vec();
        }

        store.write_atomic_v6(&segs).await.unwrap();

        let snap = store
            .load_v6_mmap_if_valid(std::slice::from_ref(&root))
            .unwrap()
            .expect("load v6");

        let mmap_idx = MmapIndex::new(snap);
        let m2 = mmap_idx
            .get_meta_by_key(FileKey { dev: 1, ino: 1 })
            .expect("get_meta_by_key");
        assert!(m2.path.to_string_lossy().contains("alpha_test"));
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
            ctime: None,
            atime: None,
        });

        let store = SnapshotStore::new(root.join("index.db"));
        let segs = idx.export_segments_v6();
        store.write_atomic_v6(&segs).await.unwrap();

        let other_root = unique_tmp_dir("other");
        let snap = store.load_v6_mmap_if_valid(&[other_root]).unwrap();
        assert!(snap.is_none());
    }

    #[tokio::test]
    async fn v6_flip_one_byte_rejects_load() {
        let root = unique_tmp_dir("v6-flip");
        std::fs::create_dir_all(&root).unwrap();

        let idx = PersistentIndex::new_with_roots(vec![root.clone()]);
        let p1 = root.join("alpha_test.txt");
        std::fs::write(&p1, b"a").unwrap();
        idx.upsert(FileMeta {
            file_key: FileKey { dev: 1, ino: 1 },
            path: p1.clone(),
            size: 1,
            mtime: None,
            ctime: None,
            atime: None,
        });

        let store = SnapshotStore::new(root.join("index.db"));
        let segs = idx.export_segments_v6();
        store.write_atomic_v6(&segs).await.unwrap();

        // 人工翻转 metas 段内的 1 个字节，应导致校验失败并拒绝加载。
        let path = store.legacy_db_path();
        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();

        let mut header = [0u8; HEADER_SIZE];
        use std::io::Read;
        f.read_exact(&mut header).unwrap();
        let manifest_len = u32::from_le_bytes(header[12..16].try_into().unwrap()) as usize;
        assert!(manifest_len >= 16 + 32);

        f.seek(SeekFrom::Start(HEADER_SIZE as u64)).unwrap();
        let mut manifest = vec![0u8; manifest_len];
        f.read_exact(&mut manifest).unwrap();

        let seg_count = u32::from_le_bytes(manifest[8..12].try_into().unwrap()) as usize;
        let desc_base = 16usize;
        let desc_size = 32usize;
        assert!(manifest.len() >= desc_base + seg_count * desc_size);

        // metas kind = 3
        let mut target: Option<(u64, u64)> = None;
        for i in 0..seg_count {
            let off = desc_base + i * desc_size;
            let d = &manifest[off..off + desc_size];
            let kind_u32 = u32::from_le_bytes(d[0..4].try_into().unwrap());
            let offset = u64::from_le_bytes(d[8..16].try_into().unwrap());
            let len = u64::from_le_bytes(d[16..24].try_into().unwrap());
            if kind_u32 == V6SegKind::Metas as u32 {
                target = Some((offset, len));
                break;
            }
        }
        let (off, seg_len) = target.expect("metas segment");
        assert!(seg_len > 0);

        let flip_off = off + (seg_len / 2);
        f.seek(SeekFrom::Start(flip_off)).unwrap();
        let mut b = [0u8; 1];
        f.read_exact(&mut b).unwrap();
        b[0] ^= 0xFF;
        f.seek(SeekFrom::Start(flip_off)).unwrap();
        f.write_all(&b).unwrap();
        f.sync_all().unwrap();

        let snap = store.load_v6_mmap_if_valid(std::slice::from_ref(&root)).unwrap();
        assert!(snap.is_none());
    }
}
