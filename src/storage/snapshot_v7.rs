use memmap2::Mmap;
use roaring::RoaringBitmap;
use std::collections::HashMap;
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

use crate::index::base_index::{BaseIndexData, FileEntryIndex, TrigramIndex};
use crate::index::file_entry_v2::FileEntry;
use crate::index::parent_index::ParentIndex;
use crate::index::path_table_v2::{PathTableBuilder, PathTableV2};
use crate::storage::checksum::{crc32c_checksum, Crc32c};

// ─────────────────────────────────────────────────────────────────────────────
// v7 单文件 mmap 格式常量
// ─────────────────────────────────────────────────────────────────────────────

const V7_MAGIC: [u8; 8] = *b"FDRDDv7\0";
const V7_VERSION: u32 = 1;
const V7_TRAILER_MAGIC: [u8; 8] = *b"TRAILv7\0";

/// Header: 64 字节，固定大小，对齐到 8 字节。
///
/// Layout:
///   magic          [u8; 8]  = "FDRDDv7\0"
///   version        u32      = 1
///   flags          u32      = 0
///   num_segments   u32
///   header_crc32c  u32      (覆盖 header [0..56])
///   reserved       [u32; 8]
const V7_HEADER_SIZE: usize = 64;

/// Trailer 固定尾部大小（不含变长段表）。
///   num_segments   u32
///   pad            u32
///   global_crc32c  u32
///   pad2           u32
///   trailer_len    u64
///   trailer_magic  [u8; 8]
const V7_TRAILER_FIXED_SIZE: usize = 4 + 4 + 4 + 4 + 8 + 8;

// ─────────────────────────────────────────────────────────────────────────────
// 段种类
// ─────────────────────────────────────────────────────────────────────────────

#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum V7SegKind {
    PathTable = 1,
    EntriesByKey = 2,
    EntriesByPath = 3,
    TrigramIndex = 4,
    ParentIndex = 5,
    Tombstones = 6,
}

#[derive(Clone, Copy, Debug)]
struct V7SegDesc {
    offset: u64,
    len: u64,
    crc32c: u32,
}

// ─────────────────────────────────────────────────────────────────────────────
// PathTable 序列化 / 反序列化（不依赖 serde，避免修改 pathtable.rs）
// ─────────────────────────────────────────────────────────────────────────────

fn encode_path_table(pt: &PathTableV2) -> Vec<u8> {
    pt.encode_raw()
}

fn decode_path_table(bytes: &[u8]) -> anyhow::Result<PathTableV2> {
    if let Some(table) = PathTableV2::decode_raw(bytes) {
        return Ok(table);
    }
    if bytes.len() < 4 {
        anyhow::bail!("path table too small");
    }
    let count = u32::from_le_bytes(bytes[0..4].try_into()?) as usize;
    let mut builder = PathTableBuilder::with_capacity(count);
    let mut off = 4usize;
    for i in 0..count {
        if off + 2 > bytes.len() {
            anyhow::bail!("path table truncated");
        }
        let len = u16::from_le_bytes(bytes[off..off + 2].try_into()?) as usize;
        off += 2;
        if off + len > bytes.len() {
            anyhow::bail!("path table truncated");
        }
        let path_bytes = bytes[off..off + len].to_vec();
        off += len;
        builder.push(i as u32, &path_bytes);
    }
    Ok(builder.build())
}

// ─────────────────────────────────────────────────────────────────────────────
// FileEntryIndex 序列化 / 反序列化
// ─────────────────────────────────────────────────────────────────────────────

fn encode_file_entry_index(fei: &FileEntryIndex) -> Vec<u8> {
    let mut out = Vec::new();
    let len = fei.len() as u32;
    out.extend_from_slice(&len.to_le_bytes());
    for i in 0..fei.len() {
        if let Some(e) = fei.get(i) {
            out.extend_from_slice(&e.dev.to_le_bytes());
            out.extend_from_slice(&e.ino.to_le_bytes());
            out.extend_from_slice(&e.generation.to_le_bytes());
            out.extend_from_slice(&e.path_idx.to_le_bytes());
            out.extend_from_slice(&e.size.to_le_bytes());
            out.extend_from_slice(&e.mtime_ns.to_le_bytes());
        }
    }
    out
}

fn decode_file_entry_index(bytes: &[u8]) -> anyhow::Result<FileEntryIndex> {
    if bytes.len() < 4 {
        anyhow::bail!("file entry index too small");
    }
    let count = u32::from_le_bytes(bytes[0..4].try_into()?) as usize;
    const REC_SIZE: usize = 8 + 8 + 4 + 4 + 8 + 8; // dev+ino+generation+path_idx+size+mtime_ns
    let expected = 4 + count * REC_SIZE;
    if bytes.len() < expected {
        anyhow::bail!("file entry index truncated");
    }
    let mut fei = FileEntryIndex::with_capacity(count);
    let mut off = 4usize;
    for _ in 0..count {
        let dev = u64::from_le_bytes(bytes[off..off + 8].try_into()?);
        let ino = u64::from_le_bytes(bytes[off + 8..off + 16].try_into()?);
        let generation = u32::from_le_bytes(bytes[off + 16..off + 20].try_into()?);
        let path_idx = u32::from_le_bytes(bytes[off + 20..off + 24].try_into()?);
        let size = u64::from_le_bytes(bytes[off + 24..off + 32].try_into()?);
        let mtime_ns = i64::from_le_bytes(bytes[off + 32..off + 40].try_into()?);
        off += REC_SIZE;

        fei.push(FileEntry::from_file_key(
            crate::core::FileKey {
                dev,
                ino,
                generation,
            },
            path_idx,
            size,
            mtime_ns,
        ));
    }
    Ok(fei.build())
}

// ─────────────────────────────────────────────────────────────────────────────
// TrigramIndex 序列化 / 反序列化
// ─────────────────────────────────────────────────────────────────────────────

fn encode_trigram_index(ti: &TrigramIndex) -> Vec<u8> {
    let mut out = Vec::new();
    let len = ti.len() as u32;
    out.extend_from_slice(&len.to_le_bytes());
    for (tri, bitmap) in &ti.inner {
        out.extend_from_slice(tri);
        out.push(0); // pad
        let mut posting = Vec::new();
        bitmap
            .serialize_into(&mut posting)
            .expect("roaring serialize");
        let posting_len: u32 = posting.len().try_into().unwrap_or(u32::MAX);
        out.extend_from_slice(&posting_len.to_le_bytes());
        out.extend_from_slice(&posting);
    }
    out
}

fn decode_trigram_index(bytes: &[u8]) -> anyhow::Result<TrigramIndex> {
    if bytes.len() < 4 {
        anyhow::bail!("trigram index too small");
    }
    let count = u32::from_le_bytes(bytes[0..4].try_into()?) as usize;
    let mut ti = TrigramIndex::new();
    let mut off = 4usize;
    for _ in 0..count {
        if off + 8 > bytes.len() {
            anyhow::bail!("trigram index truncated");
        }
        let tri = [bytes[off], bytes[off + 1], bytes[off + 2]];
        // skip pad at off+3
        let posting_len = u32::from_le_bytes(bytes[off + 4..off + 8].try_into()?) as usize;
        off += 8;
        if off + posting_len > bytes.len() {
            anyhow::bail!("trigram index posting truncated");
        }
        let bitmap = RoaringBitmap::deserialize_from(&bytes[off..off + posting_len])
            .map_err(|e| anyhow::anyhow!("roaring deserialize failed: {}", e))?;
        off += posting_len;
        ti.insert(tri, bitmap);
    }
    Ok(ti)
}

// ─────────────────────────────────────────────────────────────────────────────
// ParentIndex 序列化 / 反序列化
// ─────────────────────────────────────────────────────────────────────────────

fn encode_parent_index(pi: &ParentIndex) -> Vec<u8> {
    let mut out = Vec::new();
    // Encode dir_to_files: HashMap<u32, RoaringBitmap>
    let len = pi.dir_to_files.len() as u32;
    out.extend_from_slice(&len.to_le_bytes());
    for (dir_idx, docids) in &pi.dir_to_files {
        out.extend_from_slice(&dir_idx.to_le_bytes());
        let bitmap: RoaringBitmap = docids.iter().copied().collect();
        let mut posting = Vec::new();
        bitmap
            .serialize_into(&mut posting)
            .expect("roaring serialize");
        let posting_len: u32 = posting.len().try_into().unwrap_or(u32::MAX);
        out.extend_from_slice(&posting_len.to_le_bytes());
        out.extend_from_slice(&posting);
    }
    // Legacy subdir section is kept in the wire format but no longer materialized at runtime.
    out.extend_from_slice(&0u32.to_le_bytes());
    out
}

fn decode_parent_index(bytes: &[u8]) -> anyhow::Result<ParentIndex> {
    if bytes.len() < 4 {
        anyhow::bail!("parent index too small");
    }
    let mut off = 0usize;
    // Decode dir_to_files
    let count = u32::from_le_bytes(bytes[off..off + 4].try_into()?) as usize;
    off += 4;
    let mut dir_to_files: HashMap<u32, Vec<u32>> = HashMap::with_capacity(count);
    for _ in 0..count {
        if off + 4 > bytes.len() {
            anyhow::bail!("parent index dir_idx truncated");
        }
        let dir_idx = u32::from_le_bytes(bytes[off..off + 4].try_into()?);
        off += 4;
        if off + 4 > bytes.len() {
            anyhow::bail!("parent index posting len truncated");
        }
        let posting_len = u32::from_le_bytes(bytes[off..off + 4].try_into()?) as usize;
        off += 4;
        if off + posting_len > bytes.len() {
            anyhow::bail!("parent index posting truncated");
        }
        let rb = RoaringBitmap::deserialize_from(&bytes[off..off + posting_len])
            .map_err(|e| anyhow::anyhow!("roaring deserialize failed: {}", e))?;
        off += posting_len;
        dir_to_files.insert(dir_idx, rb.iter().collect());
    }
    // Decode and discard legacy dir_to_subdirs.
    if off + 4 > bytes.len() {
        anyhow::bail!("parent index subdir count truncated");
    }
    let subdir_count = u32::from_le_bytes(bytes[off..off + 4].try_into()?) as usize;
    off += 4;
    for _ in 0..subdir_count {
        if off + 4 > bytes.len() {
            anyhow::bail!("parent index subdir dir_idx truncated");
        }
        off += 4;
        if off + 4 > bytes.len() {
            anyhow::bail!("parent index subdir list count truncated");
        }
        let list_count = u32::from_le_bytes(bytes[off..off + 4].try_into()?) as usize;
        off += 4;
        for _ in 0..list_count {
            if off + 4 > bytes.len() {
                anyhow::bail!("parent index subdir entry truncated");
            }
            off += 4;
        }
    }
    Ok(ParentIndex { dir_to_files })
}

// ─────────────────────────────────────────────────────────────────────────────
// Tombstones 序列化 / 反序列化（RoaringBitmap）
// ─────────────────────────────────────────────────────────────────────────────

fn encode_tombstones(t: &RoaringBitmap) -> Vec<u8> {
    let mut out = Vec::new();
    t.serialize_into(&mut out).expect("roaring serialize");
    out
}

fn decode_tombstones(bytes: &[u8]) -> anyhow::Result<RoaringBitmap> {
    RoaringBitmap::deserialize_from(bytes)
        .map_err(|e| anyhow::anyhow!("tombstones deserialize failed: {}", e))
}

// ─────────────────────────────────────────────────────────────────────────────
// Header / Trailer 编解码
// ─────────────────────────────────────────────────────────────────────────────

fn encode_header(num_segments: u32, header_crc: u32) -> [u8; V7_HEADER_SIZE] {
    let mut buf = [0u8; V7_HEADER_SIZE];
    buf[0..8].copy_from_slice(&V7_MAGIC);
    buf[8..12].copy_from_slice(&V7_VERSION.to_le_bytes());
    buf[12..16].copy_from_slice(&0u32.to_le_bytes()); // flags
    buf[16..20].copy_from_slice(&num_segments.to_le_bytes());
    buf[20..24].copy_from_slice(&header_crc.to_le_bytes());
    // reserved [24..56]
    buf[56..64].copy_from_slice(&[0u8; 8]); // tail reserved
    buf
}

fn decode_header(buf: &[u8; V7_HEADER_SIZE]) -> Option<(u32, u32)> {
    if buf[0..8] != V7_MAGIC {
        return None;
    }
    let version = u32::from_le_bytes(buf[8..12].try_into().ok()?);
    if version != V7_VERSION {
        return None;
    }
    let num_segments = u32::from_le_bytes(buf[16..20].try_into().ok()?);
    let header_crc = u32::from_le_bytes(buf[20..24].try_into().ok()?);
    Some((num_segments, header_crc))
}

fn compute_header_crc(buf: &[u8; V7_HEADER_SIZE]) -> u32 {
    let mut c = Crc32c::new();
    c.update(&buf[0..20]); // before crc field
    c.update(&buf[24..56]); // after crc field, before tail reserved
    c.finalize()
}

/// Trailer 结构（变长 + 固定尾部）。
///
/// 从文件末尾读取：
///   [file_len-8 ..]  = trailer_magic
///   [file_len-16..file_len-8] = trailer_len
///   trailer 从 file_len - trailer_len 处开始
struct V7Trailer {
    num_segments: u32,
    global_crc32c: u32,
    segment_offsets: Vec<u64>,
    segment_lens: Vec<u64>,
    segment_crcs: Vec<u32>,
}

impl V7Trailer {
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for &off in &self.segment_offsets {
            out.extend_from_slice(&off.to_le_bytes());
        }
        for &len in &self.segment_lens {
            out.extend_from_slice(&len.to_le_bytes());
        }
        for &crc in &self.segment_crcs {
            out.extend_from_slice(&crc.to_le_bytes());
        }
        out.extend_from_slice(&self.global_crc32c.to_le_bytes());
        out.extend_from_slice(&self.num_segments.to_le_bytes());
        let trailer_len: u64 = (out.len() + 8 + 8) as u64; // + trailer_len + trailer_magic
        out.extend_from_slice(&trailer_len.to_le_bytes());
        out.extend_from_slice(&V7_TRAILER_MAGIC);
        out
    }

    fn decode_from_file_end(buf: &[u8]) -> Option<(Self, usize)> {
        if buf.len() < V7_TRAILER_FIXED_SIZE {
            return None;
        }
        let file_len = buf.len();
        let magic_off = file_len - 8;
        if buf[magic_off..] != V7_TRAILER_MAGIC {
            return None;
        }
        let trailer_len =
            u64::from_le_bytes(buf[magic_off - 8..magic_off].try_into().ok()?) as usize;
        if trailer_len < V7_TRAILER_FIXED_SIZE || trailer_len > file_len {
            return None;
        }
        let trailer_start = file_len - trailer_len;
        let body = &buf[trailer_start..magic_off - 8];

        // body = offsets[N] + lens[N] + crcs[N] + global_crc(4) + num_segments(4)
        // Need to determine N from num_segments at the end of body
        if body.len() < 8 {
            return None;
        }
        let num_segments = u32::from_le_bytes(body[body.len() - 4..].try_into().ok()?) as usize;
        let global_crc32c =
            u32::from_le_bytes(body[body.len() - 8..body.len() - 4].try_into().ok()?);

        let expected_body = num_segments * 8 + num_segments * 8 + num_segments * 4 + 8;
        if body.len() != expected_body {
            return None;
        }

        let mut off = 0usize;
        let mut segment_offsets = Vec::with_capacity(num_segments);
        for _ in 0..num_segments {
            segment_offsets.push(u64::from_le_bytes(body[off..off + 8].try_into().ok()?));
            off += 8;
        }
        let mut segment_lens = Vec::with_capacity(num_segments);
        for _ in 0..num_segments {
            segment_lens.push(u64::from_le_bytes(body[off..off + 8].try_into().ok()?));
            off += 8;
        }
        let mut segment_crcs = Vec::with_capacity(num_segments);
        for _ in 0..num_segments {
            segment_crcs.push(u32::from_le_bytes(body[off..off + 4].try_into().ok()?));
            off += 4;
        }

        Some((
            V7Trailer {
                num_segments: num_segments as u32,
                global_crc32c,
                segment_offsets,
                segment_lens,
                segment_crcs,
            },
            trailer_start,
        ))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// v7 加载：mmap 单文件并反序列化各段
// ─────────────────────────────────────────────────────────────────────────────

/// v7 加载后的只读视图（各段为 mmap 切片，按需反序列化）。
#[derive(Clone)]
pub struct V7Snapshot {
    mmap: Arc<Mmap>,
    segments: Vec<(V7SegKind, std::ops::Range<usize>)>,
}

impl V7Snapshot {
    pub fn bytes(&self) -> &[u8] {
        self.mmap.as_ref()
    }

    pub fn slice(&self, r: std::ops::Range<usize>) -> &[u8] {
        &self.bytes()[r]
    }

    pub fn segment(&self, kind: V7SegKind) -> Option<&[u8]> {
        self.segments
            .iter()
            .find(|(k, _)| *k == kind)
            .map(|(_, r)| &self.bytes()[r.clone()])
    }

    /// 反序列化为 BaseIndexData（当前阶段仍做反序列化，后续可优化为零拷贝）。
    pub fn to_base_index_data(&self) -> anyhow::Result<BaseIndexData> {
        let path_table = self
            .segment(V7SegKind::PathTable)
            .map(decode_path_table)
            .transpose()?
            .unwrap_or_default();
        let entries_by_key = self
            .segment(V7SegKind::EntriesByKey)
            .map(decode_file_entry_index)
            .transpose()?
            .unwrap_or_default();
        let trigram_index = self
            .segment(V7SegKind::TrigramIndex)
            .map(decode_trigram_index)
            .transpose()?
            .unwrap_or_default();
        let parent_index = self
            .segment(V7SegKind::ParentIndex)
            .map(decode_parent_index)
            .transpose()?
            .unwrap_or_default();
        let tombstones = self
            .segment(V7SegKind::Tombstones)
            .map(decode_tombstones)
            .transpose()?
            .unwrap_or_default();

        Ok(BaseIndexData {
            path_table,
            entries_by_key,
            trigram_index,
            parent_index,
            tombstones,
        })
    }
}

/// 从文件路径加载 v7 快照（校验 header/trailer/各段 CRC）。
pub fn load_v7_from_path(path: &Path) -> anyhow::Result<Option<V7Snapshot>> {
    if !path.exists() {
        return Ok(None);
    }
    let file = std::fs::File::open(path)?;
    let file_len = file.metadata()?.len() as usize;
    if file_len < V7_HEADER_SIZE + V7_TRAILER_FIXED_SIZE {
        tracing::warn!("v7 file too small, ignoring");
        return Ok(None);
    }

    // mmap 整个文件（只读 private）
    let mmap = unsafe { memmap2::MmapOptions::new().map_copy_read_only(&file)? };
    let bytes = mmap.as_ref();

    // 解析 header
    if bytes.len() < V7_HEADER_SIZE {
        return Ok(None);
    }
    let header_buf: [u8; V7_HEADER_SIZE] = bytes[0..V7_HEADER_SIZE].try_into()?;
    let (num_segments, header_crc) =
        decode_header(&header_buf).ok_or_else(|| anyhow::anyhow!("v7 header decode failed"))?;
    if compute_header_crc(&header_buf) != header_crc {
        tracing::warn!("v7 header crc mismatch, ignoring");
        return Ok(None);
    }

    // 解析 trailer（从末尾）
    let (trailer, _trailer_start) = V7Trailer::decode_from_file_end(bytes)
        .ok_or_else(|| anyhow::anyhow!("v7 trailer decode failed"))?;

    if trailer.num_segments != num_segments {
        tracing::warn!("v7 segment count mismatch");
        return Ok(None);
    }

    // 校验各段 CRC 与边界
    let mut segments = Vec::with_capacity(num_segments as usize);
    for i in 0..num_segments as usize {
        let off = trailer.segment_offsets[i] as usize;
        let len = trailer.segment_lens[i] as usize;
        let crc = trailer.segment_crcs[i];
        let end = off
            .checked_add(len)
            .ok_or_else(|| anyhow::anyhow!("v7 segment {} offset overflow", i))?;
        if end > bytes.len() {
            tracing::warn!("v7 segment {} out of bounds", i);
            return Ok(None);
        }
        let seg_bytes = &bytes[off..end];
        let computed = crc32c_checksum(seg_bytes);
        if computed != crc {
            tracing::warn!("v7 segment {} crc mismatch: {} != {}", i, computed, crc);
            return Ok(None);
        }
        // kind 需要从 header 的 SegmentDesc 表中读取，但 trailer 中没有 kind 信息。
        // 简化：v7 固定段顺序 = PathTable, EntriesByKey, EntriesByPath, TrigramIndex, ParentIndex, Tombstones
        let kind = match i {
            0 => V7SegKind::PathTable,
            1 => V7SegKind::EntriesByKey,
            2 => V7SegKind::EntriesByPath,
            3 => V7SegKind::TrigramIndex,
            4 => V7SegKind::ParentIndex,
            5 => V7SegKind::Tombstones,
            _ => {
                tracing::warn!("v7 unknown segment index {}", i);
                return Ok(None);
            }
        };
        segments.push((kind, off..end));
    }

    // global crc（覆盖所有段数据，从 header 结束到 trailer 开始）
    // 简化：计算所有段数据的 crc
    let mut global_hasher = Crc32c::new();
    for (_, r) in &segments {
        global_hasher.update(&bytes[r.clone()]);
    }
    if global_hasher.finalize() != trailer.global_crc32c {
        tracing::warn!("v7 global crc mismatch, ignoring");
        return Ok(None);
    }

    Ok(Some(V7Snapshot {
        mmap: Arc::new(mmap),
        segments,
    }))
}

// ─────────────────────────────────────────────────────────────────────────────
// v7 写入：base + delta → 排序 → 归并 → atomic write v7 单文件
// ─────────────────────────────────────────────────────────────────────────────

fn align_up(v: usize, a: usize) -> usize {
    (v + (a - 1)) & !(a - 1)
}

/// 将 BaseIndexData 原子写入 v7 单文件（tmp + rename）。
///
/// 写入流程：
/// 1) 各段序列化为 Vec<u8>
/// 2) 计算 offset / len / crc
/// 3) 写 header + segments + trailer 到 .tmp
/// 4) fsync + rename
pub fn write_v7_snapshot_atomic(path: &Path, data: &BaseIndexData) -> anyhow::Result<()> {
    let segments_bytes: Vec<(V7SegKind, Vec<u8>)> = vec![
        (V7SegKind::PathTable, encode_path_table(&data.path_table)),
        (
            V7SegKind::EntriesByKey,
            encode_file_entry_index(&data.entries_by_key),
        ),
        (
            V7SegKind::EntriesByPath,
            encode_file_entry_index(&data.entries_by_key),
        ),
        (
            V7SegKind::TrigramIndex,
            encode_trigram_index(&data.trigram_index),
        ),
        (
            V7SegKind::ParentIndex,
            encode_parent_index(&data.parent_index),
        ),
        (V7SegKind::Tombstones, encode_tombstones(&data.tombstones)),
    ];

    let num_segments = segments_bytes.len() as u32;
    let mut seg_descs: Vec<V7SegDesc> = Vec::with_capacity(segments_bytes.len());
    let mut cursor = align_up(V7_HEADER_SIZE, 8);

    for (_, bytes) in &segments_bytes {
        let offset = cursor as u64;
        let len = bytes.len() as u64;
        let crc = crc32c_checksum(bytes);
        seg_descs.push(V7SegDesc {
            offset,
            len,
            crc32c: crc,
        });
        cursor = align_up(cursor + bytes.len(), 8);
    }

    // 计算 global crc
    let mut global_hasher = Crc32c::new();
    for (_, bytes) in &segments_bytes {
        global_hasher.update(bytes);
    }
    let global_crc = global_hasher.finalize();

    let trailer = V7Trailer {
        num_segments,
        global_crc32c: global_crc,
        segment_offsets: seg_descs.iter().map(|d| d.offset).collect(),
        segment_lens: seg_descs.iter().map(|d| d.len).collect(),
        segment_crcs: seg_descs.iter().map(|d| d.crc32c).collect(),
    };
    let trailer_bytes = trailer.encode();

    // 组装文件
    let tmp_path = path.with_extension("v7.tmp");
    {
        let mut file = std::fs::File::create(&tmp_path)?;

        // Header（先占位，crc 后填）
        let mut header_buf = encode_header(num_segments, 0);
        file.write_all(&header_buf)?;

        // Segments
        let mut written = V7_HEADER_SIZE;
        for (i, (_, bytes)) in segments_bytes.iter().enumerate() {
            let target = seg_descs[i].offset as usize;
            if target > written {
                file.write_all(&vec![0u8; target - written])?;
                written = target;
            }
            file.write_all(bytes)?;
            written += bytes.len();
            let pad = align_up(written, 8) - written;
            if pad > 0 {
                file.write_all(&vec![0u8; pad])?;
                written += pad;
            }
        }

        // Trailer
        file.write_all(&trailer_bytes)?;

        // 回填 header crc
        let header_crc = compute_header_crc(&header_buf);
        header_buf = encode_header(num_segments, header_crc);
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&header_buf)?;

        file.sync_all()?;
    }

    // 原子替换
    std::fs::rename(&tmp_path, path)?;
    if let Some(parent) = path.parent() {
        if let Ok(dir) = std::fs::File::open(parent) {
            let _ = dir.sync_all();
        }
    }

    tracing::info!(
        "v7 snapshot written: {} segments, {} bytes",
        num_segments,
        V7_HEADER_SIZE + cursor + trailer_bytes.len()
    );

    Ok(())
}

/// 从 v6 segments + delta 构建 v7 快照（排序归并后写入）。
///
/// 当前为框架实现：将 base（若提供）与 delta 直接拼接，未做真正归并。
/// 后续完善为"base + delta → 去重排序 → v7"。
pub fn snapshot_now_v7(
    path: &Path,
    base: Option<&BaseIndexData>,
    delta: &BaseIndexData,
) -> anyhow::Result<()> {
    let mut merged = BaseIndexData::default();

    // 先灌入 base
    if let Some(b) = base {
        merged.path_table = b.path_table.clone();
        for i in 0..b.entries_by_key.len() {
            if let Some(e) = b.entries_by_key.get(i) {
                merged.entries_by_key.push(*e);
            }
        }
        merged.trigram_index = b.trigram_index.clone();
        merged.parent_index = b.parent_index.clone();
        merged.tombstones = b.tombstones.clone();
    }

    // 再灌入 delta（简单追加；TODO: 真正归并去重）
    for i in 0..delta.entries_by_key.len() {
        if let Some(e) = delta.entries_by_key.get(i) {
            merged.entries_by_key.push(*e);
        }
    }
    // trigram / parent / tombstones：简单合并（TODO: 真正归并）
    for (tri, bm) in &delta.trigram_index.inner {
        merged.trigram_index.insert(*tri, bm.clone());
    }
    for (dir, bm) in &delta.parent_index.dir_to_files {
        merged
            .parent_index
            .dir_to_files
            .entry(*dir)
            .and_modify(|existing| {
                existing.extend_from_slice(bm);
                existing.sort_unstable();
                existing.dedup();
            })
            .or_insert_with(|| bm.clone());
    }
    merged.tombstones |= delta.tombstones.clone();

    // 排序（key）
    merged.entries_by_key.sort_by_key();

    write_v7_snapshot_atomic(path, &merged)
}

// ─────────────────────────────────────────────────────────────────────────────
// 启动加载辅助：优先尝试 v7，回退到 v6 / 空
// ─────────────────────────────────────────────────────────────────────────────

/// 尝试加载 v7 文件；失败返回 None，由调用方回退到 v6。
pub fn try_load_v7(path: &Path) -> anyhow::Result<Option<BaseIndexData>> {
    match load_v7_from_path(path)? {
        Some(snap) => match snap.to_base_index_data() {
            Ok(data) => {
                tracing::info!("v7 snapshot loaded: {} paths", data.path_table.len());
                Ok(Some(data))
            }
            Err(e) => {
                tracing::warn!("v7 snapshot deserialize failed: {}", e);
                Ok(None)
            }
        },
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::FileKey;
    use std::path::PathBuf;

    fn tmp_v7_path(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("fd-rdd-v7-{}-{}", tag, nanos))
    }

    #[test]
    fn v7_roundtrip_base_index() {
        let path = tmp_v7_path("roundtrip");
        let mut data = BaseIndexData::default();
        data.entries_by_key.push(FileEntry::from_file_key(
            FileKey {
                dev: 1,
                ino: 42,
                generation: 0,
            },
            0,
            1024,
            -1,
        ));
        data.tombstones.insert(42);

        write_v7_snapshot_atomic(&path, &data).unwrap();
        let loaded = load_v7_from_path(&path).unwrap().unwrap();
        let decoded = loaded.to_base_index_data().unwrap();

        assert_eq!(decoded.entries_by_key.len(), 1);
        assert!(decoded.tombstones.contains(42));
    }

    #[test]
    fn v7_load_missing_returns_none() {
        let path = tmp_v7_path("missing");
        assert!(load_v7_from_path(&path).unwrap().is_none());
    }

    #[test]
    fn v7_corrupted_file_rejected() {
        let path = tmp_v7_path("corrupt");
        std::fs::write(&path, b"not a v7 file").unwrap();
        assert!(load_v7_from_path(&path).unwrap().is_none());
    }
}
