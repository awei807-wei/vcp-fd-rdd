use memmap2::Mmap;
#[cfg(unix)]
use memmap2::{Advice, UncheckedAdvice};
use roaring::RoaringBitmap;
use std::collections::HashMap;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::core::{FileKey, FileMeta};
use crate::index::base_index::{BaseIndexData, FileEntryIndex, MmapWarmupReport, TrigramIndex};
use crate::index::file_entry_v2::FileEntry;
use crate::index::parent_index::ParentIndex;
use crate::index::path_table_v2::{PathTableBuilder, PathTableV2};
use crate::query::Matcher;
use crate::storage::checksum::{crc32c_checksum, Crc32c};
use crate::util::{align_up, read_u32};

// ─────────────────────────────────────────────────────────────────────────────
// v7 单文件 mmap 格式常量
// ─────────────────────────────────────────────────────────────────────────────

const V7_MAGIC: [u8; 8] = *b"FDRDDv7\0";
const V7_VERSION_LEGACY_40B_ENTRY: u32 = 1;
const V7_VERSION: u32 = 2;
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
const FILE_ENTRY_REC_SIZE: usize = 8 + 8 + 4 + 4 + 8;
const LEGACY_FILE_ENTRY_REC_SIZE: usize = 8 + 8 + 4 + 4 + 8 + 8;
const RAW_PATH_TABLE_MAGIC: &[u8; 8] = b"PTV2raw\0";
const RAW_PATH_TABLE_HEADER_SIZE: usize = 8 + 4 * 4;
const RAW_PATH_TABLE_SLOT_SIZE: usize = 12;
const RAW_PATH_TABLE_ANCHOR_INTERVAL: usize = 256;
const TRIGRAM_SENTINEL: [u8; 3] = [0, 0, 0];

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
            out.extend_from_slice(&e.mtime_ns.to_le_bytes());
        }
    }
    out
}

#[cfg(test)]
fn encode_file_entry_index_legacy_40b(fei: &FileEntryIndex) -> Vec<u8> {
    let mut out = Vec::new();
    let len = fei.len() as u32;
    out.extend_from_slice(&len.to_le_bytes());
    for i in 0..fei.len() {
        if let Some(e) = fei.get(i) {
            out.extend_from_slice(&e.dev.to_le_bytes());
            out.extend_from_slice(&e.ino.to_le_bytes());
            out.extend_from_slice(&e.generation.to_le_bytes());
            out.extend_from_slice(&e.path_idx.to_le_bytes());
            out.extend_from_slice(&e.mtime_ns.to_le_bytes());
            out.extend_from_slice(&0u64.to_le_bytes());
        }
    }
    out
}

fn decode_file_entry_index(bytes: &[u8], snapshot_version: u32) -> anyhow::Result<FileEntryIndex> {
    if bytes.len() < 4 {
        anyhow::bail!("file entry index too small");
    }
    let count = u32::from_le_bytes(bytes[0..4].try_into()?) as usize;
    let rec_size = file_entry_rec_size(snapshot_version)?;
    let expected = 4 + count * rec_size;
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
        let mtime_ns = i64::from_le_bytes(bytes[off + 24..off + 32].try_into()?);
        off += rec_size;

        fei.push(FileEntry::from_encoded_path_idx(
            crate::core::FileKey {
                dev,
                ino,
                generation,
            },
            path_idx,
            mtime_ns,
        ));
    }
    Ok(fei.build())
}

// ─────────────────────────────────────────────────────────────────────────────
// TrigramIndex 序列化 / 反序列化
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
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

fn encode_full_path_trigram_index(data: &BaseIndexData) -> Vec<u8> {
    let mut full_path_index: HashMap<[u8; 3], RoaringBitmap> = HashMap::new();

    for (docid, entry) in data.entries_by_key.iter().enumerate() {
        if data.tombstones.contains(docid as u32) {
            continue;
        }
        let Some(path_bytes) = data.path_table.resolve(entry.path_index()) else {
            continue;
        };
        let lower = String::from_utf8_lossy(&path_bytes).to_lowercase();
        let bytes = lower.as_bytes();
        if bytes.len() < 3 {
            continue;
        }
        for tri in bytes.windows(3).map(|w| [w[0], w[1], w[2]]) {
            full_path_index.entry(tri).or_default().insert(docid as u32);
        }
    }

    full_path_index.entry(TRIGRAM_SENTINEL).or_default();
    encode_trigram_map(&full_path_index)
}

fn encode_trigram_map(index: &HashMap<[u8; 3], RoaringBitmap>) -> Vec<u8> {
    let mut entries: Vec<([u8; 3], &RoaringBitmap)> =
        index.iter().map(|(tri, bitmap)| (*tri, bitmap)).collect();
    entries.sort_by_key(|(tri, _)| *tri);

    let mut out = Vec::new();
    let len = entries.len() as u32;
    out.extend_from_slice(&len.to_le_bytes());
    for (tri, bitmap) in entries {
        out.extend_from_slice(&tri);
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

#[derive(Clone, Copy)]
struct RawPathTableLayout {
    slots_len: usize,
    suffix_start: usize,
    suffix_len: usize,
    idx_start: usize,
    idx_len: usize,
}

#[derive(Clone, Copy)]
struct RawPathTableSlot {
    suffix_offset: usize,
    shared_len: usize,
    suffix_len: usize,
    orig_idx: u32,
}

impl RawPathTableLayout {
    fn parse(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < RAW_PATH_TABLE_HEADER_SIZE
            || &bytes[..8] != RAW_PATH_TABLE_MAGIC.as_slice()
        {
            return None;
        }

        let mut off = 8usize;
        let slots_len = read_u32(bytes, &mut off)? as usize;
        let suffix_len = read_u32(bytes, &mut off)? as usize;
        let idx_len = read_u32(bytes, &mut off)? as usize;
        let _legacy_anchors_len = read_u32(bytes, &mut off)?;

        let slots_bytes = slots_len.checked_mul(RAW_PATH_TABLE_SLOT_SIZE)?;
        let suffix_start = RAW_PATH_TABLE_HEADER_SIZE.checked_add(slots_bytes)?;
        let idx_start = suffix_start.checked_add(suffix_len)?;
        let idx_bytes = idx_len.checked_mul(4)?;
        let total = idx_start.checked_add(idx_bytes)?;
        if total > bytes.len() {
            return None;
        }

        Some(Self {
            slots_len,
            suffix_start,
            suffix_len,
            idx_start,
            idx_len,
        })
    }

    fn slot(self, bytes: &[u8], pos: usize) -> Option<RawPathTableSlot> {
        if pos >= self.slots_len {
            return None;
        }
        let off = RAW_PATH_TABLE_HEADER_SIZE + pos * RAW_PATH_TABLE_SLOT_SIZE;
        let suffix_offset = u32::from_le_bytes(bytes.get(off..off + 4)?.try_into().ok()?) as usize;
        let shared_len = u16::from_le_bytes(bytes.get(off + 4..off + 6)?.try_into().ok()?) as usize;
        let suffix_len = u16::from_le_bytes(bytes.get(off + 6..off + 8)?.try_into().ok()?) as usize;
        let orig_idx = u32::from_le_bytes(bytes.get(off + 8..off + 12)?.try_into().ok()?);
        if suffix_offset.checked_add(suffix_len)? > self.suffix_len {
            return None;
        }
        Some(RawPathTableSlot {
            suffix_offset,
            shared_len,
            suffix_len,
            orig_idx,
        })
    }

    fn suffix(self, bytes: &[u8], slot: RawPathTableSlot) -> Option<&[u8]> {
        let start = self.suffix_start.checked_add(slot.suffix_offset)?;
        let end = start.checked_add(slot.suffix_len)?;
        bytes.get(start..end)
    }

    fn sorted_pos_for_idx(self, bytes: &[u8], idx: u32) -> Option<usize> {
        let idx = idx as usize;
        if idx >= self.idx_len {
            return None;
        }
        let off = self.idx_start.checked_add(idx.checked_mul(4)?)?;
        let sorted_pos = u32::from_le_bytes(bytes.get(off..off + 4)?.try_into().ok()?) as usize;
        (sorted_pos < self.slots_len).then_some(sorted_pos)
    }
}

enum V7PathResolver<'a> {
    Raw {
        bytes: &'a [u8],
        layout: RawPathTableLayout,
    },
    Decoded(PathTableV2),
}

impl V7PathResolver<'_> {
    fn resolve_into(&self, idx: u32, out: &mut Vec<u8>) -> Option<()> {
        match self {
            Self::Raw { bytes, layout } => resolve_raw_path_into(bytes, *layout, idx, out),
            Self::Decoded(table) => table.resolve_into(idx, out),
        }
    }

    fn lookup(&self, target: &[u8]) -> Option<u32> {
        match self {
            Self::Raw { bytes, layout } => lookup_raw_path(bytes, *layout, target),
            Self::Decoded(table) => table.lookup(target),
        }
    }
}

fn resolve_raw_path_into(
    bytes: &[u8],
    layout: RawPathTableLayout,
    idx: u32,
    out: &mut Vec<u8>,
) -> Option<()> {
    let sorted_pos = layout.sorted_pos_for_idx(bytes, idx)?;
    let anchor_pos = (sorted_pos / RAW_PATH_TABLE_ANCHOR_INTERVAL) * RAW_PATH_TABLE_ANCHOR_INTERVAL;
    let anchor = layout.slot(bytes, anchor_pos)?;
    out.clear();
    out.extend_from_slice(layout.suffix(bytes, anchor)?);

    for pos in (anchor_pos + 1)..=sorted_pos {
        let slot = layout.slot(bytes, pos)?;
        if slot.shared_len > out.len() {
            return None;
        }
        out.truncate(slot.shared_len);
        out.extend_from_slice(layout.suffix(bytes, slot)?);
    }

    Some(())
}

fn lookup_raw_path(bytes: &[u8], layout: RawPathTableLayout, target: &[u8]) -> Option<u32> {
    let mut path = Vec::new();
    for pos in 0..layout.slots_len {
        let slot = layout.slot(bytes, pos)?;
        if pos % RAW_PATH_TABLE_ANCHOR_INTERVAL == 0 {
            path.clear();
        }
        if slot.shared_len > path.len() {
            return None;
        }
        path.truncate(slot.shared_len);
        path.extend_from_slice(layout.suffix(bytes, slot)?);
        if path.as_slice() == target {
            return Some(slot.orig_idx);
        }
    }
    None
}

fn file_entry_rec_size(snapshot_version: u32) -> anyhow::Result<usize> {
    match snapshot_version {
        V7_VERSION => Ok(FILE_ENTRY_REC_SIZE),
        V7_VERSION_LEGACY_40B_ENTRY => Ok(LEGACY_FILE_ENTRY_REC_SIZE),
        version => anyhow::bail!("unsupported v7 snapshot version {}", version),
    }
}

fn entry_count_from_segment(bytes: &[u8], snapshot_version: u32) -> Option<usize> {
    let mut off = 0usize;
    let count = read_u32(bytes, &mut off)? as usize;
    let rec_size = file_entry_rec_size(snapshot_version).ok()?;
    let expected = 4usize.checked_add(count.checked_mul(rec_size)?)?;
    (expected <= bytes.len()).then_some(count)
}

fn file_entry_at(bytes: &[u8], snapshot_version: u32, docid: u32) -> Option<FileEntry> {
    let count = entry_count_from_segment(bytes, snapshot_version)?;
    let docid_usize = docid as usize;
    if docid_usize >= count {
        return None;
    }
    let rec_size = file_entry_rec_size(snapshot_version).ok()?;
    let off = 4 + docid_usize * rec_size;
    Some(FileEntry {
        dev: u64::from_le_bytes(bytes.get(off..off + 8)?.try_into().ok()?),
        ino: u64::from_le_bytes(bytes.get(off + 8..off + 16)?.try_into().ok()?),
        generation: u32::from_le_bytes(bytes.get(off + 16..off + 20)?.try_into().ok()?),
        path_idx: u32::from_le_bytes(bytes.get(off + 20..off + 24)?.try_into().ok()?),
        mtime_ns: i64::from_le_bytes(bytes.get(off + 24..off + 32)?.try_into().ok()?),
    })
}

fn entry_to_meta(entry: FileEntry, path_bytes: Vec<u8>) -> FileMeta {
    crate::util::entry_to_meta(&entry, &path_bytes)
}

fn posting_for_trigram(bytes: &[u8], tri: [u8; 3]) -> anyhow::Result<Option<RoaringBitmap>> {
    if bytes.len() < 4 {
        anyhow::bail!("trigram index too small");
    }
    let count = u32::from_le_bytes(bytes[0..4].try_into()?) as usize;
    let mut off = 4usize;
    for _ in 0..count {
        if off + 8 > bytes.len() {
            anyhow::bail!("trigram index truncated");
        }
        let key = [bytes[off], bytes[off + 1], bytes[off + 2]];
        let posting_len = u32::from_le_bytes(bytes[off + 4..off + 8].try_into()?) as usize;
        off += 8;
        if off + posting_len > bytes.len() {
            anyhow::bail!("trigram index posting truncated");
        }
        if key == tri {
            let bitmap = RoaringBitmap::deserialize_from(&bytes[off..off + posting_len])
                .map_err(|e| anyhow::anyhow!("roaring deserialize failed: {}", e))?;
            return Ok(Some(bitmap));
        }
        off += posting_len;
    }
    Ok(None)
}

fn trigram_index_has_sentinel(bytes: &[u8]) -> anyhow::Result<bool> {
    posting_for_trigram(bytes, TRIGRAM_SENTINEL).map(|posting| posting.is_some())
}

fn parent_posting(bytes: &[u8], parent_idx: u32) -> anyhow::Result<Option<RoaringBitmap>> {
    if bytes.len() < 4 {
        anyhow::bail!("parent index too small");
    }
    let mut off = 0usize;
    let count = u32::from_le_bytes(bytes[off..off + 4].try_into()?) as usize;
    off += 4;
    for _ in 0..count {
        if off + 8 > bytes.len() {
            anyhow::bail!("parent index dir entry truncated");
        }
        let dir_idx = u32::from_le_bytes(bytes[off..off + 4].try_into()?);
        let posting_len = u32::from_le_bytes(bytes[off + 4..off + 8].try_into()?) as usize;
        off += 8;
        if off + posting_len > bytes.len() {
            anyhow::bail!("parent index posting truncated");
        }
        if dir_idx == parent_idx {
            let bitmap = RoaringBitmap::deserialize_from(&bytes[off..off + posting_len])
                .map_err(|e| anyhow::anyhow!("roaring deserialize failed: {}", e))?;
            return Ok(Some(bitmap));
        }
        off += posting_len;
    }
    Ok(None)
}

// ─────────────────────────────────────────────────────────────────────────────
// Header / Trailer 编解码
// ─────────────────────────────────────────────────────────────────────────────

fn encode_header_with_version(
    num_segments: u32,
    header_crc: u32,
    version: u32,
) -> [u8; V7_HEADER_SIZE] {
    let mut buf = [0u8; V7_HEADER_SIZE];
    buf[0..8].copy_from_slice(&V7_MAGIC);
    buf[8..12].copy_from_slice(&version.to_le_bytes());
    buf[12..16].copy_from_slice(&0u32.to_le_bytes()); // flags
    buf[16..20].copy_from_slice(&num_segments.to_le_bytes());
    buf[20..24].copy_from_slice(&header_crc.to_le_bytes());
    // reserved [24..56]
    buf[56..64].copy_from_slice(&[0u8; 8]); // tail reserved
    buf
}

fn decode_header(buf: &[u8; V7_HEADER_SIZE]) -> Option<(u32, u32, u32)> {
    if buf[0..8] != V7_MAGIC {
        return None;
    }
    let version = u32::from_le_bytes(buf[8..12].try_into().ok()?);
    if version != V7_VERSION && version != V7_VERSION_LEGACY_40B_ENTRY {
        return None;
    }
    let num_segments = u32::from_le_bytes(buf[16..20].try_into().ok()?);
    let header_crc = u32::from_le_bytes(buf[20..24].try_into().ok()?);
    Some((version, num_segments, header_crc))
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
    version: u32,
}

impl V7Snapshot {
    pub fn bytes(&self) -> &[u8] {
        self.mmap.as_ref()
    }

    pub fn mapped_len(&self) -> usize {
        self.bytes().len()
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

    pub fn advise_dontneed(&self) {
        #[cfg(unix)]
        {
            // SAFETY: 调用方只在生成 owned 查询/物化结果后调用；映射只读，
            // 后续查询仍可按需重新 fault 页面。
            if let Err(e) = unsafe {
                self.mmap
                    .as_ref()
                    .unchecked_advise(UncheckedAdvice::DontNeed)
            } {
                tracing::debug!("v7 mmap MADV_DONTNEED failed: {}", e);
            }
        }
    }

    pub fn warmup(&self, max_bytes: u64) -> MmapWarmupReport {
        let start = std::time::Instant::now();
        let requested_len = if max_bytes == 0 {
            self.mapped_len()
        } else {
            self.mapped_len().min(max_bytes as usize)
        };

        if requested_len == 0 {
            return MmapWarmupReport {
                pages: 0,
                elapsed_ms: start.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
                cancel_reason: "empty".to_string(),
            };
        }

        #[cfg(unix)]
        {
            let cancel_reason =
                match self
                    .mmap
                    .as_ref()
                    .advise_range(Advice::WillNeed, 0, requested_len)
                {
                    Ok(()) if requested_len < self.mapped_len() => "max_bytes".to_string(),
                    Ok(()) => String::new(),
                    Err(err) => format!("madvise_error:{}", err.kind()),
                };
            MmapWarmupReport {
                pages: ((requested_len as u64).saturating_add(4095)) / 4096,
                elapsed_ms: start.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
                cancel_reason,
            }
        }

        #[cfg(not(unix))]
        {
            MmapWarmupReport {
                pages: 0,
                elapsed_ms: start.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
                cancel_reason: "unsupported".to_string(),
            }
        }
    }

    fn path_resolver(&self) -> anyhow::Result<V7PathResolver<'_>> {
        let Some(bytes) = self.segment(V7SegKind::PathTable) else {
            return Ok(V7PathResolver::Decoded(PathTableV2::default()));
        };
        if let Some(layout) = RawPathTableLayout::parse(bytes) {
            return Ok(V7PathResolver::Raw { bytes, layout });
        }
        Ok(V7PathResolver::Decoded(decode_path_table(bytes)?))
    }

    fn tombstones(&self) -> anyhow::Result<RoaringBitmap> {
        self.segment(V7SegKind::Tombstones)
            .map(decode_tombstones)
            .transpose()
            .map(|t| t.unwrap_or_default())
    }

    fn trigram_candidates(&self, matcher: &dyn Matcher) -> anyhow::Result<Option<RoaringBitmap>> {
        let Some(hint) = matcher.literal_hint() else {
            return Ok(None);
        };
        let lower = String::from_utf8_lossy(hint).to_lowercase();
        let bytes = lower.as_bytes();
        if bytes.len() < 3 {
            return Ok(None);
        }
        let Some(segment) = self.segment(V7SegKind::TrigramIndex) else {
            return Ok(None);
        };
        if !trigram_index_has_sentinel(segment)? {
            return Ok(None);
        }

        let mut acc: Option<RoaringBitmap> = None;
        for tri in bytes.windows(3).map(|w| [w[0], w[1], w[2]]) {
            let Some(posting) = posting_for_trigram(segment, tri)? else {
                return Ok(Some(RoaringBitmap::new()));
            };
            match acc {
                None => acc = Some(posting),
                Some(ref mut current) => {
                    *current &= &posting;
                    if current.is_empty() {
                        return Ok(Some(RoaringBitmap::new()));
                    }
                }
            }
        }

        Ok(acc)
    }

    pub fn query_keys(&self, matcher: &dyn Matcher) -> anyhow::Result<Vec<FileKey>> {
        let Some(entries) = self.segment(V7SegKind::EntriesByKey) else {
            return Ok(Vec::new());
        };
        let resolver = self.path_resolver()?;
        let tombstones = self.tombstones()?;
        let mut out = Vec::new();

        if let Some(candidates) = self.trigram_candidates(matcher)? {
            let mut path_bytes = Vec::new();
            for docid in candidates.iter() {
                if tombstones.contains(docid) {
                    continue;
                }
                let Some(entry) = file_entry_at(entries, self.version, docid) else {
                    continue;
                };
                if resolver
                    .resolve_into(entry.path_index(), &mut path_bytes)
                    .is_none()
                {
                    continue;
                }
                let path_str = std::str::from_utf8(&path_bytes)
                    .map(std::borrow::Cow::Borrowed)
                    .unwrap_or_else(|_| String::from_utf8_lossy(&path_bytes));
                if matcher.matches(&path_str) {
                    out.push(entry.file_key());
                }
            }
            return Ok(out);
        }

        let count = entry_count_from_segment(entries, self.version).unwrap_or(0);
        let mut path_bytes = Vec::new();
        for docid in 0..count {
            let docid = docid as u32;
            if tombstones.contains(docid) {
                continue;
            }
            let Some(entry) = file_entry_at(entries, self.version, docid) else {
                continue;
            };
            if resolver
                .resolve_into(entry.path_index(), &mut path_bytes)
                .is_none()
            {
                continue;
            }
            let path_str = std::str::from_utf8(&path_bytes)
                .map(std::borrow::Cow::Borrowed)
                .unwrap_or_else(|_| String::from_utf8_lossy(&path_bytes));
            if matcher.matches(&path_str) {
                out.push(entry.file_key());
            }
        }

        Ok(out)
    }

    pub fn query_metas(&self, matcher: &dyn Matcher) -> anyhow::Result<Vec<FileMeta>> {
        let Some(entries) = self.segment(V7SegKind::EntriesByKey) else {
            return Ok(Vec::new());
        };
        let resolver = self.path_resolver()?;
        let tombstones = self.tombstones()?;
        let mut out = Vec::new();

        if let Some(candidates) = self.trigram_candidates(matcher)? {
            let mut path_bytes = Vec::new();
            for docid in candidates.iter() {
                if tombstones.contains(docid) {
                    continue;
                }
                let Some(entry) = file_entry_at(entries, self.version, docid) else {
                    continue;
                };
                if resolver
                    .resolve_into(entry.path_index(), &mut path_bytes)
                    .is_none()
                {
                    continue;
                }
                let matched = {
                    let path_str = std::str::from_utf8(&path_bytes)
                        .map(std::borrow::Cow::Borrowed)
                        .unwrap_or_else(|_| String::from_utf8_lossy(&path_bytes));
                    matcher.matches(&path_str)
                };
                if matched {
                    out.push(entry_to_meta(entry, path_bytes.clone()));
                }
            }
            return Ok(out);
        }

        let count = entry_count_from_segment(entries, self.version).unwrap_or(0);
        let mut path_bytes = Vec::new();
        for docid in 0..count {
            let docid = docid as u32;
            if tombstones.contains(docid) {
                continue;
            }
            let Some(entry) = file_entry_at(entries, self.version, docid) else {
                continue;
            };
            if resolver
                .resolve_into(entry.path_index(), &mut path_bytes)
                .is_none()
            {
                continue;
            }
            let matched = {
                let path_str = std::str::from_utf8(&path_bytes)
                    .map(std::borrow::Cow::Borrowed)
                    .unwrap_or_else(|_| String::from_utf8_lossy(&path_bytes));
                matcher.matches(&path_str)
            };
            if matched {
                out.push(entry_to_meta(entry, path_bytes.clone()));
            }
        }

        Ok(out)
    }

    pub fn get_meta(&self, key: FileKey) -> anyhow::Result<Option<FileMeta>> {
        let Some(entries) = self.segment(V7SegKind::EntriesByKey) else {
            return Ok(None);
        };
        let resolver = self.path_resolver()?;
        let tombstones = self.tombstones()?;
        let count = entry_count_from_segment(entries, self.version).unwrap_or(0);
        let mut path_bytes = Vec::new();

        for docid in 0..count {
            let docid = docid as u32;
            if tombstones.contains(docid) {
                continue;
            }
            let Some(entry) = file_entry_at(entries, self.version, docid) else {
                continue;
            };
            if entry.file_key() != key {
                continue;
            }
            if resolver
                .resolve_into(entry.path_index(), &mut path_bytes)
                .is_none()
            {
                continue;
            }
            return Ok(Some(entry_to_meta(entry, path_bytes)));
        }

        Ok(None)
    }

    pub fn for_each_live_entry_path(
        &self,
        mut f: impl FnMut(&FileEntry, &[u8]),
    ) -> anyhow::Result<()> {
        let resolver = self.path_resolver()?;
        let Some(entries) = self.segment(V7SegKind::EntriesByKey) else {
            return Ok(());
        };
        let tombstones = self.tombstones()?;
        let count = entry_count_from_segment(entries, self.version).unwrap_or(0);
        let mut path_bytes = Vec::new();

        for docid in 0..count {
            let docid = docid as u32;
            if tombstones.contains(docid) {
                continue;
            }
            let Some(entry) = file_entry_at(entries, self.version, docid) else {
                continue;
            };
            if resolver
                .resolve_into(entry.path_index(), &mut path_bytes)
                .is_none()
            {
                continue;
            }
            f(&entry, &path_bytes);
        }

        Ok(())
    }

    pub fn live_entry_summary(&self) -> anyhow::Result<(usize, i64, i64)> {
        let Some(entries) = self.segment(V7SegKind::EntriesByKey) else {
            return Ok((0, i64::MAX, i64::MIN));
        };
        let tombstones = self.tombstones()?;
        let count = entry_count_from_segment(entries, self.version).unwrap_or(0);
        let mut live_count = 0usize;
        let mut min_mtime = i64::MAX;
        let mut max_mtime = i64::MIN;

        for docid in 0..count {
            let docid = docid as u32;
            if tombstones.contains(docid) {
                continue;
            }
            let Some(entry) = file_entry_at(entries, self.version, docid) else {
                continue;
            };
            live_count = live_count.saturating_add(1);
            if entry.mtime_ns >= 0 {
                min_mtime = min_mtime.min(entry.mtime_ns);
                max_mtime = max_mtime.max(entry.mtime_ns);
            }
        }

        Ok((live_count, min_mtime, max_mtime))
    }

    pub fn for_each_live_meta(&self, mut f: impl FnMut(FileMeta)) -> anyhow::Result<()> {
        self.for_each_live_entry_path(|entry, path_bytes| {
            f(entry_to_meta(*entry, path_bytes.to_vec()));
        })
    }

    pub fn for_each_live_meta_until(
        &self,
        mut f: impl FnMut(FileMeta) -> bool,
    ) -> anyhow::Result<()> {
        self.for_each_live_entry_path_until(|entry, path_bytes| {
            f(entry_to_meta(*entry, path_bytes.to_vec()))
        })
    }

    pub fn for_each_live_entry_path_until(
        &self,
        mut f: impl FnMut(&FileEntry, &[u8]) -> bool,
    ) -> anyhow::Result<()> {
        let resolver = self.path_resolver()?;
        let Some(entries) = self.segment(V7SegKind::EntriesByKey) else {
            return Ok(());
        };
        let tombstones = self.tombstones()?;
        let count = entry_count_from_segment(entries, self.version).unwrap_or(0);
        let mut path_bytes = Vec::new();

        for docid in 0..count {
            let docid = docid as u32;
            if tombstones.contains(docid) {
                continue;
            }
            let Some(entry) = file_entry_at(entries, self.version, docid) else {
                continue;
            };
            if resolver
                .resolve_into(entry.path_index(), &mut path_bytes)
                .is_none()
            {
                continue;
            }
            if !f(&entry, &path_bytes) {
                break;
            }
        }

        Ok(())
    }

    pub fn parent_candidates(&self, parent_path: &str) -> anyhow::Result<Vec<FileKey>> {
        let Some(entries) = self.segment(V7SegKind::EntriesByKey) else {
            return Ok(Vec::new());
        };
        let Some(parent_index) = self.segment(V7SegKind::ParentIndex) else {
            return Ok(Vec::new());
        };
        let resolver = self.path_resolver()?;
        let parent_bytes = PathBuf::from(parent_path)
            .as_os_str()
            .as_encoded_bytes()
            .to_vec();
        let Some(parent_idx) = resolver.lookup(&parent_bytes) else {
            return Ok(Vec::new());
        };
        let Some(docids) = parent_posting(parent_index, parent_idx)? else {
            return Ok(Vec::new());
        };
        let tombstones = self.tombstones()?;
        let mut out = Vec::with_capacity(docids.len() as usize);
        for docid in docids.iter() {
            if tombstones.contains(docid) {
                continue;
            }
            if let Some(entry) = file_entry_at(entries, self.version, docid) {
                out.push(entry.file_key());
            }
        }
        Ok(out)
    }

    pub fn parent_metas(&self, parent_path: &str) -> anyhow::Result<Vec<FileMeta>> {
        let Some(entries) = self.segment(V7SegKind::EntriesByKey) else {
            return Ok(Vec::new());
        };
        let Some(parent_index) = self.segment(V7SegKind::ParentIndex) else {
            return Ok(Vec::new());
        };
        let resolver = self.path_resolver()?;
        let parent_bytes = PathBuf::from(parent_path)
            .as_os_str()
            .as_encoded_bytes()
            .to_vec();
        let Some(parent_idx) = resolver.lookup(&parent_bytes) else {
            return Ok(Vec::new());
        };
        let Some(docids) = parent_posting(parent_index, parent_idx)? else {
            return Ok(Vec::new());
        };
        let tombstones = self.tombstones()?;
        let mut out = Vec::with_capacity(docids.len() as usize);
        let mut path_bytes = Vec::new();
        for docid in docids.iter() {
            if tombstones.contains(docid) {
                continue;
            }
            let Some(entry) = file_entry_at(entries, self.version, docid) else {
                continue;
            };
            if resolver
                .resolve_into(entry.path_index(), &mut path_bytes)
                .is_none()
            {
                continue;
            }
            out.push(entry_to_meta(entry, path_bytes.clone()));
        }
        Ok(out)
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
            .map(|bytes| decode_file_entry_index(bytes, self.version))
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
            cold_segments: Default::default(),
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
    let (version, num_segments, header_crc) =
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

    #[cfg(unix)]
    {
        // SAFETY: all checksum/validation slices have gone out of use before this call.
        // The mapping is read-only and file-backed; later queries can fault pages back on demand.
        if let Err(e) = unsafe { mmap.unchecked_advise(UncheckedAdvice::DontNeed) } {
            tracing::debug!("v7 mmap MADV_DONTNEED failed for {}: {}", path.display(), e);
        }
    }

    Ok(Some(V7Snapshot {
        mmap: Arc::new(mmap),
        segments,
        version,
    }))
}

/// Lightweight v7 audit used during recovery routing.
///
/// This intentionally validates only structural metadata. Full segment/global
/// CRC validation remains in `load_v7_from_path()` so audit + load do not both
/// sweep every segment during startup.
pub fn shallow_validate_v7(path: &Path) -> anyhow::Result<bool> {
    if !path.exists() {
        return Ok(false);
    }
    let file = std::fs::File::open(path)?;
    let file_len = file.metadata()?.len() as usize;
    if file_len < V7_HEADER_SIZE + V7_TRAILER_FIXED_SIZE {
        return Ok(false);
    }

    let mmap = unsafe { memmap2::MmapOptions::new().map_copy_read_only(&file)? };
    let bytes = mmap.as_ref();
    let header_buf: [u8; V7_HEADER_SIZE] = bytes[0..V7_HEADER_SIZE].try_into()?;
    let (_version, num_segments, header_crc) = match decode_header(&header_buf) {
        Some(header) => header,
        None => return Ok(false),
    };
    if compute_header_crc(&header_buf) != header_crc {
        return Ok(false);
    }

    let Some((trailer, trailer_start)) = V7Trailer::decode_from_file_end(bytes) else {
        return Ok(false);
    };
    if trailer.num_segments != num_segments {
        return Ok(false);
    }

    for i in 0..num_segments as usize {
        let off = trailer.segment_offsets[i] as usize;
        let len = trailer.segment_lens[i] as usize;
        let Some(end) = off.checked_add(len) else {
            return Ok(false);
        };
        if off < V7_HEADER_SIZE || end > trailer_start {
            return Ok(false);
        }
    }

    Ok(true)
}

// ─────────────────────────────────────────────────────────────────────────────
// v7 写入：base + delta → 排序 → 归并 → atomic write v7 单文件
// ─────────────────────────────────────────────────────────────────────────────

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
            encode_full_path_trigram_index(data),
        ),
        (
            V7SegKind::ParentIndex,
            encode_parent_index(&data.parent_index),
        ),
        (V7SegKind::Tombstones, encode_tombstones(&data.tombstones)),
    ];
    write_v7_segments_atomic(path, segments_bytes)
}

#[cfg(test)]
fn write_v7_snapshot_atomic_legacy_trigram(
    path: &Path,
    data: &BaseIndexData,
) -> anyhow::Result<()> {
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
    write_v7_segments_atomic(path, segments_bytes)
}

#[cfg(test)]
fn write_v7_snapshot_atomic_legacy_40b_entry(
    path: &Path,
    data: &BaseIndexData,
) -> anyhow::Result<()> {
    let segments_bytes: Vec<(V7SegKind, Vec<u8>)> = vec![
        (V7SegKind::PathTable, encode_path_table(&data.path_table)),
        (
            V7SegKind::EntriesByKey,
            encode_file_entry_index_legacy_40b(&data.entries_by_key),
        ),
        (
            V7SegKind::EntriesByPath,
            encode_file_entry_index_legacy_40b(&data.entries_by_key),
        ),
        (
            V7SegKind::TrigramIndex,
            encode_full_path_trigram_index(data),
        ),
        (
            V7SegKind::ParentIndex,
            encode_parent_index(&data.parent_index),
        ),
        (V7SegKind::Tombstones, encode_tombstones(&data.tombstones)),
    ];
    write_v7_segments_atomic_with_version(path, segments_bytes, V7_VERSION_LEGACY_40B_ENTRY)
}

fn write_v7_segments_atomic(
    path: &Path,
    segments_bytes: Vec<(V7SegKind, Vec<u8>)>,
) -> anyhow::Result<()> {
    write_v7_segments_atomic_with_version(path, segments_bytes, V7_VERSION)
}

fn write_v7_segments_atomic_with_version(
    path: &Path,
    segments_bytes: Vec<(V7SegKind, Vec<u8>)>,
    version: u32,
) -> anyhow::Result<()> {
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
    crate::storage::atomic_write(path, "v7.tmp", |file| {
        // Header（先占位，crc 后填）
        let mut header_buf = encode_header_with_version(num_segments, 0, version);
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
        header_buf = encode_header_with_version(num_segments, header_crc, version);
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&header_buf)?;

        Ok(())
    })?;

    tracing::info!(
        "v7 snapshot written: {} segments, {} bytes",
        num_segments,
        V7_HEADER_SIZE + cursor + trailer_bytes.len()
    );

    Ok(())
}

/// 从 v6 segments + delta 构建 v7 快照（按 filekey 归并去重后写入）。
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

    // 灌入 delta，按 filekey 归并去重：delta 中的条目覆盖 base 中同 key 条目
    // （delta 代表更新的状态）。使用 HashMap 做最终去重。

    // 收集 base + delta 的所有条目，delta 在后覆盖 base
    let mut latest_by_key: HashMap<FileKey, FileEntry> = HashMap::new();
    for i in 0..merged.entries_by_key.len() {
        if let Some(e) = merged.entries_by_key.get(i) {
            latest_by_key.insert(e.file_key(), *e);
        }
    }
    for i in 0..delta.entries_by_key.len() {
        if let Some(e) = delta.entries_by_key.get(i) {
            latest_by_key.insert(e.file_key(), *e);
        }
    }

    // 重建 entries_by_key
    merged.entries_by_key = FileEntryIndex::new();
    for (_, e) in latest_by_key {
        merged.entries_by_key.push(e);
    }

    // trigram / parent / tombstones：合并时对 parent_index 做归并去重
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

pub fn try_load_v7_cold(path: &Path, roots: &[PathBuf]) -> anyhow::Result<Option<BaseIndexData>> {
    let Some(snapshot) = load_v7_from_path(path)? else {
        return Ok(None);
    };
    let root_path = cold_root_path(roots);
    let generation = file_modified_unix_ns(path);
    let snapshot = Arc::new(snapshot);
    let data =
        BaseIndexData::from_cold_v7_snapshot(path.to_path_buf(), root_path, generation, snapshot)?;
    tracing::info!(
        "v7 snapshot mounted as manifest-only cold segment: {} entries",
        data.file_count()
    );
    Ok(Some(data))
}

fn cold_root_path(roots: &[PathBuf]) -> PathBuf {
    match roots {
        [single] => single.clone(),
        [] => PathBuf::from("/"),
        _ => PathBuf::from("/"),
    }
}

fn file_modified_unix_ns(path: &Path) -> u64 {
    std::fs::metadata(path)
        .and_then(|meta| meta.modified())
        .ok()
        .and_then(|mtime| mtime.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| {
            d.as_secs()
                .saturating_mul(1_000_000_000)
                .saturating_add(u64::from(d.subsec_nanos()))
        })
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{FileKey, FileKind};
    use crate::query::{ExactMatcher, MatchAllMatcher};
    use std::path::PathBuf;

    fn tmp_v7_path(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("fd-rdd-v7-{}-{}", tag, nanos))
    }

    fn add_path_trigrams(index: &mut TrigramIndex, path: &str, docid: u32) {
        let lower = path.to_lowercase();
        for tri in lower.as_bytes().windows(3) {
            let mut key = [0u8; 3];
            key.copy_from_slice(tri);
            index.inner.entry(key).or_default().insert(docid);
        }
    }

    fn add_basename_trigrams(index: &mut TrigramIndex, path: &str, docid: u32) {
        let Some(name) = Path::new(path).file_name() else {
            return;
        };
        let lower = name.to_string_lossy().to_lowercase();
        for tri in lower.as_bytes().windows(3) {
            let mut key = [0u8; 3];
            key.copy_from_slice(tri);
            index.inner.entry(key).or_default().insert(docid);
        }
    }

    fn add_trigram_sentinel(index: &mut TrigramIndex) {
        index.inner.entry([0, 0, 0]).or_default();
    }

    fn sample_query_data() -> (BaseIndexData, FileKey) {
        let key = FileKey {
            dev: 7,
            ino: 11,
            generation: 0,
        };
        let skipped = FileKey {
            dev: 7,
            ino: 12,
            generation: 0,
        };

        let mut paths = PathTableBuilder::new();
        paths.push(0, b"/tmp/cold/needle.txt");
        paths.push(1, b"/tmp/cold/skip.log");
        paths.push(2, b"/tmp/cold");

        let mut entries = FileEntryIndex::new();
        entries.push(FileEntry::from_file_key(key, 0, 123));
        entries.push(FileEntry::from_file_key(skipped, 1, 456));

        let mut data = BaseIndexData {
            path_table: paths.build(),
            entries_by_key: entries.build(),
            ..BaseIndexData::default()
        };
        add_path_trigrams(&mut data.trigram_index, "/tmp/cold/needle.txt", 0);
        add_path_trigrams(&mut data.trigram_index, "/tmp/cold/skip.log", 1);
        add_trigram_sentinel(&mut data.trigram_index);
        data.parent_index.dir_to_files.insert(2, vec![0, 1]);
        data.tombstones.insert(1);
        (data, key)
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
    fn v7_segment_offsets_are_aligned_and_warmup_reports_pages() {
        let path = tmp_v7_path("alignment-warmup");
        let (data, _) = sample_query_data();

        write_v7_snapshot_atomic(&path, &data).unwrap();
        let loaded = load_v7_from_path(&path).unwrap().unwrap();
        assert!(
            loaded
                .segments
                .iter()
                .all(|(_, range)| range.start % 8 == 0),
            "all v7 mmap segment offsets must remain 8-byte aligned"
        );

        let report = loaded.warmup(4096);
        assert!(report.pages >= 1);
        assert!(matches!(
            report.cancel_reason.as_str(),
            "" | "max_bytes" | "unsupported"
        ));

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn v7_roundtrip_preserves_directory_entry_kind() {
        let path = tmp_v7_path("directory-kind");
        let key = FileKey {
            dev: 3,
            ino: 44,
            generation: 0,
        };
        let mut paths = PathTableBuilder::new();
        paths.push(0, b"/tmp/dirprobe");
        let mut entries = FileEntryIndex::new();
        entries.push(FileEntry::from_file_key_and_kind(
            key,
            0,
            789,
            FileKind::Directory,
        ));
        let data = BaseIndexData {
            path_table: paths.build(),
            entries_by_key: entries.build(),
            ..BaseIndexData::default()
        };

        write_v7_snapshot_atomic(&path, &data).unwrap();
        let loaded = load_v7_from_path(&path).unwrap().unwrap();
        let decoded = loaded.to_base_index_data().unwrap();
        let entry = decoded.entries_by_key.get(0).unwrap();

        assert_eq!(entry.file_key(), key);
        assert_eq!(entry.path_index(), 0);
        assert_eq!(entry.kind(), FileKind::Directory);

        let meta = loaded.get_meta(key).unwrap().unwrap();
        assert_eq!(meta.path, PathBuf::from("/tmp/dirprobe"));
        assert_eq!(meta.kind, FileKind::Directory);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn v7_writes_version_2_and_loads_legacy_40b_entries() {
        let path = tmp_v7_path("legacy-40b-entry");
        let key = FileKey {
            dev: 9,
            ino: 77,
            generation: 3,
        };
        let mut paths = PathTableBuilder::new();
        paths.push(0, b"/tmp/legacy-size-entry.txt");
        let mut entries = FileEntryIndex::new();
        entries.push(FileEntry::from_file_key(key, 0, 456));
        let data = BaseIndexData {
            path_table: paths.build(),
            entries_by_key: entries.build(),
            ..BaseIndexData::default()
        };

        write_v7_snapshot_atomic(&path, &data).unwrap();
        let header = std::fs::read(&path).unwrap();
        assert_eq!(
            u32::from_le_bytes(header[8..12].try_into().unwrap()),
            V7_VERSION
        );

        write_v7_snapshot_atomic_legacy_40b_entry(&path, &data).unwrap();
        let loaded = load_v7_from_path(&path).unwrap().unwrap();
        let decoded = loaded.to_base_index_data().unwrap();
        let entry = decoded.entries_by_key.get(0).unwrap();
        assert_eq!(entry.file_key(), key);
        assert_eq!(entry.path_idx, 0);
        assert_eq!(entry.mtime_ns, 456);

        let meta = loaded.get_meta(key).unwrap().unwrap();
        assert_eq!(meta.path, PathBuf::from("/tmp/legacy-size-entry.txt"));
        assert_eq!(meta.size, 0);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn v7_load_missing_returns_none() {
        let path = tmp_v7_path("missing");
        assert!(load_v7_from_path(&path).unwrap().is_none());
    }

    #[test]
    fn v7_query_uses_direct_mmap_segments() {
        let path = tmp_v7_path("direct-query");
        let (data, key) = sample_query_data();
        write_v7_snapshot_atomic(&path, &data).unwrap();
        let loaded = load_v7_from_path(&path).unwrap().unwrap();

        let matcher = ExactMatcher::new("needle", false);
        assert_eq!(loaded.query_keys(&matcher).unwrap(), vec![key]);
        let metas = loaded.query_metas(&matcher).unwrap();
        assert_eq!(metas.len(), 1);
        assert_eq!(metas[0].path, PathBuf::from("/tmp/cold/needle.txt"));

        let meta = loaded.get_meta(key).unwrap().expect("meta should exist");
        assert_eq!(meta.path, PathBuf::from("/tmp/cold/needle.txt"));

        let parent_keys = loaded.parent_candidates("/tmp/cold").unwrap();
        assert_eq!(parent_keys, vec![key]);
        let parent_metas = loaded.parent_metas("/tmp/cold").unwrap();
        assert_eq!(parent_metas.len(), 1);
        assert_eq!(parent_metas[0].file_key, key);

        let missing = ExactMatcher::new("skip", false);
        assert!(loaded.query_keys(&missing).unwrap().is_empty());
        assert!(loaded.query_metas(&missing).unwrap().is_empty());

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn v7_query_full_scan_reuses_path_buffer_for_match_all() {
        let path = tmp_v7_path("match-all-full-scan");
        let (data, key) = sample_query_data();
        write_v7_snapshot_atomic(&path, &data).unwrap();
        let loaded = load_v7_from_path(&path).unwrap().unwrap();

        let matcher = MatchAllMatcher;
        assert_eq!(loaded.query_keys(&matcher).unwrap(), vec![key]);
        let metas = loaded.query_metas(&matcher).unwrap();
        assert_eq!(metas.len(), 1);
        assert_eq!(metas[0].file_key, key);
        assert_eq!(metas[0].path, PathBuf::from("/tmp/cold/needle.txt"));

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn v7_writer_persists_full_path_trigram_sentinel() {
        let path = tmp_v7_path("writer-full-path-sentinel");
        let key = FileKey {
            dev: 7,
            ino: 19,
            generation: 0,
        };

        let mut paths = PathTableBuilder::new();
        paths.push(0, b"/tmp/archive/needle.txt");

        let mut entries = FileEntryIndex::new();
        entries.push(FileEntry::from_file_key(key, 0, 123));

        let data = BaseIndexData {
            path_table: paths.build(),
            entries_by_key: entries.build(),
            ..BaseIndexData::default()
        };

        write_v7_snapshot_atomic(&path, &data).unwrap();
        let loaded = load_v7_from_path(&path).unwrap().unwrap();
        let segment = loaded
            .segment(V7SegKind::TrigramIndex)
            .expect("trigram segment");

        assert!(trigram_index_has_sentinel(segment).unwrap());
        assert!(posting_for_trigram(segment, *b"arc").unwrap().is_some());

        let matcher = ExactMatcher::new("archive", false);
        assert_eq!(loaded.query_keys(&matcher).unwrap(), vec![key]);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn v7_legacy_query_falls_back_full_scan_when_basename_trigram_missing() {
        let path = tmp_v7_path("legacy-basename-trigram-missing");
        let key = FileKey {
            dev: 7,
            ino: 21,
            generation: 0,
        };

        let mut paths = PathTableBuilder::new();
        paths.push(0, b"/tmp/archive/file.txt");

        let mut entries = FileEntryIndex::new();
        entries.push(FileEntry::from_file_key(key, 0, 123));

        let mut data = BaseIndexData {
            path_table: paths.build(),
            entries_by_key: entries.build(),
            ..BaseIndexData::default()
        };
        add_basename_trigrams(&mut data.trigram_index, "/tmp/archive/file.txt", 0);

        write_v7_snapshot_atomic_legacy_trigram(&path, &data).unwrap();
        let loaded = load_v7_from_path(&path).unwrap().unwrap();

        let matcher = ExactMatcher::new("archive", false);
        assert_eq!(loaded.query_keys(&matcher).unwrap(), vec![key]);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn v7_legacy_query_falls_back_full_scan_when_basename_trigram_intersection_is_empty() {
        let path = tmp_v7_path("legacy-basename-trigram-empty-intersection");
        let key = FileKey {
            dev: 7,
            ino: 31,
            generation: 0,
        };
        let noise_keys = [
            FileKey {
                dev: 7,
                ino: 32,
                generation: 0,
            },
            FileKey {
                dev: 7,
                ino: 33,
                generation: 0,
            },
            FileKey {
                dev: 7,
                ino: 34,
                generation: 0,
            },
            FileKey {
                dev: 7,
                ino: 35,
                generation: 0,
            },
            FileKey {
                dev: 7,
                ino: 36,
                generation: 0,
            },
        ];
        let paths_bytes: [&[u8]; 6] = [
            b"/tmp/archive/file.txt",
            b"/tmp/noise/arc.log",
            b"/tmp/noise/rch.log",
            b"/tmp/noise/chi.log",
            b"/tmp/noise/hiv.log",
            b"/tmp/noise/ive.log",
        ];
        let path_strs = [
            "/tmp/archive/file.txt",
            "/tmp/noise/arc.log",
            "/tmp/noise/rch.log",
            "/tmp/noise/chi.log",
            "/tmp/noise/hiv.log",
            "/tmp/noise/ive.log",
        ];

        let mut paths = PathTableBuilder::new();
        for (idx, bytes) in paths_bytes.iter().enumerate() {
            paths.push(idx as u32, bytes);
        }

        let mut entries = FileEntryIndex::new();
        entries.push(FileEntry::from_file_key(key, 0, 123));
        for (idx, noise_key) in noise_keys.iter().enumerate() {
            entries.push(FileEntry::from_file_key(*noise_key, idx as u32 + 1, 123));
        }

        let mut data = BaseIndexData {
            path_table: paths.build(),
            entries_by_key: entries.build(),
            ..BaseIndexData::default()
        };
        for (docid, path_str) in path_strs.iter().enumerate() {
            add_basename_trigrams(&mut data.trigram_index, path_str, docid as u32);
        }

        write_v7_snapshot_atomic_legacy_trigram(&path, &data).unwrap();
        let loaded = load_v7_from_path(&path).unwrap().unwrap();

        let matcher = ExactMatcher::new("archive", false);
        assert_eq!(loaded.query_keys(&matcher).unwrap(), vec![key]);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn v7_query_ignores_basename_only_trigram_candidates_without_sentinel() {
        let path = tmp_v7_path("basename-trigram-no-sentinel");
        let keys = [
            FileKey {
                dev: 7,
                ino: 41,
                generation: 0,
            },
            FileKey {
                dev: 7,
                ino: 42,
                generation: 0,
            },
        ];
        let path_strs = ["/tmp/archive/file.txt", "/tmp/noise/archive.log"];

        let mut paths = PathTableBuilder::new();
        for (idx, path_str) in path_strs.iter().enumerate() {
            paths.push(idx as u32, path_str.as_bytes());
        }

        let mut entries = FileEntryIndex::new();
        for (idx, key) in keys.iter().enumerate() {
            entries.push(FileEntry::from_file_key(*key, idx as u32, 123));
        }

        let mut data = BaseIndexData {
            path_table: paths.build(),
            entries_by_key: entries.build(),
            ..BaseIndexData::default()
        };
        for (docid, path_str) in path_strs.iter().enumerate() {
            add_basename_trigrams(&mut data.trigram_index, path_str, docid as u32);
        }

        write_v7_snapshot_atomic_legacy_trigram(&path, &data).unwrap();
        let loaded = load_v7_from_path(&path).unwrap().unwrap();

        let matcher = ExactMatcher::new("archive", false);
        assert_eq!(loaded.query_keys(&matcher).unwrap(), keys);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn v7_corrupted_file_rejected() {
        let path = tmp_v7_path("corrupt");
        std::fs::write(&path, b"not a v7 file").unwrap();
        assert!(load_v7_from_path(&path).unwrap().is_none());
    }
}
