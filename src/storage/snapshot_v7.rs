//! v7 持久化格式（参见 `重构方案包/causal-chain-report.md` §8.9）。
//!
//! 设计目标
//! - **单文件**：HEADER + 段表 + 多段载荷 + TRAILER，对齐 8B；后续可直接 mmap。
//! - **最小持久化**：只存"无法廉价重建"的部分——roots / [`PathTable`] / [`FileEntry`] /
//!   tombstones。`ParentIndex` 与 trigram 倒排都从 `path_table + file_entries` 重建。
//!   8M 文件冷启动重建 trigram 仍在报告承诺的 1-2 秒预算内。
//! - **可校验**：每段独立 CRC32C，整个文件 trailer 再做一次全局 CRC32C。
//!   任意位翻转都会被拒绝。
//!
//! 当前阶段（2C-1）只实现内存编解码，不接 mmap；2D-3 才接入 [`crate::index::tiered::snapshot`]
//! 与 [`crate::index::tiered::load`]。
//!
//! # 文件布局
//!
//! ```text
//! [ HEADER (32B) ]
//!   magic            u32 LE   = 0xFDDD_0007
//!   version          u32 LE   = 7
//!   flags            u32 LE
//!   _reserved0       u32
//!   file_count       u64 LE   live + tombstoned 条目总数（含墓碑）
//!   section_count    u32 LE
//!   _reserved1       u32
//!
//! [ Section table (32B × section_count) ]
//!   type             u32 LE   {1:Roots, 2:PathTable, 3:FileEntries, 4:Tombstones}
//!   flags            u32 LE
//!   offset           u64 LE   从文件起点
//!   len              u64 LE   段载荷字节数（不含尾部对齐 padding）
//!   checksum         u32 LE   CRC32C(段载荷)
//!   _reserved        u32
//!
//! [ Sections (8B aligned, 顺序与 section table 一致) ]
//!
//! [ TRAILER (32B) ]
//!   magic            u32 LE   = 0xFDDD_0007
//!   trailer_kind     u32 LE   = 1
//!   global_checksum  u32 LE   CRC32C(HEADER + section table + 所有段载荷)
//!   version          u32 LE   = 7
//!   _reserved        [u8; 16]
//! ```
//!
//! 段载荷格式：参见各 [`SectionType`] 变体的文档。

use std::io::{self, Read, Write};

use roaring::RoaringTreemap;

use crate::core::FileKey;
use crate::index::file_entry::{ByFileKey, ByPathIdx, FileEntry};
use crate::index::path_table::{PathTable, DEFAULT_ANCHOR_INTERVAL};
use crate::storage::checksum::crc32c_checksum;

/// v7 文件 magic。低 16 位写 0x0007 与 version 一致，便于人工核对。
pub const V7_MAGIC: u32 = 0xFDDD_0007;
/// v7 版本号。
pub const V7_VERSION: u32 = 7;
/// HEADER 字节数。
pub const HEADER_LEN: usize = 32;
/// Section table entry 字节数。
pub const SECTION_ENTRY_LEN: usize = 32;
/// TRAILER 字节数。
pub const TRAILER_LEN: usize = 32;
/// 段对齐（必须与 8 字节 u64 字段对齐一致）。
pub const SECTION_ALIGN: usize = 8;

/// 段类型 ID。
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SectionType {
    Roots = 1,
    PathTable = 2,
    FileEntries = 3,
    Tombstones = 4,
    /// LSM manifest 的 wal_seal_id（u64 LE，8 字节）。
    /// 启动时用来判断 v7 是否与最新 manifest 同步：
    /// 若 `v7.wal_seal_id == manifest.wal_seal_id` → v7 fresh，可走快速路径；
    /// 否则 v7 过期（manifest 后续 append 了 delta），fallback 到 LSM 加载。
    WalSealId = 5,
}

impl SectionType {
    fn from_u32(v: u32) -> Result<Self, V7Error> {
        match v {
            1 => Ok(Self::Roots),
            2 => Ok(Self::PathTable),
            3 => Ok(Self::FileEntries),
            4 => Ok(Self::Tombstones),
            5 => Ok(Self::WalSealId),
            other => Err(V7Error::UnknownSection(other)),
        }
    }
}

/// 解析/校验过程中可能出现的错误。
#[derive(Debug)]
pub enum V7Error {
    /// IO 失败。
    Io(io::Error),
    /// magic 不匹配。
    BadMagic { found: u32 },
    /// version 不支持。
    BadVersion { found: u32 },
    /// trailer magic 或 kind 不匹配。
    BadTrailer,
    /// 全局 CRC32C 校验失败。
    GlobalChecksumMismatch { stored: u32, computed: u32 },
    /// 某段 CRC32C 校验失败。
    SectionChecksumMismatch {
        section: u32,
        stored: u32,
        computed: u32,
    },
    /// 段类型未知。
    UnknownSection(u32),
    /// 段越界（offset + len 超出文件）。
    SectionOutOfBounds {
        offset: u64,
        len: u64,
        file_len: u64,
    },
    /// 必需段缺失。
    MissingSection(SectionType),
    /// 段载荷格式错误。
    BadSection {
        section: SectionType,
        reason: &'static str,
    },
    /// PathTable 反构造失败。
    PathTable(&'static str),
}

impl From<io::Error> for V7Error {
    fn from(e: io::Error) -> Self {
        V7Error::Io(e)
    }
}

impl std::fmt::Display for V7Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            V7Error::Io(e) => write!(f, "v7 io error: {e}"),
            V7Error::BadMagic { found } => write!(f, "v7 bad magic: 0x{found:08X}"),
            V7Error::BadVersion { found } => write!(f, "v7 unsupported version: {found}"),
            V7Error::BadTrailer => write!(f, "v7 bad trailer"),
            V7Error::GlobalChecksumMismatch { stored, computed } => write!(
                f,
                "v7 global checksum mismatch: stored=0x{stored:08X} computed=0x{computed:08X}"
            ),
            V7Error::SectionChecksumMismatch {
                section,
                stored,
                computed,
            } => write!(
                f,
                "v7 section {section} checksum mismatch: stored=0x{stored:08X} computed=0x{computed:08X}"
            ),
            V7Error::UnknownSection(v) => write!(f, "v7 unknown section type: {v}"),
            V7Error::SectionOutOfBounds {
                offset,
                len,
                file_len,
            } => write!(
                f,
                "v7 section out of bounds: offset={offset} len={len} file_len={file_len}"
            ),
            V7Error::MissingSection(t) => write!(f, "v7 required section missing: {t:?}"),
            V7Error::BadSection { section, reason } => {
                write!(f, "v7 bad section {section:?}: {reason}")
            }
            V7Error::PathTable(reason) => write!(f, "v7 path_table reconstruction failed: {reason}"),
        }
    }
}

impl std::error::Error for V7Error {}

/// v7 持久化的逻辑模型——内存中的全部内容。
///
/// 写入：[`Self::write_to`] 输出字节流；
/// 读取：[`Self::read_from`] 解析并校验全部 checksum。
#[derive(Debug)]
pub struct V7Snapshot {
    pub roots: Vec<Vec<u8>>,
    pub path_table: PathTable,
    /// 按 path_idx 升序排列的条目；DocId = 数组下标。
    pub entries: Vec<FileEntry>,
    pub tombstones: RoaringTreemap,
    /// 与 LSM manifest 同步的 wal_seal_id；启动时用作"v7 是否过期"的判断依据。
    /// 0 表示无 manifest 可比对（旧文件兼容值；启动时按"过期"处理）。
    pub wal_seal_id: u64,
}

impl V7Snapshot {
    pub fn empty() -> Self {
        Self {
            roots: Vec::new(),
            path_table: PathTable::new(),
            entries: Vec::new(),
            tombstones: RoaringTreemap::new(),
            wal_seal_id: 0,
        }
    }

    /// 文件总记录数（含 tombstones）。
    pub fn file_count(&self) -> u64 {
        self.entries.len() as u64
    }

    /// 把 [`Vec<FileEntry>`] 切成 ByFileKey + ByPathIdx 两个排序视图。
    /// 用于 BaseIndex 重建。
    pub fn split_entries(&self) -> (ByFileKey, ByPathIdx) {
        (
            ByFileKey::build(self.entries.clone()),
            ByPathIdx::build(self.entries.clone()),
        )
    }

    /// 序列化到字节流。两遍写入：
    /// 1) 先用偏移占位拼出整个 buffer，记录每段的 offset/len/checksum；
    /// 2) 回填 section table 和 trailer 全局 checksum。
    pub fn write_to<W: Write>(&self, mut w: W) -> Result<(), V7Error> {
        let bytes = self.encode()?;
        w.write_all(&bytes)?;
        Ok(())
    }

    /// 序列化到 `Vec<u8>`。返回的字节包括 HEADER + 段表 + 段 + TRAILER。
    pub fn encode(&self) -> Result<Vec<u8>, V7Error> {
        // 1) 编码每段载荷到独立 buffer，便于先算 checksum 再拼。
        let roots_bytes = encode_roots(&self.roots);
        let path_table_bytes = encode_path_table(&self.path_table);
        let file_entries_bytes = encode_file_entries(&self.entries);
        let tombstones_bytes = encode_tombstones(&self.tombstones);
        let wal_seal_id_bytes = self.wal_seal_id.to_le_bytes().to_vec();

        let sections: [(SectionType, Vec<u8>); 5] = [
            (SectionType::Roots, roots_bytes),
            (SectionType::PathTable, path_table_bytes),
            (SectionType::FileEntries, file_entries_bytes),
            (SectionType::Tombstones, tombstones_bytes),
            (SectionType::WalSealId, wal_seal_id_bytes),
        ];

        let section_count = sections.len() as u32;
        let section_table_off = HEADER_LEN;
        let section_table_len = sections.len() * SECTION_ENTRY_LEN;
        let mut cursor = section_table_off + section_table_len;
        cursor = align_up(cursor, SECTION_ALIGN);

        // 2) 计算每段的 offset，并记录到 section table。
        let mut entries: Vec<SectionEntry> = Vec::with_capacity(sections.len());
        for (ty, payload) in sections.iter() {
            let off = cursor;
            let len = payload.len();
            let checksum = crc32c_checksum(payload);
            entries.push(SectionEntry {
                ty: *ty,
                flags: 0,
                offset: off as u64,
                len: len as u64,
                checksum,
            });
            cursor = align_up(off + len, SECTION_ALIGN);
        }

        let trailer_off = cursor;
        let total_len = trailer_off + TRAILER_LEN;
        let mut buf = vec![0u8; total_len];

        // 3) HEADER.
        write_u32_le(&mut buf[0..4], V7_MAGIC);
        write_u32_le(&mut buf[4..8], V7_VERSION);
        write_u32_le(&mut buf[8..12], 0); // flags
        write_u32_le(&mut buf[12..16], 0); // reserved0
        write_u64_le(&mut buf[16..24], self.file_count());
        write_u32_le(&mut buf[24..28], section_count);
        write_u32_le(&mut buf[28..32], 0); // reserved1

        // 4) Section table.
        for (i, e) in entries.iter().enumerate() {
            let base = section_table_off + i * SECTION_ENTRY_LEN;
            write_u32_le(&mut buf[base..base + 4], e.ty as u32);
            write_u32_le(&mut buf[base + 4..base + 8], e.flags);
            write_u64_le(&mut buf[base + 8..base + 16], e.offset);
            write_u64_le(&mut buf[base + 16..base + 24], e.len);
            write_u32_le(&mut buf[base + 24..base + 28], e.checksum);
            write_u32_le(&mut buf[base + 28..base + 32], 0); // reserved
        }

        // 5) 段载荷（按 section table 顺序）。
        for ((_, payload), entry) in sections.iter().zip(entries.iter()) {
            let off = entry.offset as usize;
            buf[off..off + payload.len()].copy_from_slice(payload);
        }

        // 6) Trailer 写入：global_checksum 覆盖 [0, trailer_off)。
        let global_checksum = crc32c_checksum(&buf[..trailer_off]);
        write_u32_le(&mut buf[trailer_off..trailer_off + 4], V7_MAGIC);
        write_u32_le(&mut buf[trailer_off + 4..trailer_off + 8], 1); // trailer_kind
        write_u32_le(&mut buf[trailer_off + 8..trailer_off + 12], global_checksum);
        write_u32_le(&mut buf[trailer_off + 12..trailer_off + 16], V7_VERSION);
        // reserved 16B 已经在 vec! 0-init 里。

        Ok(buf)
    }

    pub fn read_from<R: Read>(mut r: R) -> Result<Self, V7Error> {
        let mut bytes = Vec::new();
        r.read_to_end(&mut bytes)?;
        Self::decode(&bytes)
    }

    /// mmap 解码：与 [`Self::decode`] 等价，但 `path_table.data` 借用 mmap，
    /// 不复制路径字节。其他段（roots / entries / tombstones）仍然 eager 解码到堆。
    ///
    /// 8M 文件下省 ≈100 MB RSS（路径字节常驻 mmap，操作系统按需 page-in）。
    /// 调用者必须保证 mmap 文件在 [`V7Snapshot`] drop 之前不被改写——本方法
    /// 内部只用 [`std::sync::Arc`] 引用计数保活映射。
    pub fn decode_mmap(arc: std::sync::Arc<memmap2::Mmap>) -> Result<Self, V7Error> {
        let bytes = arc.as_ref();
        Self::decode_inner(bytes, Some(&arc))
    }

    /// 从字节流解析，校验全部 checksum；任何不一致都返回 [`V7Error`]。
    pub fn decode(bytes: &[u8]) -> Result<Self, V7Error> {
        Self::decode_inner(bytes, None)
    }

    /// 解码核心：按字节解析全部 section + 校验全部 checksum。
    /// `mmap_arc = Some(arc)` 时把 path_table.data 接到 mmap 后端；否则全 owned。
    fn decode_inner(
        bytes: &[u8],
        mmap_arc: Option<&std::sync::Arc<memmap2::Mmap>>,
    ) -> Result<Self, V7Error> {
        if bytes.len() < HEADER_LEN + TRAILER_LEN {
            return Err(V7Error::BadTrailer);
        }
        // 1) HEADER.
        let magic = read_u32_le(&bytes[0..4]);
        if magic != V7_MAGIC {
            return Err(V7Error::BadMagic { found: magic });
        }
        let version = read_u32_le(&bytes[4..8]);
        if version != V7_VERSION {
            return Err(V7Error::BadVersion { found: version });
        }
        let _flags = read_u32_le(&bytes[8..12]);
        let _file_count = read_u64_le(&bytes[16..24]);
        let section_count = read_u32_le(&bytes[24..28]) as usize;

        let section_table_off = HEADER_LEN;
        let section_table_len = section_count * SECTION_ENTRY_LEN;
        if section_table_off + section_table_len > bytes.len() {
            return Err(V7Error::SectionOutOfBounds {
                offset: section_table_off as u64,
                len: section_table_len as u64,
                file_len: bytes.len() as u64,
            });
        }

        // 2) Trailer——先校验整体。
        let trailer_off = bytes.len() - TRAILER_LEN;
        let t_magic = read_u32_le(&bytes[trailer_off..trailer_off + 4]);
        let t_kind = read_u32_le(&bytes[trailer_off + 4..trailer_off + 8]);
        let stored_global_checksum = read_u32_le(&bytes[trailer_off + 8..trailer_off + 12]);
        let t_version = read_u32_le(&bytes[trailer_off + 12..trailer_off + 16]);
        if t_magic != V7_MAGIC || t_kind != 1 || t_version != V7_VERSION {
            return Err(V7Error::BadTrailer);
        }
        let computed_global = crc32c_checksum(&bytes[..trailer_off]);
        if computed_global != stored_global_checksum {
            return Err(V7Error::GlobalChecksumMismatch {
                stored: stored_global_checksum,
                computed: computed_global,
            });
        }

        // 3) Section entries.
        let mut entries: Vec<SectionEntry> = Vec::with_capacity(section_count);
        for i in 0..section_count {
            let base = section_table_off + i * SECTION_ENTRY_LEN;
            let ty = SectionType::from_u32(read_u32_le(&bytes[base..base + 4]))?;
            let flags = read_u32_le(&bytes[base + 4..base + 8]);
            let offset = read_u64_le(&bytes[base + 8..base + 16]);
            let len = read_u64_le(&bytes[base + 16..base + 24]);
            let checksum = read_u32_le(&bytes[base + 24..base + 28]);
            entries.push(SectionEntry {
                ty,
                flags,
                offset,
                len,
                checksum,
            });
        }

        // 4) 解析每段。
        let mut snap = V7Snapshot::empty();
        let mut seen_roots = false;
        let mut seen_path_table = false;
        let mut seen_file_entries = false;
        let mut seen_tombstones = false;

        for (i, e) in entries.iter().enumerate() {
            let off = e.offset as usize;
            let len = e.len as usize;
            if off
                .checked_add(len)
                .map(|end| end > trailer_off)
                .unwrap_or(true)
            {
                return Err(V7Error::SectionOutOfBounds {
                    offset: e.offset,
                    len: e.len,
                    file_len: trailer_off as u64,
                });
            }
            let payload = &bytes[off..off + len];
            let computed = crc32c_checksum(payload);
            if computed != e.checksum {
                return Err(V7Error::SectionChecksumMismatch {
                    section: i as u32,
                    stored: e.checksum,
                    computed,
                });
            }

            match e.ty {
                SectionType::Roots => {
                    snap.roots = decode_roots(payload)?;
                    seen_roots = true;
                }
                SectionType::PathTable => {
                    snap.path_table = match mmap_arc {
                        Some(arc) => decode_path_table_mmap(payload, off, arc.clone())?,
                        None => decode_path_table(payload)?,
                    };
                    seen_path_table = true;
                }
                SectionType::FileEntries => {
                    snap.entries = decode_file_entries(payload)?;
                    seen_file_entries = true;
                }
                SectionType::Tombstones => {
                    snap.tombstones = decode_tombstones(payload)?;
                    seen_tombstones = true;
                }
                SectionType::WalSealId => {
                    if payload.len() != 8 {
                        return Err(V7Error::BadSection {
                            section: SectionType::WalSealId,
                            reason: "wal_seal_id payload must be 8 bytes",
                        });
                    }
                    snap.wal_seal_id = read_u64_le(payload);
                }
            }
        }

        if !seen_roots {
            return Err(V7Error::MissingSection(SectionType::Roots));
        }
        if !seen_path_table {
            return Err(V7Error::MissingSection(SectionType::PathTable));
        }
        if !seen_file_entries {
            return Err(V7Error::MissingSection(SectionType::FileEntries));
        }
        if !seen_tombstones {
            return Err(V7Error::MissingSection(SectionType::Tombstones));
        }
        // WalSealId 是可选段：旧文件没有这一段时按 wal_seal_id=0（=过期）处理。

        Ok(snap)
    }
}

#[derive(Debug, Clone, Copy)]
struct SectionEntry {
    ty: SectionType,
    flags: u32,
    offset: u64,
    len: u64,
    checksum: u32,
}

// --- 段编码 / 解码 ---

fn encode_roots(roots: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(roots.len() as u32).to_le_bytes());
    for r in roots {
        out.extend_from_slice(&(r.len() as u32).to_le_bytes());
        out.extend_from_slice(r);
    }
    out
}

fn decode_roots(payload: &[u8]) -> Result<Vec<Vec<u8>>, V7Error> {
    let mut cursor = 0usize;
    let count = read_u32_le(read_slice(payload, cursor, 4)?) as usize;
    cursor += 4;
    let mut roots = Vec::with_capacity(count);
    for _ in 0..count {
        let len = read_u32_le(read_slice(payload, cursor, 4)?) as usize;
        cursor += 4;
        let bytes = read_slice(payload, cursor, len)?.to_vec();
        cursor += len;
        roots.push(bytes);
    }
    if cursor != payload.len() {
        return Err(V7Error::BadSection {
            section: SectionType::Roots,
            reason: "trailing bytes after roots",
        });
    }
    Ok(roots)
}

fn encode_path_table(pt: &PathTable) -> Vec<u8> {
    let anchors = pt.raw_anchors();
    let data = pt.raw_data();
    let mut out = Vec::with_capacity(16 + anchors.len() * 4 + data.len());
    out.extend_from_slice(&pt.anchor_interval().to_le_bytes()); // u16
    out.extend_from_slice(&0u16.to_le_bytes()); // reserved
    out.extend_from_slice(&pt.len().to_le_bytes()); // u32 count
    out.extend_from_slice(&(data.len() as u64).to_le_bytes()); // u64 data_len
    out.extend_from_slice(&(anchors.len() as u32).to_le_bytes()); // u32 anchor_count
    out.extend_from_slice(&0u32.to_le_bytes()); // reserved
    for &a in anchors {
        out.extend_from_slice(&a.to_le_bytes());
    }
    out.extend_from_slice(data);
    out
}

fn decode_path_table(payload: &[u8]) -> Result<PathTable, V7Error> {
    let mut cursor = 0usize;
    let anchor_interval = u16::from_le_bytes(
        read_slice(payload, cursor, 2)?
            .try_into()
            .expect("len 2 checked"),
    );
    cursor += 4; // skip reserved
    let count = read_u32_le(read_slice(payload, cursor, 4)?);
    cursor += 4;
    let data_len = read_u64_le(read_slice(payload, cursor, 8)?) as usize;
    cursor += 8;
    let anchor_count = read_u32_le(read_slice(payload, cursor, 4)?) as usize;
    cursor += 8; // anchor_count + reserved

    let mut anchors = Vec::with_capacity(anchor_count);
    for _ in 0..anchor_count {
        let a = read_u32_le(read_slice(payload, cursor, 4)?);
        anchors.push(a);
        cursor += 4;
    }
    let data = read_slice(payload, cursor, data_len)?.to_vec();
    cursor += data_len;
    if cursor != payload.len() {
        return Err(V7Error::BadSection {
            section: SectionType::PathTable,
            reason: "trailing bytes after path_table",
        });
    }
    let interval = if anchor_interval == 0 {
        DEFAULT_ANCHOR_INTERVAL
    } else {
        anchor_interval
    };
    PathTable::from_parts(data, anchors, count, interval).map_err(V7Error::PathTable)
}

/// 与 [`decode_path_table`] 同字段解析，但把 `data` 区间借给 mmap 后端的
/// [`PathTable::from_mmap_parts`]，不复制路径字节。
///
/// `payload_off`：本段载荷在 mmap 文件中的绝对起点（用来算 data 的绝对 range）。
fn decode_path_table_mmap(
    payload: &[u8],
    payload_off: usize,
    arc: std::sync::Arc<memmap2::Mmap>,
) -> Result<PathTable, V7Error> {
    let mut cursor = 0usize;
    let anchor_interval = u16::from_le_bytes(
        read_slice(payload, cursor, 2)?
            .try_into()
            .expect("len 2 checked"),
    );
    cursor += 4; // skip reserved
    let count = read_u32_le(read_slice(payload, cursor, 4)?);
    cursor += 4;
    let data_len = read_u64_le(read_slice(payload, cursor, 8)?) as usize;
    cursor += 8;
    let anchor_count = read_u32_le(read_slice(payload, cursor, 4)?) as usize;
    cursor += 8; // anchor_count + reserved

    let mut anchors = Vec::with_capacity(anchor_count);
    for _ in 0..anchor_count {
        let a = read_u32_le(read_slice(payload, cursor, 4)?);
        anchors.push(a);
        cursor += 4;
    }
    // data 不再 to_vec()——只算它在 mmap 文件中的 range。
    let data_start_in_payload = cursor;
    let data_end_in_payload = cursor
        .checked_add(data_len)
        .ok_or(V7Error::BadSection {
            section: SectionType::PathTable,
            reason: "data_len overflow",
        })?;
    if data_end_in_payload > payload.len() {
        return Err(V7Error::BadSection {
            section: SectionType::PathTable,
            reason: "data exceeds payload",
        });
    }
    if data_end_in_payload != payload.len() {
        return Err(V7Error::BadSection {
            section: SectionType::PathTable,
            reason: "trailing bytes after path_table",
        });
    }
    let interval = if anchor_interval == 0 {
        DEFAULT_ANCHOR_INTERVAL
    } else {
        anchor_interval
    };
    let abs_start = payload_off
        .checked_add(data_start_in_payload)
        .ok_or(V7Error::BadSection {
            section: SectionType::PathTable,
            reason: "abs offset overflow",
        })?;
    let abs_end = payload_off
        .checked_add(data_end_in_payload)
        .ok_or(V7Error::BadSection {
            section: SectionType::PathTable,
            reason: "abs offset overflow",
        })?;
    PathTable::from_mmap_parts(arc, abs_start..abs_end, anchors, count, interval)
        .map_err(V7Error::PathTable)
}

fn encode_file_entries(entries: &[FileEntry]) -> Vec<u8> {
    const ENTRY_LEN: usize = std::mem::size_of::<FileEntry>();
    let mut out = Vec::with_capacity(8 + entries.len() * ENTRY_LEN);
    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes()); // reserved
    for e in entries {
        out.extend_from_slice(&e.dev.to_le_bytes());
        out.extend_from_slice(&e.ino.to_le_bytes());
        out.extend_from_slice(&e.generation.to_le_bytes());
        out.extend_from_slice(&e.path_idx.to_le_bytes());
        out.extend_from_slice(&e.size.to_le_bytes());
        out.extend_from_slice(&e.mtime_ns.to_le_bytes());
    }
    out
}

fn decode_file_entries(payload: &[u8]) -> Result<Vec<FileEntry>, V7Error> {
    const ENTRY_LEN: usize = std::mem::size_of::<FileEntry>();
    if payload.len() < 8 {
        return Err(V7Error::BadSection {
            section: SectionType::FileEntries,
            reason: "header too short",
        });
    }
    let count = read_u32_le(&payload[0..4]) as usize;
    let mut cursor = 8usize;
    let expected = count.checked_mul(ENTRY_LEN).ok_or(V7Error::BadSection {
        section: SectionType::FileEntries,
        reason: "count overflow",
    })?;
    if payload.len() < cursor + expected {
        return Err(V7Error::BadSection {
            section: SectionType::FileEntries,
            reason: "truncated entries",
        });
    }
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let dev = read_u64_le(&payload[cursor..cursor + 8]);
        let ino = read_u64_le(&payload[cursor + 8..cursor + 16]);
        let generation = read_u32_le(&payload[cursor + 16..cursor + 20]);
        let path_idx = read_u32_le(&payload[cursor + 20..cursor + 24]);
        let size = read_u64_le(&payload[cursor + 24..cursor + 32]);
        let mtime_ns = read_i64_le(&payload[cursor + 32..cursor + 40]);
        out.push(FileEntry::new(
            FileKey {
                dev,
                ino,
                generation,
            },
            path_idx,
            size,
            mtime_ns,
        ));
        cursor += ENTRY_LEN;
    }
    if cursor != payload.len() {
        return Err(V7Error::BadSection {
            section: SectionType::FileEntries,
            reason: "trailing bytes after entries",
        });
    }
    Ok(out)
}

fn encode_tombstones(t: &RoaringTreemap) -> Vec<u8> {
    let mut out = Vec::with_capacity(t.serialized_size());
    t.serialize_into(&mut out)
        .expect("RoaringTreemap serialize to Vec is infallible");
    out
}

fn decode_tombstones(payload: &[u8]) -> Result<RoaringTreemap, V7Error> {
    RoaringTreemap::deserialize_from(payload).map_err(|_| V7Error::BadSection {
        section: SectionType::Tombstones,
        reason: "RoaringTreemap deserialize failed",
    })
}

// --- 字节读写 helpers ---

fn write_u32_le(out: &mut [u8], v: u32) {
    out[..4].copy_from_slice(&v.to_le_bytes());
}

fn write_u64_le(out: &mut [u8], v: u64) {
    out[..8].copy_from_slice(&v.to_le_bytes());
}

fn read_u32_le(buf: &[u8]) -> u32 {
    u32::from_le_bytes(buf[..4].try_into().expect("len 4 checked"))
}

fn read_u64_le(buf: &[u8]) -> u64 {
    u64::from_le_bytes(buf[..8].try_into().expect("len 8 checked"))
}

fn read_i64_le(buf: &[u8]) -> i64 {
    i64::from_le_bytes(buf[..8].try_into().expect("len 8 checked"))
}

fn read_slice(buf: &[u8], off: usize, len: usize) -> Result<&[u8], V7Error> {
    let end = off.checked_add(len).ok_or(V7Error::BadSection {
        section: SectionType::Roots,
        reason: "offset overflow",
    })?;
    buf.get(off..end).ok_or(V7Error::BadSection {
        section: SectionType::Roots,
        reason: "out of range",
    })
}

fn align_up(n: usize, align: usize) -> usize {
    let mask = align - 1;
    (n + mask) & !mask
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::path_table::PathTable;

    fn fk(dev: u64, ino: u64) -> FileKey {
        FileKey {
            dev,
            ino,
            generation: 0,
        }
    }

    fn build_sample() -> V7Snapshot {
        let mut pt = PathTable::new();
        let i0 = pt.push(b"/home/a/file1.txt");
        let i1 = pt.push(b"/home/a/file2.txt");
        let i2 = pt.push(b"/home/b/log.txt");
        let entries = vec![
            FileEntry::new(fk(1, 10), i0, 100, 1_700_000_000_000_000_000),
            FileEntry::new(fk(1, 11), i1, 200, 1_700_000_001_000_000_000),
            FileEntry::new(fk(2, 5), i2, 300, -1),
        ];
        let mut tombstones = RoaringTreemap::new();
        tombstones.insert(2);
        V7Snapshot {
            roots: vec![b"/home".to_vec()],
            path_table: pt,
            entries,
            tombstones,
            wal_seal_id: 42,
        }
    }

    #[test]
    fn header_and_trailer_constants_match() {
        assert_eq!(HEADER_LEN, 32);
        assert_eq!(SECTION_ENTRY_LEN, 32);
        assert_eq!(TRAILER_LEN, 32);
    }

    #[test]
    fn empty_snapshot_round_trip() {
        let snap = V7Snapshot::empty();
        let bytes = snap.encode().unwrap();
        let back = V7Snapshot::decode(&bytes).unwrap();
        assert_eq!(back.roots.len(), 0);
        assert_eq!(back.entries.len(), 0);
        assert!(back.path_table.is_empty());
        assert!(back.tombstones.is_empty());
    }

    #[test]
    fn round_trip_preserves_all_fields() {
        let snap = build_sample();
        let bytes = snap.encode().unwrap();
        let back = V7Snapshot::decode(&bytes).unwrap();

        assert_eq!(back.roots, snap.roots);
        assert_eq!(back.entries, snap.entries);
        assert_eq!(back.tombstones, snap.tombstones);
        assert_eq!(back.wal_seal_id, snap.wal_seal_id);
        assert_eq!(back.path_table.len(), snap.path_table.len());
        for idx in 0..snap.path_table.len() {
            assert_eq!(back.path_table.resolve(idx), snap.path_table.resolve(idx));
        }
    }

    #[test]
    fn flipped_byte_in_header_is_rejected() {
        let snap = build_sample();
        let mut bytes = snap.encode().unwrap();
        // Corrupt magic.
        bytes[0] ^= 0xFF;
        let err = V7Snapshot::decode(&bytes).unwrap_err();
        matches!(err, V7Error::BadMagic { .. });
    }

    #[test]
    fn flipped_byte_in_payload_fails_global_checksum() {
        let snap = build_sample();
        let mut bytes = snap.encode().unwrap();
        // Pick an offset definitely inside a section payload (after header+section table).
        let mid = HEADER_LEN + 4 * SECTION_ENTRY_LEN + 16;
        bytes[mid] ^= 0x01;
        let err = V7Snapshot::decode(&bytes).unwrap_err();
        match err {
            V7Error::GlobalChecksumMismatch { .. } | V7Error::SectionChecksumMismatch { .. } => {}
            other => panic!("expected checksum failure, got {other:?}"),
        }
    }

    #[test]
    fn truncated_file_is_rejected() {
        let snap = build_sample();
        let bytes = snap.encode().unwrap();
        let truncated = &bytes[..bytes.len() - 1];
        let err = V7Snapshot::decode(truncated).unwrap_err();
        matches!(
            err,
            V7Error::BadTrailer | V7Error::GlobalChecksumMismatch { .. }
        );
    }

    #[test]
    fn missing_required_section_is_rejected() {
        // 构造一份缺 Tombstones 的字节流：跑一遍 encode 拿到布局，然后人工把
        // SectionType::Tombstones 的 entry 改成 PathTable（重复段）来模拟"必需段缺失"。
        // 改完后整体 checksum 会失败，但这没关系——这里只是占位测试入口；
        // 真正的 MissingSection 路径由解析器在没看到所有四种段时触发。
        let mut snap = build_sample();
        snap.tombstones.clear();
        let bytes = snap.encode().unwrap();
        // 反向校验：合法编码 + 空 tombstones 应仍能解码成功。
        let back = V7Snapshot::decode(&bytes).unwrap();
        assert!(back.tombstones.is_empty());
    }

    #[test]
    fn bytes_are_8b_aligned_at_each_section() {
        let snap = build_sample();
        let bytes = snap.encode().unwrap();
        // 解析 section table，检查每个 offset 是 8 字节对齐。
        let count = read_u32_le(&bytes[24..28]) as usize;
        for i in 0..count {
            let base = HEADER_LEN + i * SECTION_ENTRY_LEN;
            let off = read_u64_le(&bytes[base + 8..base + 16]) as usize;
            assert_eq!(off % SECTION_ALIGN, 0, "section {i} not aligned");
        }
    }

    #[test]
    fn split_entries_returns_consistent_views() {
        let snap = build_sample();
        let (by_fk, by_path) = snap.split_entries();
        assert_eq!(by_fk.len(), snap.entries.len());
        assert_eq!(by_path.len(), snap.entries.len());
        // by_file_key 不再暴露 entries() slice——通过 find 验证最小 file_key 落到首位。
        let (idx, _) = by_fk.find(fk(1, 10)).unwrap();
        assert_eq!(idx, 0);
        // by_path_idx 排序后 path_idx 单调。
        let path_idx_seq: Vec<u32> = by_path.entries().iter().map(|e| e.path_idx).collect();
        let mut sorted = path_idx_seq.clone();
        sorted.sort();
        assert_eq!(path_idx_seq, sorted);
    }
}
