use std::path::{Path, PathBuf};
use std::sync::Arc;

#[cfg(feature = "rkyv")]
use crate::core::FileKeyEntry;
use crate::util::pathbuf_from_encoded_vec;

use super::path_arena::PathArena;
#[cfg(not(feature = "rkyv"))]
use super::snapshot_format::FKM_FLAG_LEGACY;
#[cfg(feature = "rkyv")]
use super::snapshot_format::FKM_FLAG_RKYV;
use super::snapshot_format::TRIGRAM_SENTINEL;
use super::snapshot_format::{
    BuiltSegments, CompactMeta, IndexSnapshotV5, V6Segments, FKM_MAGIC, FKM_VERSION,
};
use super::{DocId, PersistentIndex};

impl PersistentIndex {
    /// 导出 v5 快照数据
    pub fn export_snapshot_v5(&self) -> IndexSnapshotV5 {
        let (arena, metas) = self.build_legacy_metas(false);
        let tombstones = self
            .tombstones
            .read()
            .iter()
            .map(|v| v as u32)
            .collect::<Vec<u32>>();
        self.dirty
            .store(false, std::sync::atomic::Ordering::Release);
        IndexSnapshotV5 {
            roots_hash: self.roots_hash(),
            arena,
            metas,
            tombstones,
        }
    }

    /// 导出 v6 段（物理 compaction 版）：仅包含 live metas（不携带 tombstones）。
    ///
    /// 用途：段合并/replace-base 时做"真·Tombstone GC"，让段文件尺寸随真实文件系统状态收敛。
    pub fn export_segments_v6_compacted(&self) -> V6Segments {
        let compact = PersistentIndex::new_with_roots(self.roots.clone());
        self.for_each_live_meta(|m| compact.upsert_path_alias(m));
        compact.export_segments_v6()
    }

    /// 构建 v6 全部段 bytes（roots / path_arena / metas / tombstones / trigram_table /
    /// postings_blob / filekey_map），供 `export_segments_v6` 和
    /// `export_segments_v6_to_writer` 共享。
    fn build_all_segments(&self) -> BuiltSegments {
        // roots 段：u16 count + (u16 len + bytes)...
        let mut roots_bytes = Vec::new();
        let roots_count: u16 = self.roots_bytes.len().try_into().unwrap_or(u16::MAX);
        roots_bytes.extend_from_slice(&roots_count.to_le_bytes());
        for rb in self.roots_bytes.iter().take(roots_count as usize) {
            let len: u16 = rb.len().try_into().unwrap_or(u16::MAX);
            roots_bytes.extend_from_slice(&len.to_le_bytes());
            roots_bytes.extend_from_slice(&rb[..len as usize]);
        }

        let (arena, metas) = self.build_legacy_metas(false);

        // PathArena 段：raw bytes（root-relative）
        let path_arena_bytes = Arc::clone(&arena.data);

        // Metas 段：按 DocId 顺序顺排，固定记录大小（little-endian）
        //
        // MetaRecordV6:
        //   dev u64
        //   ino u64
        //   root_id u16
        //   path_off u32
        //   path_len u16
        //   mtime_unix_ns i64 (-1 表示 None)
        let mut metas_bytes = Vec::with_capacity(metas.len() * 32);
        for m in metas.iter() {
            metas_bytes.extend_from_slice(&m.file_key.dev.to_le_bytes());
            metas_bytes.extend_from_slice(&m.file_key.ino.to_le_bytes());
            metas_bytes.extend_from_slice(&m.root_id.to_le_bytes());
            metas_bytes.extend_from_slice(&m.path_off.to_le_bytes());
            metas_bytes.extend_from_slice(&m.path_len.to_le_bytes());
            metas_bytes.extend_from_slice(&m.mtime_ns.to_le_bytes());
        }

        // Tombstones 段：RoaringBitmap serialized bytes（v6 兼容格式；v8 后再切 Treemap）
        let tombstones = self.tombstones.read();
        let mut tombstones_bytes = Vec::new();
        let tomb_bitmap: roaring::RoaringBitmap = tombstones.iter().map(|v| v as u32).collect();
        tomb_bitmap
            .serialize_into(&mut tombstones_bytes)
            .expect("write to vec");
        drop(tombstones);

        // TrigramTable + PostingsBlob 段
        //
        // TrigramEntryV6 (12B):
        //   trigram [u8;3]
        //   pad u8
        //   posting_off u32
        //   posting_len u32
        let tri_idx = self.trigram_index.read();
        let mut entries: Vec<([u8; 3], u32, u32)> = Vec::with_capacity(tri_idx.len());
        let mut postings_blob_bytes = Vec::new();
        for (tri, posting) in tri_idx.iter() {
            let off: u32 = postings_blob_bytes.len().try_into().unwrap_or(u32::MAX);
            let posting_bitmap: roaring::RoaringBitmap = posting.iter().map(|v| v as u32).collect();
            posting_bitmap
                .serialize_into(&mut postings_blob_bytes)
                .expect("write to vec");
            let len: u32 = postings_blob_bytes
                .len()
                .saturating_sub(off as usize)
                .try_into()
                .unwrap_or(u32::MAX);
            entries.push((*tri, off, len));
        }

        // 能力哨兵：TRIGRAM_SENTINEL（定义于模块级常量）
        if !tri_idx.contains_key(&TRIGRAM_SENTINEL) {
            let off: u32 = postings_blob_bytes.len().try_into().unwrap_or(u32::MAX);
            roaring::RoaringBitmap::new()
                .serialize_into(&mut postings_blob_bytes)
                .expect("write to vec");
            let len: u32 = postings_blob_bytes
                .len()
                .saturating_sub(off as usize)
                .try_into()
                .unwrap_or(u32::MAX);
            entries.push((TRIGRAM_SENTINEL, off, len));
        }
        drop(tri_idx);
        entries.sort_by_key(|(tri, _, _)| *tri);

        let mut trigram_table_bytes = Vec::with_capacity(entries.len() * 12);
        for (tri, off, len) in entries {
            trigram_table_bytes.extend_from_slice(&tri);
            trigram_table_bytes.push(0); // pad
            trigram_table_bytes.extend_from_slice(&off.to_le_bytes());
            trigram_table_bytes.extend_from_slice(&len.to_le_bytes());
        }

        // FileKeyMap 段：按 (dev,ino) 排序的 FileKey -> DocId 映射，用于 mmap layer 的反查。
        //
        // 统一 header（8B，LE）：
        //   magic [u8;4] = b"FKM\0"
        //   version u16  = 1
        //   flags u16    = 0 legacy table | 1 rkyv bytes
        //
        // legacy payload：固定记录 24B（LE）：
        //   dev u64
        //   ino u64
        //   generation u32
        //   docid u32
        //
        // 注意：来源为 filekey_to_docid（天然排除 tombstone）。
        let mut pairs: Vec<(crate::core::FileKey, DocId)> = {
            let m = self.filekey_to_docid.read();
            m.iter().map(|(k, v)| (*k, *v)).collect()
        };
        pairs.sort_unstable_by_key(|(k, _)| (k.dev, k.ino, k.generation));

        let mut filekey_map_bytes = Vec::new();
        filekey_map_bytes.extend_from_slice(&FKM_MAGIC);
        filekey_map_bytes.extend_from_slice(&FKM_VERSION.to_le_bytes());

        #[cfg(feature = "rkyv")]
        {
            // rkyv：写入 Vec<FileKeyEntry> 的 archived bytes（可扩展）。
            filekey_map_bytes.extend_from_slice(&FKM_FLAG_RKYV.to_le_bytes());
            let entries: Vec<FileKeyEntry> = pairs
                .into_iter()
                .map(|(key, doc_id)| FileKeyEntry { key, doc_id })
                .collect();
            let bytes = rkyv::to_bytes::<_, 1024>(&entries).expect("rkyv to_bytes");
            filekey_map_bytes.extend_from_slice(bytes.as_ref());
        }

        #[cfg(not(feature = "rkyv"))]
        {
            // 默认：仍写 legacy 定长表（极致性能），但带 header 便于未来平滑切换。
            filekey_map_bytes.extend_from_slice(&FKM_FLAG_LEGACY.to_le_bytes());
            filekey_map_bytes.reserve(pairs.len() * 24);
            for (k, docid) in pairs {
                filekey_map_bytes.extend_from_slice(&k.dev.to_le_bytes());
                filekey_map_bytes.extend_from_slice(&k.ino.to_le_bytes());
                filekey_map_bytes.extend_from_slice(&k.generation.to_le_bytes());
                filekey_map_bytes.extend_from_slice(&(docid as u32).to_le_bytes());
            }
        }

        BuiltSegments {
            roots_bytes,
            path_arena_bytes,
            metas_bytes,
            tombstones_bytes,
            trigram_table_bytes,
            postings_blob_bytes,
            filekey_map_bytes,
        }
    }

    /// 构建完整的 v6 段集合（roots / path_arena / metas / tombstones /
    /// trigram_table / postings_blob / filekey_map），返回已 Arc 包装的 V6Segments。
    ///
    /// 供 `export_segments_v6` 与 `export_segments_v6_to_writer` 共享同一份构建产物，
    /// 避免两者各自重复调用 `build_all_segments` + Arc 包装逻辑。
    fn build_v6_segments(&self) -> V6Segments {
        let s = self.build_all_segments();
        V6Segments {
            roots_bytes: Arc::new(s.roots_bytes),
            path_arena_bytes: s.path_arena_bytes,
            metas_bytes: Arc::new(s.metas_bytes),
            trigram_table_bytes: Arc::new(s.trigram_table_bytes),
            postings_blob_bytes: Arc::new(s.postings_blob_bytes),
            tombstones_bytes: Arc::new(s.tombstones_bytes),
            filekey_map_bytes: Arc::new(s.filekey_map_bytes),
        }
    }

    pub fn export_segments_v6(&self) -> V6Segments {
        self.build_v6_segments()
    }

    fn write_segment(writer: &mut impl std::io::Write, bytes: &[u8]) -> std::io::Result<()> {
        writer.write_all(&(bytes.len() as u64).to_le_bytes())?;
        writer.write_all(bytes)?;
        Ok(())
    }

    /// 将 V6Segments 按固定顺序流式写入 writer，每段带 u64 LE 长度前缀。
    /// 顺序：roots → path_arena → metas → tombstones → trigram_table → postings_blob → filekey_map。
    ///
    /// 供 `export_segments_v6_to_writer` 及 compacted 变体共享写入逻辑。
    fn write_v6_segments_to_writer(
        writer: &mut impl std::io::Write,
        segments: &V6Segments,
    ) -> std::io::Result<()> {
        Self::write_segment(writer, &segments.roots_bytes)?;
        Self::write_segment(writer, segments.path_arena_bytes.as_ref())?;
        Self::write_segment(writer, &segments.metas_bytes)?;
        Self::write_segment(writer, &segments.tombstones_bytes)?;
        Self::write_segment(writer, &segments.trigram_table_bytes)?;
        Self::write_segment(writer, &segments.postings_blob_bytes)?;
        Self::write_segment(writer, &segments.filekey_map_bytes)?;
        Ok(())
    }

    /// 将 v6 段按固定顺序流式写入 writer，每段带 u64 LE 长度前缀。
    /// 顺序：roots → path_arena → metas → tombstones → trigram_table → postings_blob → filekey_map。
    pub fn export_segments_v6_to_writer(
        &self,
        writer: &mut impl std::io::Write,
    ) -> std::io::Result<()> {
        let segments = self.build_v6_segments();
        Self::write_v6_segments_to_writer(writer, &segments)
    }

    /// 导出 v6 段（物理 compaction 版）并流式写入 writer。
    pub fn export_segments_v6_compacted_to_writer(
        &self,
        writer: &mut impl std::io::Write,
    ) -> std::io::Result<()> {
        let compact = PersistentIndex::new_with_roots(self.roots.clone());
        self.for_each_live_meta(|m| compact.upsert_path_alias(m));
        compact.export_segments_v6_to_writer(writer)
    }

    fn build_legacy_metas(&self, live_only: bool) -> (PathArena, Vec<CompactMeta>) {
        let entries = self.entries.read();
        let paths = self.paths.read();
        let tombstones = self.tombstones.read();
        let mut arena = PathArena::new();
        let mut metas = Vec::with_capacity(entries.len());

        for (docid, entry) in entries.iter().enumerate() {
            if live_only && tombstones.contains(docid as DocId) {
                continue;
            }
            let path = paths
                .get(docid)
                .map(|bytes| pathbuf_from_encoded_vec(bytes.clone()))
                .unwrap_or_default();
            let (root_id, rel_bytes) = self.split_root_relative_bytes(path.as_path());
            let (path_off, path_len) = arena.push_bytes(&rel_bytes).unwrap_or((0, 0));
            metas.push(CompactMeta {
                file_key: entry.file_key(),
                root_id,
                path_off,
                path_len,
                mtime_ns: entry.mtime_ns,
            });
        }

        (arena, metas)
    }

    fn split_root_relative_bytes(&self, abs_path: &Path) -> (u16, Vec<u8>) {
        // 选择"最长匹配"的 root（避免 /home 与 /home/user 的歧义）。
        let mut best: Option<(usize, usize, PathBuf)> = None; // (root_id, root_bytes_len, rel_path)
        for (i, root) in self.roots.iter().enumerate() {
            if let Ok(rel) = abs_path.strip_prefix(root) {
                let root_len = self.roots_bytes.get(i).map(|b| b.len()).unwrap_or(0);
                let take = match &best {
                    Some((_, best_len, _)) => root_len > *best_len,
                    None => true,
                };
                if take {
                    best = Some((i, root_len, rel.to_path_buf()));
                }
            }
        }

        let (root_id_usize, rel_path) = if let Some((i, _, rel)) = best {
            (i, rel)
        } else {
            // 兜底：认为 path 在 "/" 下（绝对路径时去掉 leading "/"，否则保留原样）
            let rel = abs_path
                .strip_prefix(Path::new("/"))
                .unwrap_or(abs_path)
                .to_path_buf();
            (0, rel)
        };

        let rel_bytes = rel_path.as_os_str().as_encoded_bytes().to_vec();
        let root_id: u16 = root_id_usize.try_into().unwrap_or(0);
        (root_id, rel_bytes)
    }
}
