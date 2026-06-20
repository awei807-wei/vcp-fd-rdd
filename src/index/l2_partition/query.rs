use roaring::RoaringTreemap;

use crate::core::{FileKey, FileMeta};
use crate::index::file_entry_v2::FileEntry;
use crate::index::IndexLayer;
use crate::query::matcher::Matcher;
use crate::util::pathbuf_from_encoded_vec;

use super::helpers::{
    intern_parent_dirs, mtime_from_ns, normalize_short_hint, query_trigrams,
    trigram_matches_short_hint,
};
use super::types::RebuildPathTable;
use super::{DocId, PersistentIndex};

impl PersistentIndex {
    /// 查询：trigram 候选集（Roaring 交集）→ 精确过滤
    pub fn query(&self, matcher: &dyn Matcher, limit: usize) -> Vec<FileMeta> {
        // 重要：先读取 trigram_index 计算候选集，再读取 entries/tombstones/paths。
        // 写入路径通常是先更新 trigram_index 再更新 entries，如果这里反过来拿锁，
        // 在"边写边查"场景下可能形成死锁。
        let candidates = self
            .trigram_candidates(matcher)
            .or_else(|| self.short_hint_candidates(matcher));

        let entries = self.entries.read();
        let paths = self.paths.read();
        let tombstones = self.tombstones.read();

        match candidates {
            Some(bitmap) => bitmap
                .iter()
                .filter(|docid| !tombstones.contains(*docid))
                .filter_map(|docid| {
                    let entry = entries.get(docid as usize)?;
                    let path_bytes = paths.get(docid as usize)?;
                    Some((entry, path_bytes))
                })
                .filter(|(_, path_bytes)| {
                    let s = std::str::from_utf8(path_bytes)
                        .map(std::borrow::Cow::Borrowed)
                        .unwrap_or_else(|_| String::from_utf8_lossy(path_bytes));
                    matcher.matches(&s)
                })
                .map(|(entry, path_bytes)| {
                    Self::meta_from_entry_and_path(entry, path_bytes.as_slice())
                })
                .take(limit)
                .collect(),
            None => {
                // 无法用 trigram 加速（查询词太短），全量过滤
                entries
                    .iter()
                    .enumerate()
                    .filter_map(|(i, entry)| {
                        let docid: DocId = i as DocId;
                        if tombstones.contains(docid) {
                            return None;
                        }
                        let path_bytes = paths.get(i)?;
                        let s = std::str::from_utf8(path_bytes)
                            .map(std::borrow::Cow::Borrowed)
                            .unwrap_or_else(|_| String::from_utf8_lossy(path_bytes));
                        if matcher.matches(&s) {
                            Some(Self::meta_from_entry_and_path(entry, path_bytes))
                        } else {
                            None
                        }
                    })
                    .take(limit)
                    .collect()
            }
        }
    }

    /// 遍历所有"活跃"文档（跳过 tombstone），用于 Flush/Compaction/重建等离线流程。
    pub fn for_each_live_meta(&self, mut f: impl FnMut(FileMeta)) {
        let entries = self.entries.read();
        let paths = self.paths.read();
        let tombstones = self.tombstones.read();

        for (i, entry) in entries.iter().enumerate() {
            let docid: DocId = i as DocId;
            if tombstones.contains(docid) {
                continue;
            }
            let Some(path_bytes) = paths.get(i) else {
                continue;
            };
            f(Self::meta_from_entry_and_path(entry, path_bytes));
        }
    }

    fn meta_from_entry_and_path(entry: &FileEntry, path_bytes: &[u8]) -> FileMeta {
        FileMeta {
            file_key: entry.file_key(),
            path: pathbuf_from_encoded_vec(path_bytes.to_vec()),
            size: 0,
            mtime: mtime_from_ns(entry.mtime_ns),
            ctime: None,
            atime: None,
            kind: entry.kind(),
        }
    }

    fn trigram_candidates(&self, matcher: &dyn Matcher) -> Option<RoaringTreemap> {
        let hint = matcher.literal_hint()?;
        let s = String::from_utf8_lossy(hint);
        let tris = query_trigrams(s.as_ref());
        if tris.is_empty() {
            return None;
        }

        let tri_idx = self.trigram_index.read();
        let mut sorted_tris = tris.clone();
        sorted_tris.sort_by_key(|t| tri_idx.get(t).map(|b| b.len()).unwrap_or(0));

        let mut acc: Option<RoaringTreemap> = None;
        for tri in &sorted_tris {
            let posting = tri_idx.get(tri)?;
            match acc {
                None => acc = Some(posting.clone()),
                Some(ref mut a) => {
                    *a &= posting;
                    if a.is_empty() {
                        return None;
                    }
                }
            }
        }
        Some(acc.unwrap_or_default())
    }

    fn short_hint_candidates(&self, matcher: &dyn Matcher) -> Option<RoaringTreemap> {
        let hint = normalize_short_hint(matcher.literal_hint()?)?;
        let tri_idx = self.trigram_index.read();
        let mut acc = RoaringTreemap::new();

        for (tri, posting) in tri_idx.iter() {
            if trigram_matches_short_hint(*tri, &hint) {
                acc |= posting.clone();
            }
        }

        Some(acc)
    }

    pub fn to_base_index_data(&self) -> crate::index::base_index::BaseIndexData {
        let entries_v2 = self.entries.read();
        let paths_v2 = self.paths.read();
        let tombstones = self.tombstones.read();
        let trigram_index = self.trigram_index.read();

        let mut rebuild_path_table = RebuildPathTable::new();
        for root in &self.roots_bytes {
            if !root.is_empty() {
                let _ = rebuild_path_table.intern(root.clone(), true);
            }
        }
        let mut entry_path_idxs: Vec<u32> = Vec::with_capacity(entries_v2.len());
        let mut parent_entries: Vec<(u32, u64)> = Vec::with_capacity(entries_v2.len());

        for (docid_usize, abs_bytes) in paths_v2.iter().enumerate() {
            intern_parent_dirs(&mut rebuild_path_table, abs_bytes);
            let path_idx = rebuild_path_table.intern(abs_bytes.clone(), false);
            entry_path_idxs.push(path_idx);
            let docid = docid_usize as DocId;
            if !tombstones.contains(docid) {
                parent_entries.push((path_idx, docid as u64));
            }
        }

        let mut path_table_builder = crate::index::path_table_v2::PathTableBuilder::with_capacity(
            rebuild_path_table.id_to_path.len(),
        );
        for (idx, path_bytes) in rebuild_path_table.id_to_path.iter().enumerate() {
            path_table_builder.push(idx as u32, path_bytes);
        }
        let mut entry_index =
            crate::index::file_entry_v2::FileEntryIndex::with_capacity(entries_v2.len());

        for (docid_usize, entry) in entries_v2.iter().enumerate() {
            let Some(&path_idx) = entry_path_idxs.get(docid_usize) else {
                continue;
            };
            let new_entry = crate::index::file_entry_v2::FileEntry::from_file_key_and_kind(
                entry.file_key(),
                path_idx,
                entry.mtime_ns,
                entry.kind(),
            );
            entry_index.push(new_entry);
        }

        let path_table = path_table_builder.build();
        let entries_by_key = entry_index.build();
        let parent_index = crate::index::parent_index::ParentIndex::build_from_entries(
            &parent_entries,
            &rebuild_path_table,
        );

        let mut tri = crate::index::base_index::TrigramIndex::new();
        for (trigram, posting) in trigram_index.iter() {
            let bitmap: roaring::RoaringBitmap = posting.iter().map(|v| v as u32).collect();
            tri.insert(*trigram, bitmap);
        }

        let tombstones_bitmap: roaring::RoaringBitmap =
            tombstones.iter().map(|v| v as u32).collect();

        crate::index::base_index::BaseIndexData {
            path_table,
            entries_by_key,
            trigram_index: tri,
            parent_index,
            tombstones: tombstones_bitmap,
            cold_segments: Default::default(),
        }
    }
}

impl IndexLayer for PersistentIndex {
    fn query_keys(&self, matcher: &dyn Matcher) -> Vec<FileKey> {
        // 复用 L2 的 trigram 候选集计算，但只输出稳定身份（FileKey）。
        let candidates = self
            .trigram_candidates(matcher)
            .or_else(|| self.short_hint_candidates(matcher));

        let entries = self.entries.read();
        let paths = self.paths.read();
        let tombstones = self.tombstones.read();

        let mut out: Vec<FileKey> = Vec::new();

        match candidates {
            Some(bitmap) => {
                for docid in bitmap.iter() {
                    if tombstones.contains(docid) {
                        continue;
                    }
                    let Some(entry) = entries.get(docid as usize) else {
                        continue;
                    };
                    let Some(path_bytes) = paths.get(docid as usize) else {
                        continue;
                    };
                    let s = std::str::from_utf8(path_bytes)
                        .map(std::borrow::Cow::Borrowed)
                        .unwrap_or_else(|_| String::from_utf8_lossy(path_bytes));
                    if matcher.matches(&s) {
                        out.push(entry.file_key());
                    }
                }
            }
            None => {
                // 无法用 trigram 加速（查询词太短），全量过滤（不构造 PathBuf）。
                for (i, entry) in entries.iter().enumerate() {
                    let docid: DocId = i as DocId;
                    if tombstones.contains(docid) {
                        continue;
                    }
                    let Some(path_bytes) = paths.get(i) else {
                        continue;
                    };
                    let s = std::str::from_utf8(path_bytes)
                        .map(std::borrow::Cow::Borrowed)
                        .unwrap_or_else(|_| String::from_utf8_lossy(path_bytes));
                    if matcher.matches(&s) {
                        out.push(entry.file_key());
                    }
                }
            }
        }

        out
    }

    fn get_meta(&self, key: FileKey) -> Option<FileMeta> {
        let docid = { self.filekey_to_docid.read().get(&key).copied()? };
        if self.tombstones.read().contains(docid) {
            return None;
        }
        let entries = self.entries.read();
        let paths = self.paths.read();
        let entry = entries.get(docid as usize)?;
        let path_bytes = paths.get(docid as usize)?;
        Some(PersistentIndex::meta_from_entry_and_path(entry, path_bytes))
    }
}
