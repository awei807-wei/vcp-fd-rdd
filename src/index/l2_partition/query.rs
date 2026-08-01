use roaring::RoaringBitmap;

use crate::core::{FileKey, FileMeta};
use crate::index::base_index::{BaseIndexData, TrigramIndex};
use crate::index::file_entry_v2::FileEntry;
use crate::index::IndexLayer;
use crate::query::matcher::Matcher;
use crate::util::pathbuf_from_encoded_vec;

use super::helpers::{
    intern_parent_dirs, mtime_from_ns, normalize_short_hint, query_trigrams,
    trigram_matches_short_hint,
};
use super::parent_path::CompactPathTable;
use super::path_store::PathStore;
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
                    let path_bytes = paths.get_bytes(docid)?;
                    Some((entry, path_bytes))
                })
                .filter(|(_, path_bytes)| {
                    let s = std::str::from_utf8(path_bytes)
                        .map(std::borrow::Cow::Borrowed)
                        .unwrap_or_else(|_| String::from_utf8_lossy(path_bytes));
                    matcher.matches(&s)
                })
                .map(|(entry, path_bytes)| Self::meta_from_entry_and_path(entry, path_bytes))
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
                        let path_bytes = paths.get_bytes(docid)?;
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
            let Some(path_bytes) = paths.get_bytes(docid) else {
                continue;
            };
            f(Self::meta_from_entry_and_path(entry, path_bytes));
        }
    }

    pub(super) fn meta_from_entry_and_path(entry: &FileEntry, path_bytes: &[u8]) -> FileMeta {
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

    fn trigram_candidates(&self, matcher: &dyn Matcher) -> Option<RoaringBitmap> {
        let hint = matcher.literal_hint()?;
        let s = String::from_utf8_lossy(hint);
        let tris = query_trigrams(s.as_ref());
        if tris.is_empty() {
            return None;
        }

        let tri_idx = self.trigram_index.read();
        let mut sorted_tris = tris.clone();
        sorted_tris.sort_by_key(|t| tri_idx.get(t).map(|b| b.len()).unwrap_or(0));

        let mut acc: Option<RoaringBitmap> = None;
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

    fn short_hint_candidates(&self, matcher: &dyn Matcher) -> Option<RoaringBitmap> {
        let hint = normalize_short_hint(matcher.literal_hint()?)?;
        let tri_idx = self.trigram_index.read();
        let mut acc = RoaringBitmap::new();

        for (tri, posting) in tri_idx.iter() {
            if trigram_matches_short_hint(*tri, &hint) {
                acc |= posting.clone();
            }
        }

        Some(acc)
    }

    pub fn to_base_index_data(&self) -> BaseIndexData {
        let entries_v2 = self.entries.read();
        let paths_v2 = self.paths.read();
        let tombstones = self.tombstones.read();
        let trigram_index = self.trigram_index.read();

        build_base_index_data(
            &self.roots_bytes,
            &entries_v2,
            &paths_v2,
            tombstones.clone(),
            TrigramIndex {
                inner: trigram_index.clone(),
            },
        )
    }

    /// Consume this mutable L2 index and build an immutable base without
    /// cloning its largest reusable allocations (trigram postings and
    /// tombstones). Callers must relinquish the L2 generation first.
    pub fn into_base_index_data(self) -> BaseIndexData {
        let PersistentIndex {
            roots,
            roots_bytes,
            entries,
            paths,
            filekey_to_docid,
            path_hash_to_id,
            trigram_index,
            tombstones,
            dirty: _,
            parent_index,
            parent_path_table,
        } = self;

        // These derived lookup structures are not part of BaseIndexData. Drop
        // them before allocating the new path table and parent index so the
        // conversion does not retain both generations at their peak.
        drop(roots);
        drop(filekey_to_docid.into_inner());
        drop(path_hash_to_id.into_inner());
        drop(parent_index.into_inner());
        drop(parent_path_table.into_inner());

        let entries = entries.into_inner();
        let paths = paths.into_inner();
        let tombstones = tombstones.into_inner();
        let trigram_index = TrigramIndex {
            inner: trigram_index.into_inner(),
        };

        build_base_index_data(&roots_bytes, &entries, &paths, tombstones, trigram_index)
    }
}

fn build_base_index_data(
    roots_bytes: &[Vec<u8>],
    entries_v2: &[FileEntry],
    paths_v2: &PathStore,
    tombstones: RoaringBitmap,
    trigram_index: TrigramIndex,
) -> BaseIndexData {
    let (rebuild_path_table, entry_path_idxs, parent_entries) =
        build_base_path_layout(roots_bytes, entries_v2, paths_v2, &tombstones);
    let path_table = build_base_path_table(&rebuild_path_table);
    let entries_by_key = build_base_entry_index(entries_v2, &entry_path_idxs);
    let parent_index = crate::index::parent_index::ParentIndex::build_from_entries(
        &parent_entries,
        &rebuild_path_table,
    );

    BaseIndexData {
        path_table,
        entries_by_key,
        trigram_index,
        parent_index,
        tombstones,
        cold_segments: Default::default(),
    }
}

fn build_base_path_layout(
    roots_bytes: &[Vec<u8>],
    entries_v2: &[FileEntry],
    paths_v2: &PathStore,
    tombstones: &RoaringBitmap,
) -> (CompactPathTable, Vec<u32>, Vec<(u32, u64)>) {
    let mut rebuild_path_table = CompactPathTable::new();
    for root in roots_bytes {
        if !root.is_empty() {
            let _ = rebuild_path_table.intern(root, true);
        }
    }
    let mut entry_path_idxs: Vec<u32> = Vec::with_capacity(entries_v2.len());
    let mut parent_entries: Vec<(u32, u64)> = Vec::with_capacity(entries_v2.len());

    for (docid_usize, entry) in entries_v2.iter().enumerate() {
        let docid = DocId::try_from(docid_usize).expect("entries fit in u32 DocId");
        let Some(abs_bytes) = paths_v2.get_bytes(docid) else {
            continue;
        };
        intern_parent_dirs(&mut rebuild_path_table, abs_bytes);
        let path_idx = rebuild_path_table.intern(abs_bytes, entry.kind().is_directory());
        entry_path_idxs.push(path_idx);
        if !tombstones.contains(docid) {
            parent_entries.push((path_idx, docid as u64));
        }
    }

    (rebuild_path_table, entry_path_idxs, parent_entries)
}

fn build_base_path_table(
    rebuild_path_table: &CompactPathTable,
) -> crate::index::path_table_v2::PathTableV2 {
    let mut path_table_builder =
        crate::index::path_table_v2::PathTableBuilder::with_capacity(rebuild_path_table.len());
    for idx in 0..rebuild_path_table.len() {
        let path_id = u32::try_from(idx).expect("path table fits in u32");
        let path_bytes = rebuild_path_table
            .path_bytes(path_id)
            .expect("path table contains valid refs");
        path_table_builder.push(path_id, path_bytes);
    }
    path_table_builder.build()
}

fn build_base_entry_index(
    entries_v2: &[FileEntry],
    entry_path_idxs: &[u32],
) -> crate::index::file_entry_v2::FileEntryIndex {
    let mut entry_index =
        crate::index::file_entry_v2::FileEntryIndex::with_capacity(entries_v2.len());

    for (docid_usize, entry) in entries_v2.iter().enumerate() {
        let Some(&path_idx) = entry_path_idxs.get(docid_usize) else {
            continue;
        };
        entry_index.push(FileEntry::from_file_key_and_kind(
            entry.file_key(),
            path_idx,
            entry.mtime_ns,
            entry.kind(),
        ));
    }
    entry_index.build()
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
                    let Some(path_bytes) = paths.get_bytes(docid) else {
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
                    let Some(path_bytes) = paths.get_bytes(docid) else {
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
        let docid = self.filekey_representative(key)?;
        if self.tombstones.read().contains(docid) {
            return None;
        }
        let entries = self.entries.read();
        let paths = self.paths.read();
        let entry = entries.get(docid as usize)?;
        let path_bytes = paths.get_bytes(docid)?;
        Some(PersistentIndex::meta_from_entry_and_path(entry, path_bytes))
    }
}

#[cfg(test)]
mod owned_conversion_tests {
    use std::path::PathBuf;

    use crate::core::{FileKey, FileKind, FileMeta};
    use crate::query::matcher::create_matcher;

    use super::PersistentIndex;

    fn meta(file_key: FileKey, path: &str, kind: FileKind) -> FileMeta {
        FileMeta {
            file_key,
            path: PathBuf::from(path),
            size: 0,
            mtime: None,
            ctime: None,
            atime: None,
            kind,
        }
    }

    fn populated_index() -> PersistentIndex {
        let index = PersistentIndex::new_with_roots(vec![PathBuf::from("/tmp/owned-base")]);
        let shared_key = FileKey {
            dev: 7,
            ino: 11,
            generation: 0,
        };
        index.upsert(meta(
            FileKey {
                dev: 7,
                ino: 10,
                generation: 0,
            },
            "/tmp/owned-base/dir",
            FileKind::Directory,
        ));
        index.upsert_path_alias(meta(
            shared_key,
            "/tmp/owned-base/dir/hardlink-a.txt",
            FileKind::File,
        ));
        index.upsert_path_alias(meta(
            shared_key,
            "/tmp/owned-base/dir/hardlink-b.txt",
            FileKind::File,
        ));
        index.upsert(meta(
            FileKey {
                dev: 7,
                ino: 12,
                generation: 0,
            },
            "/tmp/owned-base/deleted.txt",
            FileKind::File,
        ));
        index.mark_deleted_by_path(PathBuf::from("/tmp/owned-base/deleted.txt").as_path());
        index
    }

    fn live_paths(base: &crate::index::base_index::BaseIndexData) -> Vec<PathBuf> {
        let mut paths = Vec::new();
        base.for_each_live_meta(|meta| paths.push(meta.path));
        paths.sort();
        paths
    }

    #[test]
    fn owned_base_conversion_matches_borrowed_query_parent_tombstone_and_hardlinks() {
        let borrowed_source = populated_index();
        let owned_source = populated_index();

        let borrowed = borrowed_source.to_base_index_data();
        let owned = owned_source.into_base_index_data();

        assert_eq!(live_paths(&owned), live_paths(&borrowed));
        assert_eq!(owned.tombstones, borrowed.tombstones);
        assert_eq!(
            owned.parent_candidates("/tmp/owned-base/dir"),
            borrowed.parent_candidates("/tmp/owned-base/dir")
        );

        let matcher = create_matcher("hardlink", true);
        let mut borrowed_matches = borrowed
            .query_metas(matcher.as_ref())
            .into_iter()
            .map(|item| item.meta.path)
            .collect::<Vec<_>>();
        let mut owned_matches = owned
            .query_metas(matcher.as_ref())
            .into_iter()
            .map(|item| item.meta.path)
            .collect::<Vec<_>>();
        borrowed_matches.sort();
        owned_matches.sort();
        assert_eq!(owned_matches, borrowed_matches);
        assert_eq!(owned_matches.len(), 2, "both hardlink aliases must survive");
    }
}
