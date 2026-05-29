use arc_swap::ArcSwap;
use roaring::RoaringBitmap;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::core::{FileKey, FileKind, FileMeta};
use crate::index::case_policy::{folded_lookup_bytes_lossy, unicode_case_fold_lookup};
pub use crate::index::file_entry_v2::{FileEntry, FileEntryIndex};
use crate::index::parent_index::ParentIndex;
use crate::index::path_table_v2::PathTableV2;
use crate::index::PathFreshness;
use crate::query::Matcher;
use crate::stats::BaseStats;
use crate::storage::snapshot_v7::V7Snapshot;
use crate::util::pathbuf_from_encoded_vec;

const COLD_NAME_FILTER_WORDS: usize = 256;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColdSegmentManifest {
    pub segment_id: u64,
    pub root_path: PathBuf,
    pub tier: String,
    pub generation: u64,
    pub last_scan_time: u64,
    pub freshness: String,
    pub dirty_flag: bool,
    pub entry_count: usize,
    pub segment_path: PathBuf,
    pub mtime_min_ns: i64,
    pub mtime_max_ns: i64,
    pub mmap_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct BaseQueryMatch {
    pub meta: FileMeta,
    pub manifest_only: bool,
}

#[derive(Clone, Debug)]
pub struct ColdNameFilter {
    bits: Vec<u64>,
    inserted_trigrams: usize,
}

impl Default for ColdNameFilter {
    fn default() -> Self {
        Self {
            bits: vec![0; COLD_NAME_FILTER_WORDS],
            inserted_trigrams: 0,
        }
    }
}

impl ColdNameFilter {
    fn insert_path_bytes(&mut self, path_bytes: &[u8]) {
        let folded = folded_lookup_bytes_lossy(path_bytes);
        let bytes = folded.as_slice();
        if bytes.len() < 3 {
            return;
        }
        for tri in bytes.windows(3) {
            self.insert_trigram(tri);
        }
    }

    fn insert_trigram(&mut self, tri: &[u8]) {
        let bit = trigram_filter_bit(tri);
        self.bits[bit / 64] |= 1u64 << (bit % 64);
        self.inserted_trigrams = self.inserted_trigrams.saturating_add(1);
    }

    fn might_match_literal_hint(&self, hint: Option<&[u8]>) -> bool {
        let Some(hint) = hint else {
            return true;
        };
        let folded = folded_lookup_bytes_lossy(hint);
        let bytes = folded.as_slice();
        if bytes.len() < 3 {
            return true;
        }
        bytes.windows(3).all(|tri| {
            let bit = trigram_filter_bit(tri);
            (self.bits[bit / 64] & (1u64 << (bit % 64))) != 0
        })
    }

    fn allocated_bytes(&self) -> u64 {
        (std::mem::size_of::<Self>() + self.bits.capacity() * std::mem::size_of::<u64>()) as u64
    }
}

fn trigram_filter_bit(tri: &[u8]) -> usize {
    let mut h = 0x811c9dc5u32;
    for &b in tri.iter().take(3) {
        h ^= u32::from(b);
        h = h.wrapping_mul(0x0100_0193);
    }
    (h as usize) % (COLD_NAME_FILTER_WORDS * 64)
}

#[derive(Clone)]
pub struct ColdSegment {
    pub manifest: ColdSegmentManifest,
    filter: ColdNameFilter,
    snapshot: Arc<V7Snapshot>,
}

impl std::fmt::Debug for ColdSegment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColdSegment")
            .field("manifest", &self.manifest)
            .field("filter_bits", &self.filter.bits.len())
            .field("inserted_trigrams", &self.filter.inserted_trigrams)
            .finish_non_exhaustive()
    }
}

impl ColdSegment {
    pub fn from_v7_snapshot(
        segment_id: u64,
        root_path: PathBuf,
        segment_path: PathBuf,
        generation: u64,
        snapshot: Arc<V7Snapshot>,
    ) -> anyhow::Result<Self> {
        let mut filter = ColdNameFilter::default();
        let mut entry_count = 0usize;
        let mut min_mtime = i64::MAX;
        let mut max_mtime = i64::MIN;

        snapshot.for_each_live_entry_path(|entry, path_bytes| {
            entry_count = entry_count.saturating_add(1);
            filter.insert_path_bytes(path_bytes);
            if entry.mtime_ns >= 0 {
                min_mtime = min_mtime.min(entry.mtime_ns);
                max_mtime = max_mtime.max(entry.mtime_ns);
            }
        })?;

        if min_mtime == i64::MAX {
            min_mtime = -1;
        }
        if max_mtime == i64::MIN {
            max_mtime = -1;
        }

        snapshot.advise_dontneed();

        Ok(Self {
            manifest: ColdSegmentManifest {
                segment_id,
                root_path,
                tier: "L3".to_string(),
                generation,
                last_scan_time: generation,
                freshness: "fresh".to_string(),
                dirty_flag: false,
                entry_count,
                segment_path,
                mtime_min_ns: min_mtime,
                mtime_max_ns: max_mtime,
                mmap_bytes: snapshot.mapped_len() as u64,
            },
            filter,
            snapshot,
        })
    }

    fn query_keys(&self, matcher: &dyn Matcher) -> Vec<FileKey> {
        if !self.filter.might_match_literal_hint(matcher.literal_hint()) {
            return Vec::new();
        }
        let result = self.snapshot.query_keys(matcher);
        self.snapshot.advise_dontneed();
        result.unwrap_or_else(|e| {
            tracing::warn!(
                "cold segment mmap query failed for {}: {}",
                self.manifest.segment_path.display(),
                e
            );
            Vec::new()
        })
    }

    fn query_metas(&self, matcher: &dyn Matcher) -> Vec<FileMeta> {
        if !self.filter.might_match_literal_hint(matcher.literal_hint()) {
            return Vec::new();
        }
        let result = self.snapshot.query_metas(matcher);
        self.snapshot.advise_dontneed();
        result.unwrap_or_else(|e| {
            tracing::warn!(
                "cold segment mmap metadata query failed for {}: {}",
                self.manifest.segment_path.display(),
                e
            );
            Vec::new()
        })
    }

    fn get_meta(&self, key: FileKey) -> Option<FileMeta> {
        let result = self.snapshot.get_meta(key).ok().flatten();
        self.snapshot.advise_dontneed();
        result
    }

    fn for_each_live_meta(&self, f: impl FnMut(FileMeta)) {
        let result = self.snapshot.for_each_live_meta(f);
        self.snapshot.advise_dontneed();
        match result {
            Ok(()) => {}
            Err(e) => tracing::warn!(
                "cold segment mmap metadata scan failed for {}: {}",
                self.manifest.segment_path.display(),
                e
            ),
        }
    }

    fn parent_candidates(&self, parent_path: &str) -> Vec<FileKey> {
        let result = self.snapshot.parent_candidates(parent_path);
        self.snapshot.advise_dontneed();
        result.unwrap_or_default()
    }

    fn parent_metas(&self, parent_path: &str) -> Vec<FileMeta> {
        let result = self.snapshot.parent_metas(parent_path);
        self.snapshot.advise_dontneed();
        result.unwrap_or_default()
    }

    fn manifest_bytes(&self) -> u64 {
        (std::mem::size_of::<ColdSegmentManifest>()
            + self.manifest.root_path.as_os_str().as_encoded_bytes().len()
            + self
                .manifest
                .segment_path
                .as_os_str()
                .as_encoded_bytes()
                .len()
            + self.manifest.tier.len()
            + self.manifest.freshness.len()) as u64
    }

    fn filter_bytes(&self) -> u64 {
        self.filter.allocated_bytes()
    }
}

#[derive(Clone, Debug, Default)]
pub struct ColdSegmentStore {
    segments: Vec<ColdSegment>,
}

impl ColdSegmentStore {
    pub fn single(segment: ColdSegment) -> Self {
        Self {
            segments: vec![segment],
        }
    }

    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    pub fn len(&self) -> usize {
        self.segments.len()
    }

    pub fn manifests(&self) -> impl Iterator<Item = &ColdSegmentManifest> {
        self.segments.iter().map(|segment| &segment.manifest)
    }

    fn query_keys(&self, matcher: &dyn Matcher) -> Vec<FileKey> {
        let mut out = Vec::new();
        for segment in &self.segments {
            out.extend(segment.query_keys(matcher));
        }
        out
    }

    fn query_metas(&self, matcher: &dyn Matcher) -> Vec<FileMeta> {
        let mut out = Vec::new();
        for segment in &self.segments {
            out.extend(segment.query_metas(matcher));
        }
        out
    }

    fn get_meta(&self, key: FileKey) -> Option<FileMeta> {
        self.segments
            .iter()
            .find_map(|segment| segment.get_meta(key))
    }

    fn contains_key(&self, key: FileKey) -> bool {
        self.get_meta(key).is_some()
    }

    fn for_each_live_meta(&self, mut f: impl FnMut(FileMeta)) {
        for segment in &self.segments {
            segment.for_each_live_meta(&mut f);
        }
    }

    fn parent_candidates(&self, parent_path: &str) -> Vec<FileKey> {
        let mut out = Vec::new();
        for segment in &self.segments {
            out.extend(segment.parent_candidates(parent_path));
        }
        out
    }

    fn parent_metas(&self, parent_path: &str) -> Vec<FileMeta> {
        let mut out = Vec::new();
        for segment in &self.segments {
            out.extend(segment.parent_metas(parent_path));
        }
        out
    }

    fn manifest_only_entries(&self) -> usize {
        self.segments
            .iter()
            .map(|segment| segment.manifest.entry_count)
            .sum()
    }

    fn manifest_bytes(&self) -> u64 {
        self.segments.iter().map(ColdSegment::manifest_bytes).sum()
    }

    fn filter_bytes(&self) -> u64 {
        self.segments.iter().map(ColdSegment::filter_bytes).sum()
    }

    fn mmap_bytes(&self) -> u64 {
        self.segments
            .iter()
            .map(|segment| segment.manifest.mmap_bytes)
            .sum()
    }
}

/// TrigramIndex: 只读的 trigram → RoaringBitmap 映射。
///
/// 替代 l2_partition.rs 中的 `HashMap<Trigram, RoaringTreemap>`，
/// 用于 BaseIndex（只读场景，bitmap 比 treemap 更紧凑）。
#[derive(Clone, Debug, Default)]
pub struct TrigramIndex {
    pub inner: HashMap<[u8; 3], RoaringBitmap>,
}

impl TrigramIndex {
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    pub fn insert(&mut self, trigram: [u8; 3], bitmap: RoaringBitmap) {
        self.inner.insert(trigram, bitmap);
    }

    pub fn get(&self, trigram: &[u8; 3]) -> Option<&RoaringBitmap> {
        self.inner.get(trigram)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn memory_stats(&self) -> (usize, usize, u64) {
        use std::mem::size_of;

        let mut postings_total = 0usize;
        let mut bytes =
            size_of::<Self>() + self.inner.capacity() * (size_of::<([u8; 3], RoaringBitmap)>() + 1);
        for bitmap in self.inner.values() {
            postings_total += bitmap.len() as usize;
            bytes += size_of::<RoaringBitmap>() + bitmap.serialized_size();
        }
        (self.inner.len(), postings_total, bytes as u64)
    }
}

/// BaseIndexData: 只读基础索引的快照数据。
///
/// 所有字段均为只读（ArcSwap 保证读者无锁），后台重建完成后原子切换。
#[derive(Clone, Debug, Default)]
pub struct BaseIndexData {
    pub path_table: PathTableV2,
    pub entries_by_key: FileEntryIndex,
    pub trigram_index: TrigramIndex,
    pub parent_index: ParentIndex,
    pub tombstones: RoaringBitmap,
    pub cold_segments: ColdSegmentStore,
}

impl BaseIndexData {
    pub fn from_cold_v7_snapshot(
        segment_path: PathBuf,
        root_path: PathBuf,
        generation: u64,
        snapshot: Arc<V7Snapshot>,
    ) -> anyhow::Result<Self> {
        let segment_id = cold_segment_id(segment_path.as_path(), generation);
        let segment = ColdSegment::from_v7_snapshot(
            segment_id,
            root_path,
            segment_path,
            generation,
            snapshot,
        )?;
        Ok(Self {
            cold_segments: ColdSegmentStore::single(segment),
            ..Self::default()
        })
    }

    pub fn file_count(&self) -> usize {
        self.resident_file_count()
            .saturating_add(self.cold_segments.manifest_only_entries())
    }

    fn resident_file_count(&self) -> usize {
        self.entries_by_key
            .len()
            .saturating_sub(self.tombstones.len() as usize)
    }

    pub fn memory_stats(&self) -> BaseStats {
        let path_table_bytes = self.path_table.allocated_bytes() as u64;
        let entries_bytes = self.entries_by_key.allocated_bytes() as u64;
        let (trigram_distinct, trigram_postings_total, trigram_bytes) =
            self.trigram_index.memory_stats();
        let parent_bytes = self.parent_index.allocated_bytes() as u64;
        let tombstone_bytes =
            std::mem::size_of::<RoaringBitmap>() as u64 + self.tombstones.serialized_size() as u64;
        let cold_manifest_bytes = self.cold_segments.manifest_bytes();
        let cold_filter_bytes = self.cold_segments.filter_bytes();
        let cold_mmap_bytes = self.cold_segments.mmap_bytes();
        let manifest_only_entries = self.cold_segments.manifest_only_entries();
        let estimated_bytes = path_table_bytes
            + entries_bytes
            + trigram_bytes
            + parent_bytes
            + tombstone_bytes
            + cold_manifest_bytes
            + cold_filter_bytes;

        BaseStats {
            file_count: self.file_count(),
            hot_memory_entries: self.resident_file_count(),
            manifest_only_entries,
            cold_segment_count: self.cold_segments.len(),
            path_table_entries: self.path_table.len(),
            path_table_bytes,
            entries_count: self.entries_by_key.len(),
            entries_bytes,
            trigram_distinct,
            trigram_postings_total,
            trigram_bytes,
            parent_file_dirs: self.parent_index.file_dir_count(),
            parent_subdir_dirs: self.parent_index.subdir_dir_count(),
            parent_bytes,
            tombstone_count: self.tombstones.len() as usize,
            tombstone_bytes,
            cold_manifest_bytes,
            cold_filter_bytes,
            cold_mmap_bytes,
            estimated_bytes,
        }
    }

    pub fn for_each_live_meta(&self, mut f: impl FnMut(FileMeta)) {
        self.resident_for_each_live_meta(&mut f);
        self.cold_segments.for_each_live_meta(f);
    }

    fn resident_for_each_live_meta(&self, mut f: impl FnMut(FileMeta)) {
        for (docid, entry) in self.entries_by_key.iter().enumerate() {
            if self.tombstones.contains(docid as u32) {
                continue;
            }
            let Some(path_bytes) = self.path_table.resolve(entry.path_idx) else {
                continue;
            };
            f(entry_to_meta(entry, &path_bytes));
        }
    }

    pub fn query_keys(&self, matcher: &dyn Matcher) -> Vec<FileKey> {
        let mut out = self.resident_query_keys(matcher);
        out.extend(self.cold_segments.query_keys(matcher));
        out
    }

    pub fn query_metas(&self, matcher: &dyn Matcher) -> Vec<BaseQueryMatch> {
        let mut out: Vec<BaseQueryMatch> = self
            .resident_query_metas(matcher)
            .into_iter()
            .map(|meta| BaseQueryMatch {
                meta,
                manifest_only: false,
            })
            .collect();
        out.extend(
            self.cold_segments
                .query_metas(matcher)
                .into_iter()
                .map(|meta| BaseQueryMatch {
                    meta,
                    manifest_only: true,
                }),
        );
        out
    }

    fn resident_query_keys(&self, matcher: &dyn Matcher) -> Vec<FileKey> {
        let candidates = self.trigram_candidates(matcher);
        let mut out = Vec::new();

        match candidates {
            Some(bitmap) => {
                for docid in bitmap.iter() {
                    if self.tombstones.contains(docid) {
                        continue;
                    }
                    let Some(entry) = self.entries_by_key.get(docid as usize) else {
                        continue;
                    };
                    let Some(path_bytes) = self.path_table.resolve(entry.path_idx) else {
                        continue;
                    };
                    let path_str = match std::str::from_utf8(&path_bytes) {
                        Ok(s) => std::borrow::Cow::Borrowed(s),
                        Err(_) => String::from_utf8_lossy(&path_bytes),
                    };
                    if matcher.matches(&path_str) {
                        out.push(entry.file_key());
                    }
                }
            }
            None => {
                for (docid, entry) in self.entries_by_key.iter().enumerate() {
                    if self.tombstones.contains(docid as u32) {
                        continue;
                    }
                    let Some(path_bytes) = self.path_table.resolve(entry.path_idx) else {
                        continue;
                    };
                    let path_str = match std::str::from_utf8(&path_bytes) {
                        Ok(s) => std::borrow::Cow::Borrowed(s),
                        Err(_) => String::from_utf8_lossy(&path_bytes),
                    };
                    if matcher.matches(&path_str) {
                        out.push(entry.file_key());
                    }
                }
            }
        }

        out
    }

    fn resident_query_metas(&self, matcher: &dyn Matcher) -> Vec<FileMeta> {
        let candidates = self.trigram_candidates(matcher);
        let mut out = Vec::new();

        match candidates {
            Some(bitmap) => {
                for docid in bitmap.iter() {
                    if self.tombstones.contains(docid) {
                        continue;
                    }
                    let Some(entry) = self.entries_by_key.get(docid as usize) else {
                        continue;
                    };
                    let Some(path_bytes) = self.path_table.resolve(entry.path_idx) else {
                        continue;
                    };
                    let matched = {
                        let path_str = match std::str::from_utf8(&path_bytes) {
                            Ok(s) => std::borrow::Cow::Borrowed(s),
                            Err(_) => String::from_utf8_lossy(&path_bytes),
                        };
                        matcher.matches(&path_str)
                    };
                    if matched {
                        out.push(entry_to_meta(entry, &path_bytes));
                    }
                }
            }
            None => {
                for (docid, entry) in self.entries_by_key.iter().enumerate() {
                    if self.tombstones.contains(docid as u32) {
                        continue;
                    }
                    let Some(path_bytes) = self.path_table.resolve(entry.path_idx) else {
                        continue;
                    };
                    let matched = {
                        let path_str = match std::str::from_utf8(&path_bytes) {
                            Ok(s) => std::borrow::Cow::Borrowed(s),
                            Err(_) => String::from_utf8_lossy(&path_bytes),
                        };
                        matcher.matches(&path_str)
                    };
                    if matched {
                        out.push(entry_to_meta(entry, &path_bytes));
                    }
                }
            }
        }

        out
    }

    pub fn get_meta(&self, key: FileKey) -> Option<FileMeta> {
        self.resident_get_meta(key)
            .or_else(|| self.cold_segments.get_meta(key))
    }

    fn resident_get_meta(&self, key: FileKey) -> Option<FileMeta> {
        let docid = self.entries_by_key.lookup_docid_by_filekey(key)?;
        if self.tombstones.contains(docid) {
            return None;
        }
        let entry = self.entries_by_key.get(docid as usize)?;
        let path_bytes = self.path_table.resolve(entry.path_idx)?;
        Some(entry_to_meta(entry, &path_bytes))
    }

    pub fn path_freshness(
        &self,
        path: &std::path::Path,
        file_key: FileKey,
        mtime_ns: i64,
    ) -> PathFreshness {
        let Some(meta) = self.get_meta(file_key) else {
            return PathFreshness::Missing;
        };
        let old_mtime_ns = meta
            .mtime
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .and_then(|d| i64::try_from(d.as_nanos()).ok())
            .unwrap_or(-1);
        if meta.path == path && old_mtime_ns == mtime_ns {
            PathFreshness::Unchanged
        } else {
            PathFreshness::Changed
        }
    }

    pub fn key_is_manifest_only(&self, key: FileKey) -> bool {
        self.resident_get_meta(key).is_none() && self.cold_segments.contains_key(key)
    }

    pub fn has_manifest_only_segments(&self) -> bool {
        !self.cold_segments.is_empty()
    }

    pub fn parent_query_metas(&self, parent_path: &str) -> Vec<BaseQueryMatch> {
        let mut out = Vec::new();
        for meta in self.resident_parent_metas(parent_path) {
            out.push(BaseQueryMatch {
                meta,
                manifest_only: false,
            });
        }
        out.extend(
            self.cold_segments
                .parent_metas(parent_path)
                .into_iter()
                .map(|meta| BaseQueryMatch {
                    meta,
                    manifest_only: true,
                }),
        );
        out
    }

    pub fn delete_alignment_with_parent_index(
        &self,
        dirty_dirs: &HashSet<PathBuf>,
    ) -> Vec<(u64, PathBuf)> {
        let mut dir_idxs = Vec::new();
        for dir in dirty_dirs {
            let dir_bytes = dir.as_os_str().as_encoded_bytes();
            if let Some(idx) = self.path_table.lookup(dir_bytes) {
                dir_idxs.push(idx);
            }
        }

        let to_check = self.parent_index.files_in_dirs(&dir_idxs);
        let mut result = Vec::new();
        for doc_id in to_check {
            let Some(entry) = self.entries_by_key.get(doc_id as usize) else {
                continue;
            };
            let Some(path_bytes) = self.path_table.resolve(entry.path_idx) else {
                continue;
            };
            let path = pathbuf_from_encoded_vec(path_bytes);
            result.push((doc_id as u64, path));
        }
        let mut cold_doc_id = result.len() as u64;
        self.cold_segments.for_each_live_meta(|meta| {
            if meta
                .path
                .parent()
                .is_some_and(|parent| dirty_dirs.contains(parent))
            {
                result.push((cold_doc_id, meta.path));
                cold_doc_id = cold_doc_id.saturating_add(1);
            }
        });
        result
    }

    pub fn parent_candidates(&self, parent_path: &str) -> Vec<FileKey> {
        let mut keys = self.resident_parent_candidates(parent_path);
        keys.extend(self.cold_segments.parent_candidates(parent_path));
        keys
    }

    fn resident_parent_candidates(&self, parent_path: &str) -> Vec<FileKey> {
        let parent_bytes = PathBuf::from(parent_path)
            .as_os_str()
            .as_encoded_bytes()
            .to_vec();
        let dir_idx = match self.path_table.lookup(&parent_bytes) {
            Some(idx) => idx,
            None => return Vec::new(),
        };

        let bitmap = match self.parent_index.files_in_dir(dir_idx) {
            Some(b) => b,
            None => return Vec::new(),
        };

        let mut keys = Vec::with_capacity(bitmap.len());
        for &doc_id in bitmap {
            if let Some(entry) = self.entries_by_key.get(doc_id as usize) {
                keys.push(entry.file_key());
            }
        }
        keys
    }

    fn resident_parent_metas(&self, parent_path: &str) -> Vec<FileMeta> {
        let parent_bytes = PathBuf::from(parent_path)
            .as_os_str()
            .as_encoded_bytes()
            .to_vec();
        let dir_idx = match self.path_table.lookup(&parent_bytes) {
            Some(idx) => idx,
            None => return Vec::new(),
        };

        let bitmap = match self.parent_index.files_in_dir(dir_idx) {
            Some(b) => b,
            None => return Vec::new(),
        };

        let mut metas = Vec::with_capacity(bitmap.len());
        for &doc_id in bitmap {
            let Some(entry) = self.entries_by_key.get(doc_id as usize) else {
                continue;
            };
            let Some(path_bytes) = self.path_table.resolve(entry.path_idx) else {
                continue;
            };
            metas.push(entry_to_meta(entry, &path_bytes));
        }
        metas
    }

    pub fn build_parent_index(&self) -> ParentIndex {
        // Since BaseIndexData's path_table only contains file paths and not directories,
        // we cannot fully rebuild ParentIndex from scratch using PathTableV2.
        // For now, clone the existing parent_index which was correctly built during construction.
        self.parent_index.clone()
    }

    fn trigram_candidates(&self, matcher: &dyn Matcher) -> Option<RoaringBitmap> {
        let hint = matcher.literal_hint()?;
        let lower = unicode_case_fold_lookup(&String::from_utf8_lossy(hint));
        let bytes = lower.as_bytes();
        if bytes.len() < 3 {
            return None;
        }
        let tris: Vec<[u8; 3]> = bytes.windows(3).map(|w| [w[0], w[1], w[2]]).collect();

        let mut bitmaps: Vec<RoaringBitmap> = Vec::with_capacity(tris.len());
        for tri in tris {
            bitmaps.push(self.trigram_index.get(&tri)?.clone());
        }
        bitmaps.sort_by_key(|b| b.len());

        let mut iter = bitmaps.into_iter();
        let mut acc = iter.next().unwrap_or_default();
        for b in iter {
            acc &= &b;
            if acc.is_empty() {
                return None;
            }
        }
        Some(acc)
    }
}

fn cold_segment_id(path: &Path, generation: u64) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for &b in path.as_os_str().as_encoded_bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h ^ generation
}

fn entry_to_meta(entry: &FileEntry, path_bytes: &[u8]) -> FileMeta {
    FileMeta {
        file_key: entry.file_key(),
        path: pathbuf_from_encoded_vec(path_bytes.to_vec()),
        size: 0,
        mtime: if entry.mtime_ns >= 0 {
            Some(std::time::UNIX_EPOCH + std::time::Duration::from_nanos(entry.mtime_ns as u64))
        } else {
            None
        },
        ctime: None,
        atime: None,
        kind: FileKind::File,
    }
}

impl crate::index::IndexLayer for BaseIndexData {
    fn query_keys(&self, matcher: &dyn Matcher) -> Vec<FileKey> {
        self.query_keys(matcher)
    }
    fn get_meta(&self, key: FileKey) -> Option<FileMeta> {
        self.get_meta(key)
    }
    fn file_count_estimate(&self) -> usize {
        self.file_count()
    }
}

/// BaseIndex: ArcSwap 包装的只读基础索引。
///
/// ## 设计目标
/// - 查询不持任何锁，通过 `load()` 获取 `Arc<BaseIndexData>` 快照。
/// - 后台重建（snapshot/compaction）完成后原子替换 inner。
pub struct BaseIndex {
    inner: ArcSwap<BaseIndexData>,
}

impl BaseIndex {
    pub fn new(data: BaseIndexData) -> Self {
        Self {
            inner: ArcSwap::from(Arc::new(data)),
        }
    }

    pub fn empty() -> Self {
        Self::new(BaseIndexData::default())
    }

    /// 获取当前只读快照（O(1)，无锁）。
    pub fn snapshot(&self) -> Arc<BaseIndexData> {
        self.inner.load_full()
    }

    /// 原子替换底层数据（后台重建完成后调用）。
    pub fn swap(&self, data: Arc<BaseIndexData>) -> Arc<BaseIndexData> {
        self.inner.swap(data)
    }

    /// 便利方法：从 BaseIndexData 直接替换。
    pub fn replace(&self, data: BaseIndexData) -> Arc<BaseIndexData> {
        self.swap(Arc::new(data))
    }
}

impl Default for BaseIndex {
    fn default() -> Self {
        Self::empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::FileKey;

    #[test]
    fn base_index_empty_snapshot() {
        let idx = BaseIndex::empty();
        let snap = idx.snapshot();
        assert!(snap.path_table.is_empty());
        assert!(snap.entries_by_key.is_empty());
        assert!(snap.trigram_index.is_empty());
        assert!(snap.tombstones.is_empty());
    }

    #[test]
    fn base_index_swap_atomic() {
        let idx = BaseIndex::empty();

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

        let old = idx.replace(data);
        assert!(old.entries_by_key.is_empty());

        let snap = idx.snapshot();
        assert_eq!(snap.entries_by_key.len(), 1);
    }
}
