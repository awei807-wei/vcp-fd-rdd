use arc_swap::ArcSwap;
use roaring::RoaringBitmap;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::entry_to_meta;
use crate::core::{FileKey, FileKind, FileMeta};
use crate::index::case_policy::unicode_case_fold_lookup;
pub use crate::index::file_entry_v2::{FileEntry, FileEntryIndex};
use crate::index::parent_index::ParentIndex;
use crate::index::path_table_v2::PathTableV2;
use crate::index::PathFreshness;
use crate::query::Matcher;
use crate::stats::BaseStats;
use crate::storage::snapshot_v7::V7Snapshot;
use crate::util::pathbuf_from_encoded_vec;

// 对小映射反复 MADV_DONTNEED 会把每次目录修复变成可观测的缺页风暴，
// 而保留这些页的最坏常驻成本不超过 256 KiB/segment。
const COLD_SEGMENT_RETAIN_BYTES: usize = 256 * 1024;

fn should_release_cold_segment_pages(mapped_len: usize) -> bool {
    mapped_len > COLD_SEGMENT_RETAIN_BYTES
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MmapWarmupReport {
    pub pages: u64,
    pub elapsed_ms: u64,
    pub cancel_reason: String,
}

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

#[derive(Clone)]
pub struct ColdSegment {
    pub manifest: ColdSegmentManifest,
    snapshot: Arc<V7Snapshot>,
}

impl std::fmt::Debug for ColdSegment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColdSegment")
            .field("manifest", &self.manifest)
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
        let (entry_count, mut min_mtime, mut max_mtime) = snapshot.live_entry_summary()?;

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
            snapshot,
        })
    }

    fn query_keys(&self, matcher: &dyn Matcher) -> Vec<FileKey> {
        let result = self.snapshot.query_keys(matcher);
        self.release_pages_after_access();
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
        let result = self.snapshot.query_metas(matcher);
        self.release_pages_after_access();
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
        self.release_pages_after_access();
        result
    }

    fn for_each_live_meta(&self, f: impl FnMut(FileMeta)) {
        let result = self.snapshot.for_each_live_meta(f);
        self.release_pages_after_access();
        match result {
            Ok(()) => {}
            Err(e) => tracing::warn!(
                "cold segment mmap metadata scan failed for {}: {}",
                self.manifest.segment_path.display(),
                e
            ),
        }
    }

    fn for_each_live_meta_until(&self, mut f: impl FnMut(FileMeta) -> bool) -> bool {
        let mut completed = true;
        let result = self.snapshot.for_each_live_meta_until(|meta| {
            let keep_going = f(meta);
            if !keep_going {
                completed = false;
            }
            keep_going
        });
        self.release_pages_after_access();
        match result {
            Ok(()) => completed,
            Err(e) => {
                tracing::warn!(
                    "cold segment mmap metadata scan failed for {}: {}",
                    self.manifest.segment_path.display(),
                    e
                );
                true
            }
        }
    }

    fn parent_candidates(&self, parent_path: &str) -> Vec<FileKey> {
        let result = self.snapshot.parent_candidates(parent_path);
        self.release_pages_after_access();
        result.unwrap_or_default()
    }

    fn parent_metas(&self, parent_path: &str) -> Vec<FileMeta> {
        let result = self.snapshot.parent_metas(parent_path);
        self.release_pages_after_access();
        result.unwrap_or_default()
    }

    fn release_pages_after_access(&self) {
        if should_release_cold_segment_pages(self.snapshot.mapped_len()) {
            self.snapshot.advise_dontneed();
        }
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
        0
    }

    fn warmup_mmap(&self, max_bytes: u64) -> MmapWarmupReport {
        self.snapshot.warmup(max_bytes)
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

    fn single_current_v7_snapshot(&self) -> anyhow::Result<Arc<V7Snapshot>> {
        let [segment] = self.segments.as_slice() else {
            anyhow::bail!(
                "direct_v7_unsupported: expected one cold segment, found {}",
                self.segments.len()
            );
        };
        segment.snapshot.ensure_direct_delta_compatible()?;
        Ok(segment.snapshot.clone())
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

    fn for_each_live_meta_until(&self, mut f: impl FnMut(FileMeta) -> bool) -> bool {
        for segment in &self.segments {
            if !segment.for_each_live_meta_until(&mut f) {
                return false;
            }
        }
        true
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

    fn warmup_mmap(&self, max_bytes: u64) -> MmapWarmupReport {
        let start = std::time::Instant::now();
        let mut pages = 0u64;
        let mut warmed_bytes = 0u64;
        let mut cancel_reason = String::new();

        for segment in &self.segments {
            if max_bytes > 0 && warmed_bytes >= max_bytes {
                cancel_reason = "max_bytes".to_string();
                break;
            }
            let remaining = if max_bytes == 0 {
                0
            } else {
                max_bytes.saturating_sub(warmed_bytes)
            };
            let report = segment.warmup_mmap(remaining);
            pages = pages.saturating_add(report.pages);
            warmed_bytes = warmed_bytes.saturating_add(report.pages.saturating_mul(4096));
            if !report.cancel_reason.is_empty() && report.cancel_reason != "unsupported" {
                cancel_reason = report.cancel_reason;
                break;
            }
        }

        MmapWarmupReport {
            pages,
            elapsed_ms: start.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
            cancel_reason,
        }
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

    pub(crate) fn single_current_v7_snapshot(&self) -> anyhow::Result<Arc<V7Snapshot>> {
        if !self.path_table.is_empty()
            || !self.entries_by_key.is_empty()
            || !self.trigram_index.is_empty()
            || self.parent_index.file_dir_count() != 0
            || self.parent_index.subdir_dir_count() != 0
            || !self.tombstones.is_empty()
        {
            anyhow::bail!("direct_v7_unsupported: base still contains resident index data");
        }
        self.cold_segments.single_current_v7_snapshot()
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

    pub fn for_each_live_meta_until(&self, mut f: impl FnMut(FileMeta) -> bool) -> bool {
        if !self.resident_for_each_live_meta_until(&mut f) {
            return false;
        }
        self.cold_segments.for_each_live_meta_until(f)
    }

    fn resident_for_each_live_meta(&self, mut f: impl FnMut(FileMeta)) {
        for (docid, entry) in self.entries_by_key.iter().enumerate() {
            if self.tombstones.contains(docid as u32) {
                continue;
            }
            let Some(path_bytes) = self.path_table.resolve(entry.path_index()) else {
                continue;
            };
            f(entry_to_meta(entry, &path_bytes));
        }
    }

    fn resident_for_each_live_meta_until(&self, mut f: impl FnMut(FileMeta) -> bool) -> bool {
        for (docid, entry) in self.entries_by_key.iter().enumerate() {
            if self.tombstones.contains(docid as u32) {
                continue;
            }
            let Some(path_bytes) = self.path_table.resolve(entry.path_index()) else {
                continue;
            };
            if !f(entry_to_meta(entry, &path_bytes)) {
                return false;
            }
        }
        true
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
                    let Some(path_bytes) = self.path_table.resolve(entry.path_index()) else {
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
                    let Some(path_bytes) = self.path_table.resolve(entry.path_index()) else {
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
                    let Some(path_bytes) = self.path_table.resolve(entry.path_index()) else {
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
                    let Some(path_bytes) = self.path_table.resolve(entry.path_index()) else {
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
        let path_bytes = self.path_table.resolve(entry.path_index())?;
        Some(entry_to_meta(entry, &path_bytes))
    }

    pub fn path_freshness(
        &self,
        path: &std::path::Path,
        file_key: FileKey,
        mtime_ns: i64,
        kind: FileKind,
    ) -> PathFreshness {
        let Some(meta) = self.get_meta(file_key) else {
            return PathFreshness::Missing;
        };
        let old_mtime_ns = meta
            .mtime
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .and_then(|d| i64::try_from(d.as_nanos()).ok())
            .unwrap_or(-1);
        if meta.path == path && old_mtime_ns == mtime_ns && meta.kind == kind {
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

    pub fn warmup_cold_segments(&self, max_bytes: u64) -> MmapWarmupReport {
        self.cold_segments.warmup_mmap(max_bytes)
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
            let Some(path_bytes) = self.path_table.resolve(entry.path_index()) else {
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
            let Some(path_bytes) = self.path_table.resolve(entry.path_index()) else {
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
    use crate::core::{FileKey, FileKind, FileMeta};
    use crate::index::l2_partition::PersistentIndex;
    use crate::storage::snapshot_v7::{load_v7_from_path, write_v7_snapshot_atomic};

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

    #[test]
    fn cold_segment_page_release_keeps_small_maps_resident() {
        assert!(!should_release_cold_segment_pages(
            COLD_SEGMENT_RETAIN_BYTES
        ));
        assert!(should_release_cold_segment_pages(
            COLD_SEGMENT_RETAIN_BYTES + 1
        ));
    }

    #[test]
    fn cold_base_live_meta_until_stops_before_full_materialization() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("fd-rdd-cold-until-root-{nanos}"));
        let state = std::env::temp_dir().join(format!("fd-rdd-cold-until-state-{nanos}"));
        std::fs::create_dir_all(&state).unwrap();

        let l2 = PersistentIndex::new_with_roots(vec![root.clone()]);
        for idx in 0..256u64 {
            l2.upsert_path_alias(FileMeta {
                file_key: FileKey {
                    dev: 1,
                    ino: idx + 1,
                    generation: 0,
                },
                path: root
                    .join(format!("dir-{idx:03}"))
                    .join(format!("file-{idx:03}.txt")),
                size: 0,
                mtime: None,
                ctime: None,
                atime: None,
                kind: FileKind::File,
            });
        }

        let segment_path = state.join("segment.v7");
        write_v7_snapshot_atomic(&segment_path, &l2.to_base_index_data()).unwrap();
        let snapshot = Arc::new(load_v7_from_path(&segment_path).unwrap().unwrap());
        let cold = BaseIndexData::from_cold_v7_snapshot(segment_path, root, 1, snapshot).unwrap();

        let mut visited = 0usize;
        let completed = cold.for_each_live_meta_until(|_| {
            visited += 1;
            visited < 7
        });

        assert!(!completed);
        assert_eq!(visited, 7);

        let _ = std::fs::remove_dir_all(state);
    }
}
