use arc_swap::ArcSwap;
use roaring::RoaringBitmap;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use crate::core::{FileKey, FileMeta};
pub use crate::index::file_entry_v2::{FileEntry, FileEntryIndex};
use crate::index::parent_index::ParentIndex;
use crate::index::path_table_v2::PathTableV2;
use crate::query::Matcher;
use crate::util::pathbuf_from_encoded_vec;

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
}

/// BaseIndexData: 只读基础索引的快照数据。
///
/// 所有字段均为只读（ArcSwap 保证读者无锁），后台重建完成后原子切换。
#[derive(Clone, Debug, Default)]
pub struct BaseIndexData {
    pub path_table: PathTableV2,
    pub entries_by_key: FileEntryIndex,
    pub entries_by_path: FileEntryIndex,
    pub trigram_index: TrigramIndex,
    pub parent_index: ParentIndex,
    pub tombstones: RoaringBitmap,
}

impl BaseIndexData {
    pub fn file_count(&self) -> usize {
        self.entries_by_key
            .len()
            .saturating_sub(self.tombstones.len() as usize)
    }

    pub fn for_each_live_meta(&self, mut f: impl FnMut(FileMeta)) {
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

    pub fn get_meta(&self, key: FileKey) -> Option<FileMeta> {
        let docid = self.entries_by_key.lookup_docid_by_filekey(key)?;
        if self.tombstones.contains(docid) {
            return None;
        }
        let entry = self.entries_by_key.get(docid as usize)?;
        let path_bytes = self.path_table.resolve(entry.path_idx)?;
        Some(entry_to_meta(entry, &path_bytes))
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
        for doc_id in to_check.iter() {
            let Some(path_bytes) = self.path_table.resolve(doc_id) else {
                continue;
            };
            let path = pathbuf_from_encoded_vec(path_bytes);
            result.push((doc_id as u64, path));
        }
        result
    }

    pub fn parent_candidates(&self, parent_path: &str) -> Vec<FileKey> {
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

        let mut keys = Vec::with_capacity(bitmap.len() as usize);
        for doc_id in bitmap.iter() {
            if let Some(entry) = self.entries_by_path.get(doc_id as usize) {
                keys.push(entry.file_key());
            }
        }
        keys
    }

    pub fn build_parent_index(&self) -> ParentIndex {
        // Since BaseIndexData's path_table only contains file paths and not directories,
        // we cannot fully rebuild ParentIndex from scratch using PathTableV2.
        // For now, clone the existing parent_index which was correctly built during construction.
        self.parent_index.clone()
    }

    fn trigram_candidates(&self, matcher: &dyn Matcher) -> Option<RoaringBitmap> {
        let hint = matcher.literal_hint()?;
        let lower = String::from_utf8_lossy(hint).to_lowercase();
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

fn entry_to_meta(entry: &FileEntry, path_bytes: &[u8]) -> FileMeta {
    FileMeta {
        file_key: entry.file_key(),
        path: pathbuf_from_encoded_vec(path_bytes.to_vec()),
        size: entry.size,
        mtime: if entry.mtime_ns >= 0 {
            Some(std::time::UNIX_EPOCH + std::time::Duration::from_nanos(entry.mtime_ns as u64))
        } else {
            None
        },
        ctime: None,
        atime: None,
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
            1024,
            -1,
        ));

        let old = idx.replace(data);
        assert!(old.entries_by_key.is_empty());

        let snap = idx.snapshot();
        assert_eq!(snap.entries_by_key.len(), 1);
    }
}
