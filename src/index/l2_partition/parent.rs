use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::core::FileKey;
use crate::util::pathbuf_from_encoded_vec;

use super::dedupe::{physical_dedupe_stats_from_groups, HardlinkGroup, PhysicalDedupeStats};
use super::helpers::intern_parent_dirs;
use super::parent_path::{CompactPathTable, ParentPathLookup};
use super::{DocId, PersistentIndex};

impl PersistentIndex {
    pub fn hardlink_groups(&self, min_links: usize, prefix: Option<&Path>) -> Vec<HardlinkGroup> {
        let min_links = min_links.max(2);
        let mut groups = self
            .live_paths_by_file_key(prefix)
            .into_iter()
            .filter_map(|(file_key, mut paths)| {
                if paths.len() < min_links {
                    return None;
                }
                paths.sort();
                Some(HardlinkGroup { file_key, paths })
            })
            .collect::<Vec<_>>();
        groups.sort_by_key(|group| (group.file_key, group.paths.first().cloned()));
        groups
    }

    pub fn physical_dedupe_stats(&self) -> PhysicalDedupeStats {
        self.physical_dedupe_stats_for_prefix(None)
    }

    pub fn physical_dedupe_stats_for_prefix(&self, prefix: Option<&Path>) -> PhysicalDedupeStats {
        let groups = self.live_paths_by_file_key(prefix);
        physical_dedupe_stats_from_groups(&groups)
    }

    /// 构建/重建 ParentIndex
    pub fn rebuild_parent_index(&self) {
        let (new_index, parent_lookup) = {
            let mut path_table = CompactPathTable::new();
            let mut parent_entries: Vec<(u32, u64)> = Vec::new();
            let indexed_entries = self.entries.read();
            let paths = self.paths.read();
            let tombstones = self.tombstones.read();

            for (i, entry) in indexed_entries.iter().enumerate() {
                let doc_id = DocId::try_from(i).expect("paths fit in u32 DocId");
                if tombstones.contains(doc_id) {
                    continue;
                }
                let Some(abs) = paths.get_bytes(doc_id) else {
                    continue;
                };
                intern_parent_dirs(&mut path_table, abs);
                let path_idx = path_table.intern(abs, entry.kind().is_directory());
                parent_entries.push((path_idx, doc_id as u64));
            }

            for root in &self.roots_bytes {
                if !root.is_empty() {
                    let _ = path_table.intern(root, true);
                }
            }

            let new_index = crate::index::parent_index::ParentIndex::build_from_entries(
                &parent_entries,
                &path_table,
            );
            let parent_lookup = ParentPathLookup::from_table(&path_table);
            (new_index, parent_lookup)
        };
        *self.parent_index.write() = Some(new_index);
        *self.parent_path_table.write() = Some(parent_lookup);
    }

    /// 使用 ParentIndex 的删除对齐
    pub fn delete_alignment_with_parent_index(
        &self,
        dirty_dirs: &std::collections::HashSet<PathBuf>,
    ) -> Vec<(DocId, PathBuf)> {
        let parent_idx = self.parent_index.read();
        let path_table = self.parent_path_table.read();
        if let (Some(ref index), Some(ref pt)) = (&*parent_idx, &*path_table) {
            let mut dir_idxs = Vec::new();
            for dir in dirty_dirs {
                let dir_bytes = dir.as_os_str().as_encoded_bytes().to_vec();
                if let Some(idx) = pt.lookup(&dir_bytes) {
                    dir_idxs.push(idx);
                }
            }
            let to_check = index.files_in_dirs(&dir_idxs);
            let mut result = Vec::new();
            let paths = self.paths.read();
            for doc_id in to_check {
                let Some(path_bytes) = paths.get_bytes(doc_id) else {
                    continue;
                };
                let path = pathbuf_from_encoded_vec(path_bytes.to_vec());
                result.push((doc_id, path));
            }
            result
        } else {
            Vec::new()
        }
    }

    /// 使用 ParentIndex 查询某目录下的文件候选（Query 加速）
    pub fn parent_candidates(&self, parent_path: &str) -> Vec<FileKey> {
        let parent_idx = self.parent_index.read();
        let path_table = self.parent_path_table.read();
        let (index, pt) = match (parent_idx.as_ref(), path_table.as_ref()) {
            (Some(i), Some(p)) => (i, p),
            _ => return Vec::new(),
        };
        let parent_bytes = PathBuf::from(parent_path)
            .as_os_str()
            .as_encoded_bytes()
            .to_vec();
        let parent_idx = match pt.lookup(&parent_bytes) {
            Some(idx) => idx,
            None => return Vec::new(),
        };
        let bitmap = match index.files_in_dir(parent_idx) {
            Some(b) => b,
            None => return Vec::new(),
        };

        let entries = self.entries.read();
        let mut keys = Vec::with_capacity(bitmap.len());
        for &doc_id in bitmap {
            if let Some(entry) = entries.get(doc_id as usize) {
                keys.push(entry.file_key());
            }
        }
        keys
    }

    fn live_paths_by_file_key(&self, prefix: Option<&Path>) -> HashMap<FileKey, Vec<PathBuf>> {
        let normalized_prefix = prefix.map(crate::index::tiered::normalize_path);
        let entries = self.entries.read();
        let paths = self.paths.read();
        let tombstones = self.tombstones.read();
        let mut groups: HashMap<FileKey, Vec<PathBuf>> = HashMap::new();

        for (docid_usize, entry) in entries.iter().enumerate() {
            let docid = docid_usize as DocId;
            if tombstones.contains(docid) {
                continue;
            }
            if !entry.kind().is_file() {
                continue;
            }
            let Some(path_bytes) = paths.get_bytes(docid) else {
                continue;
            };
            let path = pathbuf_from_encoded_vec(path_bytes.to_vec());
            if normalized_prefix
                .as_ref()
                .is_some_and(|prefix| !path.starts_with(prefix))
            {
                continue;
            }
            groups.entry(entry.file_key()).or_default().push(path);
        }

        groups
    }
}
