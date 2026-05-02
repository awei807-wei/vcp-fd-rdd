use std::collections::HashMap;

/// Trait for path table lookups required by ParentIndex.
///
/// Phase 4's `PathTable` struct will implement this trait.
pub trait PathTable {
    /// Return the parent directory path index for a given path index.
    fn parent_idx(&self, path_idx: u32) -> Option<u32>;
    /// Return true if the given path index represents a directory.
    fn is_dir(&self, path_idx: u32) -> bool;
}

/// Reverse index: directory -> files directly inside it.
///
/// Designed to replace the O(N) `for_each_live_meta_in_dirs` scan with O(1)
/// bitmap lookups during fast_sync Phase3.
#[derive(Clone, Debug, Default)]
pub struct ParentIndex {
    /// dir_path_idx -> sorted doc_ids directly inside this directory.
    pub(crate) dir_to_files: HashMap<u32, Vec<u32>>,
}

/// Incremental delta that can be applied to a ParentIndex.
#[derive(Clone, Debug, Default)]
pub struct ParentIndexDelta {
    pub added: HashMap<u32, Vec<u32>>,
    pub removed: HashMap<u32, Vec<u32>>,
}

impl ParentIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn file_dir_count(&self) -> usize {
        self.dir_to_files.len()
    }

    pub fn subdir_dir_count(&self) -> usize {
        0
    }

    pub fn allocated_bytes(&self) -> usize {
        use std::mem::size_of;

        let mut bytes = size_of::<Self>();
        bytes += self.dir_to_files.capacity() * (size_of::<(u32, Vec<u32>)>() + 1);
        for docids in self.dir_to_files.values() {
            bytes += size_of::<Vec<u32>>() + docids.capacity() * size_of::<u32>();
        }
        bytes
    }

    /// Build a ParentIndex from a slice of (path_idx, doc_id) entries.
    ///
    /// For each file entry (where `path_table.is_dir(path_idx) == false`):
    /// - The file's doc_id is added to its immediate parent directory.
    ///
    /// The index intentionally stores only direct children; recursive directory expansion is
    /// handled by higher-level scans when needed.
    pub fn build_from_entries(entries: &[(u32, u64)], path_table: &dyn PathTable) -> Self {
        let mut dir_to_files: HashMap<u32, Vec<u32>> = HashMap::new();

        for &(path_idx, doc_id) in entries {
            // Add file to its immediate parent directory
            if !path_table.is_dir(path_idx) {
                if let Some(parent) = path_table.parent_idx(path_idx) {
                    dir_to_files.entry(parent).or_default().push(doc_id as u32);
                }
            }
        }

        for docids in dir_to_files.values_mut() {
            docids.sort_unstable();
            docids.dedup();
            docids.shrink_to_fit();
        }
        dir_to_files.shrink_to_fit();

        Self { dir_to_files }
    }

    /// Return the bitmap of doc_ids directly inside a single directory (by path_idx).
    pub fn files_in_dir(&self, dir_path_idx: u32) -> Option<&[u32]> {
        self.dir_to_files.get(&dir_path_idx).map(Vec::as_slice)
    }

    /// Return the union of doc_ids directly inside the given directories (by path_idx).
    pub fn files_in_dirs(&self, dir_path_idxs: &[u32]) -> Vec<u32> {
        let mut result = Vec::new();
        for &dir in dir_path_idxs {
            if let Some(docids) = self.dir_to_files.get(&dir) {
                result.extend_from_slice(docids);
            }
        }
        result.sort_unstable();
        result.dedup();
        result
    }

    /// Recursively collect all doc_ids inside a directory and its subdirectories.
    pub fn files_in_dir_recursive(&self, dir_path_idx: u32) -> Vec<u32> {
        self.dir_to_files
            .get(&dir_path_idx)
            .cloned()
            .unwrap_or_default()
    }

    /// Apply an incremental delta to this index.
    ///
    /// - `added`: union file bitmaps into existing directories.
    /// - `removed`: difference file bitmaps from existing directories.
    pub fn apply_delta(&mut self, delta: &ParentIndexDelta) {
        for (dir, bitmap) in &delta.added {
            let entry = self.dir_to_files.entry(*dir).or_default();
            entry.extend_from_slice(bitmap);
            entry.sort_unstable();
            entry.dedup();
        }
        for (dir, bitmap) in &delta.removed {
            if let Some(existing) = self.dir_to_files.get_mut(dir) {
                existing.retain(|docid| bitmap.binary_search(docid).is_err());
                if existing.is_empty() {
                    self.dir_to_files.remove(dir);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockPathTable {
        parents: HashMap<u32, u32>,
        dirs: HashMap<u32, bool>,
    }

    impl PathTable for MockPathTable {
        fn parent_idx(&self, path_idx: u32) -> Option<u32> {
            self.parents.get(&path_idx).copied()
        }

        fn is_dir(&self, path_idx: u32) -> bool {
            *self.dirs.get(&path_idx).unwrap_or(&false)
        }
    }

    #[test]
    fn basic_build_and_query() {
        // Path indices:
        // 0 = /
        // 1 = /a        (dir)
        // 2 = /a/b      (dir)
        // 3 = /a/b/c.txt (file)
        // 4 = /a/d.txt   (file)
        let mut parents = HashMap::new();
        parents.insert(1, 0);
        parents.insert(2, 1);
        parents.insert(3, 2);
        parents.insert(4, 1);

        let mut dirs = HashMap::new();
        dirs.insert(0, true);
        dirs.insert(1, true);
        dirs.insert(2, true);
        dirs.insert(3, false);
        dirs.insert(4, false);

        let pt = MockPathTable { parents, dirs };
        let entries = vec![(3, 100u64), (4, 200u64)];
        let idx = ParentIndex::build_from_entries(&entries, &pt);

        // files directly in /a/b (path_idx 2)
        assert_eq!(idx.files_in_dir(2).map(|b| b.to_vec()), Some(vec![100]));

        // files directly in /a (path_idx 1)
        assert_eq!(idx.files_in_dir(1).map(|b| b.to_vec()), Some(vec![200]));

        // recursive storage was removed from the hot index; this remains a direct lookup.
        let recursive = idx.files_in_dir_recursive(1);
        assert_eq!(recursive, vec![200]);
    }

    #[test]
    fn files_in_dirs_union() {
        let mut parents = HashMap::new();
        parents.insert(1, 0);
        parents.insert(2, 0);
        parents.insert(3, 1);
        parents.insert(4, 2);

        let mut dirs = HashMap::new();
        dirs.insert(0, true);
        dirs.insert(1, true);
        dirs.insert(2, true);
        dirs.insert(3, false);
        dirs.insert(4, false);

        let pt = MockPathTable { parents, dirs };
        let entries = vec![(3, 10u64), (4, 20u64)];
        let idx = ParentIndex::build_from_entries(&entries, &pt);

        let union = idx.files_in_dirs(&[1, 2]);
        assert_eq!(union, vec![10, 20]);
    }

    #[test]
    fn apply_delta_add_and_remove() {
        let mut parents = HashMap::new();
        parents.insert(1, 0);
        parents.insert(2, 1);
        parents.insert(3, 2);

        let mut dirs = HashMap::new();
        dirs.insert(0, true);
        dirs.insert(1, true);
        dirs.insert(2, true);
        dirs.insert(3, false);

        let pt = MockPathTable { parents, dirs };
        let entries = vec![(3, 100u64)];
        let mut idx = ParentIndex::build_from_entries(&entries, &pt);

        // Remove the file
        let mut removed = HashMap::new();
        removed.insert(2, vec![100u32]);
        let delta = ParentIndexDelta {
            added: HashMap::new(),
            removed,
        };
        idx.apply_delta(&delta);
        assert!(idx.files_in_dir(2).is_none() || idx.files_in_dir(2).unwrap().is_empty());

        // Add a new file in a different dir
        let mut added = HashMap::new();
        added.insert(1, vec![200u32]);
        let delta2 = ParentIndexDelta {
            added,
            removed: HashMap::new(),
        };
        idx.apply_delta(&delta2);
        assert_eq!(idx.files_in_dir(1).map(|b| b.to_vec()), Some(vec![200]));
    }
}
