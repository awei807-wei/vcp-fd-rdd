//! FileEntry v2: fixed-size 40-byte struct + file-key lookup index.

use crate::core::FileKey;

/// Fixed-size file metadata entry (40 bytes).
///
/// Layout:
/// - dev:      8 bytes
/// - ino:      8 bytes
/// - generation: 4 bytes
/// - path_idx: 4 bytes
/// - size:     8 bytes
/// - mtime_ns: 8 bytes
/// Total: 40 bytes
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileEntry {
    pub dev: u64,
    pub ino: u64,
    pub generation: u32,
    pub path_idx: u32,
    pub size: u64,
    pub mtime_ns: i64,
}

// Assert size at compile time.
#[cfg(target_pointer_width = "64")]
const _: [(); 1] = [(); (std::mem::size_of::<FileEntry>() == 40) as usize];

impl FileEntry {
    pub fn file_key(&self) -> FileKey {
        FileKey {
            dev: self.dev,
            ino: self.ino,
            generation: self.generation,
        }
    }

    pub fn from_file_key(file_key: FileKey, path_idx: u32, size: u64, mtime_ns: i64) -> Self {
        Self {
            dev: file_key.dev,
            ino: file_key.ino,
            generation: file_key.generation,
            path_idx,
            size,
            mtime_ns,
        }
    }
}

/// Index over `FileEntry` providing DocId-order iteration and O(log N) lookup by file key.
#[derive(Clone, Debug)]
pub struct FileEntryIndex {
    /// Entries in insertion order ( DocId order ).
    entries: Vec<FileEntry>,
    /// Permutation sorted by `(dev, ino, generation)`.
    by_filekey: Vec<u32>,
}

impl FileEntryIndex {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            by_filekey: Vec::new(),
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            entries: Vec::with_capacity(cap),
            by_filekey: Vec::with_capacity(cap),
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn allocated_bytes(&self) -> usize {
        use std::mem::size_of;

        size_of::<Self>()
            + self.entries.capacity() * size_of::<FileEntry>()
            + self.by_filekey.capacity() * size_of::<u32>()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn push(&mut self, entry: FileEntry) {
        let idx = self.entries.len() as u32;
        self.entries.push(entry);
        self.by_filekey.push(idx);
    }

    /// Finalize the index: sort the file-key permutation vector.
    pub fn build(mut self) -> Self {
        self.sort_by_key();
        self
    }

    /// Sort the `by_filekey` permutation in place.
    pub fn sort_by_key(&mut self) {
        self.by_filekey.sort_by_key(|&i| {
            let e = &self.entries[i as usize];
            (e.dev, e.ino, e.generation)
        });
    }

    /// Lookup by `FileKey` using binary search. Returns a slice of matching entries
    /// (there may be multiple entries with the same key in edge cases).
    pub fn lookup_by_filekey(&self, key: FileKey) -> Option<&[FileEntry]> {
        let pos = self
            .by_filekey
            .binary_search_by(|&i| {
                let e = &self.entries[i as usize];
                (e.dev, e.ino, e.generation).cmp(&(key.dev, key.ino, key.generation))
            })
            .ok()?;
        // Expand to all equal entries.
        let mut start = pos;
        while start > 0 {
            let e = &self.entries[self.by_filekey[start - 1] as usize];
            if (e.dev, e.ino, e.generation) == (key.dev, key.ino, key.generation) {
                start -= 1;
            } else {
                break;
            }
        }
        let mut end = pos + 1;
        while end < self.by_filekey.len() {
            let e = &self.entries[self.by_filekey[end] as usize];
            if (e.dev, e.ino, e.generation) == (key.dev, key.ino, key.generation) {
                end += 1;
            } else {
                break;
            }
        }
        Some(&self.entries[self.by_filekey[start] as usize..=self.by_filekey[end - 1] as usize])
    }

    /// Lookup by `FileKey` and return the docid of the first matching entry.
    pub fn lookup_docid_by_filekey(&self, key: FileKey) -> Option<u32> {
        let pos = self
            .by_filekey
            .binary_search_by(|&i| {
                let e = &self.entries[i as usize];
                (e.dev, e.ino, e.generation).cmp(&(key.dev, key.ino, key.generation))
            })
            .ok()?;
        Some(self.by_filekey[pos])
    }

    /// Iterate over all entries in DocId order.
    pub fn iter(&self) -> impl Iterator<Item = &FileEntry> {
        self.entries.iter()
    }

    /// Get entry by its DocId (index into `entries`).
    pub fn get(&self, idx: usize) -> Option<&FileEntry> {
        self.entries.get(idx)
    }

    /// Lookup by `FileKey` and return the first match with its DocId.
    pub fn get_first_by_filekey(&self, key: FileKey) -> Option<(usize, &FileEntry)> {
        let pos = self
            .by_filekey
            .binary_search_by(|&i| {
                let e = &self.entries[i as usize];
                (e.dev, e.ino, e.generation).cmp(&(key.dev, key.ino, key.generation))
            })
            .ok()?;
        let doc_id = self.by_filekey[pos] as usize;
        Some((doc_id, &self.entries[doc_id]))
    }
}

impl Default for FileEntryIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn make_key(dev: u64, ino: u64) -> FileKey {
        FileKey {
            dev,
            ino,
            generation: 0,
        }
    }

    #[test]
    fn test_file_entry_size() {
        assert_eq!(std::mem::size_of::<FileEntry>(), 40);
    }

    #[test]
    fn test_lookup_by_filekey() {
        let mut index = FileEntryIndex::new();
        index.push(FileEntry::from_file_key(
            make_key(1, 100),
            0,
            1024,
            1_000_000,
        ));
        index.push(FileEntry::from_file_key(
            make_key(1, 200),
            1,
            2048,
            2_000_000,
        ));
        index.push(FileEntry::from_file_key(
            make_key(2, 100),
            2,
            512,
            3_000_000,
        ));
        let index = index.build();

        let r = index.lookup_by_filekey(make_key(1, 200)).unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].size, 2048);

        assert!(index.lookup_by_filekey(make_key(99, 99)).is_none());
    }

    #[test]
    fn file_entry_lookup_matches_compact_meta() {
        // Build entries in a non-sorted order to verify index sorting.
        let mut entries: Vec<FileEntry> = (0..10_000)
            .map(|i| {
                FileEntry::from_file_key(
                    make_key(i as u64 % 100, i as u64),
                    i,
                    i as u64 * 16,
                    i as i64 * 1_000_000,
                )
            })
            .collect();

        // Reverse before insertion to test sorting.
        entries.reverse();

        let mut index = FileEntryIndex::with_capacity(entries.len());
        let mut btree = BTreeMap::new();
        for e in &entries {
            index.push(*e);
            btree.insert(e.file_key(), *e);
        }
        let index = index.build();

        for e in &entries {
            let idx_result = index.lookup_by_filekey(e.file_key());
            let btree_result = btree.get(&e.file_key());
            assert!(
                idx_result.is_some(),
                "FileEntryIndex should find key {:?}",
                e.file_key()
            );
            assert_eq!(
                idx_result.unwrap()[0],
                *btree_result.unwrap(),
                "Mismatch for key {:?}",
                e.file_key()
            );
        }
    }
}
