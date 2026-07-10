//! Compact `FileKey -> DocId` hash index.
//!
//! The table stores only a `u32` DocId in each bucket. The corresponding
//! [`FileKey`] is read from the caller-owned [`FileEntry`] slice while probing,
//! avoiding a second copy of every 20-byte key.

use std::mem::size_of;

use crate::core::FileKey;
use crate::index::file_entry_v2::FileEntry;

const EMPTY: u32 = u32::MAX;
const DELETED: u32 = u32::MAX - 1;
const MIN_CAPACITY: usize = 8;
const MAX_LOAD_PERCENT: usize = 70;

/// Largest DocId that can be stored without colliding with bucket sentinels.
pub const MAX_DOC_ID: u32 = u32::MAX - 2;

/// An open-addressed `FileKey -> DocId` index with four-byte buckets.
///
/// `entries` passed to the methods must be the same DocId-ordered entry table
/// used when the index was populated. Existing DocIds must remain valid for the
/// lifetime of the index; replacing an entry in place is safe only when its
/// `FileKey` does not change, or after updating this index accordingly.
#[derive(Clone, Debug)]
pub struct CompactFileKeyIndex {
    buckets: Vec<u32>,
    len: usize,
    deleted: usize,
}

impl CompactFileKeyIndex {
    pub fn new() -> Self {
        Self {
            buckets: Vec::new(),
            len: 0,
            deleted: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Number of available hash-table slots, including empty and deleted ones.
    pub fn capacity(&self) -> usize {
        self.buckets.len()
    }

    /// Total resident allocation attributed to this value.
    pub fn allocated_bytes(&self) -> usize {
        debug_assert!(self.len() <= self.capacity());
        size_of::<Self>() + self.buckets.capacity() * size_of::<u32>()
    }

    /// Removes all mappings while retaining the bucket allocation for reuse.
    pub fn clear(&mut self) {
        self.buckets.fill(EMPTY);
        self.len = 0;
        self.deleted = 0;
    }

    pub fn get(&self, key: FileKey, entries: &[FileEntry]) -> Option<u32> {
        self.find_bucket(key, entries)
            .map(|bucket| self.buckets[bucket])
    }

    /// Inserts `key -> docid` only if no representative exists.
    ///
    /// Returns the existing DocId when the key was already present, or `None`
    /// after inserting the new mapping.
    pub fn insert_if_absent(
        &mut self,
        key: FileKey,
        docid: u32,
        entries: &[FileEntry],
    ) -> Option<u32> {
        self.assert_entry_matches(key, docid, entries);
        if let Some(bucket) = self.find_bucket(key, entries) {
            return Some(self.buckets[bucket]);
        }

        self.prepare_for_insert(entries);
        self.insert_new(key, docid, entries);
        None
    }

    /// Inserts a new mapping or changes the representative DocId for `key`.
    ///
    /// Returns the replaced DocId, if any.
    pub fn insert_or_replace(
        &mut self,
        key: FileKey,
        docid: u32,
        entries: &[FileEntry],
    ) -> Option<u32> {
        self.assert_entry_matches(key, docid, entries);
        if let Some(bucket) = self.find_bucket(key, entries) {
            return Some(std::mem::replace(&mut self.buckets[bucket], docid));
        }

        self.prepare_for_insert(entries);
        self.insert_new(key, docid, entries);
        None
    }

    /// Removes `key` and returns its former representative DocId.
    pub fn remove(&mut self, key: FileKey, entries: &[FileEntry]) -> Option<u32> {
        let bucket = self.find_bucket(key, entries)?;
        let old = std::mem::replace(&mut self.buckets[bucket], DELETED);
        self.len -= 1;
        self.deleted += 1;

        if self.len == 0 {
            self.clear();
        }
        Some(old)
    }

    /// Iterates mappings in bucket order without allocating an export buffer.
    pub fn iter<'a>(
        &'a self,
        entries: &'a [FileEntry],
    ) -> impl Iterator<Item = (FileKey, u32)> + 'a {
        self.buckets
            .iter()
            .copied()
            .filter(|&docid| is_occupied(docid))
            .map(move |docid| {
                let entry = entry_for_docid(entries, docid);
                (entry.file_key(), docid)
            })
    }

    fn prepare_for_insert(&mut self, entries: &[FileEntry]) {
        if self.buckets.is_empty() {
            self.rehash(MIN_CAPACITY, entries);
            return;
        }

        let capacity = self.capacity();
        if self.deleted > self.len && self.deleted > capacity / 4 {
            self.rehash(capacity, entries);
        }

        let used_after_insert = self.len.saturating_add(self.deleted).saturating_add(1);
        if used_after_insert.saturating_mul(100) >= self.capacity().saturating_mul(MAX_LOAD_PERCENT)
        {
            self.rehash(
                self.capacity()
                    .checked_mul(2)
                    .expect("CompactFileKeyIndex capacity overflow"),
                entries,
            );
        }
    }

    fn insert_new(&mut self, key: FileKey, docid: u32, entries: &[FileEntry]) {
        match self.find_slot(key, entries) {
            ProbeResult::Found(bucket) => {
                unreachable!(
                    "caller checked that CompactFileKeyIndex key was absent, found bucket {bucket}"
                )
            }
            ProbeResult::Vacant(bucket) => {
                if self.buckets[bucket] == DELETED {
                    self.deleted -= 1;
                }
                self.buckets[bucket] = docid;
                self.len += 1;
            }
        }
    }

    fn find_bucket(&self, key: FileKey, entries: &[FileEntry]) -> Option<usize> {
        if self.buckets.is_empty() {
            return None;
        }

        let mask = self.buckets.len() - 1;
        let mut bucket = bucket_for(key, mask);
        for _ in 0..self.buckets.len() {
            let docid = self.buckets[bucket];
            if docid == EMPTY {
                return None;
            }
            if docid != DELETED && entry_for_docid(entries, docid).file_key() == key {
                return Some(bucket);
            }
            bucket = (bucket + 1) & mask;
        }
        None
    }

    fn find_slot(&self, key: FileKey, entries: &[FileEntry]) -> ProbeResult {
        debug_assert!(!self.buckets.is_empty());
        let mask = self.buckets.len() - 1;
        let mut bucket = bucket_for(key, mask);
        let mut first_deleted = None;

        for _ in 0..self.buckets.len() {
            let docid = self.buckets[bucket];
            if docid == EMPTY {
                return ProbeResult::Vacant(first_deleted.unwrap_or(bucket));
            }
            if docid == DELETED {
                first_deleted.get_or_insert(bucket);
            } else if entry_for_docid(entries, docid).file_key() == key {
                return ProbeResult::Found(bucket);
            }
            bucket = (bucket + 1) & mask;
        }

        ProbeResult::Vacant(
            first_deleted.expect("CompactFileKeyIndex has no vacant bucket below load limit"),
        )
    }

    fn rehash(&mut self, capacity: usize, entries: &[FileEntry]) {
        let capacity = capacity.max(MIN_CAPACITY).next_power_of_two();
        let old_buckets = std::mem::replace(&mut self.buckets, vec![EMPTY; capacity]);
        self.len = 0;
        self.deleted = 0;

        for docid in old_buckets.into_iter().filter(|&docid| is_occupied(docid)) {
            let key = entry_for_docid(entries, docid).file_key();
            self.insert_new(key, docid, entries);
        }
    }

    fn assert_entry_matches(&self, key: FileKey, docid: u32, entries: &[FileEntry]) {
        assert!(
            docid <= MAX_DOC_ID,
            "DocId {docid} collides with CompactFileKeyIndex sentinels"
        );
        assert_eq!(
            entry_for_docid(entries, docid).file_key(),
            key,
            "CompactFileKeyIndex key must match entries[docid]"
        );
    }
}

impl Default for CompactFileKeyIndex {
    fn default() -> Self {
        Self::new()
    }
}

enum ProbeResult {
    Found(usize),
    Vacant(usize),
}

fn is_occupied(docid: u32) -> bool {
    docid <= MAX_DOC_ID
}

fn entry_for_docid(entries: &[FileEntry], docid: u32) -> &FileEntry {
    entries.get(docid as usize).unwrap_or_else(|| {
        panic!(
            "CompactFileKeyIndex DocId {docid} is outside entries length {}",
            entries.len()
        )
    })
}

fn bucket_for(key: FileKey, mask: usize) -> usize {
    hash_file_key(key) as usize & mask
}

fn hash_file_key(key: FileKey) -> u64 {
    let mut hash = mix64(key.dev);
    hash ^= mix64(key.ino.rotate_left(23));
    hash ^= mix64((key.generation as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15));
    mix64(hash)
}

fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
#[path = "filekey_index_tests.rs"]
mod tests;
