use super::*;

fn key(id: u64) -> FileKey {
    FileKey {
        dev: id % 7,
        ino: id.wrapping_mul(1_000_003),
        generation: (id >> 3) as u32,
    }
}

fn entry(key: FileKey, path_idx: u32) -> FileEntry {
    FileEntry::from_file_key(key, path_idx, 0)
}

#[test]
fn inserts_and_queries_entries() {
    let entries = vec![entry(key(1), 0), entry(key(2), 1), entry(key(3), 2)];
    let mut index = CompactFileKeyIndex::new();

    assert_eq!(index.insert_if_absent(key(1), 0, &entries), None);
    assert_eq!(index.insert_if_absent(key(2), 1, &entries), None);
    assert_eq!(index.insert_if_absent(key(3), 2, &entries), None);
    assert_eq!(index.len(), 3);
    assert_eq!(index.get(key(1), &entries), Some(0));
    assert_eq!(index.get(key(2), &entries), Some(1));
    assert_eq!(index.get(key(3), &entries), Some(2));
    assert_eq!(index.get(key(99), &entries), None);

    let mut pairs: Vec<_> = index.iter(&entries).collect();
    pairs.sort_unstable_by_key(|(key, _)| *key);
    assert_eq!(pairs.len(), 3);
    assert_eq!(pairs[0], (key(1), 0));
}

#[test]
fn keeps_or_replaces_same_filekey_representative() {
    let shared = key(10);
    let entries = vec![entry(shared, 0), entry(shared, 1)];
    let mut index = CompactFileKeyIndex::new();

    assert_eq!(index.insert_if_absent(shared, 0, &entries), None);
    assert_eq!(index.insert_if_absent(shared, 1, &entries), Some(0));
    assert_eq!(index.len(), 1);
    assert_eq!(index.get(shared, &entries), Some(0));

    assert_eq!(index.insert_or_replace(shared, 1, &entries), Some(0));
    assert_eq!(index.len(), 1);
    assert_eq!(index.get(shared, &entries), Some(1));
}

#[test]
fn deletion_preserves_collision_probe_chain() {
    let mask = MIN_CAPACITY - 1;
    let first = key(1);
    let first_bucket = bucket_for(first, mask);
    let second = (2..10_000)
        .map(key)
        .find(|candidate| bucket_for(*candidate, mask) == first_bucket)
        .expect("find colliding key");
    let entries = vec![entry(first, 0), entry(second, 1)];
    let mut index = CompactFileKeyIndex::new();

    index.insert_if_absent(first, 0, &entries);
    index.insert_if_absent(second, 1, &entries);
    assert_eq!(index.remove(first, &entries), Some(0));
    assert_eq!(index.get(first, &entries), None);
    assert_eq!(index.get(second, &entries), Some(1));
    assert_eq!(index.len(), 1);
}

#[test]
fn grows_and_preserves_many_keys() {
    let entries: Vec<_> = (0..2_000)
        .map(|docid| entry(key(docid as u64 + 1), docid))
        .collect();
    let mut index = CompactFileKeyIndex::new();

    for docid in 0..entries.len() as u32 {
        assert_eq!(
            index.insert_if_absent(entries[docid as usize].file_key(), docid, &entries),
            None
        );
    }

    assert_eq!(index.len(), entries.len());
    assert!(index.capacity() > MIN_CAPACITY);
    for docid in 0..entries.len() as u32 {
        assert_eq!(
            index.get(entries[docid as usize].file_key(), &entries),
            Some(docid)
        );
    }
}

#[test]
fn reuses_deleted_slots_and_clear_retains_capacity() {
    let entries: Vec<_> = (0..64)
        .map(|docid| entry(key(docid as u64 + 1), docid))
        .collect();
    let mut index = CompactFileKeyIndex::new();
    for docid in 0..32 {
        index.insert_if_absent(entries[docid].file_key(), docid as u32, &entries);
    }
    let grown_capacity = index.capacity();

    for docid in 0..24 {
        assert_eq!(
            index.remove(entries[docid].file_key(), &entries),
            Some(docid as u32)
        );
    }
    for docid in 32..48 {
        index.insert_if_absent(entries[docid].file_key(), docid as u32, &entries);
    }
    assert!(index.capacity() <= grown_capacity * 2);

    index.clear();
    assert!(index.is_empty());
    assert_eq!(index.capacity(), index.buckets.capacity());
    assert!(index.capacity() >= grown_capacity);
    assert_eq!(index.iter(&entries).count(), 0);
}

#[test]
#[should_panic(expected = "collides with CompactFileKeyIndex sentinels")]
fn rejects_sentinel_docids() {
    let entries = vec![entry(key(1), 0)];
    let mut index = CompactFileKeyIndex::new();
    index.insert_if_absent(key(1), DELETED, &entries);
}
