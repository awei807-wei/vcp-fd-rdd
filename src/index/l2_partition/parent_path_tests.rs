use super::*;
use crate::index::parent_index::{ParentIndex, PathTable};

fn constant_hash(_: &[u8]) -> u64 {
    7
}

#[test]
fn duplicate_intern_reuses_id_and_upgrades_directory_marker() {
    let mut table = CompactPathTable::new();
    let first = table.intern(b"/repo/src", false);
    let duplicate = table.intern(b"/repo/src", true);

    assert_eq!(first, duplicate);
    assert_eq!(table.len(), 1);
    assert_eq!(table.path_bytes(first), Some(b"/repo/src".as_slice()));
    assert!(table.is_dir(first));
}

#[test]
fn exact_comparison_makes_hash_collisions_safe() {
    let mut table = CompactPathTable::with_hash_fn(constant_hash);
    let first = table.intern(b"/one", true);
    let second = table.intern(b"/two", true);
    let duplicate = table.intern(b"/one", false);

    assert_ne!(first, second);
    assert_eq!(duplicate, first);
    assert_eq!(table.len(), 2);
    assert_eq!(table.lookup(b"/one"), Some(first));
    assert_eq!(table.lookup(b"/two"), Some(second));
    assert_eq!(table.lookup(b"/missing"), None);

    let lookup = ParentPathLookup::from_table(&table);
    assert_eq!(lookup.lookup(b"/one"), Some(first));
    assert_eq!(lookup.lookup(b"/two"), Some(second));
    assert_eq!(lookup.lookup(b"/missing"), None);
}

#[test]
fn parent_idx_uses_interned_parent_and_handles_root() {
    let mut table = CompactPathTable::new();
    let root = table.intern(b"/", true);
    let repo = table.intern(b"/repo", true);
    let src = table.intern(b"/repo/src", true);
    let file = table.intern(b"/repo/src/lib.rs", false);

    assert_eq!(table.parent_idx(root), None);
    assert_eq!(table.parent_idx(repo), Some(root));
    assert_eq!(table.parent_idx(src), Some(repo));
    assert_eq!(table.parent_idx(file), Some(src));
}

#[test]
fn compressed_lookup_keeps_only_directories_and_preserves_ids() {
    let mut table = CompactPathTable::new();
    let root = table.intern(b"/", true);
    let dir = table.intern(b"/repo", true);
    let file = table.intern(b"/repo/file.txt", false);
    let other_file = table.intern(b"/repo/other.txt", false);
    let entries = [(file, 41_u64), (other_file, 42_u64)];

    let parent_index = ParentIndex::build_from_entries(&entries, &table);
    let lookup = ParentPathLookup::from_table(&table);
    assert!(table.allocated_bytes() > 0);
    assert!(lookup.allocated_bytes() > 0);
    drop(table);

    assert_eq!(lookup.len(), 2);
    assert_eq!(lookup.lookup(b"/"), Some(root));
    assert_eq!(lookup.lookup(b"/repo"), Some(dir));
    assert_eq!(lookup.lookup(b"/repo/file.txt"), None);
    assert_eq!(
        parent_index.files_in_dir(lookup.lookup(b"/repo").unwrap()),
        Some([41, 42].as_slice())
    );
}
