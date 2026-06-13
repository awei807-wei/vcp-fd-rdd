//! P1 — path-entry hardlink alias behavior.

#[allow(dead_code)]
mod common;

use std::path::{Path, PathBuf};

use common::unique_tmp_dir;
use fd_rdd::core::{BuildRDD, EventRecord, EventType, FileIdentifier, FsScanRDD};
use fd_rdd::index::l2_partition::PersistentIndex;
use fd_rdd::query::matcher::create_matcher;
use fd_rdd::storage::snapshot::{stable_v7_path_for, write_stable_v7_atomic};
use fd_rdd::storage::snapshot_v7::try_load_v7_cold;

fn build_index(root: &Path) -> PersistentIndex {
    let idx = PersistentIndex::new_with_roots(vec![root.to_path_buf()]);
    let rdd = FsScanRDD::from_roots(vec![root.to_path_buf()]).with_hidden(true);
    rdd.for_each(|meta| idx.upsert(meta));
    idx
}

fn group_paths(
    idx: &PersistentIndex,
    min_links: usize,
    prefix: Option<&Path>,
) -> Vec<Vec<PathBuf>> {
    idx.hardlink_groups(min_links, prefix)
        .into_iter()
        .map(|group| group.paths)
        .collect()
}

fn sorted_paths(mut paths: Vec<PathBuf>) -> Vec<PathBuf> {
    paths.sort();
    paths
}

#[test]
fn full_build_indexes_same_inode_multiple_paths() {
    let root = unique_tmp_dir("full-build");
    std::fs::create_dir_all(root.join("a")).unwrap();
    std::fs::create_dir_all(root.join("b")).unwrap();
    let a = root.join("a/alias-a.txt");
    let b = root.join("b/alias-b.txt");
    std::fs::write(&a, b"same").unwrap();
    std::fs::hard_link(&a, &b).unwrap();

    let idx = build_index(&root);
    assert_eq!(idx.physical_dedupe_stats().live_path_count, 2);

    let qa = create_matcher("alias-a", false);
    let qb = create_matcher("alias-b", false);
    assert_eq!(idx.query(qa.as_ref(), 10).len(), 1);
    assert_eq!(idx.query(qb.as_ref(), 10).len(), 1);

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn hardlink_groups_filter_copy_min_links_and_prefix() {
    let root = unique_tmp_dir("groups");
    let same_dir = root.join("same");
    let other_dir = root.join("other");
    std::fs::create_dir_all(&same_dir).unwrap();
    std::fs::create_dir_all(&other_dir).unwrap();
    let original = same_dir.join("original.txt");
    let alias_a = same_dir.join("alias-a.txt");
    let alias_b = other_dir.join("alias-b.txt");
    let copy = same_dir.join("copy.txt");
    std::fs::write(&original, b"same").unwrap();
    std::fs::hard_link(&original, &alias_a).unwrap();
    std::fs::hard_link(&original, &alias_b).unwrap();
    std::fs::write(&copy, b"same").unwrap();

    let idx = build_index(&root);
    let groups = group_paths(&idx, 2, None);
    assert_eq!(groups.len(), 1);
    assert_eq!(
        groups[0],
        sorted_paths(vec![original.clone(), alias_a.clone(), alias_b.clone()])
    );
    assert!(!groups[0].contains(&copy));

    assert_eq!(group_paths(&idx, 3, None).len(), 1);
    assert!(group_paths(&idx, 4, None).is_empty());
    assert_eq!(group_paths(&idx, 2, Some(&same_dir)).len(), 1);
    assert!(group_paths(&idx, 2, Some(&other_dir)).is_empty());

    let stats = idx.physical_dedupe_stats();
    assert_eq!(stats.live_path_count, 4);
    assert_eq!(stats.physical_file_count, 2);
    assert_eq!(stats.hardlink_group_count, 1);
    assert_eq!(stats.hardlink_path_count, 3);
    assert_eq!(stats.duplicate_path_count, 2);
    assert_eq!(stats.max_group_size, 3);

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn delete_one_hardlink_path_keeps_other_alias_live() {
    let root = unique_tmp_dir("delete-one");
    std::fs::create_dir_all(&root).unwrap();
    let a = root.join("keep-a.txt");
    let b = root.join("drop-b.txt");
    std::fs::write(&a, b"same").unwrap();
    std::fs::hard_link(&a, &b).unwrap();

    let idx = build_index(&root);
    idx.mark_deleted_by_path(&b);

    let qa = create_matcher("keep-a", false);
    let qb = create_matcher("drop-b", false);
    assert_eq!(idx.query(qa.as_ref(), 10).len(), 1);
    assert!(idx.query(qb.as_ref(), 10).is_empty());
    assert!(group_paths(&idx, 2, None).is_empty());

    let stats = idx.physical_dedupe_stats();
    assert_eq!(stats.live_path_count, 1);
    assert_eq!(stats.physical_file_count, 1);
    assert_eq!(stats.hardlink_group_count, 0);
    assert_eq!(stats.max_group_size, 0);

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn delete_one_of_three_hardlinks_keeps_remaining_group_visible() {
    let root = unique_tmp_dir("delete-one-of-three");
    std::fs::create_dir_all(&root).unwrap();
    let original = root.join("original.txt");
    let alias_a = root.join("alias-a.txt");
    let alias_b = root.join("alias-b.txt");
    std::fs::write(&original, b"same").unwrap();
    std::fs::hard_link(&original, &alias_a).unwrap();
    std::fs::hard_link(&original, &alias_b).unwrap();

    let idx = build_index(&root);
    idx.mark_deleted_by_path(&alias_a);

    let groups = group_paths(&idx, 2, None);
    assert_eq!(groups.len(), 1);
    assert_eq!(
        groups[0],
        sorted_paths(vec![original.clone(), alias_b.clone()])
    );

    let stats = idx.physical_dedupe_stats();
    assert_eq!(stats.live_path_count, 2);
    assert_eq!(stats.physical_file_count, 1);
    assert_eq!(stats.hardlink_group_count, 1);
    assert_eq!(stats.hardlink_path_count, 2);
    assert_eq!(stats.duplicate_path_count, 1);
    assert_eq!(stats.max_group_size, 2);

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn hardlink_rename_updates_only_renamed_path() {
    let root = unique_tmp_dir("rename-one");
    std::fs::create_dir_all(&root).unwrap();
    let a = root.join("stable-a.txt");
    let b = root.join("old-b.txt");
    let c = root.join("new-c.txt");
    std::fs::write(&a, b"same").unwrap();
    std::fs::hard_link(&a, &b).unwrap();

    let idx = build_index(&root);
    std::fs::rename(&b, &c).unwrap();
    idx.apply_events(&[EventRecord {
        seq: 1,
        timestamp: std::time::SystemTime::now(),
        event_type: EventType::Rename {
            from: FileIdentifier::Path(b.clone()),
            from_path_hint: Some(b.clone()),
        },
        id: FileIdentifier::Path(c.clone()),
        path_hint: Some(c.clone()),
    }]);

    assert_eq!(
        idx.query(create_matcher("stable-a", false).as_ref(), 10)
            .len(),
        1
    );
    assert_eq!(
        idx.query(create_matcher("new-c", false).as_ref(), 10).len(),
        1
    );
    assert!(idx
        .query(create_matcher("old-b", false).as_ref(), 10)
        .is_empty());
    let groups = group_paths(&idx, 2, None);
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0], sorted_paths(vec![a.clone(), c.clone()]));

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn hardlink_aliases_survive_v7_snapshot_reload() {
    let root = unique_tmp_dir("snapshot");
    std::fs::create_dir_all(&root).unwrap();
    let a = root.join("snap-a.txt");
    let b = root.join("snap-b.txt");
    std::fs::write(&a, b"same").unwrap();
    std::fs::hard_link(&a, &b).unwrap();

    let idx = build_index(&root);
    let base = idx.to_base_index_data();
    let snap_path = root.join("index.db");
    write_stable_v7_atomic(&snap_path, &base).unwrap();

    let loaded = try_load_v7_cold(&stable_v7_path_for(&snap_path), std::slice::from_ref(&root))
        .unwrap()
        .unwrap();
    assert_eq!(loaded.file_count(), 2);
    assert_eq!(
        loaded
            .query_metas(create_matcher("snap-a", false).as_ref())
            .len(),
        1
    );
    assert_eq!(
        loaded
            .query_metas(create_matcher("snap-b", false).as_ref())
            .len(),
        1
    );
    let rehydrated = PersistentIndex::new_with_roots(vec![root.clone()]);
    loaded.for_each_live_meta(|meta| rehydrated.upsert_path_alias(meta));
    let groups = group_paths(&rehydrated, 2, None);
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0], sorted_paths(vec![a.clone(), b.clone()]));

    let _ = std::fs::remove_dir_all(root);
}
