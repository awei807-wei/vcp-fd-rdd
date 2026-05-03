//! P1 — Startup repair tests.

use std::path::PathBuf;

use fd_rdd::core::{FileKey, FileMeta};
use fd_rdd::index::l2_partition::PersistentIndex;
use fd_rdd::index::TieredIndex;
use fd_rdd::storage::snapshot::{
    write_recovery_runtime_state, write_stable_v7_atomic, RecoveryRuntimeState, SnapshotStore,
};

fn unique_tmp_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("fd-rdd-startup-repair-{}-{}", tag, nanos))
}

fn one_physical_file_base(
    root: &std::path::Path,
    name: &str,
) -> fd_rdd::index::base_index::BaseIndexData {
    let path = root.join(name);
    std::fs::write(&path, b"indexed").unwrap();
    let meta = std::fs::metadata(&path).unwrap();
    let file_key = FileKey::from_path_and_metadata(&path, &meta).unwrap();

    let idx = PersistentIndex::new_with_roots(vec![root.to_path_buf()]);
    idx.upsert(FileMeta {
        file_key,
        path,
        size: meta.len(),
        mtime: meta.modified().ok(),
        ctime: meta.created().ok(),
        atime: meta.accessed().ok(),
    });
    idx.to_base_index_data()
}

#[test]
fn startup_repair_always_scans_and_records_stats() {
    let root = unique_tmp_dir("always");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("repair-me.txt"), b"repair").unwrap();

    let index = TieredIndex::empty(vec![root.clone()]);
    let stats = index.startup_repair_if_needed(true, "always", 4, 10_000, 1.0);

    assert!(stats.ran);
    assert_eq!(stats.scanned, 1);
    assert_eq!(stats.changed, 1);
    assert_eq!(index.recovery_status().repair.scanned, 1);

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn startup_repair_dirty_only_skips_after_clean_shutdown() {
    let root = unique_tmp_dir("dirty-clean");
    std::fs::create_dir_all(&root).unwrap();
    let snap_path = root.join("index.db");
    let store = SnapshotStore::new(snap_path.clone());

    let base = one_physical_file_base(&root, "indexed.txt");
    write_stable_v7_atomic(&snap_path, &base).unwrap();
    write_recovery_runtime_state(
        &snap_path,
        &RecoveryRuntimeState {
            last_clean_shutdown: true,
            last_snapshot_unix_secs: 1,
            last_wal_seal_id: 0,
            last_startup_source: "stable".to_string(),
            last_recovery_mode: "clean-shutdown".to_string(),
        },
    )
    .unwrap();

    let index = TieredIndex::load_or_empty(&store, vec![root.clone()])
        .await
        .unwrap();
    let stats = index.startup_repair_if_needed(true, "dirty-only", 4, 10_000, 1.0);

    assert!(!index.recovery_status().report.requires_repair);
    assert!(!stats.ran);
    assert_eq!(stats.scanned, 0);

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn startup_repair_dirty_only_runs_after_unclean_shutdown() {
    let root = unique_tmp_dir("dirty-unclean");
    std::fs::create_dir_all(&root).unwrap();
    let snap_path = root.join("index.db");
    let store = SnapshotStore::new(snap_path.clone());

    let base = one_physical_file_base(&root, "indexed.txt");
    write_stable_v7_atomic(&snap_path, &base).unwrap();
    write_recovery_runtime_state(
        &snap_path,
        &RecoveryRuntimeState {
            last_clean_shutdown: false,
            last_snapshot_unix_secs: 1,
            last_wal_seal_id: 0,
            last_startup_source: "stable".to_string(),
            last_recovery_mode: "running".to_string(),
        },
    )
    .unwrap();
    std::fs::write(root.join("offline-new.txt"), b"offline").unwrap();

    let index = TieredIndex::load_or_empty(&store, vec![root.clone()])
        .await
        .unwrap();
    let stats = index.startup_repair_if_needed(true, "dirty-only", 4, 10_000, 1.0);

    assert!(index.recovery_status().report.requires_repair);
    assert!(stats.ran);
    assert_eq!(stats.scanned, 2);
    assert_eq!(stats.changed, 1);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn startup_repair_never_skips_even_when_enabled() {
    let root = unique_tmp_dir("never");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("skip-me.txt"), b"skip").unwrap();

    let index = TieredIndex::empty(vec![root.clone()]);
    let stats = index.startup_repair_if_needed(true, "never", 4, 10_000, 1.0);

    assert!(!stats.ran);
    assert_eq!(stats.scanned, 0);

    let _ = std::fs::remove_dir_all(&root);
}
