//! P1 — Snapshot recovery tests.
//!
//! Validates that corrupted snapshots and WAL files are handled gracefully
//! without panicking, and that fallback rebuild is triggered.

use std::path::PathBuf;

use fd_rdd::core::{FileKey, FileMeta};
use fd_rdd::index::l2_partition::PersistentIndex;
use fd_rdd::index::TieredIndex;
use fd_rdd::storage::snapshot::SnapshotStore;
use fd_rdd::storage::snapshot::{
    read_recovery_runtime_state, runtime_state_path_for, stable_prev_v7_path_for,
    stable_v7_path_for, write_recovery_runtime_state, write_stable_v7_atomic, RecoveryRuntimeState,
};
use fd_rdd::storage::snapshot_v7::{try_load_v7, write_v7_snapshot_atomic};

fn unique_tmp_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("fd-rdd-snap-recovery-{}-{}", tag, nanos))
}

fn one_file_base(root: &std::path::Path, name: &str) -> fd_rdd::index::base_index::BaseIndexData {
    let idx = PersistentIndex::new_with_roots(vec![root.to_path_buf()]);
    idx.upsert(FileMeta {
        file_key: FileKey {
            dev: 1,
            ino: 100,
            generation: 0,
        },
        path: root.join(name),
        size: 7,
        mtime: None,
        ctime: None,
        atime: None,
    });
    idx.to_base_index_data()
}

/// 17. 坏文件处理：快照损坏时正确识别、跳过、不 panic
#[tokio::test]
async fn corrupted_snapshot_returns_none_not_panic() {
    let root = unique_tmp_dir("corrupt-snap");
    std::fs::create_dir_all(&root).unwrap();

    let snap_path = root.join("index.db");

    // Write garbage data as a snapshot file
    std::fs::write(&snap_path, b"this is not a valid snapshot file at all").unwrap();

    let store = SnapshotStore::new(snap_path);

    // load_if_valid should return Ok(None), not panic
    let result = store.load_if_valid().await;
    match result {
        Ok(None) => {} // Expected: invalid snapshot detected gracefully
        Ok(Some(_)) => panic!("Should not have loaded a corrupted snapshot"),
        Err(_) => {} // Also acceptable: error returned instead of panic
    }

    let _ = std::fs::remove_dir_all(&root);
}

/// 17b. 空快照文件不 panic
#[tokio::test]
async fn empty_snapshot_file_returns_none() {
    let root = unique_tmp_dir("empty-snap");
    std::fs::create_dir_all(&root).unwrap();

    let snap_path = root.join("index.db");
    std::fs::write(&snap_path, b"").unwrap();

    let store = SnapshotStore::new(snap_path);
    let result = store.load_if_valid().await;
    match result {
        Ok(None) => {}
        Ok(Some(_)) => panic!("Should not have loaded an empty snapshot"),
        Err(_) => {}
    }

    let _ = std::fs::remove_dir_all(&root);
}

/// 18. 触发兜底重建：不存在的快照路径
#[tokio::test]
async fn nonexistent_snapshot_returns_none() {
    let root = unique_tmp_dir("no-snap");
    // Don't create the directory

    let snap_path = root.join("index.db");
    let store = SnapshotStore::new(snap_path);

    let result = store.load_if_valid().await;
    match result {
        Ok(None) => {} // Expected
        Ok(Some(_)) => panic!("Should not have loaded from nonexistent path"),
        Err(_) => {} // Also acceptable
    }
}

#[test]
fn stable_snapshot_rotates_current_to_prev() {
    let root = unique_tmp_dir("stable-rotate");
    std::fs::create_dir_all(&root).unwrap();
    let snap_path = root.join("index.db");

    let first = one_file_base(&root, "first.txt");
    write_stable_v7_atomic(&snap_path, &first).unwrap();
    assert!(stable_v7_path_for(&snap_path).exists());
    assert!(!stable_prev_v7_path_for(&snap_path).exists());

    let second = one_file_base(&root, "second.txt");
    write_stable_v7_atomic(&snap_path, &second).unwrap();
    assert!(stable_v7_path_for(&snap_path).exists());
    assert!(stable_prev_v7_path_for(&snap_path).exists());

    let loaded_prev = try_load_v7(&stable_prev_v7_path_for(&snap_path))
        .unwrap()
        .unwrap();
    assert_eq!(loaded_prev.file_count(), 1);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn missing_runtime_state_defaults_to_untrusted() {
    let root = unique_tmp_dir("runtime-missing");
    std::fs::create_dir_all(&root).unwrap();
    let snap_path = root.join("index.db");

    let state = read_recovery_runtime_state(&snap_path).unwrap();
    assert!(!state.last_clean_shutdown);
    assert_eq!(state.last_startup_source, "unknown");

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn runtime_state_roundtrips_atomically() {
    let root = unique_tmp_dir("runtime-roundtrip");
    std::fs::create_dir_all(&root).unwrap();
    let snap_path = root.join("index.db");

    let expected = RecoveryRuntimeState {
        last_clean_shutdown: true,
        last_snapshot_unix_secs: 42,
        last_wal_seal_id: 7,
        last_startup_source: "stable".to_string(),
        last_recovery_mode: "clean-shutdown".to_string(),
    };
    write_recovery_runtime_state(&snap_path, &expected).unwrap();

    let loaded = read_recovery_runtime_state(&snap_path).unwrap();
    assert!(loaded.last_clean_shutdown);
    assert_eq!(loaded.last_snapshot_unix_secs, 42);
    assert_eq!(loaded.last_wal_seal_id, 7);
    assert_eq!(loaded.last_startup_source, "stable");
    assert_eq!(loaded.last_recovery_mode, "clean-shutdown");

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn stable_prev_used_when_stable_is_corrupted() {
    let root = unique_tmp_dir("stable-prev");
    std::fs::create_dir_all(&root).unwrap();
    let snap_path = root.join("index.db");
    let store = SnapshotStore::new(snap_path.clone());

    let prev = one_file_base(&root, "prev.txt");
    std::fs::create_dir_all(snap_path.with_extension("d")).unwrap();
    write_v7_snapshot_atomic(&stable_prev_v7_path_for(&snap_path), &prev).unwrap();
    std::fs::write(
        stable_v7_path_for(&snap_path),
        b"not a valid stable snapshot",
    )
    .unwrap();

    let loaded = TieredIndex::load_or_empty(&store, vec![root.clone()])
        .await
        .unwrap();
    let recovery = loaded.recovery_status();
    assert_eq!(recovery.report.snapshot_source, "stable-prev");
    assert_eq!(loaded.file_count(), 1);

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn corrupted_runtime_state_marks_recovery_untrusted() {
    let root = unique_tmp_dir("runtime-corrupt");
    std::fs::create_dir_all(&root).unwrap();
    let snap_path = root.join("index.db");
    let store = SnapshotStore::new(snap_path.clone());

    let base = one_file_base(&root, "indexed.txt");
    write_stable_v7_atomic(&snap_path, &base).unwrap();
    std::fs::write(runtime_state_path_for(&snap_path), b"not valid json").unwrap();

    let loaded = TieredIndex::load_or_empty(&store, vec![root.clone()])
        .await
        .unwrap();
    let recovery = loaded.recovery_status();
    assert_eq!(recovery.report.snapshot_source, "stable");
    assert!(recovery.report.requires_repair);
    assert!(!recovery.report.previous_clean_shutdown);

    let _ = std::fs::remove_dir_all(&root);
}
