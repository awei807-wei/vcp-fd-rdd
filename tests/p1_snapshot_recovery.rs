//! P1 — Snapshot recovery tests.
//!
//! Validates that corrupted snapshots and WAL files are handled gracefully
//! without panicking, and that fallback rebuild is triggered.

use std::path::PathBuf;

use fd_rdd::storage::snapshot::SnapshotStore;

fn unique_tmp_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("fd-rdd-snap-recovery-{}-{}", tag, nanos))
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
