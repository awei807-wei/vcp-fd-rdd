//! P1 — LSM compaction tests.
//!
//! Validates that LSM compaction correctly merges segments and reclaims disk space,
//! and that old sealed WAL files and expired LSM segments are properly cleaned up.

#[allow(dead_code)]
mod common;

use std::sync::Arc;

use common::unique_tmp_dir;
use fd_rdd::core::{EventRecord, EventType, FileIdentifier};
use fd_rdd::index::TieredIndex;
use fd_rdd::storage::snapshot::{stable_snapshot_dir_for, stable_v7_path_for, SnapshotStore};

/// 19. LSM compaction: write snapshot, apply events, write another snapshot
///     Verifies the basic snapshot write/load cycle works.
#[tokio::test]
async fn lsm_snapshot_write_and_reload() {
    let root = unique_tmp_dir("lsm-basic");
    std::fs::create_dir_all(&root).unwrap();

    let snap_path = root.join("index.db");
    let store = Arc::new(SnapshotStore::new(snap_path));

    // Create index with some files
    let index = Arc::new(TieredIndex::empty(vec![root.clone()]));

    // Upsert via events
    for i in 0..100u64 {
        let path = root.join(format!("file_{:03}.txt", i));
        std::fs::write(&path, format!("content {}", i)).unwrap();
        let events = vec![EventRecord {
            seq: i + 1,
            timestamp: std::time::SystemTime::now(),
            event_type: EventType::Create,
            id: FileIdentifier::Fid { dev: 1, ino: i + 1 },
            path_hint: Some(path),
        }];
        index.apply_events(&events);
    }

    // Write snapshot
    let result = index.snapshot_now(store.clone()).await;
    assert!(result.is_ok(), "Snapshot write should succeed");

    // Reload
    let index2 = TieredIndex::load(store.as_ref(), vec![root.clone()]).await;
    assert!(index2.is_ok(), "Should load snapshot successfully");

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn failed_stable_snapshot_retains_sealed_wal() {
    let root = unique_tmp_dir("stable-failure-wal");
    let content_root = root.join("content");
    let state_root = root.join("state");
    std::fs::create_dir_all(&content_root).unwrap();
    std::fs::create_dir_all(&state_root).unwrap();

    let store = Arc::new(SnapshotStore::new(state_root.join("index.db")));
    let index = Arc::new(TieredIndex::empty(vec![content_root.clone()]));
    index.attach_wal(store.as_ref()).unwrap();

    let path = content_root.join("must-survive.txt");
    std::fs::write(&path, b"durable through WAL").unwrap();
    index.apply_events(&[EventRecord {
        seq: 1,
        timestamp: std::time::SystemTime::now(),
        event_type: EventType::Create,
        id: FileIdentifier::Path(path.clone()),
        path_hint: Some(path),
    }]);

    let stable_next = stable_snapshot_dir_for(store.path()).join("stable.next.v7");
    std::fs::create_dir_all(&stable_next).unwrap();

    let error = index
        .snapshot_now(store.clone())
        .await
        .expect_err("both stable install paths must fail on a directory target");
    assert!(error.to_string().contains("sealed WAL retained"));
    assert!(
        store.path().with_extension("v7").exists(),
        "the primary snapshot should still have completed"
    );

    let sealed_count = std::fs::read_dir(store.derived_lsm_dir_path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with("events.wal.seal-")
        })
        .count();
    assert_eq!(
        sealed_count, 1,
        "a failed stable snapshot must retain the sealed WAL for recovery"
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn disabling_stable_snapshot_retires_old_recovery_copy() {
    let root = unique_tmp_dir("disable-stable-copy");
    let content_root = root.join("content");
    let state_root = root.join("state");
    std::fs::create_dir_all(&content_root).unwrap();
    std::fs::create_dir_all(&state_root).unwrap();

    let store = Arc::new(SnapshotStore::new(state_root.join("index.db")));
    let index = Arc::new(TieredIndex::empty(vec![content_root.clone()]));
    index.attach_wal(store.as_ref()).unwrap();

    let first = content_root.join("first.txt");
    std::fs::write(&first, b"first").unwrap();
    index.apply_events(&[EventRecord {
        seq: 1,
        timestamp: std::time::SystemTime::now(),
        event_type: EventType::Create,
        id: FileIdentifier::Path(first.clone()),
        path_hint: Some(first),
    }]);
    index.snapshot_now(store.clone()).await.unwrap();
    assert!(stable_v7_path_for(store.path()).exists());

    index.set_stable_snapshot_enabled(false);
    let second = content_root.join("second.txt");
    std::fs::write(&second, b"second").unwrap();
    index.apply_events(&[EventRecord {
        seq: 2,
        timestamp: std::time::SystemTime::now(),
        event_type: EventType::Create,
        id: FileIdentifier::Path(second.clone()),
        path_hint: Some(second),
    }]);
    index.snapshot_now(store.clone()).await.unwrap();

    assert!(
        !stable_v7_path_for(store.path()).exists(),
        "disabled stable snapshots must not leave a stale preferred recovery source"
    );
    let loaded = TieredIndex::load(store.as_ref(), vec![content_root.clone()])
        .await
        .unwrap();
    assert!(!loaded.query("first").is_empty());
    assert!(!loaded.query("second").is_empty());

    let _ = std::fs::remove_dir_all(&root);
}

/// 20. 旧文件清理：seal 后的旧 WAL 正确清理
#[test]
fn wal_sealed_files_cleaned_up() {
    use fd_rdd::storage::wal::WalStore;

    let dir = unique_tmp_dir("wal-cleanup");
    std::fs::create_dir_all(&dir).unwrap();

    let wal = WalStore::open_in_dir(dir.clone()).unwrap();

    // Write some events and seal multiple times
    for i in 0..3u64 {
        let path = dir.join(format!("file_{}.txt", i));
        wal.append(&[EventRecord {
            seq: i + 1,
            timestamp: std::time::SystemTime::now(),
            event_type: EventType::Create,
            id: FileIdentifier::Path(path.clone()),
            path_hint: Some(path),
        }])
        .unwrap();
        let _ = wal.seal().unwrap();
    }

    // Count sealed files
    let sealed_count = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .map(|s| s.starts_with("events.wal.seal-"))
                .unwrap_or(false)
        })
        .count();
    assert_eq!(sealed_count, 3, "Should have exactly 3 sealed WAL files");

    // Cleanup all sealed files
    wal.cleanup_sealed_up_to(u64::MAX).unwrap();

    let sealed_after = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .map(|s| s.starts_with("events.wal.seal-"))
                .unwrap_or(false)
        })
        .count();
    assert_eq!(sealed_after, 0, "All sealed WAL files should be cleaned up");

    let _ = std::fs::remove_dir_all(&dir);
}
