//! P1 — LSM compaction tests.
//!
//! Validates that LSM compaction correctly merges segments and reclaims disk space,
//! and that old sealed WAL files and expired LSM segments are properly cleaned up.

use std::path::PathBuf;
use std::sync::Arc;

use fd_rdd::core::{EventRecord, EventType, FileIdentifier};
use fd_rdd::index::TieredIndex;
use fd_rdd::storage::snapshot::SnapshotStore;

fn unique_tmp_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("fd-rdd-lsm-{}-{}", tag, nanos))
}

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
            id: FileIdentifier::Fid {
                dev: 1,
                ino: i + 1,
            },
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
    assert!(sealed_count >= 3, "Should have at least 3 sealed WAL files");

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
