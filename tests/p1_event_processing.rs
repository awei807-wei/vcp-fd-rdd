//! P1 — Event processing tests.
//!
//! Validates rename/delete event handling and high-load event processing.

use std::path::PathBuf;
use std::sync::Arc;

use fd_rdd::core::{EventRecord, EventType, FileIdentifier};
use fd_rdd::index::TieredIndex;

fn unique_tmp_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("fd-rdd-event-{}-{}", tag, nanos))
}

/// 11. 高负载事件处理（模拟 git clone 批量创建）— 标记 `#[ignore]`
#[test]
#[ignore]
fn high_load_event_processing() {
    let root = unique_tmp_dir("high-load");
    std::fs::create_dir_all(&root).unwrap();

    let index = Arc::new(TieredIndex::empty(vec![root.clone()]));

    // Simulate 10,000 create events (like a git clone)
    let mut events: Vec<EventRecord> = Vec::new();
    for i in 0..10_000u64 {
        let path = root.join(format!("file_{:05}.txt", i));
        events.push(EventRecord {
            seq: i + 1,
            timestamp: std::time::SystemTime::now(),
            event_type: EventType::Create,
            id: FileIdentifier::Fid { dev: 1, ino: i + 1 },
            path_hint: Some(path.clone()),
        });
    }

    // Apply in batches
    for chunk in events.chunks(1000) {
        index.apply_events(chunk);
    }

    // Should not panic or OOM
    let _ = std::fs::remove_dir_all(&root);
}

/// 12. 重命名事件正确更新索引
#[test]
fn rename_event_updates_index() {
    let root = unique_tmp_dir("rename");
    std::fs::create_dir_all(&root).unwrap();

    let index = Arc::new(TieredIndex::empty(vec![root.clone()]));

    let old_path = root.join("old_name.txt");
    let new_path = root.join("new_name.txt");

    // First, upsert the file directly into L2 so it exists in the index
    use fd_rdd::core::{FileKey, FileMeta};
    let l2 = index.l2.load_full();
    l2.upsert(FileMeta {
        file_key: FileKey {
            dev: 1,
            ino: 100,
            generation: 0,
        },
        path: old_path.clone(),
        size: 42,
        mtime: None,
        ctime: None,
        atime: None,
    });

    // L2 被外部直接修改后需要刷新 base 索引
    index.refresh_base();

    // Verify old name is findable
    let results = index.query("old_name");
    assert!(
        results.iter().any(|m| m.path.ends_with("old_name.txt")),
        "Old name should be findable before rename"
    );

    // Then rename it via event
    let rename_event = EventRecord {
        seq: 2,
        timestamp: std::time::SystemTime::now(),
        event_type: EventType::Rename {
            from: FileIdentifier::Fid { dev: 1, ino: 100 },
            from_path_hint: Some(old_path.clone()),
        },
        id: FileIdentifier::Fid { dev: 1, ino: 100 },
        path_hint: Some(new_path.clone()),
    };
    index.apply_events(&[rename_event]);

    // Query for new name should find it
    let results = index.query("new_name");
    assert!(
        results.iter().any(|m| m.path.ends_with("new_name.txt")),
        "Renamed file should be findable by new name"
    );

    let _ = std::fs::remove_dir_all(&root);
}

/// 13. 删除事件正确清理索引
#[test]
fn delete_event_removes_from_index() {
    let root = unique_tmp_dir("delete");
    std::fs::create_dir_all(&root).unwrap();

    let index = Arc::new(TieredIndex::empty(vec![root.clone()]));

    let path = root.join("to_delete.txt");

    // Create
    let create_event = EventRecord {
        seq: 1,
        timestamp: std::time::SystemTime::now(),
        event_type: EventType::Create,
        id: FileIdentifier::Fid { dev: 1, ino: 200 },
        path_hint: Some(path.clone()),
    };
    index.apply_events(&[create_event]);

    // Delete
    let delete_event = EventRecord {
        seq: 2,
        timestamp: std::time::SystemTime::now(),
        event_type: EventType::Delete,
        id: FileIdentifier::Fid { dev: 1, ino: 200 },
        path_hint: Some(path.clone()),
    };
    index.apply_events(&[delete_event]);

    // Query should not find it
    let results = index.query("to_delete");
    assert!(
        results.is_empty(),
        "Deleted file should not appear in query results"
    );

    let _ = std::fs::remove_dir_all(&root);
}
