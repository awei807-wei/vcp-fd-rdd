//! P1 — Watch degradation tests.
//!
//! Validates that when inotify watch fails, the system degrades gracefully
//! (e.g., to polling) without crashing.

use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use fd_rdd::event::watcher::{EventWatcher, watch_roots};

fn unique_tmp_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("fd-rdd-watch-{}-{}", tag, nanos))
}

/// 26. watch 降级到轮询时正常工作
///     When watching a non-existent directory, watch_roots should report failure
///     without crashing.
#[test]
fn watch_nonexistent_root_reports_failure() {
    let root = unique_tmp_dir("watch-noexist");
    // Intentionally don't create the directory

    let drops = Arc::new(AtomicU64::new(0));
    let rescans = Arc::new(AtomicU64::new(0));

    let (mut _rx, mut watcher) =
        EventWatcher::start(std::slice::from_ref(&root), 64, drops, rescans, None).unwrap();

    // watch_roots should report the non-existent root as failed
    let failed = watch_roots(&mut watcher, std::slice::from_ref(&root));
    assert!(
        !failed.is_empty(),
        "Should report failure for non-existent directory"
    );
    assert_eq!(failed[0], root);
}

/// Watch existing directory succeeds
#[test]
fn watch_existing_root_succeeds() {
    let root = unique_tmp_dir("watch-exists");
    std::fs::create_dir_all(&root).unwrap();

    let drops = Arc::new(AtomicU64::new(0));
    let rescans = Arc::new(AtomicU64::new(0));

    let (_rx, mut watcher) =
        EventWatcher::start(std::slice::from_ref(&root), 64, drops, rescans, None).unwrap();

    let failed = watch_roots(&mut watcher, std::slice::from_ref(&root));
    assert!(
        failed.is_empty(),
        "Should succeed for existing directory"
    );

    let _ = std::fs::remove_dir_all(&root);
}
