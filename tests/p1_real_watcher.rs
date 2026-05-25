//! P1 — real daemon watcher event tests.

#[allow(dead_code)]
mod common;

use common::{
    unique_port, unique_tmp_dir, wait_for_file_gone, wait_for_file_visible, FdRddProcess,
};

#[test]
fn recursive_daemon_watcher_tracks_create_rename_and_delete() {
    let root = unique_tmp_dir("real-watcher-root");
    let state = unique_tmp_dir("real-watcher-state");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&state).unwrap();

    let initial = root.join("watcher_initial_probe.txt");
    std::fs::write(&initial, b"initial").unwrap();

    let port = unique_port();
    let process = FdRddProcess::spawn(
        &root,
        port,
        &state.join("index.db"),
        &[
            "--snapshot-interval-secs",
            "3600",
            "--debounce-ms",
            "20",
            "--event-channel-size",
            "4096",
        ],
    );
    common::wait_for_indexed_count(port, 1, 15).unwrap();

    let created = root.join("real_watcher_created_probe.txt");
    std::fs::write(&created, b"created").unwrap();
    assert!(
        wait_for_file_visible(port, &created, 10),
        "created file should be visible through the real watcher"
    );

    let renamed = root.join("real_watcher_renamed_probe.txt");
    std::fs::rename(&created, &renamed).unwrap();
    assert!(
        wait_for_file_visible(port, &renamed, 10),
        "renamed target should become visible through the real watcher"
    );
    assert!(
        wait_for_file_gone(port, &created, 10),
        "renamed source should disappear through the real watcher"
    );

    std::fs::remove_file(&renamed).unwrap();
    assert!(
        wait_for_file_gone(port, &renamed, 10),
        "deleted file should disappear through the real watcher"
    );

    process.kill();
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&state);
}
