//! P1 — Process restart recovery tests.

#[allow(dead_code)]
mod common;

use std::time::{SystemTime, UNIX_EPOCH};

use common::{unique_tmp_dir, wait_for_file_visible, wait_for_indexed_count, FdRddProcess};
use fd_rdd::storage::snapshot::{read_recovery_runtime_state, stable_v7_path_for};

fn unique_port() -> u16 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    18_000 + (nanos % 1_000) as u16
}

#[test]
fn sigterm_final_snapshot_resumes_incremental_changes() {
    // Under TSAN (ThreadSanitizer), the instrumented child process cannot
    // complete graceful shutdown (final snapshot + runtime-state write)
    // within any reasonable timeout — bincode serialization + CRC32C + fsync
    // are all pathologically slow with per-access instrumentation.
    //
    // This test covers recovery logic, not thread safety. The poweroff-recovery
    // CI job runs it on stable Rust (without TSAN), which is sufficient.
    if std::env::var("TSAN_OPTIONS").is_ok() {
        eprintln!("Skipping test under TSAN: graceful shutdown is too slow when instrumented");
        return;
    }
    let root = unique_tmp_dir("sigterm-resume-root");
    let state_dir = unique_tmp_dir("sigterm-resume-state");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&state_dir).unwrap();
    let snapshot_path = state_dir.join("index.db");

    let initial = root.join("initial_resume_marker.txt");
    std::fs::write(&initial, "initial").unwrap();

    let port = unique_port();
    let proc = FdRddProcess::spawn(
        &root,
        port,
        &snapshot_path,
        &["--snapshot-interval-secs", "3600", "--debounce-ms", "20"],
    );
    wait_for_indexed_count(port, 1, 15).expect("initial file should be indexed");

    let late = root.join("late_resume_marker.txt");
    std::fs::write(&late, "late").unwrap();
    assert!(
        wait_for_file_visible(port, &late, 15),
        "late file should be visible before graceful shutdown"
    );

    proc.terminate();

    let runtime_state = read_recovery_runtime_state(&snapshot_path).unwrap();
    assert!(runtime_state.last_clean_shutdown);
    assert!(stable_v7_path_for(&snapshot_path).exists());

    let restart = FdRddProcess::spawn(
        &root,
        port,
        &snapshot_path,
        &["--snapshot-interval-secs", "3600", "--debounce-ms", "20"],
    );
    assert!(
        wait_for_file_visible(port, &late, 15),
        "late file should be visible after restart from final snapshot"
    );
    restart.kill();

    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&state_dir);
}
