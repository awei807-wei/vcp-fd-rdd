//! P1 — daemon crash/recovery combination tests.

#[allow(dead_code)]
mod common;

use common::{
    unique_port, unique_tmp_dir, wait_for_file_visible, wait_for_indexed_count, FdRddProcess,
};
use fd_rdd::storage::snapshot::{
    stable_v7_path_for, write_recovery_runtime_state, RecoveryRuntimeState,
};

#[test]
fn abrupt_kill_restart_recovers_visible_incremental_file() {
    let root = unique_tmp_dir("crash-restart-root");
    let state = unique_tmp_dir("crash-restart-state");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&state).unwrap();
    std::fs::write(root.join("crash_initial_probe.txt"), b"initial").unwrap();

    let snapshot = state.join("index.db");
    let port = unique_port();
    let process = FdRddProcess::spawn(
        &root,
        port,
        &snapshot,
        &["--snapshot-interval-secs", "3600", "--debounce-ms", "20"],
    );
    wait_for_indexed_count(port, 1, 15).unwrap();

    let late = root.join("crash_late_probe.txt");
    std::fs::write(&late, b"late").unwrap();
    assert!(
        wait_for_file_visible(port, &late, 10),
        "late file should be visible before abrupt kill"
    );

    process.kill();

    let restart = FdRddProcess::spawn(
        &root,
        port,
        &snapshot,
        &["--snapshot-interval-secs", "3600", "--debounce-ms", "20"],
    );
    assert!(
        wait_for_file_visible(port, &late, 15),
        "late file should be recovered after abrupt restart"
    );
    let health = common::fd_rdd_client::health_json(port).unwrap();
    assert_eq!(health["last_clean_shutdown"], false);
    assert_eq!(health["startup_repair_ran"], true);
    restart.kill();

    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&state);
}

#[test]
fn corrupt_stable_snapshot_with_clean_runtime_state_still_repairs_from_root() {
    let root = unique_tmp_dir("corrupt-stable-root");
    let state = unique_tmp_dir("corrupt-stable-state");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&state).unwrap();

    let probe = root.join("corrupt_stable_repair_probe.txt");
    std::fs::write(&probe, b"repair").unwrap();

    let snapshot = state.join("index.db");
    let stable = stable_v7_path_for(&snapshot);
    std::fs::create_dir_all(stable.parent().unwrap()).unwrap();
    std::fs::write(&stable, b"not a valid v7 snapshot").unwrap();
    write_recovery_runtime_state(
        &snapshot,
        &RecoveryRuntimeState {
            last_clean_shutdown: true,
            last_snapshot_unix_secs: 123,
            last_wal_seal_id: 0,
            last_startup_source: "stable".to_string(),
            last_recovery_mode: "clean-shutdown".to_string(),
            root_case_policies: Vec::new(),
        },
    )
    .unwrap();

    let port = unique_port();
    let process = FdRddProcess::spawn(
        &root,
        port,
        &snapshot,
        &["--snapshot-interval-secs", "3600", "--debounce-ms", "20"],
    );
    assert!(
        wait_for_file_visible(port, &probe, 15),
        "startup repair should scan root after corrupt stable snapshot"
    );
    let health = common::fd_rdd_client::health_json(port).unwrap();
    assert_eq!(health["snapshot_source"], "empty");
    assert_eq!(health["startup_repair_ran"], true);
    process.kill();

    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&state);
}
