//! P1 — L1/L2 fast scan SLA integration tests.
//!
//! Starts a real daemon with an isolated config and verifies that non-L0
//! L1/L2 directories pick up create/delete/rename changes through the fast
//! scan lane within the configured SLA window.

#[allow(dead_code)]
mod common;

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use common::{
    fd_rdd_client, unique_port, unique_tmp_dir, wait_for_file_gone, wait_for_file_visible,
    wait_for_indexed_count, FdRddProcess,
};

const SLA_SECS: u64 = 5;
const FAST_SCAN_TARGET_SECS: u64 = 1;

fn write_test_config(config_home: &Path, root: &Path, l0: &Path, l1: &Path, l2: &Path, port: u16) {
    let config_dir = config_home.join("fd-rdd");
    std::fs::create_dir_all(&config_dir).unwrap();
    let config = format!(
        r#"
roots = ["{root}"]
http_port = {port}
snapshot_interval_secs = 3600
watch_enabled = true
watch_mode = "tiered"
ignore_enabled = false
include_hidden = true
startup_repair_enabled = false
lazy_validation_enabled = false
exclude_dirs = []

[tiered_watch]
profile = "balanced"
max_watch_dirs = 4
l0_max_cost_per_root = 4
scan_items_per_sec = 5000
scan_ms_per_tick = 20
l1_scan_interval_secs = 3
l2_scan_interval_secs = 1
l3_scan_policy = "disabled"
l1_empty_scans_to_l2 = 1
l2_empty_scans_to_l3 = 1000
ephemeral_watch_budget = 0
hot_dirs = ["{l0}", "{l1}", "{l2}"]
strict_required_hot_dirs = []
l1_l2_fast_scan_enabled = true
l1_l2_fast_scan_target_secs = {target}
l1_l2_fast_scan_tick_ms = 100
l1_l2_fast_scan_stat_budget_per_tick = 256
l1_l2_fast_scan_readdir_budget_per_tick = 64
l1_l2_fast_scan_bootstrap_budget_per_tick = 512
network_fast_scan_mode = "best_effort"
network_fast_scan_stat_budget_per_tick = 0
network_fast_scan_readdir_budget_per_tick = 0
"#,
        root = root.display(),
        port = port,
        l0 = l0.display(),
        l1 = l1.display(),
        l2 = l2.display(),
        target = FAST_SCAN_TARGET_SECS,
    );
    std::fs::write(config_dir.join("config.toml"), config).unwrap();
}

fn create_tree(root: &Path) -> (PathBuf, PathBuf, PathBuf) {
    let l0 = root.join("l0-hot");
    let l1 = root.join("l1-fast-scan");
    let l2 = root.join("l2-fast-scan");
    std::fs::create_dir_all(&l0).unwrap();
    std::fs::write(l0.join("seed.txt"), b"seed").unwrap();

    for dir in [&l1, &l2] {
        std::fs::create_dir_all(dir.join("known").join("deep")).unwrap();
        std::fs::create_dir_all(dir.join("ballast").join("a")).unwrap();
        std::fs::write(dir.join("known").join("deep").join("seed.txt"), b"seed").unwrap();
    }
    (l0, l1, l2)
}

fn get_json(port: u16, path: &str) -> serde_json::Value {
    reqwest::blocking::Client::new()
        .get(format!("http://127.0.0.1:{port}{path}"))
        .send()
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .unwrap()
}

fn wait_for_watch_state(
    port: u16,
    timeout: Duration,
    predicate: impl Fn(&serde_json::Value) -> bool,
    label: &str,
) -> serde_json::Value {
    let start = Instant::now();
    let mut last = serde_json::Value::Null;
    while start.elapsed() < timeout {
        last = get_json(port, "/watch-state");
        if predicate(&last) {
            return last;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("timed out waiting for watch-state {label}; last={last}");
}

fn assert_visible_within_sla(port: u16, path: &Path, context: &str) {
    assert!(
        wait_for_file_visible(port, path, SLA_SECS),
        "{context}: {} should become searchable within {SLA_SECS}s",
        path.display()
    );
}

fn assert_gone_within_sla(port: u16, path: &Path, context: &str) {
    assert!(
        wait_for_file_gone(port, path, SLA_SECS),
        "{context}: {} should disappear within {SLA_SECS}s",
        path.display()
    );
}

fn exercise_create_delete_rename(port: u16, dir: &Path, prefix: &str) {
    let create = dir.join(format!("{prefix}_created_probe.txt"));
    std::fs::write(&create, b"created").unwrap();
    assert_visible_within_sla(port, &create, "create");

    std::fs::remove_file(&create).unwrap();
    assert_gone_within_sla(port, &create, "delete");

    let source = dir.join(format!("{prefix}_rename_source_probe.txt"));
    let target = dir.join(format!("{prefix}_rename_target_probe.txt"));
    std::fs::write(&source, b"rename").unwrap();
    assert_visible_within_sla(port, &source, "rename source create");

    std::fs::rename(&source, &target).unwrap();
    assert_visible_within_sla(port, &target, "rename target");
    assert_gone_within_sla(port, &source, "rename source");
}

#[test]
fn tiered_fast_scan_updates_l1_and_l2_create_delete_rename_within_sla() {
    let root = unique_tmp_dir("fast-scan-sla-root");
    let state = unique_tmp_dir("fast-scan-sla-state");
    let config_home = unique_tmp_dir("fast-scan-sla-config");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&state).unwrap();
    std::fs::create_dir_all(&config_home).unwrap();
    let (l0, l1, l2) = create_tree(&root);
    let l1_deep = l1.join("known").join("deep");
    let l2_deep = l2.join("known").join("deep");

    let port = unique_port();
    write_test_config(&config_home, &root, &l0, &l1, &l2, port);
    let process = FdRddProcess::spawn_with_env(
        &root,
        port,
        &state.join("index.db"),
        &[
            "--watch-mode",
            "tiered",
            "--snapshot-interval-secs",
            "3600",
            "--debounce-ms",
            "20",
            "--event-channel-size",
            "4096",
        ],
        &[
            ("XDG_CONFIG_HOME", config_home.as_path()),
            ("XDG_DATA_HOME", state.as_path()),
            ("XDG_RUNTIME_DIR", state.as_path()),
        ],
    );
    wait_for_indexed_count(port, 5, 15).unwrap();

    let initial_watch = wait_for_watch_state(
        port,
        Duration::from_secs(5),
        |watch| {
            watch["fast_scan_enabled"].as_bool() == Some(true)
                && watch["fast_scan_known_dirs"].as_u64().unwrap_or(0) >= 5
                && watch["fast_scan_checked_dirs"].as_u64().unwrap_or(0) > 0
                && watch["l1_dirs"].as_u64().unwrap_or(0) >= 2
        },
        "fast scan bootstrap with L1 dirs",
    );
    assert_eq!(initial_watch["fast_scan_mode"], "local_strict");
    assert_eq!(initial_watch["fast_scan_budget_degraded"], false);

    exercise_create_delete_rename(port, &l1_deep, "l1_fast_scan");

    let l2_watch = wait_for_watch_state(
        port,
        Duration::from_secs(10),
        |watch| watch["l2_dirs"].as_u64().unwrap_or(0) >= 1,
        "one cold dir demoted to L2",
    );
    assert!(
        l2_watch["fast_scan_local_strict_ok"]
            .as_bool()
            .unwrap_or(false),
        "fast scan should still satisfy local strict SLA after L2 demotion: {l2_watch}"
    );

    exercise_create_delete_rename(port, &l2_deep, "l2_fast_scan");

    let final_watch = get_json(port, "/watch-state");
    assert!(
        final_watch["fast_scan_checked_dirs"].as_u64().unwrap_or(0) > 0,
        "fast scan should check known dirs: {final_watch}"
    );
    assert!(
        final_watch["fast_scan_changed_dirs"].as_u64().unwrap_or(0) >= 6,
        "fast scan should report all L1/L2 create/delete/rename parent changes: {final_watch}"
    );
    assert!(
        final_watch["fast_scan_generated_events"]
            .as_u64()
            .unwrap_or(0)
            >= 6,
        "dirty apply should generate index updates from fast scan: {final_watch}"
    );

    let health = fd_rdd_client::health_json(port).unwrap();
    assert_eq!(health["fast_scan_enabled"], true);
    assert_eq!(health["fast_scan_budget_degraded"], false);

    process.kill();
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&state);
    let _ = std::fs::remove_dir_all(&config_home);
}
