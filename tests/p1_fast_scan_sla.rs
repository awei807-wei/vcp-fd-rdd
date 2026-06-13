//! P1 — lease-hotset fast scan SLA integration tests.
//!
//! Starts a real daemon with an isolated config and verifies the current SLA
//! contract: fast scan gives a 5s target only to active lease-hotset dirs, while
//! cold dirs are repaired by the bounded cold-sweep / dirty-repair lane.

#[allow(dead_code)]
mod common;

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use common::{
    fd_rdd_client, get_json, unique_port, unique_tmp_dir, wait_for_file_gone,
    wait_for_file_visible, wait_for_indexed_count, FdRddProcess,
};

const HOTSET_SLA_SECS: u64 = 5;
const FAST_SCAN_TARGET_SECS: u64 = 1;
const COLD_SWEEP_SECS: u64 = 1;
const COLD_EVENTUAL_SECS: u64 = COLD_SWEEP_SECS;

fn write_test_config(config_home: &Path, root: &Path, port: u16) {
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
max_watch_dirs = 1
l0_max_cost_per_root = 1
scan_items_per_sec = 5000
scan_ms_per_tick = 20
l1_scan_interval_secs = {cold_sweep}
l2_scan_interval_secs = {cold_sweep}
l3_scan_policy = "disabled"
l1_empty_scans_to_l2 = 1000
l2_empty_scans_to_l3 = 1000
ephemeral_watch_budget = 0
hot_dirs = []
strict_required_hot_dirs = []
l1_l2_fast_scan_enabled = true
l1_l2_fast_scan_target_secs = {target}
l1_l2_fast_scan_tick_ms = 100
l1_l2_fast_scan_stat_budget_per_tick = 256
l1_l2_fast_scan_readdir_budget_per_tick = 64
l1_l2_fast_scan_bootstrap_budget_per_tick = 64
l1_l2_fast_scan_hotset_max_leases = 16
l1_l2_fast_scan_lease_ttl_secs = 30
l1_l2_fast_scan_proc_sampler_lease_ttl_secs = 10
l1_l2_fast_scan_explicit_lease_ttl_secs = 0
l1_l2_fast_scan_sentinel_registry_max_entries = 16
network_fast_scan_mode = "best_effort"
network_fast_scan_stat_budget_per_tick = 0
network_fast_scan_readdir_budget_per_tick = 0
"#,
        root = root.display(),
        port = port,
        target = FAST_SCAN_TARGET_SECS,
        cold_sweep = COLD_SWEEP_SECS,
    );
    std::fs::write(config_dir.join("config.toml"), config).unwrap();
}

fn create_tree(root: &Path) -> PathBuf {
    let hotset = root.join("lease-hotset");
    std::fs::create_dir_all(hotset.join("budget-inflator")).unwrap();
    std::fs::write(hotset.join("hotset_seed_probe.txt"), b"seed").unwrap();
    std::fs::write(root.join("root_seed_probe.txt"), b"seed").unwrap();
    hotset
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

fn assert_visible_within(port: u16, path: &Path, timeout_secs: u64, context: &str) {
    assert!(
        wait_for_file_visible(port, path, timeout_secs),
        "{context}: {} should become searchable within {timeout_secs}s",
        path.display()
    );
}

fn assert_gone_within(port: u16, path: &Path, timeout_secs: u64, context: &str) {
    assert!(
        wait_for_file_gone(port, path, timeout_secs),
        "{context}: {} should disappear within {timeout_secs}s",
        path.display()
    );
}

fn assert_path_query_visible_within(port: u16, path: &Path, timeout_secs: u64, context: &str) {
    let query = path.to_string_lossy().into_owned();
    let _ = fd_rdd_client::search(port, &query, 100);
    assert_visible_within(port, path, timeout_secs, context);
}

fn wait_for_search_hit(port: u16, path: &Path, timeout_secs: u64) {
    assert_visible_within(port, path, timeout_secs, "query seed for hotset lease");
}

fn exercise_hotset_create_delete_rename(port: u16, dir: &Path) {
    let create = dir.join("hotset_created_probe.txt");
    std::fs::write(&create, b"created").unwrap();
    assert_visible_within(port, &create, HOTSET_SLA_SECS, "hotset create");

    std::fs::remove_file(&create).unwrap();
    assert_gone_within(port, &create, HOTSET_SLA_SECS, "hotset delete");

    let source = dir.join("hotset_rename_source_probe.txt");
    let target = dir.join("hotset_rename_target_probe.txt");
    std::fs::write(&source, b"rename").unwrap();
    assert_visible_within(
        port,
        &source,
        HOTSET_SLA_SECS,
        "hotset rename source create",
    );

    std::fs::rename(&source, &target).unwrap();
    assert_visible_within(port, &target, HOTSET_SLA_SECS, "hotset rename target");
    assert_gone_within(port, &source, HOTSET_SLA_SECS, "hotset rename source");
}

fn exercise_cold_eventual_create(port: u16, root: &Path) -> PathBuf {
    let cold_dir = root.join("cold-eventual");
    std::fs::create_dir_all(&cold_dir).unwrap();
    let create = cold_dir.join("cold_eventual_created_probe.txt");
    std::fs::write(&create, b"created").unwrap();
    assert_path_query_visible_within(
        port,
        &create,
        COLD_EVENTUAL_SECS,
        "cold create eventual consistency",
    );
    create
}

#[test]
fn tiered_fast_scan_hotset_sla_and_cold_eventual_consistency() {
    let root = unique_tmp_dir("fast-scan-hotset-root");
    let state = unique_tmp_dir("fast-scan-hotset-state");
    let config_home = unique_tmp_dir("fast-scan-hotset-config");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&state).unwrap();
    std::fs::create_dir_all(&config_home).unwrap();
    let hotset = create_tree(&root);

    let port = unique_port();
    write_test_config(&config_home, &root, port);
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
    wait_for_indexed_count(port, 2, 15).unwrap();
    wait_for_search_hit(port, &hotset.join("hotset_seed_probe.txt"), HOTSET_SLA_SECS);

    let initial_watch = wait_for_watch_state(
        port,
        Duration::from_secs(5),
        |watch| {
            watch["fast_scan_enabled"].as_bool() == Some(true)
                && watch["fast_scan_hotset_lease_count"].as_u64().unwrap_or(0) == 1
                && watch["fast_scan_hotset_sentinel_count"]
                    .as_u64()
                    .unwrap_or(0)
                    == 1
                && watch["fast_scan_checked_dirs"].as_u64().unwrap_or(0) > 0
                && watch["fast_scan_initial_backfill_pending"]
                    .as_u64()
                    .unwrap_or(1)
                    == 0
                && watch["l1_dirs"].as_u64().unwrap_or(0) >= 1
        },
        "active lease-hotset fast scan sentinel",
    );
    assert_eq!(initial_watch["fast_scan_mode"], "local_strict");
    assert_eq!(initial_watch["fast_scan_known_dirs"], 1);
    assert_eq!(initial_watch["fast_scan_explicit_lease_count"], 0);
    assert_eq!(initial_watch["fast_scan_auto_lease_count"], 1);
    assert_eq!(initial_watch["fast_scan_budget_degraded"], false);

    exercise_hotset_create_delete_rename(port, &hotset);

    let cold_path = exercise_cold_eventual_create(port, &root);
    let final_watch = wait_for_watch_state(
        port,
        Duration::from_secs(5),
        |watch| {
            watch["fast_scan_checked_dirs"].as_u64().unwrap_or(0) > 0
                && watch["fast_scan_real_changed_dirs"].as_u64().unwrap_or(0) >= 1
                && watch["fast_scan_generated_events"].as_u64().unwrap_or(0) >= 4
                && watch["dirty_queue_len"].as_u64().unwrap_or(0) == 0
        },
        "hotset fast scan changes and cold repair drained",
    );
    // Looking up the cold file proves eventual consistency through `/search`.
    // That successful query is itself a valid lease source, so the cold root
    // may be promoted into the hotset after the assertion above has passed.
    assert!(
        final_watch["fast_scan_known_dirs"].as_u64().unwrap_or(0) >= 1,
        "hotset sentinel should remain active after cold repair: {final_watch}",
    );
    assert!(
        final_watch["fast_scan_hotset_sentinel_count"]
            .as_u64()
            .unwrap_or(0)
            >= 1,
        "at least the original hotset sentinel should remain active: {final_watch}",
    );
    assert_eq!(final_watch["fast_scan_budget_degraded"], false);

    let health = fd_rdd_client::health_json(port).unwrap();
    assert_eq!(health["fast_scan_enabled"], true);
    assert_eq!(health["fast_scan_budget_degraded"], false);
    assert!(
        health["fast_scan_hotset_sentinel_count"]
            .as_u64()
            .unwrap_or(0)
            >= 1,
        "querying the cold file may legitimately add a query lease: {health}",
    );
    assert!(
        health["fast_scan_auto_lease_count"].as_u64().unwrap_or(0) >= 1,
        "at least the original query-derived hotset lease should remain: {health}",
    );
    assert_eq!(health["cold_sweep_period_estimate"], COLD_SWEEP_SECS);
    assert!(
        health["dirty_backlog"].as_u64().is_some(),
        "health should expose dirty_backlog as the cold repair backlog bound: {health}",
    );
    assert!(
        cold_path.exists(),
        "cold eventual consistency assertion should verify the created probe path",
    );

    process.kill();
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&state);
    let _ = std::fs::remove_dir_all(&config_home);
}
