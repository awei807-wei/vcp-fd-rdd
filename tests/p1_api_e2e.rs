//! P1 — daemon API and CLI end-to-end tests.

#[allow(dead_code)]
mod common;

use common::{
    fd_rdd_query_exe_path, unique_port, unique_tmp_dir, wait_for_file_visible,
    wait_for_index_stable, FdRddProcess,
};
use reqwest::blocking::Client;
use serde_json::json;
use std::process::Command;
use std::time::{Duration, Instant};

fn get_json(port: u16, path: &str) -> serde_json::Value {
    Client::new()
        .get(format!("http://127.0.0.1:{port}{path}"))
        .send()
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .unwrap()
}

#[test]
fn http_api_manual_scan_and_observability_endpoints_work() {
    let root = unique_tmp_dir("api-e2e-root");
    let state = unique_tmp_dir("api-e2e-state");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&state).unwrap();
    std::fs::write(root.join("initial_api_probe.txt"), b"initial").unwrap();

    let port = unique_port();
    let snapshot = state.join("index.db");
    let process = FdRddProcess::spawn(
        &root,
        port,
        &snapshot,
        &[
            "--no-watch",
            "--snapshot-interval-secs",
            "3600",
            "--debounce-ms",
            "20",
        ],
    );

    assert_eq!(wait_for_index_stable(port, 1, 15).unwrap(), 1);

    let health = get_json(port, "/health");
    assert_eq!(health["status"], "ok");
    assert_eq!(health["index_health"], "static");
    assert_eq!(health["watch_enabled"], false);
    assert_eq!(health["version"], env!("CARGO_PKG_VERSION"));
    assert!(health["diagnostics"].is_object());
    assert_eq!(
        health["diagnostics"]["system"]["version"],
        env!("CARGO_PKG_VERSION")
    );
    assert!(health["diagnostics"]["storage"]["wal_events_replayed"]
        .as_u64()
        .is_some());
    assert_eq!(
        health["diagnostics"]["security"]["uds_peer_policy"],
        "same-user"
    );

    let status = get_json(port, "/status");
    assert_eq!(status["indexed_count"], 1);
    assert_eq!(status["is_rebuilding"], false);

    let memory = get_json(port, "/memory");
    assert!(memory["process_rss_bytes"].as_u64().unwrap_or(0) > 0);
    assert!(memory.get("process_swap_bytes").is_some());

    let watch = get_json(port, "/watch-state");
    assert_eq!(watch["mode"], "off");
    assert!(watch["notes"].as_array().is_some());

    let debug = get_json(port, "/debug/tiered-watch");
    assert!(debug.is_object());

    let metrics = get_json(port, "/metrics");
    assert!(metrics["queries_total"].as_u64().is_some());
    assert!(metrics["events_applied"].as_u64().is_some());

    let trim = get_json(port, "/trim");
    assert!(trim["rss_before_bytes"].as_u64().is_some());
    assert!(trim["rss_after_bytes"].as_u64().is_some());

    let client = Client::new();
    let empty_scan = client
        .post(format!("http://127.0.0.1:{port}/scan"))
        .json(&json!({ "paths": [] }))
        .send()
        .unwrap();
    assert_eq!(empty_scan.status(), reqwest::StatusCode::BAD_REQUEST);

    let forbidden_scan = client
        .post(format!("http://127.0.0.1:{port}/scan"))
        .json(&json!({ "paths": [state.to_string_lossy()] }))
        .send()
        .unwrap();
    assert_eq!(forbidden_scan.status(), reqwest::StatusCode::FORBIDDEN);
    let health_after_reject = get_json(port, "/health");
    assert!(
        health_after_reject["diagnostics"]["security"]["scan_reject_count"]
            .as_u64()
            .unwrap_or(0)
            >= 1
    );

    let late = root.join("manual_scan_api_probe.txt");
    std::fs::write(&late, b"manual").unwrap();
    assert!(
        !common::fd_rdd_client::search(port, "manual_scan_api_probe", 10)
            .iter()
            .any(|r| r.path == late),
        "--no-watch daemon should not see new files before /scan"
    );

    let scan_resp: serde_json::Value = client
        .post(format!("http://127.0.0.1:{port}/scan"))
        .json(&json!({ "paths": [root.to_string_lossy()] }))
        .send()
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .unwrap();
    assert!(scan_resp["scanned"].as_u64().unwrap_or(0) >= 2);
    assert!(
        wait_for_file_visible(port, &late, 5),
        "manual /scan should make late file searchable"
    );

    process.kill();
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&state);
}

#[cfg(unix)]
#[test]
fn fd_rdd_query_cli_streams_results_from_real_uds_socket() {
    let root = unique_tmp_dir("query-cli-root");
    let state = unique_tmp_dir("query-cli-state");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&state).unwrap();

    let probe = root.join("cli_socket_probe.txt");
    std::fs::write(&probe, b"cli").unwrap();
    let socket = state.join("fd-rdd.sock");

    let port = unique_port();
    let process = FdRddProcess::spawn(
        &root,
        port,
        &state.join("index.db"),
        &[
            "--uds-socket",
            socket.to_str().unwrap(),
            "--snapshot-interval-secs",
            "3600",
            "--debounce-ms",
            "20",
        ],
    );
    assert_eq!(wait_for_index_stable(port, 1, 15).unwrap(), 1);

    let deadline = Instant::now() + Duration::from_secs(5);
    while !socket.exists() {
        assert!(Instant::now() < deadline, "UDS socket was not created");
        std::thread::sleep(Duration::from_millis(50));
    }

    let output = Command::new(fd_rdd_query_exe_path())
        .arg("--socket")
        .arg(&socket)
        .arg("--limit")
        .arg("10")
        .arg("cli_socket_probe")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "fd-rdd-query failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("cli_socket_probe.txt"));

    process.kill();
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&state);
}
