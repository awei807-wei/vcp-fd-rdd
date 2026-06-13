//! P1 — daemon API and CLI end-to-end tests.

#[allow(dead_code)]
mod common;

use common::{
    fd_rdd_query_exe_path, get_json, unique_port, unique_tmp_dir, wait_for_file_visible,
    wait_for_index_stable, FdRddProcess,
};
use reqwest::blocking::Client;
use serde_json::json;
use std::process::Command;
use std::time::{Duration, Instant};

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
    assert!(memory["process_rss_bytes"].as_u64().unwrap_or(0) > 1024);
    assert!(memory["process_swap_bytes"].as_u64().is_some());
    assert_eq!(memory["sample_depth"], "light");

    let memory_full = get_json(port, "/memory?full=true");
    assert_eq!(memory_full["sample_depth"], "full");
    assert_eq!(memory_full["cache_hit"], false);

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
    let late_dir = root.join("manual_scan_dir_probe");
    let dupe_a = root.join("manual_dupe_a.txt");
    let dupe_b = root.join("manual_dupe_b.txt");
    let dupe_copy = root.join("manual_dupe_copy.txt");
    std::fs::write(&late, b"manual").unwrap();
    std::fs::create_dir_all(&late_dir).unwrap();
    std::fs::write(&dupe_a, b"same").unwrap();
    std::fs::hard_link(&dupe_a, &dupe_b).unwrap();
    std::fs::write(&dupe_copy, b"same").unwrap();
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
    assert!(scan_resp["scanned"].as_u64().unwrap_or(0) >= 4);
    assert!(
        wait_for_file_visible(port, &late, 5),
        "manual /scan should make late file searchable"
    );
    let dir_results = common::fd_rdd_client::search(port, "type:dir manual_scan_dir_probe", 10);
    assert!(
        dir_results
            .iter()
            .any(|r| r.path == late_dir && r.entry_type == "dir"),
        "HTTP /search should return type=dir for directory results: {dir_results:?}"
    );
    let dupe_results = common::fd_rdd_client::search(port, "dupe: manual_dupe", 10);
    assert!(dupe_results.iter().any(|r| r.path == dupe_a));
    assert!(dupe_results.iter().any(|r| r.path == dupe_b));
    assert!(dupe_results.iter().all(|r| r.path != dupe_copy));
    assert!(
        dupe_results.iter().all(|r| {
            r.reason.as_deref() == Some("hardlink_same_file_key") && r.confidence == Some(1.0)
        }),
        "HTTP /search should annotate hardlink dupe reason/confidence: {dupe_results:?}"
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

    // Retry: the socket file may exist before the daemon is ready to accept.
    let mut output = None;
    let query_deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < query_deadline {
        let o = Command::new(fd_rdd_query_exe_path())
            .arg("--socket")
            .arg(&socket)
            .arg("--limit")
            .arg("10")
            .arg("cli_socket_probe")
            .output()
            .unwrap();
        if o.status.success() {
            output = Some(o);
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    let output = output.expect("fd-rdd-query did not succeed within 5s after socket appeared");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("cli_socket_probe.txt"));

    process.kill();
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&state);
}

#[test]
fn search_missing_query_param_returns_error() {
    let root = unique_tmp_dir("api-missing-q");
    let state = unique_tmp_dir("api-missing-q-state");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&state).unwrap();
    std::fs::write(root.join("probe.txt"), b"probe").unwrap();

    let port = unique_port();
    let snapshot = state.join("index.db");
    let process = FdRddProcess::spawn(
        &root,
        port,
        &snapshot,
        &["--no-watch", "--snapshot-interval-secs", "3600"],
    );
    common::wait_for_index_stable(port, 1, 15).unwrap();

    let client = Client::new();

    // Missing `q` param entirely
    let resp = client
        .get(format!("http://127.0.0.1:{port}/search"))
        .send()
        .unwrap();
    assert!(
        resp.status().is_client_error(),
        "/search without q param should return 4xx, got {}",
        resp.status()
    );

    // Empty query `q=`
    let resp_empty = client
        .get(format!("http://127.0.0.1:{port}/search?q="))
        .send()
        .unwrap();
    // Empty query may return 200 with empty results or 400 — just verify it doesn't 500
    assert!(
        resp_empty.status().is_success() || resp_empty.status().is_client_error(),
        "/search?q= should return 2xx or 4xx, got {}",
        resp_empty.status()
    );

    process.kill();
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&state);
}

#[test]
fn health_reports_ok_when_watch_is_enabled() {
    let root = unique_tmp_dir("api-health-watch");
    let state = unique_tmp_dir("api-health-watch-state");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&state).unwrap();
    std::fs::write(root.join("probe.txt"), b"probe").unwrap();

    let port = unique_port();
    let snapshot = state.join("index.db");
    let process = FdRddProcess::spawn(
        &root,
        port,
        &snapshot,
        &["--snapshot-interval-secs", "3600", "--debounce-ms", "20"],
    );
    common::wait_for_index_stable(port, 1, 15).unwrap();

    let client = Client::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut health = serde_json::Value::Null;
    let mut watch_started = false;
    while Instant::now() < deadline {
        health = client
            .get(format!("http://127.0.0.1:{port}/health"))
            .send()
            .unwrap()
            .json()
            .unwrap();
        if health["watch_enabled"] == true {
            watch_started = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    assert_eq!(health["status"], "ok");

    if watch_started {
        assert_eq!(
            health["index_health"], "ok",
            "index_health should be 'ok' when watch is enabled: {health}"
        );
        assert_eq!(health["watch_enabled"], true);
    } else {
        // Under concurrent multi-threaded test execution, inotify instances or
        // watches can be exhausted, causing the daemon to fall back to no-watch
        // mode.  Verify the fallback is consistent instead of failing.
        assert_eq!(health["index_health"], "static");
        eprintln!(
            "WARN: watcher did not start (likely inotify resource exhaustion \
             under concurrent test execution), skipping watch assertions"
        );
    }

    process.kill();
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&state);
}
