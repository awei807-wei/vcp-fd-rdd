//! HTTP client for fd-rdd integration tests.
//!
//! Synchronous blocking client built on `reqwest::blocking`.

use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;

const BASE_TIMEOUT: Duration = Duration::from_secs(10);

/// A single search result returned by the HTTP `/search` endpoint.
#[derive(Debug, Clone, Deserialize)]
pub struct SearchResult {
    pub path: PathBuf,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub size: u64,
    #[serde(default)]
    pub score: i64,
    #[serde(default)]
    pub highlights: Vec<[usize; 2]>,
}

fn client() -> reqwest::blocking::Client {
    reqwest::blocking::Client::builder()
        .timeout(BASE_TIMEOUT)
        .build()
        .expect("build reqwest blocking client")
}

/// Perform a lightweight health check against `/health`.
pub fn health_check(port: u16) -> bool {
    let url = format!("http://127.0.0.1:{}/health", port);
    match client().get(&url).send() {
        Ok(resp) => resp.status().is_success(),
        Err(_) => false,
    }
}

/// Fetch `/status` and return the parsed JSON value.
pub fn status(port: u16) -> Option<serde_json::Value> {
    let url = format!("http://127.0.0.1:{}/status", port);
    match client().get(&url).send() {
        Ok(resp) if resp.status().is_success() => resp.json().ok(),
        _ => None,
    }
}

/// Query `/search` and deserialize into a typed `Vec<SearchResult>`.
pub fn search(port: u16, q: &str, limit: usize) -> Vec<SearchResult> {
    let url = format!("http://127.0.0.1:{}/search", port);
    match client()
        .get(&url)
        .query(&[("q", q), ("limit", &limit.to_string())])
        .send()
    {
        Ok(resp) if resp.status().is_success() => resp.json().unwrap_or_default(),
        _ => Vec::new(),
    }
}

/// Query `/search` and return the raw JSON body as a `String`.
pub fn search_raw(port: u16, q: &str, limit: usize) -> String {
    let url = format!("http://127.0.0.1:{}/search", port);
    match client()
        .get(&url)
        .query(&[("q", q), ("limit", &limit.to_string())])
        .send()
    {
        Ok(resp) if resp.status().is_success() => resp.text().unwrap_or_default(),
        _ => String::new(),
    }
}

/// Convenience helper to read the `indexed_count` field from `/status`.
pub fn indexed_count(port: u16) -> Option<usize> {
    status(port)
        .and_then(|v| v.get("indexed_count").and_then(|n| n.as_u64()))
        .map(|n| n as usize)
}
