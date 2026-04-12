use crate::index::TieredIndex;
use crate::query::scoring::{compute_highlights, score_result, ScoreConfig};
use crate::query::{execute_query, QueryMode, SortColumn, SortOrder};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

const DEFAULT_SEARCH_LIMIT: usize = 100;
const MAX_SEARCH_LIMIT: usize = 10_000;
const SEARCH_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, Default)]
pub struct HealthTelemetry {
    pub last_snapshot_time: u64,
    pub watch_failures: u64,
    pub watcher_degraded: bool,
    pub degraded_roots: usize,
    pub overflow_drops: u64,
    pub rescan_signals: u64,
}

#[derive(Deserialize)]
pub struct SearchParams {
    pub q: String,
    pub limit: Option<usize>,
    pub mode: Option<String>,
    pub sort: Option<String>,
    pub order: Option<String>,
}

#[derive(Serialize)]
pub struct SearchResult {
    pub path: String,
    pub size: u64,
    pub score: i64,
    pub highlights: Vec<[usize; 2]>,
}

#[derive(Deserialize)]
pub struct ScanParams {
    pub paths: Vec<String>,
}

#[derive(Serialize)]
pub struct ScanResponse {
    pub scanned: usize,
    pub elapsed_ms: u64,
}

#[derive(Serialize)]
pub struct StatusResponse {
    pub indexed_count: usize,
}

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub index_health: &'static str,
    pub uptime_secs: u64,
    pub index_entries: usize,
    pub version: &'static str,
    pub last_snapshot_time: u64,
    pub watch_failures: u64,
    pub watcher_degraded: bool,
    pub degraded_roots: usize,
    pub overflow_drops: u64,
    pub rescan_signals: u64,
    pub issues: Vec<String>,
}

#[derive(Clone, Copy, Debug)]
struct QueryServerConfig {
    default_limit: usize,
    max_limit: usize,
    query_timeout: Duration,
}

impl Default for QueryServerConfig {
    fn default() -> Self {
        Self {
            default_limit: DEFAULT_SEARCH_LIMIT,
            max_limit: MAX_SEARCH_LIMIT,
            query_timeout: SEARCH_TIMEOUT,
        }
    }
}

#[derive(Clone)]
struct QueryServerState {
    index: Arc<TieredIndex>,
    config: QueryServerConfig,
    start_time: Instant,
    health_provider: Arc<dyn Fn() -> HealthTelemetry + Send + Sync>,
}

pub struct QueryServer {
    pub index: Arc<TieredIndex>,
    config: QueryServerConfig,
    health_provider: Arc<dyn Fn() -> HealthTelemetry + Send + Sync>,
}

impl QueryServer {
    pub fn new(index: Arc<TieredIndex>) -> Self {
        Self {
            index,
            config: QueryServerConfig::default(),
            health_provider: Arc::new(|| HealthTelemetry::default()),
        }
    }

    pub fn with_health_provider(
        mut self,
        provider: Arc<dyn Fn() -> HealthTelemetry + Send + Sync>,
    ) -> Self {
        self.health_provider = provider;
        self
    }

    pub async fn run(self, port: u16) -> anyhow::Result<()> {
        let state = QueryServerState {
            index: self.index,
            config: self.config,
            start_time: Instant::now(),
            health_provider: self.health_provider,
        };
        let app = Router::new()
            .route("/search", get(search_handler))
            .route("/status", get(status_handler))
            .route("/health", get(health_handler))
            .route("/scan", post(scan_handler))
            .with_state(state);

        let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", port)).await?;
        tracing::info!("HTTP Query Server listening on port {}", port);
        axum::serve(listener, app).await?;
        Ok(())
    }
}

fn normalize_search_limit(limit: Option<usize>, config: QueryServerConfig) -> usize {
    limit
        .unwrap_or(config.default_limit)
        .max(1)
        .min(config.max_limit)
}

fn resolve_query_mode(mode: Option<&str>) -> Result<QueryMode, String> {
    QueryMode::parse_label(mode).map_err(|e| format!("invalid query mode: {}", e))
}

async fn search_handler(
    Query(params): Query<SearchParams>,
    State(state): State<QueryServerState>,
) -> Result<Json<Vec<SearchResult>>, (StatusCode, String)> {
    let limit = normalize_search_limit(params.limit, state.config);
    let mode =
        resolve_query_mode(params.mode.as_deref()).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let keyword = params.q;
    let index = state.index.clone();

    let kw_clone = keyword.clone();
    let sort = SortColumn::parse(params.sort.as_deref());
    let order = SortOrder::parse(params.order.as_deref());
    let search_task = tokio::task::spawn_blocking(move || {
        execute_query(index.as_ref(), &kw_clone, limit, mode, sort, order)
    });
    let results = match tokio::time::timeout(state.config.query_timeout, search_task).await {
        Ok(Ok(results)) => results,
        Ok(Err(e)) => {
            tracing::error!("HTTP search task failed: {}", e);
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "search task failed".to_string(),
            ));
        }
        Err(_) => {
            tracing::warn!(
                "HTTP search timed out after {:?} (limit={}, mode={})",
                state.config.query_timeout,
                limit,
                mode.as_str()
            );
            return Err((
                StatusCode::REQUEST_TIMEOUT,
                format!(
                    "search timed out after {} ms",
                    state.config.query_timeout.as_millis()
                ),
            ));
        }
    };

    let config = ScoreConfig::from_query(&keyword);
    let response = results
        .into_iter()
        .map(|m| {
            let path_str = m.path.to_string_lossy().into_owned();
            let score = score_result(&m, &config);
            let highlights = compute_highlights(&path_str, &keyword);
            SearchResult {
                path: path_str,
                size: m.size,
                score,
                highlights,
            }
        })
        .collect();

    Ok(Json(response))
}

async fn status_handler(State(state): State<QueryServerState>) -> Json<StatusResponse> {
    Json(StatusResponse {
        indexed_count: state.index.file_count(),
    })
}

async fn health_handler(State(state): State<QueryServerState>) -> Json<HealthResponse> {
    let uptime = state.start_time.elapsed().as_secs();
    let health = (state.health_provider)();
    let mut issues = Vec::new();
    if health.watcher_degraded {
        issues.push(format!(
            "watcher_degraded: {} unwatched directories are using fallback polling",
            health.degraded_roots
        ));
    }
    if health.watch_failures > 0 {
        issues.push(format!("watch_failures: {}", health.watch_failures));
    }
    if health.overflow_drops > 0 || health.rescan_signals > 0 {
        issues.push(format!(
            "event_recovery: overflow_drops={} rescan_signals={}",
            health.overflow_drops, health.rescan_signals
        ));
    }
    if health.last_snapshot_time == 0 {
        issues.push("snapshot_not_written_yet".to_string());
    }
    let index_health = if health.watcher_degraded {
        "degraded"
    } else if issues.is_empty() {
        "ok"
    } else {
        "warning"
    };
    Json(HealthResponse {
        status: "ok",
        index_health,
        uptime_secs: uptime,
        index_entries: state.index.file_count(),
        version: env!("CARGO_PKG_VERSION"),
        last_snapshot_time: health.last_snapshot_time,
        watch_failures: health.watch_failures,
        watcher_degraded: health.watcher_degraded,
        degraded_roots: health.degraded_roots,
        overflow_drops: health.overflow_drops,
        rescan_signals: health.rescan_signals,
        issues,
    })
}

async fn scan_handler(
    State(state): State<QueryServerState>,
    Json(params): Json<ScanParams>,
) -> Result<Json<ScanResponse>, (StatusCode, String)> {
    if params.paths.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "paths must not be empty".to_string(),
        ));
    }

    let dirs: Vec<PathBuf> = params.paths.iter().take(10).map(PathBuf::from).collect();

    let index = state.index.clone();
    let (scanned, elapsed_ms) =
        tokio::task::spawn_blocking(move || index.scan_dirs_immediate(&dirs))
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(ScanResponse {
        scanned,
        elapsed_ms,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_search_limit_clamps_to_server_bounds() {
        let cfg = QueryServerConfig::default();
        assert_eq!(normalize_search_limit(None, cfg), DEFAULT_SEARCH_LIMIT);
        assert_eq!(normalize_search_limit(Some(0), cfg), 1);
        assert_eq!(normalize_search_limit(Some(10), cfg), 10);
        assert_eq!(
            normalize_search_limit(Some(usize::MAX), cfg),
            MAX_SEARCH_LIMIT
        );
    }

    #[test]
    fn resolve_query_mode_supports_fuzzy() {
        assert_eq!(resolve_query_mode(None).unwrap(), QueryMode::Exact);
        assert_eq!(resolve_query_mode(Some("fuzzy")).unwrap(), QueryMode::Fuzzy);
        assert!(resolve_query_mode(Some("oops")).is_err());
    }
}
