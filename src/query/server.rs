use crate::index::TieredIndex;
use crate::query::{execute_query, QueryMode};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

const DEFAULT_SEARCH_LIMIT: usize = 100;
const MAX_SEARCH_LIMIT: usize = 10_000;
const SEARCH_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Deserialize)]
pub struct SearchParams {
    pub q: String,
    pub limit: Option<usize>,
    pub mode: Option<String>,
}

#[derive(Serialize)]
pub struct SearchResult {
    pub path: String,
    pub size: u64,
}

#[derive(Serialize)]
pub struct StatusResponse {
    pub indexed_count: usize,
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
}

pub struct QueryServer {
    pub index: Arc<TieredIndex>,
    config: QueryServerConfig,
}

impl QueryServer {
    pub fn new(index: Arc<TieredIndex>) -> Self {
        Self {
            index,
            config: QueryServerConfig::default(),
        }
    }

    pub async fn run(self, port: u16) -> anyhow::Result<()> {
        let state = QueryServerState {
            index: self.index,
            config: self.config,
        };
        let app = Router::new()
            .route("/search", get(search_handler))
            .route("/status", get(status_handler))
            .with_state(state);

        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
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

    let search_task =
        tokio::task::spawn_blocking(move || execute_query(index.as_ref(), &keyword, limit, mode));
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

    let response = results
        .into_iter()
        .map(|m| SearchResult {
            path: m.path.to_string_lossy().into_owned(),
            size: m.size,
        })
        .collect();

    Ok(Json(response))
}

async fn status_handler(State(state): State<QueryServerState>) -> Json<StatusResponse> {
    Json(StatusResponse {
        indexed_count: state.index.file_count(),
    })
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
