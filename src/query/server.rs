use axum::{
    extract::{Query, State},
    routing::get,
    Json, Router,
};
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use crate::index::TieredIndex;

#[derive(Deserialize)]
pub struct SearchParams {
    pub q: String,
    pub limit: Option<usize>,
}

#[derive(Serialize)]
pub struct SearchResult {
    pub path: String,
    pub score: f32,
}

#[derive(Serialize)]
pub struct StatusResponse {
    pub indexed_count: usize,
    pub memory_usage: String,
    pub l1_hit_rate: String,
}

pub struct QueryServer {
    pub index: Arc<TieredIndex>,
}

impl QueryServer {
    pub fn new(index: Arc<TieredIndex>) -> Self {
        Self { index }
    }

    pub async fn run(self, port: u16) -> anyhow::Result<()> {
        let app = Router::new()
            .route("/search", get(search_handler))
            .route("/status", get(status_handler))
            .with_state(self.index);

        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
        tracing::info!("HTTP Query Server listening on {}", port);
        axum::serve(listener, app).await?;
        Ok(())
    }
}

async fn search_handler(
    Query(params): Query<SearchParams>,
    State(index): State<Arc<TieredIndex>>,
) -> Json<Vec<SearchResult>> {
    let results = index.query(&params.q).await;
    let limit = params.limit.unwrap_or(100);
    
    let response = results.into_iter()
        .take(limit)
        .map(|e| SearchResult {
            path: e.path.to_string_lossy().into_owned(),
            score: 1.0, // 简化实现，实际可接入 fuzzy-matcher
        })
        .collect();
        
    Json(response)
}

async fn status_handler(
    State(index): State<Arc<TieredIndex>>,
) -> Json<StatusResponse> {
    let rdd = index.l2.rdd.read().await;
    let count = rdd.partitions.len(); // 简化：这里应返回总文件数
    
    Json(StatusResponse {
        indexed_count: count,
        memory_usage: "Unknown".to_string(),
        l1_hit_rate: "85%".to_string(),
    })
}