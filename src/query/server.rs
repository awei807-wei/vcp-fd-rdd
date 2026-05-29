use crate::diagnostics::{DiagnosticReport, DiagnosticSource};
use crate::event::tiered_watch::TieredWatchDebugDump;
use crate::index::TieredIndex;
use crate::query::scoring::{compute_highlights, score_result, ScoreConfig};
use crate::query::{execute_query_with_metadata_result, QueryMode, SortColumn, SortOrder};
use crate::security::{effective_http_policy, http_policy_label, HttpPolicy, RunningIdentity};
use crate::stats::{EventPipelineStats, MemoryReport, StatsReport, WatchStateReport};
use crate::storage::recovery_audit::RecoveryAuditReport;
use crate::util::maybe_trim_rss;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

const DEFAULT_SEARCH_LIMIT: usize = 100;
const MAX_SEARCH_LIMIT: usize = 10_000;
const SEARCH_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, Default)]
pub struct HealthTelemetry {
    pub last_snapshot_time: u64,
    pub watch_enabled: bool,
    pub watch_failures: u64,
    pub watcher_degraded: bool,
    pub degraded_roots: usize,
    pub event_watcher_degraded: bool,
    pub event_degraded_roots: usize,
    pub tiered_degraded: bool,
    pub tiered_unwatched_dirs: usize,
    pub overflow_drops: u64,
    pub rescan_signals: u64,
    pub snapshot_source: String,
    pub wal_events_replayed: usize,
    pub wal_sealed_used: usize,
    pub wal_truncated_tail_records: usize,
    pub wal_gap_detected: bool,
    pub wal_checkpoint_used: u64,
    pub wal_durability: String,
    pub wal_sync_interval_ms: u64,
    pub wal_sync_batch_records: usize,
    pub recovery_requires_repair: bool,
    pub recovery_requires_rebuild: bool,
    pub recovery_reasons: Vec<String>,
    pub recovery_audit: RecoveryAuditReport,
    pub startup_repair_ran: bool,
    pub startup_repair_escalated: bool,
    pub startup_repair_scanned: usize,
    pub startup_repair_changed: usize,
    pub last_clean_shutdown: bool,
    pub l1_dirs: usize,
    pub l2_dirs: usize,
    pub l3_dirs: usize,
    pub max_watch_dirs: usize,
    pub watch_budget_utilization_pct: u8,
    pub promotion_budget_blocked: u64,
    pub watch_profile: String,
    pub system_max_user_watches: usize,
    pub required_watch_cost: u64,
    pub watch_budget_shortfall: u64,
    pub strict_coverage_ok: bool,
    pub strict_coverage_failure: bool,
    pub strict_fail_on_budget_exceeded: bool,
    pub strict_uncovered_dirs: Vec<String>,
    pub diagnostics: DiagnosticReport,
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
    pub score: i64,
    pub highlights: Vec<[usize; 2]>,
    pub freshness: String,
    pub index_tier: String,
    pub validated: bool,
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
    pub is_rebuilding: bool,
}

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub index_health: &'static str,
    pub uptime_secs: u64,
    pub index_entries: usize,
    pub version: &'static str,
    pub last_snapshot_time: u64,
    pub watch_enabled: bool,
    pub watch_failures: u64,
    pub watcher_degraded: bool,
    pub degraded_roots: usize,
    pub event_watcher_degraded: bool,
    pub event_degraded_roots: usize,
    pub tiered_degraded: bool,
    pub tiered_unwatched_dirs: usize,
    pub overflow_drops: u64,
    pub rescan_signals: u64,
    pub snapshot_source: String,
    pub wal_events_replayed: usize,
    pub wal_sealed_used: usize,
    pub wal_truncated_tail_records: usize,
    pub wal_gap_detected: bool,
    pub wal_checkpoint_used: u64,
    pub wal_durability: String,
    pub wal_sync_interval_ms: u64,
    pub wal_sync_batch_records: usize,
    pub recovery_requires_repair: bool,
    pub recovery_requires_rebuild: bool,
    pub recovery_reasons: Vec<String>,
    pub recovery_audit: RecoveryAuditReport,
    pub startup_repair_ran: bool,
    pub startup_repair_escalated: bool,
    pub startup_repair_scanned: usize,
    pub startup_repair_changed: usize,
    pub last_clean_shutdown: bool,
    pub l1_dirs: usize,
    pub l2_dirs: usize,
    pub l3_dirs: usize,
    pub max_watch_dirs: usize,
    pub watch_budget_utilization_pct: u8,
    pub promotion_budget_blocked: u64,
    pub watch_profile: String,
    pub system_max_user_watches: usize,
    pub required_watch_cost: u64,
    pub watch_budget_shortfall: u64,
    pub strict_coverage_ok: bool,
    pub strict_coverage_failure: bool,
    pub strict_fail_on_budget_exceeded: bool,
    pub strict_uncovered_dirs: Vec<String>,
    pub diagnostics: DiagnosticReport,
    pub issues: Vec<String>,
}

#[derive(Serialize)]
pub struct TrimResponse {
    pub rss_before_bytes: u64,
    pub rss_after_bytes: u64,
    pub reclaimed_bytes: u64,
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
    stats_provider: Arc<dyn Fn() -> EventPipelineStats + Send + Sync>,
    watch_state_provider: Arc<dyn Fn() -> WatchStateReport + Send + Sync>,
    tiered_watch_debug_provider: Arc<dyn Fn(Option<String>) -> TieredWatchDebugDump + Send + Sync>,
    scan_reject_count: Arc<AtomicU64>,
    http_policy: HttpPolicy,
}

pub struct QueryServer {
    pub index: Arc<TieredIndex>,
    config: QueryServerConfig,
    health_provider: Arc<dyn Fn() -> HealthTelemetry + Send + Sync>,
    stats_provider: Arc<dyn Fn() -> EventPipelineStats + Send + Sync>,
    watch_state_provider: Arc<dyn Fn() -> WatchStateReport + Send + Sync>,
    tiered_watch_debug_provider: Arc<dyn Fn(Option<String>) -> TieredWatchDebugDump + Send + Sync>,
    scan_reject_count: Arc<AtomicU64>,
    http_policy: HttpPolicy,
}

impl QueryServer {
    pub fn new(index: Arc<TieredIndex>) -> Self {
        Self {
            index,
            config: QueryServerConfig::default(),
            health_provider: Arc::new(HealthTelemetry::default),
            stats_provider: Arc::new(EventPipelineStats::default),
            watch_state_provider: Arc::new(WatchStateReport::default),
            tiered_watch_debug_provider: Arc::new(|_| TieredWatchDebugDump::default()),
            scan_reject_count: Arc::new(AtomicU64::new(0)),
            http_policy: effective_http_policy(None, RunningIdentity::current()),
        }
    }

    pub fn with_health_provider(
        mut self,
        provider: Arc<dyn Fn() -> HealthTelemetry + Send + Sync>,
    ) -> Self {
        self.health_provider = provider;
        self
    }

    pub fn with_stats_provider(
        mut self,
        provider: Arc<dyn Fn() -> EventPipelineStats + Send + Sync>,
    ) -> Self {
        self.stats_provider = provider;
        self
    }

    pub fn with_watch_state_provider(
        mut self,
        provider: Arc<dyn Fn() -> WatchStateReport + Send + Sync>,
    ) -> Self {
        self.watch_state_provider = provider;
        self
    }

    pub fn with_tiered_watch_debug_provider(
        mut self,
        provider: Arc<dyn Fn(Option<String>) -> TieredWatchDebugDump + Send + Sync>,
    ) -> Self {
        self.tiered_watch_debug_provider = provider;
        self
    }

    pub fn with_http_policy(mut self, policy: HttpPolicy) -> Self {
        self.http_policy = policy;
        self
    }

    pub async fn run(self, port: u16) -> anyhow::Result<()> {
        if self.http_policy == HttpPolicy::Disabled {
            tracing::warn!("HTTP query server disabled by security policy");
            return Ok(());
        }
        let state = QueryServerState {
            index: self.index,
            config: self.config,
            start_time: Instant::now(),
            health_provider: self.health_provider,
            stats_provider: self.stats_provider,
            watch_state_provider: self.watch_state_provider,
            tiered_watch_debug_provider: self.tiered_watch_debug_provider,
            scan_reject_count: self.scan_reject_count,
            http_policy: self.http_policy,
        };
        let app = Router::new()
            .route("/search", get(search_handler))
            .route("/status", get(status_handler))
            .route("/health", get(health_handler))
            .route("/memory", get(memory_handler))
            .route("/watch-state", get(watch_state_handler))
            .route("/trim", get(trim_handler).post(trim_handler))
            .route("/metrics", get(metrics_handler))
            .route("/scan", post(scan_handler))
            .route("/debug/tiered-watch", get(debug_tiered_watch_handler))
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

    let query_started = Instant::now();
    let kw_clone = keyword.clone();
    let sort = SortColumn::parse_strict(params.sort.as_deref())
        .map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let order = SortOrder::parse(params.order.as_deref());
    let search_task = tokio::task::spawn_blocking(move || {
        execute_query_with_metadata_result(index.as_ref(), &kw_clone, limit, mode, sort, order)
    });
    let results = match tokio::time::timeout(state.config.query_timeout, search_task).await {
        Ok(Ok(Ok(results))) => results,
        Ok(Ok(Err(e))) => return Err((StatusCode::BAD_REQUEST, e.to_string())),
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
    state
        .index
        .record_query_metric(query_started.elapsed().as_micros() as u64);

    let config = ScoreConfig::from_query(&keyword);
    let response = results
        .into_iter()
        .map(|result| {
            let path_str = result.meta.path.to_string_lossy().into_owned();
            let score = score_result(&result.meta, &config);
            let highlights = compute_highlights(&path_str, &keyword);
            SearchResult {
                path: path_str,
                score,
                highlights,
                freshness: result.freshness.as_str().to_string(),
                index_tier: result.index_tier.as_str().to_string(),
                validated: result.validated,
            }
        })
        .collect();

    Ok(Json(response))
}

async fn status_handler(State(state): State<QueryServerState>) -> Json<StatusResponse> {
    Json(StatusResponse {
        indexed_count: state.index.file_count(),
        is_rebuilding: state.index.rebuild_in_progress(),
    })
}

async fn health_handler(State(state): State<QueryServerState>) -> Json<HealthResponse> {
    let uptime = state.start_time.elapsed().as_secs();
    let health = (state.health_provider)();
    let mut issues = Vec::new();
    if !health.watch_enabled {
        issues.push("watcher_disabled".to_string());
    } else if health.event_watcher_degraded {
        issues.push(format!(
            "event_watcher_degraded: {} roots are using fallback polling",
            health.event_degraded_roots
        ));
    }
    if health.tiered_degraded {
        issues.push(format!(
            "tiered_non_l0_dirs: {} directories are intentionally outside L0",
            health.tiered_unwatched_dirs
        ));
    }
    if health.strict_coverage_failure {
        issues.push(format!(
            "strict_coverage_incomplete: required_watch_cost={} max_watch_dirs={} shortfall={} uncovered={:?}",
            health.required_watch_cost,
            health.max_watch_dirs,
            health.watch_budget_shortfall,
            health.strict_uncovered_dirs
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
    if health.promotion_budget_blocked > 0 {
        issues.push(format!(
            "watch_budget: {} promotion attempt(s) blocked",
            health.promotion_budget_blocked
        ));
    }
    if health.watch_budget_utilization_pct >= 90 {
        issues.push(format!(
            "watch_budget: utilization={}%",
            health.watch_budget_utilization_pct
        ));
    }
    if health.wal_truncated_tail_records > 0 {
        issues.push(format!(
            "wal_recovery: truncated_tail_records={}",
            health.wal_truncated_tail_records
        ));
    }
    if health.wal_gap_detected {
        issues.push("wal_recovery: gap_detected=true".to_string());
    }
    if health.recovery_requires_rebuild {
        issues.push("recovery_audit: rebuild_required".to_string());
    } else if health.recovery_requires_repair {
        issues.push("recovery_audit: repair_required".to_string());
    }
    if health.startup_repair_escalated {
        issues.push("startup_repair: escalated to rebuild policy".to_string());
    }
    if health.last_snapshot_time == 0 {
        issues.push("snapshot_not_written_yet".to_string());
    }
    let index_health = if !health.watch_enabled {
        "static"
    } else if health.event_watcher_degraded
        || (health.strict_coverage_failure && health.strict_fail_on_budget_exceeded)
    {
        "degraded"
    } else if issues.is_empty() {
        "ok"
    } else {
        "warning"
    };

    let mut diagnostics = health.diagnostics.clone();
    state.index.collect(&mut diagnostics);
    diagnostics.system.version = env!("CARGO_PKG_VERSION").to_string();
    diagnostics.system.allocator = crate::ALLOCATOR_KIND.to_string();
    diagnostics.system.uptime_secs = uptime;
    diagnostics.storage.snapshot_source = health.snapshot_source.clone();
    diagnostics.storage.wal_events_replayed = health.wal_events_replayed;
    diagnostics.storage.wal_sealed_used = health.wal_sealed_used;
    diagnostics.storage.wal_truncated_tail_records = health.wal_truncated_tail_records;
    diagnostics.storage.wal_gap_detected = health.wal_gap_detected;
    diagnostics.storage.wal_checkpoint_used = health.wal_checkpoint_used;
    diagnostics.storage.wal_durability = health.wal_durability.clone();
    diagnostics.security.http_policy = http_policy_label(state.http_policy).to_string();
    diagnostics.security.scan_reject_count = state.scan_reject_count.load(Ordering::Relaxed);
    let identity = RunningIdentity::current();
    diagnostics.security.multi_user_risk = identity.multi_user_risk();
    diagnostics.watchers.denied_mount_count = diagnostics
        .watchers
        .denied_mount_count
        .saturating_add(health.watch_failures);
    diagnostics.watchers.fstype_blocked_count = diagnostics
        .watchers
        .fstype_blocked_count
        .saturating_add(health.event_degraded_roots as u64);

    Json(HealthResponse {
        status: "ok",
        index_health,
        uptime_secs: uptime,
        index_entries: state.index.file_count(),
        version: env!("CARGO_PKG_VERSION"),
        last_snapshot_time: health.last_snapshot_time,
        watch_enabled: health.watch_enabled,
        watch_failures: health.watch_failures,
        watcher_degraded: health.watcher_degraded,
        degraded_roots: health.degraded_roots,
        event_watcher_degraded: health.event_watcher_degraded,
        event_degraded_roots: health.event_degraded_roots,
        tiered_degraded: health.tiered_degraded,
        tiered_unwatched_dirs: health.tiered_unwatched_dirs,
        overflow_drops: health.overflow_drops,
        rescan_signals: health.rescan_signals,
        snapshot_source: health.snapshot_source,
        wal_events_replayed: health.wal_events_replayed,
        wal_sealed_used: health.wal_sealed_used,
        wal_truncated_tail_records: health.wal_truncated_tail_records,
        wal_gap_detected: health.wal_gap_detected,
        wal_checkpoint_used: health.wal_checkpoint_used,
        wal_durability: health.wal_durability,
        wal_sync_interval_ms: health.wal_sync_interval_ms,
        wal_sync_batch_records: health.wal_sync_batch_records,
        recovery_requires_repair: health.recovery_requires_repair,
        recovery_requires_rebuild: health.recovery_requires_rebuild,
        recovery_reasons: health.recovery_reasons,
        recovery_audit: health.recovery_audit,
        startup_repair_ran: health.startup_repair_ran,
        startup_repair_escalated: health.startup_repair_escalated,
        startup_repair_scanned: health.startup_repair_scanned,
        startup_repair_changed: health.startup_repair_changed,
        last_clean_shutdown: health.last_clean_shutdown,
        l1_dirs: health.l1_dirs,
        l2_dirs: health.l2_dirs,
        l3_dirs: health.l3_dirs,
        max_watch_dirs: health.max_watch_dirs,
        watch_budget_utilization_pct: health.watch_budget_utilization_pct,
        promotion_budget_blocked: health.promotion_budget_blocked,
        watch_profile: health.watch_profile,
        system_max_user_watches: health.system_max_user_watches,
        required_watch_cost: health.required_watch_cost,
        watch_budget_shortfall: health.watch_budget_shortfall,
        strict_coverage_ok: health.strict_coverage_ok,
        strict_coverage_failure: health.strict_coverage_failure,
        strict_fail_on_budget_exceeded: health.strict_fail_on_budget_exceeded,
        strict_uncovered_dirs: health.strict_uncovered_dirs,
        diagnostics,
        issues,
    })
}

async fn metrics_handler(State(state): State<QueryServerState>) -> impl IntoResponse {
    let mut report: StatsReport = state.index.stats_report();
    let pipeline = (state.stats_provider)();
    report.events_dropped = report
        .events_dropped
        .saturating_add(pipeline.overflow_drops);
    Json(report)
}

async fn memory_handler(State(state): State<QueryServerState>) -> Json<MemoryReport> {
    Json(state.index.memory_report((state.stats_provider)()))
}

async fn watch_state_handler(State(state): State<QueryServerState>) -> Json<WatchStateReport> {
    Json((state.watch_state_provider)())
}

async fn trim_handler() -> Json<TrimResponse> {
    let before = MemoryReport::read_process_rss();
    maybe_trim_rss();
    let after = MemoryReport::read_process_rss();
    Json(TrimResponse {
        rss_before_bytes: before,
        rss_after_bytes: after,
        reclaimed_bytes: before.saturating_sub(after),
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
    for dir in &dirs {
        if !crate::security::path_within_roots(dir, &state.index.roots) {
            state.scan_reject_count.fetch_add(1, Ordering::Relaxed);
            return Err((
                StatusCode::FORBIDDEN,
                format!("scan path is outside configured roots: {}", dir.display()),
            ));
        }
    }

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

#[derive(Deserialize)]
pub struct DebugTieredWatchParams {
    pub root: Option<String>,
}

async fn debug_tiered_watch_handler(
    Query(params): Query<DebugTieredWatchParams>,
    State(state): State<QueryServerState>,
) -> Json<TieredWatchDebugDump> {
    Json((state.tiered_watch_debug_provider)(params.root))
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

    #[test]
    fn strict_sort_rejects_removed_size_column() {
        let err = SortColumn::parse_strict(Some("size")).unwrap_err();
        assert!(err.contains("unsupported sort column"));
    }

    #[test]
    fn search_result_serializes_cold_validation_fields() {
        let value = serde_json::to_value(SearchResult {
            path: "/tmp/a.txt".to_string(),
            score: 10,
            highlights: vec![[0, 3]],
            freshness: "stale_checked".to_string(),
            index_tier: "ColdMmap".to_string(),
            validated: true,
        })
        .unwrap();

        assert_eq!(value["freshness"], "stale_checked");
        assert_eq!(value["index_tier"], "ColdMmap");
        assert_eq!(value["validated"], true);
    }
}
