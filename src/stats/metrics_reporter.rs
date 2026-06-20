use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;

use crate::index::tiered::RecoveryReasonCount;
use crate::query::HealthTelemetry;
use crate::stats::{
    EventPipelineStats, MemoryReport, MemorySampleDepth, StatsReport, WatchStateReport,
};
use crate::util::unix_secs;

/// 定期采集统一诊断指标并输出到按小时划分的 JSON Lines 文件。
pub struct MetricsReporter {
    provider: Arc<dyn Fn() -> MetricsSnapshot + Send + Sync>,
    output_dir: PathBuf,
    interval_secs: u64,
}

#[derive(serde::Serialize)]
struct MetricsEntry {
    timestamp: String,
    #[serde(flatten)]
    snapshot: MetricsSnapshot,
}

/// 文件上报的单条诊断快照。
///
/// `watch` 使用 flatten 保持旧版 JSONL 顶层 `/watch-state` 字段兼容，
/// 因此已有 `jq '.dirty_queue_len'` 之类查询不需要改。
#[derive(Debug, serde::Serialize)]
pub struct MetricsSnapshot {
    #[serde(flatten)]
    pub watch: WatchStateReport,
    pub runtime: MetricsRuntimeSnapshot,
    pub memory: MetricsMemorySnapshot,
    pub health: MetricsHealthSnapshot,
    pub diagnostics: MetricsDiagnostics,
}

#[derive(Debug, Default, serde::Serialize)]
pub struct MetricsRuntimeSnapshot {
    pub queries_total: u64,
    pub queries_avg_us: u64,
    pub query_guard_active_count: u64,
    pub query_guard_hold_count: u64,
    pub query_guard_hold_avg_us: u64,
    pub query_guard_hold_max_us: u64,
    pub query_guard_last_hold_us: u64,
    pub query_guard_slow_count: u64,
    pub exact_queries_total: u64,
    pub fuzzy_queries_total: u64,
    pub query_no_trigram_hint_count: u64,
    pub fuzzy_full_scan_count: u64,
    pub fuzzy_full_scan_candidates_total: u64,
    pub fuzzy_full_scan_elapsed_us_total: u64,
    pub fuzzy_full_scan_last_candidates: u64,
    pub fuzzy_full_scan_last_elapsed_us: u64,
    pub cold_validate_count: u64,
    pub query_stale_hit_count: u64,
    pub query_permission_denied_count: u64,
    pub events_applied: u64,
    pub events_dropped: u64,
    pub snapshot_count: u64,
    pub fast_sync_count: u64,
    pub refresh_base_count: u64,
    pub event_pipeline_last_batch_size: usize,
    pub event_pipeline_total_events_processed: u64,
    pub event_pipeline_overflow_drops: u64,
    pub event_pipeline_rescan_signals: u64,
    pub event_pipeline_watch_failures: u64,
}

#[derive(Debug, Default, serde::Serialize)]
pub struct MetricsMemorySnapshot {
    pub sample_depth: MemorySampleDepth,
    pub cache_hit: bool,
    pub cached_full_age_ms: u64,
    pub full_sample_elapsed_ms: u64,
    pub process_rss_bytes: u64,
    pub process_swap_bytes: u64,
    pub process_rss_plus_swap_bytes: u64,
    pub process_pss_bytes: Option<u64>,
    pub process_private_dirty_bytes: Option<u64>,
    pub process_major_faults: Option<u64>,
    pub index_estimated_bytes: u64,
    pub base_estimated_bytes: u64,
    pub base_hot_memory_entries: usize,
    pub base_manifest_only_entries: usize,
    pub base_cold_segment_count: usize,
    pub base_cold_mmap_bytes: u64,
    pub generation_base_strong_refs: usize,
    pub generation_l2_strong_refs: usize,
    pub dirty_queue_pending_scopes: usize,
    pub dirty_queue_pending_dirs: usize,
    pub dirty_queue_estimated_bytes: u64,
    pub l1_estimated_bytes: u64,
    pub l2_estimated_bytes: u64,
    pub overlay_estimated_bytes: u64,
    pub overlay_deleted_paths: usize,
    pub overlay_upserted_paths: usize,
    pub heap_high_water_suspected: bool,
}

#[derive(Debug, Default, serde::Serialize)]
pub struct MetricsHealthSnapshot {
    pub watch_profile: String,
    pub watch_enabled: bool,
    pub watcher_degraded: bool,
    pub degraded_roots: usize,
    pub event_watcher_degraded: bool,
    pub event_degraded_roots: usize,
    pub tiered_degraded: bool,
    pub tiered_unwatched_dirs: usize,
    pub max_watch_dirs: usize,
    pub l0_max_cost_per_root: usize,
    pub system_max_user_watches: usize,
    pub required_watch_cost: u64,
    pub watch_budget_shortfall: u64,
    pub strict_coverage_ok: bool,
    pub strict_coverage_failure: bool,
    pub strict_fail_on_budget_exceeded: bool,
    pub strict_uncovered_dirs: Vec<String>,
    pub fast_scan_enabled: bool,
    pub fast_scan_sla_ok: bool,
    pub fast_scan_local_strict_ok: bool,
    pub fast_scan_known_dirs: usize,
    pub fast_scan_local_trusted_dirs: usize,
    pub fast_scan_untrusted_dirs: usize,
    pub fast_scan_hotset_lease_count: usize,
    pub fast_scan_hotset_sentinel_count: usize,
    pub fast_scan_explicit_lease_count: usize,
    pub fast_scan_auto_lease_count: usize,
    pub fast_scan_lease_evictions: u64,
    pub fast_scan_lease_renewals: u64,
    pub fast_scan_initial_backfill_pending: usize,
    pub fast_scan_real_changed_dirs: u64,
    pub fast_scan_apply_dropped_stale_batches: u64,
    pub fast_scan_scan_workers_active: u64,
    pub fast_scan_io_budget_limited_count: u64,
    pub fast_scan_coverage_lag_p95_ms: u64,
    pub fast_scan_budget_degraded: bool,
    pub fast_scan_last_degraded_reason: String,
    pub proc_sampler_enabled: bool,
    pub proc_sampler_last_duration_ms: u64,
    pub proc_sampler_pids_seen: u64,
    pub proc_sampler_pids_scanned: u64,
    pub proc_sampler_pids_denied: u64,
    pub proc_sampler_fdinfo_read_count: u64,
    pub proc_sampler_readlink_count: u64,
    pub proc_sampler_write_fd_count: u64,
    pub proc_sampler_sampled_dirs: u64,
    pub proc_sampler_triggered_watches: u64,
    pub proc_sampler_budget_exhausted: bool,
    pub proc_sampler_unavailable: bool,
    pub watch_failures: u64,
    pub overflow_drops: u64,
    pub rescan_signals: u64,
    pub last_snapshot_time: u64,
    pub snapshot_source: String,
    pub wal_events_replayed: usize,
    pub wal_sealed_used: usize,
    pub wal_truncated_tail_records: usize,
    pub wal_gap_detected: bool,
    pub wal_checkpoint_used: u64,
    pub wal_durability: String,
    pub startup_scan_required: bool,
    pub deferred_repair: bool,
    pub deferred_dirty_dir_count: usize,
    pub deferred_unknown_scope: bool,
    pub wal_tail_dirty_dir_count: usize,
    pub deferred_repair_queue_len: usize,
    pub cold_sweep_last_completed: u64,
    pub cold_sweep_period_estimate: u64,
    pub dirty_backlog: usize,
    pub lazy_validation_pending: usize,
    pub lazy_validation_rate_limited: u64,
    pub lazy_validation_cache_hits: u64,
    pub lazy_validation_queue_full: u64,
    pub lazy_validation_completed: u64,
    pub lazy_validation_stale_hits: u64,
    pub recovery_requires_repair: bool,
    pub recovery_requires_rebuild: bool,
    pub recovery_soft_repair_needed: bool,
    pub recovery_hard_rebuild_needed: bool,
    pub recovery_soft_reasons: Vec<String>,
    pub recovery_hard_reasons: Vec<String>,
    pub recovery_reason_counts: Vec<RecoveryReasonCount>,
    pub startup_repair_ran: bool,
    pub startup_repair_escalated: bool,
    pub startup_repair_scanned: usize,
    pub startup_repair_changed: usize,
    pub startup_repair_budget_ms: u64,
    pub startup_repair_budget_exhausted: bool,
    pub startup_repair_escalation_reason: String,
    pub last_clean_shutdown: bool,
}

impl From<&HealthTelemetry> for MetricsHealthSnapshot {
    fn from(h: &HealthTelemetry) -> Self {
        Self {
            watch_profile: h.watch_profile.clone(),
            watch_enabled: h.watch_enabled,
            watcher_degraded: h.watcher_degraded,
            degraded_roots: h.degraded_roots,
            event_watcher_degraded: h.event_watcher_degraded,
            event_degraded_roots: h.event_degraded_roots,
            tiered_degraded: h.tiered_degraded,
            tiered_unwatched_dirs: h.tiered_unwatched_dirs,
            max_watch_dirs: h.max_watch_dirs,
            l0_max_cost_per_root: h.l0_max_cost_per_root,
            system_max_user_watches: h.system_max_user_watches,
            required_watch_cost: h.required_watch_cost,
            watch_budget_shortfall: h.watch_budget_shortfall,
            strict_coverage_ok: h.strict_coverage_ok,
            strict_coverage_failure: h.strict_coverage_failure,
            strict_fail_on_budget_exceeded: h.strict_fail_on_budget_exceeded,
            strict_uncovered_dirs: h.strict_uncovered_dirs.clone(),
            fast_scan_enabled: h.fast_scan_enabled,
            fast_scan_sla_ok: h.fast_scan_sla_ok,
            fast_scan_local_strict_ok: h.fast_scan_local_strict_ok,
            fast_scan_known_dirs: h.fast_scan_known_dirs,
            fast_scan_local_trusted_dirs: h.fast_scan_local_trusted_dirs,
            fast_scan_untrusted_dirs: h.fast_scan_untrusted_dirs,
            fast_scan_hotset_lease_count: h.fast_scan_hotset_lease_count,
            fast_scan_hotset_sentinel_count: h.fast_scan_hotset_sentinel_count,
            fast_scan_explicit_lease_count: h.fast_scan_explicit_lease_count,
            fast_scan_auto_lease_count: h.fast_scan_auto_lease_count,
            fast_scan_lease_evictions: h.fast_scan_lease_evictions,
            fast_scan_lease_renewals: h.fast_scan_lease_renewals,
            fast_scan_initial_backfill_pending: h.fast_scan_initial_backfill_pending,
            fast_scan_real_changed_dirs: h.fast_scan_real_changed_dirs,
            fast_scan_apply_dropped_stale_batches: h.fast_scan_apply_dropped_stale_batches,
            fast_scan_scan_workers_active: h.fast_scan_scan_workers_active,
            fast_scan_io_budget_limited_count: h.fast_scan_io_budget_limited_count,
            fast_scan_coverage_lag_p95_ms: h.fast_scan_coverage_lag_p95_ms,
            fast_scan_budget_degraded: h.fast_scan_budget_degraded,
            fast_scan_last_degraded_reason: h.fast_scan_last_degraded_reason.clone(),
            proc_sampler_enabled: h.proc_sampler_enabled,
            proc_sampler_last_duration_ms: h.proc_sampler_last_duration_ms,
            proc_sampler_pids_seen: h.proc_sampler_pids_seen,
            proc_sampler_pids_scanned: h.proc_sampler_pids_scanned,
            proc_sampler_pids_denied: h.proc_sampler_pids_denied,
            proc_sampler_fdinfo_read_count: h.proc_sampler_fdinfo_read_count,
            proc_sampler_readlink_count: h.proc_sampler_readlink_count,
            proc_sampler_write_fd_count: h.proc_sampler_write_fd_count,
            proc_sampler_sampled_dirs: h.proc_sampler_sampled_dirs,
            proc_sampler_triggered_watches: h.proc_sampler_triggered_watches,
            proc_sampler_budget_exhausted: h.proc_sampler_budget_exhausted,
            proc_sampler_unavailable: h.proc_sampler_unavailable,
            watch_failures: h.watch_failures,
            overflow_drops: h.overflow_drops,
            rescan_signals: h.rescan_signals,
            last_snapshot_time: h.last_snapshot_time,
            snapshot_source: h.snapshot_source.clone(),
            wal_events_replayed: h.wal_events_replayed,
            wal_sealed_used: h.wal_sealed_used,
            wal_truncated_tail_records: h.wal_truncated_tail_records,
            wal_gap_detected: h.wal_gap_detected,
            wal_checkpoint_used: h.wal_checkpoint_used,
            wal_durability: h.wal_durability.clone(),
            startup_scan_required: h.startup_scan_required,
            deferred_repair: h.deferred_repair,
            deferred_dirty_dir_count: h.deferred_dirty_dir_count,
            deferred_unknown_scope: h.deferred_unknown_scope,
            wal_tail_dirty_dir_count: h.wal_tail_dirty_dir_count,
            deferred_repair_queue_len: h.deferred_repair_queue_len,
            cold_sweep_last_completed: h.cold_sweep_last_completed,
            cold_sweep_period_estimate: h.cold_sweep_period_estimate,
            dirty_backlog: h.dirty_backlog,
            lazy_validation_pending: h.lazy_validation_pending,
            lazy_validation_rate_limited: h.lazy_validation_rate_limited,
            lazy_validation_cache_hits: h.lazy_validation_cache_hits,
            lazy_validation_queue_full: h.lazy_validation_queue_full,
            lazy_validation_completed: h.lazy_validation_completed,
            lazy_validation_stale_hits: h.lazy_validation_stale_hits,
            recovery_requires_repair: h.recovery_requires_repair,
            recovery_requires_rebuild: h.recovery_requires_rebuild,
            recovery_soft_repair_needed: h.recovery_soft_repair_needed,
            recovery_hard_rebuild_needed: h.recovery_hard_rebuild_needed,
            recovery_soft_reasons: h.recovery_soft_reasons.clone(),
            recovery_hard_reasons: h.recovery_hard_reasons.clone(),
            recovery_reason_counts: h.recovery_reason_counts.clone(),
            startup_repair_ran: h.startup_repair_ran,
            startup_repair_escalated: h.startup_repair_escalated,
            startup_repair_scanned: h.startup_repair_scanned,
            startup_repair_changed: h.startup_repair_changed,
            startup_repair_budget_ms: h.startup_repair_budget_ms,
            startup_repair_budget_exhausted: h.startup_repair_budget_exhausted,
            startup_repair_escalation_reason: h.startup_repair_escalation_reason.clone(),
            last_clean_shutdown: h.last_clean_shutdown,
        }
    }
}

#[derive(Debug, Default, serde::Serialize)]
pub struct MetricsDiagnostics {
    pub status: &'static str,
    pub data_freshness_ok: bool,
    pub event_loss_suspected: bool,
    pub watch_budget_near_limit: bool,
    pub rss_plus_swap_bytes: u64,
    pub non_l0_dirs: usize,
    pub issues: Vec<String>,
}

impl MetricsSnapshot {
    pub fn new(
        watch: WatchStateReport,
        runtime: MetricsRuntimeSnapshot,
        memory: MetricsMemorySnapshot,
        health: MetricsHealthSnapshot,
    ) -> Self {
        let diagnostics = MetricsDiagnostics::from_parts(&watch, &runtime, &memory, &health);
        Self {
            watch,
            runtime,
            memory,
            health,
            diagnostics,
        }
    }
}

impl MetricsRuntimeSnapshot {
    pub fn from_reports(stats: StatsReport, pipeline: EventPipelineStats) -> Self {
        Self {
            queries_total: stats.queries_total,
            queries_avg_us: stats.queries_avg_us,
            query_guard_active_count: stats.query_guard_active_count,
            query_guard_hold_count: stats.query_guard_hold_count,
            query_guard_hold_avg_us: stats.query_guard_hold_avg_us,
            query_guard_hold_max_us: stats.query_guard_hold_max_us,
            query_guard_last_hold_us: stats.query_guard_last_hold_us,
            query_guard_slow_count: stats.query_guard_slow_count,
            exact_queries_total: stats.exact_queries_total,
            fuzzy_queries_total: stats.fuzzy_queries_total,
            query_no_trigram_hint_count: stats.query_no_trigram_hint_count,
            fuzzy_full_scan_count: stats.fuzzy_full_scan_count,
            fuzzy_full_scan_candidates_total: stats.fuzzy_full_scan_candidates_total,
            fuzzy_full_scan_elapsed_us_total: stats.fuzzy_full_scan_elapsed_us_total,
            fuzzy_full_scan_last_candidates: stats.fuzzy_full_scan_last_candidates,
            fuzzy_full_scan_last_elapsed_us: stats.fuzzy_full_scan_last_elapsed_us,
            cold_validate_count: stats.cold_validate_count,
            query_stale_hit_count: stats.query_stale_hit_count,
            query_permission_denied_count: stats.query_permission_denied_count,
            events_applied: stats.events_applied,
            events_dropped: stats.events_dropped,
            snapshot_count: stats.snapshot_count,
            fast_sync_count: stats.fast_sync_count,
            refresh_base_count: stats.refresh_base_count,
            event_pipeline_last_batch_size: pipeline.last_batch_size,
            event_pipeline_total_events_processed: pipeline.total_events_processed,
            event_pipeline_overflow_drops: pipeline.overflow_drops,
            event_pipeline_rescan_signals: pipeline.rescan_signals,
            event_pipeline_watch_failures: pipeline.watch_failures,
        }
    }
}

impl MetricsMemorySnapshot {
    pub fn from_report(report: &MemoryReport) -> Self {
        Self {
            sample_depth: report.sample_depth,
            cache_hit: report.cache_hit,
            cached_full_age_ms: report.cached_full_age_ms,
            full_sample_elapsed_ms: report.full_sample_elapsed_ms,
            process_rss_bytes: report.process_rss_bytes,
            process_swap_bytes: report.process_swap_bytes,
            process_rss_plus_swap_bytes: report
                .process_rss_bytes
                .saturating_add(report.process_swap_bytes),
            process_pss_bytes: report.process_smaps_rollup.as_ref().map(|s| s.pss_bytes),
            process_private_dirty_bytes: report
                .process_smaps_rollup
                .as_ref()
                .map(|s| s.private_dirty_bytes),
            process_major_faults: report.process_faults.as_ref().map(|f| f.majflt),
            index_estimated_bytes: report.index_estimated_bytes,
            base_estimated_bytes: report.base.estimated_bytes,
            base_hot_memory_entries: report.base.hot_memory_entries,
            base_manifest_only_entries: report.base.manifest_only_entries,
            base_cold_segment_count: report.base.cold_segment_count,
            base_cold_mmap_bytes: report.base.cold_mmap_bytes,
            generation_base_strong_refs: report.generation.base_strong_refs,
            generation_l2_strong_refs: report.generation.l2_strong_refs,
            dirty_queue_pending_scopes: report.dirty_queue.pending_scopes,
            dirty_queue_pending_dirs: report.dirty_queue.pending_dirs,
            dirty_queue_estimated_bytes: report.dirty_queue.estimated_bytes,
            l1_estimated_bytes: report.l1.estimated_bytes,
            l2_estimated_bytes: report.l2.estimated_bytes,
            overlay_estimated_bytes: report.overlay.estimated_bytes,
            overlay_deleted_paths: report.overlay.deleted_paths,
            overlay_upserted_paths: report.overlay.upserted_paths,
            heap_high_water_suspected: report.heap_high_water_suspected,
        }
    }
}

impl MetricsDiagnostics {
    fn from_parts(
        watch: &WatchStateReport,
        runtime: &MetricsRuntimeSnapshot,
        memory: &MetricsMemorySnapshot,
        health: &MetricsHealthSnapshot,
    ) -> Self {
        let data_freshness_ok = watch.dirty_queue_len == 0
            && watch.query_stale_hit_count == 0
            && watch.stale_dirs == 0
            && watch.dirty_dirs == 0;
        let event_loss_suspected = runtime.events_dropped > 0
            || runtime.event_pipeline_overflow_drops > 0
            || runtime.event_pipeline_rescan_signals > 0
            || health.overflow_drops > 0
            || health.rescan_signals > 0;
        let watch_budget_near_limit = watch.watch_budget_utilization_pct >= 90;
        let non_l0_dirs = watch.l1_dirs + watch.l2_dirs + watch.l3_dirs;

        let mut issues = Vec::new();
        if watch.dirty_queue_len > 0 {
            issues.push(format!("dirty_queue_len={}", watch.dirty_queue_len));
        }
        if health.dirty_backlog > 0 && health.dirty_backlog != watch.dirty_queue_len {
            issues.push(format!("dirty_backlog={}", health.dirty_backlog));
        }
        if watch.query_stale_hit_count > 0 {
            issues.push(format!(
                "query_stale_hit_count={}",
                watch.query_stale_hit_count
            ));
        }
        if watch.query_permission_denied_count > 0 {
            issues.push(format!(
                "query_permission_denied_count={}",
                watch.query_permission_denied_count
            ));
        }
        if watch.stale_dirs > 0 || watch.dirty_dirs > 0 {
            issues.push(format!(
                "watch_freshness: stale_dirs={} dirty_dirs={}",
                watch.stale_dirs, watch.dirty_dirs
            ));
        }
        if event_loss_suspected {
            issues.push(format!(
                "event_recovery: dropped={} overflow={} rescan={}",
                runtime.events_dropped,
                runtime.event_pipeline_overflow_drops,
                runtime.event_pipeline_rescan_signals
            ));
        }
        if health.event_watcher_degraded {
            issues.push(format!(
                "event_watcher_degraded: {} roots",
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
                "strict_coverage_incomplete: required_watch_cost={} max_watch_dirs={} l0_max_cost_per_root={} shortfall={} uncovered={:?}",
                health.required_watch_cost,
                health.max_watch_dirs,
                health.l0_max_cost_per_root,
                health.watch_budget_shortfall,
                health.strict_uncovered_dirs
            ));
        }
        if health.fast_scan_enabled && !health.fast_scan_local_strict_ok {
            issues.push(format!(
                "fast_scan_local_strict_failed: lag_p95_ms={} known_dirs={} local_trusted_dirs={}",
                health.fast_scan_coverage_lag_p95_ms,
                health.fast_scan_known_dirs,
                health.fast_scan_local_trusted_dirs
            ));
        }
        if health.fast_scan_enabled && health.fast_scan_budget_degraded {
            issues.push(format!(
                "fast_scan_budget_degraded: {}",
                health.fast_scan_last_degraded_reason
            ));
        }
        if health.fast_scan_enabled && health.fast_scan_untrusted_dirs > 0 {
            issues.push(format!(
                "fast_scan_untrusted_dirs: {} best-effort directories",
                health.fast_scan_untrusted_dirs
            ));
        }
        if health.proc_sampler_enabled && health.proc_sampler_budget_exhausted {
            issues.push(format!(
                "proc_sampler_budget_exhausted: pids_seen={} sampled_dirs={}",
                health.proc_sampler_pids_seen, health.proc_sampler_sampled_dirs
            ));
        }
        if health.proc_sampler_enabled && health.proc_sampler_unavailable {
            issues.push("proc_sampler_unavailable=true".to_string());
        }
        if watch.eventually_consistent_dirs > 0 {
            issues.push(format!(
                "l3_consistency: eventually_consistent_dirs={} scanned_fresh_dirs={}",
                watch.eventually_consistent_dirs, watch.scanned_fresh_dirs
            ));
        }
        if watch_budget_near_limit {
            issues.push(format!(
                "watch_budget_near_limit: utilization={}%",
                watch.watch_budget_utilization_pct
            ));
        }
        if health.startup_repair_escalated
            && (health.recovery_requires_rebuild || health.recovery_hard_rebuild_needed)
        {
            issues.push("startup_repair_escalated=true".to_string());
        }
        if health.wal_gap_detected {
            issues.push("wal_gap_detected=true".to_string());
        }
        if health.recovery_requires_rebuild {
            issues.push("recovery_requires_rebuild=true".to_string());
        } else if health.recovery_requires_repair {
            issues.push("recovery_requires_repair=true".to_string());
        }
        if memory.heap_high_water_suspected {
            issues.push("heap_high_water_suspected=true".to_string());
        }
        if memory.process_swap_bytes > 0 {
            issues.push(format!("process_swap_bytes={}", memory.process_swap_bytes));
        }

        let status = if event_loss_suspected
            || health.event_watcher_degraded
            || (health.strict_coverage_failure && health.strict_fail_on_budget_exceeded)
            || (health.fast_scan_enabled && !health.fast_scan_local_strict_ok)
            || (health.fast_scan_enabled && health.fast_scan_budget_degraded)
            || watch.dirty_queue_len > 0
            || health.dirty_backlog > 0
            || watch.query_stale_hit_count > 0
        {
            "degraded"
        } else if issues.is_empty() {
            "ok"
        } else {
            "warning"
        };

        Self {
            status,
            data_freshness_ok,
            event_loss_suspected,
            watch_budget_near_limit,
            rss_plus_swap_bytes: memory.process_rss_plus_swap_bytes,
            non_l0_dirs,
            issues,
        }
    }
}

impl MetricsReporter {
    pub fn new(
        provider: Arc<dyn Fn() -> MetricsSnapshot + Send + Sync>,
        output_dir: PathBuf,
        interval_secs: u64,
    ) -> Self {
        Self {
            provider,
            output_dir,
            interval_secs,
        }
    }

    pub async fn run(self) {
        if let Err(e) = std::fs::create_dir_all(&self.output_dir) {
            tracing::error!(
                "Failed to create metrics output directory {}: {}",
                self.output_dir.display(),
                e
            );
            return;
        }

        let mut ticker = interval(Duration::from_secs(self.interval_secs));

        loop {
            ticker.tick().await;
            if let Err(e) = self.write_report().await {
                tracing::error!("Failed to write metrics report: {}", e);
            }
        }
    }

    async fn write_report(&self) -> io::Result<()> {
        let snapshot = (self.provider)();
        let now_secs = unix_secs();

        let timestamp = format_iso_timestamp(now_secs);
        let filename = format_filename(now_secs);
        let path = self.output_dir.join(&filename);

        let entry = MetricsEntry {
            timestamp,
            snapshot,
        };

        let line = serde_json::to_string(&entry)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let path_clone = path.clone();
        let result = tokio::task::spawn_blocking(move || {
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path_clone)?;
            writeln!(file, "{}", line)?;
            Ok::<(), io::Error>(())
        })
        .await;

        match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(e) => Err(io::Error::other(format!(
                "metrics write task panicked: {}",
                e
            ))),
        }
    }
}

fn format_iso_timestamp(secs: u64) -> String {
    let (y, m, d, hh, mm, ss) = unix_to_utc_datetime(secs);
    format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", y, m, d, hh, mm, ss)
}

fn format_filename(secs: u64) -> String {
    let (y, m, d, hh, _, _) = unix_to_utc_datetime(secs);
    format!("metrics_{:04}-{:02}-{:02}_{:02}.json", y, m, d, hh)
}

/// 将 Unix 秒数转换为 UTC 时间 (year, month, day, hour, minute, second)。
/// 使用 Howard Hinnant 的算法：http://howardhinnant.github.io/date_algorithms.html
fn unix_to_utc_datetime(secs: u64) -> (u64, u64, u64, u64, u64, u64) {
    let days = secs / 86400;
    let rem = secs % 86400;
    let hh = rem / 3600;
    let mm = (rem % 3600) / 60;
    let ss = rem % 60;

    let z = days + 719468;
    let era = z / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let mut y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    if m <= 2 {
        y += 1;
    }

    (y, m, d, hh, mm, ss)
}

#[cfg(test)]
mod tests {
    use super::{
        format_filename, format_iso_timestamp, unix_to_utc_datetime, MetricsHealthSnapshot,
        MetricsMemorySnapshot, MetricsRuntimeSnapshot, MetricsSnapshot,
    };
    use crate::stats::WatchStateReport;

    #[test]
    fn unix_epoch_is_1970_01_01() {
        let (y, m, d, hh, mm, ss) = unix_to_utc_datetime(0);
        assert_eq!(y, 1970);
        assert_eq!(m, 1);
        assert_eq!(d, 1);
        assert_eq!(hh, 0);
        assert_eq!(mm, 0);
        assert_eq!(ss, 0);
    }

    #[test]
    fn leap_year_feb_29() {
        // 1972-02-29 00:00:00 UTC
        let secs = 2 * 365 * 86400 + 31 * 86400 + 28 * 86400;
        let (y, m, d, hh, mm, ss) = unix_to_utc_datetime(secs as u64);
        assert_eq!(y, 1972);
        assert_eq!(m, 2);
        assert_eq!(d, 29);
        assert_eq!(hh, 0);
        assert_eq!(mm, 0);
        assert_eq!(ss, 0);
    }

    #[test]
    fn iso_timestamp_format() {
        let ts = format_iso_timestamp(0);
        assert_eq!(ts, "1970-01-01T00:00:00Z");
    }

    #[test]
    fn filename_format() {
        let name = format_filename(0);
        assert_eq!(name, "metrics_1970-01-01_00.json");
    }

    #[test]
    fn snapshot_keeps_watch_fields_flattened() {
        let watch = WatchStateReport {
            dirty_queue_len: 2,
            watch_budget_utilization_pct: 91,
            l3_dirs: 1,
            ..WatchStateReport::default()
        };

        let memory = MetricsMemorySnapshot {
            process_rss_bytes: 1024,
            process_swap_bytes: 2048,
            process_rss_plus_swap_bytes: 3072,
            ..MetricsMemorySnapshot::default()
        };
        let health = MetricsHealthSnapshot {
            watch_enabled: true,
            tiered_degraded: true,
            tiered_unwatched_dirs: 1,
            ..MetricsHealthSnapshot::default()
        };

        let snapshot =
            MetricsSnapshot::new(watch, MetricsRuntimeSnapshot::default(), memory, health);
        let value = serde_json::to_value(snapshot).expect("serialize metrics snapshot");

        assert_eq!(value["dirty_queue_len"], 2);
        assert_eq!(value["watch_budget_utilization_pct"], 91);
        assert!(value.get("watch").is_none());
        assert_eq!(value["memory"]["process_rss_plus_swap_bytes"], 3072);
        assert_eq!(value["diagnostics"]["status"], "degraded");
        assert_eq!(value["diagnostics"]["non_l0_dirs"], 1);
    }

    #[test]
    fn metrics_reports_proc_sampler_budget_issues() {
        let health = MetricsHealthSnapshot {
            proc_sampler_enabled: true,
            proc_sampler_pids_seen: 64,
            proc_sampler_sampled_dirs: 8,
            proc_sampler_budget_exhausted: true,
            proc_sampler_unavailable: true,
            ..MetricsHealthSnapshot::default()
        };

        let diagnostics = super::MetricsDiagnostics::from_parts(
            &WatchStateReport::default(),
            &MetricsRuntimeSnapshot::default(),
            &MetricsMemorySnapshot::default(),
            &health,
        );

        assert_eq!(diagnostics.status, "warning");
        assert!(diagnostics
            .issues
            .iter()
            .any(|issue| issue.contains("proc_sampler_budget_exhausted")));
        assert!(diagnostics
            .issues
            .iter()
            .any(|issue| issue == "proc_sampler_unavailable=true"));
    }
}
