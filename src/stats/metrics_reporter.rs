use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;

use crate::stats::{EventPipelineStats, MemoryReport, StatsReport, WatchStateReport};

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
    pub cold_validate_count: u64,
    pub query_stale_hit_count: u64,
    pub events_applied: u64,
    pub events_dropped: u64,
    pub snapshot_count: u64,
    pub fast_sync_count: u64,
    pub event_pipeline_last_batch_size: usize,
    pub event_pipeline_total_events_processed: u64,
    pub event_pipeline_overflow_drops: u64,
    pub event_pipeline_rescan_signals: u64,
    pub event_pipeline_watch_failures: u64,
}

#[derive(Debug, Default, serde::Serialize)]
pub struct MetricsMemorySnapshot {
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
    pub system_max_user_watches: usize,
    pub required_watch_cost: u64,
    pub watch_budget_shortfall: u64,
    pub strict_coverage_ok: bool,
    pub strict_coverage_failure: bool,
    pub strict_fail_on_budget_exceeded: bool,
    pub strict_uncovered_dirs: Vec<String>,
    pub watch_failures: u64,
    pub overflow_drops: u64,
    pub rescan_signals: u64,
    pub last_snapshot_time: u64,
    pub snapshot_source: String,
    pub wal_events_replayed: usize,
    pub wal_truncated_tail_records: usize,
    pub startup_repair_ran: bool,
    pub startup_repair_escalated: bool,
    pub startup_repair_scanned: usize,
    pub startup_repair_changed: usize,
    pub last_clean_shutdown: bool,
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
            cold_validate_count: stats.cold_validate_count,
            query_stale_hit_count: stats.query_stale_hit_count,
            events_applied: stats.events_applied,
            events_dropped: stats.events_dropped,
            snapshot_count: stats.snapshot_count,
            fast_sync_count: stats.fast_sync_count,
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
        if watch.query_stale_hit_count > 0 {
            issues.push(format!(
                "query_stale_hit_count={}",
                watch.query_stale_hit_count
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
                "strict_coverage_incomplete: required_watch_cost={} max_watch_dirs={} shortfall={} uncovered={:?}",
                health.required_watch_cost,
                health.max_watch_dirs,
                health.watch_budget_shortfall,
                health.strict_uncovered_dirs
            ));
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
        if health.startup_repair_escalated {
            issues.push("startup_repair_escalated=true".to_string());
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
            || watch.dirty_queue_len > 0
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

fn unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
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
}
