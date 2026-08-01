//! Strongly typed runtime diagnostics for `/health`.

use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct DiagnosticReport {
    pub system: SystemDiagnostics,
    pub storage: StorageDiagnostics,
    pub security: SecurityDiagnostics,
    pub clocks: ClockDiagnostics,
    pub watchers: WatcherDiagnostics,
    pub io: IoDiagnostics,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct SystemDiagnostics {
    pub version: String,
    pub allocator: String,
    pub uptime_secs: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct StorageDiagnostics {
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
    pub quarantine_roots: usize,
    pub freeze_gates: usize,
    pub freeze_blocked_events: u64,
    pub quarantine_verify_pending: usize,
    pub quarantine_verified_roots: u64,
    pub case_policy_conflict_count: u64,
    pub hardlink_group_count: usize,
    pub hardlink_max_group_size: usize,
    pub content_index_enabled: bool,
    pub content_indexed_paths: usize,
    pub content_indexed_bytes: u64,
    pub content_index_last_elapsed_ms: u64,
    pub content_hash_queue_pending: usize,
    pub content_hash_candidate_count: usize,
    pub content_hash_confirmed_groups: usize,
    pub content_hash_skipped_count: usize,
    pub content_hash_last_skip_reason: String,
    pub content_hash_last_elapsed_ms: u64,
    pub case_policy_roots: Vec<RootCasePolicyDiagnostics>,
    pub mmap_warmup_enabled: bool,
    pub mmap_warmup_pages: u64,
    pub mmap_warmup_elapsed_ms: u64,
    pub mmap_warmup_cancel_reason: String,
    pub refresh_base_count: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RootCasePolicyDiagnostics {
    pub root_path: String,
    pub detected_policy: String,
    pub conflict_count: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SecurityDiagnostics {
    pub http_policy: String,
    pub uds_peer_policy: String,
    pub scan_reject_count: u64,
    pub per_user_daemon_recommended: bool,
    pub multi_user_risk: bool,
}

impl Default for SecurityDiagnostics {
    fn default() -> Self {
        Self {
            http_policy: "localhost-debug".to_string(),
            uds_peer_policy: "same-user".to_string(),
            scan_reject_count: 0,
            per_user_daemon_recommended: true,
            multi_user_risk: false,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ClockDiagnostics {
    pub skew_count: u64,
    pub last_drift_ms: u64,
    pub cutoff_trusted: bool,
    pub reconciliation_count: u64,
    pub reconciliation_window_active: bool,
}

impl Default for ClockDiagnostics {
    fn default() -> Self {
        Self {
            skew_count: 0,
            last_drift_ms: 0,
            cutoff_trusted: true,
            reconciliation_count: 0,
            reconciliation_window_active: false,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct WatcherDiagnostics {
    pub fstype_blocked_count: u64,
    pub network_fs_ignored_count: u64,
    pub fuse_probe_timeout_count: u64,
    pub one_file_system_boundary_count: u64,
    pub denied_mount_count: u64,
    pub allowed_override_count: u64,
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
    pub inotify_dirty_dirs_enqueued: u64,
    pub inotify_dirty_dirs_suppressed: u64,
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
    pub waterline_soft_degraded: bool,
    pub waterline_hard_degraded: bool,
    pub waterline_effective_l3_scan_interval_secs: u64,
    pub waterline_effective_rotating_budget: usize,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct IoDiagnostics {
    pub ioprio_class: String,
    pub ioprio_set_failed: bool,
    pub psi_some_avg10: Option<f32>,
    pub psi_full_avg10: Option<f32>,
    pub backoff_count: u64,
    pub current_backoff_ms: u64,
    pub token_bucket_consume_count: u64,
    pub token_bucket_limited_count: u64,
}

pub trait DiagnosticSource: Send + Sync {
    fn collect(&self, report: &mut DiagnosticReport);
}

#[derive(Default)]
pub struct DiagnosticRegistry {
    sources: Vec<Arc<dyn DiagnosticSource>>,
}

impl DiagnosticRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, source: Arc<dyn DiagnosticSource>) {
        self.sources.push(source);
    }

    pub fn collect(&self) -> DiagnosticReport {
        let mut report = DiagnosticReport::default();
        for source in &self.sources {
            source.collect(&mut report);
        }
        report
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestSource;

    impl DiagnosticSource for TestSource {
        fn collect(&self, report: &mut DiagnosticReport) {
            report.storage.quarantine_roots = 2;
            report.security.scan_reject_count = 1;
        }
    }

    #[test]
    fn registry_collects_fixed_sections_from_sources() {
        let mut registry = DiagnosticRegistry::new();
        registry.register(Arc::new(TestSource));

        let report = registry.collect();

        assert_eq!(report.storage.quarantine_roots, 2);
        assert_eq!(report.security.scan_reject_count, 1);
        assert!(report.clocks.cutoff_trusted);
        assert_eq!(report.watchers.denied_mount_count, 0);
    }
}
