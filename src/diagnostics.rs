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
    pub quarantine_roots: usize,
    pub freeze_gates: usize,
    pub freeze_blocked_events: u64,
    pub quarantine_verify_pending: usize,
    pub quarantine_verified_roots: u64,
    pub case_policy_conflict_count: u64,
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
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct IoDiagnostics {
    pub ioprio_class: String,
    pub ioprio_set_failed: bool,
    pub psi_some_avg10: Option<f32>,
    pub psi_full_avg10: Option<f32>,
    pub backoff_count: u64,
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
