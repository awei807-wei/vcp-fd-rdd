pub(crate) mod arena;
pub(crate) mod events;
pub(crate) mod load;
mod memory;
mod quarantine;
mod query;
mod query_plan;
pub(crate) mod rebuild;
mod snapshot;
pub(crate) mod sync;

#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::core::AdaptiveScheduler;
use crate::diagnostics::{DiagnosticReport, DiagnosticSource};
use crate::event::sync::DirtyQueue;
use crate::fs_policy::{FsPolicyConfig, SharedMountPolicyCounters};
use crate::index::l1_cache::L1Cache;
use crate::index::l2_partition::PersistentIndex;
use crate::index::l3_cold::IndexBuilder;
use crate::stats::{StatsCollector, StatsReport};
use crate::storage::quarantine::{FreezeGate, QuarantineState, RootStateRecord};
use crate::storage::recovery_audit::RecoveryAuditReport;
use crate::storage::traits::WriteAheadLog;
use crate::storage::wal::WalDurability;

use self::rebuild::RebuildState;

const REBUILD_COOLDOWN: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ScanOutcome {
    pub scanned: usize,
    pub changed: usize,
    pub elapsed_ms: u64,
}

#[derive(Clone, Debug, Default)]
pub struct StartupRecoveryReport {
    pub snapshot_source: String,
    pub wal_events_replayed: usize,
    pub wal_sealed_used: usize,
    pub wal_truncated_tail_records: usize,
    pub wal_gap_detected: bool,
    pub wal_checkpoint_used: u64,
    pub requires_repair: bool,
    pub requires_rebuild: bool,
    pub previous_clean_shutdown: bool,
    pub reasons: Vec<String>,
    pub audit: RecoveryAuditReport,
}

#[derive(Clone, Debug, Default)]
pub struct StartupRepairStats {
    pub ran: bool,
    pub escalated: bool,
    pub scanned: usize,
    pub changed: usize,
    pub elapsed_ms: u64,
}

#[derive(Clone, Debug)]
pub struct DirtyScanOutcome {
    pub dir: PathBuf,
    pub outcome: ScanOutcome,
    pub reason: crate::event::sync::DirtyReason,
}

#[derive(Clone, Debug, Default)]
pub struct DirtyProcessReport {
    pub entries_processed: usize,
    pub dirs_scanned: usize,
    pub changed: usize,
    pub elapsed_ms: u64,
    pub fast_sync_upserts: usize,
    pub fast_sync_deletes: usize,
    pub failed: bool,
    pub outcomes: Vec<DirtyScanOutcome>,
}

#[derive(Clone, Debug, Default)]
pub struct RecoveryStatus {
    pub report: StartupRecoveryReport,
    pub repair: StartupRepairStats,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryResultFreshness {
    Fresh,
    StaleChecked,
    Changed,
    Unknown,
}

impl QueryResultFreshness {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Fresh => "fresh",
            Self::StaleChecked => "stale_checked",
            Self::Changed => "changed",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryResultIndexTier {
    HotMemory,
    WarmMemory,
    ColdMmap,
    FrozenManifestOnly,
}

impl QueryResultIndexTier {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::HotMemory => "HotMemory",
            Self::WarmMemory => "WarmMemory",
            Self::ColdMmap => "ColdMmap",
            Self::FrozenManifestOnly => "FrozenManifestOnly",
        }
    }
}

#[derive(Clone, Debug)]
pub struct QueryResultMeta {
    pub meta: crate::core::FileMeta,
    pub freshness: QueryResultFreshness,
    pub index_tier: QueryResultIndexTier,
    pub validated: bool,
}

impl QueryResultMeta {
    pub fn hot(meta: crate::core::FileMeta) -> Self {
        Self {
            meta,
            freshness: QueryResultFreshness::Fresh,
            index_tier: QueryResultIndexTier::HotMemory,
            validated: false,
        }
    }

    pub fn cold(
        meta: crate::core::FileMeta,
        freshness: QueryResultFreshness,
        index_tier: QueryResultIndexTier,
        validated: bool,
    ) -> Self {
        Self {
            meta,
            freshness,
            index_tier,
            validated,
        }
    }
}

pub(crate) fn pathbuf_from_bytes(bytes: impl AsRef<[u8]>) -> PathBuf {
    use unicode_normalization::UnicodeNormalization;
    let s = String::from_utf8_lossy(bytes.as_ref());
    PathBuf::from(s.nfc().collect::<String>())
}

pub(crate) fn normalize_path(path: &std::path::Path) -> PathBuf {
    use unicode_normalization::UnicodeNormalization;
    let s = path.to_string_lossy();
    PathBuf::from(s.nfc().collect::<String>())
}

/// 三级索引：L1 热缓存 → L2 持久索引（内存常驻）→ L3 构建器（不在查询链路）
pub struct TieredIndex {
    pub l1: L1Cache,
    pub l2: ArcSwap<PersistentIndex>,
    pub l3: IndexBuilder,
    pub(self) scheduler: Mutex<AdaptiveScheduler>,
    pub(self) wal: Mutex<Option<Arc<dyn WriteAheadLog + Send + Sync>>>,
    pub event_seq: AtomicU64,
    pub(self) rebuild_state: Mutex<RebuildState>,
    pub(self) delta_buffer: Mutex<crate::index::delta_buffer::DeltaBuffer>,
    pub base: ArcSwap<crate::index::base_index::BaseIndexData>,
    pub(self) flush_requested: AtomicBool,
    pub(self) flush_notify: Notify,
    pub(self) auto_flush_overlay_paths: AtomicU64,
    pub(self) auto_flush_overlay_bytes: AtomicU64,
    pub(self) periodic_flush_min_events: AtomicU64,
    pub(self) periodic_flush_min_bytes: AtomicU64,
    pub(self) pending_flush_events: AtomicU64,
    pub(self) pending_flush_bytes: AtomicU64,
    pub(self) last_snapshot_time: AtomicU64,
    pub roots: Vec<PathBuf>,
    pub include_hidden: bool,
    pub ignore_enabled: bool,
    pub follow_symlinks: bool,
    pub exclude_dirs: Vec<String>,
    pub fs_policy_config: FsPolicyConfig,
    pub(self) fast_sync_semaphore: Arc<tokio::sync::Semaphore>,
    pub(self) dirty_queue: Mutex<DirtyQueue>,
    pub(self) dirty_notify: Notify,
    pub(self) recovery_status: Mutex<RecoveryStatus>,
    pub(self) quarantine_state: Mutex<QuarantineState>,
    pub(self) freeze_gate: Mutex<FreezeGate>,
    pub(self) quarantine_verify_pending: AtomicU64,
    pub(self) quarantine_verified_roots: AtomicU64,
    pub(self) clock_skew: Mutex<crate::clock::ClockSkewDetector>,
    pub(self) clock_reconciliation_count: AtomicU64,
    pub(self) ioprio_idle_set: AtomicBool,
    pub(self) ioprio_set_failed: AtomicBool,
    pub(self) stable_snapshot_enabled: AtomicBool,
    pub(self) mount_policy_counters: Arc<SharedMountPolicyCounters>,
    pub(self) io_governor: Arc<crate::io_governor::IoGovernor>,
    pub(self) stats: Arc<StatsCollector>,
}

impl TieredIndex {
    pub fn rebuild_in_progress(&self) -> bool {
        self.rebuild_state.lock().in_progress
    }

    pub fn recovery_status(&self) -> RecoveryStatus {
        self.recovery_status.lock().clone()
    }

    pub(crate) fn set_startup_recovery_report(&self, report: StartupRecoveryReport) {
        self.recovery_status.lock().report = report;
    }

    pub(crate) fn set_startup_repair_stats(&self, repair: StartupRepairStats) {
        self.recovery_status.lock().repair = repair;
    }

    pub fn set_stable_snapshot_enabled(&self, enabled: bool) {
        self.stable_snapshot_enabled
            .store(enabled, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn set_wal_durability(&self, durability: WalDurability) {
        if let Some(wal) = self.wal.lock().as_ref() {
            wal.set_durability(durability);
        }
    }

    pub fn wal_durability(&self) -> WalDurability {
        self.wal
            .lock()
            .as_ref()
            .map(|wal| wal.durability())
            .unwrap_or_default()
    }

    pub fn install_freeze_gate(&self, gate: FreezeGate) {
        *self.freeze_gate.lock() = gate;
    }

    pub fn restore_quarantine_from_wal(&self, records: &[RootStateRecord]) {
        let (gate, pending) = {
            let mut state = self.quarantine_state.lock();
            state.apply_wal_records(records);
            (state.freeze_gate(), state.active_root_count() as u64)
        };
        self.install_freeze_gate(gate);
        self.quarantine_verify_pending
            .store(pending, Ordering::Relaxed);
    }

    pub fn apply_root_state_record(&self, record: RootStateRecord) {
        if let Some(wal) = self.wal.lock().clone() {
            if let Err(e) = wal.append_root_events(std::slice::from_ref(&record)) {
                tracing::warn!("WAL root-state append failed (continuing): {}", e);
            }
        }

        self.apply_root_state_record_in_memory(record);
    }

    fn try_apply_root_state_record_after_wal(&self, record: RootStateRecord) -> bool {
        let Some(wal) = self.wal.lock().clone() else {
            tracing::warn!("WAL root-state append skipped: WAL is not attached");
            return false;
        };
        if let Err(e) = wal.append_root_events(std::slice::from_ref(&record)) {
            tracing::warn!("WAL root-state append failed: {}", e);
            return false;
        }
        self.apply_root_state_record_in_memory(record);
        true
    }

    fn apply_root_state_record_in_memory(&self, record: RootStateRecord) {
        let (gate, pending) = {
            let mut state = self.quarantine_state.lock();
            state.apply_wal_record(record.clone());
            (state.freeze_gate(), state.active_root_count() as u64)
        };
        self.install_freeze_gate(gate);
        self.quarantine_verify_pending
            .store(pending, Ordering::Relaxed);

        if matches!(
            record.kind,
            crate::storage::quarantine::RootStateKind::OnlineRoot
        ) && !record.affected_prefixes.is_empty()
        {
            self.enqueue_dirty_dirs(
                record.affected_prefixes,
                crate::event::sync::DirtyReason::StartupRepair,
            );
        }
    }

    pub fn path_is_frozen(&self, path: &Path) -> bool {
        self.freeze_gate.lock().is_path_frozen(path)
    }

    pub(crate) fn clock_cutoff_for_dirty(&self, cutoff_ns: u64) -> u64 {
        let trusted = self.clock_skew.lock().cutoff_trusted();
        crate::clock::cutoff_for_crawl(cutoff_ns, trusted)
    }

    pub(crate) fn observe_clock_boundary(&self) {
        let skewed = self
            .clock_skew
            .lock()
            .observe(std::time::SystemTime::now(), std::time::Instant::now());
        if skewed {
            self.clock_reconciliation_count
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn mark_clock_reconciled(&self) {
        self.clock_skew.lock().mark_reconciled();
    }

    pub(crate) fn record_idle_io_priority_result(&self, result: std::io::Result<()>) {
        match result {
            Ok(()) => {
                self.ioprio_idle_set.store(true, Ordering::Relaxed);
            }
            Err(err) => {
                self.ioprio_set_failed.store(true, Ordering::Relaxed);
                tracing::debug!("idle ioprio best-effort setup failed: {}", err);
            }
        }
    }

    pub(crate) fn set_current_thread_idle_io_priority_for_scan(&self) {
        self.record_idle_io_priority_result(
            crate::io_governor::set_current_thread_idle_io_priority_best_effort(),
        );
    }

    pub fn record_query_metric(&self, elapsed_us: u64) {
        self.stats.record_query(elapsed_us);
    }

    pub(crate) fn begin_query_guard_metric(&self) {
        self.stats.begin_query_guard();
    }

    pub(crate) fn finish_query_guard_metric(
        &self,
        elapsed_us: u64,
        slow_threshold_us: u64,
    ) -> bool {
        self.stats.finish_query_guard(elapsed_us, slow_threshold_us)
    }

    pub(crate) fn record_exact_query_metric(&self) {
        self.stats.record_exact_query();
    }

    pub(crate) fn record_fuzzy_query_metric(&self) {
        self.stats.record_fuzzy_query();
    }

    pub(crate) fn record_query_no_trigram_hint_metric(&self) {
        self.stats.record_query_no_trigram_hint();
    }

    pub(crate) fn record_fuzzy_full_scan_metric(&self, candidates: u64, elapsed_us: u64) {
        self.stats.record_fuzzy_full_scan(candidates, elapsed_us);
    }

    pub fn stats_report(&self) -> StatsReport {
        self.stats.report()
    }

    pub fn mount_policy_counters(&self) -> Arc<SharedMountPolicyCounters> {
        self.mount_policy_counters.clone()
    }

    pub fn fs_policy_config(&self) -> FsPolicyConfig {
        self.fs_policy_config.clone()
    }
}

impl DiagnosticSource for TieredIndex {
    fn collect(&self, report: &mut DiagnosticReport) {
        let quarantine = self.quarantine_state.lock();
        let gate = self.freeze_gate.lock();
        let clock = self.clock_skew.lock();

        report.storage.quarantine_roots = quarantine.active_root_count();
        report.storage.freeze_gates = gate.frozen_root_count();
        report.storage.freeze_blocked_events = gate.blocked_events();
        report.storage.quarantine_verify_pending =
            self.quarantine_verify_pending.load(Ordering::Relaxed) as usize;
        report.storage.quarantine_verified_roots =
            self.quarantine_verified_roots.load(Ordering::Relaxed);
        report.clocks.skew_count = clock.skew_count();
        report.clocks.last_drift_ms = clock.last_negative_drift().as_millis() as u64;
        report.clocks.cutoff_trusted = clock.cutoff_trusted();
        report.clocks.reconciliation_count =
            self.clock_reconciliation_count.load(Ordering::Relaxed);
        report.clocks.reconciliation_window_active = !clock.cutoff_trusted();
        let ioprio_idle = self.ioprio_idle_set.load(Ordering::Relaxed);
        let ioprio_failed = self.ioprio_set_failed.load(Ordering::Relaxed);
        report.io.ioprio_class = if ioprio_idle {
            "idle".to_string()
        } else if ioprio_failed {
            "unavailable".to_string()
        } else {
            "unset".to_string()
        };
        report.io.ioprio_set_failed = report.io.ioprio_set_failed || ioprio_failed;
        if let Some(pressure) = self.io_governor.last_pressure() {
            report.io.psi_some_avg10 = Some(pressure.some_avg10);
            report.io.psi_full_avg10 = Some(pressure.full_avg10);
        }
        report.io.backoff_count = report
            .io
            .backoff_count
            .saturating_add(self.io_governor.backoff_count());
        report.io.token_bucket_limited_count = report
            .io
            .token_bucket_limited_count
            .saturating_add(self.io_governor.token_bucket_limited_count());

        let mount = self.mount_policy_counters.snapshot();
        report.watchers.fstype_blocked_count = report
            .watchers
            .fstype_blocked_count
            .saturating_add(mount.fstype_blocked_count);
        report.watchers.network_fs_ignored_count = report
            .watchers
            .network_fs_ignored_count
            .saturating_add(mount.network_fs_ignored_count);
        report.watchers.fuse_probe_timeout_count = report
            .watchers
            .fuse_probe_timeout_count
            .saturating_add(mount.fuse_probe_timeout_count);
        report.watchers.one_file_system_boundary_count = report
            .watchers
            .one_file_system_boundary_count
            .saturating_add(mount.one_file_system_boundary_count);
        report.watchers.denied_mount_count = report
            .watchers
            .denied_mount_count
            .saturating_add(mount.denied_mount_count);
        report.watchers.allowed_override_count = report
            .watchers
            .allowed_override_count
            .saturating_add(mount.allowed_override_count);
    }
}

// Re-exports
