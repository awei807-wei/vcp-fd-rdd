pub(crate) mod arena;
mod content;
mod directory_manifest;
pub(crate) mod events;
mod lazy_validation;
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
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::config::{MmapWarmupConfig, QueryConfig, RuntimeProfileSettings};
use crate::core::AdaptiveScheduler;
use crate::diagnostics::{DiagnosticReport, DiagnosticSource, RootCasePolicyDiagnostics};
use crate::event::sync::DirtyQueue;
use crate::fs_policy::{FsPolicyConfig, SharedMountPolicyCounters};
use crate::index::l1_cache::L1Cache;
use crate::index::l2_partition::PersistentIndex;
use crate::index::l3_cold::IndexBuilder;
use crate::stats::{L2Stats, StatsCollector, StatsReport};
use crate::storage::quarantine::{FreezeGate, RootStateRecord};
use crate::storage::recovery_audit::RecoveryAuditReport;
use crate::storage::traits::WriteAheadLog;
use crate::storage::wal::WalDurability;
use crate::util::unix_secs;

use self::quarantine::RecoveryQuarantine;
use self::rebuild::RebuildState;
pub use directory_manifest::DirectoryManifestReport;
use directory_manifest::DirectoryManifestStore;

const REBUILD_COOLDOWN: Duration = Duration::from_secs(60);
const RUNTIME_SUBTREE_TOMBSTONE_TTL: Duration = Duration::from_secs(300);

#[derive(Clone, Debug)]
struct RuntimeSubtreeTombstone {
    root_path: PathBuf,
    expires_at: Instant,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum OwnedSnapshotLifecycle {
    #[default]
    None,
    Pending,
    Writing,
}

impl OwnedSnapshotLifecycle {
    fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Pending => "pending",
            Self::Writing => "writing",
        }
    }
}

#[derive(Clone, Debug, Default)]
struct OwnedSnapshotTelemetry {
    lifecycle: OwnedSnapshotLifecycle,
    l2: L2Stats,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ScanOutcome {
    pub scanned: usize,
    pub changed: usize,
    pub elapsed_ms: u64,
    pub project_roots: Vec<PathBuf>,
}

#[derive(Clone, Debug, Default)]
pub struct StartupRecoveryReport {
    pub snapshot_source: String,
    pub wal_events_replayed: usize,
    pub wal_sealed_used: usize,
    pub wal_truncated_tail_records: usize,
    pub wal_gap_detected: bool,
    pub wal_checkpoint_used: u64,
    pub startup_scan_required: bool,
    pub requires_repair: bool,
    pub requires_rebuild: bool,
    pub soft_repair_needed: bool,
    pub deferred_repair: bool,
    pub deferred_dirty_dirs: Vec<PathBuf>,
    pub deferred_unknown_scope: bool,
    pub hard_rebuild_needed: bool,
    pub previous_clean_shutdown: bool,
    pub reasons: Vec<String>,
    pub soft_reasons: Vec<String>,
    pub hard_reasons: Vec<String>,
    pub repair_reason_counts: Vec<RecoveryReasonCount>,
    pub audit: RecoveryAuditReport,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct RecoveryReasonCount {
    pub reason: String,
    pub count: usize,
}

#[derive(Clone, Debug, Default)]
pub struct StartupRepairStats {
    pub ran: bool,
    pub escalated: bool,
    pub scanned: usize,
    pub changed: usize,
    pub elapsed_ms: u64,
    pub budget_ms: u64,
    pub budget_exhausted: bool,
    pub escalation_reason: String,
}

#[derive(Clone, Debug)]
pub struct DirtyScanOutcome {
    pub dir: PathBuf,
    pub outcome: ScanOutcome,
    pub reason: crate::event::sync::DirtyReason,
    pub manifest_skipped: bool,
    pub completion_ready: bool,
}

#[derive(Clone, Debug, Default)]
pub struct DirtyProcessReport {
    pub entries_processed: usize,
    pub dirs_scanned: usize,
    pub changed: usize,
    pub elapsed_ms: u64,
    pub fast_sync_upserts: usize,
    pub fast_sync_deletes: usize,
    pub dropped_stale_batches: usize,
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
    pub reason: Option<String>,
    pub confidence: Option<f32>,
}

impl QueryResultMeta {
    pub fn hot(meta: crate::core::FileMeta) -> Self {
        Self {
            meta,
            freshness: QueryResultFreshness::Fresh,
            index_tier: QueryResultIndexTier::HotMemory,
            validated: false,
            reason: None,
            confidence: None,
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
            reason: None,
            confidence: None,
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

/// IO tuning knobs extracted from `TieredIndex`.
pub struct IoTuning {
    pub ioprio_idle_set: AtomicBool,
    pub ioprio_set_failed: AtomicBool,
    pub mmap_warmup_enabled: AtomicBool,
    pub mmap_warmup_pages: AtomicU64,
    pub mmap_warmup_elapsed_ms: AtomicU64,
    pub mmap_warmup_cancel_reason: Mutex<String>,
    pub stable_snapshot_enabled: AtomicBool,
}

impl Default for IoTuning {
    fn default() -> Self {
        Self {
            ioprio_idle_set: AtomicBool::new(false),
            ioprio_set_failed: AtomicBool::new(false),
            mmap_warmup_enabled: AtomicBool::new(false),
            mmap_warmup_pages: AtomicU64::new(0),
            mmap_warmup_elapsed_ms: AtomicU64::new(0),
            mmap_warmup_cancel_reason: Mutex::new("disabled".to_string()),
            stable_snapshot_enabled: AtomicBool::new(true),
        }
    }
}

/// Runtime subtree tombstone & cold-sweep tracking extracted from `TieredIndex`.
pub struct TombstoneTracker {
    pub ttl_secs: AtomicU64,
    pub(self) entries: Mutex<Vec<RuntimeSubtreeTombstone>>,
    pub recent_stale_hit_dirs: Mutex<Vec<PathBuf>>,
    pub cold_sweep_last_completed: AtomicU64,
    pub cold_sweep_period_estimate: AtomicU64,
}

impl Default for TombstoneTracker {
    fn default() -> Self {
        Self {
            ttl_secs: AtomicU64::new(RUNTIME_SUBTREE_TOMBSTONE_TTL.as_secs()),
            entries: Mutex::new(Vec::new()),
            recent_stale_hit_dirs: Mutex::new(Vec::new()),
            cold_sweep_last_completed: AtomicU64::new(0),
            cold_sweep_period_estimate: AtomicU64::new(0),
        }
    }
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
    /// Serializes the WAL/delta mutation boundary with snapshot generation
    /// capture. Event application holds this gate from WAL append through the
    /// mutable L2 update; snapshotting holds it from WAL seal through durable
    /// publication. This prevents a sealed WAL record from landing in the next
    /// delta generation after the sealed file has been retired.
    pub(self) snapshot_event_gate: Mutex<()>,
    pub(self) delta_buffer: Mutex<crate::index::delta_buffer::DeltaBuffer>,
    pub base: ArcSwap<crate::index::base_index::BaseIndexData>,
    pub(self) flush_requested: AtomicBool,
    pub(self) flush_notify: Notify,
    /// Set once a termination signal has been received. From this point the
    /// periodic snapshot loop exits and rebuild admission is closed. Snapshot
    /// publication always leaves the runtime marker unclean; the runtime owns
    /// the sole clean-shutdown commit after every required persistence step.
    pub(self) shutting_down: AtomicBool,
    pub(self) auto_flush_overlay_paths: AtomicU64,
    pub(self) auto_flush_overlay_bytes: AtomicU64,
    pub(self) periodic_flush_min_events: AtomicU64,
    pub(self) periodic_flush_min_bytes: AtomicU64,
    pub(self) periodic_flush_max_staleness_secs: AtomicU64,
    pub(self) pending_flush_since_unix_secs: AtomicU64,
    pub(self) rebuild_cooldown_secs: AtomicU64,
    pub(self) wal_seal_bytes: AtomicU64,
    pub(self) pending_flush_events: AtomicU64,
    pub(self) pending_flush_bytes: AtomicU64,
    pub(self) last_snapshot_time: AtomicU64,
    /// A completed rebuild must be checkpointed even when it produced an empty
    /// index; file_count alone cannot distinguish that generation from "no work".
    pub(self) rebuild_snapshot_pending: AtomicBool,
    /// Exclusively owned rebuild output waiting for bounded-memory v7 writing.
    /// It is intentionally not query-visible and is consumed exactly once by
    /// the snapshot generation gate.
    pub(self) pending_snapshot_generation:
        Mutex<Option<crate::index::l2_partition::PersistentIndex>>,
    /// Lock-independent memory/lifecycle snapshot retained while the owned
    /// generation moves out of `pending_snapshot_generation` into the writer.
    pub(self) owned_snapshot_telemetry: Mutex<OwnedSnapshotTelemetry>,
    pub roots: Vec<PathBuf>,
    pub include_hidden: bool,
    pub ignore_enabled: bool,
    pub follow_symlinks: bool,
    pub exclude_dirs: Vec<String>,
    pub fs_policy_config: FsPolicyConfig,
    pub(self) fast_sync_semaphore: Arc<tokio::sync::Semaphore>,
    pub(self) dirty_queue: Mutex<DirtyQueue>,
    pub(self) dirty_notify: Notify,
    pub(self) recovery_quarantine: RecoveryQuarantine,
    pub(self) clock_skew: Mutex<crate::clock::ClockSkewDetector>,
    pub(self) clock_reconciliation_count: AtomicU64,
    pub(self) root_case_policies: Mutex<Vec<RootCasePolicyDiagnostics>>,
    pub(self) io_tuning: IoTuning,
    pub(self) mount_policy_counters: Arc<SharedMountPolicyCounters>,
    pub(self) io_governor: Arc<crate::io_governor::IoGovernor>,
    pub(self) stats: Arc<StatsCollector>,
    pub(self) content: content::ContentIndexState,
    pub(self) directory_manifests: DirectoryManifestStore,
    pub(self) lazy_validation: lazy_validation::LazyValidationRuntime,
    pub(self) query_max_verify_per_query: AtomicU64,
    pub(self) query_verify_timeout_ms: AtomicU64,
    pub(self) query_overlay_meta_cache_enabled: std::sync::atomic::AtomicBool,
    pub(self) tombstones: TombstoneTracker,
    pub(self) memory_report_cache: Mutex<MemoryReportCache>,
}

#[derive(Clone, Debug, Default)]
struct MemoryReportCache {
    report: Option<crate::stats::MemoryReport>,
    sampled_at: Option<Instant>,
}

impl TieredIndex {
    pub fn rebuild_in_progress(&self) -> bool {
        self.rebuild_state.lock().in_progress
    }

    /// Mark the index as shutting down. Call this once on a termination signal,
    /// before the final snapshot, so periodic snapshots stop and delayed
    /// rebuild requests cannot begin after the shutdown boundary.
    pub fn begin_shutdown(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
        let mut rebuild = self.rebuild_state.lock();
        rebuild.requested = false;
        rebuild.scheduled = false;
        drop(rebuild);
        // Wake the snapshot loop so it observes the flag and exits promptly
        // instead of sleeping out the rest of its interval.
        self.flush_notify.notify_one();
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::SeqCst)
    }

    pub fn recovery_status(&self) -> RecoveryStatus {
        self.recovery_quarantine.status.lock().clone()
    }

    pub(crate) fn set_startup_recovery_report(&self, report: StartupRecoveryReport) {
        self.recovery_quarantine.status.lock().report = report;
    }

    pub(crate) fn set_startup_repair_stats(&self, repair: StartupRepairStats) {
        self.recovery_quarantine.status.lock().repair = repair;
    }

    pub(crate) fn mark_rebuild_recovery_complete(&self) {
        let mut status = self.recovery_quarantine.status.lock();
        let report = &mut status.report;
        if !(report.startup_scan_required
            || report.requires_repair
            || report.requires_rebuild
            || report.hard_rebuild_needed)
        {
            return;
        }

        report.startup_scan_required = false;
        report.requires_repair = false;
        report.requires_rebuild = false;
        report.soft_repair_needed = false;
        report.deferred_repair = false;
        report.deferred_dirty_dirs.clear();
        report.deferred_unknown_scope = false;
        report.hard_rebuild_needed = false;
        report.reasons.clear();
        report.soft_reasons.clear();
        report.hard_reasons.clear();
        report.repair_reason_counts.clear();
        report.audit.requires_repair = false;
        report.audit.requires_rebuild = false;
        report.audit.reasons.clear();
        status.repair.escalated = false;
        status.repair.escalation_reason.clear();
    }

    pub fn set_stable_snapshot_enabled(&self, enabled: bool) {
        self.io_tuning
            .stable_snapshot_enabled
            .store(enabled, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn apply_runtime_profile_settings(&self, settings: RuntimeProfileSettings) {
        self.set_auto_flush_limits(
            settings.auto_flush_overlay_paths,
            settings.auto_flush_overlay_bytes,
        );
        self.set_periodic_flush_batch_limits(
            settings.periodic_flush_min_events,
            settings.periodic_flush_min_bytes,
        );
        self.periodic_flush_max_staleness_secs.store(
            settings.periodic_flush_max_staleness_secs,
            Ordering::Relaxed,
        );
        self.rebuild_cooldown_secs
            .store(settings.rebuild_cooldown_secs.max(1), Ordering::Relaxed);
        self.wal_seal_bytes
            .store(settings.wal_seal_bytes, Ordering::Relaxed);
    }

    pub fn set_runtime_subtree_tombstone_ttl_secs(&self, secs: u64) {
        self.tombstones.ttl_secs.store(secs, Ordering::Relaxed);
    }

    pub fn apply_query_config(&self, config: QueryConfig) {
        self.query_max_verify_per_query
            .store(config.max_verify_per_query.max(1) as u64, Ordering::Relaxed);
        self.query_verify_timeout_ms
            .store(config.verify_timeout_ms.max(1), Ordering::Relaxed);
        self.query_overlay_meta_cache_enabled
            .store(config.overlay_meta_cache_enabled, Ordering::Relaxed);
    }

    pub(self) fn query_overlay_meta_cache_enabled(&self) -> bool {
        self.query_overlay_meta_cache_enabled
            .load(Ordering::Relaxed)
    }

    fn cleanup_runtime_subtree_tombstones_locked(
        tombstones: &mut Vec<RuntimeSubtreeTombstone>,
        now: Instant,
    ) {
        tombstones.retain(|tombstone| tombstone.expires_at > now);
    }

    // TODO: consider trie or sorted vec + binary search when tombstone count > 16.
    pub(self) fn path_blocked_by_runtime_subtree_tombstone(&self, path: &Path) -> bool {
        let mut tombstones = self.tombstones.entries.lock();
        Self::cleanup_runtime_subtree_tombstones_locked(&mut tombstones, Instant::now());
        tombstones
            .iter()
            .any(|tombstone| path.starts_with(tombstone.root_path.as_path()))
    }

    pub(self) fn note_runtime_subtree_tombstones_for_events(
        &self,
        events: &[crate::core::EventRecord],
    ) {
        let now = Instant::now();
        let ttl_secs = self.tombstones.ttl_secs.load(Ordering::Relaxed);
        let expires_at = now + Duration::from_secs(ttl_secs);
        let mut tombstones = self.tombstones.entries.lock();
        Self::cleanup_runtime_subtree_tombstones_locked(&mut tombstones, now);

        for ev in events {
            match &ev.event_type {
                crate::core::EventType::Delete => {
                    if let Some(path) = ev.best_path() {
                        tombstones.push(RuntimeSubtreeTombstone {
                            root_path: normalize_path(path),
                            expires_at,
                        });
                    }
                }
                crate::core::EventType::Rename {
                    from,
                    from_path_hint,
                } => {
                    if let Some(from_path) = from_path_hint.as_deref().or_else(|| from.as_path()) {
                        tombstones.push(RuntimeSubtreeTombstone {
                            root_path: normalize_path(from_path),
                            expires_at,
                        });
                    }
                }
                crate::core::EventType::Create | crate::core::EventType::Modify => {}
            }
        }
    }

    pub(super) fn clear_runtime_subtree_tombstones_after_generation(&self) {
        self.tombstones.entries.lock().clear();
        self.tombstones.recent_stale_hit_dirs.lock().clear();
    }

    #[cfg(test)]
    fn force_expire_runtime_subtree_tombstones(&self) {
        let mut tombstones = self.tombstones.entries.lock();
        for tombstone in tombstones.iter_mut() {
            tombstone.expires_at = Instant::now();
        }
        Self::cleanup_runtime_subtree_tombstones_locked(&mut tombstones, Instant::now());
    }

    #[cfg(test)]
    fn runtime_subtree_tombstone_count(&self) -> usize {
        let mut tombstones = self.tombstones.entries.lock();
        Self::cleanup_runtime_subtree_tombstones_locked(&mut tombstones, Instant::now());
        tombstones.len()
    }

    pub fn cold_sweep_last_completed(&self) -> u64 {
        self.tombstones
            .cold_sweep_last_completed
            .load(Ordering::Relaxed)
    }

    pub fn cold_sweep_period_estimate(&self) -> u64 {
        self.tombstones
            .cold_sweep_period_estimate
            .load(Ordering::Relaxed)
    }

    pub fn set_cold_sweep_period_estimate(&self, secs: u64) {
        self.tombstones
            .cold_sweep_period_estimate
            .store(secs, Ordering::Relaxed);
    }

    pub(self) fn mark_cold_sweep_completed(&self) {
        self.tombstones
            .cold_sweep_last_completed
            .store(unix_secs(), Ordering::Relaxed);
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
        *self.recovery_quarantine.freeze_gate.lock() = gate;
    }

    pub fn restore_quarantine_from_wal(&self, records: &[RootStateRecord]) {
        let (gate, pending) = {
            let mut state = self.recovery_quarantine.quarantine_state.lock();
            state.apply_wal_records(records);
            (state.freeze_gate(), state.active_root_count() as u64)
        };
        self.install_freeze_gate(gate);
        self.recovery_quarantine
            .verify_pending
            .store(pending, Ordering::Relaxed);
    }

    pub fn apply_root_state_record(&self, record: RootStateRecord) {
        let _snapshot_boundary = self.snapshot_event_gate.lock();
        if let Some(wal) = self.wal.lock().clone() {
            if let Err(e) = wal.append_root_events(std::slice::from_ref(&record)) {
                tracing::warn!("WAL root-state append failed (continuing): {}", e);
            }
        }

        self.apply_root_state_record_in_memory(record);
    }

    fn try_apply_root_state_record_after_wal(&self, record: RootStateRecord) -> bool {
        let _snapshot_boundary = self.snapshot_event_gate.lock();
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
            let mut state = self.recovery_quarantine.quarantine_state.lock();
            state.apply_wal_record(record.clone());
            (state.freeze_gate(), state.active_root_count() as u64)
        };
        self.install_freeze_gate(gate);
        self.recovery_quarantine
            .verify_pending
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
        self.recovery_quarantine
            .freeze_gate
            .lock()
            .is_path_frozen(path)
    }

    pub(crate) fn clock_cutoff_for_dirty(&self, cutoff_ns: u64) -> u64 {
        let trusted = self.clock_skew.lock().cutoff_trusted();
        crate::clock::cutoff_for_crawl(cutoff_ns, trusted)
    }

    pub(crate) fn observe_clock_boundary(&self) {
        self.observe_clock_boundary_at(std::time::SystemTime::now(), std::time::Instant::now());
    }

    pub(crate) fn observe_clock_boundary_at(
        &self,
        wall: std::time::SystemTime,
        mono: std::time::Instant,
    ) {
        let skewed = self.clock_skew.lock().observe(wall, mono);
        if skewed {
            self.clock_reconciliation_count
                .fetch_add(1, Ordering::Relaxed);
            self.enqueue_dirty(
                crate::event::sync::DirtyScope::All { cutoff_ns: 0 },
                crate::event::sync::DirtyReason::StartupRepair,
            );
        }
    }

    pub(crate) fn mark_clock_reconciled(&self) {
        self.clock_skew.lock().mark_reconciled();
    }

    pub(crate) fn clock_cutoff_trusted(&self) -> bool {
        self.clock_skew.lock().cutoff_trusted()
    }

    pub(crate) fn record_idle_io_priority_result(&self, result: std::io::Result<()>) {
        match result {
            Ok(()) => {
                self.io_tuning
                    .ioprio_idle_set
                    .store(true, Ordering::Relaxed);
            }
            Err(err) => {
                self.io_tuning
                    .ioprio_set_failed
                    .store(true, Ordering::Relaxed);
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

    pub(crate) fn finish_query_guard_metric_ns(
        &self,
        elapsed_ns: u64,
        slow_threshold_us: u64,
    ) -> bool {
        self.stats
            .finish_query_guard_ns(elapsed_ns, slow_threshold_us)
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

    pub fn directory_manifest_report(&self) -> DirectoryManifestReport {
        self.directory_manifests.report()
    }

    pub fn set_root_case_policy_diagnostics(&self, roots: Vec<RootCasePolicyDiagnostics>) {
        *self.root_case_policies.lock() = roots;
    }

    pub fn root_case_policy_diagnostics(&self) -> Vec<RootCasePolicyDiagnostics> {
        self.root_case_policies.lock().clone()
    }

    pub fn refresh_root_case_policy_diagnostics(&self) -> Vec<RootCasePolicyDiagnostics> {
        use crate::index::case_policy::{detect_root_case_policy, CasePolicy};

        let mut roots = Vec::with_capacity(self.roots.len());
        for root in &self.roots {
            let detected = detect_root_case_policy(root, None);
            let detected_policy = match detected.detected_policy {
                CasePolicy::Sensitive => "Sensitive",
                CasePolicy::Insensitive => "Insensitive",
                CasePolicy::Auto => "Auto",
                CasePolicy::Unknown => "Unknown",
            }
            .to_string();
            roots.push(RootCasePolicyDiagnostics {
                root_path: root.display().to_string(),
                detected_policy,
                conflict_count: detected.conflict_count,
            });
        }
        self.set_root_case_policy_diagnostics(roots.clone());
        roots
    }

    pub fn apply_mmap_warmup_config(&self, config: MmapWarmupConfig) {
        self.io_tuning
            .mmap_warmup_enabled
            .store(config.enable, Ordering::Relaxed);
        if config.enable {
            self.warmup_current_mmap_segments(config.max_bytes);
        } else {
            self.io_tuning.mmap_warmup_pages.store(0, Ordering::Relaxed);
            self.io_tuning
                .mmap_warmup_elapsed_ms
                .store(0, Ordering::Relaxed);
            *self.io_tuning.mmap_warmup_cancel_reason.lock() = "disabled".to_string();
        }
    }

    pub(crate) fn warmup_current_mmap_segments(&self, max_bytes: u64) {
        self.io_governor.before_io();
        let report = self.base.load_full().warmup_cold_segments(max_bytes);
        self.io_tuning
            .mmap_warmup_pages
            .store(report.pages, Ordering::Relaxed);
        self.io_tuning
            .mmap_warmup_elapsed_ms
            .store(report.elapsed_ms, Ordering::Relaxed);
        *self.io_tuning.mmap_warmup_cancel_reason.lock() = report.cancel_reason;
    }
}

impl DiagnosticSource for TieredIndex {
    fn collect(&self, report: &mut DiagnosticReport) {
        let quarantine_roots = self
            .recovery_quarantine
            .quarantine_state
            .lock()
            .active_root_count();
        let (freeze_gates, freeze_blocked_events) = {
            let gate = self.recovery_quarantine.freeze_gate.lock();
            (gate.frozen_root_count(), gate.blocked_events())
        };
        let (
            clock_skew_count,
            clock_last_drift_ms,
            clock_cutoff_trusted,
            reconciliation_window_active,
        ) = {
            let clock = self.clock_skew.lock();
            let cutoff_trusted = clock.cutoff_trusted();
            (
                clock.skew_count(),
                clock.last_negative_drift().as_millis() as u64,
                cutoff_trusted,
                !cutoff_trusted,
            )
        };

        report.storage.quarantine_roots = quarantine_roots;
        report.storage.freeze_gates = freeze_gates;
        report.storage.freeze_blocked_events = freeze_blocked_events;
        report.storage.quarantine_verify_pending = self
            .recovery_quarantine
            .verify_pending
            .load(Ordering::Relaxed) as usize;
        report.storage.quarantine_verified_roots = self
            .recovery_quarantine
            .verified_roots
            .load(Ordering::Relaxed);
        let l2_physical = self.l2.load().physical_dedupe_stats();
        report.storage.hardlink_group_count = l2_physical.hardlink_group_count;
        report.storage.hardlink_max_group_size = l2_physical.max_group_size;
        report.storage.content_index_enabled = self.content.enabled.load(Ordering::Relaxed);
        report.storage.content_indexed_paths =
            self.content.indexed_paths.load(Ordering::Relaxed) as usize;
        report.storage.content_indexed_bytes = self.content.indexed_bytes.load(Ordering::Relaxed);
        report.storage.content_index_last_elapsed_ms =
            self.content.last_elapsed_ms.load(Ordering::Relaxed);
        report.storage.content_hash_queue_pending =
            self.content.hash_queue_pending.load(Ordering::Relaxed) as usize;
        report.storage.content_hash_candidate_count =
            self.content.hash_candidate_count.load(Ordering::Relaxed) as usize;
        report.storage.content_hash_confirmed_groups =
            self.content.hash_confirmed_groups.load(Ordering::Relaxed) as usize;
        report.storage.content_hash_skipped_count =
            self.content.hash_skipped_count.load(Ordering::Relaxed) as usize;
        report.storage.content_hash_last_elapsed_ms =
            self.content.hash_last_elapsed_ms.load(Ordering::Relaxed);
        report.storage.content_hash_last_skip_reason =
            self.content.hash_last_skip_reason.lock().clone();
        report.storage.case_policy_roots = self.root_case_policy_diagnostics();
        report.storage.case_policy_conflict_count = report
            .storage
            .case_policy_roots
            .iter()
            .map(|root| root.conflict_count)
            .sum();
        report.storage.mmap_warmup_enabled =
            self.io_tuning.mmap_warmup_enabled.load(Ordering::Relaxed);
        report.storage.mmap_warmup_pages = self.io_tuning.mmap_warmup_pages.load(Ordering::Relaxed);
        report.storage.mmap_warmup_elapsed_ms = self
            .io_tuning
            .mmap_warmup_elapsed_ms
            .load(Ordering::Relaxed);
        report.storage.mmap_warmup_cancel_reason =
            self.io_tuning.mmap_warmup_cancel_reason.lock().clone();
        report.storage.refresh_base_count = self.stats_report().refresh_base_count;

        report.clocks.skew_count = clock_skew_count;
        report.clocks.last_drift_ms = clock_last_drift_ms;
        report.clocks.cutoff_trusted = clock_cutoff_trusted;
        report.clocks.reconciliation_count =
            self.clock_reconciliation_count.load(Ordering::Relaxed);
        report.clocks.reconciliation_window_active = reconciliation_window_active;
        let ioprio_idle = self.io_tuning.ioprio_idle_set.load(Ordering::Relaxed);
        let ioprio_failed = self.io_tuning.ioprio_set_failed.load(Ordering::Relaxed);
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
        report.io.current_backoff_ms = report
            .io
            .current_backoff_ms
            .max(self.io_governor.current_backoff_ms());
        report.io.token_bucket_consume_count = report
            .io
            .token_bucket_consume_count
            .saturating_add(self.io_governor.operations());
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
