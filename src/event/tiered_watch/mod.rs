use std::collections::HashMap;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::RwLock;

use crate::config::NetworkFastScanMode;
use crate::event::proc_sampler::ProcSamplerReport;
use crate::util::unix_secs;

use ephemeral::{DirtyScopeObservation, EphemeralWatchLease};
use fast_scan::{network_fast_scan_mode_to_u8, FastScanState};
mod cold_window;
mod ephemeral;
mod fast_scan;
mod registry;
mod report;
#[cfg(test)]
mod tests;
mod types;

pub use types::*;

pub(super) struct DirState {
    tier: AtomicU8,
    watch_cost: AtomicU64,
    last_event_unix_secs: AtomicU64,
    last_scan_unix_secs: AtomicU64,
    empty_scan_count: AtomicU32,
    last_changed_count: AtomicU64,
    event_score: AtomicU64,
    last_score_update_unix_secs: AtomicU64,
    next_scan_unix_secs: AtomicU64,
    promotion_pending: AtomicBool,
    demotion_pending: AtomicBool,
    dirty: AtomicBool,
    dirty_since_unix_secs: AtomicU64,
    freshness: AtomicU8,
    index_residency: AtomicU8,
    budget_blocked_count: AtomicU32,
    last_budget_blocked_unix_secs: AtomicU64,
    high_priority_scan: AtomicBool,
    rotating_cold_window_progress: RwLock<RotatingColdWindowProgress>,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct RotatingColdWindowProgress {
    pub(super) last_scan_seq: u64,
    pub(super) last_scan_cycle_id: u64,
    pub(super) last_event_seq: u64,
    pub(super) last_event_cycle_id: u64,
}

impl DirState {
    pub(super) fn new(tier: WatchTier, watch_cost: usize, now: u64) -> Self {
        let is_l0 = tier == WatchTier::L0;
        Self {
            tier: AtomicU8::new(tier.as_u8()),
            watch_cost: AtomicU64::new(watch_cost as u64),
            last_event_unix_secs: AtomicU64::new(now),
            last_scan_unix_secs: AtomicU64::new(0),
            empty_scan_count: AtomicU32::new(0),
            last_changed_count: AtomicU64::new(0),
            event_score: AtomicU64::new(if is_l0 { 16 } else { 0 }),
            last_score_update_unix_secs: AtomicU64::new(now),
            next_scan_unix_secs: AtomicU64::new(0),
            promotion_pending: AtomicBool::new(false),
            demotion_pending: AtomicBool::new(false),
            dirty: AtomicBool::new(false),
            dirty_since_unix_secs: AtomicU64::new(0),
            freshness: AtomicU8::new(if is_l0 {
                Freshness::Fresh.as_u8()
            } else {
                Freshness::Unknown.as_u8()
            }),
            index_residency: AtomicU8::new(match tier {
                WatchTier::L0 => IndexResidency::HotMemory.as_u8(),
                WatchTier::L1 => IndexResidency::WarmMemory.as_u8(),
                WatchTier::L2 => IndexResidency::ColdMmap.as_u8(),
                WatchTier::L3 => IndexResidency::FrozenManifestOnly.as_u8(),
            }),
            budget_blocked_count: AtomicU32::new(0),
            last_budget_blocked_unix_secs: AtomicU64::new(0),
            high_priority_scan: AtomicBool::new(false),
            rotating_cold_window_progress: RwLock::new(RotatingColdWindowProgress::default()),
        }
    }

    pub(super) fn tier(&self) -> WatchTier {
        WatchTier::from_u8(self.tier.load(Ordering::Relaxed))
    }

    pub(super) fn freshness(&self) -> Freshness {
        Freshness::from_u8(self.freshness.load(Ordering::Relaxed))
    }

    pub(super) fn set_freshness(&self, f: Freshness) {
        self.freshness.store(f.as_u8(), Ordering::Release);
    }

    pub(super) fn index_residency(&self) -> IndexResidency {
        IndexResidency::from_u8(self.index_residency.load(Ordering::Relaxed))
    }

    pub(super) fn set_index_residency(&self, r: IndexResidency) {
        self.index_residency.store(r.as_u8(), Ordering::Release);
    }
}

pub struct TieredWatchRuntime {
    dirs: RwLock<HashMap<PathBuf, Arc<DirState>>>,
    ephemeral: RwLock<HashMap<PathBuf, EphemeralWatchLease>>,
    dirty_observations: RwLock<HashMap<PathBuf, DirtyScopeObservation>>,
    rotating_cold_window_leases: RwLock<HashMap<PathBuf, cold_window::RotatingColdWindowLease>>,
    rotating_cold_window_seen: RwLock<HashSet<PathBuf>>,
    max_watch_dirs: u64,
    l0_max_cost_per_root: u64,
    current_watch_cost: AtomicU64,
    ephemeral_watch_budget: u64,
    current_ephemeral_watch_cost: AtomicU64,
    ephemeral_watch_created: AtomicU64,
    ephemeral_watch_expired: AtomicU64,
    ephemeral_watch_evicted: AtomicU64,
    ephemeral_watch_budget_blocked: AtomicU64,
    rotating_cold_window_enabled: AtomicBool,
    rotating_cold_window_budget: AtomicUsize,
    rotating_cold_window_ttl_secs: AtomicU64,
    rotating_cold_window_max_cost_per_root: AtomicUsize,
    rotating_cold_window_max_dirs_per_tick: AtomicUsize,
    rotating_cold_window_cycle_id: AtomicU64,
    rotating_cold_window_causal_seq: AtomicU64,
    rotating_cold_window_promoted_to_ephemeral: AtomicU64,
    rotating_cold_window_fast_scan_lease_dirs: AtomicU64,
    rotating_cold_window_scan_only_dirs: AtomicU64,
    rotating_cold_window_budget_blocked: AtomicU64,
    rotating_cold_window_last_tick_unix_secs: AtomicU64,
    scan_items_per_sec: usize,
    scan_ms_per_tick: u64,
    promotions: AtomicU64,
    demotions: AtomicU64,
    replacements: AtomicU64,
    promotion_budget_blocked: AtomicU64,
    watch_mount_policy_rejected: AtomicU64,
    watch_exclude_rejected: AtomicU64,
    last_budget_blocked_kernel_watch_cost: AtomicU64,
    last_budget_blocked_budget_remaining: AtomicU64,
    last_budget_blocked_reason: RwLock<String>,
    cold_validate_count: AtomicU64,
    dirty_queue_len: AtomicUsize,
    query_stale_hit_count: AtomicU64,
    query_permission_denied_count: AtomicU64,
    fast_scan_enabled: AtomicBool,
    fast_scan_target_secs: AtomicU64,
    fast_scan_tick_ms: AtomicU64,
    fast_scan_local_stat_budget_per_tick: AtomicUsize,
    fast_scan_network_stat_budget_per_tick: AtomicUsize,
    fast_scan_local_readdir_budget_per_tick: AtomicUsize,
    fast_scan_network_readdir_budget_per_tick: AtomicUsize,
    fast_scan_initial_backfill_budget_per_tick: AtomicUsize,
    fast_scan_hotset_max_leases: AtomicUsize,
    fast_scan_lease_ttl_secs: AtomicU64,
    fast_scan_proc_sampler_lease_ttl_secs: AtomicU64,
    fast_scan_explicit_lease_ttl_secs: AtomicU64,
    fast_scan_sentinel_registry_max_entries: AtomicUsize,
    fast_scan_network_mode: AtomicU8,
    fast_scan_state: RwLock<FastScanState>,
    fast_scan_checked_dirs: AtomicU64,
    fast_scan_changed_dirs: AtomicU64,
    fast_scan_generated_events: AtomicU64,
    fast_scan_pending_changed_dirs: AtomicUsize,
    fast_scan_budget_degraded: AtomicBool,
    fast_scan_bootstrap_next_unix_ms: AtomicU64,
    fast_scan_parent_fence_retries: AtomicU64,
    fast_scan_epoch_conflicts: AtomicU64,
    fast_scan_lease_evictions: AtomicU64,
    fast_scan_lease_renewals: AtomicU64,
    fast_scan_real_changed_dirs: AtomicU64,
    fast_scan_apply_dropped_stale_batches: AtomicU64,
    fast_scan_scan_workers_active: AtomicU64,
    fast_scan_io_budget_limited_count: AtomicU64,
    proc_sampler_enabled: AtomicBool,
    proc_sampler_last_duration_ms: AtomicU64,
    proc_sampler_pids_seen: AtomicU64,
    proc_sampler_pids_scanned: AtomicU64,
    proc_sampler_pids_denied: AtomicU64,
    proc_sampler_fdinfo_read_count: AtomicU64,
    proc_sampler_readlink_count: AtomicU64,
    proc_sampler_write_fd_count: AtomicU64,
    proc_sampler_sampled_dirs: AtomicU64,
    proc_sampler_triggered_watches: AtomicU64,
    proc_sampler_budget_exhausted: AtomicBool,
    proc_sampler_unavailable: AtomicBool,
    last_adjustment_unix_secs: AtomicU64,
    /// Waterline alarm — adaptive L3 scan degradation state.
    waterline_alarm: cold_window::WaterlineAlarm,
}

impl TieredWatchRuntime {
    pub fn new(
        l0_roots: Vec<(PathBuf, usize)>,
        l1_roots: Vec<(PathBuf, usize)>,
        max_watch_dirs: usize,
        scan_items_per_sec: usize,
        scan_ms_per_tick: u64,
    ) -> Self {
        Self::new_with_l0_max_cost_and_ephemeral(
            l0_roots,
            l1_roots,
            max_watch_dirs,
            max_watch_dirs,
            scan_items_per_sec,
            scan_ms_per_tick,
            0,
        )
    }

    pub fn new_with_ephemeral(
        l0_roots: Vec<(PathBuf, usize)>,
        l1_roots: Vec<(PathBuf, usize)>,
        max_watch_dirs: usize,
        scan_items_per_sec: usize,
        scan_ms_per_tick: u64,
        ephemeral_watch_budget: usize,
    ) -> Self {
        Self::new_with_l0_max_cost_and_ephemeral(
            l0_roots,
            l1_roots,
            max_watch_dirs,
            max_watch_dirs,
            scan_items_per_sec,
            scan_ms_per_tick,
            ephemeral_watch_budget,
        )
    }

    pub fn new_with_l0_max_cost_and_ephemeral(
        l0_roots: Vec<(PathBuf, usize)>,
        l1_roots: Vec<(PathBuf, usize)>,
        max_watch_dirs: usize,
        l0_max_cost_per_root: usize,
        scan_items_per_sec: usize,
        scan_ms_per_tick: u64,
        ephemeral_watch_budget: usize,
    ) -> Self {
        let now = unix_secs();
        let max_watch_dirs = max_watch_dirs.max(1);
        let l0_max_cost_per_root = if l0_max_cost_per_root == 0 {
            max_watch_dirs
        } else {
            l0_max_cost_per_root.clamp(1, max_watch_dirs)
        };
        let mut current_watch_cost = 0u64;
        let mut dirs = HashMap::new();

        for (path, cost) in l0_roots {
            current_watch_cost = current_watch_cost.saturating_add(cost as u64);
            dirs.insert(path, Arc::new(DirState::new(WatchTier::L0, cost, now)));
        }
        for (path, cost) in l1_roots {
            dirs.entry(path)
                .or_insert_with(|| Arc::new(DirState::new(WatchTier::L1, cost, now)));
        }

        Self {
            dirs: RwLock::new(dirs),
            ephemeral: RwLock::new(HashMap::new()),
            dirty_observations: RwLock::new(HashMap::new()),
            rotating_cold_window_leases: RwLock::new(HashMap::new()),
            rotating_cold_window_seen: RwLock::new(HashSet::new()),
            max_watch_dirs: max_watch_dirs as u64,
            l0_max_cost_per_root: l0_max_cost_per_root as u64,
            current_watch_cost: AtomicU64::new(current_watch_cost),
            ephemeral_watch_budget: ephemeral_watch_budget as u64,
            current_ephemeral_watch_cost: AtomicU64::new(0),
            ephemeral_watch_created: AtomicU64::new(0),
            ephemeral_watch_expired: AtomicU64::new(0),
            ephemeral_watch_evicted: AtomicU64::new(0),
            ephemeral_watch_budget_blocked: AtomicU64::new(0),
            rotating_cold_window_enabled: AtomicBool::new(false),
            rotating_cold_window_budget: AtomicUsize::new(128),
            rotating_cold_window_ttl_secs: AtomicU64::new(180),
            rotating_cold_window_max_cost_per_root: AtomicUsize::new(64),
            rotating_cold_window_max_dirs_per_tick: AtomicUsize::new(8),
            rotating_cold_window_cycle_id: AtomicU64::new(0),
            rotating_cold_window_causal_seq: AtomicU64::new(0),
            rotating_cold_window_promoted_to_ephemeral: AtomicU64::new(0),
            rotating_cold_window_fast_scan_lease_dirs: AtomicU64::new(0),
            rotating_cold_window_scan_only_dirs: AtomicU64::new(0),
            rotating_cold_window_budget_blocked: AtomicU64::new(0),
            rotating_cold_window_last_tick_unix_secs: AtomicU64::new(0),
            scan_items_per_sec,
            scan_ms_per_tick,
            promotions: AtomicU64::new(0),
            demotions: AtomicU64::new(0),
            replacements: AtomicU64::new(0),
            promotion_budget_blocked: AtomicU64::new(0),
            watch_mount_policy_rejected: AtomicU64::new(0),
            watch_exclude_rejected: AtomicU64::new(0),
            last_budget_blocked_kernel_watch_cost: AtomicU64::new(0),
            last_budget_blocked_budget_remaining: AtomicU64::new(0),
            last_budget_blocked_reason: RwLock::new(String::new()),
            cold_validate_count: AtomicU64::new(0),
            dirty_queue_len: AtomicUsize::new(0),
            query_stale_hit_count: AtomicU64::new(0),
            query_permission_denied_count: AtomicU64::new(0),
            fast_scan_enabled: AtomicBool::new(true),
            fast_scan_target_secs: AtomicU64::new(5),
            fast_scan_tick_ms: AtomicU64::new(1_000),
            fast_scan_local_stat_budget_per_tick: AtomicUsize::new(5_000),
            fast_scan_network_stat_budget_per_tick: AtomicUsize::new(128),
            fast_scan_local_readdir_budget_per_tick: AtomicUsize::new(512),
            fast_scan_network_readdir_budget_per_tick: AtomicUsize::new(16),
            fast_scan_initial_backfill_budget_per_tick: AtomicUsize::new(2_048),
            fast_scan_hotset_max_leases: AtomicUsize::new(512),
            fast_scan_lease_ttl_secs: AtomicU64::new(1_800),
            fast_scan_proc_sampler_lease_ttl_secs: AtomicU64::new(300),
            fast_scan_explicit_lease_ttl_secs: AtomicU64::new(0),
            fast_scan_sentinel_registry_max_entries: AtomicUsize::new(512),
            fast_scan_network_mode: AtomicU8::new(network_fast_scan_mode_to_u8(
                NetworkFastScanMode::BestEffort,
            )),
            fast_scan_state: RwLock::new(FastScanState::default()),
            fast_scan_checked_dirs: AtomicU64::new(0),
            fast_scan_changed_dirs: AtomicU64::new(0),
            fast_scan_generated_events: AtomicU64::new(0),
            fast_scan_pending_changed_dirs: AtomicUsize::new(0),
            fast_scan_budget_degraded: AtomicBool::new(false),
            fast_scan_bootstrap_next_unix_ms: AtomicU64::new(0),
            fast_scan_parent_fence_retries: AtomicU64::new(0),
            fast_scan_epoch_conflicts: AtomicU64::new(0),
            fast_scan_lease_evictions: AtomicU64::new(0),
            fast_scan_lease_renewals: AtomicU64::new(0),
            fast_scan_real_changed_dirs: AtomicU64::new(0),
            fast_scan_apply_dropped_stale_batches: AtomicU64::new(0),
            fast_scan_scan_workers_active: AtomicU64::new(0),
            fast_scan_io_budget_limited_count: AtomicU64::new(0),
            proc_sampler_enabled: AtomicBool::new(false),
            proc_sampler_last_duration_ms: AtomicU64::new(0),
            proc_sampler_pids_seen: AtomicU64::new(0),
            proc_sampler_pids_scanned: AtomicU64::new(0),
            proc_sampler_pids_denied: AtomicU64::new(0),
            proc_sampler_fdinfo_read_count: AtomicU64::new(0),
            proc_sampler_readlink_count: AtomicU64::new(0),
            proc_sampler_write_fd_count: AtomicU64::new(0),
            proc_sampler_sampled_dirs: AtomicU64::new(0),
            proc_sampler_triggered_watches: AtomicU64::new(0),
            proc_sampler_budget_exhausted: AtomicBool::new(false),
            proc_sampler_unavailable: AtomicBool::new(false),
            last_adjustment_unix_secs: AtomicU64::new(now),
            waterline_alarm: cold_window::WaterlineAlarm::new(),
        }
    }

    pub fn set_proc_sampler_enabled(&self, enabled: bool) {
        self.proc_sampler_enabled.store(enabled, Ordering::Relaxed);
    }

    pub fn record_proc_sampler_report(&self, report: ProcSamplerReport, triggered_watches: u64) {
        self.proc_sampler_last_duration_ms
            .store(report.duration_ms, Ordering::Relaxed);
        self.proc_sampler_pids_seen
            .store(report.pids_seen, Ordering::Relaxed);
        self.proc_sampler_pids_scanned
            .store(report.pids_scanned, Ordering::Relaxed);
        self.proc_sampler_pids_denied
            .store(report.pids_denied, Ordering::Relaxed);
        self.proc_sampler_fdinfo_read_count
            .store(report.fdinfo_read_count, Ordering::Relaxed);
        self.proc_sampler_readlink_count
            .store(report.readlink_count, Ordering::Relaxed);
        self.proc_sampler_write_fd_count
            .store(report.write_fd_count, Ordering::Relaxed);
        self.proc_sampler_sampled_dirs
            .store(report.sampled_dirs, Ordering::Relaxed);
        self.proc_sampler_triggered_watches
            .store(triggered_watches, Ordering::Relaxed);
        self.proc_sampler_budget_exhausted
            .store(report.budget_exhausted, Ordering::Relaxed);
        self.proc_sampler_unavailable
            .store(report.unavailable, Ordering::Relaxed);
    }

    pub fn record_event_paths<'a>(
        &self,
        paths: impl IntoIterator<Item = &'a PathBuf>,
    ) -> Vec<PathBuf> {
        let now = unix_secs();
        let paths = paths.into_iter().collect::<Vec<_>>();
        self.record_ephemeral_events(&paths, now);
        self.record_rotating_cold_window_event_progress(&paths, now, Instant::now());
        let dirs = self.dirs.read();
        let mut dirty_dirs = Vec::new();
        let mut l0_event_lease_dirs = Vec::new();
        for path in paths {
            for (root, state) in dirs.iter() {
                if !path_is_under_or_equal(path, root) {
                    continue;
                }
                if state.tier() == WatchTier::L0 {
                    state.last_event_unix_secs.store(now, Ordering::Relaxed);
                    state.empty_scan_count.store(0, Ordering::Relaxed);
                    state
                        .event_score
                        .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |score| {
                            Some(score.saturating_add(8).min(10_000))
                        })
                        .ok();
                    state.dirty.store(false, Ordering::Relaxed);
                    state.set_freshness(Freshness::Fresh);
                    if let Some(parent) = path.parent() {
                        l0_event_lease_dirs.push(parent.to_path_buf());
                    }
                } else {
                    state.dirty.store(true, Ordering::Relaxed);
                    state.dirty_since_unix_secs.store(now, Ordering::Relaxed);
                    state.set_freshness(Freshness::Dirty);
                    if let Some(parent) = path.parent() {
                        dirty_dirs.push(parent.to_path_buf());
                    }
                }
            }
        }
        drop(dirs);
        l0_event_lease_dirs.sort();
        l0_event_lease_dirs.dedup();
        self.grant_fast_scan_leases(l0_event_lease_dirs, FastScanLeaseKind::L0Event, None, 2);
        dirty_dirs.sort();
        dirty_dirs.dedup();
        dirty_dirs
    }

    pub fn covering_tier(&self, path: &Path) -> Option<WatchTier> {
        let dirs = self.dirs.read();
        dirs.iter()
            .filter(|(root, _)| path_is_under_or_equal(path, root))
            .max_by_key(|(root, _)| root.as_os_str().as_encoded_bytes().len())
            .map(|(_, state)| state.tier())
    }

    pub fn max_watch_dirs(&self) -> usize {
        self.max_watch_dirs as usize
    }

    pub fn l0_max_cost_per_root(&self) -> usize {
        self.l0_max_cost_per_root as usize
    }

    pub fn note_watch_mount_policy_rejected(&self) {
        self.watch_mount_policy_rejected
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn note_watch_exclude_rejected(&self) {
        self.watch_exclude_rejected.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn record_last_budget_blocked(
        &self,
        reason: String,
        kernel_watch_cost: u64,
        budget_remaining: u64,
    ) {
        self.last_budget_blocked_kernel_watch_cost
            .store(kernel_watch_cost, Ordering::Relaxed);
        self.last_budget_blocked_budget_remaining
            .store(budget_remaining, Ordering::Relaxed);
        *self.last_budget_blocked_reason.write() = reason;
    }

    pub fn set_dirty_queue_len(&self, len: usize) {
        self.dirty_queue_len.store(len, Ordering::Relaxed);
    }

    pub fn set_query_stale_hit_count(&self, count: u64) {
        self.query_stale_hit_count.store(count, Ordering::Relaxed);
    }

    pub fn set_query_permission_denied_count(&self, count: u64) {
        self.query_permission_denied_count
            .store(count, Ordering::Relaxed);
    }

    pub(super) fn state(&self, path: &Path) -> Option<Arc<DirState>> {
        self.dirs.read().get(path).cloned()
    }
}

pub(super) fn path_is_under_or_equal(path: &Path, root: &Path) -> bool {
    path == root || path.starts_with(root)
}

pub(super) fn path_has_component(path: &Path, component: &str) -> bool {
    path.components()
        .any(|part| part.as_os_str().to_string_lossy() == component)
}

pub(super) fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}
