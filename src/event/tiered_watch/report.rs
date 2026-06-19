//! Report and debug dump assembly.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use crate::stats::WatchStateReport;
use crate::util::unix_secs;

use super::fast_scan::{fast_scan_mode_label, network_fast_scan_mode_from_u8};
use super::types::*;
use super::{path_is_under_or_equal, unix_millis, TieredWatchRuntime};

/// Tier-level directory counts and watch-cost breakdown collected by
/// [`TieredWatchRuntime::collect_tier_counts`].
struct TierCounts {
    l0_dirs: usize,
    l1_dirs: usize,
    l2_dirs: usize,
    l3_dirs: usize,
    l0_candidates: usize,
    l0_watch_cost: u64,
    l1_watch_cost: u64,
    l2_watch_cost: u64,
    l3_watch_cost: u64,
    pending_promotions: usize,
    next_scan_unix_secs: u64,
    event_score_total: u64,
    scan_backlog_by_tier: [usize; 4],
}

/// Freshness breakdown collected by [`TieredWatchRuntime::collect_freshness_counts`].
struct FreshnessCounts {
    fresh_dirs: usize,
    scanned_fresh_dirs: usize,
    eventually_consistent_dirs: usize,
    stale_dirs: usize,
    dirty_dirs: usize,
    unknown_dirs: usize,
}

/// Index-residency breakdown collected by [`TieredWatchRuntime::collect_residency_counts`].
struct ResidencyCounts {
    hot_memory_dirs: usize,
    warm_memory_dirs: usize,
    cold_mmap_dirs: usize,
    frozen_manifest_dirs: usize,
}

/// Fast-scan telemetry snapshot collected by [`TieredWatchRuntime::collect_fast_scan_summary`].
struct FastScanSummary {
    fast_scan_enabled: bool,
    fast_scan_target_secs: u64,
    fast_scan_tick_ms: u64,
    fast_scan_mode: String,
    fast_scan_sla_ok: bool,
    fast_scan_local_strict_ok: bool,
    fast_scan_known_dirs: usize,
    fast_scan_local_trusted_dirs: usize,
    fast_scan_untrusted_dirs: usize,
    fast_scan_hotset_lease_count: usize,
    fast_scan_hotset_sentinel_count: usize,
    fast_scan_explicit_lease_count: usize,
    fast_scan_auto_lease_count: usize,
    fast_scan_initial_backfill_pending: usize,
    fast_scan_pending_changed_dirs: usize,
    fast_scan_coverage_lag_p50_ms: u64,
    fast_scan_coverage_lag_p95_ms: u64,
    fast_scan_coverage_lag_p99_ms: u64,
    fast_scan_budget_degraded: bool,
    fast_scan_last_overdue_dir: String,
    fast_scan_last_degraded_reason: String,
}

impl TieredWatchRuntime {
    /// Collect per-tier directory counts, watch costs, scan backlog, and event scores.
    fn collect_tier_counts(&self, now: u64) -> TierCounts {
        let dirs = self.dirs.read();
        let l0_candidates = dirs.len();
        let mut l0_dirs = 0usize;
        let mut l1_dirs = 0usize;
        let mut l2_dirs = 0usize;
        let mut l3_dirs = 0usize;
        let mut pending_promotions = 0usize;
        let mut next_scan_unix_secs = u64::MAX;
        let mut event_score_total = 0u64;
        let mut l0_watch_cost = 0u64;
        let mut l1_watch_cost = 0u64;
        let mut l2_watch_cost = 0u64;
        let mut l3_watch_cost = 0u64;
        let mut scan_backlog_by_tier = [0usize; 4];

        for state in dirs.values() {
            let tier = state.tier();
            let cost = state.watch_cost.load(Ordering::Relaxed);
            match tier {
                WatchTier::L0 => {
                    l0_dirs += 1;
                    l0_watch_cost = l0_watch_cost.saturating_add(cost);
                }
                WatchTier::L1 => {
                    l1_dirs += 1;
                    l1_watch_cost = l1_watch_cost.saturating_add(cost);
                }
                WatchTier::L2 => {
                    l2_dirs += 1;
                    l2_watch_cost = l2_watch_cost.saturating_add(cost);
                }
                WatchTier::L3 => {
                    l3_dirs += 1;
                    l3_watch_cost = l3_watch_cost.saturating_add(cost);
                }
            }
            if state.promotion_pending.load(Ordering::Relaxed) {
                pending_promotions += 1;
            }
            if !matches!(tier, WatchTier::L0) {
                let next_scan = state.next_scan_unix_secs.load(Ordering::Relaxed);
                if next_scan > 0 {
                    next_scan_unix_secs = next_scan_unix_secs.min(next_scan);
                }
                if next_scan <= now {
                    scan_backlog_by_tier[tier.as_u8() as usize] += 1;
                }
            }
            event_score_total =
                event_score_total.saturating_add(state.event_score.load(Ordering::Relaxed));
        }

        TierCounts {
            l0_dirs,
            l1_dirs,
            l2_dirs,
            l3_dirs,
            l0_candidates,
            l0_watch_cost,
            l1_watch_cost,
            l2_watch_cost,
            l3_watch_cost,
            pending_promotions,
            next_scan_unix_secs,
            event_score_total,
            scan_backlog_by_tier,
        }
    }

    /// Collect freshness statistics (fresh / stale / dirty / unknown / L3-scanned).
    fn collect_freshness_counts(&self) -> FreshnessCounts {
        let dirs = self.dirs.read();
        let mut fresh_dirs = 0usize;
        let mut scanned_fresh_dirs = 0usize;
        let mut eventually_consistent_dirs = 0usize;
        let mut stale_dirs = 0usize;
        let mut dirty_dirs = 0usize;
        let mut unknown_dirs = 0usize;

        for state in dirs.values() {
            let tier = state.tier();
            let freshness = state.freshness();
            if tier == WatchTier::L3 {
                eventually_consistent_dirs += 1;
                if freshness == Freshness::Fresh {
                    scanned_fresh_dirs += 1;
                }
            }
            match freshness {
                Freshness::Fresh => fresh_dirs += 1,
                Freshness::Stale => stale_dirs += 1,
                Freshness::Dirty => dirty_dirs += 1,
                Freshness::Unknown => unknown_dirs += 1,
            }
        }

        FreshnessCounts {
            fresh_dirs,
            scanned_fresh_dirs,
            eventually_consistent_dirs,
            stale_dirs,
            dirty_dirs,
            unknown_dirs,
        }
    }

    /// Collect index-residency statistics (hot / warm / cold-mmap / frozen-manifest).
    fn collect_residency_counts(&self) -> ResidencyCounts {
        let dirs = self.dirs.read();
        let mut hot_memory_dirs = 0usize;
        let mut warm_memory_dirs = 0usize;
        let mut cold_mmap_dirs = 0usize;
        let mut frozen_manifest_dirs = 0usize;

        for state in dirs.values() {
            match state.index_residency() {
                IndexResidency::HotMemory => hot_memory_dirs += 1,
                IndexResidency::WarmMemory => warm_memory_dirs += 1,
                IndexResidency::ColdMmap => cold_mmap_dirs += 1,
                IndexResidency::FrozenManifestOnly => frozen_manifest_dirs += 1,
            }
        }

        ResidencyCounts {
            hot_memory_dirs,
            warm_memory_dirs,
            cold_mmap_dirs,
            frozen_manifest_dirs,
        }
    }

    /// Collect fast-scan telemetry: lease/sentinel counts, coverage lags, SLA status.
    fn collect_fast_scan_summary(&self) -> FastScanSummary {
        let fast_scan_enabled = self.fast_scan_enabled.load(Ordering::Relaxed);
        let fast_scan_target_secs = self.fast_scan_target_secs.load(Ordering::Relaxed);
        let fast_scan_tick_ms = self.fast_scan_tick_ms.load(Ordering::Relaxed);
        let fast_scan_target_ms = fast_scan_target_secs.saturating_mul(1_000);
        let fast_scan_network_mode =
            network_fast_scan_mode_from_u8(self.fast_scan_network_mode.load(Ordering::Relaxed));
        let fast_scan_now_ms = unix_millis();
        let fast_state = self.fast_scan_state.read();
        let fast_scan_known_dirs = fast_state.sentinels.len();
        let fast_scan_hotset_lease_count = fast_state.leases.len();
        let fast_scan_hotset_sentinel_count = fast_state.sentinels.len();
        let fast_scan_explicit_lease_count = fast_state
            .leases
            .values()
            .filter(|lease| lease.lease_kind.is_explicit())
            .count();
        let fast_scan_auto_lease_count =
            fast_scan_hotset_lease_count.saturating_sub(fast_scan_explicit_lease_count);
        let fast_scan_local_trusted_dirs = fast_state
            .sentinels
            .values()
            .filter(|sentinel| {
                sentinel.strict_sla_allowed
                    && sentinel.sentinel_state == FastScanSentinelState::Active
            })
            .count();
        let fast_scan_untrusted_dirs =
            fast_scan_known_dirs.saturating_sub(fast_scan_local_trusted_dirs);
        let fast_scan_pending_changed_dirs = fast_state.changed_dir_queue.len();
        let fast_scan_initial_backfill_pending = fast_state.initial_backfill_queue.len();
        let fast_scan_uncovered_lease_count = fast_state
            .leases
            .keys()
            .filter(|path| {
                !fast_state.sentinels.contains_key(path.as_path())
                    && !matches!(self.covering_tier(path.as_path()), Some(WatchTier::L0))
            })
            .count();
        let mut fast_lags = Vec::with_capacity(fast_scan_known_dirs);
        let mut last_overdue_dir = String::new();
        let mut max_lag = 0u64;
        for (path, sentinel) in fast_state.sentinels.iter() {
            if sentinel.sentinel_state != FastScanSentinelState::Active {
                continue;
            }
            let lag = if sentinel.last_checked_unix_ms == 0 {
                fast_scan_target_ms
            } else {
                fast_scan_now_ms.saturating_sub(sentinel.last_checked_unix_ms)
            };
            if lag > max_lag {
                max_lag = lag;
                last_overdue_dir = path.to_string_lossy().to_string();
            }
            fast_lags.push(lag);
        }
        fast_lags.sort_unstable();
        let fast_scan_coverage_lag_p50_ms = percentile_ms(&fast_lags, 50);
        let fast_scan_coverage_lag_p95_ms = percentile_ms(&fast_lags, 95);
        let fast_scan_coverage_lag_p99_ms = percentile_ms(&fast_lags, 99);
        let fast_scan_last_degraded_reason = fast_state.last_degraded_reason.clone();
        drop(fast_state);

        let fast_scan_budget_degraded = self.fast_scan_budget_degraded.load(Ordering::Relaxed);
        let fast_scan_backfill_pending =
            fast_scan_initial_backfill_pending > 0 || fast_scan_uncovered_lease_count > 0;
        let fast_scan_local_strict_ok = !fast_scan_enabled
            || (!fast_scan_backfill_pending
                && (fast_scan_local_trusted_dirs == 0
                    || (!fast_scan_budget_degraded
                        && fast_scan_coverage_lag_p99_ms <= fast_scan_target_ms)));
        let fast_scan_sla_ok = fast_scan_local_strict_ok && fast_scan_untrusted_dirs == 0;
        let fast_scan_mode = fast_scan_mode_label(
            fast_scan_enabled,
            fast_scan_local_trusted_dirs,
            fast_scan_untrusted_dirs,
            fast_scan_network_mode,
        )
        .to_string();

        FastScanSummary {
            fast_scan_enabled,
            fast_scan_target_secs,
            fast_scan_tick_ms,
            fast_scan_mode,
            fast_scan_sla_ok,
            fast_scan_local_strict_ok,
            fast_scan_known_dirs,
            fast_scan_local_trusted_dirs,
            fast_scan_untrusted_dirs,
            fast_scan_hotset_lease_count,
            fast_scan_hotset_sentinel_count,
            fast_scan_explicit_lease_count,
            fast_scan_auto_lease_count,
            fast_scan_initial_backfill_pending,
            fast_scan_pending_changed_dirs,
            fast_scan_coverage_lag_p50_ms,
            fast_scan_coverage_lag_p95_ms,
            fast_scan_coverage_lag_p99_ms,
            fast_scan_budget_degraded,
            fast_scan_last_overdue_dir: last_overdue_dir,
            fast_scan_last_degraded_reason,
        }
    }

    pub fn report(&self) -> WatchStateReport {
        let now = unix_secs();
        let tc = self.collect_tier_counts(now);
        let fc = self.collect_freshness_counts();
        let rc = self.collect_residency_counts();

        let ephemeral = self.ephemeral.read();
        let ephemeral_watch_dirs = ephemeral
            .values()
            .filter(|lease| !lease.pending_add && !lease.pending_remove)
            .count();
        let ephemeral_watch_cost = self.current_ephemeral_watch_cost.load(Ordering::Relaxed);
        drop(ephemeral);

        let fs = self.collect_fast_scan_summary();

        let mut notes = vec![
            "tiered runtime controls L0/L1/L2/L3 hotness scheduling".to_string(),
            "cold L0 directories can be replaced when a hotter candidate needs budget".to_string(),
        ];
        if self.ephemeral_watch_budget > 0 {
            notes.push(format!(
                "ephemeral watcher leases active={}/{} cost={}/{}",
                ephemeral_watch_dirs,
                self.ephemeral_watch_created.load(Ordering::Relaxed),
                ephemeral_watch_cost,
                self.ephemeral_watch_budget
            ));
        }
        if tc.pending_promotions > 0 {
            notes.push(format!(
                "{} promotion(s) are waiting for watcher command completion",
                tc.pending_promotions
            ));
        }
        let blocked = self.promotion_budget_blocked.load(Ordering::Relaxed);
        let watch_mount_policy_rejected = self.watch_mount_policy_rejected.load(Ordering::Relaxed);
        let watch_exclude_rejected = self.watch_exclude_rejected.load(Ordering::Relaxed);
        let last_budget_blocked_kernel_watch_cost = self
            .last_budget_blocked_kernel_watch_cost
            .load(Ordering::Relaxed);
        let last_budget_blocked_budget_remaining = self
            .last_budget_blocked_budget_remaining
            .load(Ordering::Relaxed);
        let last_budget_blocked_reason = self.last_budget_blocked_reason.read().clone();
        if blocked > 0 {
            notes.push(format!(
                "{} promotion attempt(s) were blocked by watch budget",
                blocked
            ));
        }
        if watch_mount_policy_rejected > 0 {
            notes.push(format!(
                "{} watch candidate(s) were rejected by mount policy",
                watch_mount_policy_rejected
            ));
        }
        if watch_exclude_rejected > 0 {
            notes.push(format!(
                "{} watch candidate(s) were rejected by exclude rules",
                watch_exclude_rejected
            ));
        }
        if !last_budget_blocked_reason.is_empty() {
            notes.push(format!(
                "last budget rejection: {}",
                last_budget_blocked_reason
            ));
        }
        if fs.fast_scan_enabled {
            notes.push(format!(
                "fast scan mode={} hotset_leases={} hotset_sentinels={} local_trusted={} untrusted={} target_secs={}",
                fs.fast_scan_mode,
                fs.fast_scan_hotset_lease_count,
                fs.fast_scan_hotset_sentinel_count,
                fs.fast_scan_local_trusted_dirs,
                fs.fast_scan_untrusted_dirs,
                fs.fast_scan_target_secs
            ));
            notes.push("fast scan SLA applies to active lease hotset; cold dirs are bounded by cold_sweep_period_estimate and dirty_backlog".to_string());
        }
        if fs.fast_scan_budget_degraded && !fs.fast_scan_last_degraded_reason.is_empty() {
            notes.push(format!(
                "fast scan degraded: {}",
                fs.fast_scan_last_degraded_reason
            ));
        }
        if self.proc_sampler_enabled.load(Ordering::Relaxed) {
            notes.push(format!(
                "proc sampler write-fd dirs={} triggered_ephemeral={}",
                self.proc_sampler_sampled_dirs.load(Ordering::Relaxed),
                self.proc_sampler_triggered_watches.load(Ordering::Relaxed)
            ));
        }
        let watched_dirs_estimated = self.current_watch_cost.load(Ordering::Relaxed) as usize;
        let logical_watch_cost = tc
            .l0_watch_cost
            .saturating_add(tc.l1_watch_cost)
            .saturating_add(tc.l2_watch_cost)
            .saturating_add(tc.l3_watch_cost);
        let kernel_watch_cost = watched_dirs_estimated as u64;
        let skipped_watch_cost = logical_watch_cost.saturating_sub(kernel_watch_cost);
        let watch_budget_utilization_pct = if self.max_watch_dirs == 0 {
            0
        } else {
            ((watched_dirs_estimated as u64)
                .saturating_mul(100)
                .checked_div(self.max_watch_dirs)
                .unwrap_or(0))
            .min(100) as u8
        };

        WatchStateReport {
            mode: "tiered".to_string(),
            backend: "notify".to_string(),
            watch_profile: "runtime".to_string(),
            l0_dirs: tc.l0_dirs,
            l1_dirs: tc.l1_dirs,
            l2_dirs: tc.l2_dirs,
            l3_dirs: tc.l3_dirs,
            watched_dirs_estimated,
            max_watch_dirs: self.max_watch_dirs as usize,
            l0_max_cost_per_root: self.l0_max_cost_per_root as usize,
            system_max_user_watches: 0,
            required_watch_cost: 0,
            watch_budget_shortfall: 0,
            strict_coverage_ok: true,
            strict_coverage_failure: false,
            strict_fail_on_budget_exceeded: false,
            strict_uncovered_dirs: Vec::new(),
            l0_candidates: tc.l0_candidates,
            l0_admitted: tc.l0_dirs,
            l0_rejected: tc.l1_dirs + tc.l2_dirs + tc.l3_dirs,
            scan_backlog: tc.l1_dirs + tc.l2_dirs + tc.l3_dirs,
            scan_items_per_sec: self.scan_items_per_sec,
            scan_ms_per_tick: self.scan_ms_per_tick,
            promotions: self.promotions.load(Ordering::Relaxed),
            demotions: self.demotions.load(Ordering::Relaxed),
            l0_replacements: self.replacements.load(Ordering::Relaxed),
            promotion_budget_blocked: blocked,
            watch_mount_policy_rejected,
            watch_exclude_rejected,
            last_budget_blocked_kernel_watch_cost,
            last_budget_blocked_budget_remaining,
            last_budget_blocked_reason,
            watch_budget_utilization_pct,
            last_adjustment_unix_secs: self.last_adjustment_unix_secs.load(Ordering::Relaxed),
            next_scan_unix_secs: if tc.next_scan_unix_secs == u64::MAX {
                0
            } else {
                tc.next_scan_unix_secs
            },
            event_score_total: tc.event_score_total,
            fresh_dirs: fc.fresh_dirs,
            scanned_fresh_dirs: fc.scanned_fresh_dirs,
            eventually_consistent_dirs: fc.eventually_consistent_dirs,
            stale_dirs: fc.stale_dirs,
            dirty_dirs: fc.dirty_dirs,
            unknown_dirs: fc.unknown_dirs,
            hot_memory_dirs: rc.hot_memory_dirs,
            warm_memory_dirs: rc.warm_memory_dirs,
            cold_mmap_dirs: rc.cold_mmap_dirs,
            frozen_manifest_dirs: rc.frozen_manifest_dirs,
            notes,
            logical_watch_cost,
            kernel_watch_cost,
            skipped_watch_cost,
            l0_watch_cost: tc.l0_watch_cost,
            l1_watch_cost: tc.l1_watch_cost,
            l2_watch_cost: tc.l2_watch_cost,
            l3_watch_cost: tc.l3_watch_cost,
            ephemeral_watch_cost,
            ephemeral_watch_budget: self.ephemeral_watch_budget as usize,
            ephemeral_watch_dirs,
            ephemeral_watch_created: self.ephemeral_watch_created.load(Ordering::Relaxed),
            ephemeral_watch_expired: self.ephemeral_watch_expired.load(Ordering::Relaxed),
            ephemeral_watch_evicted: self.ephemeral_watch_evicted.load(Ordering::Relaxed),
            ephemeral_watch_budget_blocked: self
                .ephemeral_watch_budget_blocked
                .load(Ordering::Relaxed),
            scan_backlog_by_tier: tc.scan_backlog_by_tier,
            dirty_queue_len: self.dirty_queue_len.load(Ordering::Relaxed),
            cold_validate_count: self.cold_validate_count.load(Ordering::Relaxed),
            query_stale_hit_count: self.query_stale_hit_count.load(Ordering::Relaxed),
            query_permission_denied_count: self
                .query_permission_denied_count
                .load(Ordering::Relaxed),
            directory_manifest_dirs: 0,
            directory_manifest_skipped_scans: 0,
            directory_manifest_changed_scans: 0,
            directory_manifest_untrusted_clock_bypass: 0,
            fast_scan_enabled: fs.fast_scan_enabled,
            fast_scan_mode: fs.fast_scan_mode,
            fast_scan_sla_ok: fs.fast_scan_sla_ok,
            fast_scan_local_strict_ok: fs.fast_scan_local_strict_ok,
            fast_scan_target_secs: fs.fast_scan_target_secs,
            fast_scan_tick_ms: fs.fast_scan_tick_ms,
            fast_scan_known_dirs: fs.fast_scan_known_dirs,
            fast_scan_local_trusted_dirs: fs.fast_scan_local_trusted_dirs,
            fast_scan_untrusted_dirs: fs.fast_scan_untrusted_dirs,
            fast_scan_hotset_lease_count: fs.fast_scan_hotset_lease_count,
            fast_scan_hotset_sentinel_count: fs.fast_scan_hotset_sentinel_count,
            fast_scan_explicit_lease_count: fs.fast_scan_explicit_lease_count,
            fast_scan_auto_lease_count: fs.fast_scan_auto_lease_count,
            fast_scan_lease_evictions: self.fast_scan_lease_evictions.load(Ordering::Relaxed),
            fast_scan_lease_renewals: self.fast_scan_lease_renewals.load(Ordering::Relaxed),
            fast_scan_initial_backfill_pending: fs.fast_scan_initial_backfill_pending,
            fast_scan_real_changed_dirs: self.fast_scan_real_changed_dirs.load(Ordering::Relaxed),
            fast_scan_apply_dropped_stale_batches: self
                .fast_scan_apply_dropped_stale_batches
                .load(Ordering::Relaxed),
            fast_scan_scan_workers_active: self
                .fast_scan_scan_workers_active
                .load(Ordering::Relaxed),
            fast_scan_io_budget_limited_count: self
                .fast_scan_io_budget_limited_count
                .load(Ordering::Relaxed),
            fast_scan_pending_changed_dirs: fs.fast_scan_pending_changed_dirs,
            fast_scan_checked_dirs: self.fast_scan_checked_dirs.load(Ordering::Relaxed),
            fast_scan_changed_dirs: self.fast_scan_changed_dirs.load(Ordering::Relaxed),
            fast_scan_generated_events: self.fast_scan_generated_events.load(Ordering::Relaxed),
            fast_scan_coverage_lag_p50_ms: fs.fast_scan_coverage_lag_p50_ms,
            fast_scan_coverage_lag_p95_ms: fs.fast_scan_coverage_lag_p95_ms,
            fast_scan_coverage_lag_p99_ms: fs.fast_scan_coverage_lag_p99_ms,
            fast_scan_budget_degraded: fs.fast_scan_budget_degraded,
            fast_scan_last_overdue_dir: fs.fast_scan_last_overdue_dir,
            fast_scan_last_degraded_reason: fs.fast_scan_last_degraded_reason,
            fast_scan_parent_fence_retries: self
                .fast_scan_parent_fence_retries
                .load(Ordering::Relaxed),
            fast_scan_epoch_conflicts: self.fast_scan_epoch_conflicts.load(Ordering::Relaxed),
            proc_sampler_enabled: self.proc_sampler_enabled.load(Ordering::Relaxed),
            proc_sampler_last_duration_ms: self
                .proc_sampler_last_duration_ms
                .load(Ordering::Relaxed),
            proc_sampler_pids_seen: self.proc_sampler_pids_seen.load(Ordering::Relaxed),
            proc_sampler_pids_scanned: self.proc_sampler_pids_scanned.load(Ordering::Relaxed),
            proc_sampler_pids_denied: self.proc_sampler_pids_denied.load(Ordering::Relaxed),
            proc_sampler_fdinfo_read_count: self
                .proc_sampler_fdinfo_read_count
                .load(Ordering::Relaxed),
            proc_sampler_readlink_count: self.proc_sampler_readlink_count.load(Ordering::Relaxed),
            proc_sampler_write_fd_count: self.proc_sampler_write_fd_count.load(Ordering::Relaxed),
            proc_sampler_sampled_dirs: self.proc_sampler_sampled_dirs.load(Ordering::Relaxed),
            proc_sampler_triggered_watches: self
                .proc_sampler_triggered_watches
                .load(Ordering::Relaxed),
            proc_sampler_budget_exhausted: self
                .proc_sampler_budget_exhausted
                .load(Ordering::Relaxed),
            proc_sampler_unavailable: self.proc_sampler_unavailable.load(Ordering::Relaxed),
        }
    }

    pub fn debug_dump(&self, root_filter: Option<&str>) -> TieredWatchDebugDump {
        let dirs = self.dirs.read();
        let filter = root_filter.map(|s| s.to_string());
        let dir_paths = dirs.keys().cloned().collect::<Vec<_>>();
        let l0_paths = dirs
            .iter()
            .filter_map(|(path, state)| {
                if state.tier() == WatchTier::L0 {
                    Some(path.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let mut entries = Vec::new();
        let mut l0_dirs = 0usize;
        let mut l1_dirs = 0usize;
        let mut l2_dirs = 0usize;
        let mut l3_dirs = 0usize;
        let mut total_event_score = 0u64;
        let ephemeral_paths = self
            .ephemeral
            .read()
            .values()
            .filter(|lease| !lease.pending_remove)
            .map(|lease| lease.path.clone())
            .collect::<HashSet<_>>();

        for (path, state) in dirs.iter() {
            if let Some(ref prefix) = filter {
                if !path.to_string_lossy().starts_with(prefix) {
                    continue;
                }
            }
            let tier = state.tier();
            let cost = state.watch_cost.load(Ordering::Relaxed);
            let event_score = state.event_score.load(Ordering::Relaxed);
            let last_event = state.last_event_unix_secs.load(Ordering::Relaxed);
            let last_scan = state.last_scan_unix_secs.load(Ordering::Relaxed);
            let empty_scan_count = state.empty_scan_count.load(Ordering::Relaxed);
            let promotion_pending = state.promotion_pending.load(Ordering::Relaxed);
            let demotion_pending = state.demotion_pending.load(Ordering::Relaxed);
            let next_scan_unix_secs = state.next_scan_unix_secs.load(Ordering::Relaxed);
            let budget_blocked_count = state.budget_blocked_count.load(Ordering::Relaxed);
            let last_budget_blocked_unix_secs =
                state.last_budget_blocked_unix_secs.load(Ordering::Relaxed);
            let dirty = state.dirty.load(Ordering::Relaxed);
            let high_priority_scan = state.high_priority_scan.load(Ordering::Relaxed);
            let nearest_ancestor_root = nearest_ancestor_root(path.as_path(), &dir_paths);
            let descendant_roots = descendant_roots(path.as_path(), &dir_paths);
            let l0_covering_root = nearest_covering_root(path.as_path(), &l0_paths);
            let budget_isolated_from_ancestor = nearest_ancestor_root.is_some();
            let nested_relation = nested_relation(
                path.as_path(),
                nearest_ancestor_root.as_deref(),
                !descendant_roots.is_empty(),
                l0_covering_root.as_deref(),
            );

            match tier {
                WatchTier::L0 => l0_dirs += 1,
                WatchTier::L1 => l1_dirs += 1,
                WatchTier::L2 => l2_dirs += 1,
                WatchTier::L3 => l3_dirs += 1,
            }
            total_event_score = total_event_score.saturating_add(event_score);

            entries.push(TieredWatchDebugDir {
                path: path.to_string_lossy().to_string(),
                watch_tier: format!("{:?}", tier),
                index_tier: format!("{:?}", state.index_residency()),
                watch_cost: cost,
                event_score,
                last_event,
                last_scan,
                empty_scan_count,
                promotion_pending,
                demotion_pending,
                dirty,
                freshness: display_freshness(tier, state.freshness()).to_string(),
                next_scan_unix_secs,
                budget_blocked_count,
                last_budget_blocked_unix_secs,
                high_priority_scan,
                ephemeral_watch: ephemeral_paths.contains(path),
                nearest_ancestor_root,
                descendant_roots,
                l0_covering_root,
                budget_isolated_from_ancestor,
                nested_relation,
            });
        }

        entries.sort_by(|a, b| a.path.cmp(&b.path));

        TieredWatchDebugDump {
            dirs: entries,
            summary: TieredWatchDebugSummary {
                l0_dirs,
                l1_dirs,
                l2_dirs,
                l3_dirs,
                ephemeral_watch_dirs: ephemeral_paths.len(),
                ephemeral_watch_cost: self.current_ephemeral_watch_cost.load(Ordering::Relaxed),
                ephemeral_watch_budget: self.ephemeral_watch_budget as usize,
                total_event_score,
            },
        }
    }
}

pub(super) fn nearest_ancestor_root(path: &Path, roots: &[PathBuf]) -> Option<String> {
    roots
        .iter()
        .filter(|root| root.as_path() != path && path_is_under_or_equal(path, root.as_path()))
        .max_by_key(|root| root.as_os_str().as_encoded_bytes().len())
        .map(|root| root.to_string_lossy().to_string())
}

pub(super) fn nearest_covering_root(path: &Path, roots: &[PathBuf]) -> Option<String> {
    roots
        .iter()
        .filter(|root| path_is_under_or_equal(path, root.as_path()))
        .max_by_key(|root| root.as_os_str().as_encoded_bytes().len())
        .map(|root| root.to_string_lossy().to_string())
}

pub(super) fn descendant_roots(path: &Path, roots: &[PathBuf]) -> Vec<String> {
    let mut descendants = roots
        .iter()
        .filter(|root| root.as_path() != path && path_is_under_or_equal(root.as_path(), path))
        .map(|root| root.to_string_lossy().to_string())
        .collect::<Vec<_>>();
    descendants.sort();
    descendants
}

pub(super) fn nested_relation(
    path: &Path,
    nearest_ancestor_root: Option<&str>,
    has_descendants: bool,
    l0_covering_root: Option<&str>,
) -> String {
    if let Some(l0_root) = l0_covering_root {
        let path = path.to_string_lossy();
        if l0_root != path.as_ref() {
            return "covered_by_l0_ancestor".to_string();
        }
    }
    if nearest_ancestor_root.is_some() {
        return "nested_under_cold_root".to_string();
    }
    if has_descendants {
        return "ancestor_of_nested_roots".to_string();
    }
    "standalone".to_string()
}

pub(super) fn display_freshness(tier: WatchTier, freshness: Freshness) -> &'static str {
    match (tier, freshness) {
        (WatchTier::L3, Freshness::Fresh) => "ScannedFresh",
        (WatchTier::L3, Freshness::Stale | Freshness::Unknown) => "EventuallyConsistent",
        (_, Freshness::Fresh) => "Fresh",
        (_, Freshness::Stale) => "Stale",
        (_, Freshness::Dirty) => "Dirty",
        (_, Freshness::Unknown) => "Unknown",
    }
}

pub(super) fn percentile_ms(values: &[u64], percentile: usize) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let pct = percentile.min(100);
    let idx = ((values.len().saturating_sub(1)) * pct).div_ceil(100);
    values[idx]
}
