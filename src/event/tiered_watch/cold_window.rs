//! Rotating cold window logic: tier rotation, scan batching, promotion/demotion.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;

use crate::config::{L3ScanPolicy, TieredWatchConfig};
use crate::index::tiered::ScanOutcome;
use crate::util::unix_secs;

use super::path_is_under_or_equal;

use super::types::*;
use super::{DirState, TieredWatchRuntime};

impl TieredWatchRuntime {
    pub fn expired_l0(&self, idle_ttl_secs: u64) -> Vec<PathBuf> {
        let now = unix_secs();
        let dirs = self.dirs.read();
        dirs.iter()
            .filter_map(|(path, state)| {
                if state.tier() != WatchTier::L0 {
                    return None;
                }
                if state.demotion_pending.load(Ordering::Relaxed) {
                    return None;
                }
                let last_event = state.last_event_unix_secs.load(Ordering::Relaxed);
                if last_event > 0 && now.saturating_sub(last_event) > idle_ttl_secs {
                    Some(path.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn decay_scores(&self, now: u64, decay_interval_secs: u64, decay_amount: u64) {
        let dirs = self.dirs.read();
        for state in dirs.values() {
            let last_update = state.last_score_update_unix_secs.load(Ordering::Relaxed);
            if now.saturating_sub(last_update) >= decay_interval_secs {
                state
                    .event_score
                    .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |score| {
                        Some(score.saturating_sub(decay_amount))
                    })
                    .ok();
                state
                    .last_score_update_unix_secs
                    .store(now, Ordering::Relaxed);
            }
        }
    }

    pub fn scan_batch(&self, limit: usize) -> Vec<PathBuf> {
        let now = unix_secs();
        let dirs = self.dirs.read();
        let mut candidates = dirs
            .iter()
            .filter_map(|(path, state)| {
                let tier = state.tier();
                if !matches!(tier, WatchTier::L1 | WatchTier::L2 | WatchTier::L3) {
                    return None;
                }
                if state.promotion_pending.load(Ordering::Relaxed)
                    || state.demotion_pending.load(Ordering::Relaxed)
                {
                    return None;
                }
                let next_scan = state.next_scan_unix_secs.load(Ordering::Relaxed);
                if next_scan > now {
                    return None;
                }
                Some((
                    tier.as_u8(),
                    next_scan,
                    std::cmp::Reverse(if state.high_priority_scan.load(Ordering::Relaxed) {
                        1u8
                    } else {
                        0u8
                    }),
                    std::cmp::Reverse(state.budget_blocked_count.load(Ordering::Relaxed)),
                    std::cmp::Reverse(state.event_score.load(Ordering::Relaxed)),
                    path.clone(),
                ))
            })
            .collect::<Vec<_>>();
        candidates.sort_by_key(|(tier, next_scan, high_pri, blocked, score, path)| {
            (*tier, *next_scan, *high_pri, *blocked, *score, path.clone())
        });
        candidates
            .into_iter()
            .take(limit)
            .map(|(_, _, _, _, _, path)| path)
            .collect()
    }

    pub fn l1_batch(&self, limit: usize) -> Vec<PathBuf> {
        self.scan_batch(limit)
    }

    pub fn mark_demotion_pending(&self, path: &Path) -> bool {
        let Some(state) = self.state(path) else {
            return false;
        };
        if state.tier() != WatchTier::L0 {
            return false;
        }
        !state.demotion_pending.swap(true, Ordering::AcqRel)
    }

    pub fn confirm_demoted(&self, path: &Path) {
        if let Some(state) = self.state(path) {
            state.tier.store(WatchTier::L1.as_u8(), Ordering::Release);
            state.demotion_pending.store(false, Ordering::Release);
            state.set_index_residency(IndexResidency::WarmMemory);
            state.empty_scan_count.store(0, Ordering::Relaxed);
            state
                .next_scan_unix_secs
                .store(unix_secs(), Ordering::Relaxed);
            let cost = state.watch_cost.load(Ordering::Relaxed);
            let _ = self.current_watch_cost.fetch_update(
                Ordering::AcqRel,
                Ordering::Relaxed,
                |current| Some(current.saturating_sub(cost)),
            );
            self.demotions.fetch_add(1, Ordering::Relaxed);
            self.last_adjustment_unix_secs
                .store(unix_secs(), Ordering::Relaxed);
        }
    }

    pub fn rollback_demote(&self, path: &Path) {
        if let Some(state) = self.state(path) {
            state.demotion_pending.store(false, Ordering::Release);
        }
    }

    pub fn register_dynamic_candidate(
        &self,
        path: PathBuf,
        watch_cost: usize,
    ) -> PromotionDecision {
        let now = unix_secs();
        let state = {
            let mut dirs = self.dirs.write();
            dirs.entry(path.clone())
                .or_insert_with(|| Arc::new(DirState::new(WatchTier::L1, watch_cost, now)))
                .clone()
        };

        state.last_event_unix_secs.store(now, Ordering::Relaxed);
        state
            .event_score
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |score| {
                Some(score.saturating_add(32).min(10_000))
            })
            .ok();

        if matches!(state.tier(), WatchTier::L0) {
            return PromotionDecision::NotEligible;
        }
        if state.promotion_pending.load(Ordering::Relaxed) {
            return PromotionDecision::NotEligible;
        }

        state.watch_cost.store(watch_cost as u64, Ordering::Relaxed);

        self.try_reserve_promotion(path.as_path())
    }

    pub fn register_project_marker_candidate(
        &self,
        path: PathBuf,
        watch_cost: usize,
    ) -> PromotionDecision {
        let now = unix_secs();
        let state = {
            let mut dirs = self.dirs.write();
            dirs.entry(path.clone())
                .or_insert_with(|| Arc::new(DirState::new(WatchTier::L1, watch_cost, now)))
                .clone()
        };

        state.last_event_unix_secs.store(now, Ordering::Relaxed);
        state.next_scan_unix_secs.store(0, Ordering::Relaxed);
        state.high_priority_scan.store(true, Ordering::Relaxed);
        state
            .event_score
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |score| {
                Some(score.saturating_add(96).min(10_000))
            })
            .ok();

        if matches!(state.tier(), WatchTier::L0) {
            return PromotionDecision::NotEligible;
        }
        if state.promotion_pending.load(Ordering::Relaxed) {
            return PromotionDecision::NotEligible;
        }

        state.watch_cost.store(watch_cost as u64, Ordering::Relaxed);

        self.grant_fast_scan_lease(path.clone(), FastScanLeaseKind::ProjectMarker, None, 4);
        self.try_reserve_promotion(path.as_path())
    }

    pub fn record_scan(&self, path: &Path, outcome: ScanOutcome) {
        self.record_scan_inner(path, outcome, false);
    }

    pub(super) fn record_scan_inner(
        &self,
        path: &Path,
        outcome: ScanOutcome,
        manifest_skipped: bool,
    ) {
        if let Some(state) = self.state(path) {
            let now = unix_secs();
            if !manifest_skipped && matches!(state.tier(), WatchTier::L2 | WatchTier::L3) {
                self.cold_validate_count.fetch_add(1, Ordering::Relaxed);
            }
            state.last_scan_unix_secs.store(now, Ordering::Relaxed);
            state
                .last_changed_count
                .store(outcome.changed as u64, Ordering::Relaxed);
            if outcome.changed == 0 {
                state.empty_scan_count.fetch_add(1, Ordering::Relaxed);
                state
                    .event_score
                    .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |score| {
                        Some(score.saturating_sub(1))
                    })
                    .ok();
            } else {
                state.empty_scan_count.store(0, Ordering::Relaxed);
                state
                    .event_score
                    .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |score| {
                        Some(
                            score
                                .saturating_add((outcome.changed as u64).saturating_mul(4))
                                .saturating_add(16)
                                .min(10_000),
                        )
                    })
                    .ok();
            }
            state.dirty.store(false, Ordering::Relaxed);
            state.set_freshness(Freshness::Fresh);
        }
    }

    pub fn record_scan_for_path(&self, path: &Path, outcome: ScanOutcome) -> Option<PathBuf> {
        self.record_scan_for_path_with_manifest_status(path, outcome, false)
    }

    pub fn record_scan_for_path_with_manifest_status(
        &self,
        path: &Path,
        outcome: ScanOutcome,
        manifest_skipped: bool,
    ) -> Option<PathBuf> {
        let target = {
            let dirs = self.dirs.read();
            dirs.iter()
                .filter(|(root, _)| path_is_under_or_equal(path, root))
                .max_by_key(|(root, _)| root.as_os_str().as_encoded_bytes().len())
                .map(|(root, _)| root.clone())
        }?;
        self.record_scan_inner(target.as_path(), outcome, manifest_skipped);
        Some(target)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn apply_scan_policy(
        &self,
        path: &Path,
        l1_interval_secs: u64,
        l2_interval_secs: u64,
        l3_scan_policy: L3ScanPolicy,
        l3_interval_secs: u64,
        l1_empty_scans_to_l2: u32,
        l2_empty_scans_to_l3: u32,
    ) {
        let Some(state) = self.state(path) else {
            return;
        };
        let now = unix_secs();
        let changed = state.last_changed_count.load(Ordering::Relaxed);
        let empty_scans = state.empty_scan_count.load(Ordering::Relaxed);
        let tier = state.tier();

        if changed > 0 {
            if matches!(tier, WatchTier::L2 | WatchTier::L3) {
                state.tier.store(WatchTier::L1.as_u8(), Ordering::Release);
                state.set_index_residency(IndexResidency::WarmMemory);
                state.empty_scan_count.store(0, Ordering::Relaxed);
                self.last_adjustment_unix_secs.store(now, Ordering::Relaxed);
            }
            state.next_scan_unix_secs.store(
                now.saturating_add(l1_interval_secs.max(1)),
                Ordering::Relaxed,
            );
            return;
        }

        match tier {
            WatchTier::L1 if empty_scans >= l1_empty_scans_to_l2.max(1) => {
                state.tier.store(WatchTier::L2.as_u8(), Ordering::Release);
                state.set_index_residency(IndexResidency::ColdMmap);
                state.empty_scan_count.store(0, Ordering::Relaxed);
                state.next_scan_unix_secs.store(
                    now.saturating_add(l2_interval_secs.max(l1_interval_secs).max(1)),
                    Ordering::Relaxed,
                );
                self.last_adjustment_unix_secs.store(now, Ordering::Relaxed);
            }
            WatchTier::L2 if empty_scans >= l2_empty_scans_to_l3.max(1) => {
                state.tier.store(WatchTier::L3.as_u8(), Ordering::Release);
                state.set_index_residency(IndexResidency::FrozenManifestOnly);
                state.empty_scan_count.store(0, Ordering::Relaxed);
                state.next_scan_unix_secs.store(
                    next_l3_scan_unix_secs(now, l3_scan_policy, l3_interval_secs),
                    Ordering::Relaxed,
                );
                self.last_adjustment_unix_secs.store(now, Ordering::Relaxed);
            }
            WatchTier::L1 => {
                state.next_scan_unix_secs.store(
                    now.saturating_add(l1_interval_secs.max(1)),
                    Ordering::Relaxed,
                );
            }
            WatchTier::L2 => {
                state.next_scan_unix_secs.store(
                    now.saturating_add(l2_interval_secs.max(l1_interval_secs).max(1)),
                    Ordering::Relaxed,
                );
            }
            WatchTier::L3 => {
                state.next_scan_unix_secs.store(
                    next_l3_scan_unix_secs(now, l3_scan_policy, l3_interval_secs),
                    Ordering::Relaxed,
                );
            }
            WatchTier::L0 => {}
        }
    }

    pub fn try_reserve_promotion(&self, path: &Path) -> PromotionDecision {
        let Some(state) = self.state(path) else {
            return PromotionDecision::NotEligible;
        };
        if matches!(state.tier(), WatchTier::L0) {
            return PromotionDecision::NotEligible;
        }
        if state.promotion_pending.swap(true, Ordering::AcqRel) {
            return PromotionDecision::NotEligible;
        }

        let cost = state.watch_cost.load(Ordering::Relaxed);
        if self.l0_per_root_guard_active() && cost > self.l0_max_cost_per_root.max(1) {
            let now = unix_secs();
            state.promotion_pending.store(false, Ordering::Release);
            let blocked_count = state
                .budget_blocked_count
                .fetch_add(1, Ordering::Relaxed)
                .saturating_add(1);
            state
                .last_budget_blocked_unix_secs
                .store(now, Ordering::Relaxed);
            if blocked_count > 1 {
                state.high_priority_scan.store(true, Ordering::Relaxed);
            }
            self.promotion_budget_blocked
                .fetch_add(1, Ordering::Relaxed);
            self.record_last_budget_blocked(
                format!(
                    "promotion per-root cost blocked: kernel_watch_cost={} l0_max_cost_per_root={}",
                    cost, self.l0_max_cost_per_root
                ),
                cost,
                self.l0_max_cost_per_root,
            );
            return PromotionDecision::BudgetBlocked;
        }
        let reserved = self
            .current_watch_cost
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |current| {
                if current.saturating_add(cost) <= self.max_watch_dirs {
                    Some(current.saturating_add(cost))
                } else {
                    None
                }
            })
            .is_ok();

        if reserved {
            PromotionDecision::SendAdd
        } else if let Some(victim) = self.reserve_by_replacing_cold_l0(path, cost) {
            PromotionDecision::Replace {
                demote: victim,
                promote: path.to_path_buf(),
            }
        } else {
            let current = self.current_watch_cost.load(Ordering::Relaxed);
            let remaining = self.max_watch_dirs.saturating_sub(current);
            let now = unix_secs();
            state.promotion_pending.store(false, Ordering::Release);
            let blocked_count = state
                .budget_blocked_count
                .fetch_add(1, Ordering::Relaxed)
                .saturating_add(1);
            state
                .last_budget_blocked_unix_secs
                .store(now, Ordering::Relaxed);
            if blocked_count > 1 {
                state.high_priority_scan.store(true, Ordering::Relaxed);
            }
            self.promotion_budget_blocked
                .fetch_add(1, Ordering::Relaxed);
            self.record_last_budget_blocked(
                format!(
                    "promotion budget blocked: kernel_watch_cost={} budget_remaining={}",
                    cost, remaining
                ),
                cost,
                remaining,
            );
            PromotionDecision::BudgetBlocked
        }
    }

    pub(super) fn reserve_by_replacing_cold_l0(
        &self,
        promote: &Path,
        promote_cost: u64,
    ) -> Option<PathBuf> {
        let now = unix_secs();
        let dirs = self.dirs.read();
        let promote_score = dirs
            .get(promote)
            .map(|state| state.event_score.load(Ordering::Relaxed))
            .unwrap_or(0);
        let current = self.current_watch_cost.load(Ordering::Relaxed);
        let mut candidates = dirs
            .iter()
            .filter_map(|(path, state)| {
                if state.tier() != WatchTier::L0
                    || state.demotion_pending.load(Ordering::Relaxed)
                    || state.promotion_pending.load(Ordering::Relaxed)
                {
                    return None;
                }
                if path_is_under_or_equal(promote, path) {
                    return None;
                }
                let victim_cost = state.watch_cost.load(Ordering::Relaxed);
                if current
                    .saturating_sub(victim_cost)
                    .saturating_add(promote_cost)
                    > self.max_watch_dirs
                {
                    return None;
                }
                let victim_score = state.event_score.load(Ordering::Relaxed);
                if victim_score > promote_score.saturating_sub(1) {
                    return None;
                }
                let last_event = state.last_event_unix_secs.load(Ordering::Relaxed);
                Some((
                    victim_score,
                    last_event,
                    path.clone(),
                    state.clone(),
                    victim_cost,
                ))
            })
            .collect::<Vec<_>>();
        drop(dirs);

        candidates
            .sort_by_key(|(score, last_event, path, _, _)| (*score, *last_event, path.clone()));

        for (_, _, path, state, _) in candidates {
            if state.demotion_pending.swap(true, Ordering::AcqRel) {
                continue;
            }
            self.replacements.fetch_add(1, Ordering::Relaxed);
            self.last_adjustment_unix_secs.store(now, Ordering::Relaxed);
            return Some(path);
        }

        None
    }

    pub fn reserve_pending_promotion(&self, path: &Path) -> bool {
        let Some(state) = self.state(path) else {
            return false;
        };
        if !state.promotion_pending.load(Ordering::Relaxed) {
            return false;
        }
        let cost = state.watch_cost.load(Ordering::Relaxed);
        if self.l0_per_root_guard_active() && cost > self.l0_max_cost_per_root.max(1) {
            return false;
        }
        self.current_watch_cost
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |current| {
                if current.saturating_add(cost) <= self.max_watch_dirs {
                    Some(current.saturating_add(cost))
                } else {
                    None
                }
            })
            .is_ok()
    }

    pub(super) fn l0_per_root_guard_active(&self) -> bool {
        self.l0_max_cost_per_root < self.max_watch_dirs
    }

    pub fn cancel_pending_promotion(&self, path: &Path) {
        if let Some(state) = self.state(path) {
            state.promotion_pending.store(false, Ordering::Release);
        }
    }

    pub fn rollback_replacement(&self, demote: &Path, promote: &Path) {
        if let Some(state) = self.state(demote) {
            state.demotion_pending.store(false, Ordering::Release);
        }
        self.cancel_pending_promotion(promote);
    }

    pub fn confirm_promoted(&self, path: &Path) {
        if let Some(state) = self.state(path) {
            state.tier.store(WatchTier::L0.as_u8(), Ordering::Release);
            state.promotion_pending.store(false, Ordering::Release);
            state.set_index_residency(IndexResidency::HotMemory);
            state.empty_scan_count.store(0, Ordering::Relaxed);
            state
                .last_event_unix_secs
                .store(unix_secs(), Ordering::Relaxed);
            state
                .event_score
                .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |score| {
                    Some(score.saturating_add(32).min(10_000))
                })
                .ok();
            self.promotions.fetch_add(1, Ordering::Relaxed);
            self.last_adjustment_unix_secs
                .store(unix_secs(), Ordering::Relaxed);
        }
        self.grant_fast_scan_lease(path.to_path_buf(), FastScanLeaseKind::L0Event, None, 2);
    }

    pub fn rollback_promote(&self, path: &Path) {
        if let Some(state) = self.state(path) {
            let cost = state.watch_cost.load(Ordering::Relaxed);
            let _ = self.current_watch_cost.fetch_update(
                Ordering::AcqRel,
                Ordering::Relaxed,
                |current| Some(current.saturating_sub(cost)),
            );
            state.promotion_pending.store(false, Ordering::Release);
            state.tier.store(WatchTier::L1.as_u8(), Ordering::Release);
        }
    }
}

pub(super) fn next_l3_scan_unix_secs(now: u64, policy: L3ScanPolicy, interval_secs: u64) -> u64 {
    if policy.schedules_periodic_scan() {
        now.saturating_add(interval_secs.max(1))
    } else {
        u64::MAX
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Rotating cold window — RotatingColdWindowLease + tick processing
// ════════════════════════════════════════════════════════════════════════════

#[derive(Debug)]
pub(super) struct RotatingColdWindowLease {
    pub(super) action: RotatingColdWindowActionKind,
    pub(super) expires_unix_secs: u64,
    pub(super) expires_at: Instant,
    pub(super) follow_up_interval_secs: u64,
    pub(super) next_follow_up_at: Instant,
    pub(super) follow_up_scans_remaining: u8,
    pub(super) cycle_id: u64,
    pub(super) score: u64,
    pub(super) watch_cost: u64,
}

impl RotatingColdWindowLease {
    pub(super) fn is_active(&self, now_unix_secs: u64, now: Instant) -> bool {
        self.expires_unix_secs > now_unix_secs && self.expires_at > now
    }
}

fn rotating_cold_window_controls_ephemeral_watch(action: RotatingColdWindowActionKind) -> bool {
    matches!(
        action,
        RotatingColdWindowActionKind::EphemeralWatch | RotatingColdWindowActionKind::ScanOnly
    )
}

fn rotating_scan_follow_up_interval_secs(ttl_secs: u64) -> u64 {
    let quotient = ttl_secs / 5;
    let remainder = ttl_secs % 5;
    quotient.saturating_add(u64::from(remainder != 0)).max(1)
}

// ════════════════════════════════════════════════════════════════════════════
// Waterline alarm — adaptive L3 scan degradation
// ════════════════════════════════════════════════════════════════════════════

/// Number of consecutive below-recovery-threshold checks required to clear
/// soft degradation.
const WATERLINE_SOFT_RECOVERY_CHECKS: u32 = 3;
/// Number of consecutive below-recovery-threshold checks required to clear
/// hard degradation.
const WATERLINE_HARD_RECOVERY_CHECKS: u32 = 5;

/// Immutable configuration snapshot for the waterline alarm.
#[derive(Debug, Clone)]
struct WaterlineAlarmConfig {
    enabled: bool,
    sla_ms: u64,
    soft_trigger_pct: f64,
    soft_recover_pct: f64,
    hard_trigger_pct: f64,
    hard_recover_pct: f64,
    hard_degraded_l3_interval_secs: u64,
    soft_budget_reduction_pct: f64,
}

impl Default for WaterlineAlarmConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            sla_ms: 5_000,
            soft_trigger_pct: 0.8,
            soft_recover_pct: 0.4,
            hard_trigger_pct: 0.8,
            hard_recover_pct: 0.4,
            hard_degraded_l3_interval_secs: 86_400,
            soft_budget_reduction_pct: 0.5,
        }
    }
}

/// Adaptive backpressure mechanism that degrades L3 scanning and the rotating
/// cold-window when the fast-scan lane falls behind.
pub(super) struct WaterlineAlarm {
    config: RwLock<WaterlineAlarmConfig>,
    l2_scan_interval_ms: AtomicU64,
    configured_l3_scan_interval_secs: AtomicU64,
    configured_rotating_budget: AtomicUsize,
    soft_degraded: AtomicBool,
    hard_degraded: AtomicBool,
    soft_recovery_streak: AtomicU32,
    hard_recovery_streak: AtomicU32,
}

impl WaterlineAlarm {
    pub(super) fn new() -> Self {
        Self {
            config: RwLock::new(WaterlineAlarmConfig::default()),
            l2_scan_interval_ms: AtomicU64::new(300_000),
            configured_l3_scan_interval_secs: AtomicU64::new(21_600),
            configured_rotating_budget: AtomicUsize::new(128),
            soft_degraded: AtomicBool::new(false),
            hard_degraded: AtomicBool::new(false),
            soft_recovery_streak: AtomicU32::new(0),
            hard_recovery_streak: AtomicU32::new(0),
        }
    }

    pub(super) fn apply_config(&self, config: &TieredWatchConfig) {
        let wc = WaterlineAlarmConfig {
            enabled: config.waterline_alarm_enabled,
            sla_ms: config.waterline_sla_ms,
            soft_trigger_pct: config.waterline_soft_trigger_pct,
            soft_recover_pct: config.waterline_soft_recover_pct,
            hard_trigger_pct: config.waterline_hard_trigger_pct,
            hard_recover_pct: config.waterline_hard_recover_pct,
            hard_degraded_l3_interval_secs: config.waterline_hard_degraded_l3_interval_secs,
            soft_budget_reduction_pct: config.waterline_soft_budget_reduction_pct,
        };
        *self.config.write() = wc;
        self.l2_scan_interval_ms.store(
            config.l2_scan_interval_secs.saturating_mul(1_000),
            Ordering::Relaxed,
        );
        self.configured_l3_scan_interval_secs
            .store(config.l3_scan_interval_secs, Ordering::Relaxed);
        self.configured_rotating_budget
            .store(config.rotating_cold_window_budget.max(1), Ordering::Relaxed);
    }

    pub(super) fn is_soft_degraded(&self) -> bool {
        self.soft_degraded.load(Ordering::Relaxed)
    }

    pub(super) fn is_hard_degraded(&self) -> bool {
        self.hard_degraded.load(Ordering::Relaxed)
    }

    pub(super) fn effective_l3_scan_interval_secs(&self) -> u64 {
        if self.is_hard_degraded() {
            let cfg = self.config.read();
            cfg.hard_degraded_l3_interval_secs
        } else {
            self.configured_l3_scan_interval_secs
                .load(Ordering::Relaxed)
        }
    }

    pub(super) fn effective_rotating_budget(&self) -> usize {
        let configured = self.configured_rotating_budget.load(Ordering::Relaxed);
        if self.is_soft_degraded() {
            let cfg = self.config.read();
            let reduction = cfg.soft_budget_reduction_pct.clamp(0.0, 1.0);
            let effective = (configured as f64) * (1.0 - reduction);
            (effective.round() as usize).max(1)
        } else {
            configured
        }
    }

    pub(super) fn check(&self, lag_p99_ms: u64) -> u64 {
        let cfg = self.config.read();
        if !cfg.enabled {
            return lag_p99_ms;
        }

        let sla_ms = cfg.sla_ms.max(1);
        let l2_ms = self.l2_scan_interval_ms.load(Ordering::Relaxed).max(1);

        // ── Soft level ──────────────────────────────────────────────────────
        let soft_trigger_ms = (sla_ms as f64 * cfg.soft_trigger_pct) as u64;
        let soft_recover_ms = (sla_ms as f64 * cfg.soft_recover_pct) as u64;

        let was_soft = self.is_soft_degraded();
        if !was_soft && lag_p99_ms > soft_trigger_ms {
            self.soft_degraded.store(true, Ordering::Relaxed);
            self.soft_recovery_streak.store(0, Ordering::Relaxed);
            tracing::warn!(
                lag_p99_ms,
                soft_trigger_ms,
                sla_ms,
                "waterline alarm: soft degradation triggered — \
                 reducing rotating cold-window budget"
            );
        } else if was_soft {
            if lag_p99_ms < soft_recover_ms {
                let streak = self
                    .soft_recovery_streak
                    .fetch_add(1, Ordering::Relaxed)
                    .saturating_add(1);
                if streak >= WATERLINE_SOFT_RECOVERY_CHECKS {
                    self.soft_degraded.store(false, Ordering::Relaxed);
                    self.soft_recovery_streak.store(0, Ordering::Relaxed);
                    tracing::info!(
                        lag_p99_ms,
                        soft_recover_ms,
                        streak,
                        "waterline alarm: soft degradation recovered — \
                         rotating budget restored"
                    );
                }
            } else {
                self.soft_recovery_streak.store(0, Ordering::Relaxed);
            }
        }

        // ── Hard level ──────────────────────────────────────────────────────
        let hard_trigger_ms = (l2_ms as f64 * cfg.hard_trigger_pct) as u64;
        let hard_recover_ms = (l2_ms as f64 * cfg.hard_recover_pct) as u64;

        let was_hard = self.is_hard_degraded();
        if !was_hard && lag_p99_ms > hard_trigger_ms {
            self.hard_degraded.store(true, Ordering::Relaxed);
            self.hard_recovery_streak.store(0, Ordering::Relaxed);
            tracing::error!(
                lag_p99_ms,
                hard_trigger_ms,
                l2_interval_ms = l2_ms,
                "waterline alarm: hard degradation triggered — \
                 L3 scan interval overridden to {}s",
                cfg.hard_degraded_l3_interval_secs
            );
        } else if was_hard {
            if lag_p99_ms < hard_recover_ms {
                let streak = self
                    .hard_recovery_streak
                    .fetch_add(1, Ordering::Relaxed)
                    .saturating_add(1);
                if streak >= WATERLINE_HARD_RECOVERY_CHECKS {
                    self.hard_degraded.store(false, Ordering::Relaxed);
                    self.hard_recovery_streak.store(0, Ordering::Relaxed);
                    tracing::info!(
                        lag_p99_ms,
                        hard_recover_ms,
                        streak,
                        "waterline alarm: hard degradation recovered — \
                         L3 scan interval restored"
                    );
                }
            } else {
                self.hard_recovery_streak.store(0, Ordering::Relaxed);
            }
        }

        lag_p99_ms
    }
}

impl TieredWatchRuntime {
    pub fn apply_rotating_cold_window_config(&self, config: &TieredWatchConfig) {
        self.rotating_cold_window_enabled
            .store(config.rotating_cold_window_enabled, Ordering::Relaxed);
        self.rotating_cold_window_budget
            .store(config.rotating_cold_window_budget.max(1), Ordering::Relaxed);
        self.rotating_cold_window_ttl_secs.store(
            config.rotating_cold_window_ttl_secs.max(1),
            Ordering::Relaxed,
        );
        self.rotating_cold_window_max_cost_per_root.store(
            config.rotating_cold_window_max_cost_per_root.max(1),
            Ordering::Relaxed,
        );
        self.rotating_cold_window_max_dirs_per_tick.store(
            config.rotating_cold_window_max_dirs_per_tick.max(1),
            Ordering::Relaxed,
        );
    }

    pub fn rotating_cold_window_tick_config(&self) -> RotatingColdWindowConfig {
        let effective_budget = self.waterline_alarm.effective_rotating_budget();
        let configured_budget = self.rotating_cold_window_budget.load(Ordering::Relaxed);
        let budget = if self.waterline_alarm.is_soft_degraded() {
            effective_budget.min(configured_budget).max(1)
        } else {
            configured_budget
        };
        let configured_max_dirs = self
            .rotating_cold_window_max_dirs_per_tick
            .load(Ordering::Relaxed);
        let max_dirs_per_tick = if self.waterline_alarm.is_soft_degraded() && configured_budget > 0
        {
            let ratio = budget as f64 / configured_budget as f64;
            (((configured_max_dirs as f64) * ratio).round() as usize)
                .max(1)
                .min(configured_max_dirs)
        } else {
            configured_max_dirs
        };
        RotatingColdWindowConfig {
            enabled: self.rotating_cold_window_enabled.load(Ordering::Relaxed),
            budget,
            ttl_secs: self.rotating_cold_window_ttl_secs.load(Ordering::Relaxed),
            max_cost_per_root: self
                .rotating_cold_window_max_cost_per_root
                .load(Ordering::Relaxed),
            max_dirs_per_tick,
        }
    }

    pub fn apply_waterline_config(&self, config: &TieredWatchConfig) {
        self.waterline_alarm.apply_config(config);
    }

    pub fn effective_l3_scan_interval_secs(&self) -> u64 {
        self.waterline_alarm.effective_l3_scan_interval_secs()
    }

    pub fn waterline_soft_degraded(&self) -> bool {
        self.waterline_alarm.is_soft_degraded()
    }

    pub fn waterline_hard_degraded(&self) -> bool {
        self.waterline_alarm.is_hard_degraded()
    }

    pub fn waterline_effective_rotating_budget(&self) -> usize {
        self.waterline_alarm.effective_rotating_budget()
    }

    pub fn record_rotating_cold_window_scan_completion(&self, path: &Path, cycle_id: u64) -> bool {
        let Some(state) = self.state(path) else {
            return false;
        };
        let now_unix_secs = unix_secs();
        let now = Instant::now();
        let mut progress = state.rotating_cold_window_progress.write();
        {
            let leases = self.rotating_cold_window_leases.read();
            let Some(lease) = leases.get(path) else {
                return false;
            };
            if lease.cycle_id != cycle_id || !lease.is_active(now_unix_secs, now) {
                return false;
            }
        }
        let sequence = self
            .rotating_cold_window_causal_seq
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1);
        progress.last_scan_seq = sequence;
        progress.last_scan_cycle_id = cycle_id;
        true
    }

    pub(super) fn record_rotating_cold_window_event_progress(
        &self,
        paths: &[&PathBuf],
        now_unix_secs: u64,
        now: Instant,
    ) {
        let roots = {
            let leases = self.rotating_cold_window_leases.read();
            let mut roots = HashSet::new();
            for path in paths {
                let nearest = leases
                    .iter()
                    .filter(|(root, lease)| {
                        lease.action == RotatingColdWindowActionKind::EphemeralWatch
                            && lease.is_active(now_unix_secs, now)
                            && path_is_under_or_equal(path.as_path(), root.as_path())
                    })
                    .max_by_key(|(root, _)| root.as_os_str().as_encoded_bytes().len());
                if let Some((root, lease)) = nearest {
                    roots.insert((root.clone(), lease.cycle_id));
                }
            }
            roots
        };
        if roots.is_empty() {
            return;
        }
        let dirs = self.dirs.read();
        for (root, cycle_id) in roots {
            let Some(state) = dirs.get(root.as_path()) else {
                continue;
            };
            let mut progress = state.rotating_cold_window_progress.write();
            {
                let leases = self.rotating_cold_window_leases.read();
                let Some(lease) = leases.get(root.as_path()) else {
                    continue;
                };
                if lease.cycle_id != cycle_id
                    || lease.action != RotatingColdWindowActionKind::EphemeralWatch
                    || !lease.is_active(now_unix_secs, now)
                {
                    continue;
                }
            }
            let sequence = self
                .rotating_cold_window_causal_seq
                .fetch_add(1, Ordering::AcqRel)
                .saturating_add(1);
            progress.last_event_seq = sequence;
            progress.last_event_cycle_id = cycle_id;
        }
    }

    /// Evaluate the waterline alarm against the latest p99 coverage lag.
    /// Called from `report()` at each metrics sample. Returns the lag value.
    pub(super) fn check_waterline_alarm(&self, lag_p99_ms: u64) -> u64 {
        self.waterline_alarm.check(lag_p99_ms)
    }

    pub fn rotating_cold_window_tick(
        &self,
        config: RotatingColdWindowConfig,
    ) -> RotatingColdWindowTick {
        if !config.enabled {
            return RotatingColdWindowTick::default();
        }

        let now = unix_secs();
        let now_instant = Instant::now();
        let budget = config.budget.max(1);
        let ttl_secs = config.ttl_secs.max(1);
        let max_dirs_per_tick = config.max_dirs_per_tick.max(1);
        let max_ephemeral_cost = config.max_cost_per_root.max(1) as u64;
        let max_fast_scan_cost = max_ephemeral_cost.saturating_mul(8).max(max_ephemeral_cost);

        {
            let mut leases = self.rotating_cold_window_leases.write();
            leases.retain(|_, lease| lease.is_active(now, now_instant));
        }

        let active_count = self.rotating_cold_window_leases.read().len();
        if active_count >= budget {
            self.rotating_cold_window_budget_blocked
                .fetch_add(1, Ordering::Relaxed);
            return RotatingColdWindowTick {
                cycle_id: self.rotating_cold_window_cycle_id.load(Ordering::Relaxed),
                budget_blocked: true,
                ..RotatingColdWindowTick::default()
            };
        }

        let capacity = budget.saturating_sub(active_count).min(max_dirs_per_tick);
        let (cold_paths, mut candidates) = {
            let dirs = self.dirs.read();
            let active = self.rotating_cold_window_leases.read();
            let mut cold_paths = HashSet::new();
            let mut candidates = Vec::new();
            for (path, state) in dirs.iter() {
                let tier = state.tier();
                if !matches!(tier, WatchTier::L2 | WatchTier::L3) {
                    continue;
                }
                cold_paths.insert(path.clone());
                if active.contains_key(path.as_path())
                    || state.promotion_pending.load(Ordering::Relaxed)
                    || state.demotion_pending.load(Ordering::Relaxed)
                {
                    continue;
                }
                let last_scan = state.last_scan_unix_secs.load(Ordering::Relaxed);
                let last_event = state.last_event_unix_secs.load(Ordering::Relaxed);
                let scan_age = if last_scan > 0 {
                    now.saturating_sub(last_scan)
                } else {
                    now.saturating_sub(last_event)
                };
                let event_score = state.event_score.load(Ordering::Relaxed);
                let budget_blocked = u64::from(state.budget_blocked_count.load(Ordering::Relaxed));
                let dirty_bonus = if state.dirty.load(Ordering::Relaxed) {
                    128
                } else {
                    0
                };
                let high_priority_bonus = if state.high_priority_scan.load(Ordering::Relaxed) {
                    256
                } else {
                    0
                };
                let freshness_bonus = match state.freshness() {
                    Freshness::Dirty => 192,
                    Freshness::Stale | Freshness::Unknown => 96,
                    Freshness::Fresh => 0,
                };
                let tier_bonus = if tier == WatchTier::L3 { 64 } else { 32 };
                let score = scan_age
                    .saturating_div(60)
                    .saturating_add(event_score.saturating_mul(4))
                    .saturating_add(budget_blocked.saturating_mul(32))
                    .saturating_add(dirty_bonus)
                    .saturating_add(high_priority_bonus)
                    .saturating_add(freshness_bonus)
                    .saturating_add(tier_bonus);
                let watch_cost = state.watch_cost.load(Ordering::Relaxed);
                candidates.push((path.clone(), watch_cost, score, scan_age));
            }
            (cold_paths, candidates)
        };

        if cold_paths.is_empty() || candidates.is_empty() {
            return RotatingColdWindowTick {
                cycle_id: self.rotating_cold_window_cycle_id.load(Ordering::Relaxed),
                ..RotatingColdWindowTick::default()
            };
        }

        let mut seen = self.rotating_cold_window_seen.write();
        seen.retain(|path| cold_paths.contains(path));
        if seen.len() >= cold_paths.len() {
            seen.clear();
            self.rotating_cold_window_cycle_id
                .fetch_add(1, Ordering::Relaxed);
        }

        let has_unseen_candidate = candidates
            .iter()
            .any(|(path, _, _, _)| !seen.contains(path));
        if has_unseen_candidate {
            candidates.retain(|(path, _, _, _)| !seen.contains(path));
        } else {
            seen.clear();
            self.rotating_cold_window_cycle_id
                .fetch_add(1, Ordering::Relaxed);
        }

        candidates.sort_by(|a, b| {
            b.2.cmp(&a.2)
                .then_with(|| b.3.cmp(&a.3))
                .then_with(|| a.0.cmp(&b.0))
        });

        let cycle_id = self.rotating_cold_window_cycle_id.load(Ordering::Relaxed);
        let expires_unix_secs = now.saturating_add(ttl_secs);
        let expires_at = now_instant
            .checked_add(Duration::from_secs(ttl_secs))
            .unwrap_or(now_instant);
        let follow_up_interval_secs = rotating_scan_follow_up_interval_secs(ttl_secs);
        let next_follow_up_at = now_instant
            .checked_add(Duration::from_secs(follow_up_interval_secs))
            .unwrap_or(now_instant);
        let mut actions = Vec::new();
        let mut leases = self.rotating_cold_window_leases.write();
        for (path, watch_cost, score, _) in candidates.into_iter().take(capacity) {
            let action = rotating_cold_window_action_for_cost(
                watch_cost,
                max_ephemeral_cost,
                max_fast_scan_cost,
            );
            leases.insert(
                path.clone(),
                RotatingColdWindowLease {
                    action,
                    expires_unix_secs,
                    expires_at,
                    follow_up_interval_secs,
                    next_follow_up_at,
                    follow_up_scans_remaining: 2,
                    cycle_id,
                    score,
                    watch_cost,
                },
            );
            seen.insert(path.clone());
            match action {
                RotatingColdWindowActionKind::EphemeralWatch => {
                    self.rotating_cold_window_promoted_to_ephemeral
                        .fetch_add(1, Ordering::Relaxed);
                }
                RotatingColdWindowActionKind::FastScanLease => {
                    self.rotating_cold_window_fast_scan_lease_dirs
                        .fetch_add(1, Ordering::Relaxed);
                }
                RotatingColdWindowActionKind::ScanOnly => {
                    self.rotating_cold_window_scan_only_dirs
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
            actions.push(RotatingColdWindowAction {
                path,
                action,
                watch_cost,
                score,
                expires_unix_secs,
            });
        }
        drop(leases);
        drop(seen);

        if actions.is_empty() {
            self.rotating_cold_window_budget_blocked
                .fetch_add(1, Ordering::Relaxed);
            return RotatingColdWindowTick {
                cycle_id,
                budget_blocked: true,
                actions,
            };
        }

        self.rotating_cold_window_last_tick_unix_secs
            .store(now, Ordering::Relaxed);
        RotatingColdWindowTick {
            cycle_id,
            actions,
            budget_blocked: false,
        }
    }

    pub fn take_due_rotating_cold_window_scans(&self) -> Vec<(PathBuf, u64)> {
        let now_unix_secs = unix_secs();
        let now = Instant::now();
        let mut leases = self.rotating_cold_window_leases.write();
        let mut due = Vec::new();
        for (path, lease) in leases.iter_mut() {
            if lease.action != RotatingColdWindowActionKind::ScanOnly
                || lease.follow_up_scans_remaining == 0
                || !lease.is_active(now_unix_secs, now)
                || lease.next_follow_up_at > now
            {
                continue;
            }
            due.push((path.clone(), lease.cycle_id));
            lease.follow_up_scans_remaining = lease.follow_up_scans_remaining.saturating_sub(1);
            if lease.follow_up_scans_remaining > 0 {
                let delay_multiplier = if lease.follow_up_scans_remaining == 1 {
                    2
                } else {
                    1
                };
                lease.next_follow_up_at = now
                    .checked_add(Duration::from_secs(
                        lease
                            .follow_up_interval_secs
                            .saturating_mul(delay_multiplier),
                    ))
                    .unwrap_or(now);
            }
        }
        due
    }

    pub(super) fn rotating_cold_window_ephemeral_ttl_cap_secs(
        &self,
        path: &Path,
        now_unix_secs: u64,
    ) -> Option<u64> {
        let now = Instant::now();
        self.rotating_cold_window_leases
            .read()
            .iter()
            .filter_map(|(root, lease)| {
                if !rotating_cold_window_controls_ephemeral_watch(lease.action)
                    || !lease.is_active(now_unix_secs, now)
                    || !path_is_under_or_equal(path, root.as_path())
                {
                    return None;
                }
                let wall_remaining = lease.expires_unix_secs.saturating_sub(now_unix_secs);
                let monotonic_remaining = lease.expires_at.saturating_duration_since(now);
                let monotonic_remaining_secs = monotonic_remaining
                    .as_secs()
                    .saturating_add(u64::from(monotonic_remaining.subsec_nanos() > 0));
                Some(wall_remaining.min(monotonic_remaining_secs).max(1))
            })
            .min()
    }

    pub fn rotating_cold_window_expired_ephemeral_covers(&self, path: &Path) -> bool {
        let now_unix_secs = unix_secs();
        let now = Instant::now();
        self.rotating_cold_window_leases
            .read()
            .iter()
            .any(|(root, lease)| {
                rotating_cold_window_controls_ephemeral_watch(lease.action)
                    && !lease.is_active(now_unix_secs, now)
                    && path_is_under_or_equal(path, root.as_path())
            })
    }

    pub fn cancel_rotating_cold_window_lease(&self, path: &Path) {
        self.rotating_cold_window_leases.write().remove(path);
        self.rotating_cold_window_seen.write().remove(path);
    }

    pub fn downgrade_rotating_cold_window_lease_to_scan_only(&self, path: &Path) -> bool {
        let mut leases = self.rotating_cold_window_leases.write();
        let Some(lease) = leases.get_mut(path) else {
            return false;
        };
        if lease.action != RotatingColdWindowActionKind::ScanOnly {
            lease.action = RotatingColdWindowActionKind::ScanOnly;
            self.rotating_cold_window_scan_only_dirs
                .fetch_add(1, Ordering::Relaxed);
        }
        true
    }
}

fn rotating_cold_window_action_for_cost(
    watch_cost: u64,
    max_ephemeral_cost: u64,
    max_fast_scan_cost: u64,
) -> RotatingColdWindowActionKind {
    if watch_cost <= max_ephemeral_cost {
        RotatingColdWindowActionKind::EphemeralWatch
    } else if watch_cost <= max_fast_scan_cost {
        RotatingColdWindowActionKind::FastScanLease
    } else {
        RotatingColdWindowActionKind::ScanOnly
    }
}
