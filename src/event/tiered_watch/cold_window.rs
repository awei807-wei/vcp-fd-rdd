//! Rotating cold window logic: tier rotation, scan batching, promotion/demotion.

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::config::L3ScanPolicy;
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
