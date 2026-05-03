use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::index::tiered::ScanOutcome;
use crate::stats::WatchStateReport;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WatchTier {
    L0,
    L1,
    L2,
    L3,
}

impl WatchTier {
    fn as_u8(self) -> u8 {
        match self {
            Self::L0 => 0,
            Self::L1 => 1,
            Self::L2 => 2,
            Self::L3 => 3,
        }
    }

    fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::L0,
            1 => Self::L1,
            2 => Self::L2,
            _ => Self::L3,
        }
    }
}

#[derive(Debug)]
struct DirState {
    tier: AtomicU8,
    watch_cost: AtomicU64,
    last_event_unix_secs: AtomicU64,
    last_scan_unix_secs: AtomicU64,
    empty_scan_count: AtomicU32,
    last_changed_count: AtomicU64,
    promotion_pending: AtomicBool,
    demotion_pending: AtomicBool,
}

impl DirState {
    fn new(tier: WatchTier, watch_cost: usize, now: u64) -> Self {
        Self {
            tier: AtomicU8::new(tier.as_u8()),
            watch_cost: AtomicU64::new(watch_cost as u64),
            last_event_unix_secs: AtomicU64::new(now),
            last_scan_unix_secs: AtomicU64::new(0),
            empty_scan_count: AtomicU32::new(0),
            last_changed_count: AtomicU64::new(0),
            promotion_pending: AtomicBool::new(false),
            demotion_pending: AtomicBool::new(false),
        }
    }

    fn tier(&self) -> WatchTier {
        WatchTier::from_u8(self.tier.load(Ordering::Relaxed))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PromotionDecision {
    SendAdd,
    BudgetBlocked,
    NotEligible,
}

#[derive(Debug)]
pub struct TieredWatchRuntime {
    dirs: RwLock<HashMap<PathBuf, Arc<DirState>>>,
    max_watch_dirs: u64,
    current_watch_cost: AtomicU64,
    scan_items_per_sec: usize,
    scan_ms_per_tick: u64,
    promotions: AtomicU64,
    demotions: AtomicU64,
    promotion_budget_blocked: AtomicU64,
    last_adjustment_unix_secs: AtomicU64,
}

impl TieredWatchRuntime {
    pub fn new(
        l0_roots: Vec<(PathBuf, usize)>,
        l1_roots: Vec<(PathBuf, usize)>,
        max_watch_dirs: usize,
        scan_items_per_sec: usize,
        scan_ms_per_tick: u64,
    ) -> Self {
        let now = unix_secs();
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
            max_watch_dirs: max_watch_dirs as u64,
            current_watch_cost: AtomicU64::new(current_watch_cost),
            scan_items_per_sec,
            scan_ms_per_tick,
            promotions: AtomicU64::new(0),
            demotions: AtomicU64::new(0),
            promotion_budget_blocked: AtomicU64::new(0),
            last_adjustment_unix_secs: AtomicU64::new(now),
        }
    }

    pub fn record_event_paths<'a>(&self, paths: impl IntoIterator<Item = &'a PathBuf>) {
        let now = unix_secs();
        let dirs = self.dirs.read();
        for path in paths {
            for (root, state) in dirs.iter() {
                if state.tier() == WatchTier::L0 && path_is_under_or_equal(path, root) {
                    state.last_event_unix_secs.store(now, Ordering::Relaxed);
                    state.empty_scan_count.store(0, Ordering::Relaxed);
                }
            }
        }
    }

    pub fn max_watch_dirs(&self) -> usize {
        self.max_watch_dirs as usize
    }

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

    pub fn l1_batch(&self, limit: usize) -> Vec<PathBuf> {
        let dirs = self.dirs.read();
        let mut candidates = dirs
            .iter()
            .filter_map(|(path, state)| {
                if state.tier() != WatchTier::L1 {
                    return None;
                }
                if state.promotion_pending.load(Ordering::Relaxed)
                    || state.demotion_pending.load(Ordering::Relaxed)
                {
                    return None;
                }
                Some((
                    state.last_scan_unix_secs.load(Ordering::Relaxed),
                    path.clone(),
                ))
            })
            .collect::<Vec<_>>();
        candidates.sort_by_key(|(last_scan, path)| (*last_scan, path.clone()));
        candidates
            .into_iter()
            .take(limit)
            .map(|(_, path)| path)
            .collect()
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
            state.empty_scan_count.store(0, Ordering::Relaxed);
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

        if state.tier() != WatchTier::L1 {
            return PromotionDecision::NotEligible;
        }
        if state.promotion_pending.load(Ordering::Relaxed) {
            return PromotionDecision::NotEligible;
        }

        state.watch_cost.store(watch_cost as u64, Ordering::Relaxed);

        self.try_reserve_promotion(path.as_path())
    }

    pub fn record_scan(&self, path: &Path, outcome: ScanOutcome) {
        if let Some(state) = self.state(path) {
            state
                .last_scan_unix_secs
                .store(unix_secs(), Ordering::Relaxed);
            state
                .last_changed_count
                .store(outcome.changed as u64, Ordering::Relaxed);
            if outcome.changed == 0 {
                state.empty_scan_count.fetch_add(1, Ordering::Relaxed);
            } else {
                state.empty_scan_count.store(0, Ordering::Relaxed);
            }
        }
    }

    pub fn try_reserve_promotion(&self, path: &Path) -> PromotionDecision {
        let Some(state) = self.state(path) else {
            return PromotionDecision::NotEligible;
        };
        if state.tier() != WatchTier::L1 {
            return PromotionDecision::NotEligible;
        }
        if state.promotion_pending.swap(true, Ordering::AcqRel) {
            return PromotionDecision::NotEligible;
        }

        let cost = state.watch_cost.load(Ordering::Relaxed);
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
        } else {
            state.promotion_pending.store(false, Ordering::Release);
            self.promotion_budget_blocked
                .fetch_add(1, Ordering::Relaxed);
            PromotionDecision::BudgetBlocked
        }
    }

    pub fn confirm_promoted(&self, path: &Path) {
        if let Some(state) = self.state(path) {
            state.tier.store(WatchTier::L0.as_u8(), Ordering::Release);
            state.promotion_pending.store(false, Ordering::Release);
            state.empty_scan_count.store(0, Ordering::Relaxed);
            state
                .last_event_unix_secs
                .store(unix_secs(), Ordering::Relaxed);
            self.promotions.fetch_add(1, Ordering::Relaxed);
            self.last_adjustment_unix_secs
                .store(unix_secs(), Ordering::Relaxed);
        }
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

    pub fn report(&self) -> WatchStateReport {
        let dirs = self.dirs.read();
        let mut l0_dirs = 0usize;
        let mut l1_dirs = 0usize;
        let mut l2_dirs = 0usize;
        let mut l3_dirs = 0usize;
        let mut pending_promotions = 0usize;

        for state in dirs.values() {
            match state.tier() {
                WatchTier::L0 => l0_dirs += 1,
                WatchTier::L1 => l1_dirs += 1,
                WatchTier::L2 => l2_dirs += 1,
                WatchTier::L3 => l3_dirs += 1,
            }
            if state.promotion_pending.load(Ordering::Relaxed) {
                pending_promotions += 1;
            }
        }

        let mut notes = vec![
            "tiered runtime controls L0/L1 migration".to_string(),
            "L2/L3 are reserved for the next phase".to_string(),
        ];
        if pending_promotions > 0 {
            notes.push(format!(
                "{} promotion(s) are waiting for watcher command completion",
                pending_promotions
            ));
        }
        let blocked = self.promotion_budget_blocked.load(Ordering::Relaxed);
        if blocked > 0 {
            notes.push(format!(
                "{} promotion attempt(s) were blocked by watch budget",
                blocked
            ));
        }
        let watched_dirs_estimated = self.current_watch_cost.load(Ordering::Relaxed) as usize;
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
            l0_dirs,
            l1_dirs,
            l2_dirs,
            l3_dirs,
            watched_dirs_estimated,
            max_watch_dirs: self.max_watch_dirs as usize,
            l0_candidates: dirs.len(),
            l0_admitted: l0_dirs,
            l0_rejected: l1_dirs + l2_dirs + l3_dirs,
            scan_backlog: l1_dirs + l2_dirs,
            scan_items_per_sec: self.scan_items_per_sec,
            scan_ms_per_tick: self.scan_ms_per_tick,
            promotions: self.promotions.load(Ordering::Relaxed),
            demotions: self.demotions.load(Ordering::Relaxed),
            promotion_budget_blocked: blocked,
            watch_budget_utilization_pct,
            last_adjustment_unix_secs: self.last_adjustment_unix_secs.load(Ordering::Relaxed),
            notes,
        }
    }

    fn state(&self, path: &Path) -> Option<Arc<DirState>> {
        self.dirs.read().get(path).cloned()
    }
}

fn path_is_under_or_equal(path: &Path, root: &Path) -> bool {
    path == root || path.starts_with(root)
}

fn unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn runtime() -> TieredWatchRuntime {
        TieredWatchRuntime::new(
            vec![(PathBuf::from("/tmp/hot"), 2)],
            vec![(PathBuf::from("/tmp/warm"), 3)],
            5,
            5_000,
            20,
        )
    }

    #[test]
    fn l0_event_refresh_blocks_idle_expiry() {
        let rt = runtime();
        rt.record_event_paths([&PathBuf::from("/tmp/hot/file.txt")]);
        assert!(rt.expired_l0(60).is_empty());
    }

    #[test]
    fn l1_scan_changed_resets_empty_count() {
        let rt = runtime();
        let warm = PathBuf::from("/tmp/warm");

        rt.record_scan(
            warm.as_path(),
            ScanOutcome {
                scanned: 1,
                changed: 0,
                elapsed_ms: 1,
            },
        );
        rt.record_scan(
            warm.as_path(),
            ScanOutcome {
                scanned: 1,
                changed: 1,
                elapsed_ms: 1,
            },
        );

        let report = rt.report();
        assert_eq!(report.l1_dirs, 1);
        assert_eq!(report.scan_backlog, 1);
    }

    #[test]
    fn promotion_reserves_budget_and_rollback_releases_it() {
        let rt = runtime();
        let warm = PathBuf::from("/tmp/warm");

        assert_eq!(
            rt.try_reserve_promotion(warm.as_path()),
            PromotionDecision::SendAdd
        );
        assert_eq!(rt.report().watched_dirs_estimated, 5);

        rt.rollback_promote(warm.as_path());
        assert_eq!(rt.report().watched_dirs_estimated, 2);
        assert_eq!(rt.report().l1_dirs, 1);
    }

    #[test]
    fn confirm_demoted_releases_watch_budget() {
        let rt = runtime();
        let hot = PathBuf::from("/tmp/hot");

        assert!(rt.mark_demotion_pending(hot.as_path()));
        rt.confirm_demoted(hot.as_path());

        let report = rt.report();
        assert_eq!(report.l0_dirs, 0);
        assert_eq!(report.l1_dirs, 2);
        assert_eq!(report.watched_dirs_estimated, 0);
        assert_eq!(report.demotions, 1);
    }

    #[test]
    fn dynamic_candidate_reserves_budget_and_promotes() {
        let rt = runtime();
        let dynamic = PathBuf::from("/tmp/hot/new-child");

        assert_eq!(
            rt.register_dynamic_candidate(dynamic.clone(), 1),
            PromotionDecision::SendAdd
        );
        let reserved = rt.report();
        assert_eq!(reserved.watched_dirs_estimated, 3);
        assert_eq!(reserved.l1_dirs, 2);

        rt.confirm_promoted(dynamic.as_path());
        let promoted = rt.report();
        assert_eq!(promoted.l0_dirs, 2);
        assert_eq!(promoted.l1_dirs, 1);
        assert_eq!(promoted.promotions, 1);
    }

    #[test]
    fn dynamic_candidate_stays_l1_when_budget_blocked() {
        let rt = runtime();
        let dynamic = PathBuf::from("/tmp/hot/too-large-child");

        assert_eq!(
            rt.register_dynamic_candidate(dynamic, 4),
            PromotionDecision::BudgetBlocked
        );
        let report = rt.report();
        assert_eq!(report.watched_dirs_estimated, 2);
        assert_eq!(report.l0_dirs, 1);
        assert_eq!(report.l1_dirs, 2);
        assert!(report
            .notes
            .iter()
            .any(|note| note.contains("blocked by watch budget")));
    }
}
