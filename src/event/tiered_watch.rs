use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::config::L3ScanPolicy;
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Freshness {
    Fresh,
    Stale,
    Dirty,
    Unknown,
}

impl Freshness {
    fn as_u8(self) -> u8 {
        match self {
            Self::Fresh => 0,
            Self::Stale => 1,
            Self::Dirty => 2,
            Self::Unknown => 3,
        }
    }
    fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Fresh,
            1 => Self::Stale,
            2 => Self::Dirty,
            _ => Self::Unknown,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexResidency {
    HotMemory,
    WarmMemory,
    ColdMmap,
    FrozenManifestOnly,
}

impl IndexResidency {
    fn as_u8(self) -> u8 {
        match self {
            Self::HotMemory => 0,
            Self::WarmMemory => 1,
            Self::ColdMmap => 2,
            Self::FrozenManifestOnly => 3,
        }
    }
    fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::HotMemory,
            1 => Self::WarmMemory,
            2 => Self::ColdMmap,
            _ => Self::FrozenManifestOnly,
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
}

impl DirState {
    fn new(tier: WatchTier, watch_cost: usize, now: u64) -> Self {
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
        }
    }

    fn tier(&self) -> WatchTier {
        WatchTier::from_u8(self.tier.load(Ordering::Relaxed))
    }

    fn freshness(&self) -> Freshness {
        Freshness::from_u8(self.freshness.load(Ordering::Relaxed))
    }

    fn set_freshness(&self, f: Freshness) {
        self.freshness.store(f.as_u8(), Ordering::Release);
    }

    fn index_residency(&self) -> IndexResidency {
        IndexResidency::from_u8(self.index_residency.load(Ordering::Relaxed))
    }

    fn set_index_residency(&self, r: IndexResidency) {
        self.index_residency.store(r.as_u8(), Ordering::Release);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PromotionDecision {
    SendAdd,
    Replace { demote: PathBuf, promote: PathBuf },
    BudgetBlocked,
    NotEligible,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EphemeralWatchDecision {
    Add(PathBuf),
    Replace { remove: PathBuf, add: PathBuf },
    NotEligible,
    BudgetBlocked,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EphemeralWatchExpiry {
    Idle,
    Ttl,
    NoChange,
    CoveredByL0,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EphemeralWatchRemoval {
    pub path: PathBuf,
    pub reason: EphemeralWatchExpiry,
}

#[derive(Clone, Debug)]
pub struct EphemeralWatchConfig {
    pub budget: usize,
    pub ttl_secs: u64,
    pub idle_secs: u64,
    pub max_cost_per_root: usize,
    pub repeat_window_secs: u64,
    pub repeat_threshold: u32,
    pub no_change_limit: u32,
}

impl Default for EphemeralWatchConfig {
    fn default() -> Self {
        Self {
            budget: 0,
            ttl_secs: 600,
            idle_secs: 120,
            max_cost_per_root: 64,
            repeat_window_secs: 60,
            repeat_threshold: 2,
            no_change_limit: 3,
        }
    }
}

#[derive(Debug)]
struct EphemeralWatchLease {
    path: PathBuf,
    watch_cost: u64,
    created_unix_secs: u64,
    last_event_unix_secs: AtomicU64,
    last_dirty_unix_secs: u64,
    dirty_hits: u32,
    no_change_scans: u32,
    value_score: u64,
    pending_add: bool,
    pending_remove: bool,
}

impl EphemeralWatchLease {
    fn pending(path: PathBuf, watch_cost: usize, now: u64) -> Self {
        Self {
            path,
            watch_cost: watch_cost as u64,
            created_unix_secs: now,
            last_event_unix_secs: AtomicU64::new(now),
            last_dirty_unix_secs: now,
            dirty_hits: 1,
            no_change_scans: 0,
            value_score: 1,
            pending_add: true,
            pending_remove: false,
        }
    }
}

#[derive(Debug)]
struct DirtyScopeObservation {
    last_unix_secs: u64,
    hits: u32,
    score: u64,
}

#[derive(Debug)]
pub struct TieredWatchRuntime {
    dirs: RwLock<HashMap<PathBuf, Arc<DirState>>>,
    ephemeral: RwLock<HashMap<PathBuf, EphemeralWatchLease>>,
    dirty_observations: RwLock<HashMap<PathBuf, DirtyScopeObservation>>,
    max_watch_dirs: u64,
    current_watch_cost: AtomicU64,
    ephemeral_watch_budget: u64,
    current_ephemeral_watch_cost: AtomicU64,
    ephemeral_watch_created: AtomicU64,
    ephemeral_watch_expired: AtomicU64,
    ephemeral_watch_evicted: AtomicU64,
    ephemeral_watch_budget_blocked: AtomicU64,
    scan_items_per_sec: usize,
    scan_ms_per_tick: u64,
    promotions: AtomicU64,
    demotions: AtomicU64,
    replacements: AtomicU64,
    promotion_budget_blocked: AtomicU64,
    cold_validate_count: AtomicU64,
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
        Self::new_with_ephemeral(
            l0_roots,
            l1_roots,
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
            ephemeral: RwLock::new(HashMap::new()),
            dirty_observations: RwLock::new(HashMap::new()),
            max_watch_dirs: max_watch_dirs as u64,
            current_watch_cost: AtomicU64::new(current_watch_cost),
            ephemeral_watch_budget: ephemeral_watch_budget as u64,
            current_ephemeral_watch_cost: AtomicU64::new(0),
            ephemeral_watch_created: AtomicU64::new(0),
            ephemeral_watch_expired: AtomicU64::new(0),
            ephemeral_watch_evicted: AtomicU64::new(0),
            ephemeral_watch_budget_blocked: AtomicU64::new(0),
            scan_items_per_sec,
            scan_ms_per_tick,
            promotions: AtomicU64::new(0),
            demotions: AtomicU64::new(0),
            replacements: AtomicU64::new(0),
            promotion_budget_blocked: AtomicU64::new(0),
            cold_validate_count: AtomicU64::new(0),
            last_adjustment_unix_secs: AtomicU64::new(now),
        }
    }

    pub fn record_event_paths<'a>(
        &self,
        paths: impl IntoIterator<Item = &'a PathBuf>,
    ) -> Vec<PathBuf> {
        let now = unix_secs();
        let paths = paths.into_iter().collect::<Vec<_>>();
        self.record_ephemeral_events(&paths, now);
        let dirs = self.dirs.read();
        let mut dirty_dirs = Vec::new();
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
        dirty_dirs.sort();
        dirty_dirs.dedup();
        dirty_dirs
    }

    fn record_ephemeral_events(&self, paths: &[&PathBuf], now: u64) {
        if paths.is_empty() {
            return;
        }
        let ephemeral = self.ephemeral.read();
        for event_path in paths {
            let mut best: Option<(&PathBuf, &EphemeralWatchLease)> = None;
            for (root, lease) in ephemeral.iter() {
                if lease.pending_add || lease.pending_remove {
                    continue;
                }
                if !path_is_under_or_equal(event_path, root) {
                    continue;
                }
                let should_replace = best
                    .as_ref()
                    .map(|(best_root, _)| {
                        root.as_os_str().as_encoded_bytes().len()
                            > best_root.as_os_str().as_encoded_bytes().len()
                    })
                    .unwrap_or(true);
                if should_replace {
                    best = Some((root, lease));
                }
            }
            if let Some((_, lease)) = best {
                lease.last_event_unix_secs.store(now, Ordering::Relaxed);
            }
        }
    }

    pub fn note_dirty_scope(
        &self,
        path: PathBuf,
        watch_cost: usize,
        exclude_dirs: &[String],
        config: &EphemeralWatchConfig,
    ) -> EphemeralWatchDecision {
        let now = unix_secs();
        self.note_dirty_scope_at(path, watch_cost, exclude_dirs, config, now, 0)
    }

    pub fn note_dirty_scope_with_changed(
        &self,
        path: PathBuf,
        watch_cost: usize,
        exclude_dirs: &[String],
        config: &EphemeralWatchConfig,
        changed: usize,
    ) -> EphemeralWatchDecision {
        let now = unix_secs();
        self.note_dirty_scope_at(path, watch_cost, exclude_dirs, config, now, changed)
    }

    pub fn note_dirty_scope_at(
        &self,
        path: PathBuf,
        watch_cost: usize,
        exclude_dirs: &[String],
        config: &EphemeralWatchConfig,
        now: u64,
        changed: usize,
    ) -> EphemeralWatchDecision {
        if config.budget == 0 || watch_cost == 0 {
            return EphemeralWatchDecision::NotEligible;
        }
        if watch_cost > config.max_cost_per_root.max(1) {
            return EphemeralWatchDecision::NotEligible;
        }
        if exclude_dirs
            .iter()
            .any(|name| !name.is_empty() && path_has_component(path.as_path(), name))
        {
            return EphemeralWatchDecision::NotEligible;
        }
        {
            let dirs = self.dirs.read();
            if dirs.iter().any(|(root, state)| {
                state.tier() == WatchTier::L0 && path_is_under_or_equal(path.as_path(), root)
            }) {
                return EphemeralWatchDecision::NotEligible;
            }
        }

        let observed = self.observe_dirty_scope(path.as_path(), now, changed, config);
        let key = path.clone();
        {
            let mut ephemeral = self.ephemeral.write();
            if let Some(lease) = ephemeral.get_mut(&key) {
                lease.dirty_hits = observed;
                lease.last_dirty_unix_secs = now;
                lease.last_event_unix_secs.store(now, Ordering::Relaxed);
                if changed == 0 {
                    lease.no_change_scans = lease.no_change_scans.saturating_add(1);
                } else {
                    lease.no_change_scans = 0;
                    lease.value_score = lease
                        .value_score
                        .saturating_add((changed as u64).saturating_mul(4).saturating_add(8));
                }
                return EphemeralWatchDecision::NotEligible;
            }
        }
        if observed < config.repeat_threshold.max(1) {
            return EphemeralWatchDecision::NotEligible;
        }

        let observations = self.dirty_observations.read();
        let candidate_score = observations
            .get(&key)
            .map(|entry| entry.score)
            .unwrap_or(u64::from(observed));
        let mut ephemeral = self.ephemeral.write();
        let covered_by_ephemeral = ephemeral.iter().any(|(root, lease)| {
            !lease.pending_remove && path_is_under_or_equal(path.as_path(), root.as_path())
        });
        if covered_by_ephemeral {
            return EphemeralWatchDecision::NotEligible;
        }

        let mut probe = EphemeralWatchLease::pending(path.clone(), watch_cost, now);
        probe.dirty_hits = observed;
        probe.value_score = candidate_score;
        let cost = probe.watch_cost;
        let budget = config.budget as u64;

        if self
            .current_ephemeral_watch_cost
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |current| {
                if current.saturating_add(cost) <= budget {
                    Some(current.saturating_add(cost))
                } else {
                    None
                }
            })
            .is_ok()
        {
            ephemeral.insert(key, probe);
            return EphemeralWatchDecision::Add(path);
        }

        if let Some(victim) = choose_ephemeral_victim(&ephemeral, path.as_path(), cost, budget) {
            let victim_cost = ephemeral
                .get(&victim)
                .map(|lease| lease.watch_cost)
                .unwrap_or(0);
            if self
                .current_ephemeral_watch_cost
                .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |current| {
                    let next = current.saturating_sub(victim_cost).saturating_add(cost);
                    if next <= budget {
                        Some(next)
                    } else {
                        None
                    }
                })
                .is_ok()
            {
                if let Some(victim_lease) = ephemeral.get_mut(&victim) {
                    victim_lease.pending_remove = true;
                }
                ephemeral.insert(key, probe);
                self.ephemeral_watch_evicted.fetch_add(1, Ordering::Relaxed);
                return EphemeralWatchDecision::Replace {
                    remove: victim,
                    add: path,
                };
            }
        }

        self.ephemeral_watch_budget_blocked
            .fetch_add(1, Ordering::Relaxed);
        EphemeralWatchDecision::BudgetBlocked
    }

    pub fn record_dirty_scope_repeat(
        &self,
        path: &Path,
        changed: usize,
        config: &EphemeralWatchConfig,
    ) {
        let now = unix_secs();
        let hits = self.observe_dirty_scope(path, now, changed, config);
        let mut ephemeral = self.ephemeral.write();
        if let Some(lease) = ephemeral.get_mut(path) {
            lease.dirty_hits = hits;
            lease.last_dirty_unix_secs = now;
            if changed == 0 {
                lease.no_change_scans = lease.no_change_scans.saturating_add(1);
            } else {
                lease.no_change_scans = 0;
                lease.value_score = lease
                    .value_score
                    .saturating_add((changed as u64).saturating_mul(4).saturating_add(8));
            }
        }
    }

    fn observe_dirty_scope(
        &self,
        path: &Path,
        now: u64,
        changed: usize,
        config: &EphemeralWatchConfig,
    ) -> u32 {
        let key = path.to_path_buf();
        let mut observations = self.dirty_observations.write();
        let entry = observations
            .entry(key)
            .and_modify(|entry| {
                if now.saturating_sub(entry.last_unix_secs) <= config.repeat_window_secs.max(1) {
                    entry.hits = entry.hits.saturating_add(1);
                } else {
                    entry.hits = 1;
                    entry.score = 0;
                }
                entry.last_unix_secs = now;
                entry.score = entry
                    .score
                    .saturating_add((changed as u64).saturating_add(1));
            })
            .or_insert_with(|| DirtyScopeObservation {
                last_unix_secs: now,
                hits: 1,
                score: (changed as u64).saturating_add(1),
            });
        entry.hits
    }

    pub fn expire_ephemeral_watches(
        &self,
        idle_secs: u64,
        ttl_secs: u64,
        no_change_limit: u32,
    ) -> Vec<EphemeralWatchRemoval> {
        let now = unix_secs();
        self.expire_ephemeral_watches_at(now, idle_secs, ttl_secs, no_change_limit)
    }

    pub fn expire_ephemeral_watches_at(
        &self,
        now: u64,
        idle_secs: u64,
        ttl_secs: u64,
        no_change_limit: u32,
    ) -> Vec<EphemeralWatchRemoval> {
        let l0_roots = {
            let dirs = self.dirs.read();
            dirs.iter()
                .filter_map(|(path, state)| {
                    if state.tier() == WatchTier::L0 {
                        Some(path.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        };
        let mut removals = Vec::new();
        let mut ephemeral = self.ephemeral.write();
        for lease in ephemeral.values_mut() {
            if lease.pending_add || lease.pending_remove {
                continue;
            }
            let reason = if l0_roots
                .iter()
                .any(|root| path_is_under_or_equal(lease.path.as_path(), root.as_path()))
            {
                Some(EphemeralWatchExpiry::CoveredByL0)
            } else if ttl_secs > 0 && now.saturating_sub(lease.created_unix_secs) >= ttl_secs {
                Some(EphemeralWatchExpiry::Ttl)
            } else if idle_secs > 0
                && now.saturating_sub(lease.last_event_unix_secs.load(Ordering::Relaxed))
                    >= idle_secs
            {
                Some(EphemeralWatchExpiry::Idle)
            } else if no_change_limit > 0 && lease.no_change_scans >= no_change_limit {
                Some(EphemeralWatchExpiry::NoChange)
            } else {
                None
            };
            if let Some(reason) = reason {
                lease.pending_remove = true;
                removals.push(EphemeralWatchRemoval {
                    path: lease.path.clone(),
                    reason,
                });
            }
        }
        removals
    }

    pub fn confirm_ephemeral_added(&self, path: &Path) {
        if let Some(lease) = self.ephemeral.write().get_mut(path) {
            if lease.pending_add {
                lease.pending_add = false;
                self.ephemeral_watch_created.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn rollback_ephemeral_add(&self, path: &Path) {
        let removed = self.ephemeral.write().remove(path);
        if let Some(lease) = removed {
            self.release_ephemeral_cost(lease.watch_cost);
        }
    }

    pub fn confirm_ephemeral_removed(&self, path: &Path) {
        let removed = self.ephemeral.write().remove(path);
        if let Some(lease) = removed {
            self.release_ephemeral_cost(lease.watch_cost);
            self.ephemeral_watch_expired.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn confirm_ephemeral_evicted(&self, path: &Path) {
        let _ = self.ephemeral.write().remove(path);
    }

    pub fn rollback_ephemeral_remove(&self, path: &Path) {
        if let Some(lease) = self.ephemeral.write().get_mut(path) {
            lease.pending_remove = false;
        }
    }

    pub fn rollback_ephemeral_replace(&self, remove: &Path, add: &Path) {
        let mut leases = self.ephemeral.write();
        let add_cost = leases
            .remove(add)
            .map(|lease| lease.watch_cost)
            .unwrap_or(0);
        let mut victim_cost = 0;
        if let Some(victim) = leases.get_mut(remove) {
            victim.pending_remove = false;
            victim_cost = victim.watch_cost;
        }
        drop(leases);
        let _ = self.current_ephemeral_watch_cost.fetch_update(
            Ordering::AcqRel,
            Ordering::Relaxed,
            |current| Some(current.saturating_sub(add_cost).saturating_add(victim_cost)),
        );
    }

    fn release_ephemeral_cost(&self, cost: u64) {
        let _ = self.current_ephemeral_watch_cost.fetch_update(
            Ordering::AcqRel,
            Ordering::Relaxed,
            |current| Some(current.saturating_sub(cost)),
        );
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

    pub fn record_scan(&self, path: &Path, outcome: ScanOutcome) {
        if let Some(state) = self.state(path) {
            let now = unix_secs();
            if matches!(state.tier(), WatchTier::L2 | WatchTier::L3) {
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
        let target = {
            let dirs = self.dirs.read();
            dirs.iter()
                .filter(|(root, _)| path_is_under_or_equal(path, root))
                .max_by_key(|(root, _)| root.as_os_str().as_encoded_bytes().len())
                .map(|(root, _)| root.clone())
        }?;
        self.record_scan(target.as_path(), outcome);
        Some(target)
    }

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
            if let Some(victim) = self.reserve_by_replacing_cold_l0(path, cost) {
                PromotionDecision::Replace {
                    demote: victim,
                    promote: path.to_path_buf(),
                }
            } else {
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
                PromotionDecision::BudgetBlocked
            }
        }
    }

    fn reserve_by_replacing_cold_l0(&self, promote: &Path, promote_cost: u64) -> Option<PathBuf> {
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
        let now = unix_secs();
        let mut fresh_dirs = 0usize;
        let mut stale_dirs = 0usize;
        let mut dirty_dirs = 0usize;
        let mut unknown_dirs = 0usize;
        let mut hot_memory_dirs = 0usize;
        let mut warm_memory_dirs = 0usize;
        let mut cold_mmap_dirs = 0usize;
        let mut frozen_manifest_dirs = 0usize;

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
            match state.freshness() {
                Freshness::Fresh => fresh_dirs += 1,
                Freshness::Stale => stale_dirs += 1,
                Freshness::Dirty => dirty_dirs += 1,
                Freshness::Unknown => unknown_dirs += 1,
            }
            match state.index_residency() {
                IndexResidency::HotMemory => hot_memory_dirs += 1,
                IndexResidency::WarmMemory => warm_memory_dirs += 1,
                IndexResidency::ColdMmap => cold_mmap_dirs += 1,
                IndexResidency::FrozenManifestOnly => frozen_manifest_dirs += 1,
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
        drop(dirs);

        let ephemeral = self.ephemeral.read();
        let ephemeral_watch_dirs = ephemeral
            .values()
            .filter(|lease| !lease.pending_add && !lease.pending_remove)
            .count();
        let ephemeral_watch_cost = self.current_ephemeral_watch_cost.load(Ordering::Relaxed);
        drop(ephemeral);

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
            l0_candidates,
            l0_admitted: l0_dirs,
            l0_rejected: l1_dirs + l2_dirs + l3_dirs,
            scan_backlog: l1_dirs + l2_dirs + l3_dirs,
            scan_items_per_sec: self.scan_items_per_sec,
            scan_ms_per_tick: self.scan_ms_per_tick,
            promotions: self.promotions.load(Ordering::Relaxed),
            demotions: self.demotions.load(Ordering::Relaxed),
            l0_replacements: self.replacements.load(Ordering::Relaxed),
            promotion_budget_blocked: blocked,
            watch_budget_utilization_pct,
            last_adjustment_unix_secs: self.last_adjustment_unix_secs.load(Ordering::Relaxed),
            next_scan_unix_secs: if next_scan_unix_secs == u64::MAX {
                0
            } else {
                next_scan_unix_secs
            },
            event_score_total,
            fresh_dirs,
            stale_dirs,
            dirty_dirs,
            unknown_dirs,
            hot_memory_dirs,
            warm_memory_dirs,
            cold_mmap_dirs,
            frozen_manifest_dirs,
            notes,
            l0_watch_cost,
            l1_watch_cost,
            l2_watch_cost,
            l3_watch_cost,
            ephemeral_watch_cost,
            ephemeral_watch_budget: self.ephemeral_watch_budget as usize,
            ephemeral_watch_dirs,
            ephemeral_watch_created: self.ephemeral_watch_created.load(Ordering::Relaxed),
            ephemeral_watch_expired: self.ephemeral_watch_expired.load(Ordering::Relaxed),
            ephemeral_watch_evicted: self.ephemeral_watch_evicted.load(Ordering::Relaxed),
            ephemeral_watch_budget_blocked: self
                .ephemeral_watch_budget_blocked
                .load(Ordering::Relaxed),
            scan_backlog_by_tier,
            dirty_queue_len: 0,
            cold_validate_count: self.cold_validate_count.load(Ordering::Relaxed),
            query_stale_hit_count: 0,
        }
    }

    fn state(&self, path: &Path) -> Option<Arc<DirState>> {
        self.dirs.read().get(path).cloned()
    }

    pub fn debug_dump(&self, root_filter: Option<&str>) -> TieredWatchDebugDump {
        let dirs = self.dirs.read();
        let filter = root_filter.map(|s| s.to_string());
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
                freshness: format!("{:?}", state.freshness()),
                next_scan_unix_secs,
                budget_blocked_count,
                last_budget_blocked_unix_secs,
                high_priority_scan,
                ephemeral_watch: ephemeral_paths.contains(path),
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

#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct TieredWatchDebugDir {
    pub path: String,
    pub watch_tier: String,
    pub index_tier: String,
    pub watch_cost: u64,
    pub event_score: u64,
    pub last_event: u64,
    pub last_scan: u64,
    pub empty_scan_count: u32,
    pub promotion_pending: bool,
    pub demotion_pending: bool,
    pub dirty: bool,
    pub freshness: String,
    pub next_scan_unix_secs: u64,
    pub budget_blocked_count: u32,
    pub last_budget_blocked_unix_secs: u64,
    pub high_priority_scan: bool,
    pub ephemeral_watch: bool,
}

#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct TieredWatchDebugSummary {
    pub l0_dirs: usize,
    pub l1_dirs: usize,
    pub l2_dirs: usize,
    pub l3_dirs: usize,
    pub ephemeral_watch_dirs: usize,
    pub ephemeral_watch_cost: u64,
    pub ephemeral_watch_budget: usize,
    pub total_event_score: u64,
}

#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct TieredWatchDebugDump {
    pub dirs: Vec<TieredWatchDebugDir>,
    pub summary: TieredWatchDebugSummary,
}

fn path_is_under_or_equal(path: &Path, root: &Path) -> bool {
    path == root || path.starts_with(root)
}

fn path_has_component(path: &Path, component: &str) -> bool {
    path.components()
        .any(|part| part.as_os_str().to_string_lossy() == component)
}

fn next_l3_scan_unix_secs(now: u64, policy: L3ScanPolicy, interval_secs: u64) -> u64 {
    if policy.schedules_periodic_scan() {
        now.saturating_add(interval_secs.max(1))
    } else {
        u64::MAX
    }
}

fn choose_ephemeral_victim(
    leases: &HashMap<PathBuf, EphemeralWatchLease>,
    candidate: &Path,
    candidate_cost: u64,
    budget: u64,
) -> Option<PathBuf> {
    let current = leases
        .values()
        .map(|lease| lease.watch_cost)
        .fold(0u64, u64::saturating_add);
    let mut victims = leases
        .iter()
        .filter_map(|(path, lease)| {
            if lease.pending_add || lease.pending_remove {
                return None;
            }
            if path_is_under_or_equal(candidate, path) {
                return None;
            }
            if current
                .saturating_sub(lease.watch_cost)
                .saturating_add(candidate_cost)
                > budget
            {
                return None;
            }
            Some((
                lease.value_score,
                lease.last_event_unix_secs.load(Ordering::Relaxed),
                path.clone(),
            ))
        })
        .collect::<Vec<_>>();
    victims.sort_by_key(|(score, last_event, path)| (*score, *last_event, path.clone()));
    victims.into_iter().next().map(|(_, _, path)| path)
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
            rt.register_dynamic_candidate(dynamic, 6),
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

    #[test]
    fn empty_scans_demote_l1_to_l2_and_l3() {
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
        rt.apply_scan_policy(warm.as_path(), 1, 2, L3ScanPolicy::Interval, 4, 1, 1);
        let l2 = rt.report();
        assert_eq!(l2.l1_dirs, 0);
        assert_eq!(l2.l2_dirs, 1);
        assert!(l2.next_scan_unix_secs > 0);

        rt.record_scan(
            warm.as_path(),
            ScanOutcome {
                scanned: 1,
                changed: 0,
                elapsed_ms: 1,
            },
        );
        rt.apply_scan_policy(warm.as_path(), 1, 2, L3ScanPolicy::Interval, 4, 1, 1);
        let l3 = rt.report();
        assert_eq!(l3.l2_dirs, 0);
        assert_eq!(l3.l3_dirs, 1);
    }

    #[test]
    fn l2_change_returns_to_l1_and_can_promote() {
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
        rt.apply_scan_policy(warm.as_path(), 1, 2, L3ScanPolicy::Interval, 4, 1, 1);
        rt.record_scan(
            warm.as_path(),
            ScanOutcome {
                scanned: 1,
                changed: 2,
                elapsed_ms: 1,
            },
        );
        rt.apply_scan_policy(warm.as_path(), 1, 2, L3ScanPolicy::Interval, 4, 1, 1);

        assert_eq!(rt.report().l1_dirs, 1);
        assert_eq!(
            rt.try_reserve_promotion(warm.as_path()),
            PromotionDecision::SendAdd
        );
    }

    #[test]
    fn hotter_candidate_replaces_cold_l0_when_budget_full() {
        let rt = TieredWatchRuntime::new(
            vec![(PathBuf::from("/tmp/cold"), 2)],
            vec![(PathBuf::from("/tmp/hotter"), 2)],
            2,
            5_000,
            20,
        );
        let hotter = PathBuf::from("/tmp/hotter");

        rt.record_scan(
            hotter.as_path(),
            ScanOutcome {
                scanned: 1,
                changed: 4,
                elapsed_ms: 1,
            },
        );

        assert_eq!(
            rt.try_reserve_promotion(hotter.as_path()),
            PromotionDecision::Replace {
                demote: PathBuf::from("/tmp/cold"),
                promote: hotter.clone(),
            }
        );
        assert_eq!(rt.report().l0_replacements, 1);

        rt.confirm_demoted(Path::new("/tmp/cold"));
        assert!(rt.reserve_pending_promotion(hotter.as_path()));
        rt.confirm_promoted(hotter.as_path());

        let report = rt.report();
        assert_eq!(report.l0_dirs, 1);
        assert_eq!(report.l1_dirs, 1);
        assert_eq!(report.watched_dirs_estimated, 2);
    }

    #[test]
    fn report_includes_watch_cost_per_tier() {
        let rt = runtime();
        let report = rt.report();
        assert_eq!(report.l0_watch_cost, 2);
        assert_eq!(report.l1_watch_cost, 3);
        assert_eq!(report.l2_watch_cost, 0);
        assert_eq!(report.l3_watch_cost, 0);
    }

    #[test]
    fn report_includes_scan_backlog_by_tier() {
        let rt = runtime();
        // L1 dir has next_scan = 0 (default), which is <= now, so it counts as backlog.
        let report = rt.report();
        assert_eq!(report.scan_backlog_by_tier[0], 0);
        assert_eq!(report.scan_backlog_by_tier[1], 1);
        assert_eq!(report.scan_backlog_by_tier[2], 0);
        assert_eq!(report.scan_backlog_by_tier[3], 0);
    }

    #[test]
    fn debug_dump_returns_all_dirs_and_summary() {
        let rt = runtime();
        let dump = rt.debug_dump(None);
        assert_eq!(dump.dirs.len(), 2);
        assert_eq!(dump.summary.l0_dirs, 1);
        assert_eq!(dump.summary.l1_dirs, 1);
        assert_eq!(dump.summary.l2_dirs, 0);
        assert_eq!(dump.summary.l3_dirs, 0);
        assert!(dump.summary.total_event_score > 0);
    }

    #[test]
    fn debug_dump_filters_by_root_prefix() {
        let rt = runtime();
        let dump = rt.debug_dump(Some("/tmp/hot"));
        assert_eq!(dump.dirs.len(), 1);
        assert!(dump.dirs[0].path.starts_with("/tmp/hot"));
    }

    #[test]
    fn debug_dump_includes_observability_fields() {
        let rt = runtime();
        rt.record_event_paths([&PathBuf::from("/tmp/warm/file.txt")]);

        let dump = rt.debug_dump(Some("/tmp/warm"));
        assert_eq!(dump.dirs.len(), 1);
        let warm = &dump.dirs[0];
        assert_eq!(warm.watch_tier, "L1");
        assert_eq!(warm.index_tier, "WarmMemory");
        assert!(warm.dirty);
        assert_eq!(warm.freshness, "Dirty");
        assert_eq!(warm.next_scan_unix_secs, 0);
        assert_eq!(warm.budget_blocked_count, 0);
        assert_eq!(warm.last_budget_blocked_unix_secs, 0);
        assert!(!warm.high_priority_scan);
    }

    #[test]
    fn score_decay_reduces_event_score_over_time() {
        let rt = runtime();
        let warm = PathBuf::from("/tmp/warm");

        // Give warm a non-zero event score via a changed scan
        rt.record_scan(
            warm.as_path(),
            ScanOutcome {
                scanned: 1,
                changed: 1,
                elapsed_ms: 1,
            },
        );

        let before = rt.report().event_score_total;
        assert!(before > 0);

        // Decay with a far-future now so interval is exceeded
        rt.decay_scores(unix_secs() + 3_600, 3_600, 5);

        let after = rt.report().event_score_total;
        assert!(
            after < before,
            "expected score to decay: before={before}, after={after}"
        );
    }

    #[test]
    fn budget_blocked_increments_budget_blocked_count() {
        let rt =
            TieredWatchRuntime::new(vec![(PathBuf::from("/tmp/hot"), 2)], vec![], 2, 5_000, 20);
        let dynamic = PathBuf::from("/tmp/hot/child");

        // First block
        assert_eq!(
            rt.register_dynamic_candidate(dynamic.clone(), 5),
            PromotionDecision::BudgetBlocked
        );

        // Second block should set high_priority_scan
        assert_eq!(
            rt.register_dynamic_candidate(dynamic.clone(), 5),
            PromotionDecision::BudgetBlocked
        );

        // Verify via scan_batch ordering: dynamic should appear first
        let batch = rt.scan_batch(10);
        assert!(
            batch.contains(&dynamic),
            "dynamic should be in scan_batch after budget blocked"
        );
    }

    #[test]
    fn high_priority_scan_affects_scan_batch_ordering() {
        let rt =
            TieredWatchRuntime::new(vec![(PathBuf::from("/tmp/hot"), 2)], vec![], 2, 5_000, 20);
        let hot = PathBuf::from("/tmp/hot");

        // Pump up the L0 score so replacement is impossible
        rt.record_scan(
            hot.as_path(),
            ScanOutcome {
                scanned: 1,
                changed: 10,
                elapsed_ms: 1,
            },
        );

        let a = PathBuf::from("/tmp/a");
        let b = PathBuf::from("/tmp/b");

        // Register as dynamic candidates; both get BudgetBlocked because budget is full
        // and L0 victim is too hot to replace.
        assert_eq!(
            rt.register_dynamic_candidate(a.clone(), 1),
            PromotionDecision::BudgetBlocked
        );
        assert_eq!(
            rt.register_dynamic_candidate(b.clone(), 1),
            PromotionDecision::BudgetBlocked
        );

        // Block /tmp/a a second time to flip high_priority_scan
        assert_eq!(
            rt.try_reserve_promotion(a.as_path()),
            PromotionDecision::BudgetBlocked
        );

        let batch = rt.scan_batch(10);
        let a_pos = batch.iter().position(|p| p == &a);
        let b_pos = batch.iter().position(|p| p == &b);

        assert!(
            a_pos.is_some() && b_pos.is_some(),
            "both a and b should be in batch"
        );
        assert!(
            a_pos.unwrap() < b_pos.unwrap(),
            "high-priority a should come before b"
        );
    }

    #[test]
    fn sustained_l0_events_keep_hot_dir_from_being_replaced() {
        let rt = TieredWatchRuntime::new(
            vec![(PathBuf::from("/tmp/hot"), 1)],
            vec![(PathBuf::from("/tmp/candidate"), 1)],
            1,
            5_000,
            20,
        );
        let hot_event = PathBuf::from("/tmp/hot/file.txt");
        let candidate = PathBuf::from("/tmp/candidate");

        for _ in 0..12 {
            rt.record_event_paths([&hot_event]);
        }
        rt.record_scan(
            candidate.as_path(),
            ScanOutcome {
                scanned: 1,
                changed: 2,
                elapsed_ms: 1,
            },
        );

        assert_eq!(
            rt.try_reserve_promotion(candidate.as_path()),
            PromotionDecision::BudgetBlocked,
            "sustained L0 events should keep the hot directory ahead of a weaker replacement candidate; report={:?}",
            rt.report()
        );
        let report = rt.report();
        assert_eq!(report.l0_dirs, 1);
        assert_eq!(report.watched_dirs_estimated, 1);
        assert_eq!(report.l0_replacements, 0);
        assert_eq!(report.promotion_budget_blocked, 1);
    }

    #[test]
    fn replacement_candidate_cannot_evict_its_ancestor_l0() {
        let rt = TieredWatchRuntime::new(
            vec![(PathBuf::from("/workspace"), 3)],
            vec![(PathBuf::from("/workspace/project"), 1)],
            3,
            5_000,
            20,
        );
        let child = PathBuf::from("/workspace/project");

        rt.record_scan(
            child.as_path(),
            ScanOutcome {
                scanned: 10,
                changed: 20,
                elapsed_ms: 1,
            },
        );

        assert_eq!(
            rt.try_reserve_promotion(child.as_path()),
            PromotionDecision::BudgetBlocked,
            "a child candidate must not free budget by evicting its covering ancestor; report={:?}",
            rt.report()
        );
        let report = rt.report();
        assert_eq!(report.l0_dirs, 1);
        assert_eq!(report.l1_dirs, 1);
        assert_eq!(report.watched_dirs_estimated, 3);
        assert_eq!(report.l0_replacements, 0);
    }

    #[test]
    fn freshness_roundtrip() {
        assert_eq!(
            Freshness::from_u8(Freshness::Fresh.as_u8()),
            Freshness::Fresh
        );
        assert_eq!(
            Freshness::from_u8(Freshness::Stale.as_u8()),
            Freshness::Stale
        );
        assert_eq!(
            Freshness::from_u8(Freshness::Dirty.as_u8()),
            Freshness::Dirty
        );
        assert_eq!(
            Freshness::from_u8(Freshness::Unknown.as_u8()),
            Freshness::Unknown
        );
        assert_eq!(Freshness::from_u8(255), Freshness::Unknown);
    }

    #[test]
    fn index_residency_roundtrip() {
        assert_eq!(
            IndexResidency::from_u8(IndexResidency::HotMemory.as_u8()),
            IndexResidency::HotMemory
        );
        assert_eq!(
            IndexResidency::from_u8(IndexResidency::WarmMemory.as_u8()),
            IndexResidency::WarmMemory
        );
        assert_eq!(
            IndexResidency::from_u8(IndexResidency::ColdMmap.as_u8()),
            IndexResidency::ColdMmap
        );
        assert_eq!(
            IndexResidency::from_u8(IndexResidency::FrozenManifestOnly.as_u8()),
            IndexResidency::FrozenManifestOnly
        );
        assert_eq!(
            IndexResidency::from_u8(255),
            IndexResidency::FrozenManifestOnly
        );
    }

    #[test]
    fn dir_state_initializes_correctly_by_tier() {
        let now = unix_secs();
        let l0 = DirState::new(WatchTier::L0, 1, now);
        assert_eq!(l0.tier(), WatchTier::L0);
        assert_eq!(l0.freshness(), Freshness::Fresh);
        assert_eq!(l0.index_residency(), IndexResidency::HotMemory);
        assert!(!l0.dirty.load(Ordering::Relaxed));

        let l1 = DirState::new(WatchTier::L1, 1, now);
        assert_eq!(l1.tier(), WatchTier::L1);
        assert_eq!(l1.freshness(), Freshness::Unknown);
        assert_eq!(l1.index_residency(), IndexResidency::WarmMemory);

        let l2 = DirState::new(WatchTier::L2, 1, now);
        assert_eq!(l2.tier(), WatchTier::L2);
        assert_eq!(l2.freshness(), Freshness::Unknown);
        assert_eq!(l2.index_residency(), IndexResidency::ColdMmap);

        let l3 = DirState::new(WatchTier::L3, 1, now);
        assert_eq!(l3.tier(), WatchTier::L3);
        assert_eq!(l3.freshness(), Freshness::Unknown);
        assert_eq!(l3.index_residency(), IndexResidency::FrozenManifestOnly);
    }

    #[test]
    fn record_event_paths_sets_dirty_for_non_l0() {
        let rt = runtime();
        let _warm = PathBuf::from("/tmp/warm");
        let dirty_dirs = rt.record_event_paths([&PathBuf::from("/tmp/warm/leaf/file.txt")]);
        let report = rt.report();
        assert_eq!(report.dirty_dirs, 1);
        assert_eq!(report.dirty_queue_len, 0);
        assert_eq!(report.fresh_dirs, 1); // L0 is fresh
        assert_eq!(dirty_dirs, vec![PathBuf::from("/tmp/warm/leaf")]);
    }

    #[test]
    fn record_scan_clears_dirty_and_sets_fresh() {
        let rt = runtime();
        let warm = PathBuf::from("/tmp/warm");
        rt.record_event_paths([&PathBuf::from("/tmp/warm/file.txt")]);
        let before = rt.report();
        assert_eq!(before.dirty_dirs, 1);

        rt.record_scan(
            warm.as_path(),
            ScanOutcome {
                scanned: 1,
                changed: 0,
                elapsed_ms: 1,
            },
        );
        let after = rt.report();
        assert_eq!(after.dirty_dirs, 0);
        assert_eq!(after.dirty_queue_len, 0);
        assert_eq!(after.fresh_dirs, 2); // both L0 and L1 are fresh now
    }

    #[test]
    fn record_scan_for_leaf_clears_covering_cold_root() {
        let rt = runtime();
        let leaf = PathBuf::from("/tmp/warm/deep/file.txt");
        rt.record_event_paths([&leaf]);
        assert_eq!(rt.report().dirty_dirs, 1);

        let recorded = rt.record_scan_for_path(
            Path::new("/tmp/warm/deep"),
            ScanOutcome {
                scanned: 1,
                changed: 0,
                elapsed_ms: 1,
            },
        );

        assert_eq!(recorded, Some(PathBuf::from("/tmp/warm")));
        let report = rt.report();
        assert_eq!(report.dirty_dirs, 0);
        assert_eq!(report.fresh_dirs, 2);
    }

    #[test]
    fn demotion_and_promotion_update_residency() {
        let rt = runtime();
        let hot = PathBuf::from("/tmp/hot");
        let warm = PathBuf::from("/tmp/warm");

        // L0 -> L1 demotion
        rt.mark_demotion_pending(hot.as_path());
        rt.confirm_demoted(hot.as_path());
        let report = rt.report();
        assert_eq!(report.hot_memory_dirs, 0);
        assert_eq!(report.warm_memory_dirs, 2);

        // Promote warm to L0
        rt.try_reserve_promotion(warm.as_path());
        rt.confirm_promoted(warm.as_path());
        let report2 = rt.report();
        assert_eq!(report2.hot_memory_dirs, 1);
        assert_eq!(report2.warm_memory_dirs, 1);
    }

    #[test]
    fn scan_policy_demotion_updates_residency() {
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
        rt.apply_scan_policy(warm.as_path(), 1, 2, L3ScanPolicy::Interval, 4, 1, 1);
        let l2 = rt.report();
        assert_eq!(l2.l2_dirs, 1);
        assert_eq!(l2.cold_mmap_dirs, 1);
        assert_eq!(l2.warm_memory_dirs, 0);
        assert_eq!(l2.cold_validate_count, 0);

        rt.record_scan(
            warm.as_path(),
            ScanOutcome {
                scanned: 1,
                changed: 0,
                elapsed_ms: 1,
            },
        );
        rt.apply_scan_policy(warm.as_path(), 1, 2, L3ScanPolicy::Interval, 4, 1, 1);
        let l3 = rt.report();
        assert_eq!(l3.l3_dirs, 1);
        assert_eq!(l3.frozen_manifest_dirs, 1);
        assert_eq!(l3.cold_mmap_dirs, 0);
        assert_eq!(l3.cold_validate_count, 1);
    }

    #[test]
    fn l3_interval_policy_uses_explicit_l3_interval() {
        let rt = runtime();
        let warm = PathBuf::from("/tmp/warm");
        let before = unix_secs();

        rt.record_scan(
            warm.as_path(),
            ScanOutcome {
                scanned: 1,
                changed: 0,
                elapsed_ms: 1,
            },
        );
        rt.apply_scan_policy(warm.as_path(), 1, 2, L3ScanPolicy::Interval, 99, 1, 1);
        rt.record_scan(
            warm.as_path(),
            ScanOutcome {
                scanned: 1,
                changed: 0,
                elapsed_ms: 1,
            },
        );
        rt.apply_scan_policy(warm.as_path(), 1, 2, L3ScanPolicy::Interval, 99, 1, 1);

        let dump = rt.debug_dump(Some("/tmp/warm"));
        let dir = dump.dirs.first().expect("warm dir should be present");
        assert_eq!(dir.watch_tier, "L3");
        assert!(dir.next_scan_unix_secs >= before.saturating_add(99));
        assert!(dir.next_scan_unix_secs < before.saturating_add(120));
    }

    #[test]
    fn l3_validate_on_query_policy_disables_periodic_l3_scan() {
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
        rt.apply_scan_policy(
            warm.as_path(),
            1,
            2,
            L3ScanPolicy::ValidateOnQuery,
            99,
            1,
            1,
        );
        rt.record_scan(
            warm.as_path(),
            ScanOutcome {
                scanned: 1,
                changed: 0,
                elapsed_ms: 1,
            },
        );
        rt.apply_scan_policy(
            warm.as_path(),
            1,
            2,
            L3ScanPolicy::ValidateOnQuery,
            99,
            1,
            1,
        );

        let dump = rt.debug_dump(Some("/tmp/warm"));
        let dir = dump.dirs.first().expect("warm dir should be present");
        assert_eq!(dir.watch_tier, "L3");
        assert_eq!(dir.next_scan_unix_secs, u64::MAX);
        assert!(rt.scan_batch(8).is_empty());
    }

    #[test]
    fn l3_disabled_policy_disables_periodic_l3_scan() {
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
        rt.apply_scan_policy(warm.as_path(), 1, 2, L3ScanPolicy::Disabled, 99, 1, 1);
        rt.record_scan(
            warm.as_path(),
            ScanOutcome {
                scanned: 1,
                changed: 0,
                elapsed_ms: 1,
            },
        );
        rt.apply_scan_policy(warm.as_path(), 1, 2, L3ScanPolicy::Disabled, 99, 1, 1);

        let dump = rt.debug_dump(Some("/tmp/warm"));
        let dir = dump.dirs.first().expect("warm dir should be present");
        assert_eq!(dir.watch_tier, "L3");
        assert_eq!(dir.next_scan_unix_secs, u64::MAX);
        assert!(rt.scan_batch(8).is_empty());
    }

    #[test]
    fn budget_blocked_tracks_per_dir() {
        let rt = runtime();
        let dynamic = PathBuf::from("/tmp/hot/too-large-child");

        assert_eq!(
            rt.register_dynamic_candidate(dynamic.clone(), 6),
            PromotionDecision::BudgetBlocked
        );
        let report = rt.report();
        assert_eq!(report.promotion_budget_blocked, 1);
    }

    #[test]
    fn ephemeral_watch_created_after_repeated_dirty_scope() {
        let rt = TieredWatchRuntime::new_with_ephemeral(Vec::new(), Vec::new(), 1, 5_000, 20, 4);
        let cfg = EphemeralWatchConfig {
            budget: 4,
            repeat_window_secs: 10,
            repeat_threshold: 2,
            max_cost_per_root: 2,
            ..EphemeralWatchConfig::default()
        };
        let dir = PathBuf::from("/tmp/cold");

        assert_eq!(
            rt.note_dirty_scope_at(dir.clone(), 2, &[], &cfg, 100, 1),
            EphemeralWatchDecision::NotEligible
        );
        assert_eq!(
            rt.note_dirty_scope_at(dir.clone(), 2, &[], &cfg, 105, 1),
            EphemeralWatchDecision::Add(dir.clone())
        );

        let reserved = rt.report();
        assert_eq!(reserved.ephemeral_watch_cost, 2);
        assert_eq!(reserved.ephemeral_watch_dirs, 0);

        rt.confirm_ephemeral_added(dir.as_path());
        let report = rt.report();
        assert_eq!(report.ephemeral_watch_dirs, 1);
        assert_eq!(report.ephemeral_watch_created, 1);
        assert_eq!(report.ephemeral_watch_budget_blocked, 0);
    }

    #[test]
    fn ephemeral_watch_respects_exclude_l0_and_cost_limits() {
        let rt = TieredWatchRuntime::new_with_ephemeral(
            vec![(PathBuf::from("/tmp/hot"), 1)],
            Vec::new(),
            1,
            5_000,
            20,
            4,
        );
        let cfg = EphemeralWatchConfig {
            budget: 4,
            repeat_threshold: 1,
            max_cost_per_root: 2,
            ..EphemeralWatchConfig::default()
        };

        assert_eq!(
            rt.note_dirty_scope_at(PathBuf::from("/tmp/hot/leaf"), 1, &[], &cfg, 100, 1),
            EphemeralWatchDecision::NotEligible
        );
        assert_eq!(
            rt.note_dirty_scope_at(
                PathBuf::from("/tmp/cold/node_modules/pkg"),
                1,
                &["node_modules".to_string()],
                &cfg,
                100,
                1,
            ),
            EphemeralWatchDecision::NotEligible
        );
        assert_eq!(
            rt.note_dirty_scope_at(PathBuf::from("/tmp/cold/too-large"), 3, &[], &cfg, 100, 1),
            EphemeralWatchDecision::NotEligible
        );

        let report = rt.report();
        assert_eq!(report.ephemeral_watch_cost, 0);
        assert_eq!(report.ephemeral_watch_dirs, 0);
    }

    #[test]
    fn ephemeral_watch_expires_for_idle_ttl_no_change_and_l0_cover() {
        let rt = TieredWatchRuntime::new_with_ephemeral(Vec::new(), Vec::new(), 1, 5_000, 20, 10);
        let cfg = EphemeralWatchConfig {
            budget: 10,
            repeat_threshold: 1,
            max_cost_per_root: 2,
            ..EphemeralWatchConfig::default()
        };

        let idle = PathBuf::from("/tmp/idle");
        assert_eq!(
            rt.note_dirty_scope_at(idle.clone(), 1, &[], &cfg, 100, 1),
            EphemeralWatchDecision::Add(idle.clone())
        );
        rt.confirm_ephemeral_added(idle.as_path());
        let removals = rt.expire_ephemeral_watches_at(130, 20, 100, 3);
        assert_eq!(removals.len(), 1);
        assert_eq!(removals[0].path, idle);
        assert_eq!(removals[0].reason, EphemeralWatchExpiry::Idle);
        rt.confirm_ephemeral_removed(removals[0].path.as_path());

        let ttl = PathBuf::from("/tmp/ttl");
        assert_eq!(
            rt.note_dirty_scope_at(ttl.clone(), 1, &[], &cfg, 200, 1),
            EphemeralWatchDecision::Add(ttl.clone())
        );
        rt.confirm_ephemeral_added(ttl.as_path());
        let removals = rt.expire_ephemeral_watches_at(260, 100, 50, 3);
        assert_eq!(removals[0].reason, EphemeralWatchExpiry::Ttl);
        rt.confirm_ephemeral_removed(removals[0].path.as_path());

        let quiet = PathBuf::from("/tmp/quiet");
        assert_eq!(
            rt.note_dirty_scope_at(quiet.clone(), 1, &[], &cfg, 300, 0),
            EphemeralWatchDecision::Add(quiet.clone())
        );
        rt.confirm_ephemeral_added(quiet.as_path());
        rt.record_dirty_scope_repeat(quiet.as_path(), 0, &cfg);
        rt.record_dirty_scope_repeat(quiet.as_path(), 0, &cfg);
        let removals = rt.expire_ephemeral_watches_at(301, 100, 100, 2);
        assert_eq!(removals[0].reason, EphemeralWatchExpiry::NoChange);
        rt.confirm_ephemeral_removed(removals[0].path.as_path());

        let covered = PathBuf::from("/workspace/project");
        assert_eq!(
            rt.note_dirty_scope_at(covered.clone(), 1, &[], &cfg, 400, 1),
            EphemeralWatchDecision::Add(covered.clone())
        );
        rt.confirm_ephemeral_added(covered.as_path());
        let l0 = PathBuf::from("/workspace");
        assert_eq!(
            rt.register_dynamic_candidate(l0.clone(), 1),
            PromotionDecision::SendAdd
        );
        rt.confirm_promoted(l0.as_path());
        let removals = rt.expire_ephemeral_watches_at(401, 100, 100, 3);
        assert_eq!(removals[0].reason, EphemeralWatchExpiry::CoveredByL0);
    }

    #[test]
    fn ephemeral_budget_replaces_low_value_lease_and_rolls_back() {
        let rt = TieredWatchRuntime::new_with_ephemeral(Vec::new(), Vec::new(), 1, 5_000, 20, 2);
        let cfg = EphemeralWatchConfig {
            budget: 2,
            repeat_threshold: 1,
            max_cost_per_root: 1,
            ..EphemeralWatchConfig::default()
        };
        let old = PathBuf::from("/tmp/old");
        let new = PathBuf::from("/tmp/new");

        assert_eq!(
            rt.note_dirty_scope_at(old.clone(), 1, &[], &cfg, 100, 0),
            EphemeralWatchDecision::Add(old.clone())
        );
        rt.confirm_ephemeral_added(old.as_path());
        assert_eq!(
            rt.note_dirty_scope_at(PathBuf::from("/tmp/other"), 1, &[], &cfg, 101, 0),
            EphemeralWatchDecision::Add(PathBuf::from("/tmp/other"))
        );

        assert_eq!(
            rt.note_dirty_scope_at(new.clone(), 1, &[], &cfg, 102, 10),
            EphemeralWatchDecision::Replace {
                remove: old.clone(),
                add: new.clone(),
            }
        );
        let replaced = rt.report();
        assert_eq!(replaced.ephemeral_watch_cost, 2);
        assert_eq!(replaced.ephemeral_watch_evicted, 1);

        rt.rollback_ephemeral_replace(old.as_path(), new.as_path());
        let rolled_back = rt.report();
        assert_eq!(rolled_back.ephemeral_watch_cost, 2);
        assert_eq!(rolled_back.ephemeral_watch_dirs, 1);
    }
}
