//! Ephemeral watch logic: lease management, dirty scope observation, victim selection.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::util::unix_secs;

use super::path_has_component;
use super::path_is_under_or_equal;

use super::types::*;
use super::TieredWatchRuntime;

#[derive(Debug)]
pub(super) struct EphemeralWatchLease {
    pub(super) path: PathBuf,
    pub(super) watch_cost: u64,
    pub(super) created_unix_secs: u64,
    pub(super) last_event_unix_secs: AtomicU64,
    pub(super) last_dirty_unix_secs: u64,
    pub(super) dirty_hits: u32,
    pub(super) no_change_scans: u32,
    pub(super) value_score: u64,
    pub(super) pending_add: bool,
    pub(super) pending_remove: bool,
}

impl EphemeralWatchLease {
    pub(super) fn pending(path: PathBuf, watch_cost: usize, now: u64) -> Self {
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
pub(super) struct DirtyScopeObservation {
    pub(super) last_unix_secs: u64,
    pub(super) hits: u32,
    pub(super) score: u64,
}

impl TieredWatchRuntime {
    pub(super) fn record_ephemeral_events(&self, paths: &[&PathBuf], now: u64) {
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
            self.note_watch_exclude_rejected();
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
        let remaining =
            budget.saturating_sub(self.current_ephemeral_watch_cost.load(Ordering::Relaxed));
        self.record_last_budget_blocked(
            format!(
                "ephemeral budget blocked: kernel_watch_cost={} budget_remaining={}",
                cost, remaining
            ),
            cost,
            remaining,
        );
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

    pub(super) fn observe_dirty_scope(
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

    pub(super) fn release_ephemeral_cost(&self, cost: u64) {
        let _ = self.current_ephemeral_watch_cost.fetch_update(
            Ordering::AcqRel,
            Ordering::Relaxed,
            |current| Some(current.saturating_sub(cost)),
        );
    }
}

pub(super) fn choose_ephemeral_victim(
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
