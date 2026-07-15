//! Fast scan lease logic: sentinel state, tick config, lease management.

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use serde::{Deserialize, Serialize};

use crate::config::{NetworkFastScanMode, TieredWatchConfig};
use crate::fs_policy::MountTable;
use crate::util::unix_secs;

use super::unix_millis;

use super::types::*;
use super::TieredWatchRuntime;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct DirSentinelSignature {
    pub(super) dev: u64,
    pub(super) ino: u64,
    pub(super) mtime_ns: i128,
    pub(super) ctime_ns: i128,
    pub(super) nlink: u64,
}

#[cfg(unix)]
pub(super) fn dir_sentinel_signature(meta: &std::fs::Metadata) -> DirSentinelSignature {
    use std::os::unix::fs::MetadataExt;

    DirSentinelSignature {
        dev: meta.dev(),
        ino: meta.ino(),
        mtime_ns: i128::from(meta.mtime())
            .saturating_mul(1_000_000_000)
            .saturating_add(i128::from(meta.mtime_nsec())),
        ctime_ns: i128::from(meta.ctime())
            .saturating_mul(1_000_000_000)
            .saturating_add(i128::from(meta.ctime_nsec())),
        nlink: meta.nlink(),
    }
}

#[cfg(not(unix))]
pub(super) fn dir_sentinel_signature(meta: &std::fs::Metadata) -> DirSentinelSignature {
    let mtime_ns = meta
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos().min(i128::MAX as u128) as i128)
        .unwrap_or(0);
    DirSentinelSignature {
        mtime_ns,
        ctime_ns: mtime_ns,
        nlink: 0,
        ..DirSentinelSignature::default()
    }
}

#[derive(Clone, Debug)]
pub(super) struct FastScanLease {
    pub(super) lease_kind: FastScanLeaseKind,
    pub(super) created_unix_secs: u64,
    pub(super) expires_unix_secs: u64,
    pub(super) last_used_unix_secs: u64,
    pub(super) priority: u32,
    pub(super) estimated_scan_cost: u64,
    pub(super) strict_sla_allowed: bool,
    pub(super) source_score: u64,
    pub(super) renew_count: u64,
    pub(super) sentinel_state: FastScanSentinelState,
}

impl FastScanLease {
    pub(super) fn expired(&self, now: u64) -> bool {
        self.expires_unix_secs != u64::MAX && self.expires_unix_secs <= now
    }
}

#[derive(Clone, Debug)]
pub(super) struct DirSentinel {
    pub(super) mount_id: u32,
    pub(super) fstype: String,
    pub(super) class: FastScanMountClass,
    pub(super) lease_kind: FastScanLeaseKind,
    pub(super) signature: DirSentinelSignature,
    pub(super) trust_clock: bool,
    pub(super) trust_nlink: bool,
    pub(super) strict_sla_allowed: bool,
    pub(super) sentinel_state: FastScanSentinelState,
    pub(super) last_checked_unix_ms: u64,
    pub(super) last_changed_unix_ms: u64,
    pub(super) coverage_lag_ms: u64,
}

#[derive(Debug, Default)]
pub(super) struct FastScanState {
    pub(super) leases: HashMap<PathBuf, FastScanLease>,
    pub(super) sentinels: HashMap<PathBuf, DirSentinel>,
    pub(super) initial_backfill_queue: VecDeque<PathBuf>,
    pub(super) changed_dir_queue: VecDeque<PathBuf>,
    pub(super) last_degraded_reason: String,
}

impl TieredWatchRuntime {
    pub fn apply_fast_scan_config(&self, config: &TieredWatchConfig) {
        self.fast_scan_enabled
            .store(config.l1_l2_fast_scan_enabled, Ordering::Relaxed);
        self.fast_scan_target_secs
            .store(config.l1_l2_fast_scan_target_secs.max(1), Ordering::Relaxed);
        self.fast_scan_tick_ms
            .store(config.l1_l2_fast_scan_tick_ms.max(100), Ordering::Relaxed);
        self.fast_scan_local_stat_budget_per_tick.store(
            config.l1_l2_fast_scan_stat_budget_per_tick.max(1),
            Ordering::Relaxed,
        );
        self.fast_scan_network_stat_budget_per_tick.store(
            config.network_fast_scan_stat_budget_per_tick,
            Ordering::Relaxed,
        );
        self.fast_scan_local_readdir_budget_per_tick.store(
            config.l1_l2_fast_scan_readdir_budget_per_tick.max(1),
            Ordering::Relaxed,
        );
        self.fast_scan_network_readdir_budget_per_tick.store(
            config.network_fast_scan_readdir_budget_per_tick,
            Ordering::Relaxed,
        );
        self.fast_scan_initial_backfill_budget_per_tick.store(
            config.l1_l2_fast_scan_bootstrap_budget_per_tick.max(1),
            Ordering::Relaxed,
        );
        self.fast_scan_hotset_max_leases.store(
            config.l1_l2_fast_scan_hotset_max_leases.max(1),
            Ordering::Relaxed,
        );
        self.fast_scan_lease_ttl_secs.store(
            config.l1_l2_fast_scan_lease_ttl_secs.max(1),
            Ordering::Relaxed,
        );
        self.fast_scan_proc_sampler_lease_ttl_secs.store(
            config.l1_l2_fast_scan_proc_sampler_lease_ttl_secs.max(1),
            Ordering::Relaxed,
        );
        self.fast_scan_explicit_lease_ttl_secs.store(
            config.l1_l2_fast_scan_explicit_lease_ttl_secs,
            Ordering::Relaxed,
        );
        self.fast_scan_sentinel_registry_max_entries.store(
            config.l1_l2_fast_scan_sentinel_registry_max_entries.max(1),
            Ordering::Relaxed,
        );
        self.fast_scan_network_mode.store(
            network_fast_scan_mode_to_u8(config.network_fast_scan_mode),
            Ordering::Relaxed,
        );
    }

    pub fn fast_scan_tick_config(&self) -> FastScanTickConfig {
        FastScanTickConfig {
            enabled: self.fast_scan_enabled.load(Ordering::Relaxed),
            target_secs: self.fast_scan_target_secs.load(Ordering::Relaxed),
            tick_ms: self.fast_scan_tick_ms.load(Ordering::Relaxed),
            local_stat_budget_per_tick: self
                .fast_scan_local_stat_budget_per_tick
                .load(Ordering::Relaxed),
            network_stat_budget_per_tick: self
                .fast_scan_network_stat_budget_per_tick
                .load(Ordering::Relaxed),
            local_readdir_budget_per_tick: self
                .fast_scan_local_readdir_budget_per_tick
                .load(Ordering::Relaxed),
            network_readdir_budget_per_tick: self
                .fast_scan_network_readdir_budget_per_tick
                .load(Ordering::Relaxed),
            initial_backfill_budget_per_tick: self
                .fast_scan_initial_backfill_budget_per_tick
                .load(Ordering::Relaxed),
            max_hotset_leases: self.fast_scan_hotset_max_leases.load(Ordering::Relaxed),
            lease_ttl_secs: self.fast_scan_lease_ttl_secs.load(Ordering::Relaxed),
            proc_sampler_lease_ttl_secs: self
                .fast_scan_proc_sampler_lease_ttl_secs
                .load(Ordering::Relaxed),
            explicit_lease_ttl_secs: self
                .fast_scan_explicit_lease_ttl_secs
                .load(Ordering::Relaxed),
            sentinel_registry_max_entries: self
                .fast_scan_sentinel_registry_max_entries
                .load(Ordering::Relaxed),
            network_mode: network_fast_scan_mode_from_u8(
                self.fast_scan_network_mode.load(Ordering::Relaxed),
            ),
        }
    }

    pub fn should_bootstrap_fast_scan_dirs(&self, candidate_limit: usize) -> bool {
        self.should_bootstrap_fast_scan_dirs_at(candidate_limit, unix_millis())
    }

    pub fn should_bootstrap_fast_scan_dirs_at(&self, candidate_limit: usize, now_ms: u64) -> bool {
        if !self.fast_scan_enabled.load(Ordering::Relaxed) {
            return false;
        }
        let next_allowed = self
            .fast_scan_bootstrap_next_unix_ms
            .load(Ordering::Relaxed);
        if next_allowed > now_ms {
            return false;
        }

        let state = self.fast_scan_state.read();
        let now_secs = unix_secs();
        let has_uncovered_lease = state.leases.iter().any(|(path, lease)| {
            !lease.expired(now_secs)
                && !state.sentinels.contains_key(path)
                && !matches!(self.covering_tier(path.as_path()), Some(WatchTier::L0))
        });
        has_uncovered_lease && state.initial_backfill_queue.len() < candidate_limit.max(1)
    }

    pub fn fast_scan_l0_roots(&self) -> Vec<PathBuf> {
        let dirs = self.dirs.read();
        dirs.iter()
            .filter_map(|(path, state)| {
                if state.tier() == WatchTier::L0 {
                    Some(path.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn fast_scan_bootstrap_excluded_roots(&self) -> Vec<PathBuf> {
        let mut roots = self.fast_scan_l0_roots();
        roots.extend(self.fast_scan_state.read().sentinels.keys().cloned());
        roots.sort();
        roots.dedup();
        roots
    }

    pub fn pending_fast_scan_lease_dirs(&self, limit: usize) -> Vec<PathBuf> {
        let limit = limit.max(1);
        let now = unix_secs();
        let state = self.fast_scan_state.read();
        let mut dirs = state
            .leases
            .iter()
            .filter_map(|(path, lease)| {
                if lease.expired(now) || state.sentinels.contains_key(path) {
                    return None;
                }
                if matches!(self.covering_tier(path.as_path()), Some(WatchTier::L0)) {
                    return None;
                }
                Some(path.clone())
            })
            .collect::<Vec<_>>();
        dirs.sort();
        dirs.truncate(limit);
        dirs
    }

    pub fn seed_fast_scan_explicit_leases(&self, dirs: impl IntoIterator<Item = PathBuf>) -> usize {
        dirs.into_iter()
            .filter(|path| {
                std::fs::symlink_metadata(path)
                    .map(|meta| meta.is_dir())
                    .unwrap_or(false)
            })
            .map(|path| self.grant_fast_scan_lease(path, FastScanLeaseKind::Explicit, None, 1))
            .filter(|inserted| *inserted)
            .count()
    }

    pub fn grant_fast_scan_leases(
        &self,
        dirs: impl IntoIterator<Item = PathBuf>,
        kind: FastScanLeaseKind,
        ttl_secs: Option<u64>,
        source_score: u64,
    ) -> usize {
        dirs.into_iter()
            .filter(|path| {
                std::fs::symlink_metadata(path)
                    .map(|meta| meta.is_dir())
                    .unwrap_or(false)
            })
            .map(|path| self.grant_fast_scan_lease(path, kind, ttl_secs, source_score))
            .filter(|inserted| *inserted)
            .count()
    }

    /// Grants query hotset leases only where no recursive ephemeral watcher exists.
    pub fn grant_query_fast_scan_leases(
        &self,
        dirs: impl IntoIterator<Item = PathBuf>,
        ttl_secs: Option<u64>,
        source_score: u64,
    ) -> usize {
        let uncovered = dirs
            .into_iter()
            .filter(|path| !self.confirmed_ephemeral_watch_covers(path));
        self.grant_fast_scan_leases(uncovered, FastScanLeaseKind::Query, ttl_secs, source_score)
    }

    pub fn grant_fast_scan_lease(
        &self,
        path: PathBuf,
        kind: FastScanLeaseKind,
        ttl_secs: Option<u64>,
        source_score: u64,
    ) -> bool {
        if !self.fast_scan_enabled.load(Ordering::Relaxed) {
            return false;
        }

        let now = unix_secs();
        let ttl = ttl_secs.unwrap_or_else(|| self.default_fast_scan_lease_ttl(kind));
        let expires = if kind.is_explicit() && ttl == 0 {
            u64::MAX
        } else {
            now.saturating_add(ttl.max(1))
        };
        let path = normalize_fast_scan_dir(path);
        let mut state = self.fast_scan_state.write();
        self.prune_expired_fast_scan_leases_locked(&mut state, now);

        if state.leases.contains_key(path.as_path()) {
            let lease_kind = {
                let lease = state
                    .leases
                    .get_mut(path.as_path())
                    .expect("checked lease existence");
                lease.last_used_unix_secs = now;
                lease.expires_unix_secs = lease.expires_unix_secs.max(expires);
                lease.priority = lease.priority.max(kind.priority());
                lease.source_score = lease.source_score.saturating_add(source_score.max(1));
                lease.renew_count = lease.renew_count.saturating_add(1);
                if kind.priority() >= lease.lease_kind.priority() {
                    lease.lease_kind = kind;
                }
                lease.lease_kind
            };
            if let Some(sentinel) = state.sentinels.get_mut(path.as_path()) {
                sentinel.lease_kind = lease_kind;
            } else {
                self.fast_scan_bootstrap_next_unix_ms
                    .store(0, Ordering::Relaxed);
            }
            self.fast_scan_lease_renewals
                .fetch_add(1, Ordering::Relaxed);
            return false;
        }

        let max_leases = self
            .fast_scan_hotset_max_leases
            .load(Ordering::Relaxed)
            .max(1);
        if state.leases.len() >= max_leases && !self.evict_one_fast_scan_lease_locked(&mut state) {
            self.fast_scan_budget_degraded
                .store(true, Ordering::Relaxed);
            state.last_degraded_reason =
                format!("lease hotset budget full: max_leases={max_leases}");
            return false;
        }

        state.leases.insert(
            path.clone(),
            FastScanLease {
                lease_kind: kind,
                created_unix_secs: now,
                expires_unix_secs: expires,
                last_used_unix_secs: now,
                priority: kind.priority(),
                estimated_scan_cost: 1,
                strict_sla_allowed: false,
                source_score: source_score.max(1),
                renew_count: 0,
                sentinel_state: FastScanSentinelState::BackfillPending,
            },
        );
        self.fast_scan_bootstrap_next_unix_ms
            .store(0, Ordering::Relaxed);
        true
    }

    pub(super) fn default_fast_scan_lease_ttl(&self, kind: FastScanLeaseKind) -> u64 {
        match kind {
            FastScanLeaseKind::Explicit => self
                .fast_scan_explicit_lease_ttl_secs
                .load(Ordering::Relaxed),
            FastScanLeaseKind::ProcSampler => self
                .fast_scan_proc_sampler_lease_ttl_secs
                .load(Ordering::Relaxed)
                .max(1),
            _ => self.fast_scan_lease_ttl_secs.load(Ordering::Relaxed).max(1),
        }
    }

    pub(super) fn prune_expired_fast_scan_leases_locked(
        &self,
        state: &mut FastScanState,
        now: u64,
    ) {
        let expired = state
            .leases
            .iter()
            .filter_map(|(path, lease)| lease.expired(now).then_some(path.clone()))
            .collect::<Vec<_>>();
        for path in expired {
            state.leases.remove(path.as_path());
            state.sentinels.remove(path.as_path());
            remove_queued_path(&mut state.initial_backfill_queue, path.as_path());
            remove_queued_path(&mut state.changed_dir_queue, path.as_path());
            self.fast_scan_lease_evictions
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(super) fn evict_one_fast_scan_lease_locked(&self, state: &mut FastScanState) -> bool {
        let Some(victim) = state
            .leases
            .iter()
            .filter(|(_, lease)| !lease.lease_kind.is_explicit())
            .min_by_key(|(path, lease)| {
                (
                    lease.priority,
                    lease.source_score,
                    lease.renew_count,
                    std::cmp::Reverse(lease.expires_unix_secs),
                    (*path).clone(),
                )
            })
            .map(|(path, _)| path.clone())
        else {
            return false;
        };
        state.leases.remove(victim.as_path());
        state.sentinels.remove(victim.as_path());
        remove_queued_path(&mut state.initial_backfill_queue, victim.as_path());
        remove_queued_path(&mut state.changed_dir_queue, victim.as_path());
        self.fast_scan_lease_evictions
            .fetch_add(1, Ordering::Relaxed);
        true
    }

    pub fn record_fast_scan_bootstrap_result(
        &self,
        candidate_count: usize,
        inserted: usize,
        candidate_limit: usize,
        exhausted_retry_ms: u64,
    ) {
        self.record_fast_scan_bootstrap_result_at(
            candidate_count,
            inserted,
            candidate_limit,
            exhausted_retry_ms,
            unix_millis(),
        );
    }

    pub fn record_fast_scan_bootstrap_result_at(
        &self,
        _candidate_count: usize,
        inserted: usize,
        _candidate_limit: usize,
        exhausted_retry_ms: u64,
        now_ms: u64,
    ) {
        if inserted == 0 {
            self.fast_scan_bootstrap_next_unix_ms.store(
                now_ms.saturating_add(exhausted_retry_ms.max(1_000)),
                Ordering::Relaxed,
            );
        } else {
            self.fast_scan_bootstrap_next_unix_ms
                .store(0, Ordering::Relaxed);
        }
    }

    pub fn bootstrap_fast_scan_dirs(
        &self,
        dirs: impl IntoIterator<Item = PathBuf>,
        mount_table: &MountTable,
        limit: usize,
    ) -> usize {
        if !self.fast_scan_enabled.load(Ordering::Relaxed) {
            return 0;
        }
        let mut inserted = 0usize;
        let now_ms = unix_millis();
        let now_secs = unix_secs();
        let mut state = self.fast_scan_state.write();
        self.prune_expired_fast_scan_leases_locked(&mut state, now_secs);
        for raw_path in dirs.into_iter().take(limit.max(1)) {
            let path = normalize_fast_scan_dir(raw_path);
            if state.sentinels.contains_key(path.as_path()) {
                continue;
            }
            if state
                .leases
                .get(path.as_path())
                .is_none_or(|lease| lease.expired(now_secs))
            {
                continue;
            }
            if matches!(self.covering_tier(path.as_path()), Some(WatchTier::L0)) {
                continue;
            }
            let Ok(meta) = std::fs::symlink_metadata(&path) else {
                continue;
            };
            if !meta.is_dir() {
                continue;
            }
            let (mount_id, fstype, class) = fast_scan_mount_info(path.as_path(), mount_table);
            let strict_sla_allowed = class.strict_sla_allowed();
            let lease_kind = if let Some(lease) = state.leases.get_mut(path.as_path()) {
                lease.strict_sla_allowed = strict_sla_allowed;
                lease.sentinel_state = FastScanSentinelState::BackfillPending;
                lease.lease_kind
            } else {
                FastScanLeaseKind::Query
            };
            state.sentinels.insert(
                path.clone(),
                DirSentinel {
                    mount_id,
                    fstype,
                    class,
                    lease_kind,
                    signature: dir_sentinel_signature(&meta),
                    trust_clock: strict_sla_allowed,
                    trust_nlink: strict_sla_allowed,
                    strict_sla_allowed,
                    sentinel_state: FastScanSentinelState::BackfillPending,
                    last_checked_unix_ms: 0,
                    last_changed_unix_ms: now_ms,
                    coverage_lag_ms: 0,
                },
            );
            if !state
                .initial_backfill_queue
                .iter()
                .any(|queued| queued == &path)
            {
                state.initial_backfill_queue.push_back(path);
            }
            inserted = inserted.saturating_add(1);
        }
        inserted
    }

    pub fn fast_scan_tick(
        &self,
        mount_table: &MountTable,
        config: FastScanTickConfig,
    ) -> FastScanTickResult {
        if !config.enabled {
            return FastScanTickResult::default();
        }

        let now_ms = unix_millis();
        let now_secs = unix_secs();
        let target_ms = config.target_secs.saturating_mul(1_000);
        let tick_ms = config.tick_ms.max(1);
        let mut local_budget = config.local_stat_budget_per_tick.max(1);
        let mut untrusted_budget = if config.network_mode == NetworkFastScanMode::Disabled {
            0
        } else {
            config.network_stat_budget_per_tick
        };
        let mut checked_dirs = 0usize;

        let mut state = self.fast_scan_state.write();
        self.prune_expired_fast_scan_leases_locked(&mut state, now_secs);
        let stale_sentinels = state
            .sentinels
            .keys()
            .filter(|path| {
                state
                    .leases
                    .get(path.as_path())
                    .is_none_or(|lease| lease.expired(now_secs))
            })
            .cloned()
            .collect::<Vec<_>>();
        for path in stale_sentinels {
            state.sentinels.remove(path.as_path());
            remove_queued_path(&mut state.initial_backfill_queue, path.as_path());
            remove_queued_path(&mut state.changed_dir_queue, path.as_path());
        }

        let local_known = state
            .sentinels
            .values()
            .filter(|sentinel| {
                sentinel.strict_sla_allowed
                    && sentinel.sentinel_state == FastScanSentinelState::Active
            })
            .count();
        let required_local_per_tick = required_fast_scan_budget(local_known, target_ms, tick_ms);
        let budget_degraded =
            local_known > 0 && config.local_stat_budget_per_tick < required_local_per_tick;
        self.fast_scan_budget_degraded
            .store(budget_degraded, Ordering::Relaxed);
        if budget_degraded {
            state.last_degraded_reason = format!(
                "local fast scan budget {} below required {} dirs/tick",
                config.local_stat_budget_per_tick, required_local_per_tick
            );
        }

        let mut candidates = state
            .sentinels
            .iter_mut()
            .filter_map(|(path, sentinel)| {
                sentinel.coverage_lag_ms = now_ms.saturating_sub(sentinel.last_checked_unix_ms);
                let enabled_for_mount = sentinel.strict_sla_allowed
                    || config.network_mode != NetworkFastScanMode::Disabled;
                if !enabled_for_mount || sentinel.coverage_lag_ms < target_ms {
                    return None;
                }
                Some((
                    sentinel.strict_sla_allowed,
                    sentinel.sentinel_state == FastScanSentinelState::Active,
                    sentinel.last_checked_unix_ms,
                    path.clone(),
                ))
            })
            .collect::<Vec<_>>();
        candidates.sort_by_key(|(strict, active, last_checked, path)| {
            (
                std::cmp::Reverse(*strict),
                std::cmp::Reverse(*active),
                *last_checked,
                path.clone(),
            )
        });

        for (strict_sla_allowed, _, _, path) in candidates {
            if strict_sla_allowed {
                if local_budget == 0 {
                    continue;
                }
                local_budget -= 1;
            } else {
                if untrusted_budget == 0 {
                    continue;
                }
                untrusted_budget -= 1;
            }

            let meta = match std::fs::symlink_metadata(&path) {
                Ok(meta) => meta,
                Err(_) => {
                    state.sentinels.remove(path.as_path());
                    continue;
                }
            };
            if !meta.is_dir() {
                state.sentinels.remove(path.as_path());
                continue;
            };
            let Some((changed, sentinel_strict, sentinel_state)) = ({
                let Some(sentinel) = state.sentinels.get_mut(path.as_path()) else {
                    continue;
                };

                let (mount_id, fstype, class) = fast_scan_mount_info(path.as_path(), mount_table);
                let signature = dir_sentinel_signature(&meta);
                let strict_poll = !sentinel.strict_sla_allowed
                    && config.network_mode == NetworkFastScanMode::StrictPoll;
                let changed = strict_poll || signature != sentinel.signature;
                sentinel.mount_id = mount_id;
                sentinel.fstype = fstype;
                sentinel.class = class;
                sentinel.strict_sla_allowed = class.strict_sla_allowed();
                sentinel.trust_clock = sentinel.strict_sla_allowed;
                sentinel.trust_nlink = sentinel.strict_sla_allowed;
                sentinel.signature = signature;
                sentinel.last_checked_unix_ms = now_ms;
                sentinel.coverage_lag_ms = 0;
                checked_dirs = checked_dirs.saturating_add(1);
                if changed {
                    sentinel.last_changed_unix_ms = now_ms;
                }
                Some((
                    changed,
                    sentinel.strict_sla_allowed,
                    sentinel.sentinel_state,
                ))
            }) else {
                continue;
            };
            if let Some(lease) = state.leases.get_mut(path.as_path()) {
                lease.strict_sla_allowed = sentinel_strict;
                lease.last_used_unix_secs = now_secs;
                lease.sentinel_state = sentinel_state;
            }

            if changed && !state.changed_dir_queue.iter().any(|queued| queued == &path) {
                state.changed_dir_queue.push_back(path);
            }
        }

        let initial_budget = config.initial_backfill_budget_per_tick.max(1);
        let mut initial_dirs = Vec::new();
        while initial_dirs.len() < initial_budget {
            let Some(path) = state.initial_backfill_queue.pop_front() else {
                break;
            };
            if state
                .leases
                .get(path.as_path())
                .is_none_or(|lease| lease.expired(now_secs))
            {
                continue;
            }
            if let Some(sentinel) = state.sentinels.get_mut(path.as_path()) {
                sentinel.sentinel_state = FastScanSentinelState::Active;
            }
            if let Some(lease) = state.leases.get_mut(path.as_path()) {
                lease.sentinel_state = FastScanSentinelState::Active;
            }
            initial_dirs.push(path);
        }

        let changed_budget = config
            .local_readdir_budget_per_tick
            .saturating_add(config.network_readdir_budget_per_tick)
            .max(1);
        let mut changed_dirs = Vec::new();
        while changed_dirs.len() < changed_budget {
            let Some(path) = state.changed_dir_queue.pop_front() else {
                break;
            };
            changed_dirs.push(path);
        }
        let pending = state.changed_dir_queue.len();
        let pending_initial = state.initial_backfill_queue.len();
        self.fast_scan_pending_changed_dirs
            .store(pending, Ordering::Relaxed);
        self.fast_scan_checked_dirs
            .fetch_add(checked_dirs as u64, Ordering::Relaxed);
        self.fast_scan_changed_dirs.fetch_add(
            initial_dirs.len().saturating_add(changed_dirs.len()) as u64,
            Ordering::Relaxed,
        );
        self.fast_scan_real_changed_dirs
            .fetch_add(changed_dirs.len() as u64, Ordering::Relaxed);
        if pending > 0 {
            self.fast_scan_budget_degraded
                .store(true, Ordering::Relaxed);
            state.last_degraded_reason = format!("changed-dir queue pending={pending}");
            self.fast_scan_io_budget_limited_count
                .fetch_add(1, Ordering::Relaxed);
        } else if pending_initial > 0 {
            state.last_degraded_reason =
                format!("initial hotset backfill pending={pending_initial}");
        }

        FastScanTickResult {
            checked_dirs,
            initial_dirs,
            changed_dirs,
            budget_degraded: self.fast_scan_budget_degraded.load(Ordering::Relaxed),
        }
    }

    pub fn record_fast_scan_generated_events(&self, count: usize) {
        self.fast_scan_generated_events
            .fetch_add(count as u64, Ordering::Relaxed);
    }

    pub fn record_fast_scan_apply_dropped_stale_batches(&self, count: usize) {
        self.fast_scan_apply_dropped_stale_batches
            .fetch_add(count as u64, Ordering::Relaxed);
    }

    pub fn record_fast_scan_parent_fence_retry(&self) {
        self.fast_scan_parent_fence_retries
            .fetch_add(1, Ordering::Relaxed);
    }
}

pub(super) fn network_fast_scan_mode_to_u8(mode: NetworkFastScanMode) -> u8 {
    match mode {
        NetworkFastScanMode::BestEffort => 0,
        NetworkFastScanMode::StrictPoll => 1,
        NetworkFastScanMode::Disabled => 2,
    }
}

pub(super) fn network_fast_scan_mode_from_u8(value: u8) -> NetworkFastScanMode {
    match value {
        1 => NetworkFastScanMode::StrictPoll,
        2 => NetworkFastScanMode::Disabled,
        _ => NetworkFastScanMode::BestEffort,
    }
}

pub(super) fn network_fast_scan_mode_label(mode: NetworkFastScanMode) -> &'static str {
    match mode {
        NetworkFastScanMode::BestEffort => "best_effort",
        NetworkFastScanMode::StrictPoll => "strict_poll",
        NetworkFastScanMode::Disabled => "disabled",
    }
}

pub(super) fn fast_scan_mode_label(
    enabled: bool,
    local_trusted_dirs: usize,
    untrusted_dirs: usize,
    network_mode: NetworkFastScanMode,
) -> &'static str {
    if !enabled {
        "disabled"
    } else if local_trusted_dirs > 0 && untrusted_dirs > 0 {
        "mixed"
    } else if local_trusted_dirs > 0 {
        "local_strict"
    } else if untrusted_dirs > 0 {
        network_fast_scan_mode_label(network_mode)
    } else {
        "idle"
    }
}

pub(super) fn required_fast_scan_budget(known_dirs: usize, target_ms: u64, tick_ms: u64) -> usize {
    if known_dirs == 0 {
        return 0;
    }
    let target_ms = target_ms.max(1);
    let numerator = (known_dirs as u128).saturating_mul(u128::from(tick_ms.max(1)));
    numerator.div_ceil(u128::from(target_ms)).max(1) as usize
}

pub(super) fn fast_scan_mount_info(
    path: &Path,
    mount_table: &MountTable,
) -> (u32, String, FastScanMountClass) {
    let Some(mount) = mount_table.best_match(path) else {
        return (0, "unknown".to_string(), FastScanMountClass::Unknown);
    };
    (
        mount.mount_id,
        mount.fstype.clone(),
        classify_fast_scan_fstype(&mount.fstype),
    )
}

pub(super) fn normalize_fast_scan_dir(path: PathBuf) -> PathBuf {
    path.canonicalize().unwrap_or(path)
}

pub(super) fn remove_queued_path(queue: &mut VecDeque<PathBuf>, path: &Path) {
    queue.retain(|queued| queued.as_path() != path);
}
